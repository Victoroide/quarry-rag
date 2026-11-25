package main

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"

	"github.com/Victoroide/quarry-rag/internal/chunker"
	"github.com/Victoroide/quarry-rag/internal/domain"
	pgstore "github.com/Victoroide/quarry-rag/internal/store/postgres"
)

// corpus reads the completed documents that PostgreSQL holds authoritatively.
type corpus interface {
	DocumentRevisions(ctx context.Context, after string, limit int) ([]pgstore.DocumentRevision, error)
	LoadDocumentSnapshot(ctx context.Context, id string) (*pgstore.DocumentSnapshot, error)
}

// ledger records durable per-document progress for one target generation.
type ledger interface {
	GenerationCheckpoints(ctx context.Context, generation string, documentIDs []string) (map[string]pgstore.Checkpoint, error)
	BeginGenerationDocument(ctx context.Context, generation, documentID string, chunkIDs []string) ([]string, error)
	CompleteGenerationDocument(ctx context.Context, generation, documentID, revision, incarnation string, chunkIDs []string, copied, embedded int) error
	OrphanedGenerationDocuments(ctx context.Context, generation string, limit int) ([]pgstore.Checkpoint, error)
	ForgetGenerationDocument(ctx context.Context, generation, documentID string) error
}

// vectorReader reads a vector store without changing it.
type vectorReader interface {
	Fetch(ctx context.Context, ids []string) (map[string]domain.Chunk, error)
	ListIDs(ctx context.Context, token string, limit int) (ids []string, next string, err error)
	QueryFiltered(ctx context.Context, embedding []float32, topK int, filter domain.VectorFilter) ([]domain.ScoredChunk, error)
}

// vectorTarget is a staged vector store.
type vectorTarget interface {
	vectorReader
	Upsert(ctx context.Context, namespace string, chunks []domain.Chunk) error
	DeleteIDs(ctx context.Context, ids []string) error
}

// decideCopy reports whether stored vectors may be reused. Reuse needs a proven
// source profile in the same embedding space; equal dimensions never suffice. A
// different extraction or chunk version cannot be bridged here, because the
// stored chunks would not match the target's text.
func decideCopy(source *domain.EmbeddingProfile, target domain.EmbeddingProfile) (reuse bool, reason string, err error) {
	switch {
	case source == nil:
		return false, "the source profile is unknown; attest it or re-embed", nil
	case !source.SameTextVersion(target):
		return false, "", fmt.Errorf("the target extraction_chunk_version %q differs from the stored chunks' %q; re-extraction needs the original files and is a separate operation", target.ExtractionChunkVersion, source.ExtractionChunkVersion)
	case !source.SameEmbeddingSpace(target):
		return false, "the embedding profiles differ; vectors are recomputed from the stored chunks", nil
	}
	return true, "", nil
}

// deleteBatch bounds the keys of one deletion request on every store.
const deleteBatch = 1000

type stageReport struct {
	Generation   string   `json:"generation"`
	Scanned      int      `json:"scanned_documents"`
	Skipped      int      `json:"current_documents"`
	Staged       int      `json:"staged_documents"`
	Vanished     int      `json:"vanished_documents"`
	Inconsistent []string `json:"inconsistent_documents"`
	Pruned       int      `json:"pruned_documents"`
	Chunks       int      `json:"staged_chunks"`
	Copied       int      `json:"copied_chunks"`
	Embedded     int      `json:"embedded_chunks"`
	Bytes        int64    `json:"staged_bytes"`
	Complete     bool     `json:"complete"`
	Stopped      string   `json:"stopped,omitempty"`
	NextAfter    string   `json:"next_after,omitempty"`
}

// errStop ends a run cleanly at a bound without staging a partial document.
type errStop string

func (e errStop) Error() string { return string(e) }

// stager copies or computes the vectors of completed documents into a target,
// recording a durable checkpoint for every document.
type stager struct {
	corpus     corpus
	ledger     ledger
	target     vectorTarget
	generation string
	profile    domain.EmbeddingProfile
	opts       options
	// copyFrom serves reusable vectors; nil forces every vector to be computed.
	copyFrom vectorReader
	embed    domain.EmbeddingProvider
	calls    int
}

func validVector(v []float32, dimensions int) error {
	if len(v) != dimensions {
		return fmt.Errorf("vector has %d dimensions, want %d", len(v), dimensions)
	}
	nonzero := false
	for _, x := range v {
		if math.IsNaN(float64(x)) || math.IsInf(float64(x), 0) {
			return errors.New("vector holds a non-finite value")
		}
		nonzero = nonzero || x != 0
	}
	if !nonzero {
		return errors.New("a cosine vector must be nonzero")
	}
	return nil
}

func (s *stager) run(ctx context.Context) (stageReport, error) {
	rep := stageReport{Generation: s.generation, Inconsistent: []string{}}
	after := s.opts.After
	for {
		page, err := s.corpus.DocumentRevisions(ctx, after, s.opts.Page)
		if err != nil {
			return rep, err
		}
		ids := make([]string, len(page))
		for i, rev := range page {
			ids[i] = rev.ID
		}
		prior, err := s.ledger.GenerationCheckpoints(ctx, s.generation, ids)
		if err != nil {
			return rep, err
		}
		for _, rev := range page {
			after = rev.ID
			rep.NextAfter = after
			rep.Scanned++
			cp := prior[rev.ID]
			if cp.Complete && cp.SourceRevision == rev.Revision && cp.Incarnation == rev.Incarnation {
				rep.Skipped++
				continue
			}
			if rev.DeclaredChunks != rev.Chunks {
				rep.Inconsistent = append(rep.Inconsistent, rev.ID)
				continue
			}
			if rep.Staged >= s.opts.MaxDocuments {
				rep.Stopped = "document limit"
				return rep, nil
			}
			if stageErr := s.stageDocument(ctx, rev, &rep); stageErr != nil {
				var stop errStop
				if errors.As(stageErr, &stop) {
					rep.Stopped = string(stop)
					return rep, nil
				}
				return rep, fmt.Errorf("document %s: %w", rev.ID, stageErr)
			}
		}
		if len(page) < s.opts.Page {
			break
		}
	}
	rep.NextAfter = ""
	if err := s.prune(ctx, &rep); err != nil {
		return rep, err
	}
	rep.Complete = len(rep.Inconsistent) == 0
	return rep, nil
}

func (s *stager) stageDocument(ctx context.Context, rev pgstore.DocumentRevision, rep *stageReport) error {
	snap, err := s.corpus.LoadDocumentSnapshot(ctx, rev.ID)
	if err != nil {
		return err
	}
	if snap == nil {
		rep.Vanished++
		return nil
	}
	if snap.DeclaredChunks != len(snap.Chunks) {
		rep.Inconsistent = append(rep.Inconsistent, rev.ID)
		return nil
	}
	var bytes int64
	ids := make([]string, len(snap.Chunks))
	for i, c := range snap.Chunks {
		ids[i] = string(c.ID)
		bytes += int64(len(c.Text))
	}
	if rep.Chunks+len(ids) > s.opts.MaxChunks || rep.Bytes+bytes > s.opts.MaxBytes {
		if rep.Staged == 0 {
			return fmt.Errorf("document alone exceeds -max-chunks or -max-bytes (%d chunks, %d bytes)", len(ids), bytes)
		}
		return errStop("chunk or byte limit")
	}
	chunks, copied, embedded, err := s.vectors(ctx, snap)
	if err != nil {
		return err
	}
	recorded, err := s.ledger.BeginGenerationDocument(ctx, s.generation, rev.ID, ids)
	if err != nil {
		return err
	}
	for start := 0; start < len(chunks); start += s.opts.Batch {
		if err = s.target.Upsert(ctx, "", chunks[start:min(start+s.opts.Batch, len(chunks))]); err != nil {
			return fmt.Errorf("write vectors: %w", err)
		}
	}
	var stale []string
	for _, id := range recorded {
		if !slices.Contains(ids, id) {
			stale = append(stale, id)
		}
	}
	if deleteErr := s.deleteKeys(ctx, stale); deleteErr != nil {
		return deleteErr
	}
	if completeErr := s.ledger.CompleteGenerationDocument(ctx, s.generation, rev.ID, snap.Revision, snap.Incarnation, ids, copied, embedded); completeErr != nil {
		return completeErr
	}
	rep.Staged++
	rep.Chunks += len(ids)
	rep.Bytes += bytes
	rep.Copied += copied
	rep.Embedded += embedded
	return nil
}

func (s *stager) deleteKeys(ctx context.Context, ids []string) error {
	for start := 0; start < len(ids); start += deleteBatch {
		if err := s.target.DeleteIDs(ctx, ids[start:min(start+deleteBatch, len(ids))]); err != nil {
			return fmt.Errorf("remove stale vectors: %w", err)
		}
	}
	return nil
}

// vectors returns the document's chunks with target vectors. A source vector
// is reused only when it is valid, belongs to the same chunk text and the
// source profile is proven identical to the target's; everything else is
// computed from the stored text within the embedding call budget.
func (s *stager) vectors(ctx context.Context, snap *pgstore.DocumentSnapshot) (chunks []domain.Chunk, copied, embedded int, err error) {
	chunks = make([]domain.Chunk, len(snap.Chunks))
	var reusable map[string]domain.Chunk
	if s.copyFrom != nil {
		ids := make([]string, len(snap.Chunks))
		for i, c := range snap.Chunks {
			ids[i] = string(c.ID)
		}
		reusable = make(map[string]domain.Chunk, len(ids))
		for start := 0; start < len(ids); start += 100 {
			part, fetchErr := s.copyFrom.Fetch(ctx, ids[start:min(start+100, len(ids))])
			if fetchErr != nil {
				return nil, 0, 0, fmt.Errorf("read source vectors: %w", fetchErr)
			}
			for id, c := range part {
				reusable[id] = c
			}
		}
	}
	var missing []int
	for i, c := range snap.Chunks {
		c.Metadata = chunkMetadata(snap, c)
		chunks[i] = c
		src, ok := reusable[string(c.ID)]
		srcTenant, _ := src.Metadata[domain.VectorTenantKey].(string)
		if ok && src.DocumentID == c.DocumentID && src.Text == c.Text && src.Index == c.Index &&
			(srcTenant == "" || srcTenant == snap.TenantID) &&
			validVector(src.Embedding, s.profile.Dimensions) == nil {
			chunks[i].Embedding = src.Embedding
			copied++
			continue
		}
		missing = append(missing, i)
	}
	if len(missing) == 0 {
		return chunks, copied, 0, nil
	}
	if s.embed == nil || s.calls+len(missing) > s.opts.MaxEmbedCalls {
		return nil, 0, 0, errStop("embedding call budget")
	}
	for _, i := range missing {
		vector, embedErr := s.embed.Embed(ctx, chunks[i].Text)
		s.calls++
		if embedErr != nil {
			return nil, 0, 0, fmt.Errorf("embed chunk %s: %w", chunks[i].ID, embedErr)
		}
		if invalid := validVector(vector, s.profile.Dimensions); invalid != nil {
			return nil, 0, 0, fmt.Errorf("embed chunk %s: %w", chunks[i].ID, invalid)
		}
		chunks[i].Embedding = vector
		embedded++
	}
	return chunks, copied, embedded, nil
}

// chunkMetadata derives the stored labels from PostgreSQL, the authority for
// document type and collection, and from the chunk text.
func chunkMetadata(snap *pgstore.DocumentSnapshot, c domain.Chunk) map[string]any {
	kind, quality := chunker.Classify(c.Text)
	md := map[string]any{"doc_type": snap.DocType, "kind": string(kind), "quality": string(quality)}
	if snap.Collection != "" {
		md["collection"] = snap.Collection
	}
	if snap.TenantID != "" {
		md[domain.VectorTenantKey] = snap.TenantID
	}
	return md
}

// prune removes target keys of documents that are no longer eligible sources.
func (s *stager) prune(ctx context.Context, rep *stageReport) error {
	for {
		orphans, err := s.ledger.OrphanedGenerationDocuments(ctx, s.generation, 100)
		if err != nil {
			return err
		}
		if len(orphans) == 0 {
			return nil
		}
		for _, o := range orphans {
			if deleteErr := s.deleteKeys(ctx, o.ChunkIDs); deleteErr != nil {
				return deleteErr
			}
			if forgetErr := s.ledger.ForgetGenerationDocument(ctx, s.generation, o.DocumentID); forgetErr != nil {
				return forgetErr
			}
			rep.Pruned++
		}
	}
}
