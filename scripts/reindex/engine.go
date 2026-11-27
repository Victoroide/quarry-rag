package main

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/Victoroide/quarry-rag/internal/chunker"
	"github.com/Victoroide/quarry-rag/internal/domain"
	pcstore "github.com/Victoroide/quarry-rag/internal/store/pinecone"
	pgstore "github.com/Victoroide/quarry-rag/internal/store/postgres"
)

// corpus reads the completed documents that PostgreSQL holds authoritatively.
type corpus interface {
	DocumentRevisions(ctx context.Context, after string, limit int) ([]pgstore.DocumentRevision, error)
	HasDocumentsAfter(ctx context.Context, after string) (bool, error)
	LoadDocumentSnapshotBounded(ctx context.Context, id string, maxChunks int, maxBytes int64) (*pgstore.DocumentSnapshot, error)
}

// ledger records durable per-document progress for one target generation.
type ledger interface {
	GenerationCheckpoints(ctx context.Context, generation string, documentIDs []string) (map[string]pgstore.Checkpoint, error)
	BeginGenerationDocument(ctx context.Context, generation, documentID string, chunkIDs []string) ([]string, error)
	CompleteGenerationDocument(ctx context.Context, generation, documentID, revision, incarnation string, chunkIDs []string, copied, embedded int) error
	OrphanedGenerationDocuments(ctx context.Context, generation string, limit int) ([]pgstore.Checkpoint, error)
	HasOrphanedGenerationDocuments(ctx context.Context, generation string) (bool, error)
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
	Generation     string   `json:"generation"`
	Scanned        int      `json:"scanned_documents"`
	Skipped        int      `json:"current_documents"`
	Staged         int      `json:"staged_documents"`
	Vanished       int      `json:"vanished_documents"`
	Inconsistent   []string `json:"inconsistent_documents"`
	Unsupported    []string `json:"unsupported_documents"`
	Pruned         int      `json:"pruned_documents"`
	PrunedKeys     int      `json:"pruned_keys"`
	ScannedKeys    int      `json:"scanned_target_keys"`
	Chunks         int      `json:"staged_chunks"`
	Copied         int      `json:"copied_chunks"`
	Embedded       int      `json:"embedded_chunks"`
	Bytes          int64    `json:"staged_bytes"`
	Complete       bool     `json:"complete"`
	Stopped        string   `json:"stopped,omitempty"`
	NextAfter      string   `json:"next_after,omitempty"`
	NextPruneAfter string   `json:"next_prune_after,omitempty"`
	loadedChunks   int
	loadedBytes    int64
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
	fence      *pgstore.GenerationFence
	retired    *pgstore.Client
	// taggedTarget is true when the target stores revision-qualified hosted
	// vector IDs: manifest keys with revision_id metadata, or tagged keys.
	taggedTarget bool
	// copyFromTagged is the same property of the store that copyFrom reads.
	copyFromTagged bool
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
	rep := stageReport{Generation: s.generation, Inconsistent: []string{}, Unsupported: []string{}, NextAfter: s.opts.After, NextPruneAfter: s.opts.PruneAfter}
	for rep.Scanned < s.opts.MaxDocuments {
		limit := min(s.opts.Page, s.opts.MaxDocuments-rep.Scanned)
		page, err := s.corpus.DocumentRevisions(ctx, rep.NextAfter, limit)
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
			rep.Scanned++
			cp := prior[rev.ID]
			switch {
			case rev.DeclaredChunks != rev.Chunks:
				rep.Inconsistent = append(rep.Inconsistent, rev.ID)
			case taggedRefusal(s.taggedTarget, rev) != "":
				rep.Unsupported = append(rep.Unsupported, rev.ID)
			case cp.Complete && cp.SourceRevision == rev.Revision && cp.Incarnation == rev.Incarnation:
				rep.Skipped++
			default:
				if stageErr := s.stageDocument(ctx, rev, &rep); stageErr != nil {
					stageErr = stopStage(&rep, fmt.Errorf("document %s: %w", rev.ID, stageErr))
					return rep, stageErr
				}
			}
			rep.NextAfter = rev.ID
		}
		if len(page) < limit {
			break
		}
	}
	if rep.Scanned == s.opts.MaxDocuments {
		more, err := s.corpus.HasDocumentsAfter(ctx, rep.NextAfter)
		if err != nil {
			return rep, err
		}
		if more {
			rep.Stopped = "document limit"
			return rep, nil
		}
	}
	if err := s.prune(ctx, &rep); err != nil {
		err = stopStage(&rep, err)
		return rep, err
	}
	if s.retired != nil {
		if err := s.pruneRetired(ctx, &rep); err != nil {
			err = stopStage(&rep, err)
			return rep, err
		}
	}
	rep.NextAfter = ""
	rep.Complete = len(rep.Inconsistent) == 0 && len(rep.Unsupported) == 0
	return rep, nil
}

func stopStage(rep *stageReport, err error) error {
	var stop errStop
	if errors.As(err, &stop) {
		rep.Stopped = string(stop)
		return nil
	}
	return err
}

func (s *stager) stageDocument(ctx context.Context, rev pgstore.DocumentRevision, rep *stageReport) error {
	snap, err := s.corpus.LoadDocumentSnapshotBounded(ctx, rev.ID, s.opts.MaxChunks-rep.loadedChunks, s.opts.MaxBytes-rep.loadedBytes)
	if errors.Is(err, pgstore.ErrSnapshotLimit) {
		if rep.Staged == 0 {
			return fmt.Errorf("document alone exceeds -max-chunks or -max-bytes: %w", err)
		}
		return errStop("chunk or byte limit")
	}
	if err != nil {
		return err
	}
	if snap == nil {
		rep.Vanished++
		return nil
	}
	rep.loadedChunks += len(snap.Chunks)
	rep.loadedBytes += snap.TextBytes
	if snap.DeclaredChunks != len(snap.Chunks) {
		rep.Inconsistent = append(rep.Inconsistent, rev.ID)
		return nil
	}
	if taggedRefusal(s.taggedTarget, snap.DocumentRevision) != "" {
		rep.Unsupported = append(rep.Unsupported, rev.ID)
		return nil
	}
	ids := make([]string, len(snap.Chunks))
	for i, c := range snap.Chunks {
		ids[i], err = vectorKey(s.taggedTarget, snap.DocumentRevision, c)
		if err != nil {
			return err
		}
	}
	chunks, copied, embedded, err := s.vectors(ctx, snap)
	if err != nil {
		return err
	}
	recorded, err := s.ledger.BeginGenerationDocument(ctx, s.generation, rev.ID, ids)
	if err != nil {
		return err
	}
	var stale []string
	current := make(map[string]bool, len(ids))
	for _, id := range ids {
		current[id] = true
	}
	for _, id := range recorded {
		if !current[id] {
			stale = append(stale, id)
		}
	}
	if len(ids)+len(stale) > s.opts.MaxChunks-rep.Chunks-rep.PrunedKeys {
		if rep.Staged == 0 {
			return errors.New("document and its stale keys alone exceed -max-chunks")
		}
		return errStop("chunk or byte limit")
	}
	for start := 0; start < len(chunks); start += s.opts.Batch {
		if err = s.write(ctx, func() error {
			return s.target.Upsert(ctx, "", chunks[start:min(start+s.opts.Batch, len(chunks))])
		}); err != nil {
			return fmt.Errorf("write vectors: %w", err)
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
	rep.Bytes += snap.TextBytes
	rep.PrunedKeys += len(stale)
	rep.Copied += copied
	rep.Embedded += embedded
	return nil
}

func (s *stager) write(ctx context.Context, write func() error) error {
	if s.fence == nil {
		return write()
	}
	return s.fence.Write(ctx, write)
}

func (s *stager) deleteKeys(ctx context.Context, ids []string) error {
	for start := 0; start < len(ids); start += deleteBatch {
		if err := s.write(ctx, func() error {
			return s.target.DeleteIDs(ctx, ids[start:min(start+deleteBatch, len(ids))])
		}); err != nil {
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
	var sourceKeys []string
	if s.copyFrom != nil {
		sourceKeys = make([]string, len(snap.Chunks))
		for i, c := range snap.Chunks {
			sourceKeys[i], err = vectorKey(s.copyFromTagged, snap.DocumentRevision, c)
			if err != nil {
				return nil, 0, 0, err
			}
		}
		reusable = make(map[string]domain.Chunk, len(sourceKeys))
		for start := 0; start < len(sourceKeys); start += 100 {
			part, fetchErr := s.copyFrom.Fetch(ctx, sourceKeys[start:min(start+100, len(sourceKeys))])
			if hardSourceError(ctx, fetchErr) {
				return nil, 0, 0, fmt.Errorf("read source vectors: %w", fetchErr)
			}
			for id, c := range part {
				reusable[id] = c
			}
		}
	}
	var missing []int
	for i, c := range snap.Chunks {
		c.Metadata = chunkMetadata(s.taggedTarget, snap, c)
		// A target record names the document incarnation it was written for, so
		// that retrieval can discard a record that outlived its document.
		c.VectorIncarnation = snap.Incarnation
		if s.taggedTarget && snap.ActiveRevisionID != "" {
			c.RevisionID = snap.ActiveRevisionID
			c.VectorID = domain.FormatRevisionVectorID(domain.DocumentID(snap.ID), snap.ActiveRevisionID, c.Index)
		}
		chunks[i] = c
		sourceKey := string(c.ID)
		if s.copyFrom != nil {
			sourceKey = sourceKeys[i]
		}
		src, ok := reusable[sourceKey]
		identityMatches := sourceIdentityMatches(s.copyFromTagged, snap, c, src, ok)
		if identityMatches {
			copyLayoutMetadata(chunks[i].Metadata, src.Metadata)
		}
		if identityMatches && validVector(src.Embedding, s.profile.Dimensions) == nil {
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

func hardSourceError(ctx context.Context, err error) bool {
	return err != nil && (ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, pgstore.ErrGenerationDrift))
}

// chunkMetadata derives the stored labels from PostgreSQL, the authority for
// document type and collection, and from the chunk text.
func chunkMetadata(tagged bool, snap *pgstore.DocumentSnapshot, c domain.Chunk) map[string]any {
	kind, quality := chunker.Classify(c.Text)
	md := map[string]any{"doc_type": snap.DocType, "kind": string(kind), "quality": string(quality)}
	if snap.Collection != "" {
		md["collection"] = snap.Collection
	}
	if snap.TenantID != "" {
		md[domain.VectorTenantKey] = snap.TenantID
	}
	copyLayoutMetadata(md, c.Metadata)
	if tagged && isTagged(snap.DocumentRevision) {
		md[domain.VectorRevisionKey] = snap.Incarnation
		md[domain.VectorAttemptKey] = snap.VectorAttempt
	}
	return md
}

func copyLayoutMetadata(dst, src map[string]any) {
	domain.CopyValidLayoutProvenance(dst, src)
}

// prune removes target keys of documents that are no longer eligible sources.
func (s *stager) prune(ctx context.Context, rep *stageReport) error {
	for {
		remaining := s.opts.MaxDocuments - rep.Scanned - rep.Pruned
		if remaining == 0 {
			more, err := s.ledger.HasOrphanedGenerationDocuments(ctx, s.generation)
			if err != nil || !more {
				return err
			}
			return errStop("orphan document limit")
		}
		orphans, err := s.ledger.OrphanedGenerationDocuments(ctx, s.generation, min(s.opts.Page, remaining))
		if err != nil {
			return err
		}
		if len(orphans) == 0 {
			return nil
		}
		for _, o := range orphans {
			if len(o.ChunkIDs) > s.opts.MaxChunks-rep.Chunks-rep.PrunedKeys {
				return errStop("orphan key limit")
			}
			if deleteErr := s.deleteKeys(ctx, o.ChunkIDs); deleteErr != nil {
				return deleteErr
			}
			if forgetErr := s.ledger.ForgetGenerationDocument(ctx, s.generation, o.DocumentID); forgetErr != nil {
				return forgetErr
			}
			rep.Pruned++
			rep.PrunedKeys += len(o.ChunkIDs)
		}
	}
}

func (s *stager) pruneRetired(ctx context.Context, rep *stageReport) error {
	for {
		limit := min(deleteBatch, s.opts.MaxChunks-rep.ScannedKeys)
		if limit == 0 {
			return errStop("target key scan limit")
		}
		keys, next, err := s.target.ListIDs(ctx, rep.NextPruneAfter, limit)
		if err != nil {
			return fmt.Errorf("list retired target keys: %w", err)
		}
		var untagged []string
		var tagged []pgstore.TaggedVectorKey
		for _, key := range keys {
			if s.taggedTarget {
				if doc, rev, att, chunk, ok := pcstore.ParseTaggedVectorID(key); ok {
					tagged = append(tagged, pgstore.TaggedVectorKey{
						Key:        key,
						DocumentID: doc,
						Revision:   rev,
						Attempt:    att,
						ChunkID:    chunk,
					})
					continue
				}
			}
			untagged = append(untagged, key)
		}
		var extra []string
		if len(untagged) > 0 {
			uExtra, uErr := s.retired.UnexpectedChunkKeys(ctx, untagged, s.taggedTarget)
			if uErr != nil {
				return uErr
			}
			extra = append(extra, uExtra...)
		}
		if len(tagged) > 0 {
			tExtra, tErr := s.retired.UnexpectedTaggedKeys(ctx, tagged)
			if tErr != nil {
				return tErr
			}
			extra = append(extra, tExtra...)
		}
		if deleteErr := s.deleteKeys(ctx, extra); deleteErr != nil {
			return deleteErr
		}
		rep.PrunedKeys += len(extra)
		rep.ScannedKeys += len(keys)
		rep.NextPruneAfter = next
		if next == "" {
			return nil
		}
	}
}
