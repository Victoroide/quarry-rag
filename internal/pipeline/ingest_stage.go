package pipeline

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/Victoroide/quarry-rag/internal/chunker"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/embeddinginput"
	"github.com/Victoroide/quarry-rag/internal/metrics"
	"github.com/Victoroide/quarry-rag/internal/tracing"
	"golang.org/x/sync/errgroup"
)

// ErrNoChunks indicates that document text produced no indexable chunks.
var ErrNoChunks = errors.New("no chunks produced")

// ChunkStage splits a raw document into chunks using the sliding window chunker.
type ChunkStage struct {
	chunker *chunker.Chunker
	profile *embeddinginput.Profile
}

// NewChunkStage creates a ChunkStage.
func NewChunkStage(c *chunker.Chunker) *ChunkStage {
	return NewChunkStageWithEmbeddingProfile(c, nil)
}

// NewChunkStageWithEmbeddingProfile creates a ChunkStage with local model limits.
func NewChunkStageWithEmbeddingProfile(c *chunker.Chunker, p *embeddinginput.Profile) *ChunkStage {
	return &ChunkStage{chunker: c, profile: p}
}

// ID returns the stage identifier.
func (s *ChunkStage) ID() StageID { return StageChunk }

// Run splits the raw text or ordered layout sections into chunks.
func (s *ChunkStage) Run(ctx context.Context, ic *IngestContext) error {
	redact := tracing.RedactIdentifiers(ctx) || ic.Tenant != ""
	var err error
	if len(ic.LayoutSections) > 0 {
		ic.Chunks, err = s.chunker.ChunkLayout(ctx, ic.DocumentID, ic.LayoutSections, s.profile)
	} else {
		ic.Chunks, err = s.chunker.ChunkForEmbedding(ctx, ic.DocumentID, ic.RawText, s.profile)
	}
	if err != nil {
		if redact {
			return fmt.Errorf("chunk_stage: %w", err)
		}
		return fmt.Errorf("chunk_stage: document %s: %w", ic.DocumentID, err)
	}
	if len(ic.Chunks) == 0 {
		if redact {
			return fmt.Errorf("chunk_stage: %w", ErrNoChunks)
		}
		return fmt.Errorf("chunk_stage: document %s: %w", ic.DocumentID, ErrNoChunks)
	}
	if redact {
		slog.DebugContext(ctx, "document chunked", "chunks", len(ic.Chunks))
	} else {
		slog.DebugContext(ctx, "document chunked", "document_id", ic.DocumentID, "chunks", len(ic.Chunks))
	}
	return nil
}

// EmbedStage generates embeddings for all chunks concurrently.
type EmbedStage struct {
	embedder    domain.EmbeddingProvider
	concurrency int
}

// NewEmbedStage creates an EmbedStage.
func NewEmbedStage(emb domain.EmbeddingProvider, concurrency int) *EmbedStage {
	if concurrency < 1 {
		concurrency = 1
	}
	return &EmbedStage{embedder: emb, concurrency: concurrency}
}

// ID returns the stage identifier.
func (s *EmbedStage) ID() StageID { return StageEmbed }

// Run generates embeddings for all chunks.
func (s *EmbedStage) Run(ctx context.Context, ic *IngestContext) error {
	redact := tracing.RedactIdentifiers(ctx) || ic.Tenant != ""
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(s.concurrency)
	for i := range ic.Chunks {
		if groupCtx.Err() != nil {
			break
		}
		group.Go(func() error {
			if err := groupCtx.Err(); err != nil {
				return err
			}
			emb, err := s.embedder.Embed(groupCtx, ic.Chunks[i].Text)
			if err != nil {
				if redact {
					return fmt.Errorf("embed_stage: %w", err)
				}
				return fmt.Errorf("embed_stage: chunk %s: %w", ic.Chunks[i].ID, err)
			}
			ic.Chunks[i].Embedding = emb
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if redact {
		slog.DebugContext(ctx, "document chunks embedded", "chunks", len(ic.Chunks))
	} else {
		slog.DebugContext(ctx, "document chunks embedded", "document_id", ic.DocumentID, "chunks", len(ic.Chunks))
	}
	return nil
}

// PersistStage upserts chunks to the default vector namespace and can wait until search returns them.
type PersistStage struct {
	vectorStore       domain.VectorStore
	visibilityTimeout time.Duration
	lsnVisibility     bool
}

// PersistOption configures vector persistence.
type PersistOption func(*PersistStage)

// WithVisibilityWait bounds the wait for search visibility; zero disables it.
func WithVisibilityWait(timeout time.Duration) PersistOption {
	return func(s *PersistStage) { s.visibilityTimeout = timeout }
}

// WithLSNVisibility checks acknowledged write progress instead of sampling matches.
func WithLSNVisibility() PersistOption {
	return func(s *PersistStage) { s.lsnVisibility = true }
}

// NewPersistStage creates a PersistStage.
func NewPersistStage(vs domain.VectorStore, opts ...PersistOption) *PersistStage {
	s := &PersistStage{vectorStore: vs}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// ID returns the stage identifier.
func (s *PersistStage) ID() StageID { return StagePersist }

// Run writes labeled vectors to the default namespace and waits for search visibility when configured.
func (s *PersistStage) Run(ctx context.Context, ic *IngestContext) error {
	ic.IndexObservation = domain.IndexObservation{}
	// Labels are supplied by callers and never inferred from document content.
	docType := ic.DocType
	if docType == "" {
		docType = domain.DefaultDocType
	}
	if ic.Metadata == nil {
		ic.Metadata = make(map[string]any)
	}
	ic.Metadata["doc_type"] = docType

	// Inject doc_type into each chunk's metadata for Pinecone.
	for i := range ic.Chunks {
		if ic.Chunks[i].Metadata == nil {
			ic.Chunks[i].Metadata = make(map[string]any)
		}
		ic.Chunks[i].Metadata["doc_type"] = docType
		if ic.VectorRevision != "" {
			ic.Chunks[i].Metadata[domain.VectorRevisionKey] = ic.VectorRevision
			ic.Chunks[i].Metadata[domain.VectorAttemptKey] = ic.VectorAttempt
		}
		if ic.Collection != "" {
			ic.Chunks[i].Metadata["collection"] = ic.Collection
		}
		if ic.Tenant != "" {
			ic.Chunks[i].Metadata[domain.VectorTenantKey] = ic.Tenant
		}
	}

	if _, transactional := s.vectorStore.(domain.TransactionalVectorStore); transactional {
		return ctx.Err()
	}

	if ic.RevisionID != "" {
		for i := range ic.Chunks {
			ic.Chunks[i].RevisionID = ic.RevisionID
			ic.Chunks[i].VectorID = domain.FormatRevisionVectorID(ic.DocumentID, ic.RevisionID, ic.Chunks[i].Index)
		}
		if ic.PersistManifest != nil {
			if err := ic.PersistManifest(ctx, ic.Chunks); err != nil {
				return fmt.Errorf("persist_stage: manifest: %w", err)
			}
		}
	}

	// Upsert vectors to the default namespace with doc_id metadata.
	var probe domain.VisibilityProbe
	upsert := func(ctx context.Context) error {
		incarnation := domain.VectorIncarnation(ctx)
		for i := range ic.Chunks {
			if len(ic.Chunks[i].Embedding) != 0 {
				ic.Chunks[i].VectorIncarnation = incarnation
			}
		}
		call := func(ctx context.Context) error {
			if writer, ok := s.vectorStore.(domain.VisibilityWriter); s.lsnVisibility && s.visibilityTimeout > 0 && ok {
				var err error
				probe, err = writer.UpsertWithVisibility(ctx, "", ic.Chunks)
				return err
			}
			return s.vectorStore.Upsert(ctx, "", ic.Chunks)
		}
		if store, ok := s.vectorStore.(domain.VectorDispatchStore); ok && store.ManagesVectorDispatch() {
			return call(ctx)
		}
		return domain.DispatchVectorMutation(ctx, false, call)
	}
	var err error
	switch {
	case ic.MutateVectors != nil:
		err = ic.MutateVectors(ctx, upsert)
	case ic.VectorRevision != "":
		return errors.New("persist_stage: job vector guard is unavailable")
	default:
		err = upsert(ctx)
	}
	if err != nil {
		return fmt.Errorf("persist_stage: upsert to default namespace: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	if tracing.RedactIdentifiers(ctx) || ic.Tenant != "" {
		slog.DebugContext(ctx, "document vectors persisted", "doc_type", docType, "chunks", len(ic.Chunks))
	} else {
		slog.DebugContext(ctx, "document vectors persisted", "document_id", ic.DocumentID, "doc_type", docType, "chunks", len(ic.Chunks))
	}
	if s.visibilityTimeout <= 0 {
		ic.observeIndex(domain.VisibilityDisabled)
		return nil
	}
	var target domain.Chunk
	embedded := 0
	for _, chunk := range ic.Chunks {
		if len(chunk.Embedding) > 0 {
			target = chunk
			embedded++
		}
	}
	if !s.lsnVisibility {
		probe = s.sampleProbe(ic, target, min(embedded, 10))
	}
	if embedded == 0 || probe == nil {
		ic.observeIndex(domain.VisibilityUnknown)
		return nil
	}
	return s.waitForVisibility(ctx, ic, target.Embedding, probe)
}

func (ic *IngestContext) observeIndex(result domain.IndexVisibility) {
	now := time.Now().UTC()
	ic.IndexObservation = domain.IndexObservation{Visibility: result, ObservedAt: &now}
}

func (s *PersistStage) sampleProbe(ic *IngestContext, target domain.Chunk, topK int) domain.VisibilityProbe {
	store, ok := s.vectorStore.(domain.FilteredVectorStore)
	if !ok {
		return nil
	}
	filter := domain.VectorFilter{DocumentIDs: []string{string(ic.DocumentID)}}
	return func(ctx context.Context, embedding []float32) (domain.IndexVisibility, error) {
		chunks, err := store.QueryFiltered(ctx, embedding, topK, filter)
		if err != nil {
			return "", err
		}
		for _, chunk := range chunks {
			revision, _ := chunk.Chunk.Metadata[domain.VectorRevisionKey].(string)
			attempt, _ := chunk.Chunk.Metadata[domain.VectorAttemptKey].(string)
			if chunk.Chunk.ID == target.ID &&
				revision == ic.VectorRevision &&
				attempt == ic.VectorAttempt &&
				chunk.Chunk.VectorIncarnation == target.VectorIncarnation &&
				(target.RevisionID == "" || chunk.Chunk.RevisionID == target.RevisionID) {
				return domain.VisibilitySampleSeen, nil
			}
		}
		return "", nil
	}
}

func (s *PersistStage) waitForVisibility(ctx context.Context, ic *IngestContext, embedding []float32, probe domain.VisibilityProbe) error {
	start := time.Now()
	waitCtx, cancel := context.WithTimeout(ctx, s.visibilityTimeout)
	defer cancel()
	if stopping := ic.Stopping; stopping != nil {
		go func() {
			select {
			case <-stopping:
				cancel()
			case <-waitCtx.Done():
			}
		}()
	}
	backoff := 200 * time.Millisecond
	var probeErr error
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if waitCtx.Err() != nil || visibilityStopped(ic.Stopping) {
			break
		}
		result, err := probe(waitCtx, embedding)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if visibilityStopped(ic.Stopping) || (s.lsnVisibility && waitCtx.Err() != nil) {
			break
		}
		if err == nil && result != "" {
			ic.observeIndex(result)
			label := string(result)
			if result == domain.VisibilitySampleSeen {
				label = "visible"
			}
			metrics.FromContext(ctx).IndexVisibility.WithLabelValues(label).Inc()
			return nil
		}
		if waitCtx.Err() != nil {
			break
		}
		probeErr = err
		timer := time.NewTimer(backoff)
		select {
		case <-waitCtx.Done():
			timer.Stop()
		case <-timer.C:
		}
		backoff = min(2*backoff, 2*time.Second)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	ic.observeIndex(domain.VisibilityTimeout)
	metrics.FromContext(ctx).IndexVisibility.WithLabelValues("timeout").Inc()
	var attrs []any
	if !tracing.RedactIdentifiers(ctx) && ic.Tenant == "" {
		attrs = append(attrs, "document_id", ic.DocumentID)
	}
	attrs = append(attrs, "waited_ms", time.Since(start).Milliseconds())
	if probeErr != nil {
		attrs = append(attrs, "error", tracing.ErrorForTelemetry(ctx, probeErr))
	}
	slog.WarnContext(ctx, "document vectors not yet searchable", attrs...)
	return nil
}

func visibilityStopped(stopping <-chan struct{}) bool {
	select {
	case <-stopping:
		return true
	default:
		return false
	}
}
