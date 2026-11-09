package pipeline

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/Victoroide/quarry-rag/internal/chunker"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/metrics"
	"golang.org/x/sync/errgroup"
)

// ErrNoChunks indicates that document text produced no indexable chunks.
var ErrNoChunks = errors.New("no chunks produced")

// ChunkStage splits a raw document into chunks using the sliding window chunker.
type ChunkStage struct {
	chunker *chunker.Chunker
}

// NewChunkStage creates a ChunkStage.
func NewChunkStage(c *chunker.Chunker) *ChunkStage {
	return &ChunkStage{chunker: c}
}

// ID returns the stage identifier.
func (s *ChunkStage) ID() StageID { return StageChunk }

// Run splits the raw text into chunks.
func (s *ChunkStage) Run(ctx context.Context, ic *IngestContext) error {
	ic.Chunks = s.chunker.Chunk(ic.DocumentID, ic.RawText)
	if len(ic.Chunks) == 0 {
		return fmt.Errorf("chunk_stage: document %s: %w", ic.DocumentID, ErrNoChunks)
	}
	slog.DebugContext(ctx, "document chunked", "document_id", ic.DocumentID, "chunks", len(ic.Chunks))
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
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(s.concurrency)
	for i := range ic.Chunks {
		group.Go(func() error {
			emb, err := s.embedder.Embed(groupCtx, ic.Chunks[i].Text)
			if err != nil {
				return fmt.Errorf("embed_stage: chunk %s: %w", ic.Chunks[i].ID, err)
			}
			ic.Chunks[i].Embedding = emb
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return err
	}
	slog.DebugContext(ctx, "document chunks embedded", "document_id", ic.DocumentID, "chunks", len(ic.Chunks))
	return nil
}

// PersistStage upserts chunks to the default vector namespace and can wait until search returns them.
type PersistStage struct {
	vectorStore       domain.VectorStore
	visibilityTimeout time.Duration
}

// PersistOption configures vector persistence.
type PersistOption func(*PersistStage)

// WithVisibilityWait bounds the wait for search visibility; zero disables it.
func WithVisibilityWait(timeout time.Duration) PersistOption {
	return func(s *PersistStage) { s.visibilityTimeout = timeout }
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
		if ic.Collection != "" {
			ic.Chunks[i].Metadata["collection"] = ic.Collection
		}
	}

	if _, transactional := s.vectorStore.(domain.TransactionalVectorStore); transactional {
		return ctx.Err()
	}

	// Upsert vectors to the default namespace with doc_id metadata.
	if err := s.vectorStore.Upsert(ctx, "", ic.Chunks); err != nil {
		return fmt.Errorf("persist_stage: upsert to default namespace: %w", err)
	}

	slog.DebugContext(ctx, "document vectors persisted", "document_id", ic.DocumentID, "doc_type", docType, "chunks", len(ic.Chunks))
	return s.waitForVisibility(ctx, ic)
}

func (s *PersistStage) waitForVisibility(ctx context.Context, ic *IngestContext) error {
	store, ok := s.vectorStore.(domain.FilteredVectorStore)
	if s.visibilityTimeout <= 0 || !ok {
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
	if embedded == 0 {
		return nil
	}

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
	filter := domain.VectorFilter{DocumentIDs: []string{string(ic.DocumentID)}}
	backoff := 200 * time.Millisecond
	var probeErr error
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if waitCtx.Err() != nil {
			break
		}
		chunks, err := store.QueryFiltered(waitCtx, target.Embedding, min(embedded, 10), filter)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err == nil {
			for _, chunk := range chunks {
				if chunk.Chunk.ID == target.ID {
					metrics.FromContext(ctx).IndexVisibility.WithLabelValues("visible").Inc()
					return nil
				}
			}
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
	metrics.FromContext(ctx).IndexVisibility.WithLabelValues("timeout").Inc()
	attrs := []any{"document_id", ic.DocumentID, "waited_ms", time.Since(start).Milliseconds()}
	if probeErr != nil {
		attrs = append(attrs, "error", probeErr)
	}
	slog.WarnContext(ctx, "document vectors not yet searchable", attrs...)
	return nil
}
