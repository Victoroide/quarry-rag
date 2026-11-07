package tracing

import (
	"context"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type vectorStore struct {
	domain.VectorStore
	deleter  domain.VectorDeleter
	filtered domain.FilteredVectorStore
	system   string
}

// WrapVectorStore traces complete stores while preserving scoped search and deletion.
func WrapVectorStore(inner domain.VectorStore, system string) domain.VectorStore {
	deleter, canDelete := inner.(domain.VectorDeleter)
	filtered, canFilter := inner.(domain.FilteredVectorStore)
	if !canDelete || !canFilter {
		return inner
	}
	return &vectorStore{inner, deleter, filtered, system}
}

func (s *vectorStore) begin(ctx context.Context, operation, kind string) (context.Context, trace.Span) {
	return start(ctx, "vector."+operation, kind, trace.SpanKindClient, attribute.String("db.system.name", s.system), attribute.String("db.operation.name", operation))
}

func (s *vectorStore) Upsert(ctx context.Context, namespace string, chunks []domain.Chunk) error {
	ctx, span := s.begin(ctx, "upsert", "tool")
	defer span.End()
	span.SetAttributes(attribute.Int("quarry.vector.records", len(chunks)))
	err := s.VectorStore.Upsert(ctx, namespace, chunks)
	RecordError(span, err)
	return err
}

func (s *vectorStore) query(ctx context.Context, topK int, call func(context.Context) ([]domain.ScoredChunk, error)) ([]domain.ScoredChunk, error) {
	ctx, span := s.begin(ctx, "query", "retriever")
	defer span.End()
	results, err := call(ctx)
	span.SetAttributes(attribute.Int("quarry.vector.top_k", topK), attribute.Int("quarry.vector.results", len(results)))
	RecordError(span, err)
	return results, err
}

func (s *vectorStore) Query(ctx context.Context, namespace string, embedding []float32, topK int, docType string) ([]domain.ScoredChunk, error) {
	return s.query(ctx, topK, func(ctx context.Context) ([]domain.ScoredChunk, error) {
		return s.VectorStore.Query(ctx, namespace, embedding, topK, docType)
	})
}

func (s *vectorStore) QueryAllNamespaces(ctx context.Context, embedding []float32, topK int, docType string) ([]domain.ScoredChunk, error) {
	return s.query(ctx, topK, func(ctx context.Context) ([]domain.ScoredChunk, error) {
		return s.VectorStore.QueryAllNamespaces(ctx, embedding, topK, docType)
	})
}

func (s *vectorStore) QueryFiltered(ctx context.Context, embedding []float32, topK int, filter domain.VectorFilter) ([]domain.ScoredChunk, error) {
	return s.query(ctx, topK, func(ctx context.Context) ([]domain.ScoredChunk, error) {
		return s.filtered.QueryFiltered(ctx, embedding, topK, filter)
	})
}

func (s *vectorStore) DeleteByDocID(ctx context.Context, docID string) error {
	ctx, span := s.begin(ctx, "delete", "tool")
	defer span.End()
	err := s.deleter.DeleteByDocID(ctx, docID)
	RecordError(span, err)
	return err
}

func (s *vectorStore) DeleteAllVectorsInNamespace(ctx context.Context, namespace string) error {
	ctx, span := s.begin(ctx, "delete", "tool")
	defer span.End()
	err := s.deleter.DeleteAllVectorsInNamespace(ctx, namespace)
	RecordError(span, err)
	return err
}
