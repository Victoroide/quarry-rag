package tracing

import (
	"context"
	"errors"

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
	wrapped := &vectorStore{inner, deleter, filtered, system}
	if _, transactional := inner.(domain.TransactionalVectorStore); transactional {
		return &transactionalVectorStore{wrapped}
	}
	scoped, canDeleteRevision := inner.(domain.RevisionVectorDeleter)
	revisions := revisionDeletes{wrapped, scoped}
	refill, canRefill := inner.(domain.VisibilityRefillStore)
	writer, canWrite := inner.(domain.VisibilityWriter)
	if canRefill {
		visible := &visibilityRefillStore{wrapped, refill}
		if canWrite {
			hosted := &hostedVisibilityStore{visible, writer}
			if canDeleteRevision {
				return &hostedRevisionVectorStore{hosted, revisions}
			}
			return hosted
		}
		if canDeleteRevision {
			return &revisionRefillVectorStore{visible, revisions}
		}
		return visible
	}
	if canWrite {
		writable := &visibilityWriterStore{wrapped, writer}
		if canDeleteRevision {
			return &writableRevisionVectorStore{writable, revisions}
		}
		return writable
	}
	if canDeleteRevision {
		return &revisionVectorStore{wrapped, revisions}
	}
	return wrapped
}

type revisionVectorStore struct {
	*vectorStore
	revisionDeletes
}

type revisionRefillVectorStore struct {
	*visibilityRefillStore
	revisionDeletes
}

type hostedRevisionVectorStore struct {
	*hostedVisibilityStore
	revisionDeletes
}

type writableRevisionVectorStore struct {
	*visibilityWriterStore
	revisionDeletes
}

type revisionDeletes struct {
	store     *vectorStore
	revisions domain.RevisionVectorDeleter
}

func (s revisionDeletes) DeleteByRevision(ctx context.Context, docID, revision string) error {
	ctx, span := s.store.begin(ctx, "delete", "tool")
	defer span.End()
	err := s.revisions.DeleteByRevision(ctx, docID, revision)
	RecordError(span, err)
	return err
}

type transactionalVectorStore struct{ *vectorStore }

func (*transactionalVectorStore) TransactionalVectors() {}

type visibilityRefillStore struct {
	*vectorStore
	refill domain.VisibilityRefillStore
}

func (s *visibilityRefillStore) VisibilityRefillLimit() int { return s.refill.VisibilityRefillLimit() }

func (s *visibilityRefillStore) QueryFilteredBudgeted(ctx context.Context, embedding []float32, topK int, filter domain.VectorFilter, budget int) ([]domain.ScoredChunk, int, error) {
	var used int
	chunks, err := s.query(ctx, topK, func(ctx context.Context) ([]domain.ScoredChunk, error) {
		chunks, attempts, err := s.refill.QueryFilteredBudgeted(ctx, embedding, topK, filter, budget)
		used = attempts
		return chunks, err
	})
	return chunks, used, err
}

// visibilityWriterStore keeps the write receipts of a store that does not refill.
type visibilityWriterStore struct {
	*vectorStore
	writer domain.VisibilityWriter
}

func (s *visibilityWriterStore) UpsertWithVisibility(ctx context.Context, namespace string, chunks []domain.Chunk) (domain.VisibilityProbe, error) {
	return s.upsertWithVisibility(ctx, s.writer, namespace, chunks)
}

// hostedVisibilityStore keeps both optional hosted capabilities, which are
// enabled independently: refill at query time and receipts at ingestion.
type hostedVisibilityStore struct {
	*visibilityRefillStore
	writer domain.VisibilityWriter
}

func (s *hostedVisibilityStore) UpsertWithVisibility(ctx context.Context, namespace string, chunks []domain.Chunk) (domain.VisibilityProbe, error) {
	return s.upsertWithVisibility(ctx, s.writer, namespace, chunks)
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

func (s *vectorStore) upsertWithVisibility(ctx context.Context, writer domain.VisibilityWriter, namespace string, chunks []domain.Chunk) (domain.VisibilityProbe, error) {
	ctx, span := s.begin(ctx, "upsert", "tool")
	defer span.End()
	span.SetAttributes(attribute.Int("quarry.vector.records", len(chunks)))
	probe, err := writer.UpsertWithVisibility(ctx, namespace, chunks)
	RecordError(span, err)
	return probe, err
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

func (s *vectorStore) QueryFilteredWithExactness(ctx context.Context, embedding []float32, topK int, filter domain.VectorFilter) ([]domain.ScoredChunk, bool, error) {
	store, ok := s.filtered.(domain.FilteredVectorStoreWithExactness)
	if !ok {
		results, err := s.QueryFiltered(ctx, embedding, topK, filter)
		return results, false, err
	}
	var exact bool
	results, err := s.query(ctx, topK, func(ctx context.Context) ([]domain.ScoredChunk, error) {
		chunks, usedExact, queryErr := store.QueryFilteredWithExactness(ctx, embedding, topK, filter)
		exact = usedExact
		return chunks, queryErr
	})
	return results, exact, err
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

func (s *vectorStore) QueryCandidates(ctx context.Context, embedding []float32, topK int, filter domain.VectorFilter) ([]domain.ScoredChunk, error) {
	if candidates, ok := s.VectorStore.(domain.VectorCandidateStore); ok {
		return s.query(ctx, topK, func(ctx context.Context) ([]domain.ScoredChunk, error) {
			return candidates.QueryCandidates(ctx, embedding, topK, filter)
		})
	}
	return s.QueryFiltered(ctx, embedding, topK, filter)
}

func (s *vectorStore) ManagesVectorDispatch() bool {
	store, ok := s.VectorStore.(domain.VectorDispatchStore)
	return ok && store.ManagesVectorDispatch()
}

func (s revisionDeletes) DeleteByAttempt(ctx context.Context, docID, revision, attempt string) error {
	ctx, span := s.store.begin(ctx, "delete", "tool")
	defer span.End()
	deleter, ok := s.revisions.(domain.AttemptVectorDeleter)
	if !ok {
		return errors.New("attempt vector cleanup is unavailable")
	}
	err := deleter.DeleteByAttempt(ctx, docID, revision, attempt)
	RecordError(span, err)
	return err
}
