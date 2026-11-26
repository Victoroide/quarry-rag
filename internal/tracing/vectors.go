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
	var base domain.VectorStore
	switch {
	case canRefill && canWrite && canDeleteRevision:
		base = &hostedRevisionVectorStore{&hostedVisibilityStore{&visibilityRefillStore{wrapped, refill}, writer}, revisions}
	case canRefill && canWrite:
		base = &hostedVisibilityStore{&visibilityRefillStore{wrapped, refill}, writer}
	case canRefill && canDeleteRevision:
		base = &revisionRefillVectorStore{&visibilityRefillStore{wrapped, refill}, revisions}
	case canRefill:
		base = &visibilityRefillStore{wrapped, refill}
	case canWrite && canDeleteRevision:
		base = &writableRevisionVectorStore{&visibilityWriterStore{wrapped, writer}, revisions}
	case canWrite:
		base = &visibilityWriterStore{wrapped, writer}
	case canDeleteRevision:
		base = &revisionVectorStore{wrapped, revisions}
	default:
		base = wrapped
	}
	manifestDeleter, canManifestDelete := inner.(domain.ManifestRevisionVectorDeleter)
	checker, canCheck := inner.(domain.VectorPresenceChecker)
	if !canManifestDelete && !canCheck {
		if rev, ok := base.(*revisionVectorStore); ok {
			if memory, isMem := inner.(domain.InMemoryIndex); isMem {
				return &memoryVectorStore{rev, memory}
			}
		}
		return base
	}
	mt := manifestTracer{wrapped, manifestDeleter, checker}
	switch v := base.(type) {
	case *hostedRevisionVectorStore:
		return &hostedRevisionManifestStore{v, mt}
	case *revisionRefillVectorStore:
		return &refillRevisionManifestStore{v, mt}
	case *writableRevisionVectorStore:
		return &writableRevisionManifestStore{v, mt}
	case *revisionVectorStore:
		rm := &revisionManifestStore{v, mt}
		if memory, isMem := inner.(domain.InMemoryIndex); isMem {
			return &memoryManifestVectorStore{rm, memory}
		}
		return rm
	case *hostedVisibilityStore:
		return &hostedManifestStore{v, mt}
	case *visibilityRefillStore:
		return &refillManifestStore{v, mt}
	case *visibilityWriterStore:
		return &writerManifestStore{v, mt}
	default:
		return &manifestVectorStore{wrapped, mt}
	}
}

type memoryManifestVectorStore struct {
	*revisionManifestStore
	domain.InMemoryIndex
}

type manifestTracer struct {
	vs       *vectorStore
	manifest domain.ManifestRevisionVectorDeleter
	checker  domain.VectorPresenceChecker
}

func (m manifestTracer) DeleteByManifestRevision(ctx context.Context, docID, revisionID string, knownIDs []string) error {
	ctx, span := m.vs.begin(ctx, "delete", "tool")
	defer span.End()
	if m.manifest == nil {
		return errors.New("manifest revision vector cleanup is unavailable")
	}
	err := m.manifest.DeleteByManifestRevision(ctx, docID, revisionID, knownIDs)
	RecordError(span, err)
	return err
}

func (m manifestTracer) DeleteVectorIDs(ctx context.Context, namespace string, ids []string) error {
	ctx, span := m.vs.begin(ctx, "delete", "tool")
	defer span.End()
	span.SetAttributes(attribute.Int("quarry.vector.records", len(ids)))
	if m.checker == nil {
		return errors.New("vector presence delete is unavailable")
	}
	err := m.checker.DeleteVectorIDs(ctx, namespace, ids)
	RecordError(span, err)
	return err
}

func (m manifestTracer) FetchPresentVectorIDs(ctx context.Context, namespace string, ids []string) ([]string, error) {
	ctx, span := m.vs.begin(ctx, "fetch", "tool")
	defer span.End()
	if m.checker == nil {
		return nil, errors.New("vector presence fetch is unavailable")
	}
	present, err := m.checker.FetchPresentVectorIDs(ctx, namespace, ids)
	RecordError(span, err)
	return present, err
}

func (m manifestTracer) ListPrefixVectorIDs(ctx context.Context, namespace, prefix string, limit int, token string) (ids []string, nextToken string, supported bool, err error) {
	ctx, span := m.vs.begin(ctx, "list", "tool")
	defer span.End()
	if m.checker == nil {
		return nil, "", false, errors.New("vector presence list is unavailable")
	}
	ids, nextToken, supported, err = m.checker.ListPrefixVectorIDs(ctx, namespace, prefix, limit, token)
	RecordError(span, err)
	return ids, nextToken, supported, err
}

type manifestVectorStore struct {
	*vectorStore
	manifestTracer
}

type refillManifestStore struct {
	*visibilityRefillStore
	manifestTracer
}

type writerManifestStore struct {
	*visibilityWriterStore
	manifestTracer
}

type hostedManifestStore struct {
	*hostedVisibilityStore
	manifestTracer
}

type revisionManifestStore struct {
	*revisionVectorStore
	manifestTracer
}

type refillRevisionManifestStore struct {
	*revisionRefillVectorStore
	manifestTracer
}

type writableRevisionManifestStore struct {
	*writableRevisionVectorStore
	manifestTracer
}

type hostedRevisionManifestStore struct {
	*hostedRevisionVectorStore
	manifestTracer
}

// memoryVectorStore keeps the owner identity and deletion slots of a
// process-local index visible through tracing.
type memoryVectorStore struct {
	*revisionVectorStore
	domain.InMemoryIndex
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

func (s *vectorStore) ObservesVectorIncarnation() bool {
	reader, ok := s.VectorStore.(domain.VectorIncarnationReader)
	return ok && reader.ObservesVectorIncarnation()
}

// GrantScope reports how the wrapped store enforces an authorized read
// scope. A store without that enforcement reports the empty kind.
func (s *vectorStore) GrantScope() domain.GrantScopeKind {
	if scoped, ok := s.VectorStore.(domain.GrantScopedVectorStore); ok {
		return scoped.GrantScope()
	}
	return ""
}

func (s *vectorStore) QueryFiltered(ctx context.Context, embedding []float32, topK int, filter domain.VectorFilter) ([]domain.ScoredChunk, error) {
	return s.query(ctx, topK, func(ctx context.Context) ([]domain.ScoredChunk, error) {
		return s.filtered.QueryFiltered(ctx, embedding, topK, filter)
	})
}

func (s *transactionalVectorStore) QueryFilteredWithExactness(ctx context.Context, embedding []float32, topK int, filter domain.VectorFilter) ([]domain.ScoredChunk, bool, error) {
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
