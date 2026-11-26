package tracing_test

import (
	"context"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/retriever"
	"github.com/Victoroide/quarry-rag/internal/store/memory"
	"github.com/Victoroide/quarry-rag/internal/store/pinecone"
	"github.com/Victoroide/quarry-rag/internal/store/postgres"
	"github.com/Victoroide/quarry-rag/internal/tracing"
	"github.com/stretchr/testify/require"
)

type variantBaseStore struct {
	*memory.FakeVectorStore
	sawAuthorized *domain.AuthorizedScope
	exactCalls    int
	refillCalls   int
	budgetCalls   int
}

func (s *variantBaseStore) QueryFiltered(ctx context.Context, embedding []float32, topK int, filter domain.VectorFilter) ([]domain.ScoredChunk, error) {
	s.sawAuthorized = filter.Authorized
	return s.FakeVectorStore.QueryFiltered(ctx, embedding, topK, filter)
}

type variantTxStore struct{ *variantBaseStore }

func (*variantTxStore) TransactionalVectors() {}
func (s *variantTxStore) QueryFilteredWithExactness(ctx context.Context, embedding []float32, topK int, filter domain.VectorFilter) ([]domain.ScoredChunk, bool, error) {
	s.exactCalls++
	s.sawAuthorized = filter.Authorized
	chunks, err := s.FakeVectorStore.QueryFiltered(ctx, embedding, topK, filter)
	return chunks, true, err
}

type variantIncarnationTxStore struct{ *variantTxStore }

func (*variantIncarnationTxStore) ObservesVectorIncarnation() bool { return true }

type variantRefillStore struct{ *variantBaseStore }

func (*variantRefillStore) VisibilityRefillLimit() int { return 10 }
func (s *variantRefillStore) QueryCandidates(ctx context.Context, embedding []float32, topK int, filter domain.VectorFilter) ([]domain.ScoredChunk, error) {
	s.refillCalls++
	s.sawAuthorized = filter.Authorized
	return s.FakeVectorStore.QueryFiltered(ctx, embedding, topK, filter)
}
func (s *variantRefillStore) QueryFilteredBudgeted(ctx context.Context, embedding []float32, topK int, filter domain.VectorFilter, _ int) ([]domain.ScoredChunk, int, error) {
	s.budgetCalls++
	s.sawAuthorized = filter.Authorized
	chunks, err := s.FakeVectorStore.QueryFiltered(ctx, embedding, topK, filter)
	return chunks, 1, err
}

type variantWriterStore struct{ *variantBaseStore }

func (*variantWriterStore) UpsertWithVisibility(context.Context, string, []domain.Chunk) (domain.VisibilityProbe, error) {
	return nil, nil
}

type variantHostedStore struct{ *variantRefillStore }

func (*variantHostedStore) UpsertWithVisibility(context.Context, string, []domain.Chunk) (domain.VisibilityProbe, error) {
	return nil, nil
}

type variantPlainStore struct {
	sawAuthorized *domain.AuthorizedScope
}

func (*variantPlainStore) Upsert(context.Context, string, []domain.Chunk) error { return nil }
func (*variantPlainStore) Query(context.Context, string, []float32, int, string) ([]domain.ScoredChunk, error) {
	return nil, nil
}
func (*variantPlainStore) QueryAllNamespaces(context.Context, []float32, int, string) ([]domain.ScoredChunk, error) {
	return nil, nil
}
func (*variantPlainStore) Ping(context.Context) error                                { return nil }
func (*variantPlainStore) DeleteByDocID(context.Context, string) error               { return nil }
func (*variantPlainStore) DeleteAllVectorsInNamespace(context.Context, string) error { return nil }
func (s *variantPlainStore) QueryFiltered(_ context.Context, _ []float32, _ int, filter domain.VectorFilter) ([]domain.ScoredChunk, error) {
	s.sawAuthorized = filter.Authorized
	return nil, nil
}

type variantNoRevisionRefillStore struct{ *variantPlainStore }

func (*variantNoRevisionRefillStore) VisibilityRefillLimit() int { return 10 }
func (s *variantNoRevisionRefillStore) QueryCandidates(_ context.Context, _ []float32, _ int, filter domain.VectorFilter) ([]domain.ScoredChunk, error) {
	s.sawAuthorized = filter.Authorized
	return nil, nil
}
func (s *variantNoRevisionRefillStore) QueryFilteredBudgeted(_ context.Context, _ []float32, _ int, filter domain.VectorFilter, _ int) ([]domain.ScoredChunk, int, error) {
	s.sawAuthorized = filter.Authorized
	return nil, 1, nil
}

type variantNoRevisionWriterStore struct{ *variantPlainStore }

func (*variantNoRevisionWriterStore) UpsertWithVisibility(context.Context, string, []domain.Chunk) (domain.VisibilityProbe, error) {
	return nil, nil
}

type variantNoRevisionHostedStore struct{ *variantNoRevisionRefillStore }

func (*variantNoRevisionHostedStore) UpsertWithVisibility(context.Context, string, []domain.Chunk) (domain.VisibilityProbe, error) {
	return nil, nil
}

type declaredKind domain.GrantScopeKind

func (k declaredKind) GrantScope() domain.GrantScopeKind { return domain.GrantScopeKind(k) }

func withDeclaredGrantScope(variant string, kind domain.GrantScopeKind, declare bool) (store domain.VectorStore, lastScope func() *domain.AuthorizedScope) {
	switch variant {
	case "plain":
		s := &variantPlainStore{}
		if declare {
			return struct {
				*variantPlainStore
				declaredKind
			}{s, declaredKind(kind)}, func() *domain.AuthorizedScope { return s.sawAuthorized }
		}
		return s, func() *domain.AuthorizedScope { return s.sawAuthorized }
	case "refill":
		s := &variantNoRevisionRefillStore{&variantPlainStore{}}
		if declare {
			return struct {
				*variantNoRevisionRefillStore
				declaredKind
			}{s, declaredKind(kind)}, func() *domain.AuthorizedScope { return s.sawAuthorized }
		}
		return s, func() *domain.AuthorizedScope { return s.sawAuthorized }
	case "writer":
		s := &variantNoRevisionWriterStore{&variantPlainStore{}}
		if declare {
			return struct {
				*variantNoRevisionWriterStore
				declaredKind
			}{s, declaredKind(kind)}, func() *domain.AuthorizedScope { return s.sawAuthorized }
		}
		return s, func() *domain.AuthorizedScope { return s.sawAuthorized }
	case "hosted":
		s := &variantNoRevisionHostedStore{&variantNoRevisionRefillStore{&variantPlainStore{}}}
		if declare {
			return struct {
				*variantNoRevisionHostedStore
				declaredKind
			}{s, declaredKind(kind)}, func() *domain.AuthorizedScope { return s.sawAuthorized }
		}
		return s, func() *domain.AuthorizedScope { return s.sawAuthorized }
	case "revision":
		s := &variantBaseStore{FakeVectorStore: memory.NewFakeVectorStore()}
		if declare {
			return struct {
				*variantBaseStore
				declaredKind
			}{s, declaredKind(kind)}, func() *domain.AuthorizedScope { return s.sawAuthorized }
		}
		return s, func() *domain.AuthorizedScope { return s.sawAuthorized }
	case "revision-refill":
		s := &variantRefillStore{&variantBaseStore{FakeVectorStore: memory.NewFakeVectorStore()}}
		if declare {
			return struct {
				*variantRefillStore
				declaredKind
			}{s, declaredKind(kind)}, func() *domain.AuthorizedScope { return s.sawAuthorized }
		}
		return s, func() *domain.AuthorizedScope { return s.sawAuthorized }
	case "revision-writer":
		s := &variantWriterStore{&variantBaseStore{FakeVectorStore: memory.NewFakeVectorStore()}}
		if declare {
			return struct {
				*variantWriterStore
				declaredKind
			}{s, declaredKind(kind)}, func() *domain.AuthorizedScope { return s.sawAuthorized }
		}
		return s, func() *domain.AuthorizedScope { return s.sawAuthorized }
	case "hosted-revision":
		s := &variantHostedStore{&variantRefillStore{&variantBaseStore{FakeVectorStore: memory.NewFakeVectorStore()}}}
		if declare {
			return struct {
				*variantHostedStore
				declaredKind
			}{s, declaredKind(kind)}, func() *domain.AuthorizedScope { return s.sawAuthorized }
		}
		return s, func() *domain.AuthorizedScope { return s.sawAuthorized }
	default:
		s := &variantIncarnationTxStore{&variantTxStore{&variantBaseStore{FakeVectorStore: memory.NewFakeVectorStore()}}}
		if declare {
			return struct {
				*variantIncarnationTxStore
				declaredKind
			}{s, declaredKind(kind)}, func() *domain.AuthorizedScope { return s.sawAuthorized }
		}
		return s, func() *domain.AuthorizedScope { return s.sawAuthorized }
	}
}

type staticEmbedder struct{}

func (staticEmbedder) Embed(context.Context, string) ([]float32, error) {
	return []float32{1, 0}, nil
}

// TestWrappedStoresPreserveGrantScope checks the concrete storage adapters
// and the in-memory double through tracing.WrapVectorStore and Retriever.
func TestWrappedStoresPreserveGrantScope(t *testing.T) {
	for _, tc := range []struct {
		name        string
		inner       domain.VectorStore
		system      string
		wantKind    domain.GrantScopeKind
		wantCapable bool
	}{
		{"postgres", &postgres.VectorStore{}, "postgresql", domain.GrantScopeRelation, true},
		{"pinecone", pinecone.NewDriver(nil), "pinecone", domain.GrantScopeMembership, true},
		{"memory", memory.NewFakeVectorStore(), "memory", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			wrapped := tracing.WrapVectorStore(tc.inner, tc.system)
			require.NotEqual(t, tc.inner, wrapped)
			ret := retriever.NewRetriever(wrapped, nil, 0.5, 10)
			kind, capable := ret.GrantScope()
			require.Equal(t, tc.wantCapable, capable)
			require.Equal(t, tc.wantKind, kind)
		})
	}
}

// TestWrappedVectorStoreVariantsPreserveAuthorizedCapabilities exercises every
// wrapper variant in WrapVectorStore so none drops GrantScope or Authorized.
func TestWrappedVectorStoreVariantsPreserveAuthorizedCapabilities(t *testing.T) {
	scope := &domain.AuthorizedScope{
		TenantID:     "00000000-0000-4000-8000-0000000000a0",
		PrincipalID:  "00000000-0000-4000-8000-0000000000a1",
		Incarnations: map[string]string{"00000000-0000-4000-8000-000000000001": "inc-1"},
	}
	filter := domain.VectorFilter{DocumentIDs: []string{"00000000-0000-4000-8000-000000000001"}, Authorized: scope}

	variants := []struct {
		name               string
		observes           bool
		supportsRefill     bool
		supportsExactCheck bool
	}{
		{"plain", false, false, false},
		{"refill", false, true, false},
		{"writer", false, false, false},
		{"hosted", false, true, false},
		{"revision", false, false, false},
		{"revision-refill", false, true, false},
		{"revision-writer", false, false, false},
		{"hosted-revision", false, true, false},
		{"transactional", true, false, true},
	}

	kinds := []struct {
		name        string
		kind        domain.GrantScopeKind
		declare     bool
		wantCapable bool
	}{
		{"unscoped", "", false, false},
		{"empty-kind", "", true, false},
		{"relation", domain.GrantScopeRelation, true, true},
		{"membership", domain.GrantScopeMembership, true, true},
	}

	for _, variant := range variants {
		for _, k := range kinds {
			t.Run(variant.name+"/"+k.name, func(t *testing.T) {
				inner, lastScope := withDeclaredGrantScope(variant.name, k.kind, k.declare)
				wrapped := tracing.WrapVectorStore(inner, "test")
				require.NotEqual(t, inner, wrapped)
				ret := retriever.NewRetriever(wrapped, staticEmbedder{}, 0.5, 10)

				gotKind, capable := ret.GrantScope()
				require.Equal(t, k.wantCapable, capable)
				if k.wantCapable {
					require.Equal(t, k.kind, gotKind)
				} else {
					require.Empty(t, gotKind)
				}
				require.Equal(t, variant.observes, ret.ObservesVectorIncarnation())
				require.Equal(t, variant.supportsRefill, ret.SupportsVisibilityRefill())

				_, err := ret.RetrieveWithEmbedding(t.Context(), []float32{1, 0}, retriever.RetrievalModeGlobal, "", 5, filter)
				if !k.wantCapable {
					require.ErrorIs(t, err, domain.ErrGrantScopeUnsupported)
					require.Nil(t, lastScope())
					return
				}
				require.NoError(t, err)
				require.Equal(t, scope, lastScope())

				_, _, _, err = ret.EmbedQuery(t.Context(), "test", filter, 5)
				require.NoError(t, err)
				require.Equal(t, scope, lastScope())

				if variant.supportsExactCheck {
					exactStore, ok := wrapped.(domain.FilteredVectorStoreWithExactness)
					require.True(t, ok)
					_, exact, exactErr := exactStore.QueryFilteredWithExactness(t.Context(), []float32{1, 0}, 5, filter)
					require.NoError(t, exactErr)
					require.True(t, exact)
					require.Equal(t, scope, lastScope())
				}
				if variant.supportsRefill {
					verify := func(_ context.Context, c []domain.ScoredChunk, _ domain.VectorFilter) ([]domain.ScoredChunk, bool, error) {
						return c, false, nil
					}
					_, refillErr := ret.RetrieveVisibleWithEmbedding(t.Context(), []float32{1, 0}, retriever.RetrievalModeGlobal, "", 5, filter, 2, verify)
					require.NoError(t, refillErr)
					require.Equal(t, scope, lastScope())
				}
			})
		}
	}
}
