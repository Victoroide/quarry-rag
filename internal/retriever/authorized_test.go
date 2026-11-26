package retriever

import (
	"context"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/stretchr/testify/require"
)

type scopeCounter struct {
	domain.VectorStore
	searches int
	global   int
}

func (s *scopeCounter) QueryAllNamespaces(context.Context, []float32, int, string) ([]domain.ScoredChunk, error) {
	s.global++
	return []domain.ScoredChunk{{Chunk: domain.Chunk{ID: "hidden", DocumentID: "hidden", Text: "hidden"}, Score: 1}}, nil
}

func (s *scopeCounter) QueryFiltered(context.Context, []float32, int, domain.VectorFilter) ([]domain.ScoredChunk, error) {
	s.searches++
	return nil, nil
}

// refillStore adds the hosted refill surface a grants search could also reach.
type refillStore struct{ *scopeCounter }

func (refillStore) VisibilityRefillLimit() int { return 100 }
func (s refillStore) QueryCandidates(context.Context, []float32, int, domain.VectorFilter) ([]domain.ScoredChunk, error) {
	s.searches++
	return nil, nil
}

func (s refillStore) QueryFilteredBudgeted(context.Context, []float32, int, domain.VectorFilter, int) ([]domain.ScoredChunk, int, error) {
	s.searches++
	return nil, 1, nil
}

// scopedRefillStore declares the capability that the plain refill store lacks.
type scopedRefillStore struct{ refillStore }

func (scopedRefillStore) GrantScope() domain.GrantScopeKind { return domain.GrantScopeMembership }

type countingEmbedder struct {
	domain.EmbeddingProvider
	calls *int
}

func (e countingEmbedder) Embed(context.Context, string) ([]float32, error) {
	*e.calls++
	return []float32{1}, nil
}

func authorizedFilter() domain.VectorFilter {
	return domain.VectorFilter{DocumentIDs: []string{"doc"}, Authorized: &domain.AuthorizedScope{TenantID: "tenant", PrincipalID: "principal"}}
}

func refuseAll(t *testing.T, calls map[string]func(context.Context) error) {
	t.Helper()
	for name, call := range calls {
		require.ErrorIs(t, call(t.Context()), domain.ErrGrantScopeUnsupported, name)
	}
}

func TestAuthorizedSearchRefusesAnUnscopedStoreBeforeAnyProviderCall(t *testing.T) {
	filter := authorizedFilter()
	counter := &scopeCounter{}
	embeds := 0
	ret := NewRetriever(counter, countingEmbedder{calls: &embeds}, .7, 10)
	refuseAll(t, map[string]func(context.Context) error{
		"Retrieve": func(ctx context.Context) error {
			_, err := ret.Retrieve(ctx, "q", RetrievalModeGlobal, 5, filter)
			return err
		},
		"RetrieveWithEmbedding": func(ctx context.Context) error {
			_, err := ret.RetrieveWithEmbedding(ctx, []float32{1}, RetrievalModeLocal, "doc", 5, filter)
			return err
		},
		"RetrieveWithVisibility": func(ctx context.Context) error {
			_, err := ret.RetrieveWithVisibility(ctx, []float32{1}, RetrievalModeGlobal, "", 5, filter,
				func(_ context.Context, c []domain.ScoredChunk) ([]domain.ScoredChunk, error) { return c, nil })
			return err
		},
		"RetrieveEvidenceLocal": func(ctx context.Context) error {
			_, err := ret.RetrieveEvidenceLocal(ctx, []float32{1}, "doc", nil, 5, filter)
			return err
		},
		"EmbedQuery": func(ctx context.Context) error { _, _, _, err := ret.EmbedQuery(ctx, "q", filter, 5); return err },
		"EmbedQueryWithVisibility": func(ctx context.Context) error {
			_, _, _, err := ret.EmbedQueryWithVisibility(ctx, "q", filter, 5)
			return err
		},
	})
	require.Zero(t, embeds, "the query is not embedded for a search that cannot be scoped")
	require.Zero(t, counter.searches)
	require.Zero(t, counter.global, "authorized searches never fall back to the unfiltered search")
}

func TestAuthorizedRefillSearchRefusesAStoreWithoutTheCapability(t *testing.T) {
	filter := authorizedFilter()
	counter := &scopeCounter{}
	embeds := 0
	ret := NewRetriever(refillStore{counter}, countingEmbedder{calls: &embeds}, .7, 10)
	verify := func(context.Context, []domain.ScoredChunk, domain.VectorFilter) ([]domain.ScoredChunk, bool, error) {
		return nil, false, nil
	}
	refuseAll(t, map[string]func(context.Context) error{
		"EmbedQueryForVisibilityRefill": func(ctx context.Context) error {
			_, _, _, err := ret.EmbedQueryForVisibilityRefill(ctx, "q", filter, 5)
			return err
		},
		"RetrieveVisibleWithEmbedding": func(ctx context.Context) error {
			_, err := ret.RetrieveVisibleWithEmbedding(ctx, []float32{1}, RetrievalModeGlobal, "", 5, filter, 2, verify)
			return err
		},
		"RetrieveVisibleFromProbe": func(ctx context.Context) error {
			_, err := ret.RetrieveVisibleFromProbe(ctx, []float32{1}, RoutedProbe{}, 5, filter, 2, verify)
			return err
		},
	})
	require.Zero(t, embeds)
	require.Zero(t, counter.searches)
}

func TestAuthorizedSearchNeverReachesTheUnfilteredFallback(t *testing.T) {
	// A store that declares the capability but only offers the unfiltered
	// search must not serve an authorized request through it.
	store := &capableUnfilteredStore{}
	ret := NewRetriever(store, nil, .7, 10)
	_, err := ret.RetrieveWithEmbedding(t.Context(), []float32{1}, RetrievalModeGlobal, "", 5, authorizedFilter())
	require.ErrorIs(t, err, domain.ErrGrantScopeUnsupported)
	require.Zero(t, store.global)
}

type capableUnfilteredStore struct {
	domain.VectorStore
	global int
}

func (*capableUnfilteredStore) GrantScope() domain.GrantScopeKind { return domain.GrantScopeRelation }

func (s *capableUnfilteredStore) QueryAllNamespaces(context.Context, []float32, int, string) ([]domain.ScoredChunk, error) {
	s.global++
	return nil, nil
}

func TestAuthorizedSearchUsesAScopeCapableStore(t *testing.T) {
	embeds := 0
	store := scopedRefillStore{refillStore{&scopeCounter{}}}
	ret := NewRetriever(store, countingEmbedder{calls: &embeds}, .7, 10)
	kind, ok := ret.GrantScope()
	require.True(t, ok)
	require.Equal(t, domain.GrantScopeMembership, kind)
	filter := authorizedFilter()
	_, err := ret.RetrieveWithEmbedding(t.Context(), []float32{1}, RetrievalModeGlobal, "", 5, filter)
	require.NoError(t, err)
	require.Equal(t, 1, store.searches)
	_, _, _, err = ret.EmbedQuery(t.Context(), "q", filter, 5)
	require.NoError(t, err)
	require.Equal(t, 1, embeds)
	_, ok = NewRetriever(&scopeCounter{}, nil, .7, 10).GrantScope()
	require.False(t, ok)
}

func TestUnscopedSearchKeepsItsFallbackBehavior(t *testing.T) {
	store := &capableUnfilteredStore{}
	ret := NewRetriever(store, nil, .7, 10)
	_, err := ret.RetrieveWithEmbedding(t.Context(), []float32{1}, RetrievalModeGlobal, "", 5, domain.VectorFilter{})
	require.NoError(t, err)
	require.Equal(t, 1, store.global, "service-mode searches are unchanged")
}

type declaredScopeStore struct {
	*scopeCounter
	kind domain.GrantScopeKind
}

func (s declaredScopeStore) GrantScope() domain.GrantScopeKind { return s.kind }

func TestAuthorizedSearchRefusesEmptyOrUnknownGrantScope(t *testing.T) {
	filter := authorizedFilter()
	for name, kind := range map[string]domain.GrantScopeKind{
		"empty":   "",
		"unknown": "custom",
	} {
		t.Run(name, func(t *testing.T) {
			counter := &scopeCounter{}
			embeds := 0
			ret := NewRetriever(declaredScopeStore{scopeCounter: counter, kind: kind}, countingEmbedder{calls: &embeds}, .7, 10)
			gotKind, ok := ret.GrantScope()
			require.False(t, ok)
			require.Empty(t, gotKind)
			refuseAll(t, map[string]func(context.Context) error{
				"Retrieve": func(ctx context.Context) error {
					_, err := ret.Retrieve(ctx, "q", RetrievalModeGlobal, 5, filter)
					return err
				},
				"RetrieveWithEmbedding": func(ctx context.Context) error {
					_, err := ret.RetrieveWithEmbedding(ctx, []float32{1}, RetrievalModeGlobal, "", 5, filter)
					return err
				},
				"EmbedQuery": func(ctx context.Context) error {
					_, _, _, err := ret.EmbedQuery(ctx, "q", filter, 5)
					return err
				},
			})
			require.Zero(t, embeds)
			require.Zero(t, counter.searches)
			require.Zero(t, counter.global)
		})
	}
}

// relationStore is a relation-scoped store that may or may not join a set.
type relationStore struct{ *scopeCounter }

func (relationStore) GrantScope() domain.GrantScopeKind { return domain.GrantScopeRelation }

type joiningRelationStore struct {
	relationStore
	joins bool
}

func (s joiningRelationStore) SupportsDocumentSets() bool { return s.joins }

func setFilter(scope domain.AuthorizedScope) domain.VectorFilter {
	scope.DocumentSet = &domain.DocumentSetRef{ID: "set"}
	return domain.VectorFilter{Authorized: &scope}
}

func TestSetSearchNeverRunsWhereTheStoreCannotConfineItToTheSet(t *testing.T) {
	scope := domain.AuthorizedScope{TenantID: "tenant", PrincipalID: "principal"}
	for name, store := range map[string]domain.VectorStore{
		"relation store with no set capability": relationStore{&scopeCounter{}},
		"relation store that reports it cannot": joiningRelationStore{relationStore: relationStore{&scopeCounter{}}, joins: false},
		"hosted store given no member map":      scopedRefillStore{refillStore{&scopeCounter{}}},
	} {
		t.Run(name, func(t *testing.T) {
			embeds := 0
			ret := NewRetriever(store, countingEmbedder{calls: &embeds}, .7, 10)
			filter := setFilter(scope)
			filter.DocumentIDs = []string{"doc"}
			refuseAll(t, map[string]func(context.Context) error{
				"Retrieve": func(ctx context.Context) error {
					_, err := ret.Retrieve(ctx, "q", RetrievalModeGlobal, 5, filter)
					return err
				},
				"EmbedQuery": func(ctx context.Context) error {
					_, _, _, err := ret.EmbedQuery(ctx, "q", filter, 5)
					return err
				},
			})
			require.Zero(t, embeds, "nothing is embedded for a search that cannot be confined")
		})
	}
}

func TestSetSearchRunsWhereTheStoreConfinesIt(t *testing.T) {
	scope := domain.AuthorizedScope{TenantID: "tenant", PrincipalID: "principal"}
	relation := joiningRelationStore{relationStore: relationStore{&scopeCounter{}}, joins: true}
	embeds := 0
	ret := NewRetriever(relation, countingEmbedder{calls: &embeds}, .7, 10)
	require.True(t, ret.SupportsDocumentSets())
	filter := setFilter(scope)
	_, err := ret.Retrieve(t.Context(), "q", RetrievalModeGlobal, 5, filter)
	require.NoError(t, err)
	require.Equal(t, 1, relation.searches)

	hostedScope := scope
	hostedScope.Incarnations = map[string]string{"doc": "incarnation"}
	hosted := scopedRefillStore{refillStore{&scopeCounter{}}}
	ret = NewRetriever(hosted, countingEmbedder{calls: &embeds}, .7, 10)
	require.True(t, ret.SupportsDocumentSets(), "a hosted store receives the intersection as its member list")
	filter = setFilter(hostedScope)
	filter.DocumentIDs = []string{"doc"}
	_, err = ret.Retrieve(t.Context(), "q", RetrievalModeGlobal, 5, filter)
	require.NoError(t, err)

	plain := NewRetriever(&scopeCounter{}, countingEmbedder{calls: &embeds}, .7, 10)
	require.False(t, plain.SupportsDocumentSets(), "a store with no scope capability never serves sets")
	_, priced := plain.SearchPartitions(5)
	require.False(t, priced)
}

func TestSetSearchOrdersEqualScoresByChunkID(t *testing.T) {
	store := &tiedStore{}
	ret := NewRetriever(store, countingEmbedder{calls: new(int)}, .7, 10)
	filter := setFilter(domain.AuthorizedScope{TenantID: "tenant", PrincipalID: "principal", Incarnations: map[string]string{"doc": "i"}})
	filter.DocumentIDs = []string{"doc"}
	result, err := ret.RetrieveWithVisibility(t.Context(), []float32{1}, RetrievalModeGlobal, "", 3, filter,
		func(_ context.Context, chunks []domain.ScoredChunk) ([]domain.ScoredChunk, error) { return chunks, nil })
	require.NoError(t, err)
	ids := []domain.ChunkID{result.Chunks[0].Chunk.ID, result.Chunks[1].Chunk.ID, result.Chunks[2].Chunk.ID}
	require.Equal(t, []domain.ChunkID{"a", "b", "c"}, ids)
	require.Len(t, result.Chunks, 3, "truncation follows the merge")
}

// tiedStore returns hosted candidates in a scrambled order with equal scores.
type tiedStore struct{ domain.VectorStore }

func (tiedStore) GrantScope() domain.GrantScopeKind { return domain.GrantScopeMembership }

func (tiedStore) QueryFiltered(context.Context, []float32, int, domain.VectorFilter) ([]domain.ScoredChunk, error) {
	return nil, nil
}

func (tiedStore) QueryCandidates(context.Context, []float32, int, domain.VectorFilter) ([]domain.ScoredChunk, error) {
	var out []domain.ScoredChunk
	for _, id := range []domain.ChunkID{"d", "b", "e", "a", "c", "b"} {
		out = append(out, domain.ScoredChunk{Chunk: domain.Chunk{ID: id, DocumentID: "doc"}, Score: .5})
	}
	return out, nil
}
