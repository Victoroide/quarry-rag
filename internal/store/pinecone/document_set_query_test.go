package pinecone

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/pinecone-io/go-pinecone/v5/pinecone"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// hostedRecorder is an exact hosted double. It answers each request with one
// hit for the first document of its predicate, all at the same score, so the
// merge order is decided by chunk identifiers alone.
type hostedRecorder struct {
	t     *testing.T
	mu    sync.Mutex
	calls []recordedRequest
	// fail, when set, decides the answer of the numbered request (1-based).
	fail func(call int) error
}

type recordedRequest struct {
	documents, incarnations []string
	tenant, docType, label  string
}

func (h *hostedRecorder) conn() *fakeIndexConn {
	return &fakeIndexConn{query: func(_ context.Context, req *pinecone.QueryByVectorValuesRequest) (*pinecone.QueryVectorsResponse, error) {
		documents, incarnations := hostedFilterMembership(h.t, req.MetadataFilter)
		rec := recordedRequest{
			documents:    documents,
			incarnations: incarnations,
			tenant:       hostedFilterEquality(req.MetadataFilter, domain.VectorTenantKey),
			docType:      hostedFilterEquality(req.MetadataFilter, "doc_type"),
			label:        hostedFilterEquality(req.MetadataFilter, "collection"),
		}
		h.mu.Lock()
		h.calls = append(h.calls, rec)
		n := len(h.calls)
		fail := h.fail
		h.mu.Unlock()
		if fail != nil {
			if err := fail(n); err != nil {
				return nil, err
			}
		}
		return &pinecone.QueryVectorsResponse{Matches: []*pinecone.ScoredVector{hostedHit(h.t, rec.documents[0], rec.incarnations[0], rec.tenant, "chunk-"+rec.documents[0])}}, nil
	}}
}

func (h *hostedRecorder) count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.calls)
}

func setScope(count int, budget *domain.SearchBudget) (scope *domain.AuthorizedScope, ids []string) {
	scope, ids = memberScope(count)
	scope.DocumentSet = &domain.DocumentSetRef{ID: "20000000-0000-4000-8000-000000000001", Revision: "revision"}
	scope.Budget = budget
	return scope, ids
}

func TestHostedSearchPartitionsAtTheDocumentedBoundaries(t *testing.T) {
	for _, tc := range []struct {
		members int
		sizes   []int
	}{
		{9999, []int{9999}},
		{10000, []int{10000}},
		{10001, []int{10000, 1}},
		{20000, []int{10000, 10000}},
		{100000, slices.Repeat([]int{10000}, 10)},
	} {
		t.Run(itoa(tc.members), func(t *testing.T) {
			scope, ids := setScope(tc.members, domain.NewSearchBudget(10, len(tc.sizes)))
			hosted := &hostedRecorder{t: t}
			driver := NewDriver(testClient(hosted.conn()))
			partitions, ok := driver.SearchPartitions(tc.members)
			require.True(t, ok)
			require.Len(t, tc.sizes, partitions)

			filter := domain.VectorFilter{DocType: "report", Collection: "team", DocumentIDs: ids, Authorized: scope}
			_, err := driver.QueryFiltered(t.Context(), []float32{1}, 3, filter)
			require.NoError(t, err)
			require.Equal(t, len(tc.sizes), hosted.count(), "one request per partition")
			require.Equal(t, len(tc.sizes), scope.Budget.Used(), "every request counts against the budget")
			seen := map[string]bool{}
			for i, call := range hosted.calls {
				require.Len(t, call.documents, tc.sizes[i])
				require.Len(t, call.incarnations, tc.sizes[i], "each predicate describes the same documents")
				require.LessOrEqual(t, len(call.documents), maxInValues)
				require.Equal(t, scopeTenant, call.tenant, "the tenant filter is on every request")
				require.Equal(t, "report", call.docType)
				require.Equal(t, "team", call.label)
				for j, id := range call.documents {
					require.False(t, seen[id], "a document is sent once")
					seen[id] = true
					require.Equal(t, scope.Incarnations[id], call.incarnations[j])
				}
			}
			require.Len(t, seen, tc.members, "every authorized incarnation is partitioned, none dropped")
		})
	}
}

func TestSearchPartitionsPricesAScope(t *testing.T) {
	driver := NewDriver(testClient(&fakeIndexConn{}))
	for members, want := range map[int]int{0: 0, 1: 1, 9999: 1, 10000: 1, 10001: 2, 20000: 2, 20001: 3, 100000: 10, 100001: 11} {
		got, ok := driver.SearchPartitions(members)
		require.True(t, ok)
		require.Equal(t, want, got, "%d members", members)
	}
	_, ok := driver.SearchPartitions(-1)
	require.False(t, ok)
}

func TestSparseLocalPassSendsOneDocumentWhateverTheScope(t *testing.T) {
	scope, ids := setScope(100000, domain.NewSearchBudget(10, 10))
	hosted := &hostedRecorder{t: t}
	driver := NewDriver(testClient(hosted.conn()))
	filter := domain.VectorFilter{DocumentIDs: ids[:1], Authorized: scope}
	_, err := driver.QueryFiltered(t.Context(), []float32{1}, 3, filter)
	require.NoError(t, err)
	require.Equal(t, 1, hosted.count())
	require.Len(t, hosted.calls[0].documents, 1)
	require.Equal(t, 1, scope.Budget.Used())
}

func TestMergedHitsOrderByScoreThenChunkAndTruncateAfterMerging(t *testing.T) {
	scope, ids := setScope(35000, domain.NewSearchBudget(10, 4))
	hosted := &hostedRecorder{t: t}
	driver := NewDriver(testClient(hosted.conn()))
	filter := domain.VectorFilter{DocumentIDs: ids, Authorized: scope}
	merged, err := driver.QueryFiltered(t.Context(), []float32{1}, 3, filter)
	require.NoError(t, err)
	require.Equal(t, 4, hosted.count())
	require.Len(t, merged, 3, "truncation applies once, after the partitions merge")
	for i := 1; i < len(merged); i++ {
		require.Less(t, merged[i-1].Chunk.ID, merged[i].Chunk.ID, "equal scores order by ascending chunk ID")
	}
	candidates, err := driver.QueryCandidates(t.Context(), []float32{1}, 3, domain.VectorFilter{DocumentIDs: ids, Authorized: setScopeWithBudget(scope, domain.NewSearchBudget(10, 4))})
	require.NoError(t, err)
	require.Len(t, candidates, 4, "candidates keep every partition's hits for verification before any merge")
}

func setScopeWithBudget(scope *domain.AuthorizedScope, budget *domain.SearchBudget) *domain.AuthorizedScope {
	copied := *scope
	copied.Budget = budget
	return &copied
}

func TestRetriesAreCountedAndOnlySpendTheSpareBudget(t *testing.T) {
	unavailable := status.Error(codes.Unavailable, "try again")
	t.Run("a planned budget leaves no room to retry", func(t *testing.T) {
		scope, ids := setScope(10001, domain.NewSearchBudget(2, 2))
		hosted := &hostedRecorder{t: t, fail: func(int) error { return unavailable }}
		driver := NewDriver(testClient(hosted.conn()))
		_, err := driver.QueryFiltered(t.Context(), []float32{1}, 3, domain.VectorFilter{DocumentIDs: ids, Authorized: scope})
		require.Error(t, err)
		require.Equal(t, 1, hosted.count(), "the first failure ends the request and nothing retries silently")
		require.Equal(t, 1, scope.Budget.Used())
	})
	t.Run("spare budget pays for a retry", func(t *testing.T) {
		scope, ids := setScope(10000, domain.NewSearchBudget(5, 1))
		hosted := &hostedRecorder{t: t, fail: func(call int) error {
			if call == 1 {
				return unavailable
			}
			return nil
		}}
		driver := NewDriver(testClient(hosted.conn()))
		chunks, err := driver.QueryFiltered(t.Context(), []float32{1}, 3, domain.VectorFilter{DocumentIDs: ids, Authorized: scope})
		require.NoError(t, err)
		require.Len(t, chunks, 1)
		require.Equal(t, 2, hosted.count())
		require.Equal(t, 2, scope.Budget.Used(), "the retry is an attempt like any other")
	})
	t.Run("no sequence of failures exceeds the total", func(t *testing.T) {
		for _, shape := range [][2]int{{1, 1}, {3, 3}, {4, 3}, {5, 3}, {10, 3}, {10, 10}} {
			limit, planned := shape[0], shape[1]
			scope, ids := setScope(30000, domain.NewSearchBudget(limit, planned))
			hosted := &hostedRecorder{t: t, fail: func(int) error { return unavailable }}
			driver := NewDriver(testClient(hosted.conn()))
			_, err := driver.QueryFiltered(t.Context(), []float32{1}, 3, domain.VectorFilter{DocumentIDs: ids, Authorized: scope})
			require.Error(t, err)
			require.LessOrEqual(t, hosted.count(), limit)
			require.LessOrEqual(t, scope.Budget.Used(), limit)
			require.Equal(t, hosted.count(), scope.Budget.Used(), "every attempt was counted and none was hidden")
		}
	})
}

func TestTimedOutFinalBatchFailsTheWholeRequest(t *testing.T) {
	scope, ids := setScope(30000, domain.NewSearchBudget(3, 3))
	hosted := &hostedRecorder{t: t, fail: func(call int) error {
		if call == 3 {
			return context.DeadlineExceeded
		}
		return nil
	}}
	driver := NewDriver(testClient(hosted.conn()))
	filter := domain.VectorFilter{DocumentIDs: ids, Authorized: scope}
	chunks, err := driver.QueryFiltered(t.Context(), []float32{1}, 3, filter)
	require.Error(t, err)
	require.Nil(t, chunks, "a ranking from the partitions that answered is never returned")
	require.Equal(t, 3, hosted.count())

	scope = setScopeWithBudget(scope, domain.NewSearchBudget(3, 3))
	hosted.mu.Lock()
	hosted.calls = nil
	hosted.mu.Unlock()
	candidates, err := driver.QueryCandidates(t.Context(), []float32{1}, 3, domain.VectorFilter{DocumentIDs: ids, Authorized: scope})
	require.Error(t, err)
	require.Nil(t, candidates)
}

func TestFailedMiddlePartitionStopsLaterBatches(t *testing.T) {
	scope, ids := setScope(30000, domain.NewSearchBudget(3, 3))
	hosted := &hostedRecorder{t: t, fail: func(call int) error {
		if call == 2 {
			return status.Error(codes.InvalidArgument, "rejected")
		}
		return nil
	}}
	driver := NewDriver(testClient(hosted.conn()))
	chunks, err := driver.QueryFiltered(t.Context(), []float32{1}, 3, domain.VectorFilter{DocumentIDs: ids, Authorized: scope})
	require.Error(t, err)
	require.Nil(t, chunks)
	require.Equal(t, 2, hosted.count(), "no batch follows the failure")
	require.Equal(t, 2, scope.Budget.Used())
}

func TestExhaustedBudgetFailsBeforeTheBatchIsSent(t *testing.T) {
	scope, ids := setScope(20000, domain.NewSearchBudget(1, 1))
	hosted := &hostedRecorder{t: t}
	driver := NewDriver(testClient(hosted.conn()))
	chunks, err := driver.QueryFiltered(t.Context(), []float32{1}, 3, domain.VectorFilter{DocumentIDs: ids, Authorized: scope})
	require.ErrorIs(t, err, domain.ErrVectorQueryBudget)
	require.Nil(t, chunks)
	require.Equal(t, 1, hosted.count(), "the second batch is never dispatched")
}

func TestAuthorityIsCheckedBeforeEveryBatch(t *testing.T) {
	lapsed := errors.New("document set expired")
	scope, ids := setScope(30000, domain.NewSearchBudget(3, 3))
	var checks int
	scope.BeforeSearch = func(context.Context) error {
		checks++
		if checks == 2 {
			return lapsed
		}
		return nil
	}
	hosted := &hostedRecorder{t: t}
	driver := NewDriver(testClient(hosted.conn()))
	chunks, err := driver.QueryFiltered(t.Context(), []float32{1}, 3, domain.VectorFilter{DocumentIDs: ids, Authorized: scope})
	require.ErrorIs(t, err, lapsed)
	require.Nil(t, chunks)
	require.Equal(t, 2, checks)
	require.Equal(t, 1, hosted.count(), "the batch after the lapse never reaches the index")
	require.Equal(t, 1, scope.Budget.Used(), "a refused batch spends nothing")
}

func TestCancellationStopsTheSearchAndReturnsUnusedAttempts(t *testing.T) {
	scope, ids := setScope(30000, domain.NewSearchBudget(3, 3))
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	hosted := &hostedRecorder{t: t, fail: func(int) error { cancel(); return nil }}
	driver := NewDriver(testClient(hosted.conn()))
	chunks, err := driver.QueryFiltered(ctx, []float32{1}, 3, domain.VectorFilter{DocumentIDs: ids, Authorized: scope})
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, chunks)
	require.Equal(t, 1, hosted.count(), "no batch follows a disconnect")
	require.Equal(t, 1, scope.Budget.Used(), "the attempt reserved for the canceled batch was returned")
}

func TestRequestBudgetAndRefillBudgetNeverMix(t *testing.T) {
	scope, ids := setScope(10, domain.NewSearchBudget(3, 3))
	hosted := &hostedRecorder{t: t}
	driver := NewDriver(testClient(hosted.conn()))
	_, _, err := driver.QueryFilteredBudgeted(t.Context(), []float32{1}, 3, domain.VectorFilter{DocumentIDs: ids, Authorized: scope}, 3)
	require.ErrorIs(t, err, domain.ErrGrantScopeUnsupported)
	require.Zero(t, hosted.count())
}

func TestASetScopeNeverSearchesWithoutMembers(t *testing.T) {
	scope, _ := setScope(2, domain.NewSearchBudget(3, 3))
	hosted := &hostedRecorder{t: t}
	driver := NewDriver(testClient(hosted.conn()))
	_, err := driver.QueryFiltered(t.Context(), []float32{1}, 3, domain.VectorFilter{Authorized: scope})
	require.ErrorIs(t, err, domain.ErrGrantScopeUnsupported)
	chunks, err := driver.QueryFiltered(t.Context(), []float32{1}, 3, domain.VectorFilter{DocumentIDs: []string{}, Authorized: scope})
	require.NoError(t, err)
	require.Empty(t, chunks, "an empty intersection performs no request")
	require.Zero(t, hosted.count())
	require.Zero(t, scope.Budget.Used())
}

func TestSearchBudgetAccounting(t *testing.T) {
	budget := domain.NewSearchBudget(5, 3)
	allowed, err := budget.Acquire(3)
	require.NoError(t, err)
	require.Equal(t, 3, allowed, "two spare attempts cover two retries")
	other, err := budget.Acquire(3)
	require.NoError(t, err)
	require.Equal(t, 1, other, "the pool is spent, so a later search gets its one planned attempt")
	budget.Release(allowed, 1)
	require.Equal(t, 2, budget.Used(), "unused attempts return")
	again, err := budget.Acquire(3)
	require.NoError(t, err)
	require.Equal(t, 3, again, "returned retries can be spent again")
	budget.Release(again, 3)
	require.Equal(t, 5, budget.Used())
	_, err = budget.Acquire(1)
	require.ErrorIs(t, err, domain.ErrVectorQueryBudget)
	_, err = domain.NewSearchBudget(5, 5).Acquire(0)
	require.ErrorIs(t, err, domain.ErrVectorQueryBudget)
	budget.Release(0, 0)
	require.Equal(t, 5, budget.Used(), "a release that returns nothing changes nothing")
}

func TestSearchBudgetIsSafeForConcurrentSearches(t *testing.T) {
	budget := domain.NewSearchBudget(10, 4)
	var wg sync.WaitGroup
	for range 40 {
		wg.Go(func() {
			allowed, err := budget.Acquire(3)
			if err == nil {
				budget.Release(allowed, allowed)
			}
		})
	}
	wg.Wait()
	require.LessOrEqual(t, budget.Used(), 10)
}

func itoa(n int) string {
	digits := []byte{}
	if n == 0 {
		return "0"
	}
	for ; n > 0; n /= 10 {
		digits = append([]byte{byte('0' + n%10)}, digits...)
	}
	return string(digits)
}
