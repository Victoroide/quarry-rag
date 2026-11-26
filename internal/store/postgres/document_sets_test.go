package postgres

import (
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

func (f *retrievalFixture) createSet(t *testing.T, p domain.Principal, key string, ids ...string) DocumentSet {
	t.Helper()
	set, err := f.c.CreateDocumentSet(t.Context(), DocumentSetRequest{TenantID: p.TenantID, OwnerID: p.ID, ImportKey: key, DocumentIDs: ids, MaxMembers: 100})
	require.NoError(t, err)
	return set
}

func (f *retrievalFixture) setRef(set DocumentSet) *domain.DocumentSetRef {
	return &domain.DocumentSetRef{ID: set.ID, Revision: set.Revision, ExpiresAt: set.ExpiresAt}
}

// expire moves the set's whole lifetime into the past while keeping its constraints.
func (f *retrievalFixture) expire(t *testing.T, set DocumentSet) {
	t.Helper()
	grantExec(t, f.c, `UPDATE grant_catalog_document_sets SET created_at=created_at-interval '3 hours', expires_at=expires_at-interval '3 hours' WHERE set_id=$1`, set.ID)
}

func (f *retrievalFixture) memberCount(t *testing.T, set string) int {
	t.Helper()
	var n int
	require.NoError(t, f.c.Pool.QueryRow(t.Context(), `SELECT count(*) FROM grant_catalog_document_set_members WHERE set_id=$1`, set).Scan(&n))
	return n
}

func (f *retrievalFixture) setCount(t *testing.T) int {
	t.Helper()
	var n int
	require.NoError(t, f.c.Pool.QueryRow(t.Context(), `SELECT count(*) FROM grant_catalog_document_sets`).Scan(&n))
	return n
}

func (f *retrievalFixture) deriveIDs(t *testing.T, p domain.Principal, req domain.ReadScopeRequest) ([]string, domain.ReadScope) {
	t.Helper()
	req.Membership = max(req.Membership, 100)
	scope, err := f.reader.DeriveReadScope(t.Context(), p, req)
	require.NoError(t, err)
	ids := make([]string, 0, len(scope.Documents))
	for _, doc := range scope.Documents {
		ids = append(ids, doc.ID)
	}
	return ids, scope
}

func TestDocumentSetCreationRecordsOwnerTenantRevisionAndExpiry(t *testing.T) {
	f := newRetrievalFixture(t)
	before := time.Now()
	set := f.createSet(t, f.owner, "", f.shared, f.own, f.own)
	require.NotEqual(t, uuid.Nil.String(), set.ID)
	require.NotEqual(t, set.ID, set.Revision)
	require.Equal(t, 2, set.Members, "repeated identifiers collapse")
	require.False(t, set.Replayed)
	require.WithinDuration(t, before.Add(time.Hour), set.ExpiresAt, time.Minute, "one hour is the default lifetime")

	var tenant, owner, createdBy string
	var count int
	require.NoError(t, f.c.Pool.QueryRow(t.Context(), `SELECT tenant_id::text, owner_principal_id::text, created_by, member_count FROM grant_catalog_document_sets WHERE set_id=$1`, set.ID).
		Scan(&tenant, &owner, &createdBy, &count))
	require.Equal(t, f.owner.TenantID, tenant)
	require.Equal(t, f.owner.ID, owner)
	require.NotEmpty(t, createdBy)
	require.Equal(t, 2, count)
	require.Equal(t, 2, f.memberCount(t, set.ID))

	for ttl, ok := range map[time.Duration]bool{time.Second: true, 24 * time.Hour: true, 24*time.Hour + time.Second: false, -time.Second: false} {
		_, err := f.c.CreateDocumentSet(t.Context(), DocumentSetRequest{TenantID: f.owner.TenantID, OwnerID: f.owner.ID, TTL: ttl, MaxMembers: 10})
		if ok {
			require.NoError(t, err, ttl.String())
		} else {
			require.Error(t, err, ttl.String())
		}
	}
}

func TestDocumentSetMayBeEmpty(t *testing.T) {
	f := newRetrievalFixture(t)
	set := f.createSet(t, f.owner, "")
	require.Zero(t, set.Members)
	ids, scope := f.deriveIDs(t, f.owner, domain.ReadScopeRequest{DocumentSetID: set.ID})
	require.Empty(t, ids)
	require.True(t, scope.Empty, "a valid empty intersection is empty, not unrestricted")
	require.NotNil(t, scope.DocumentSet)
	require.Equal(t, set.Revision, scope.DocumentSet.Revision)
}

func TestDocumentSetRejectsUnusableMembersUniformlyAndPublishesNothing(t *testing.T) {
	f := newRetrievalFixture(t)
	pending := ownedRequest(f.owner, "", "accepted but not indexed")
	require.Equal(t, domain.AcceptAccepted, f.accept(t, pending).Outcome)
	cases := map[string][]string{
		"unknown":             {uuid.NewString()},
		"owned by a peer":     {f.peerOnly},
		"another tenant":      {f.other},
		"unfinished":          {string(pending.Document.ID)},
		"one bad among good":  {f.own, f.peerOnly},
		"unknown among good":  {f.shared, uuid.NewString(), f.own},
		"duplicate of a bad":  {f.peerOnly, f.peerOnly},
		"all kinds together":  {f.own, f.peerOnly, f.other, uuid.NewString()},
		"readable and absent": {f.shared, string(pending.Document.ID)},
	}
	var messages []string
	for name, ids := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := f.c.CreateDocumentSet(t.Context(), DocumentSetRequest{TenantID: f.owner.TenantID, OwnerID: f.owner.ID, DocumentIDs: ids, MaxMembers: 10})
			require.ErrorContains(t, err, "unknown or unauthorized document set member")
			messages = append(messages, err.Error())
			require.Zero(t, f.setCount(t), "a rejected manifest publishes no partial set")
		})
	}
	for _, message := range messages {
		require.Equal(t, messages[0], message, "the failure never says which member was refused or why")
	}
}

func TestDocumentSetCreationRequiresAnActiveOwnerOfTheTenant(t *testing.T) {
	f := newRetrievalFixture(t)
	for name, request := range map[string]DocumentSetRequest{
		"unknown owner":       {TenantID: f.tenant, OwnerID: uuid.NewString()},
		"owner of the other":  {TenantID: f.otherTenant, OwnerID: f.owner.ID},
		"unknown tenant":      {TenantID: uuid.NewString(), OwnerID: f.owner.ID},
		"disabled principal":  {TenantID: f.tenant, OwnerID: f.disabled.ID},
		"nonexistent pairing": {TenantID: f.otherTenant, OwnerID: f.peer.ID},
	} {
		t.Run(name, func(t *testing.T) {
			request.MaxMembers = 10
			_, err := f.c.CreateDocumentSet(t.Context(), request)
			require.ErrorContains(t, err, "unknown or inactive owner")
		})
	}
	require.Zero(t, f.setCount(t))
	set := f.createSet(t, f.owner, "")
	f.setEnabled(t, f.owner, "owner", false)
	_, err := f.c.CreateDocumentSet(t.Context(), DocumentSetRequest{TenantID: f.tenant, OwnerID: f.owner.ID, MaxMembers: 10})
	require.ErrorContains(t, err, "unknown or inactive owner")
	_, err = f.reader.DeriveReadScope(t.Context(), f.owner, domain.ReadScopeRequest{DocumentSetID: set.ID, Membership: 10})
	require.ErrorIs(t, err, domain.ErrReadDenied, "a deactivated owner reads nothing")
	state, live, err := f.reader.DocumentSetState(t.Context(), f.owner, *f.setRef(set))
	require.NoError(t, err)
	require.False(t, state.Active)
	require.False(t, live, "the set of a deactivated owner is not live")
	f.setEnabled(t, f.owner, "owner", true)
	_, live, err = f.reader.DocumentSetState(t.Context(), f.owner, *f.setRef(set))
	require.NoError(t, err)
	require.True(t, live, "reactivating the owner revives its unexpired set")
}

func TestDocumentSetResolvesOnlyForItsOwner(t *testing.T) {
	f := newRetrievalFixture(t)
	set := f.createSet(t, f.owner, "", f.own, f.shared)
	ids, scope := f.deriveIDs(t, f.owner, domain.ReadScopeRequest{DocumentSetID: set.ID})
	require.ElementsMatch(t, []string{f.own, f.shared}, ids)
	require.Equal(t, set.ID, scope.DocumentSet.ID)
	require.Equal(t, set.Revision, scope.DocumentSet.Revision)
	require.WithinDuration(t, set.ExpiresAt, scope.DocumentSet.ExpiresAt, time.Millisecond)

	_, absentErr := f.reader.DeriveReadScope(t.Context(), f.owner, domain.ReadScopeRequest{DocumentSetID: uuid.NewString(), Membership: 10})
	require.ErrorIs(t, absentErr, domain.ErrDocumentSetNotFound)
	for name, who := range map[string]domain.Principal{"peer in the tenant": f.peer, "principal of another tenant": f.outsider} {
		t.Run(name, func(t *testing.T) {
			_, err := f.reader.DeriveReadScope(t.Context(), who, domain.ReadScopeRequest{DocumentSetID: set.ID, Membership: 10})
			require.ErrorIs(t, err, domain.ErrDocumentSetNotFound)
			require.Equal(t, absentErr.Error(), err.Error(), "a foreign reference matches an absent one")
			_, live, stateErr := f.reader.DocumentSetState(t.Context(), who, *f.setRef(set))
			require.NoError(t, stateErr)
			require.False(t, live)
		})
	}
	// A principal naming the owner's tenant and identity from another tenant is not active there.
	_, err := f.reader.DeriveReadScope(t.Context(), domain.Principal{ID: f.owner.ID, TenantID: f.otherTenant}, domain.ReadScopeRequest{DocumentSetID: set.ID, Membership: 10})
	require.Error(t, err)
	_, live, err := f.reader.DocumentSetState(t.Context(), f.owner, domain.DocumentSetRef{ID: set.ID, Revision: uuid.NewString()})
	require.NoError(t, err)
	require.False(t, live, "a different revision never matches")
}

func TestDocumentSetIsOnlyASelectorOverLiveGrants(t *testing.T) {
	f := newRetrievalFixture(t)
	set := f.createSet(t, f.owner, "", f.own, f.shared)

	f.grantRead(t, f.peer, f.peerOnly, f.owner, true)
	all, _ := f.deriveIDs(t, f.owner, domain.ReadScopeRequest{})
	require.ElementsMatch(t, []string{f.own, f.shared, f.peerOnly}, all)
	got, _ := f.deriveIDs(t, f.owner, domain.ReadScopeRequest{DocumentSetID: set.ID})
	require.ElementsMatch(t, []string{f.own, f.shared}, got, "a later grant to a document outside the set cannot add it")

	f.grantRead(t, f.peer, f.shared, f.owner, false)
	got, scope := f.deriveIDs(t, f.owner, domain.ReadScopeRequest{DocumentSetID: set.ID})
	require.Equal(t, []string{f.own}, got, "a revoked member leaves the intersection")
	require.Equal(t, f.currentRevision(t), scope.Revision)
	require.Equal(t, 2, f.memberCount(t, set.ID), "the membership itself never changes")

	f.grantRead(t, f.peer, f.shared, f.owner, true)
	got, _ = f.deriveIDs(t, f.owner, domain.ReadScopeRequest{DocumentSetID: set.ID})
	require.ElementsMatch(t, []string{f.own, f.shared}, got)

	got, _ = f.deriveIDs(t, f.owner, domain.ReadScopeRequest{DocumentSetID: set.ID, DocumentIDs: []string{f.shared, f.peerOnly, f.other, uuid.NewString()}})
	require.Equal(t, []string{f.shared}, got, "an inline list narrows the set")
	_, empty := f.deriveIDs(t, f.owner, domain.ReadScopeRequest{DocumentSetID: set.ID, DocumentIDs: []string{}})
	require.True(t, empty.Empty, "an explicit empty list never widens the set")
	_, empty = f.deriveIDs(t, f.owner, domain.ReadScopeRequest{DocumentSetID: set.ID, DocumentIDs: []string{f.peerOnly}})
	require.True(t, empty.Empty)
	_, empty = f.deriveIDs(t, f.owner, domain.ReadScopeRequest{DocumentSetID: set.ID, DocType: "report"})
	require.True(t, empty.Empty)
	_, empty = f.deriveIDs(t, f.owner, domain.ReadScopeRequest{DocumentSetID: set.ID, Collection: "team"})
	require.True(t, empty.Empty)
	got, _ = f.deriveIDs(t, f.owner, domain.ReadScopeRequest{DocumentSetID: set.ID, DocType: "guide", Collection: ""})
	require.ElementsMatch(t, []string{f.own, f.shared}, got)

	over, err := f.reader.DeriveReadScope(t.Context(), f.owner, domain.ReadScopeRequest{DocumentSetID: set.ID, Membership: 1})
	require.NoError(t, err)
	require.True(t, over.Exceeded)
	require.Nil(t, over.Documents)
	none, err := f.reader.DeriveReadScope(t.Context(), f.owner, domain.ReadScopeRequest{DocumentSetID: set.ID})
	require.NoError(t, err)
	require.False(t, none.Empty, "a zero membership asks only whether anything qualifies")
}

func TestDocumentSetNeverYieldsADeletedOrReplacedMember(t *testing.T) {
	f := newRetrievalFixture(t)
	set := f.createSet(t, f.owner, "", f.own, f.shared)
	grantExec(t, f.c, `DELETE FROM documents WHERE id=$1`, f.own)
	require.Equal(t, 1, f.memberCount(t, set.ID), "deleting a document removes its membership")
	got, _ := f.deriveIDs(t, f.owner, domain.ReadScopeRequest{DocumentSetID: set.ID})
	require.Equal(t, []string{f.shared}, got)

	// A replacement under the same identifier has a new incarnation that no set names.
	grantExec(t, f.c, `INSERT INTO documents(id,content,status) VALUES($1,'replacement','completed')`, f.own)
	got, _ = f.deriveIDs(t, f.owner, domain.ReadScopeRequest{DocumentSetID: set.ID})
	require.Equal(t, []string{f.shared}, got)
	chunks, err := f.vectors.QueryFiltered(t.Context(), retrievalQuery, 10, domain.VectorFilter{Authorized: &domain.AuthorizedScope{TenantID: f.tenant, PrincipalID: f.owner.ID, DocumentSet: f.setRef(set)}})
	require.NoError(t, err)
	require.Equal(t, []string{f.shared}, documentIDs(chunks))
}

func TestExpiredDocumentSetResolvesAsAbsent(t *testing.T) {
	f := newRetrievalFixture(t)
	set := f.createSet(t, f.owner, "", f.own)
	_, absentErr := f.reader.DeriveReadScope(t.Context(), f.owner, domain.ReadScopeRequest{DocumentSetID: uuid.NewString(), Membership: 10})
	f.expire(t, set)
	_, err := f.reader.DeriveReadScope(t.Context(), f.owner, domain.ReadScopeRequest{DocumentSetID: set.ID, Membership: 10})
	require.ErrorIs(t, err, domain.ErrDocumentSetNotFound)
	require.Equal(t, absentErr.Error(), err.Error())
	state, live, err := f.reader.DocumentSetState(t.Context(), f.owner, *f.setRef(set))
	require.NoError(t, err)
	require.True(t, state.Active)
	require.False(t, live)
	chunks, err := f.vectors.QueryFiltered(t.Context(), retrievalQuery, 10, domain.VectorFilter{Authorized: &domain.AuthorizedScope{TenantID: f.tenant, PrincipalID: f.owner.ID, DocumentSet: f.setRef(set)}})
	require.NoError(t, err)
	require.Empty(t, chunks, "an expired set selects nothing in the ranking statement")
}

func TestVectorSearchConfinesRankingToTheSetBeforeTheLimit(t *testing.T) {
	f := newRetrievalFixture(t)
	only := f.createSet(t, f.owner, "", f.shared)
	both := f.createSet(t, f.owner, "", f.own, f.shared)
	empty := f.createSet(t, f.owner, "")
	peerSet := f.createSet(t, f.peer, "", f.peerOnly, f.shared)
	for name, limit := range map[string]int{"exact": 100, "approximate": 0} {
		t.Run(name, func(t *testing.T) {
			f.vectors.exactScopeMax = limit
			search := func(who domain.Principal, set DocumentSet, topK int, mutate func(*domain.VectorFilter)) []string {
				t.Helper()
				filter := domain.VectorFilter{Authorized: &domain.AuthorizedScope{TenantID: who.TenantID, PrincipalID: who.ID, DocumentSet: f.setRef(set)}}
				if mutate != nil {
					mutate(&filter)
				}
				chunks, err := f.vectors.QueryFiltered(t.Context(), retrievalQuery, topK, filter)
				require.NoError(t, err)
				return documentIDs(chunks)
			}
			require.Equal(t, []string{f.shared}, search(f.owner, only, 1, nil), "the nearer readable document outside the set cannot take the limit")
			require.Equal(t, []string{f.own, f.shared}, search(f.owner, both, 10, nil))
			require.Empty(t, search(f.owner, empty, 10, nil))
			require.Equal(t, []string{f.peerOnly, f.shared}, search(f.peer, peerSet, 10, nil))
			require.Empty(t, search(f.peer, only, 10, nil), "another principal cannot search with a set it does not own")
			require.Empty(t, search(f.outsider, both, 10, nil))
			require.Equal(t, []string{f.own}, search(f.owner, both, 10, func(filter *domain.VectorFilter) { filter.DocumentIDs = []string{f.own, f.peerOnly} }))
			require.Empty(t, search(f.owner, both, 10, func(filter *domain.VectorFilter) { filter.DocumentIDs = []string{} }))
			require.Empty(t, search(f.owner, both, 10, func(filter *domain.VectorFilter) { filter.DocType = "report" }))
			malformed := domain.VectorFilter{Authorized: &domain.AuthorizedScope{TenantID: f.tenant, PrincipalID: f.owner.ID, DocumentSet: &domain.DocumentSetRef{ID: "x'; SELECT 1 --"}}}
			chunks, err := f.vectors.QueryFiltered(t.Context(), retrievalQuery, 10, malformed)
			require.NoError(t, err)
			require.Empty(t, chunks, "a malformed reference selects nothing")
		})
	}
	f.vectors.exactScopeMax = 0
	var store domain.DocumentSetScopedVectorStore = f.vectors
	require.Equal(t, domain.GrantScopeRelation, store.GrantScope())
}

func TestVectorSearchWithASetFollowsRevocationAndExpiry(t *testing.T) {
	f := newRetrievalFixture(t)
	set := f.createSet(t, f.owner, "", f.own, f.shared)
	filter := domain.VectorFilter{Authorized: &domain.AuthorizedScope{TenantID: f.tenant, PrincipalID: f.owner.ID, DocumentSet: f.setRef(set)}}
	search := func() []string {
		t.Helper()
		chunks, err := f.vectors.QueryFiltered(t.Context(), retrievalQuery, 10, filter)
		require.NoError(t, err)
		return documentIDs(chunks)
	}
	require.Equal(t, []string{f.own, f.shared}, search())
	f.grantRead(t, f.peer, f.shared, f.owner, false)
	require.Equal(t, []string{f.own}, search())
	f.setEnabled(t, f.owner, "owner", false)
	require.Empty(t, search())
	f.setEnabled(t, f.owner, "owner", true)
	f.expire(t, set)
	require.Empty(t, search())
}

func TestSubjectEvidenceInASetRequiresMembership(t *testing.T) {
	f := newRetrievalFixture(t)
	candidates, err := f.vectors.QueryFiltered(t.Context(), retrievalQuery, 10, domain.VectorFilter{})
	require.NoError(t, err)
	require.Len(t, candidates, 4)
	set := f.createSet(t, f.owner, "", f.shared)
	checks := f.reader.EvidenceInSet(f.owner, *f.setRef(set), "", "")

	visible, previews, err := checks.Visibility.VisibleEvidenceWithPreviews(t.Context(), candidates, []string{f.own, f.peerOnly, f.shared, f.other})
	require.NoError(t, err)
	require.Equal(t, []string{f.shared}, documentIDs(visible), "readable documents outside the set are dropped before any use")
	require.NotContains(t, previews, f.own)
	docs, err := checks.Documents.VisibleDocumentIDs(t.Context(), []string{f.own, f.shared, f.peerOnly})
	require.NoError(t, err)
	require.Equal(t, map[string]struct{}{f.shared: {}}, docs)
	texts, err := checks.Previews.DocumentPreviews(t.Context(), []string{f.own, f.shared})
	require.NoError(t, err)
	require.NotContains(t, texts, f.own)

	f.expire(t, set)
	visible, _, err = checks.Visibility.VisibleEvidenceWithPreviews(t.Context(), candidates, nil)
	require.NoError(t, err)
	require.Empty(t, visible, "an expired set releases nothing")
}

func TestDocumentSetImportKeyReturnsTheOriginalOrReportsAConflict(t *testing.T) {
	f := newRetrievalFixture(t)
	first := f.createSet(t, f.owner, "nightly", f.own, f.shared)
	again, err := f.c.CreateDocumentSet(t.Context(), DocumentSetRequest{TenantID: f.tenant, OwnerID: f.owner.ID, ImportKey: "nightly", DocumentIDs: []string{f.shared, f.own, f.own}, MaxMembers: 10, TTL: 2 * time.Hour})
	require.NoError(t, err)
	require.True(t, again.Replayed)
	require.Equal(t, first.ID, again.ID)
	require.Equal(t, first.Revision, again.Revision)
	require.True(t, first.ExpiresAt.Equal(again.ExpiresAt), "a retry never changes the original lifetime")
	require.Equal(t, 1, f.setCount(t))

	_, err = f.c.CreateDocumentSet(t.Context(), DocumentSetRequest{TenantID: f.tenant, OwnerID: f.owner.ID, ImportKey: "nightly", DocumentIDs: []string{f.own}, MaxMembers: 10})
	require.ErrorContains(t, err, "import key conflict")
	_, err = f.c.CreateDocumentSet(t.Context(), DocumentSetRequest{TenantID: f.tenant, OwnerID: f.owner.ID, ImportKey: "nightly", MaxMembers: 10})
	require.ErrorContains(t, err, "import key conflict", "an empty retry of a nonempty import is a changed content")
	require.Equal(t, 1, f.setCount(t))

	other := f.createSet(t, f.peer, "nightly", f.peerOnly)
	require.NotEqual(t, first.ID, other.ID, "a key is scoped to its owner")
	require.False(t, other.Replayed)
	unkeyed := f.createSet(t, f.owner, "", f.own, f.shared)
	require.NotEqual(t, first.ID, unkeyed.ID)

	f.expire(t, first)
	renewed := f.createSet(t, f.owner, "nightly", f.own)
	require.NotEqual(t, first.ID, renewed.ID, "an expired set frees its key")
	require.False(t, renewed.Replayed)
	require.Equal(t, 0, f.memberCount(t, first.ID), "the expired set was removed with its members")
}

func TestConcurrentIdenticalImportsYieldOneSet(t *testing.T) {
	f := newRetrievalFixture(t)
	var wg sync.WaitGroup
	results := make([]DocumentSet, 6)
	errs := make([]error, len(results))
	for i := range results {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i], errs[i] = f.c.CreateDocumentSet(t.Context(), DocumentSetRequest{TenantID: f.tenant, OwnerID: f.owner.ID, ImportKey: "race", DocumentIDs: []string{f.own, f.shared}, MaxMembers: 10})
		}()
	}
	wg.Wait()
	replayed := 0
	for i := range results {
		require.NoError(t, errs[i])
		require.Equal(t, results[0].ID, results[i].ID)
		if results[i].Replayed {
			replayed++
		}
	}
	require.Equal(t, len(results)-1, replayed)
	require.Equal(t, 1, f.setCount(t))
}

func TestActiveDocumentSetsPerOwnerAreCapped(t *testing.T) {
	f := newRetrievalFixture(t)
	for range 15 {
		f.createSet(t, f.owner, "")
	}
	kept := f.createSet(t, f.owner, "kept", f.own)
	_, err := f.c.CreateDocumentSet(t.Context(), DocumentSetRequest{TenantID: f.tenant, OwnerID: f.owner.ID, MaxMembers: 10})
	require.ErrorContains(t, err, "active document set limit reached")
	f.createSet(t, f.peer, "")
	again, err := f.c.CreateDocumentSet(t.Context(), DocumentSetRequest{TenantID: f.tenant, OwnerID: f.owner.ID, ImportKey: "kept", DocumentIDs: []string{f.own}, MaxMembers: 10})
	require.NoError(t, err, "a replay needs no free slot")
	require.True(t, again.Replayed)
	require.Equal(t, kept.ID, again.ID)
}

func TestAnExpiredSetReleasesItsSlotAndIsRemovedInSmallSteps(t *testing.T) {
	f := newRetrievalFixture(t)
	var sets []DocumentSet
	for range 16 {
		sets = append(sets, f.createSet(t, f.owner, ""))
	}
	f.expire(t, sets[0])
	f.createSet(t, f.owner, "")
	require.Equal(t, 16, f.setCount(t), "the expired set was purged by the next creation")
	_, err := f.c.CreateDocumentSet(t.Context(), DocumentSetRequest{TenantID: f.tenant, OwnerID: f.owner.ID, MaxMembers: 10})
	require.ErrorContains(t, err, "active document set limit reached")

	for _, set := range sets[1:11] {
		f.expire(t, set)
	}
	f.createSet(t, f.owner, "")
	var expired int
	require.NoError(t, f.c.Pool.QueryRow(t.Context(), `SELECT count(*) FROM grant_catalog_document_sets WHERE expires_at <= clock_timestamp()`).Scan(&expired))
	require.Equal(t, 6, expired, "one creation removes at most four expired sets")

	removed, err := f.c.PurgeExpiredDocumentSets(t.Context())
	require.NoError(t, err)
	require.Equal(t, 6, removed)
	removed, err = f.c.PurgeExpiredDocumentSets(t.Context())
	require.NoError(t, err)
	require.Zero(t, removed)
}

func TestConcurrentCreationsNeverExceedTheActiveCap(t *testing.T) {
	f := newRetrievalFixture(t)
	var wg sync.WaitGroup
	errs := make([]error, 24)
	for i := range errs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, errs[i] = f.c.CreateDocumentSet(t.Context(), DocumentSetRequest{TenantID: f.tenant, OwnerID: f.owner.ID, DocumentIDs: []string{f.own}, MaxMembers: 10})
		}()
	}
	wg.Wait()
	created := 0
	for _, err := range errs {
		if err == nil {
			created++
		} else {
			require.ErrorContains(t, err, "active document set limit reached")
		}
	}
	require.Equal(t, 16, created)
	require.Equal(t, 16, f.setCount(t))
}

func TestDatabaseEnforcesTheMembershipCeilingAndInputShape(t *testing.T) {
	f := newRetrievalFixture(t)
	call := func(ttl int, key *string, docs []string, max int) error {
		_, err := f.c.Pool.Exec(t.Context(), `SELECT * FROM grant_catalog_create_document_set($1::uuid,$2::uuid,$3,$4,$5::uuid[],$6)`, f.tenant, f.owner.ID, ttl, key, docs, max)
		return err
	}
	longKey, none := strings.Repeat("k", 129), []string{}
	for name, err := range map[string]error{
		"more raw entries than the ceiling": call(60, nil, []string{f.own, f.own, f.own}, 2),
		"ceiling above the maximum":         call(60, nil, none, 100001),
		"zero ceiling":                      call(60, nil, none, 0),
		"lifetime above a day":              call(86401, nil, none, 10),
		"zero lifetime":                     call(0, nil, none, 10),
		"empty key":                         call(60, new(string), none, 10),
		"long key":                          call(60, &longKey, none, 10),
		"missing manifest":                  call(60, nil, nil, 10),
	} {
		t.Run(name, func(t *testing.T) {
			requireSQLState(t, err, "QD001")
		})
	}
	require.Zero(t, f.setCount(t))
}

func TestDocumentSetMembershipHasCompositeTenantConstraints(t *testing.T) {
	f := newRetrievalFixture(t)
	set := f.createSet(t, f.owner, "", f.own)
	var incarnation string
	require.NoError(t, f.c.Pool.QueryRow(t.Context(), `SELECT incarnation::text FROM grant_catalog_document_set_members WHERE set_id=$1`, set.ID).Scan(&incarnation))
	insert := func(tenant, document, incarnation string) error {
		_, err := f.c.Pool.Exec(t.Context(), `INSERT INTO grant_catalog_document_set_members(set_id,tenant_id,document_id,incarnation) VALUES($1,$2,$3,$4)`, set.ID, tenant, document, incarnation)
		return err
	}
	requireSQLState(t, insert(f.tenant, f.own, incarnation), "23505")
	requireSQLState(t, insert(f.otherTenant, f.other, uuid.NewString()), "23503")
	requireSQLState(t, insert(f.otherTenant, f.shared, incarnation), "23503")
	requireSQLState(t, insert(f.tenant, f.shared, uuid.NewString()), "23503")
	require.Equal(t, 1, f.memberCount(t, set.ID))
	_, err := f.c.Pool.Exec(t.Context(), `UPDATE grant_catalog_document_sets SET expires_at=created_at+interval '25 hours' WHERE set_id=$1`, set.ID)
	requireSQLState(t, err, "23514")
}

func TestDocumentSetPrivilegesFollowThePublishedGrants(t *testing.T) {
	databaseURL := startTestDatabase(t)
	owner := migrationTestClient(t, databaseURL)
	applyMigrationPrefix(t, owner, 10)
	roles := newGrantRoles(t, databaseURL, servingRoles...)
	grantExec(t, owner, roles.sql(`CREATE ROLE {quarry_service}; CREATE ROLE {quarry_operator}; CREATE ROLE {quarry_auditor};
		ALTER DEFAULT PRIVILEGES GRANT EXECUTE ON FUNCTIONS TO {quarry_service}, {quarry_operator};
		ALTER DEFAULT PRIVILEGES GRANT SELECT ON TABLES TO {quarry_service}, {quarry_operator}`))
	require.NoError(t, owner.RunMigrations(t.Context()))
	var inherited bool
	require.NoError(t, owner.Pool.QueryRow(t.Context(), roles.sql(`SELECT
		has_table_privilege('{quarry_service}','grant_catalog_document_sets_live','select')
		OR has_table_privilege('{quarry_service}','grant_catalog_document_set_readable','select')
		OR has_table_privilege('{quarry_service}','grant_catalog_document_sets','select')
		OR has_table_privilege('{quarry_service}','grant_catalog_document_set_members','select')
		OR has_table_privilege('public','grant_catalog_document_set_readable','select')
		OR has_function_privilege('{quarry_operator}','grant_catalog_create_document_set(uuid,uuid,integer,text,uuid[],integer)','execute')
		OR has_function_privilege('{quarry_operator}','grant_catalog_purge_document_sets(integer)','execute')
		OR has_function_privilege('{quarry_service}','grant_catalog_create_document_set_impl(uuid,uuid,integer,text,uuid[],integer)','execute')
		OR has_function_privilege('public','grant_catalog_create_document_set(uuid,uuid,integer,text,uuid[],integer)','execute')`)).Scan(&inherited))
	require.False(t, inherited, "the new objects are not reachable through default privileges")
	grantExec(t, owner, publishedServingSQL(t, roles, "grant-catalog.md", "GRANT USAGE ON SCHEMA public"))

	f := provisionOwnedFixture(t, owner)
	service := grantRoleClient(t, databaseURL, "SET SESSION AUTHORIZATION "+pgx.Identifier{roles.name("quarry_service")}.Sanitize())
	operator := grantRoleClient(t, databaseURL, "SET SESSION AUTHORIZATION "+pgx.Identifier{roles.name("quarry_operator")}.Sanitize())
	created, err := operator.CreateDocumentSet(t.Context(), DocumentSetRequest{TenantID: f.tenant, OwnerID: f.owner.ID, MaxMembers: 10})
	require.NoError(t, err, "the operator needs only the published function grants")
	removed, err := operator.PurgeExpiredDocumentSets(t.Context())
	require.NoError(t, err)
	require.Zero(t, removed)

	reader := GrantReader{Pool: service.Pool}
	scope, err := reader.DeriveReadScope(t.Context(), f.owner, domain.ReadScopeRequest{DocumentSetID: created.ID, Membership: 5})
	require.NoError(t, err, "the serving login resolves through the two views")
	require.True(t, scope.Empty)
	_, live, err := reader.DocumentSetState(t.Context(), f.owner, *scope.DocumentSet)
	require.NoError(t, err)
	require.True(t, live)

	_, err = service.CreateDocumentSet(t.Context(), DocumentSetRequest{TenantID: f.tenant, OwnerID: f.owner.ID, MaxMembers: 10})
	require.Error(t, err, "the serving login cannot create sets")
	for _, relation := range []string{"grant_catalog_document_sets", "grant_catalog_document_set_members"} {
		_, err = service.Pool.Exec(t.Context(), `SELECT 1 FROM `+relation)
		require.Error(t, err, "the serving login reads no set table")
	}
	_, err = operator.Pool.Exec(t.Context(), `SELECT 1 FROM grant_catalog_document_set_readable`)
	require.Error(t, err, "the operator reads no relation")
}

func TestDocumentSetIdentifiersAreServerGeneratedAndUnique(t *testing.T) {
	f := newRetrievalFixture(t)
	seen := map[string]struct{}{}
	revisions := map[string]struct{}{}
	for range 8 {
		set := f.createSet(t, f.owner, "")
		seen[set.ID] = struct{}{}
		revisions[set.Revision] = struct{}{}
		f.expire(t, set)
	}
	require.Len(t, seen, 8)
	require.Len(t, revisions, 8)
	ids := make([]string, 0, len(seen))
	for id := range seen {
		parsed, err := uuid.Parse(id)
		require.NoError(t, err)
		require.Equal(t, uuid.Version(4), parsed.Version())
		ids = append(ids, id)
	}
	require.False(t, slices.Contains(ids, uuid.Nil.String()))
}
