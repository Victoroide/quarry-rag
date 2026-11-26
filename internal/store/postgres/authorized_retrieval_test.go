package postgres

import (
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/pipeline"
	"github.com/Victoroide/quarry-rag/internal/retriever"
	"github.com/Victoroide/quarry-rag/internal/tracing"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

// retrievalFixture holds two tenants and, in the first, two principals that
// share a label. The hidden documents sit closest to every query, so any
// search that ranked before applying grants would return them first.
type retrievalFixture struct {
	*ownedFixture
	vectors *VectorStore
	reader  *GrantReader

	own, shared, peerOnly, other string
	ownIncarnation               string
}

var retrievalQuery = []float32{1, 0, 0}

func newRetrievalFixture(t *testing.T) *retrievalFixture {
	t.Helper()
	c, vectors := vectorDatabase(t)
	f := &retrievalFixture{ownedFixture: provisionOwnedFixture(t, c), vectors: vectors, reader: &GrantReader{Pool: c.Pool}}
	// Same label, same text and the nearest vector in both hidden documents.
	f.own, f.ownIncarnation = f.complete(t, f.owner, "alpha policy text", []float32{.8, .6, 0})
	f.peerOnly, _ = f.complete(t, f.peer, "alpha policy text", []float32{1, 0, 0})
	f.shared, _ = f.complete(t, f.peer, "shared guide text", []float32{.6, .8, 0})
	f.other, _ = f.complete(t, f.outsider, "alpha policy text", []float32{1, 0, 0})
	f.grantRead(t, f.peer, f.shared, f.owner, true)
	return f
}

// complete accepts, indexes and publishes one owned document.
func (f *retrievalFixture) complete(t *testing.T, owner domain.Principal, text string, embedding []float32) (id, incarnation string) {
	t.Helper()
	req := ownedRequest(owner, "", text)
	req.DocType = "guide"
	require.Equal(t, domain.AcceptAccepted, f.accept(t, req).Outcome)
	job := f.claim(t)
	chunk := domain.Chunk{ID: domain.ChunkID(uuid.NewString()), DocumentID: req.Document.ID, Text: text, TokenCount: 3, Embedding: embedding}
	ok, err := f.c.CompleteJob(t.Context(), *job, []domain.Chunk{chunk}, domain.IndexObservation{})
	require.NoError(t, err)
	require.True(t, ok)
	return string(req.Document.ID), req.Owner.Incarnation
}

func (f *retrievalFixture) grantRead(t *testing.T, owner domain.Principal, document string, reader domain.Principal, read bool) {
	t.Helper()
	var incarnation string
	require.NoError(t, f.c.Pool.QueryRow(t.Context(), `SELECT COALESCE(incarnation,id)::text FROM documents WHERE id=$1`, document).Scan(&incarnation))
	f.importBatch(t, func(b map[string]any) {
		b["grants"] = []grantEntry{{"tenant_id": owner.TenantID, "principal_id": reader.ID, "document_id": document, "incarnation": incarnation, "can_read": read, "can_download": false}}
	})
}

func (f *retrievalFixture) currentRevision(t *testing.T) int64 {
	t.Helper()
	var revision int64
	require.NoError(t, f.c.Pool.QueryRow(t.Context(), `SELECT revision FROM grant_catalog_policy_revision`).Scan(&revision))
	return revision
}

func documentIDs(chunks []domain.ScoredChunk) []string {
	var ids []string
	for _, sc := range chunks {
		ids = append(ids, string(sc.Chunk.DocumentID))
	}
	return ids
}

func TestReadScopeListsOnlyWhatThePrincipalMayReadNow(t *testing.T) {
	f := newRetrievalFixture(t)
	ctx := t.Context()
	derive := func(p domain.Principal, req domain.ReadScopeRequest) domain.ReadScope {
		t.Helper()
		scope, err := f.reader.DeriveReadScope(ctx, p, req)
		require.NoError(t, err)
		return scope
	}
	ids := func(scope domain.ReadScope) []string {
		out := make([]string, 0, len(scope.Documents))
		for _, doc := range scope.Documents {
			out = append(out, doc.ID)
		}
		return out
	}

	all := derive(f.owner, domain.ReadScopeRequest{Membership: 10})
	require.ElementsMatch(t, []string{f.own, f.shared}, ids(all), "owned and granted documents only; identical text elsewhere is invisible")
	require.False(t, all.Empty)
	require.Equal(t, f.currentRevision(t), all.Revision)
	for _, doc := range all.Documents {
		var stored string
		require.NoError(t, f.c.Pool.QueryRow(ctx, `SELECT COALESCE(incarnation,id)::text FROM documents WHERE id=$1`, doc.ID).Scan(&stored))
		require.Equal(t, stored, doc.Incarnation)
	}
	require.Equal(t, []string{f.other}, ids(derive(f.outsider, domain.ReadScopeRequest{Membership: 10})))

	require.Equal(t, []string{f.shared}, ids(derive(f.owner, domain.ReadScopeRequest{Membership: 10, DocumentIDs: []string{f.shared, f.peerOnly, f.other, uuid.NewString()}})),
		"foreign and missing identifiers drop out of an inline list")
	require.True(t, derive(f.owner, domain.ReadScopeRequest{Membership: 10, DocumentIDs: []string{}}).Empty, "an explicit empty list never widens the scope")
	require.True(t, derive(f.owner, domain.ReadScopeRequest{Membership: 10, DocumentIDs: []string{f.peerOnly, f.other}}).Empty)
	require.Equal(t, []string{f.own}, ids(derive(f.owner, domain.ReadScopeRequest{Membership: 10, DocumentIDs: []string{f.own}, Collection: "", DocType: "guide"})))
	require.True(t, derive(f.owner, domain.ReadScopeRequest{Membership: 10, DocType: "report"}).Empty)
	require.True(t, derive(f.owner, domain.ReadScopeRequest{Membership: 10, Collection: "team"}).Empty)

	none := derive(f.owner, domain.ReadScopeRequest{})
	require.False(t, none.Empty, "a zero membership asks only whether anything qualifies")
	require.Nil(t, none.Documents)
	over := derive(f.owner, domain.ReadScopeRequest{Membership: 1})
	require.True(t, over.Exceeded)
	require.Nil(t, over.Documents, "an incomplete listing is never returned")
	require.False(t, derive(f.owner, domain.ReadScopeRequest{Membership: 2}).Exceeded)

	f.grantRead(t, f.peer, f.shared, f.owner, false)
	require.Equal(t, []string{f.own}, ids(derive(f.owner, domain.ReadScopeRequest{Membership: 10})), "a revoked grant leaves at the next read")
	state, err := f.reader.ReadState(ctx, f.owner)
	require.NoError(t, err)
	require.True(t, state.Active)
	require.Equal(t, f.currentRevision(t), state.Revision)

	f.setEnabled(t, f.owner, "owner", false)
	_, err = f.reader.DeriveReadScope(ctx, f.owner, domain.ReadScopeRequest{Membership: 10})
	require.ErrorIs(t, err, domain.ErrReadDenied)
	state, err = f.reader.ReadState(ctx, f.owner)
	require.NoError(t, err)
	require.False(t, state.Active)
	// A principal from another tenant cannot name a tenant it does not belong to.
	state, err = f.reader.ReadState(ctx, domain.Principal{ID: f.owner.ID, TenantID: f.otherTenant})
	require.NoError(t, err)
	require.False(t, state.Active)
}

func TestReadableRelationExcludesUnfinishedReplacedAndDeletedDocuments(t *testing.T) {
	f := newRetrievalFixture(t)
	ctx := t.Context()
	pending := ownedRequest(f.owner, "", "accepted but not indexed")
	require.Equal(t, domain.AcceptAccepted, f.accept(t, pending).Outcome)
	scope, err := f.reader.DeriveReadScope(ctx, f.owner, domain.ReadScopeRequest{Membership: 10})
	require.NoError(t, err)
	require.Len(t, scope.Documents, 2, "an accepted document is not readable before it completes")

	before := f.currentRevision(t)
	grantExec(t, f.c, `DELETE FROM documents WHERE id=$1`, f.own)
	scope, err = f.reader.DeriveReadScope(ctx, f.owner, domain.ReadScopeRequest{Membership: 10})
	require.NoError(t, err)
	require.Len(t, scope.Documents, 1)
	require.Equal(t, f.shared, scope.Documents[0].ID)
	require.Equal(t, f.currentRevision(t), scope.Revision)
	require.Greater(t, scope.Revision, before, "deleting an owned document advances the policy revision")
}

func TestVectorSearchAppliesGrantsBeforeRankingAndLimit(t *testing.T) {
	f := newRetrievalFixture(t)
	ctx := t.Context()
	for name, limit := range map[string]int{"exact": 100, "approximate": 0} {
		t.Run(name, func(t *testing.T) {
			f.vectors.exactScopeMax = limit
			for _, tc := range []struct {
				name     string
				who      domain.Principal
				filter   domain.VectorFilter
				topK     int
				expected []string
			}{
				{"owner takes the nearest readable document", f.owner, domain.VectorFilter{}, 1, []string{f.own}},
				{"owner ranks readable documents only", f.owner, domain.VectorFilter{}, 10, []string{f.own, f.shared}},
				{"peer sees its own pair", f.peer, domain.VectorFilter{}, 10, []string{f.peerOnly, f.shared}},
				{"another tenant sees only its document", f.outsider, domain.VectorFilter{}, 10, []string{f.other}},
				{"an inline list never widens", f.owner, domain.VectorFilter{DocumentIDs: []string{f.peerOnly, f.other}}, 10, nil},
				{"an inline list narrows", f.owner, domain.VectorFilter{DocumentIDs: []string{f.peerOnly, f.shared}}, 10, []string{f.shared}},
				{"type and label narrow further", f.owner, domain.VectorFilter{DocType: "guide", Collection: ""}, 10, []string{f.own, f.shared}},
				{"an unmatched type has no hit", f.owner, domain.VectorFilter{DocType: "report"}, 10, nil},
			} {
				t.Run(tc.name, func(t *testing.T) {
					filter := tc.filter
					filter.Authorized = &domain.AuthorizedScope{TenantID: tc.who.TenantID, PrincipalID: tc.who.ID}
					chunks, exact, err := f.vectors.QueryFilteredWithExactness(ctx, retrievalQuery, tc.topK, filter)
					require.NoError(t, err)
					require.Equal(t, limit > 0, exact)
					require.Equal(t, tc.expected, documentIDs(chunks), "ordered by distance among readable documents")
					for _, sc := range chunks {
						require.NotEmpty(t, sc.ObservedIncarnation)
					}
				})
			}
		})
	}
	f.vectors.exactScopeMax = 0
}

func TestVectorSearchFollowsRevocationAndPrincipalState(t *testing.T) {
	f := newRetrievalFixture(t)
	ctx := t.Context()
	filter := domain.VectorFilter{Authorized: &domain.AuthorizedScope{TenantID: f.owner.TenantID, PrincipalID: f.owner.ID}}
	search := func() []string {
		t.Helper()
		chunks, err := f.vectors.QueryFiltered(ctx, retrievalQuery, 10, filter)
		require.NoError(t, err)
		return documentIDs(chunks)
	}
	require.Equal(t, []string{f.own, f.shared}, search())
	f.grantRead(t, f.peer, f.shared, f.owner, false)
	require.Equal(t, []string{f.own}, search())
	f.setEnabled(t, f.owner, "owner", false)
	require.Empty(t, search(), "a disabled principal reads nothing")
	f.setEnabled(t, f.owner, "owner", true)
	require.Equal(t, []string{f.own}, search())
}

func TestVectorSearchRefusesIncompleteScopes(t *testing.T) {
	f := newRetrievalFixture(t)
	ctx := t.Context()
	for name, scope := range map[string]*domain.AuthorizedScope{
		"no principal":         {TenantID: f.owner.TenantID},
		"no tenant":            {PrincipalID: f.owner.ID},
		"malformed principal":  {TenantID: f.owner.TenantID, PrincipalID: "x'; SELECT 1 --"},
		"principal of another": {TenantID: f.otherTenant, PrincipalID: f.owner.ID},
	} {
		t.Run(name, func(t *testing.T) {
			chunks, err := f.vectors.QueryFiltered(ctx, retrievalQuery, 10, domain.VectorFilter{Authorized: scope})
			require.NoError(t, err)
			require.Empty(t, chunks)
		})
	}
	var store domain.GrantScopedVectorStore = f.vectors
	require.Equal(t, domain.GrantScopeRelation, store.GrantScope())
}

func TestUnscopedVectorSearchIsUnchanged(t *testing.T) {
	f := newRetrievalFixture(t)
	chunks, err := f.vectors.QueryFiltered(t.Context(), retrievalQuery, 10, domain.VectorFilter{})
	require.NoError(t, err)
	require.ElementsMatch(t, []string{f.own, f.peerOnly, f.shared, f.other}, documentIDs(chunks), "service mode keeps its corpus-wide search")
}

func TestSubjectEvidenceChecksRequireCurrentReadableDocuments(t *testing.T) {
	f := newRetrievalFixture(t)
	ctx := t.Context()
	candidates, err := f.vectors.QueryFiltered(ctx, retrievalQuery, 10, domain.VectorFilter{})
	require.NoError(t, err)
	require.Len(t, candidates, 4)
	checks := f.reader.Evidence(f.owner, "", "")

	visible, previews, err := checks.Visibility.VisibleEvidenceWithPreviews(ctx, candidates, []string{f.own, f.peerOnly, f.shared, f.other})
	require.NoError(t, err)
	require.ElementsMatch(t, []string{f.own, f.shared}, documentIDs(visible), "hidden hits are dropped before any use")
	require.NotContains(t, previews, f.peerOnly, "a hidden preview is never read")
	require.NotContains(t, previews, f.other)

	service := Visibility{Pool: f.c.Pool}
	all, _, err := service.VisibleEvidenceWithPreviews(ctx, candidates, nil)
	require.NoError(t, err)
	require.Len(t, all, 4, "service mode does not consult grants")

	docs, err := checks.Documents.VisibleDocumentIDs(ctx, []string{f.own, f.peerOnly, f.shared, f.other})
	require.NoError(t, err)
	require.Len(t, docs, 2)
	require.Contains(t, docs, f.own)
	require.Contains(t, docs, f.shared)
	texts, err := checks.Previews.DocumentPreviews(ctx, []string{f.own, f.peerOnly, f.other})
	require.NoError(t, err)
	for id := range texts {
		require.Equal(t, f.own, id)
	}
	observer, ok := checks.Visibility.(domain.EvidenceSnapshotObserver)
	require.True(t, ok)
	snapshot, err := observer.ObserveEvidenceSnapshot(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, snapshot)

	f.grantRead(t, f.peer, f.shared, f.owner, false)
	visible, _, err = checks.Visibility.VisibleEvidenceWithPreviews(ctx, candidates, nil)
	require.NoError(t, err)
	require.Equal(t, []string{f.own}, documentIDs(visible), "the next check sees the revocation")
}

func TestServingLoginNeedsOnlyTheDocumentedRetrievalPrivileges(t *testing.T) {
	databaseURL := startTestDatabase(t)
	owner := migrationTestClient(t, databaseURL)
	applyMigrationPrefix(t, owner, 10)
	roles := newGrantRoles(t, databaseURL, servingRoles...)
	grantExec(t, owner, roles.sql(`CREATE ROLE {quarry_service}; CREATE ROLE {quarry_operator}; CREATE ROLE {quarry_auditor};
		ALTER DEFAULT PRIVILEGES GRANT EXECUTE ON FUNCTIONS TO {quarry_service}, {quarry_operator};
		ALTER DEFAULT PRIVILEGES GRANT SELECT ON TABLES TO {quarry_service}, {quarry_operator}`))
	require.NoError(t, owner.RunMigrations(t.Context()))
	var inherited bool
	require.NoError(t, owner.Pool.QueryRow(t.Context(), roles.sql(`SELECT has_table_privilege('{quarry_service}','grant_catalog_readable_documents','select')
		OR has_table_privilege('public','grant_catalog_readable_documents','select')
		OR has_function_privilege('{quarry_service}','grant_catalog_read_revision(uuid,uuid)','execute')
		OR has_function_privilege('public','grant_catalog_read_revision(uuid,uuid)','execute')`)).Scan(&inherited))
	require.False(t, inherited, "the new relation and function are not reachable through default privileges")
	grantExec(t, owner, publishedServingSQL(t, roles, "grant-catalog.md", "GRANT USAGE ON SCHEMA public"))

	f := provisionOwnedFixture(t, owner)
	service := grantRoleClient(t, databaseURL, "SET SESSION AUTHORIZATION "+pgx.Identifier{roles.name("quarry_service")}.Sanitize())
	reader := GrantReader{Pool: service.Pool}
	state, err := reader.ReadState(t.Context(), f.owner)
	require.NoError(t, err)
	require.True(t, state.Active)
	scope, err := reader.DeriveReadScope(t.Context(), f.owner, domain.ReadScopeRequest{Membership: 5})
	require.NoError(t, err)
	require.True(t, scope.Empty)
	_, err = service.Pool.Exec(t.Context(), `SELECT 1 FROM grant_catalog_document_access`)
	require.Error(t, err, "the serving login still reads no catalog relation")
	_, err = service.Pool.Exec(t.Context(), `SELECT 1 FROM grant_catalog_document_ownership`)
	require.Error(t, err)
}

func TestAuthorizedPgvectorIncludesTaggedEvidence(t *testing.T) {
	f := newRetrievalFixture(t)
	ctx := t.Context()
	for _, id := range []string{f.own, f.shared, f.peerOnly, f.other} {
		_, err := f.c.Pool.Exec(ctx, `UPDATE documents SET scoped_vectors=TRUE, vector_attempt=$2, vector_incarnation_required=TRUE WHERE id=$1`, id, uuid.NewString())
		require.NoError(t, err)
	}
	wrapped := tracing.WrapVectorStore(f.vectors, "postgresql")
	require.Equal(t, domain.GrantScopeRelation, wrapped.(domain.GrantScopedVectorStore).GrantScope())
	for _, limit := range []int{0, 10} {
		f.vectors.exactScopeMax = limit
		authorized := &domain.AuthorizedScope{TenantID: f.owner.TenantID, PrincipalID: f.owner.ID}
		filter := domain.VectorFilter{Authorized: authorized}
		filtered := wrapped.(domain.FilteredVectorStore)
		hits, err := filtered.QueryFiltered(ctx, retrievalQuery, 10, filter)
		require.NoError(t, err)
		require.Equal(t, []string{f.own, f.shared}, documentIDs(hits))
		ret := retriever.NewRetriever(wrapped, nil, 0.1, 10)
		vis := Visibility{Pool: f.c.Pool, Subject: &f.owner}
		auth := pipeline.NewReadAuthorization(f.owner, time.Now().Add(time.Minute), f.reader, *authorized, f.currentRevision(t))
		qc := &pipeline.QueryContext{
			Authorization: auth, EvidenceVisibility: vis, MaxChunks: 10,
			QueryEmbedding: retrievalQuery, RetrievalMode: string(retriever.RetrievalModeGlobal),
		}
		require.NoError(t, pipeline.NewRetrievalStage(ret).Run(ctx, qc))
		require.Equal(t, []string{f.own, f.shared}, documentIDs(qc.RetrievedChunks))
		filter.Collection = "excluded"
		hits, err = filtered.QueryFiltered(ctx, retrievalQuery, 10, filter)
		require.NoError(t, err)
		require.Empty(t, hits)
	}
}
