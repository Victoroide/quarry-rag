package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/api/middleware"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/store/postgres"
	"github.com/Victoroide/quarry-rag/internal/testutil/pgtest"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func newInventoryHandlerFixture() (*scopedMockStore, domain.Principal, string, string) {
	reader := domain.Principal{ID: uuid.NewString(), TenantID: uuid.NewString()}
	owner := domain.Principal{ID: uuid.NewString(), TenantID: reader.TenantID}
	store := newScopedMockStore(owner, reader)
	first := "00000000-0000-4000-8000-000000000001"
	second := "00000000-0000-4000-8000-000000000002"
	for _, id := range []string{first, second} {
		store.docs[id] = &scopedMemoryEntry{
			owner: owner, readers: map[string]bool{reader.ID: true},
			summary: domain.DocumentSummary{ID: id, Status: "completed"},
		}
	}
	return store, reader, first, second
}

func TestScopedInventoryEndsStreamOnPolicyChange(t *testing.T) {
	for _, change := range []string{"revoke", "grant_before_cursor", "disable", "expire", "empty_page", "continuous_churn"} {
		t.Run(change, func(t *testing.T) {
			store, reader, first, second := newInventoryHandlerFixture()
			fixed := time.Now()
			now := fixed
			checks := 0
			store.beforePolicy = func() {
				checks++
				if checks < 2 || (checks > 2 && change != "continuous_churn") {
					return
				}
				switch change {
				case "revoke":
					delete(store.docs[second].readers, reader.ID)
				case "grant_before_cursor":
					id := "00000000-0000-4000-8000-000000000000"
					store.docs[id] = &scopedMemoryEntry{owner: reader, summary: domain.DocumentSummary{ID: id}}
				case "disable":
					store.active[reader.ID] = false
				case "expire":
					now = fixed.Add(time.Minute)
				case "empty_page":
					delete(store.docs, second)
				}
				store.revision++
			}
			h := ListDocumentsHandler(store, WithScopedListing(), withScopedStreamBatchSize(1), withListingClock(func() time.Time { return now }))
			rec := serveScoped(h, http.MethodGet, "/documents", &reader, fixed.Add(time.Minute))
			require.Equal(t, http.StatusOK, rec.Code)
			require.Contains(t, rec.Body.String(), first)
			require.NotContains(t, rec.Body.String(), second)
			require.NotContains(t, rec.Body.String(), `"count":`)
			if change == "continuous_churn" {
				require.Equal(t, 1+maxPolicyRecheckAttempts, checks)
			}
		})
	}
}

func TestScopedInventoryRefreshesPageAndStatus(t *testing.T) {
	for _, status := range []bool{false, true} {
		for _, change := range []string{"revoke", "disable", "continuous_churn"} {
			store, reader, first, _ := newInventoryHandlerFixture()
			checks := 0
			store.beforePolicy = func() {
				checks++
				if checks != 1 && change != "continuous_churn" {
					return
				}
				store.revision++
				if change == "disable" {
					store.active[reader.ID] = false
				}
				delete(store.docs[first].readers, reader.ID)
			}
			h, target := ListDocumentsHandler(store, WithScopedListing()), "/documents?limit=1000"
			if status {
				h, target = DocumentStatusHandler(store, WithScopedStatus()), "/documents/"+first
			}
			rec := serveScoped(h, http.MethodGet, target, &reader, time.Now().Add(time.Minute))
			switch change {
			case "revoke":
				if status {
					require.Equal(t, http.StatusNotFound, rec.Code)
				} else {
					require.Equal(t, http.StatusOK, rec.Code)
					require.Contains(t, rec.Body.String(), `"count":1`)
				}
				require.Equal(t, 2, checks)
			case "disable":
				require.Equal(t, http.StatusForbidden, rec.Code)
				require.Equal(t, 1, checks)
			case "continuous_churn":
				require.Equal(t, http.StatusServiceUnavailable, rec.Code)
				require.Equal(t, maxPolicyRecheckAttempts, checks)
			}
			require.NotContains(t, rec.Body.String(), first)
		}
	}
}

func TestScopedInventoryPolicyReadFailure(t *testing.T) {
	for _, failure := range []error{context.DeadlineExceeded, context.Canceled} {
		store, reader, first, _ := newInventoryHandlerFixture()
		store.policyErr = failure
		for target, handler := range map[string]http.HandlerFunc{
			"/documents":          ListDocumentsHandler(store, WithScopedListing()),
			"/documents?limit=1":  ListDocumentsHandler(store, WithScopedListing()),
			"/documents/" + first: DocumentStatusHandler(store, WithScopedStatus()),
		} {
			rec := serveScoped(handler, http.MethodGet, target, &reader, time.Now().Add(time.Minute))
			require.Equal(t, http.StatusInternalServerError, rec.Code)
			require.NotContains(t, rec.Body.String(), first)
			require.NotContains(t, rec.Body.String(), failure.Error())
		}
	}
}

func TestInventoryAuditUsesCatalogRevision(t *testing.T) {
	store, reader, first, second := newInventoryHandlerFixture()
	store.revision, store.catalogRevision = 2, 45
	audit := &auditMock{}
	list := ListDocumentsHandler(store, WithScopedListing(), WithListingAudit(audit), withScopedStreamBatchSize(1))
	status := DocumentStatusHandler(store, WithScopedStatus(), WithStatusAudit(audit))
	expiry := time.Now().Add(time.Minute)
	require.Equal(t, http.StatusOK, serveScoped(list, http.MethodGet, "/documents?limit=1", &reader, expiry).Code)
	require.Equal(t, http.StatusOK, serveScoped(status, http.MethodGet, "/documents/"+first, &reader, expiry).Code)
	require.Equal(t, http.StatusNotFound, serveScoped(status, http.MethodGet, "/documents/"+uuid.NewString(), &reader, expiry).Code)

	checks := 0
	store.beforePolicy = func() {
		checks++
		if checks == 2 {
			store.revision++
			store.catalogRevision++
		}
	}
	rec := serveScoped(list, http.MethodGet, "/documents", &reader, expiry)
	require.Equal(t, http.StatusOK, rec.Code)
	require.NotContains(t, rec.Body.String(), second)
	require.Len(t, audit.events, 5)
	for i, revision := range []int64{45, 45, 45, 45, 46} {
		require.NotNil(t, audit.events[i].PolicyRevision)
		require.Equal(t, revision, *audit.events[i].PolicyRevision)
	}
	require.Equal(t, "not_found", audit.events[2].Reason)
	require.Equal(t, "policy_changed", audit.events[4].Reason)
}

func TestScopedInventoryStopsOnDisconnect(t *testing.T) {
	store, reader, first, second := newInventoryHandlerFixture()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	checks := 0
	store.beforePolicy = func() {
		checks++
		if checks == 2 {
			cancel()
		}
	}
	req := httptest.NewRequestWithContext(middleware.ContextWithSubject(ctx, middleware.VerifiedSubject{
		Principal: reader, ExpiresAt: time.Now().Add(time.Minute),
	}), http.MethodGet, "/documents", http.NoBody)
	rec := httptest.NewRecorder()
	ListDocumentsHandler(store, WithScopedListing(), withScopedStreamBatchSize(1)).ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, rec.Body.String(), first)
	require.NotContains(t, rec.Body.String(), second)
	require.NotContains(t, rec.Body.String(), `"count":`)
}

type inventoryRecheckStore struct {
	*postgres.Client
	beforePolicy func()
}

func (s inventoryRecheckStore) ScopedPolicyState(ctx context.Context, p domain.Principal) (domain.ScopedPolicyState, error) {
	s.beforePolicy()
	return s.Client.ScopedPolicyState(ctx, p)
}

func TestScopedInventoryIgnoresUnrelatedPolicyCommits(t *testing.T) {
	ctx := t.Context()
	c, err := postgres.NewClient(ctx, pgtest.Start(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close(context.Background()) })
	require.NoError(t, c.RunMigrations(ctx))
	tenant, foreignTenant := uuid.NewString(), uuid.NewString()
	owner, sameTenant, foreignTenantUser := uuid.NewString(), uuid.NewString(), uuid.NewString()
	grantPrincipal := func(tenant, id, subject string, enabled bool) map[string]any {
		return map[string]any{"tenant_id": tenant, "principal_id": id, "issuer": "inventory issuer", "subject": subject, "enabled": enabled}
	}
	catalog := int64(0)
	apply := func(batch map[string]any) {
		t.Helper()
		batch["batch_id"], batch["expected_revision"] = uuid.NewString(), catalog
		data, encodeErr := json.Marshal(batch)
		require.NoError(t, encodeErr)
		result, importErr := c.ImportGrantBatch(ctx, bytes.NewReader(data), false)
		require.NoError(t, importErr)
		catalog++
		require.Equal(t, catalog, result.Revision)
	}
	apply(map[string]any{
		"tenants": []map[string]string{{"tenant_id": tenant}, {"tenant_id": foreignTenant}},
		"principals": []map[string]any{
			grantPrincipal(tenant, owner, "caller", true),
			grantPrincipal(tenant, sameTenant, "same tenant", true),
			grantPrincipal(foreignTenant, foreignTenantUser, "foreign tenant", true),
		},
		"collections": []map[string]string{{"tenant_id": tenant, "collection": ""}, {"tenant_id": tenant, "collection": "archive"}},
	})
	ids := []string{
		"00000000-0000-4000-8000-000000000011", "00000000-0000-4000-8000-000000000012",
		"00000000-0000-4000-8000-000000000013", "00000000-0000-4000-8000-000000000014",
	}
	assignments := make([]map[string]string, 0, len(ids))
	for i, id := range ids {
		collection := ""
		if i == len(ids)-1 {
			collection = "archive"
		}
		_, insertErr := c.Pool.Exec(ctx, `INSERT INTO documents(id,content,status,collection) VALUES($1,'same text','completed',$2)`, id, collection)
		require.NoError(t, insertErr)
		var source string
		require.NoError(t, c.Pool.QueryRow(ctx, `SELECT source_version FROM grant_catalog_source_snapshot($1)`, id).Scan(&source))
		assignments = append(assignments, map[string]string{
			"tenant_id": tenant, "principal_id": owner, "collection": collection, "document_id": id, "source_version": source,
		})
	}
	apply(map[string]any{"assignments": assignments})
	candidates, err := c.BackfillCandidates(ctx, "", 10)
	require.NoError(t, err)
	require.Len(t, candidates, len(ids))
	incarnations := make(map[string]string, len(candidates))
	for _, candidate := range candidates {
		published, publishErr := c.PublishVerifiedDocument(ctx, candidate)
		require.NoError(t, publishErr)
		require.True(t, published)
		incarnations[candidate.DocumentID] = candidate.Incarnation
	}

	checks := 0
	enabled := true
	store := inventoryRecheckStore{Client: c, beforePolicy: func() {
		checks++
		enabled = !enabled
		apply(map[string]any{"principals": []map[string]any{
			grantPrincipal(tenant, sameTenant, "same tenant", enabled),
			grantPrincipal(foreignTenant, foreignTenantUser, "foreign tenant", enabled),
		}})
	}}
	list := ListDocumentsHandler(store, WithScopedListing(), withScopedStreamBatchSize(2))
	status := DocumentStatusHandler(store, WithScopedStatus())
	subject := domain.Principal{TenantID: tenant, ID: owner}
	checkList := func(target string, want []string, wantChecks int) {
		t.Helper()
		checks = 0
		rec := serveScoped(list, http.MethodGet, target, &subject, time.Now().Add(time.Minute))
		require.Equal(t, http.StatusOK, rec.Code)
		var response struct {
			Documents []domain.DocumentSummary `json:"documents"`
			Count     int                      `json:"count"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
		require.Equal(t, len(want), response.Count)
		got := make([]string, 0, len(response.Documents))
		for _, doc := range response.Documents {
			got = append(got, doc.ID)
		}
		require.Equal(t, want, got)
		require.Equal(t, wantChecks, checks)
	}
	checkList("/documents?limit=1000&after="+ids[0], ids[1:], 1)
	checkList("/documents?limit=1000&after="+ids[3], []string{}, 1)
	checkList("/documents?limit=1000&collection=archive", ids[3:], 1)
	checks = 0
	rec := serveScoped(status, http.MethodGet, "/documents/"+ids[0], &subject, time.Now().Add(time.Minute))
	require.Equal(t, http.StatusOK, rec.Code)
	var doc domain.DocumentSummary
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &doc))
	require.Equal(t, ids[0], doc.ID)
	require.Equal(t, 1, checks)
	checkList("/documents", ids, 3)

	grant := func(id string, read bool) map[string]any {
		return map[string]any{"tenant_id": tenant, "principal_id": sameTenant, "document_id": id,
			"incarnation": incarnations[id], "can_read": read, "can_download": false}
	}
	reader := domain.Principal{TenantID: tenant, ID: sameTenant}
	apply(map[string]any{
		"principals": []map[string]any{grantPrincipal(tenant, sameTenant, "same tenant", true)},
		"grants":     []map[string]any{grant(ids[0], true), grant(ids[1], true)},
	})
	checks = 0
	store.beforePolicy = func() {
		checks++
		if checks == 2 {
			apply(map[string]any{"grants": []map[string]any{grant(ids[1], false)}})
		}
	}
	stream := ListDocumentsHandler(store, WithScopedListing(), withScopedStreamBatchSize(1))
	rec = serveScoped(stream, http.MethodGet, "/documents", &reader, time.Now().Add(time.Minute))
	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, rec.Body.String(), ids[0])
	require.NotContains(t, rec.Body.String(), ids[1])
	require.NotContains(t, rec.Body.String(), `"count":`)
	require.Equal(t, 3, checks)
}
