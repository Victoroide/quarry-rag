package postgres

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/chunker"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

type ownedFixture struct {
	c           *Client
	tenant      string
	otherTenant string
	owner       domain.Principal
	peer        domain.Principal
	disabled    domain.Principal
	outsider    domain.Principal
	revision    int64
}

func newOwnedFixture(t *testing.T) *ownedFixture {
	t.Helper()
	return provisionOwnedFixture(t, jobsDatabase(t))
}

func provisionOwnedFixture(t *testing.T, c *Client) *ownedFixture {
	t.Helper()
	f := &ownedFixture{c: c, tenant: uuid.NewString(), otherTenant: uuid.NewString()}
	f.owner = domain.Principal{ID: uuid.NewString(), TenantID: f.tenant}
	f.peer = domain.Principal{ID: uuid.NewString(), TenantID: f.tenant}
	f.disabled = domain.Principal{ID: uuid.NewString(), TenantID: f.tenant}
	f.outsider = domain.Principal{ID: uuid.NewString(), TenantID: f.otherTenant}
	f.importBatch(t, func(b map[string]any) {
		b["tenants"] = []grantEntry{{"tenant_id": f.tenant}, {"tenant_id": f.otherTenant}}
		b["collections"] = []grantEntry{
			{"tenant_id": f.tenant, "collection": ""}, {"tenant_id": f.tenant, "collection": "team"},
			{"tenant_id": f.otherTenant, "collection": ""}, {"tenant_id": f.otherTenant, "collection": "team"},
		}
		b["principals"] = []grantEntry{
			grantPrincipal(f.tenant, f.owner.ID, "owner", true), grantPrincipal(f.tenant, f.peer.ID, "peer", true),
			grantPrincipal(f.tenant, f.disabled.ID, "disabled", false), grantPrincipal(f.otherTenant, f.outsider.ID, "outsider", true),
		}
		b["create_permissions"] = []grantEntry{
			{"tenant_id": f.tenant, "principal_id": f.owner.ID, "collection": "", "can_create": true},
			{"tenant_id": f.tenant, "principal_id": f.owner.ID, "collection": "team", "can_create": true},
			{"tenant_id": f.tenant, "principal_id": f.peer.ID, "collection": "", "can_create": true},
			{"tenant_id": f.tenant, "principal_id": f.disabled.ID, "collection": "", "can_create": true},
			{"tenant_id": f.otherTenant, "principal_id": f.outsider.ID, "collection": "", "can_create": true},
		}
	})
	return f
}

func (f *ownedFixture) importBatch(t *testing.T, fill func(map[string]any)) {
	t.Helper()
	require.NoError(t, f.c.Pool.QueryRow(t.Context(), `SELECT revision FROM grant_catalog_policy_revision`).Scan(&f.revision))
	batch := testGrantBatch(f.revision)
	fill(batch)
	result, err := importTestBatch(t.Context(), f.c, batch, false)
	require.NoError(t, err)
	f.revision = result.Revision
}

func (f *ownedFixture) setCreate(t *testing.T, p domain.Principal, collection string, allowed bool) {
	t.Helper()
	f.importBatch(t, func(b map[string]any) {
		b["create_permissions"] = []grantEntry{{"tenant_id": p.TenantID, "principal_id": p.ID, "collection": collection, "can_create": allowed}}
	})
}

func (f *ownedFixture) setEnabled(t *testing.T, p domain.Principal, subject string, enabled bool) {
	t.Helper()
	f.importBatch(t, func(b map[string]any) {
		b["principals"] = []grantEntry{grantPrincipal(p.TenantID, p.ID, subject, enabled)}
	})
}

func (f *ownedFixture) reserve(t *testing.T, p domain.Principal, collection string) (id, incarnation string) {
	t.Helper()
	id = uuid.NewString()
	f.importBatch(t, func(b map[string]any) {
		b["reservations"] = []grantEntry{{"tenant_id": p.TenantID, "principal_id": p.ID, "collection": collection, "document_id": id}}
	})
	require.NoError(t, f.c.Pool.QueryRow(t.Context(), `SELECT incarnation::text FROM grant_catalog_document_reservations WHERE document_id=$1`, id).Scan(&incarnation))
	return id, incarnation
}

func ownedRequest(p domain.Principal, collection, content string) domain.AcceptRequest {
	return domain.AcceptRequest{
		Document:     domain.Document{ID: domain.DocumentID(uuid.NewString()), Content: content},
		Fingerprint:  chunker.ScopedFingerprint(collection, content),
		Collection:   collection,
		ContentDedup: domain.ContentDedupOff,
		MaxPending:   100,
		Owner:        &domain.OwnerBinding{Principal: p, Incarnation: uuid.NewString(), ValidUntil: time.Now().Add(time.Minute)},
	}
}

func suppliedOwnedRequest(p domain.Principal, collection, content, id, incarnation string) domain.AcceptRequest {
	req := ownedRequest(p, collection, content)
	req.Document.ID, req.CallerSupplied = domain.DocumentID(id), true
	req.Owner.Incarnation = incarnation
	return req
}

type storeCounts struct{ documents, jobs, fingerprints, ownership, assigned int }

func (f *ownedFixture) counts(t *testing.T) storeCounts {
	t.Helper()
	var n storeCounts
	require.NoError(t, f.c.Pool.QueryRow(t.Context(), `SELECT
		(SELECT count(*) FROM documents), (SELECT count(*) FROM ingest_jobs), (SELECT count(*) FROM document_fingerprints),
		(SELECT count(*) FROM grant_catalog_document_ownership), (SELECT count(*) FROM grant_catalog_assigned_incarnations)`).
		Scan(&n.documents, &n.jobs, &n.fingerprints, &n.ownership, &n.assigned))
	return n
}

func (f *ownedFixture) accept(t *testing.T, req domain.AcceptRequest) domain.AcceptResult {
	t.Helper()
	result, err := f.c.AcceptDocument(t.Context(), req)
	require.NoError(t, err)
	return result
}

func (f *ownedFixture) authorize(t *testing.T, p domain.Principal, collection, id string) domain.OwnedTarget {
	t.Helper()
	target, err := f.c.AuthorizeOwnedIngest(t.Context(), p, collection, domain.DocumentID(id))
	require.NoError(t, err)
	return target
}

type ownershipRow struct {
	tenant, principal, origin, collection string
	ready                                 bool
}

func (f *ownedFixture) ownership(t *testing.T, id, incarnation string) (ownershipRow, bool) {
	t.Helper()
	var row ownershipRow
	err := f.c.Pool.QueryRow(t.Context(), `SELECT tenant_id::text, principal_id::text, origin, collection, ready
		FROM grant_catalog_document_ownership WHERE document_id=$1 AND incarnation=$2`, id, incarnation).
		Scan(&row.tenant, &row.principal, &row.origin, &row.collection, &row.ready)
	if errors.Is(err, pgx.ErrNoRows) {
		return row, false
	}
	require.NoError(t, err)
	return row, true
}

func (f *ownedFixture) claim(t *testing.T) *domain.Job {
	t.Helper()
	job, err := f.c.ClaimJob(t.Context(), "worker", time.Minute, 5)
	require.NoError(t, err)
	require.NotNil(t, job)
	return job
}

func chunkFor(id domain.DocumentID, index int) domain.Chunk {
	return domain.Chunk{ID: domain.ChunkID(uuid.NewString()), DocumentID: id, Index: index, Text: fmt.Sprintf("chunk %d", index), TokenCount: 2}
}

func TestOwnedAcceptanceStampsOneOwnerPerIncarnation(t *testing.T) {
	f := newOwnedFixture(t)
	req := ownedRequest(f.owner, "team", "private text")
	result := f.accept(t, req)
	require.Equal(t, domain.AcceptAccepted, result.Outcome)
	require.Equal(t, req.Owner.Incarnation, result.Incarnation)

	row, found := f.ownership(t, string(req.Document.ID), req.Owner.Incarnation)
	require.True(t, found)
	require.Equal(t, ownershipRow{tenant: f.tenant, principal: f.owner.ID, origin: "accepted", collection: "team"}, row, "ownership is stamped from the verified principal and starts quarantined")
	var assigned bool
	var policy, stored string
	require.NoError(t, f.c.Pool.QueryRow(t.Context(), `SELECT grant_catalog_assigned, content_dedup, incarnation::text FROM documents WHERE id=$1`, string(req.Document.ID)).Scan(&assigned, &policy, &stored))
	require.True(t, assigned)
	require.Equal(t, "off", policy)
	require.Equal(t, req.Owner.Incarnation, stored)
	counts := f.counts(t)
	require.Equal(t, storeCounts{documents: 1, jobs: 1, ownership: 1, assigned: 1}, counts, "no shared fingerprint is registered")

	job := f.claim(t)
	require.Equal(t, f.tenant, job.Tenant)
	require.Equal(t, req.Owner.Incarnation, job.Incarnation)
	ok, err := f.c.CompleteJob(t.Context(), *job, []domain.Chunk{chunkFor(req.Document.ID, 0)}, domain.IndexObservation{})
	require.NoError(t, err)
	require.True(t, ok)
	row, _ = f.ownership(t, string(req.Document.ID), req.Owner.Incarnation)
	require.True(t, row.ready, "a completed accepted document publishes its own readiness")
	require.Zero(t, f.counts(t).fingerprints, "completion never registers a shared match")
	assertDocumentAccess(t, f.c, string(req.Document.ID), f.owner.ID, true, true, true)
	assertDocumentAccess(t, f.c, string(req.Document.ID), f.peer.ID, false, false, false)
}

func TestOwnedAcceptanceRefusesSharedIdentity(t *testing.T) {
	f := newOwnedFixture(t)
	req := ownedRequest(f.owner, "", "text")
	req.ContentDedup = domain.ContentDedupCollection
	_, err := f.c.AcceptDocument(t.Context(), req)
	require.ErrorIs(t, err, errOwnedPolicy)
	req = ownedRequest(f.owner, "", "text")
	req.TryLegacyPDFText = true
	_, err = f.c.AcceptDocument(t.Context(), req)
	require.ErrorIs(t, err, errOwnedPolicy)
	req = ownedRequest(f.owner, "", "text")
	_, err = f.c.AcceptExistingPDF(t.Context(), req, true)
	require.ErrorIs(t, err, errOwnedPolicy)
	req = ownedRequest(f.owner, "", "text")
	_, err = f.c.AcceptExistingPDF(t.Context(), req, false)
	require.ErrorIs(t, err, errOwnedPolicy)
	req = suppliedOwnedRequest(f.owner, "", "text", uuid.NewString(), uuid.NewString())
	_, err = f.c.AcceptExistingPDF(t.Context(), req, true)
	require.ErrorIs(t, err, errOwnedPolicy)
	req = ownedRequest(f.owner, "", "text")
	req.EvaluationReceipt = "receipt"
	_, err = f.c.AcceptDocument(t.Context(), req)
	require.ErrorIs(t, err, errOwnedPolicy)
	req = ownedRequest(f.owner, "", "text")
	req.Owner.Incarnation = ""
	_, err = f.c.AcceptDocument(t.Context(), req)
	require.ErrorIs(t, err, errOwnedPolicy)
	req = ownedRequest(f.owner, "", "text")
	req.Owner.ValidUntil = time.Time{}
	_, err = f.c.AcceptDocument(t.Context(), req)
	require.ErrorIs(t, err, errOwnedPolicy)
	require.Equal(t, storeCounts{}, f.counts(t))
}

func TestExpiredCredentialCannotCompleteDurableAcceptance(t *testing.T) {
	f := newOwnedFixture(t)
	reservedID, reservedIncarnation := f.reserve(t, f.owner, "team")

	failedReq := ownedRequest(f.owner, "team", "retry text")
	require.Equal(t, domain.AcceptAccepted, f.accept(t, failedReq).Outcome)
	job := f.claim(t)
	require.NoError(t, f.c.FailJob(t.Context(), *job, "failed"))
	before := f.counts(t)

	expiredNew := ownedRequest(f.owner, "team", "expired new")
	expiredNew.Owner.ValidUntil = time.Now().Add(-time.Second)
	require.Equal(t, domain.AcceptUnauthorized, f.accept(t, expiredNew).Outcome)

	expiredReserved := suppliedOwnedRequest(f.owner, "team", "expired reserved", reservedID, reservedIncarnation)
	expiredReserved.Owner.ValidUntil = time.Now().Add(-time.Millisecond)
	require.Equal(t, domain.AcceptUnauthorized, f.accept(t, expiredReserved).Outcome)

	expiredRetry := suppliedOwnedRequest(f.owner, "team", "retry text", string(failedReq.Document.ID), failedReq.Owner.Incarnation)
	expiredRetry.Owner.ValidUntil = time.Now().Add(-time.Second)
	require.Equal(t, domain.AcceptUnauthorized, f.accept(t, expiredRetry).Outcome)

	require.Equal(t, before, f.counts(t), "expired acceptance writes no document, job or ownership")
	var state, status string
	require.NoError(t, f.c.Pool.QueryRow(t.Context(), `SELECT state FROM grant_catalog_document_reservations WHERE document_id=$1`, reservedID).Scan(&state))
	require.Equal(t, "pending", state, "an expired acceptance leaves the reservation pending")
	require.NoError(t, f.c.Pool.QueryRow(t.Context(), `SELECT status FROM documents WHERE id=$1`, string(failedReq.Document.ID)).Scan(&status))
	require.Equal(t, "failed", status, "an expired retry leaves the failed document unchanged")
}

func TestIdenticalPrivateBytesStayIndependentAcrossPrincipals(t *testing.T) {
	f := newOwnedFixture(t)
	first := f.accept(t, ownedRequest(f.owner, "", "identical bytes"))
	second := f.accept(t, ownedRequest(f.peer, "", "identical bytes"))
	third := f.accept(t, ownedRequest(f.outsider, "", "identical bytes"))
	for _, result := range []domain.AcceptResult{first, second, third} {
		require.Equal(t, domain.AcceptAccepted, result.Outcome)
	}
	require.Equal(t, storeCounts{documents: 3, jobs: 3, ownership: 3, assigned: 3}, f.counts(t))
}

func TestOwnedDecisionsHideEveryUnusableIdentity(t *testing.T) {
	f := newOwnedFixture(t)
	foreign := f.accept(t, ownedRequest(f.peer, "", "peer document"))
	require.Equal(t, domain.AcceptAccepted, foreign.Outcome)
	reservedID, reservedIncarnation := f.reserve(t, f.owner, "")
	unassigned := uuid.NewString()
	grantExec(t, f.c, `INSERT INTO documents(id,content,status) VALUES($1,'legacy','completed')`, unassigned)
	deleted := ownedRequest(f.owner, "", "to delete")
	require.Equal(t, domain.AcceptAccepted, f.accept(t, deleted).Outcome)
	require.NoError(t, f.c.DeleteDocument(t.Context(), deleted.Document.ID))

	for name, id := range map[string]string{
		"missing":                       uuid.NewString(),
		"foreign owner":                 string(foreign.DocumentID),
		"unassigned":                    unassigned,
		"deleted":                       string(deleted.Document.ID),
		"reserved for another subject":  reservedID,
		"reservation in another tenant": reservedID,
	} {
		p := f.owner
		switch name {
		case "reserved for another subject":
			p = f.peer
		case "reservation in another tenant":
			p = f.outsider
		}
		require.Equal(t, domain.OwnedNotFound, f.authorize(t, p, "", id).Decision, name)
	}
	require.Equal(t, domain.OwnedNotFound, f.authorize(t, f.owner, "team", reservedID).Decision, "a reservation is bound to its collection")
	target := f.authorize(t, f.owner, "", reservedID)
	require.Equal(t, domain.OwnedTarget{Decision: domain.OwnedReserved, Incarnation: reservedIncarnation}, target)
	require.Equal(t, domain.OwnedExisting, f.authorize(t, f.peer, "", string(foreign.DocumentID)).Decision)
	require.Equal(t, domain.OwnedNew, f.authorize(t, f.owner, "", "").Decision)
	require.Equal(t, domain.OwnedForbidden, f.authorize(t, f.owner, "unlisted", "").Decision, "an unprovisioned collection cannot be created in")
	require.Equal(t, domain.OwnedForbidden, f.authorize(t, f.peer, "team", "").Decision, "create permission is per collection")
	require.Equal(t, domain.OwnedForbidden, f.authorize(t, f.disabled, "", "").Decision, "a disabled principal is refused")
}

func TestUnusableSuppliedIdentityCausesNoObservableChange(t *testing.T) {
	f := newOwnedFixture(t)
	foreign := f.accept(t, ownedRequest(f.peer, "", "peer document"))
	before := f.counts(t)
	var documentBefore string
	require.NoError(t, f.c.Pool.QueryRow(t.Context(), `SELECT to_jsonb(d)::text FROM documents d WHERE id=$1`, string(foreign.DocumentID)).Scan(&documentBefore))

	for _, id := range []domain.DocumentID{domain.DocumentID(uuid.NewString()), foreign.DocumentID} {
		for _, content := range []string{"peer document", "different content"} {
			req := suppliedOwnedRequest(f.owner, "", content, string(id), uuid.NewString())
			result := f.accept(t, req)
			require.Equal(t, domain.AcceptNotFound, result.Outcome, "the answer never depends on the foreign document's state or content")
		}
	}
	require.Equal(t, before, f.counts(t))
	var documentAfter string
	require.NoError(t, f.c.Pool.QueryRow(t.Context(), `SELECT to_jsonb(d)::text FROM documents d WHERE id=$1`, string(foreign.DocumentID)).Scan(&documentAfter))
	require.Equal(t, documentBefore, documentAfter, "no other document is mutated")
}

func TestReservationIsConsumedOnceAndPinsItsIncarnation(t *testing.T) {
	f := newOwnedFixture(t)
	id, incarnation := f.reserve(t, f.owner, "")
	req := suppliedOwnedRequest(f.owner, "", "reserved text", id, incarnation)
	result := f.accept(t, req)
	require.Equal(t, domain.AcceptAccepted, result.Outcome)
	var stored, state string
	require.NoError(t, f.c.Pool.QueryRow(t.Context(), `SELECT incarnation::text FROM documents WHERE id=$1`, id).Scan(&stored))
	require.Equal(t, incarnation, stored)
	require.NoError(t, f.c.Pool.QueryRow(t.Context(), `SELECT state FROM grant_catalog_document_reservations WHERE document_id=$1`, id).Scan(&state))
	require.Equal(t, "spent", state)

	again := f.accept(t, suppliedOwnedRequest(f.owner, "", "reserved text", id, incarnation))
	require.Equal(t, domain.AcceptInProgress, again.Outcome, "the owner sees the processing document")
	require.Equal(t, storeCounts{documents: 1, jobs: 1, ownership: 1, assigned: 1}, f.counts(t), "at most one job is admitted")
	for _, p := range []domain.Principal{f.peer, f.outsider} {
		require.Equal(t, domain.AcceptNotFound, f.accept(t, suppliedOwnedRequest(p, "", "reserved text", id, incarnation)).Outcome)
	}
	stale := f.accept(t, suppliedOwnedRequest(f.owner, "", "reserved text", id, uuid.NewString()))
	require.Equal(t, domain.AcceptNotFound, stale.Outcome, "an authorization for another incarnation does not apply")
}

func TestPermissionRevokedBeforeDurableAcceptanceStopsIt(t *testing.T) {
	for _, tc := range []struct {
		name   string
		revoke func(*testing.T, *ownedFixture)
	}{
		{"create permission", func(t *testing.T, f *ownedFixture) { f.setCreate(t, f.owner, "team", false) }},
		{"principal disabled", func(t *testing.T, f *ownedFixture) { f.setEnabled(t, f.owner, "owner", false) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newOwnedFixture(t)
			reservedID, reservedIncarnation := f.reserve(t, f.owner, "team")
			require.Equal(t, domain.OwnedNew, f.authorize(t, f.owner, "team", "").Decision, "authorization succeeded earlier")
			tc.revoke(t, f)
			before := f.counts(t)
			require.Equal(t, domain.AcceptForbidden, f.accept(t, ownedRequest(f.owner, "team", "new text")).Outcome)
			require.Equal(t, domain.AcceptForbidden, f.accept(t, suppliedOwnedRequest(f.owner, "team", "reserved", reservedID, reservedIncarnation)).Outcome)
			require.Equal(t, before, f.counts(t))
			var state string
			require.NoError(t, f.c.Pool.QueryRow(t.Context(), `SELECT state FROM grant_catalog_document_reservations WHERE document_id=$1`, reservedID).Scan(&state))
			require.Equal(t, "pending", state, "a refused acceptance does not spend the reservation")
		})
	}
}

func TestAcceptanceHoldsTheCreatePermissionUntilCommit(t *testing.T) {
	f := newOwnedFixture(t)
	ctx := t.Context()
	tx, err := f.c.Pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	var permitted bool
	require.NoError(t, tx.QueryRow(ctx, `SELECT grant_catalog_lock_ingest_permission($1::uuid,$2::uuid,'')`, f.owner.TenantID, f.owner.ID).Scan(&permitted))
	require.True(t, permitted)

	other, err := f.c.Pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = other.Rollback(context.WithoutCancel(ctx)) }()
	_, err = other.Exec(ctx, `SET LOCAL lock_timeout='200ms'`)
	require.NoError(t, err)
	_, err = other.Exec(ctx, `UPDATE grant_catalog_create_permissions SET can_create=false WHERE principal_id=$1 AND collection=''`, f.owner.ID)
	require.ErrorContains(t, err, "lock timeout", "a revocation waits for the acceptance transaction")
}

func TestSimultaneousAcceptsOfOneReservationAdmitOneJob(t *testing.T) {
	f := newOwnedFixture(t)
	id, incarnation := f.reserve(t, f.owner, "")
	const callers = 6
	outcomes := make(chan domain.AcceptOutcome, callers)
	var wg sync.WaitGroup
	for range callers {
		wg.Go(func() {
			result, err := f.c.AcceptDocument(t.Context(), suppliedOwnedRequest(f.owner, "", "reserved text", id, incarnation))
			if err != nil {
				t.Error(err)
				return
			}
			outcomes <- result.Outcome
		})
	}
	wg.Wait()
	close(outcomes)
	accepted, inProgress := 0, 0
	for outcome := range outcomes {
		switch outcome {
		case domain.AcceptAccepted:
			accepted++
		case domain.AcceptInProgress:
			inProgress++
		default:
			t.Errorf("unexpected outcome %d", outcome)
		}
	}
	require.Equal(t, 1, accepted)
	require.Equal(t, callers-1, inProgress)
	require.Equal(t, storeCounts{documents: 1, jobs: 1, ownership: 1, assigned: 1}, f.counts(t))
}

func TestOwnedRetryHonorsQueueCapacityAndConflicts(t *testing.T) {
	f := newOwnedFixture(t)
	req := ownedRequest(f.owner, "", "retry text")
	req.MaxPending = 1
	require.Equal(t, domain.AcceptAccepted, f.accept(t, req).Outcome)
	job := f.claim(t)
	require.NoError(t, f.c.FailJob(t.Context(), *job, "provider failed"))
	filler := ownedRequest(f.peer, "", "filler")
	filler.MaxPending = 1
	require.Equal(t, domain.AcceptAccepted, f.accept(t, filler).Outcome)

	retry := suppliedOwnedRequest(f.owner, "", "retry text", string(req.Document.ID), req.Owner.Incarnation)
	retry.MaxPending = 1
	require.Equal(t, domain.AcceptQueueFull, f.accept(t, retry).Outcome, "an authorized retry at capacity is refused")
	var status string
	require.NoError(t, f.c.Pool.QueryRow(t.Context(), `SELECT status FROM documents WHERE id=$1`, string(req.Document.ID)).Scan(&status))
	require.Equal(t, "failed", status)

	conflicting := suppliedOwnedRequest(f.owner, "", "other text", string(req.Document.ID), req.Owner.Incarnation)
	require.Equal(t, domain.AcceptConflict, f.accept(t, conflicting).Outcome, "only the owner sees a content conflict")
	elsewhere := suppliedOwnedRequest(f.owner, "team", "retry text", string(req.Document.ID), req.Owner.Incarnation)
	require.Equal(t, domain.AcceptConflict, f.accept(t, elsewhere).Outcome)

	drain := f.claim(t)
	require.NoError(t, f.c.FailJob(t.Context(), *drain, "drained"))
	retry.MaxPending = 5
	require.Equal(t, domain.AcceptRequeued, f.accept(t, retry).Outcome)
	require.Equal(t, domain.AcceptInProgress, f.accept(t, retry).Outcome)
	var jobs int
	require.NoError(t, f.c.Pool.QueryRow(t.Context(), `SELECT count(*) FROM ingest_jobs WHERE document_id=$1`, string(req.Document.ID)).Scan(&jobs))
	require.Equal(t, 1, jobs, "a retry admits at most one job")
}

func TestOwnedRetryRevalidatesPermissionAndNeverRegistersSharedMatches(t *testing.T) {
	f := newOwnedFixture(t)
	req := ownedRequest(f.owner, "", "retry text")
	require.Equal(t, domain.AcceptAccepted, f.accept(t, req).Outcome)
	job := f.claim(t)
	require.NoError(t, f.c.FailJob(t.Context(), *job, "failed"))
	f.setCreate(t, f.owner, "", false)
	retry := suppliedOwnedRequest(f.owner, "", "retry text", string(req.Document.ID), req.Owner.Incarnation)
	require.Equal(t, domain.AcceptForbidden, f.accept(t, retry).Outcome, "a retry needs current create permission")
	f.setCreate(t, f.owner, "", true)
	require.Equal(t, domain.AcceptRequeued, f.accept(t, retry).Outcome)
	require.Zero(t, f.counts(t).fingerprints)
}

func TestRetiredIncarnationCannotPublishAfterDeletionAndUUIDReuse(t *testing.T) {
	f := newOwnedFixture(t)
	req := ownedRequest(f.owner, "", "first life")
	require.Equal(t, domain.AcceptAccepted, f.accept(t, req).Outcome)
	stale := f.claim(t)
	require.NoError(t, f.c.DeleteDocument(t.Context(), req.Document.ID))
	_, found := f.ownership(t, string(req.Document.ID), req.Owner.Incarnation)
	require.False(t, found, "deletion removes the ownership of that incarnation")

	grantExec(t, f.c, `INSERT INTO documents(id,content,status) VALUES($1,'replacement','processing')`, string(req.Document.ID))
	grantExec(t, f.c, `INSERT INTO ingest_jobs(document_id) VALUES($1)`, string(req.Document.ID))
	var retiredStored, identifierUsed bool
	require.NoError(t, f.c.Pool.QueryRow(t.Context(), `SELECT EXISTS(SELECT 1 FROM documents WHERE id=$1 AND COALESCE(incarnation,id)=$2::uuid),
		EXISTS(SELECT 1 FROM documents WHERE id=$1)`, string(req.Document.ID), stale.Incarnation).Scan(&retiredStored, &identifierUsed))
	require.False(t, retiredStored, "the retired incarnation is gone even though the identifier exists")
	require.True(t, identifierUsed)

	ok, err := f.c.CompleteJob(t.Context(), *stale, []domain.Chunk{chunkFor(req.Document.ID, 0)}, domain.IndexObservation{})
	require.NoError(t, err)
	require.False(t, ok, "a retired lease cannot publish into the replacement")
	ok, err = f.c.HeartbeatJob(t.Context(), *stale, time.Minute)
	require.NoError(t, err)
	require.False(t, ok)
	var chunks int
	require.NoError(t, f.c.Pool.QueryRow(t.Context(), `SELECT count(*) FROM chunks WHERE document_id=$1`, string(req.Document.ID)).Scan(&chunks))
	require.Zero(t, chunks)

	require.Equal(t, domain.OwnedNotFound, f.authorize(t, f.owner, "", string(req.Document.ID)).Decision, "the former owner holds no claim on the reused identifier")
	require.Equal(t, domain.AcceptNotFound, f.accept(t, suppliedOwnedRequest(f.owner, "", "first life", string(req.Document.ID), req.Owner.Incarnation)).Outcome)
	var count int
	require.NoError(t, f.c.Pool.QueryRow(t.Context(), `SELECT count(*) FROM grant_catalog_assigned_incarnations WHERE document_id=$1 AND incarnation=$2`, string(req.Document.ID), req.Owner.Incarnation).Scan(&count))
	require.Equal(t, 1, count, "the retired pair stays recorded so it cannot be assigned again")
}

func TestAssignedDocumentsNeverRegisterSharedMatchesOnCompletion(t *testing.T) {
	f := newOwnedFixture(t)
	register := func(t *testing.T, assign bool) int {
		t.Helper()
		legacy := jobsRequest("legacy " + uuid.NewString())
		legacy.ContentDedup = domain.ContentDedupCollection
		require.Equal(t, domain.AcceptAccepted, f.accept(t, legacy).Outcome)
		grantExec(t, f.c, `DELETE FROM document_fingerprints WHERE document_id=$1`, string(legacy.Document.ID))
		if assign {
			f.importBatch(t, func(b map[string]any) {
				b["assignments"] = []grantEntry{grantAssignment(t, f.c, f.tenant, f.owner.ID, string(legacy.Document.ID))}
			})
		}
		job := f.claim(t)
		require.Equal(t, string(legacy.Document.ID), string(job.Document.ID))
		ok, err := f.c.CompleteJob(t.Context(), *job, []domain.Chunk{chunkFor(legacy.Document.ID, 0)}, domain.IndexObservation{})
		require.NoError(t, err)
		require.True(t, ok)
		var registered int
		require.NoError(t, f.c.Pool.QueryRow(t.Context(), `SELECT count(*) FROM document_fingerprints WHERE document_id=$1`, string(legacy.Document.ID)).Scan(&registered))
		if assign {
			var origin string
			var ready bool
			require.NoError(t, f.c.Pool.QueryRow(t.Context(), `SELECT origin, ready FROM grant_catalog_document_ownership WHERE document_id=$1`, string(legacy.Document.ID)).Scan(&origin, &ready))
			require.Equal(t, "imported", origin)
			require.False(t, ready, "a historical assignment stays quarantined until verified")
		}
		return registered
	}
	require.Equal(t, 1, register(t, false), "service-mode completion keeps its shared registration")
	require.Zero(t, register(t, true), "a legacy retry cannot register a shared match once assigned")
}

func TestAcceptedDocumentDeletionRemovesOwnershipAndAudits(t *testing.T) {
	f := newOwnedFixture(t)
	req := ownedRequest(f.owner, "", "doomed")
	require.Equal(t, domain.AcceptAccepted, f.accept(t, req).Outcome)
	require.NoError(t, f.c.DeleteDocument(t.Context(), req.Document.ID))
	require.Equal(t, storeCounts{assigned: 1}, f.counts(t))
}
