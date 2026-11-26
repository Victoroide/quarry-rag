package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/api/handler"
	"github.com/Victoroide/quarry-rag/internal/api/middleware"
	"github.com/Victoroide/quarry-rag/internal/domain"
	pgstore "github.com/Victoroide/quarry-rag/internal/store/postgres"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

type ownedUploadRig struct {
	*rig
	owner domain.Principal
}

func newOwnedUploadRig(t *testing.T, pg *pgstore.Client, bucket string) *ownedUploadRig {
	t.Helper()
	r := &ownedUploadRig{rig: newRig(t, pg, bucket, true), owner: domain.Principal{ID: uuid.NewString(), TenantID: uuid.NewString()}}
	type entry map[string]any
	r.importPolicy(entry{
		"tenants":     []entry{{"tenant_id": r.owner.TenantID}},
		"collections": []entry{{"tenant_id": r.owner.TenantID, "collection": ""}},
		"principals": []entry{{"tenant_id": r.owner.TenantID, "principal_id": r.owner.ID,
			"issuer": "synthetic issuer", "subject": r.owner.ID, "enabled": true}},
		"create_permissions": []entry{{"tenant_id": r.owner.TenantID, "principal_id": r.owner.ID, "collection": "", "can_create": true}},
	})
	return r
}

func (r *ownedUploadRig) importPolicy(batch map[string]any) {
	r.t.Helper()
	var revision int64
	require.NoError(r.t, r.pg.Pool.QueryRow(r.t.Context(), `SELECT revision FROM grant_catalog_policy_revision`).Scan(&revision))
	batch["batch_id"], batch["expected_revision"] = uuid.NewString(), revision
	body, err := json.Marshal(batch)
	require.NoError(r.t, err)
	_, err = r.pg.ImportGrantBatch(r.t.Context(), bytes.NewReader(body), false)
	require.NoError(r.t, err)
}

func (r *ownedUploadRig) uploadOwned(fields ...string) *httptest.ResponseRecorder {
	r.t.Helper()
	ctx := middleware.ContextWithSubject(r.t.Context(), middleware.VerifiedSubject{
		Principal: r.owner, ExpiresAt: time.Now().Add(time.Minute),
	})
	return r.multipartUpload(ctx, "", noteText, []handler.IngestOption{
		handler.WithOwnedIngestion(), handler.WithContentDedup(domain.ContentDedupOff),
	}, fields...)
}

func (r *ownedUploadRig) originalKey() string {
	r.t.Helper()
	keys := r.bucket.keys()
	require.Len(r.t, keys, 1)
	require.Len(r.t, strings.Split(keys[0], "/"), 6)
	require.Zero(r.t, r.bucket.plain)
	return keys[0]
}

func (r *ownedUploadRig) assertAdoption(key string) {
	r.t.Helper()
	parts := strings.Split(key, "/")
	var version string
	require.NoError(r.t, r.pg.Pool.QueryRow(r.t.Context(), `
		SELECT u.version_id FROM source_uploads u
		JOIN documents d ON d.id = u.linked_document_id AND d.s3_key = u.object_key
		JOIN grant_catalog_document_ownership o ON o.document_id = d.id AND o.incarnation = d.incarnation
		WHERE u.id = $1 AND u.state = 'linked' AND d.id = $2 AND d.incarnation = $3
			AND o.tenant_id = $4 AND o.principal_id = $5 AND u.object_key = $6`,
		parts[4], parts[2], parts[3], r.owner.TenantID, r.owner.ID, key).Scan(&version))
	require.Equal(r.t, "v1", version)
	referenced, err := r.pg.ReferencedSourceKeys(r.t.Context(), []string{key})
	require.NoError(r.t, err)
	require.True(r.t, referenced[key])
}

func TestOwnedJournalRefusedAndExpiredReclaimed(t *testing.T) {
	pg := newDatabase(t)
	for _, tc := range []struct {
		name   string
		status int
		state  string
		race   func(*ownedUploadRig, domain.AcceptRequest)
	}{
		{"revoked", http.StatusForbidden, "staged", func(r *ownedUploadRig, _ domain.AcceptRequest) {
			r.importPolicy(map[string]any{"create_permissions": []map[string]any{
				{"tenant_id": r.owner.TenantID, "principal_id": r.owner.ID, "collection": "", "can_create": false},
			}})
		}},
		{"superseded", http.StatusNotFound, "staged", func(r *ownedUploadRig, req domain.AcceptRequest) {
			tag, err := r.pg.Pool.Exec(r.t.Context(), `UPDATE grant_catalog_document_reservations SET state='superseded' WHERE document_id=$1`, string(req.Document.ID))
			require.NoError(r.t, err)
			require.EqualValues(r.t, 1, tag.RowsAffected())
		}},
		{"expired credential", http.StatusUnauthorized, "staged", func(_ *ownedUploadRig, req domain.AcceptRequest) {
			req.Owner.ValidUntil = time.Now().Add(-time.Minute)
		}},
		{"claimed upload", http.StatusServiceUnavailable, "claimed", func(r *ownedUploadRig, req domain.AcceptRequest) {
			r.expire()
			claimed, err := r.pg.ClaimSourceUploads(r.t.Context(), pgstore.SourceUploadSelection{
				Bucket: r.bucket.name, Limit: 1,
			}, uuid.NewString(), 0)
			require.NoError(r.t, err)
			require.Len(r.t, claimed, 1)
			require.Equal(r.t, req.SourceUpload.ID, claimed[0].ID)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := newOwnedUploadRig(t, pg, "owned-refusal")
			id := uuid.NewString()
			r.importPolicy(map[string]any{"reservations": []map[string]any{
				{"tenant_id": r.owner.TenantID, "principal_id": r.owner.ID, "collection": "", "document_id": id},
			}})
			r.store.stopBefore = func(req domain.AcceptRequest) bool {
				require.NotNil(t, req.SourceUpload)
				tc.race(r, req)
				return false
			}
			rec := r.uploadOwned("document_id", id)
			require.Equal(t, tc.status, rec.Code, rec.Body.String())
			key := r.originalKey()
			require.Equal(t, map[string]int{tc.state: 1}, r.states())
			var document, ownership bool
			require.NoError(t, r.pg.Pool.QueryRow(t.Context(), `SELECT EXISTS(SELECT FROM documents WHERE id=$1),
				EXISTS(SELECT FROM grant_catalog_document_ownership WHERE document_id=$1)`, id).Scan(&document, &ownership))
			require.False(t, document)
			require.False(t, ownership)

			r.bucket.addVersion(key, []byte("retried version"))
			r.expire()
			require.Equal(t, 2, r.reconcile(applyOptions()).DeletedVersions)
			require.Equal(t, 1, r.quiesce(applyOptions()).Settled)
			require.Empty(t, r.bucket.keys())
			require.Empty(t, r.states())
		})
	}
}

func TestOwnedJournalCrashAfterWriteReclaimed(t *testing.T) {
	pg := newDatabase(t)
	for _, name := range []string{"crash before acknowledgement", "PUT timeout"} {
		t.Run(name, func(t *testing.T) {
			r := newOwnedUploadRig(t, pg, "owned-"+name)
			if name == "PUT timeout" {
				r.bucket.putMode = func(string) putMode { return putStoresButFail }
			} else {
				r.store.acknowledge = errCrash
			}
			r.store.stopBefore = func(domain.AcceptRequest) bool { return true }
			rec := r.uploadOwned()
			require.Equal(t, http.StatusServiceUnavailable, rec.Code, rec.Body.String())
			key := r.originalKey()
			require.Equal(t, map[string]int{"staged": 1}, r.states())

			opts := applyOptions()
			opts.MinAge = time.Hour
			_, err := r.pg.Pool.Exec(t.Context(), `UPDATE source_uploads SET expires_at=NOW()-interval '1 hour' WHERE object_key=$1`, key)
			require.NoError(t, err)
			require.Zero(t, r.reconcile(opts).Examined, "lease expiry without minimum age is insufficient")
			r.expire()
			require.Equal(t, 1, r.reconcile(opts).DeletedVersions)
			require.Empty(t, r.bucket.keys())
			pending := r.reconcile(opts)
			require.Len(t, pending.Uploads, 1)
			require.Equal(t, reasonUnacknowledged, pending.Uploads[0].Reason)
			require.True(t, pending.Uploads[0].WritePending)
		})
	}
}

func TestOwnedJournalUncertainAcceptanceAdoptedOrReclaimed(t *testing.T) {
	pg := newDatabase(t)
	t.Run("rolled back", func(t *testing.T) {
		r := newOwnedUploadRig(t, pg, "owned-rolled-back")
		// A deferred constraint trigger fails COMMIT after the document,
		// ownership and journal updates have been made in the transaction.
		_, err := r.pg.Pool.Exec(t.Context(), `
			CREATE FUNCTION fail_owned_upload_commit() RETURNS trigger LANGUAGE plpgsql AS $$
			BEGIN RAISE EXCEPTION 'synthetic commit failure'; END;
			$$;
			CREATE CONSTRAINT TRIGGER fail_owned_upload_commit AFTER INSERT ON documents
				DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION fail_owned_upload_commit();`)
		require.NoError(t, err)
		t.Cleanup(func() {
			_, cleanupErr := r.pg.Pool.Exec(context.WithoutCancel(t.Context()), `DROP TRIGGER fail_owned_upload_commit ON documents; DROP FUNCTION fail_owned_upload_commit()`)
			require.NoError(t, cleanupErr)
		})
		rec := r.uploadOwned()
		require.Equal(t, http.StatusServiceUnavailable, rec.Code, rec.Body.String())
		r.originalKey()
		require.Equal(t, map[string]int{"staged": 1}, r.states())
		r.expire()
		require.Equal(t, 1, r.reconcile(applyOptions()).DeletedVersions)
		r.quiesce(applyOptions())
		require.Empty(t, r.bucket.keys())
	})
	t.Run("committed with reply lost", func(t *testing.T) {
		r := newOwnedUploadRig(t, pg, "owned-lost-commit-reply")
		r.store.commitThen = errCrash
		rec := r.uploadOwned()
		require.Equal(t, http.StatusServiceUnavailable, rec.Code, rec.Body.String())
		key := r.originalKey()
		r.assertAdoption(key)
		r.expire()
		require.Zero(t, r.reconcile(applyOptions()).Examined)
		require.Equal(t, []string{key}, r.bucket.keys())
	})
}

func TestOwnedJournalSuccessfulAdoptionSurvivesReclamation(t *testing.T) {
	r := newOwnedUploadRig(t, newDatabase(t), "owned-adopted")
	rec := r.uploadOwned()
	require.Equal(t, http.StatusAccepted, rec.Code, rec.Body.String())
	key := r.originalKey()
	r.assertAdoption(key)

	// A legacy five-segment journal entry and a six-segment orphan can be
	// reclaimed in one pass while the adopted original survives.
	r.store.stopBefore = func(domain.AcceptRequest) bool { return true }
	require.Equal(t, http.StatusServiceUnavailable, r.upload("", noteText+" Unowned.").Code)
	require.Equal(t, http.StatusServiceUnavailable, r.uploadOwned().Code)
	require.Equal(t, map[string]int{"linked": 1, "staged": 2}, r.states())
	r.expire()
	require.Equal(t, 2, r.reconcile(applyOptions()).DeletedVersions)
	r.quiesce(applyOptions())
	require.Equal(t, []string{key}, r.bucket.keys())
	require.Equal(t, map[string]int{"linked": 1}, r.states())
	r.assertAdoption(key)
}
