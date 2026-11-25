package main

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	pgstore "github.com/Victoroide/quarry-rag/internal/store/postgres"
	"github.com/aws/smithy-go"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

const noteText = "The quarterly archive note describes the retention schedule for scanned originals."

func linkedKeys(t *testing.T, r *rig) []string {
	t.Helper()
	rows, err := r.pg.Pool.Query(t.Context(), `SELECT s3_key FROM documents WHERE s3_key IS NOT NULL ORDER BY s3_key`)
	require.NoError(t, err)
	defer rows.Close()
	var keys []string
	for rows.Next() {
		var key string
		require.NoError(t, rows.Scan(&key))
		keys = append(keys, key)
	}
	require.NoError(t, rows.Err())
	return keys
}

func survivors(r *rig, before []string) []string {
	var kept []string
	linked := map[string]bool{}
	for _, key := range linkedKeys(r.t, r) {
		linked[key] = true
	}
	for _, key := range before {
		if linked[key] {
			kept = append(kept, key)
		}
	}
	return kept
}

func TestSourceUploadCrashPointsAreReclaimed(t *testing.T) {
	pg := newDatabase(t)
	always := func(domain.AcceptRequest) bool { return true }
	for _, tc := range []struct {
		name    string
		arrange func(r *rig, note string)
		objects int
		pending bool
	}{
		{"after staging", func(r *rig, note string) {
			r.bucket.putMode = func(string) putMode { return putStopsBefore }
			require.Equal(t, http.StatusServiceUnavailable, r.upload("", note).Code)
		}, 0, true},
		{"timeout after a successful write", func(r *rig, note string) {
			r.bucket.putMode = func(string) putMode { return putStoresButFail }
			require.Equal(t, http.StatusServiceUnavailable, r.upload("", note).Code)
		}, 1, true},
		{"after the write", func(r *rig, note string) {
			r.store.acknowledge = errCrash
			r.store.stopBefore = always
			require.Equal(t, http.StatusServiceUnavailable, r.upload("", note).Code)
		}, 1, true},
		{"after the SQL commit with the response lost", func(r *rig, note string) {
			r.store.commitThen = errCrash
			require.Equal(t, http.StatusServiceUnavailable, r.upload("", note).Code)
		}, 1, false},
		{"accepted upload", func(r *rig, note string) {
			require.Equal(t, http.StatusAccepted, r.upload("", note).Code)
		}, 1, false},
		{"after another upload wins acceptance", func(r *rig, note string) {
			r.loseRace("", note)
			require.Equal(t, http.StatusAccepted, r.upload("", note).Code, "the loser reports the winner's document")
		}, 2, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := newRig(t, pg, "crash-"+strings.ReplaceAll(tc.name, " ", "-"), false)
			tc.arrange(r, noteText+" Scenario: "+tc.name)
			before := r.bucket.keys()
			require.Len(t, before, tc.objects)
			kept := survivors(r, before)

			r.expire()
			if tc.pending {
				r.reconcile(applyOptions())
				pending := r.reconcile(applyOptions())
				require.Equal(t, reasonUnacknowledged, pending.Uploads[0].Reason)
				require.True(t, pending.Uploads[0].WritePending)
			} else {
				r.quiesce(applyOptions())
			}
			require.ElementsMatch(t, kept, r.bucket.keys(), "every unlinked object is removed and every linked one survives")
			for _, key := range kept {
				var linked int
				require.NoError(t, pg.Pool.QueryRow(t.Context(), `SELECT count(*) FROM source_uploads WHERE object_key=$1 AND state='linked'`, key).Scan(&linked))
				require.Equal(t, 1, linked)
			}
			require.Zero(t, r.states()["staged"])
			require.Equal(t, tc.pending, r.states()["claimed"] == 1)
		})
	}
}

func TestUncertainInFlightCommitPreservesTheOriginal(t *testing.T) {
	r := newRig(t, newDatabase(t), "uncertain-commit", true)
	tx, err := r.pg.Pool.Begin(t.Context())
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(context.WithoutCancel(t.Context())) }()

	var key string
	r.store.stopBefore = func(req domain.AcceptRequest) bool {
		r.expire()
		key = *req.Document.S3Key
		id := string(req.Document.ID)
		_, err := tx.Exec(t.Context(), `INSERT INTO documents(id,content,s3_key) VALUES($1,$2,$3)`, id, req.Document.Content, key)
		require.NoError(t, err)
		_, err = tx.Exec(t.Context(), `UPDATE source_uploads SET state='linked',linked_document_id=$1 WHERE id=$2`, id, req.SourceUpload.ID)
		require.NoError(t, err)
		return true
	}
	// A plain lookup can see no document while adoption holds the row lock.
	// If a handler attempts a direct delete, let the commit finish at that instant.
	r.bucket.failDelete = func(deleting, _ string) error {
		require.Equal(t, key, deleting)
		return tx.Commit(t.Context())
	}
	require.Equal(t, http.StatusServiceUnavailable, r.upload("", noteText).Code)
	require.Equal(t, 1, r.bucket.versionCount(key), "the writer must leave the object for fenced cleanup")
	require.Zero(t, r.reconcile(applyOptions()).Examined, "the open acceptance transaction holds the due row")
	r.bucket.failDelete = nil
	require.NoError(t, tx.Commit(t.Context()))
	require.Zero(t, r.reconcile(applyOptions()).Examined)
	require.Equal(t, []string{key}, linkedKeys(t, r))
	require.Equal(t, map[string]int{"linked": 1}, r.states())
	require.Equal(t, 1, r.bucket.versionCount(key))
}

func TestDelayedStageAcknowledgmentCannotStartAnotherWrite(t *testing.T) {
	r := newRig(t, newDatabase(t), "delayed-stage", true)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	r.store.staged = func(upload domain.SourceUpload) {
		r.expire()
		opts := applyOptions()
		opts.Settle = time.Minute
		require.Equal(t, 1, r.reconcile(opts).Pending)
		_, err := r.pg.Pool.Exec(t.Context(), `UPDATE source_uploads
			SET claimed_at=NOW()-interval '2 hours',next_check_at=NOW() WHERE id=$1`, upload.ID)
		require.NoError(t, err)
		require.Equal(t, 1, r.reconcile(opts).Settled)
		cancel()
	}
	r.bucket.putMode = func(string) putMode { return putStoresButFail }
	require.Equal(t, http.StatusServiceUnavailable, r.uploadWithContext(ctx, "", noteText).Code)
	require.Empty(t, r.states())
	require.Empty(t, r.bucket.keys())
	require.Zero(t, r.bucket.mutations(), "an expired writer must never send a new PUT")
}

func TestDuplicateWithRetriedPutRetainsEveryVersionForCleanup(t *testing.T) {
	r := newRig(t, newDatabase(t), "duplicate-retry", true)
	var loser, heldVersion string
	r.bucket.putMode = func(key string) putMode {
		if loser == "" {
			loser = key
			heldVersion = r.bucket.storeLocked(key, []byte(noteText))
			return putRetryAcknowledged
		}
		return putStores
	}
	r.loseRace("", noteText)
	require.Equal(t, http.StatusAccepted, r.upload("", noteText).Code)
	winner := linkedKeys(t, r)
	require.Len(t, winner, 1)
	require.NotEqual(t, winner[0], loser)
	require.Equal(t, 2, r.bucket.versionCount(loser), "a version can survive even a successful PUT retry")
	require.Equal(t, map[string]int{"linked": 1, "staged": 1}, r.states())
	require.NoError(t, r.bucket.Delete(t.Context(), loser))
	r.bucket.holds[loser+"@"+heldVersion] = true
	r.expire()

	opts := applyOptions()
	opts.Settle = time.Minute
	recheck := func() {
		t.Helper()
		_, err := r.pg.Pool.Exec(t.Context(), `UPDATE source_uploads SET next_check_at=NOW() WHERE object_key=$1`, loser)
		require.NoError(t, err)
	}
	report := r.reconcile(opts)
	require.Equal(t, reasonLegalHold, report.Uploads[0].Reason)
	require.Zero(t, report.DeletedVersions)
	require.Equal(t, 3, r.bucket.versionCount(loser), "the held version protects the whole key")
	delete(r.bucket.holds, loser+"@"+heldVersion)

	recheck()
	report = r.reconcile(opts)
	require.Equal(t, 3, report.DeletedVersions, "both versions and the delete marker are removed")
	require.Zero(t, r.bucket.versionCount(loser))
	recheck()
	report = r.reconcile(opts)
	require.True(t, report.Uploads[0].Acknowledged)
	require.Equal(t, reasonUnacknowledged, report.Uploads[0].Reason)
	require.Zero(t, report.Settled, "earlier requests may still land after an acknowledged retry")

	r.bucket.land(loser)
	recheck()
	require.Equal(t, 1, r.reconcile(opts).DeletedVersions)
	_, err := r.pg.Pool.Exec(t.Context(), `UPDATE source_uploads
		SET claimed_at=NOW()-interval '2 hours', next_check_at=NOW() WHERE object_key=$1`, loser)
	require.NoError(t, err)
	require.Zero(t, r.reconcile(opts).Settled)
	require.Equal(t, winner, r.bucket.keys())
	require.Equal(t, 1, r.bucket.versionCount(winner[0]))
	require.Equal(t, map[string]int{"linked": 1, "claimed": 1}, r.states())
}

func TestLatePutAfterTheFirstCheckIsReconciled(t *testing.T) {
	r := newRig(t, newDatabase(t), "late", false)
	r.bucket.putMode = func(string) putMode { return putLandsLater }
	require.Equal(t, http.StatusServiceUnavailable, r.upload("", noteText).Code)
	require.Empty(t, r.bucket.keys())
	r.expire()

	first := r.reconcile(applyOptions())
	require.Equal(t, 1, first.Pending)
	require.Equal(t, "unacknowledged_write", first.Uploads[0].Reason, "a missing acknowledgment stays pending")
	require.Zero(t, first.Settled)
	require.Equal(t, map[string]int{"claimed": 1}, r.states())

	r.bucket.landAll()
	require.Len(t, r.bucket.keys(), 1, "the write completed after the first check saw nothing")

	second := r.reconcile(applyOptions())
	require.Equal(t, 1, second.DeletedVersions)
	require.Empty(t, r.bucket.keys())
	require.Equal(t, map[string]int{"claimed": 1}, r.states(), "the tombstone stays until a later check is clean")

	third := r.reconcile(applyOptions())
	require.Zero(t, third.Settled)
	require.Equal(t, reasonUnacknowledged, third.Uploads[0].Reason)
	require.Equal(t, map[string]int{"claimed": 1}, r.states())
}

func TestTimedOutPUTAfterSettlementWindow(t *testing.T) {
	pg := newDatabase(t)
	for _, tc := range []struct {
		name                  string
		mode                  putMode
		acknowledged, pending bool
	}{
		{"lost-only-reply", putLandsLater, false, true},
		{"later-retry-acknowledged", putRetryAcknowledged, true, true},
		{"definitely-settled", putStores, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := newRig(t, pg, tc.name, true)
			require.Equal(t, http.StatusAccepted, r.upload("linked-"+tc.name, noteText).Code)
			winner := r.bucket.keys()
			require.Len(t, winner, 1)
			r.store.stopBefore = func(domain.AcceptRequest) bool { return true }
			r.bucket.putMode = func(string) putMode { return tc.mode }
			require.Equal(t, http.StatusServiceUnavailable, r.upload("unlinked", noteText).Code)
			var key string
			require.NoError(t, pg.Pool.QueryRow(t.Context(), `SELECT object_key FROM source_uploads WHERE bucket=$1 AND state='staged'`, r.bucket.name).Scan(&key))
			r.expire()
			opts, err := parseOptions([]string{"-source-uploads", "-apply"}, &strings.Builder{})
			require.NoError(t, err)
			first := r.reconcile(opts)
			require.Len(t, first.Uploads, 1)
			require.Equal(t, tc.acknowledged, first.Uploads[0].Acknowledged)
			require.Zero(t, r.bucket.versionCount(key))

			advance := func(elapsed time.Duration) {
				t.Helper()
				_, err := pg.Pool.Exec(t.Context(), `UPDATE source_uploads SET
					claimed_at=NOW()-make_interval(secs=>$2),next_check_at=NOW() WHERE object_key=$1`, key, elapsed.Seconds())
				require.NoError(t, err)
			}
			advance(opts.Settle + time.Second)
			second := r.reconcile(opts)
			if tc.pending {
				require.Zero(t, second.Settled)
				require.Equal(t, reasonUnacknowledged, second.Uploads[0].Reason)
				require.True(t, second.Uploads[0].WritePending)
				r.bucket.land(key)
				require.Equal(t, 1, r.bucket.versionCount(key))
				advance(48 * time.Hour)
				require.Equal(t, 1, r.reconcile(opts).DeletedVersions, "the late write must remain discoverable")
				advance(49 * time.Hour)
				preview := r.reconcile(inventoryOptions())
				require.Equal(t, reasonUnacknowledged, preview.Uploads[0].Reason)
				require.True(t, preview.Uploads[0].WritePending)
				require.Equal(t, map[string]int{"linked": 1, "claimed": 1}, r.states())
			} else {
				require.Equal(t, 1, second.Settled)
				require.Equal(t, map[string]int{"linked": 1}, r.states())
			}
			require.Equal(t, winner, r.bucket.keys())
			require.Equal(t, 1, r.bucket.versionCount(winner[0]))
		})
	}
}

func TestVersionedBucketsLoseEveryVersionOfAnUnlinkedKey(t *testing.T) {
	r := newRig(t, newDatabase(t), "versioned", true)
	r.bucket.putMode = func(string) putMode { return putStoresButFail }
	require.Equal(t, http.StatusServiceUnavailable, r.upload("", noteText).Code)
	key := r.bucket.keys()[0]
	r.bucket.addVersion(key, []byte("retried write"))
	require.NoError(t, r.bucket.Delete(t.Context(), key), "a plain delete only adds a marker")
	require.Equal(t, 3, r.bucket.versionCount(key))
	plain := r.bucket.plain
	r.expire()

	preview := r.reconcile(inventoryOptions())
	require.Equal(t, "delete", preview.Uploads[0].Action)
	require.Equal(t, 3, preview.Uploads[0].Versions, "the inventory counts the versions behind the marker")
	require.Equal(t, 3, r.bucket.versionCount(key))

	require.Equal(t, 3, r.reconcile(applyOptions()).DeletedVersions)
	require.Equal(t, 1, r.reconcile(applyOptions()).Pending)
	require.Zero(t, r.bucket.versionCount(key), "versions and markers are physically removed")
	require.Equal(t, plain, r.bucket.plain, "cleanup deletes by version and never relies on a delete marker")
	require.Len(t, r.bucket.removed, 3)
}

func TestHeldAndAmbiguousObjectsStayPendingWithTheirReason(t *testing.T) {
	pg := newDatabase(t)
	denied := &smithy.GenericAPIError{Code: "AccessDenied", Message: "denied"}
	for _, tc := range []struct {
		name, reason string
		arrange      func(b *bucketDouble, key string)
		resolve      func(b *bucketDouble, key string)
	}{
		{"legal hold", "legal_hold", func(b *bucketDouble, key string) { b.holds[key+"@null"] = true }, func(b *bucketDouble, key string) { delete(b.holds, key+"@null") }},
		{"retention", "retention_until=", func(b *bucketDouble, key string) { b.retention[key+"@null"] = time.Now().Add(time.Hour) }, func(b *bucketDouble, key string) { delete(b.retention, key+"@null") }},
		{"unlisted versions", "versions_unlisted", func(b *bucketDouble, _ string) { b.failList = denied }, func(b *bucketDouble, _ string) { b.failList = nil }},
		{"unknown lock", "lock_unknown", func(b *bucketDouble, _ string) { b.failLock = denied }, func(b *bucketDouble, _ string) { b.failLock = nil }},
		{"failed delete", "delete_failed:AccessDenied", func(b *bucketDouble, _ string) {
			b.failDelete = func(string, string) error { return denied }
		}, func(b *bucketDouble, _ string) { b.failDelete = nil }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := newRig(t, pg, "held-"+strings.ReplaceAll(tc.name, " ", "-"), false)
			r.store.stopBefore = func(domain.AcceptRequest) bool { return true }
			require.Equal(t, http.StatusServiceUnavailable, r.upload("", noteText).Code)
			key := r.bucket.keys()[0]
			tc.arrange(r.bucket, key)
			r.expire()

			report := r.reconcile(applyOptions())
			require.Equal(t, 1, report.Pending)
			require.Equal(t, "pending", report.Uploads[0].Action)
			require.True(t, strings.HasPrefix(report.Uploads[0].Reason, tc.reason), report.Uploads[0].Reason)
			require.Equal(t, []string{key}, r.bucket.keys(), "a protected or uncertain object is never removed")
			require.Zero(t, r.bucket.removedCount())
			summary := r.reconcile(inventoryOptions())
			found := false
			for _, s := range summary.Journal {
				found = found || (s.State == "claimed" && strings.HasPrefix(s.Reason, tc.reason) && s.Count == 1)
			}
			require.True(t, found, "the journal summary shows %q: %+v", tc.reason, summary.Journal)

			tc.resolve(r.bucket, key)
			r.quiesce(applyOptions())
			require.Empty(t, r.bucket.keys(), "the object is removed once nothing protects it")
		})
	}
}

func TestTruncatedVersionHistoryStaysPending(t *testing.T) {
	r := newRig(t, newDatabase(t), "truncated", true)
	r.bucket.putMode = func(string) putMode { return putStoresButFail }
	require.Equal(t, http.StatusServiceUnavailable, r.upload("", noteText).Code)
	key := r.bucket.keys()[0]
	for range maxInspectedVersions {
		r.bucket.addVersion(key, []byte("retry"))
	}
	r.expire()
	report := r.reconcile(applyOptions())
	require.Equal(t, "versions_truncated", report.Uploads[0].Reason)
	require.Equal(t, maxInspectedVersions+1, r.bucket.versionCount(key), "an incomplete history is never partially removed")
}

func TestOneHeldVersionProtectsTheWholeKey(t *testing.T) {
	r := newRig(t, newDatabase(t), "partial-hold", true)
	r.bucket.putMode = func(string) putMode { return putStoresButFail }
	require.Equal(t, http.StatusServiceUnavailable, r.upload("", noteText).Code)
	key := r.bucket.keys()[0]
	r.bucket.addVersion(key, []byte("second"))
	r.bucket.holds[key+"@v1"] = true
	r.expire()
	report := r.reconcile(applyOptions())
	require.Equal(t, "legal_hold", report.Uploads[0].Reason)
	require.Equal(t, 2, r.bucket.versionCount(key))
}

func TestInventoryPerformsNoWrites(t *testing.T) {
	r := newRig(t, newDatabase(t), "inventory", true)
	r.bucket.putMode = func(string) putMode { return putStoresButFail }
	require.Equal(t, http.StatusServiceUnavailable, r.upload("", noteText).Code)
	r.bucket.putMode = nil
	require.Equal(t, http.StatusAccepted, r.upload("other", noteText).Code)
	var held string
	require.NoError(t, r.pg.Pool.QueryRow(t.Context(), `SELECT object_key FROM source_uploads WHERE state='staged'`).Scan(&held))
	versions, _, err := r.bucket.ListVersions(t.Context(), held, 5)
	require.NoError(t, err)
	r.bucket.holds[held+"@"+versions[0].ID] = true
	r.expire()
	// One upload is claimed and pending under a hold; a second one is still staged.
	require.Equal(t, 1, r.reconcile(applyOptions()).Pending)
	delete(r.bucket.holds, held+"@"+versions[0].ID)
	r.bucket.putMode = func(string) putMode { return putStoresButFail }
	require.Equal(t, http.StatusServiceUnavailable, r.upload("", "Another note that stopped before acceptance.").Code)
	r.bucket.putMode = nil
	r.expire()

	snapshot := func() string {
		var out string
		require.NoError(t, r.pg.Pool.QueryRow(t.Context(), `SELECT (SELECT coalesce(jsonb_agg(to_jsonb(s) ORDER BY id),'[]') FROM source_uploads s)::text || (SELECT coalesce(jsonb_agg(to_jsonb(d)-'incarnation' ORDER BY id),'[]') FROM documents d)::text`).Scan(&out))
		return out
	}
	before, mutations, reads, keys := snapshot(), r.bucket.mutations(), r.bucket.reads, r.bucket.keys()

	report := r.reconcile(inventoryOptions())
	require.Equal(t, "inventory", report.Mode)
	require.Equal(t, mutations, r.bucket.mutations(), "inventory writes and deletes no object")
	require.Greater(t, r.bucket.reads, reads, "inventory inspects the exact keys")
	require.Equal(t, before, snapshot(), "inventory changes no durable state")
	require.Equal(t, keys, r.bucket.keys())
	require.Equal(t, 2, report.Examined)
	require.Zero(t, report.DeletedVersions)
	require.Zero(t, report.Settled)
	for _, u := range report.Uploads {
		require.Equal(t, "delete", u.Action, "both unlinked objects would be removed")
		require.Positive(t, u.Versions)
	}
	require.NotEmpty(t, report.Journal)
}

func TestSameDataInTwoCollectionsKeepsSeparateOwners(t *testing.T) {
	r := newRig(t, newDatabase(t), "collections", false)
	r.store.stopBefore = func(req domain.AcceptRequest) bool { return req.Collection == "alpha" }
	var wg sync.WaitGroup
	codes := map[string]int{}
	var mu sync.Mutex
	for _, collection := range []string{"alpha", "beta"} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			code := r.upload(collection, noteText).Code
			mu.Lock()
			codes[collection] = code
			mu.Unlock()
		}()
	}
	wg.Wait()
	require.Equal(t, map[string]int{"alpha": http.StatusServiceUnavailable, "beta": http.StatusAccepted}, codes)
	require.Len(t, r.bucket.keys(), 2, "each attempt wrote its own object")
	require.Len(t, linkedKeys(t, r), 1)

	r.expire()
	r.quiesce(applyOptions())
	require.Equal(t, linkedKeys(t, r), r.bucket.keys(), "only the interrupted collection's object is reclaimed")
	var collection string
	require.NoError(t, r.pg.Pool.QueryRow(t.Context(), `SELECT collection FROM source_uploads WHERE bucket='collections' AND state='linked'`).Scan(&collection))
	require.Equal(t, "beta", collection)
}

func TestCleanupProtectsReferencedAndUnrelatedObjects(t *testing.T) {
	r := newRig(t, newDatabase(t), "shared", false)
	ctx := t.Context()
	require.Equal(t, http.StatusAccepted, r.upload("", noteText).Code)
	linked := r.bucket.keys()[0]
	_, err := r.pg.Pool.Exec(ctx, `UPDATE documents SET text_source='ocr_pending'`)
	require.NoError(t, err)
	_, err = r.pg.Pool.Exec(ctx, `UPDATE source_uploads SET created_at=NOW()-interval '90 days',expires_at=NOW()-interval '90 days' WHERE state='linked'`)
	require.NoError(t, err)
	var jobs int
	require.NoError(t, r.pg.Pool.QueryRow(ctx, `SELECT count(*) FROM ingest_jobs`).Scan(&jobs))
	require.Equal(t, 1, jobs, "the delayed OCR job still waits")

	r.bucket.putMode = func(string) putMode { return putStoresButFail }
	require.Equal(t, http.StatusServiceUnavailable, r.upload("", "A different note that never reached acceptance.").Code)
	r.bucket.putMode = nil
	var unlinked string
	for _, key := range r.bucket.keys() {
		if key != linked {
			unlinked = key
		}
	}
	require.NotEmpty(t, unlinked)
	neighbors := []string{unlinked + ".bak", unlinked + "2", strings.TrimSuffix(unlinked, "note.txt"), "documents/document/legacy-id/old-format.pdf", "tenant-b/export.pdf", "unrelated"}
	for _, key := range neighbors {
		r.bucket.put(key, []byte("not journaled"))
	}
	legacyID := uuid.NewString()
	_, err = r.pg.Pool.Exec(ctx, `INSERT INTO documents(id,content,s3_key,status) VALUES($1,'legacy','documents/document/legacy-id/old-format.pdf','completed')`, legacyID)
	require.NoError(t, err)
	// A journaled row of another bucket and rows with keys this service never writes.
	foreign := uuid.NewString()
	_, err = r.pg.Pool.Exec(ctx, `INSERT INTO source_uploads(id,document_id,collection,bucket,object_key,expires_at,created_at) VALUES($1,$2,'','other-bucket','documents/document/x/y/z.txt',NOW()-interval '2 hours',NOW()-interval '3 hours')`, foreign, uuid.NewString())
	require.NoError(t, err)
	odd, referenced := uuid.NewString(), uuid.NewString()
	referencedDoc := uuid.NewString()
	referencedKey := "documents/document/" + referencedDoc + "/" + referenced + "/note.txt"
	_, err = r.pg.Pool.Exec(ctx, `INSERT INTO documents(id,content,s3_key,status) VALUES($1,'adopted elsewhere',$2,'completed')`, referencedDoc, referencedKey)
	require.NoError(t, err)
	r.bucket.put(referencedKey, []byte("adopted elsewhere"))
	for id, row := range map[string][2]string{odd: {uuid.NewString(), "tenant-b/export.pdf"}, referenced: {referencedDoc, referencedKey}} {
		_, err = r.pg.Pool.Exec(ctx, `INSERT INTO source_uploads(id,document_id,collection,bucket,object_key,expires_at,created_at) VALUES($1,$2,'','shared',$3,NOW()-interval '2 hours',NOW()-interval '3 hours')`, id, row[0], row[1])
		require.NoError(t, err)
	}
	r.expire()
	before := r.bucket.keys()

	report := r.reconcile(applyOptions())
	reasons := map[string]string{}
	for _, u := range report.Uploads {
		reasons[u.UploadID] = u.Reason
	}
	require.Equal(t, "unrecognized_key", reasons[odd])
	require.Equal(t, "referenced_by_document", reasons[referenced])
	after := r.bucket.keys()
	require.NotContains(t, after, unlinked, "the unlinked journaled object is reclaimed")
	require.ElementsMatch(t, before, append(append([]string{}, after...), unlinked), "nothing else changed")
	require.Contains(t, after, linked)
	for _, key := range neighbors {
		require.Contains(t, after, key)
	}
	var other string
	require.NoError(t, r.pg.Pool.QueryRow(ctx, `SELECT state FROM source_uploads WHERE id=$1`, foreign).Scan(&other))
	require.Equal(t, "staged", other, "another bucket's journal is never claimed")
}

type countingJournal struct {
	*pgstore.Client
	mu       sync.Mutex
	claims   []int
	failDone error
}

func (j *countingJournal) ClaimSourceUploads(ctx context.Context, sel pgstore.SourceUploadSelection, claim string, lease time.Duration) ([]pgstore.SourceUploadRecord, error) {
	rows, err := j.Client.ClaimSourceUploads(ctx, sel, claim, lease)
	j.mu.Lock()
	j.claims = append(j.claims, len(rows))
	j.mu.Unlock()
	return rows, err
}

func (j *countingJournal) DeferSourceUpload(ctx context.Context, id, claim, reason string, retry time.Duration) (bool, error) {
	if j.failDone != nil {
		return false, j.failDone
	}
	return j.Client.DeferSourceUpload(ctx, id, claim, reason, retry)
}

func (j *countingJournal) SettleSourceUpload(ctx context.Context, id, claim string) (bool, error) {
	if j.failDone != nil {
		return false, j.failDone
	}
	return j.Client.SettleSourceUpload(ctx, id, claim)
}

func stageUnlinked(t *testing.T, r *rig, n int) {
	t.Helper()
	prior := r.store.stopBefore
	r.store.stopBefore = func(domain.AcceptRequest) bool { return true }
	defer func() { r.store.stopBefore = prior }()
	for i := range n {
		require.Equal(t, http.StatusServiceUnavailable, r.upload("", noteText+strings.Repeat(" x", i+1)).Code)
	}
	r.expire()
}

func TestMinimumAgeAfterByteLimitedClaim(t *testing.T) {
	r := newRig(t, newDatabase(t), "age-retry", true)
	stageUnlinked(t, r, 1)
	keys := r.bucket.keys()
	require.Len(t, keys, 1)

	initial := applyOptions()
	initial.MinAge = 2 * time.Hour
	initial.MaxBytes = 1
	first := r.reconcile(initial)
	require.Len(t, first.Uploads, 1)
	require.Equal(t, reasonByteLimit, first.Uploads[0].Reason)
	require.Equal(t, 1, r.states()["claimed"])

	inventory, apply := inventoryOptions(), applyOptions()
	inventory.MinAge = 24 * time.Hour
	apply.MinAge = inventory.MinAge
	for _, report := range []uploadReport{r.reconcile(inventory), r.reconcile(apply)} {
		require.Zero(t, report.Examined, "%s must leave the young claimed upload untouched", report.Mode)
		require.Zero(t, report.DeletedVersions)
		require.Zero(t, report.DeletedBytes)
		require.Equal(t, keys, r.bucket.keys())
		require.Equal(t, 1, r.states()["claimed"])
	}

	_, err := r.pg.Pool.Exec(t.Context(), `UPDATE source_uploads SET created_at=NOW()-interval '25 hours' WHERE bucket=$1`, r.bucket.name)
	require.NoError(t, err)
	report := r.reconcile(apply)
	require.Equal(t, 1, report.Examined)
	require.Equal(t, 1, report.DeletedVersions)
	require.Empty(t, r.bucket.keys())
}

func TestCleanupLimitsScansAgeBytesAndBatches(t *testing.T) {
	pg := newDatabase(t)
	t.Run("scan limit", func(t *testing.T) {
		r := newRig(t, pg, "limit-scan", false)
		stageUnlinked(t, r, 3)
		opts := applyOptions()
		opts.Limit = 2
		report := r.reconcile(opts)
		require.Equal(t, 2, report.Examined)
		require.True(t, report.Limited)
		require.Len(t, r.bucket.keys(), 1, "work beyond the limit waits for the next run")
	})
	t.Run("minimum age", func(t *testing.T) {
		r := newRig(t, pg, "limit-age", false)
		stageUnlinked(t, r, 2)
		opts := applyOptions()
		opts.MinAge = 24 * time.Hour
		report := r.reconcile(opts)
		require.Zero(t, report.Examined)
		require.Len(t, r.bucket.keys(), 2)
		require.Equal(t, 2, r.states()["staged"])
	})
	t.Run("batch size", func(t *testing.T) {
		r := newRig(t, pg, "limit-batch", false)
		stageUnlinked(t, r, 3)
		journal := &countingJournal{Client: pg}
		opts := applyOptions()
		opts.Batch = 1
		report, err := reconcileSourceUploads(t.Context(), journal, r.bucket, r.bucket.name, opts)
		require.NoError(t, err)
		require.Equal(t, 3, report.Examined)
		for _, n := range journal.claims {
			require.LessOrEqual(t, n, 1)
		}
		require.GreaterOrEqual(t, len(journal.claims), 3)
	})
	t.Run("byte budget", func(t *testing.T) {
		r := newRig(t, pg, "limit-bytes", false)
		r.bucket.putMode = func(string) putMode { return putStoresButFail }
		for i := range 3 {
			require.Equal(t, http.StatusServiceUnavailable, r.upload("", strings.Repeat("b", 40+i)+" note text").Code)
		}
		r.expire()
		var sizes []int64
		for _, key := range r.bucket.keys() {
			versions, _, err := r.bucket.ListVersions(t.Context(), key, 5)
			require.NoError(t, err)
			sizes = append(sizes, versions[0].Size)
		}
		opts := applyOptions()
		opts.MaxBytes = sizes[0] + sizes[1] + 1
		report := r.reconcile(opts)
		require.LessOrEqual(t, report.DeletedBytes, opts.MaxBytes)
		require.Equal(t, 2, report.DeletedVersions)
		reasons := 0
		for _, u := range report.Uploads {
			if u.Reason == "byte_limit" {
				reasons++
			}
		}
		require.Equal(t, 1, reasons)
		require.Len(t, r.bucket.keys(), 1)
	})
}

func TestInventoryAndApplyShareTheByteBudget(t *testing.T) {
	pg := newDatabase(t)
	verify := func(t *testing.T, r *rig, opts options) uploadReport {
		t.Helper()
		before := r.bucket.mutations()
		inventory := r.reconcile(opts)
		require.Zero(t, inventory.DeletedBytes)
		require.Zero(t, inventory.DeletedVersions)
		require.Equal(t, before, r.bucket.mutations())
		opts.Apply = true
		applied := r.reconcile(opts)
		require.Equal(t, inventory.Uploads, applied.Uploads)
		return applied
	}
	t.Run("oversized version", func(t *testing.T) {
		r := newRig(t, pg, "preview-oversized", false)
		stageUnlinked(t, r, 1)
		opts := inventoryOptions()
		opts.MaxBytes = 1
		report := verify(t, r, opts)
		require.Len(t, report.Uploads, 1)
		require.Equal(t, actionPending, report.Uploads[0].Action)
		require.Equal(t, reasonByteLimit, report.Uploads[0].Reason)
		require.Zero(t, report.Uploads[0].Bytes)
		require.Zero(t, report.DeletedVersions)
	})
	t.Run("partial removal across rows", func(t *testing.T) {
		r := newRig(t, pg, "preview-shared", true)
		stageUnlinked(t, r, 2)
		due, err := pg.DueSourceUploads(t.Context(), pgstore.SourceUploadSelection{Bucket: r.bucket.name, Limit: 2})
		require.NoError(t, err)
		require.Len(t, due, 2)
		young, old := due[1].Key, due[0].Key
		_, err = pg.Pool.Exec(t.Context(), `UPDATE source_uploads SET created_at=NOW()-interval '4 hours'
			WHERE object_key=$1`, old)
		require.NoError(t, err)
		_, err = pg.Pool.Exec(t.Context(), `UPDATE source_uploads SET expires_at=NOW()-interval '3 hours'
			WHERE object_key=$1`, young)
		require.NoError(t, err)
		r.bucket.addVersion(young, []byte("retry"))
		r.bucket.addVersion(old, []byte("copy"))
		opts := inventoryOptions()
		opts.Batch = 2
		opts.MaxBytes = 5
		report := verify(t, r, opts)
		require.Len(t, report.Uploads, 2)
		require.Equal(t, young, report.Uploads[0].Key, "due time takes precedence over age")
		require.Equal(t, 1, report.Uploads[0].Removals)
		require.EqualValues(t, 5, report.Uploads[0].Bytes)
		require.Equal(t, reasonByteLimit, report.Uploads[0].Reason)
		require.Equal(t, old, report.Uploads[1].Key)
		require.Zero(t, report.Uploads[1].Removals)
		require.Equal(t, reasonByteLimit, report.Uploads[1].Reason)
		require.Equal(t, 1, report.DeletedVersions)
		require.EqualValues(t, 5, report.DeletedBytes)
		require.Equal(t, 1, r.bucket.versionCount(young))
		require.Equal(t, 2, r.bucket.versionCount(old))
	})
}

func TestInterruptedCleanupResumesWithoutLosingAnObject(t *testing.T) {
	pg := newDatabase(t)
	t.Run("runner stops after claiming", func(t *testing.T) {
		r := newRig(t, pg, "stop-claim", false)
		stageUnlinked(t, r, 1)
		_, err := pg.ClaimSourceUploads(t.Context(), pgstore.SourceUploadSelection{Bucket: r.bucket.name, Limit: 10}, uuid.NewString(), time.Hour)
		require.NoError(t, err)
		require.Zero(t, r.reconcile(applyOptions()).Examined, "a live claim excludes other runners")
		_, err = pg.Pool.Exec(t.Context(), `UPDATE source_uploads SET claim_until=NOW()-interval '1 second' WHERE bucket=$1`, r.bucket.name)
		require.NoError(t, err)
		r.quiesce(applyOptions())
		require.Empty(t, r.bucket.keys())
	})
	t.Run("delete succeeds but the outcome is not recorded", func(t *testing.T) {
		r := newRig(t, pg, "stop-record", false)
		r.store.stopBefore = func(domain.AcceptRequest) bool { return true }
		require.Equal(t, http.StatusServiceUnavailable, r.upload("", noteText).Code)
		r.expire()
		journal := &countingJournal{Client: pg, failDone: errors.New("connection lost")}
		_, err := reconcileSourceUploads(t.Context(), journal, r.bucket, r.bucket.name, applyOptions())
		require.Error(t, err)
		require.Empty(t, r.bucket.keys(), "the object was removed before the failure")
		_, err = pg.Pool.Exec(t.Context(), `UPDATE source_uploads SET claim_until=NOW()-interval '1 second' WHERE bucket=$1`, r.bucket.name)
		require.NoError(t, err)
		r.quiesce(applyOptions())
		require.Zero(t, r.states()["claimed"])
	})
	t.Run("cleanup claims an upload before acceptance", func(t *testing.T) {
		r := newRig(t, pg, "fenced", false)
		r.store.stopBefore = func(domain.AcceptRequest) bool {
			r.expire()
			report := r.reconcile(applyOptions())
			require.Equal(t, 1, report.DeletedVersions, "the finished write is removed by cleanup")
			return false
		}
		rec := r.upload("", noteText)
		require.Equal(t, http.StatusServiceUnavailable, rec.Code)
		require.Equal(t, "30", rec.Header().Get("Retry-After"))
		require.Contains(t, rec.Body.String(), "retry the upload")
		require.Empty(t, linkedKeys(t, r), "an upload claimed by cleanup cannot become accepted")
		r.store.stopBefore = nil
		r.quiesce(applyOptions())
		require.Empty(t, r.bucket.keys())
		retry := r.upload("", noteText)
		require.Equal(t, http.StatusAccepted, retry.Code, retry.Body.String())
		require.Len(t, r.bucket.keys(), 1)
	})
}
