package postgres

import (
	"sync"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/chunker"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func expireUpload(t *testing.T, c *Client, id string, age time.Duration) {
	t.Helper()
	_, err := c.Pool.Exec(t.Context(), `UPDATE source_uploads SET created_at=NOW()-make_interval(secs=>$2),expires_at=NOW()-make_interval(secs=>$2)+make_interval(secs=>$3) WHERE id=$1`,
		id, age.Seconds(), domain.SourceUploadLease.Seconds())
	require.NoError(t, err)
}

func ids(records []SourceUploadRecord) []string {
	out := make([]string, 0, len(records))
	for _, r := range records {
		out = append(out, r.ID)
	}
	return out
}

func TestDueSourceUploadsSelectsOnlyExpiredUnlinkedRecords(t *testing.T) {
	c := jobsDatabase(t)
	fresh, _ := stagedUpload(t, c, "")
	old, _ := stagedUpload(t, c, "")
	young, _ := stagedUpload(t, c, "")
	linked, linkedReq := stagedUpload(t, c, "")
	_, err := c.AcceptDocument(t.Context(), linkedReq)
	require.NoError(t, err)
	foreign, _ := stagedUpload(t, c, "")
	_, err = c.Pool.Exec(t.Context(), `UPDATE source_uploads SET bucket='elsewhere' WHERE id=$1`, foreign.ID)
	require.NoError(t, err)
	expireUpload(t, c, old.ID, 3*time.Hour)
	expireUpload(t, c, young.ID, 30*time.Minute)
	expireUpload(t, c, linked.ID, 3*time.Hour)
	expireUpload(t, c, foreign.ID, 3*time.Hour)

	var before string
	require.NoError(t, c.Pool.QueryRow(t.Context(), `SELECT jsonb_agg(to_jsonb(s) ORDER BY id)::text FROM source_uploads s`).Scan(&before))
	due, err := c.DueSourceUploads(t.Context(), SourceUploadSelection{Bucket: uploadTestBucket, MinAge: time.Hour, Limit: 10})
	require.NoError(t, err)
	require.Equal(t, []string{old.ID}, ids(due), "fresh, young, linked and other-bucket uploads are never due")
	require.Equal(t, old.Key, due[0].Key)
	require.Equal(t, "staged", due[0].State)
	require.False(t, due[0].Now.IsZero())
	var after string
	require.NoError(t, c.Pool.QueryRow(t.Context(), `SELECT jsonb_agg(to_jsonb(s) ORDER BY id)::text FROM source_uploads s`).Scan(&after))
	require.JSONEq(t, before, after, "listing due uploads must not write")
	_ = fresh
}

func TestDueSourceUploadsHonorsTheLimitOldestFirst(t *testing.T) {
	c := jobsDatabase(t)
	var created []string
	for i := range 4 {
		u, _ := stagedUpload(t, c, "")
		expireUpload(t, c, u.ID, time.Duration(10-i)*time.Hour)
		created = append(created, u.ID)
	}
	due, err := c.DueSourceUploads(t.Context(), SourceUploadSelection{Bucket: uploadTestBucket, Limit: 2})
	require.NoError(t, err)
	require.Equal(t, created[:2], ids(due))
}

func TestClaimSourceUploadsFencesAndSchedulesRechecks(t *testing.T) {
	c := jobsDatabase(t)
	u, _ := stagedUpload(t, c, "")
	expireUpload(t, c, u.ID, 3*time.Hour)
	sel := SourceUploadSelection{Bucket: uploadTestBucket, Limit: 10}

	claim := uuid.NewString()
	claimed, err := c.ClaimSourceUploads(t.Context(), sel, claim, time.Hour)
	require.NoError(t, err)
	require.Equal(t, []string{u.ID}, ids(claimed))
	require.Equal(t, "claimed", claimed[0].State)
	row, _ := readUpload(t, c, u.ID)
	require.Equal(t, "claimed", row.State)

	again, err := c.ClaimSourceUploads(t.Context(), sel, uuid.NewString(), time.Hour)
	require.NoError(t, err)
	require.Empty(t, again, "a live claim excludes other runners")

	other := uuid.NewString()
	ok, err := c.DeferSourceUpload(t.Context(), u.ID, other, "stale", time.Minute)
	require.NoError(t, err)
	require.False(t, ok, "only the claim holder records an outcome")
	ok, err = c.DeferSourceUpload(t.Context(), u.ID, claim, "awaiting_settlement", time.Hour)
	require.NoError(t, err)
	require.True(t, ok)
	var checks int
	var reason string
	var next time.Time
	require.NoError(t, c.Pool.QueryRow(t.Context(), `SELECT checks,reason,next_check_at FROM source_uploads WHERE id=$1`, u.ID).Scan(&checks, &reason, &next))
	require.Equal(t, 1, checks)
	require.Equal(t, "awaiting_settlement", reason)
	require.True(t, next.After(time.Now().Add(30*time.Minute)))

	again, err = c.ClaimSourceUploads(t.Context(), sel, uuid.NewString(), time.Hour)
	require.NoError(t, err)
	require.Empty(t, again, "a deferred recheck is not due before its time")

	_, err = c.Pool.Exec(t.Context(), `UPDATE source_uploads SET next_check_at=NOW() WHERE id=$1`, u.ID)
	require.NoError(t, err)
	due, err := c.DueSourceUploads(t.Context(), sel)
	require.NoError(t, err)
	require.Equal(t, []string{u.ID}, ids(due))
	require.Equal(t, 1, due[0].Checks)
	require.Equal(t, "awaiting_settlement", due[0].Reason)
	require.False(t, due[0].ClaimedAt.IsZero(), "the first claim time anchors settlement")
	resumed, err := c.ClaimSourceUploads(t.Context(), sel, uuid.NewString(), time.Hour)
	require.NoError(t, err)
	require.Equal(t, due[0].ClaimedAt, resumed[0].ClaimedAt)
}

func TestSourceUploadSelectionSkipsHandledRecords(t *testing.T) {
	c := jobsDatabase(t)
	first, _ := stagedUpload(t, c, "")
	second, _ := stagedUpload(t, c, "")
	expireUpload(t, c, first.ID, 4*time.Hour)
	expireUpload(t, c, second.ID, 3*time.Hour)
	sel := SourceUploadSelection{Bucket: uploadTestBucket, Limit: 10, Skip: []string{first.ID}}
	due, err := c.DueSourceUploads(t.Context(), sel)
	require.NoError(t, err)
	require.Equal(t, []string{second.ID}, ids(due))
	claimed, err := c.ClaimSourceUploads(t.Context(), sel, uuid.NewString(), time.Hour)
	require.NoError(t, err)
	require.Equal(t, []string{second.ID}, ids(claimed))
	row, _ := readUpload(t, c, first.ID)
	require.Equal(t, "staged", row.State, "a skipped upload is left untouched")
}

func TestAbandonedClaimIsRecoveredAfterItsLease(t *testing.T) {
	c := jobsDatabase(t)
	u, _ := stagedUpload(t, c, "")
	expireUpload(t, c, u.ID, 3*time.Hour)
	sel := SourceUploadSelection{Bucket: uploadTestBucket, Limit: 10}
	_, err := c.ClaimSourceUploads(t.Context(), sel, uuid.NewString(), time.Hour)
	require.NoError(t, err)
	due, err := c.DueSourceUploads(t.Context(), sel)
	require.NoError(t, err)
	require.Empty(t, due)
	_, err = c.Pool.Exec(t.Context(), `UPDATE source_uploads SET claim_until=NOW()-interval '1 second' WHERE id=$1`, u.ID)
	require.NoError(t, err)
	recovered, err := c.ClaimSourceUploads(t.Context(), sel, uuid.NewString(), time.Hour)
	require.NoError(t, err)
	require.Equal(t, []string{u.ID}, ids(recovered))
}

func TestSettleSourceUploadRemovesOnlyTheClaimedRecord(t *testing.T) {
	c := jobsDatabase(t)
	u, _ := stagedUpload(t, c, "")
	expireUpload(t, c, u.ID, 3*time.Hour)
	claim := uuid.NewString()
	_, err := c.ClaimSourceUploads(t.Context(), SourceUploadSelection{Bucket: uploadTestBucket, Limit: 10}, claim, time.Hour)
	require.NoError(t, err)
	ok, err := c.SettleSourceUpload(t.Context(), u.ID, uuid.NewString())
	require.NoError(t, err)
	require.False(t, ok)
	_, present := readUpload(t, c, u.ID)
	require.True(t, present)
	ok, err = c.SettleSourceUpload(t.Context(), u.ID, claim)
	require.NoError(t, err)
	require.True(t, ok)
	_, present = readUpload(t, c, u.ID)
	require.False(t, present)
}

func TestSettleSourceUploadRejectsAnUncertainDispatch(t *testing.T) {
	c := jobsDatabase(t)
	u, _ := stagedUpload(t, c, "")
	require.NoError(t, c.BeginSourceUpload(t.Context(), u.ID))
	require.NoError(t, c.RecordSourceUploadResult(t.Context(), u.ID, domain.SourceUploadResult{VersionID: "retry", Acknowledged: true}))
	expireUpload(t, c, u.ID, 3*time.Hour)
	claim := uuid.NewString()
	records, err := c.ClaimSourceUploads(t.Context(), SourceUploadSelection{Bucket: uploadTestBucket, Limit: 1}, claim, time.Hour)
	require.NoError(t, err)
	require.Len(t, records, 1)
	require.True(t, records[0].Acknowledged)
	require.True(t, records[0].WritePending)
	ok, err := c.SettleSourceUpload(t.Context(), u.ID, claim)
	require.NoError(t, err)
	require.False(t, ok)
}

func TestClaimingNeverRacesAcceptanceToTwoOwners(t *testing.T) {
	c := jobsDatabase(t)
	sel := SourceUploadSelection{Bucket: uploadTestBucket, Limit: 10}
	for range 20 {
		u, req := stagedUpload(t, c, "")
		// Distinct content keeps each round independent of earlier documents.
		req.Document.Content = "race " + u.ID
		req.Fingerprint = chunker.ScopedFingerprint("", req.Document.Content)
		expireUpload(t, c, u.ID, 3*time.Hour)
		var wg sync.WaitGroup
		var accepted domain.AcceptOutcome
		var claimed []SourceUploadRecord
		var acceptErr, claimErr error
		wg.Add(2)
		go func() {
			defer wg.Done()
			result, err := c.AcceptDocument(t.Context(), req)
			accepted, acceptErr = result.Outcome, err
		}()
		go func() {
			defer wg.Done()
			claimed, claimErr = c.ClaimSourceUploads(t.Context(), sel, uuid.NewString(), time.Hour)
		}()
		wg.Wait()
		require.NoError(t, acceptErr)
		require.NoError(t, claimErr)
		row, _ := readUpload(t, c, u.ID)
		switch accepted {
		case domain.AcceptAccepted:
			require.Equal(t, "linked", row.State)
			require.Empty(t, claimed, "a linked upload cannot be claimed")
		case domain.AcceptUploadExpired:
			require.Equal(t, "claimed", row.State)
			require.Equal(t, []string{u.ID}, ids(claimed))
			documents, _, _ := documentCount(t, c, u.DocumentID)
			require.Zero(t, documents)
		default:
			t.Fatalf("unexpected outcome %d", accepted)
		}
	}
}

func TestParallelClaimsAreDisjoint(t *testing.T) {
	c := jobsDatabase(t)
	for range 12 {
		u, _ := stagedUpload(t, c, "")
		expireUpload(t, c, u.ID, 3*time.Hour)
	}
	sel := SourceUploadSelection{Bucket: uploadTestBucket, Limit: 12}
	results := make([][]SourceUploadRecord, 3)
	var wg sync.WaitGroup
	for i := range results {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var err error
			results[i], err = c.ClaimSourceUploads(t.Context(), sel, uuid.NewString(), time.Hour)
			require.NoError(t, err)
		}()
	}
	wg.Wait()
	seen := map[string]bool{}
	for _, claimed := range results {
		for _, id := range ids(claimed) {
			require.False(t, seen[id], "upload %s was claimed twice", id)
			seen[id] = true
		}
	}
	require.Len(t, seen, 12)
}

func TestReferencedSourceKeysFindsDocumentsAndDeletionReceipts(t *testing.T) {
	c := jobsDatabase(t)
	_, req := stagedUpload(t, c, "")
	_, err := c.AcceptDocument(t.Context(), req)
	require.NoError(t, err)
	_, err = c.Pool.Exec(t.Context(), `INSERT INTO document_deletions(document_id,incarnation,kind,source_key,vector_mode) VALUES($1,$2,'quarantine','documents/pending-delete','local')`, uuid.NewString(), uuid.NewString())
	require.NoError(t, err)
	got, err := c.ReferencedSourceKeys(t.Context(), []string{*req.Document.S3Key, "documents/pending-delete", "documents/unreferenced"})
	require.NoError(t, err)
	require.Equal(t, map[string]bool{*req.Document.S3Key: true, "documents/pending-delete": true}, got)
	empty, err := c.ReferencedSourceKeys(t.Context(), nil)
	require.NoError(t, err)
	require.Empty(t, empty)
}

func TestSourceUploadSummariesGroupStatesAndReasons(t *testing.T) {
	c := jobsDatabase(t)
	a, _ := stagedUpload(t, c, "")
	b, _ := stagedUpload(t, c, "")
	held, _ := stagedUpload(t, c, "")
	linked, req := stagedUpload(t, c, "")
	require.NoError(t, c.BeginSourceUpload(t.Context(), b.ID))
	require.NoError(t, c.RecordSourceUploadResult(t.Context(), b.ID, domain.SourceUploadResult{VersionID: "v1", Acknowledged: true, Settled: true}))
	_, err := c.AcceptDocument(t.Context(), req)
	require.NoError(t, err)
	expireUpload(t, c, held.ID, 3*time.Hour)
	claim := uuid.NewString()
	_, err = c.ClaimSourceUploads(t.Context(), SourceUploadSelection{Bucket: uploadTestBucket, Limit: 1}, claim, time.Hour)
	require.NoError(t, err)
	_, err = c.DeferSourceUpload(t.Context(), held.ID, claim, "legal_hold", time.Hour)
	require.NoError(t, err)

	summaries, err := c.SourceUploadSummaries(t.Context(), uploadTestBucket)
	require.NoError(t, err)
	type key struct {
		state, reason string
		acked         bool
	}
	got := map[key]int{}
	for _, s := range summaries {
		got[key{s.State, s.Reason, s.Acknowledged}] += s.Count
		require.False(t, s.Oldest.IsZero())
	}
	require.Equal(t, map[key]int{
		{"staged", "", false}: 1, {"staged", "", true}: 1,
		{"claimed", "legal_hold", false}: 1,
		{"linked", "", true}:             1,
	}, got)
	_, _, _ = a, linked, held
}
