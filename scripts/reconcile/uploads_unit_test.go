package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	pgstore "github.com/Victoroide/quarry-rag/internal/store/postgres"
	"github.com/stretchr/testify/require"
)

func TestSourceUploadOptions(t *testing.T) {
	opts, err := parseOptions([]string{"-source-uploads"}, &bytes.Buffer{})
	require.NoError(t, err)
	require.Equal(t, options{
		Limit: defaultSourceUploadLimit, Page: 100, SourceUploads: true,
		MinAge: time.Hour, Settle: 30 * time.Minute, MaxBytes: 1 << 30, Batch: 100,
	}, opts, "the default is inventory-only with bounded work")
	require.False(t, opts.Apply)

	opts, err = parseOptions([]string{"-source-uploads", "-apply", "-limit=7", "-min-age=0", "-settle=2m", "-max-bytes=10", "-batch=1000"}, &bytes.Buffer{})
	require.NoError(t, err)
	require.True(t, opts.Apply)
	require.Equal(t, 7, opts.Limit)
	require.Zero(t, opts.MinAge)
	require.Equal(t, 2*time.Minute, opts.Settle)
	require.EqualValues(t, 10, opts.MaxBytes)
	require.Equal(t, 1000, opts.Batch)

	vector, err := parseOptions(nil, &bytes.Buffer{})
	require.NoError(t, err)
	require.False(t, vector.SourceUploads)
	require.Equal(t, 100000, vector.Limit, "vector reconciliation keeps its defaults")
}

func TestSourceUploadOptionsRejectInvalidCombinations(t *testing.T) {
	for _, args := range [][]string{
		{"-source-uploads", "-backfill-collections"},
		{"-source-uploads", "-canonical-document-ids"},
		{"-source-uploads", "-after=00000000-0000-4000-8000-000000000001"},
		{"-source-uploads", "-page=10"},
		{"-source-uploads", "-settle=59s"},
		{"-source-uploads", "-min-age=-1s"},
		{"-source-uploads", "-max-bytes=0"},
		{"-source-uploads", "-batch=0"},
		{"-source-uploads", "-batch=1001"},
		{"-source-uploads", "-limit=0"},
		{"-min-age=1h"},
		{"-settle=1h"},
		{"-max-bytes=5"},
		{"-batch=5"},
	} {
		_, err := parseOptions(args, &bytes.Buffer{})
		require.Error(t, err, "%v", args)
	}
}

func TestRecognizedKeyMatchesOnlyGeneratedKeys(t *testing.T) {
	row := pgstore.SourceUploadRecord{ID: "11111111-1111-4111-8111-111111111111", DocumentID: "22222222-2222-4222-8222-222222222222"}
	for key, want := range map[string]bool{
		"documents/reference/" + row.DocumentID + "/" + row.ID + "/report.pdf": true,
		"documents/reference/" + row.DocumentID + "/" + row.ID + "/a/b.pdf":    false,
		"documents/reference/" + row.DocumentID + "/" + row.ID:                 false,
		"documents/reference/" + row.DocumentID + "/other/report.pdf":          false,
		"documents/reference/other/" + row.ID + "/report.pdf":                  false,
		"tenant/reference/" + row.DocumentID + "/" + row.ID + "/report.pdf":    false,
		"documents/" + row.DocumentID + "/" + row.ID + "/report.pdf":           false,
		"": false,
		"documents/reference/" + row.DocumentID + "/" + row.ID + "/../x/y.pdf": false,
	} {
		row.Key = key
		require.Equal(t, want, recognizedKey(row), key)
	}
}

func TestSettlementWaitsForOutstandingWrites(t *testing.T) {
	claimed := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	settle := 30 * time.Minute
	for name, tc := range map[string]struct {
		row  pgstore.SourceUploadRecord
		want bool
	}{
		"an earlier attempt can outlive an acknowledgment": {
			pgstore.SourceUploadRecord{Acknowledged: true, WritePending: true, Checks: 1, ClaimedAt: claimed, Now: claimed.Add(2 * settle)}, false,
		},
		"unacknowledged write after the window": {pgstore.SourceUploadRecord{WritePending: true, Checks: 1, ClaimedAt: claimed, Now: claimed.Add(2 * settle)}, false},
		"first check of a settled write":        {pgstore.SourceUploadRecord{Checks: 0, ClaimedAt: claimed, Now: claimed.Add(2 * settle)}, false},
		"window still open":                     {pgstore.SourceUploadRecord{Checks: 1, ClaimedAt: claimed, Now: claimed.Add(settle - time.Second)}, false},
		"window elapsed after an earlier check": {pgstore.SourceUploadRecord{Checks: 1, ClaimedAt: claimed, Now: claimed.Add(settle)}, true},
	} {
		t.Run(name, func(t *testing.T) { require.Equal(t, tc.want, settlementDue(tc.row, settle)) })
	}
}

func TestRetryDelayBacksOffForEveryReasonButSettlement(t *testing.T) {
	base := 10 * time.Minute
	require.Equal(t, base, retryDelay(reasonAwaitingSettlement, 5, base))
	for _, reason := range []string{
		reasonUnacknowledged, reasonLegalHold, reasonRetention + "2030-01-01T00:00:00Z", reasonVersionsUnlisted,
		reasonVersionsTruncated, reasonVersionIDMissing, reasonVersionSizeInvalid, reasonLockUnknown,
		reasonDeleteFailed, reasonDeleteFailed + ":AccessDenied", reasonByteLimit, reasonReferenced, reasonUnrecognizedKey,
	} {
		require.Equal(t, base, retryDelay(reason, 0, base))
		require.Equal(t, 2*base, retryDelay(reason, 1, base))
		require.Equal(t, base<<maxBackoffSteps, retryDelay(reason, 50, base))
		require.Equal(t, maxRetryDelay, retryDelay(reason, 50, 6*time.Hour))
	}
	require.Zero(t, retryDelay(reasonLegalHold, 9, 0))
}

// clockJournal replays the row transitions that depend on elapsed time for a
// single journal row. Settlement is unconditional, so only the reconciliation
// decision can keep an unresolved write.
type clockJournal struct {
	pendingUploadJournal
	row *pgstore.SourceUploadRecord
	now time.Time
}

func (j *clockJournal) ClaimSourceUploads(_ context.Context, sel pgstore.SourceUploadSelection, _ string, _ time.Duration) ([]pgstore.SourceUploadRecord, error) {
	if j.row == nil || len(sel.Skip) > 0 || j.now.Before(j.row.NextCheckAt) {
		return nil, nil
	}
	j.row.Now = j.now
	if j.row.ClaimedAt.IsZero() {
		j.row.ClaimedAt = j.now
	}
	return []pgstore.SourceUploadRecord{*j.row}, nil
}

func (j *clockJournal) DeferSourceUpload(_ context.Context, _, _, reason string, delay time.Duration) (bool, error) {
	j.row.Checks++
	j.row.Reason = reason
	j.row.NextCheckAt = j.now.Add(delay)
	return true, nil
}

func (j *clockJournal) SettleSourceUpload(context.Context, string, string) (bool, error) {
	j.row = nil
	return true, nil
}

func TestUnresolvedWriteRechecksBackOff(t *testing.T) {
	opts, err := parseOptions([]string{"-source-uploads", "-apply"}, &strings.Builder{})
	require.NoError(t, err)
	bucket := newBucket("unresolved-write", true)
	row := pendingUploadRow(bucket)
	row.CreatedAt = row.Now.Add(-2 * time.Hour)
	row.ExpiresAt = row.CreatedAt.Add(15 * time.Minute)
	row.WritePending = true
	journal := &clockJournal{row: &row, now: row.Now}
	start := journal.now

	const horizon = 30 * 24 * time.Hour
	examined := 0
	for journal.now.Before(start.Add(horizon)) {
		report, err := reconcileSourceUploads(t.Context(), journal, bucket, bucket.name, opts)
		require.NoError(t, err)
		require.Zero(t, report.Settled)
		examined += report.Examined
		journal.now = journal.now.Add(time.Minute)
	}
	require.NotNil(t, journal.row, "an unresolved write never leaves the journal")
	require.Equal(t, reasonUnacknowledged, journal.row.Reason)
	// The pace doubles up to its ceiling, so a month costs a bounded number
	// of exact-key checks instead of one every settle/3.
	ceiling := min(opts.Settle/3<<maxBackoffSteps, maxRetryDelay)
	require.LessOrEqual(t, examined, maxBackoffSteps+1+int(horizon/ceiling)+1)
	// The key is still rechecked at the ceiling, so a late version is found.
	require.GreaterOrEqual(t, examined, int(horizon/ceiling))
}

func FuzzRecognizedKey(f *testing.F) {
	id, doc := "11111111-1111-4111-8111-111111111111", "22222222-2222-4222-8222-222222222222"
	f.Add("documents/reference/"+doc+"/"+id+"/report.pdf", id, doc)
	f.Add("documents/"+doc+"/"+id+"/report.pdf", id, doc)
	f.Add("documents/a/b/c/d/e", "c", "b")
	f.Add("", "", "")
	f.Fuzz(func(t *testing.T, key, uploadID, documentID string) {
		row := pgstore.SourceUploadRecord{ID: uploadID, DocumentID: documentID, Key: key}
		if !recognizedKey(row) {
			return
		}
		parts := strings.Split(key, "/")
		if len(parts) != 5 || parts[0] != "documents" || parts[2] != documentID || parts[3] != uploadID {
			t.Fatalf("accepted %q for document %q and upload %q", key, documentID, uploadID)
		}
	})
}
