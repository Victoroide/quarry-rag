package postgres

import (
	"strings"
	"sync"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/chunker"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

const uploadTestBucket = "originals"

func stagedUpload(t *testing.T, c *Client, collection string) (domain.SourceUpload, domain.AcceptRequest) {
	t.Helper()
	docID := uuid.NewString()
	upload := domain.SourceUpload{ID: uuid.NewString(), DocumentID: domain.DocumentID(docID), Collection: collection, Bucket: uploadTestBucket}
	upload.Key = "documents/document/" + docID + "/" + upload.ID + "/scan.pdf"
	require.NoError(t, c.StageSourceUpload(t.Context(), upload))
	key := upload.Key
	req := domain.AcceptRequest{
		Document:    domain.Document{ID: upload.DocumentID, Content: "same bytes", S3Key: &key},
		Fingerprint: chunker.ScopedFingerprint(collection, "same bytes"),
		Collection:  collection, MaxPending: 1000,
		SourceUpload: &domain.SourceUploadLink{ID: upload.ID, VersionID: "v1"},
	}
	return upload, req
}

type uploadRow struct {
	State, Collection, Version string
	Linked                     *string
	Acknowledged               bool
}

func readUpload(t *testing.T, c *Client, id string) (uploadRow, bool) {
	t.Helper()
	var row uploadRow
	var version *string
	err := c.Pool.QueryRow(t.Context(), `SELECT state,collection,version_id,linked_document_id::text,acknowledged_at IS NOT NULL FROM source_uploads WHERE id=$1`, id).
		Scan(&row.State, &row.Collection, &version, &row.Linked, &row.Acknowledged)
	if err != nil {
		return row, false
	}
	if version != nil {
		row.Version = *version
	}
	return row, true
}

func documentCount(t *testing.T, c *Client, id domain.DocumentID) (documents, jobs, fingerprints int) {
	t.Helper()
	require.NoError(t, c.Pool.QueryRow(t.Context(), `SELECT
		(SELECT count(*) FROM documents WHERE id=$1),
		(SELECT count(*) FROM ingest_jobs WHERE document_id=$1),
		(SELECT count(*) FROM document_fingerprints WHERE document_id=$1)`, string(id)).Scan(&documents, &jobs, &fingerprints))
	return documents, jobs, fingerprints
}

func TestAcceptanceLinksStagedUploadAtomically(t *testing.T) {
	c := jobsDatabase(t)
	upload, req := stagedUpload(t, c, "archive")
	row, ok := readUpload(t, c, upload.ID)
	require.True(t, ok)
	require.Equal(t, uploadRow{State: "staged", Collection: "archive"}, row)

	result, err := c.AcceptDocument(t.Context(), req)
	require.NoError(t, err)
	require.Equal(t, domain.AcceptAccepted, result.Outcome)
	row, _ = readUpload(t, c, upload.ID)
	require.Equal(t, "linked", row.State)
	require.Equal(t, "v1", row.Version)
	require.True(t, row.Acknowledged)
	require.NotNil(t, row.Linked)
	require.Equal(t, string(upload.DocumentID), *row.Linked)
	stored, err := c.GetDocumentS3Key(t.Context(), upload.DocumentID)
	require.NoError(t, err)
	require.Equal(t, upload.Key, *stored)
}

func TestClaimedUploadCannotBecomeAccepted(t *testing.T) {
	c := jobsDatabase(t)
	upload, req := stagedUpload(t, c, "")
	_, err := c.Pool.Exec(t.Context(), `UPDATE source_uploads SET state='claimed',claim=gen_random_uuid(),claimed_at=NOW() WHERE id=$1`, upload.ID)
	require.NoError(t, err)

	result, err := c.AcceptDocument(t.Context(), req)
	require.NoError(t, err)
	require.Equal(t, domain.AcceptUploadExpired, result.Outcome)
	documents, jobs, fingerprints := documentCount(t, c, upload.DocumentID)
	require.Zero(t, documents+jobs+fingerprints, "fenced acceptance must leave no partial state")
	row, _ := readUpload(t, c, upload.ID)
	require.Equal(t, "claimed", row.State)
	require.Nil(t, row.Linked)
}

func TestUploadLinkRequiresMatchingOwner(t *testing.T) {
	for name, mutate := range map[string]func(*domain.AcceptRequest){
		"other collection": func(r *domain.AcceptRequest) {
			r.Collection = "other"
			r.Fingerprint = chunker.ScopedFingerprint("other", "same bytes")
		},
		"other document": func(r *domain.AcceptRequest) { r.Document.ID = domain.DocumentID(uuid.NewString()) },
		"other object": func(r *domain.AcceptRequest) {
			other := "documents/document/elsewhere/file.pdf"
			r.Document.S3Key = &other
		},
		"unknown upload": func(r *domain.AcceptRequest) { r.SourceUpload = &domain.SourceUploadLink{ID: uuid.NewString()} },
	} {
		t.Run(name, func(t *testing.T) {
			c := jobsDatabase(t)
			upload, req := stagedUpload(t, c, "archive")
			mutate(&req)
			result, err := c.AcceptDocument(t.Context(), req)
			require.NoError(t, err)
			require.Equal(t, domain.AcceptUploadExpired, result.Outcome)
			documents, _, _ := documentCount(t, c, req.Document.ID)
			require.Zero(t, documents)
			row, _ := readUpload(t, c, upload.ID)
			require.Equal(t, "staged", row.State)
		})
	}
}

func TestUploadLinkNeedsAnObjectKey(t *testing.T) {
	c := jobsDatabase(t)
	_, req := stagedUpload(t, c, "")
	req.Document.S3Key = nil
	_, err := c.AcceptDocument(t.Context(), req)
	require.Error(t, err)
}

func TestSameBytesInTwoCollectionsKeepSeparateOwners(t *testing.T) {
	c := jobsDatabase(t)
	first, firstReq := stagedUpload(t, c, "alpha")
	second, secondReq := stagedUpload(t, c, "beta")
	var wg sync.WaitGroup
	outcomes := make([]domain.AcceptOutcome, 2)
	for i, req := range []domain.AcceptRequest{firstReq, secondReq} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			result, err := c.AcceptDocument(t.Context(), req)
			if err == nil {
				outcomes[i] = result.Outcome
			}
		}()
	}
	wg.Wait()
	require.Equal(t, []domain.AcceptOutcome{domain.AcceptAccepted, domain.AcceptAccepted}, outcomes)
	for _, upload := range []domain.SourceUpload{first, second} {
		row, _ := readUpload(t, c, upload.ID)
		require.Equal(t, "linked", row.State)
		require.Equal(t, string(upload.DocumentID), *row.Linked)
		require.Equal(t, upload.Collection, row.Collection)
	}
}

func TestDuplicateAcceptanceLeavesUploadStagedForReconciliation(t *testing.T) {
	c := jobsDatabase(t)
	first, firstReq := stagedUpload(t, c, "")
	_, err := c.AcceptDocument(t.Context(), firstReq)
	require.NoError(t, err)
	second, secondReq := stagedUpload(t, c, "")
	result, err := c.AcceptDocument(t.Context(), secondReq)
	require.NoError(t, err)
	require.Equal(t, domain.AcceptInProgress, result.Outcome)
	row, _ := readUpload(t, c, second.ID)
	require.Equal(t, "staged", row.State)

	row, _ = readUpload(t, c, first.ID)
	require.Equal(t, "linked", row.State)
}

func TestUploadResultAccountsForDispatchAfterClaim(t *testing.T) {
	c := jobsDatabase(t)
	upload, _ := stagedUpload(t, c, "")
	require.NoError(t, c.BeginSourceUpload(t.Context(), upload.ID))
	require.Error(t, c.BeginSourceUpload(t.Context(), upload.ID), "only one SDK operation may start")
	claim := uuid.NewString()
	_, err := c.Pool.Exec(t.Context(), `UPDATE source_uploads SET state='claimed',claim=$2,claimed_at=NOW() WHERE id=$1`, upload.ID, claim)
	require.NoError(t, err)
	require.Error(t, c.BeginSourceUpload(t.Context(), upload.ID))
	require.NoError(t, c.RecordSourceUploadResult(t.Context(), upload.ID, domain.SourceUploadResult{VersionID: "v7", Acknowledged: true, Settled: true}))
	row, ok := readUpload(t, c, upload.ID)
	require.True(t, ok, "a claimed tombstone stays until cleanup settles it")
	require.Equal(t, "claimed", row.State)
	require.Equal(t, "v7", row.Version)
	var state, currentClaim string
	require.NoError(t, c.Pool.QueryRow(t.Context(), `SELECT write_state,claim::text FROM source_uploads WHERE id=$1`, upload.ID).Scan(&state, &currentClaim))
	require.Equal(t, "settled", state)
	require.Equal(t, claim, currentClaim)
}

func TestStageRejectsDuplicateAndOversizedRecords(t *testing.T) {
	c := jobsDatabase(t)
	upload, _ := stagedUpload(t, c, "")
	again := upload
	again.ID = uuid.NewString()
	require.Error(t, c.StageSourceUpload(t.Context(), again), "an object key has one owner")
	tooLong := domain.SourceUpload{ID: uuid.NewString(), DocumentID: domain.DocumentID(uuid.NewString()), Bucket: uploadTestBucket, Key: strings.Repeat("k", 1025)}
	require.Error(t, c.StageSourceUpload(t.Context(), tooLong))
	for _, invalid := range []domain.SourceUpload{
		{ID: "not-a-uuid", DocumentID: domain.DocumentID(uuid.NewString()), Bucket: uploadTestBucket, Key: "k"},
		{ID: uuid.NewString(), DocumentID: "not-a-uuid", Bucket: uploadTestBucket, Key: "k"},
		{ID: uuid.NewString(), DocumentID: domain.DocumentID(uuid.NewString()), Key: "k"},
		{ID: uuid.NewString(), DocumentID: domain.DocumentID(uuid.NewString()), Bucket: uploadTestBucket},
	} {
		require.Error(t, c.StageSourceUpload(t.Context(), invalid))
	}
}

func TestStagedUploadLeaseIsBounded(t *testing.T) {
	c := jobsDatabase(t)
	upload, _ := stagedUpload(t, c, "")
	var seconds float64
	require.NoError(t, c.Pool.QueryRow(t.Context(), `SELECT EXTRACT(EPOCH FROM expires_at-created_at) FROM source_uploads WHERE id=$1`, upload.ID).Scan(&seconds))
	require.InDelta(t, domain.SourceUploadLease.Seconds(), seconds, 1)
}

func TestDeletingDocumentRemovesItsLinkedUpload(t *testing.T) {
	c := jobsDatabase(t)
	upload, req := stagedUpload(t, c, "")
	_, err := c.AcceptDocument(t.Context(), req)
	require.NoError(t, err)
	other, _ := stagedUpload(t, c, "")
	require.NoError(t, c.DeleteDocument(t.Context(), upload.DocumentID))
	_, ok := readUpload(t, c, upload.ID)
	require.False(t, ok)
	_, ok = readUpload(t, c, other.ID)
	require.True(t, ok, "other uploads are independent of the deleted document")
}

func beforeSourceUploadMigration(t *testing.T) int {
	t.Helper()
	migrations, err := loadMigrations(testMigrationFiles)
	require.NoError(t, err)
	for _, m := range migrations {
		if m.name == "014_source_uploads.sql" {
			return m.version - 1
		}
	}
	require.FailNow(t, "missing source uploads migration")
	return 0
}

func TestLegacyDocumentsSurviveTheUploadMigration(t *testing.T) {
	url := startTestDatabase(t)
	c := migrationTestClient(t, url)
	applyMigrationPrefix(t, c, beforeSourceUploadMigration(t))
	key := "documents/document/legacy/scan.pdf"
	req := jobsRequest("legacy original")
	req.Document.S3Key = &key
	// The database holds only the schema before the upload journal, so the
	// legacy rows are written in the shape of an executable of that time.
	// Current admission needs later migrations and cannot run here.
	id := string(req.Document.ID)
	_, err := c.Pool.Exec(t.Context(), `INSERT INTO documents(id,content,s3_key,status,fingerprint) VALUES($1,$2,$3,'processing',$4)`,
		id, req.Document.Content, key, req.Fingerprint)
	require.NoError(t, err)
	require.NoError(t, c.StoreFingerprint(t.Context(), req.Fingerprint, id))
	_, err = c.Pool.Exec(t.Context(), `INSERT INTO ingest_jobs(document_id) VALUES($1)`, id)
	require.NoError(t, err)

	require.NoError(t, c.RunMigrations(t.Context()))
	stored, err := c.GetDocumentS3Key(t.Context(), req.Document.ID)
	require.NoError(t, err)
	require.Equal(t, key, *stored)
	var rows int
	require.NoError(t, c.Pool.QueryRow(t.Context(), `SELECT count(*) FROM source_uploads`).Scan(&rows))
	require.Zero(t, rows, "legacy ownership is never inferred")
	requireRecordedMigrations(t, c)
	again, err := c.AcceptDocument(t.Context(), req)
	require.NoError(t, err)
	require.Equal(t, domain.AcceptInProgress, again.Outcome, "documents without a journal row still deduplicate")
}
