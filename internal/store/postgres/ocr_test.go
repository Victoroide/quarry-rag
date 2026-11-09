package postgres

import (
	"strings"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/stretchr/testify/require"
)

func TestTextSourceMigrationPreservesDocuments(t *testing.T) {
	c := migrationTestClient(t, startTestDatabase(t))
	files, err := loadMigrations(testMigrationFiles)
	require.NoError(t, err)
	for _, f := range files {
		if f.version < 8 {
			_, err = c.Pool.Exec(t.Context(), f.sql)
			require.NoError(t, err)
		}
	}
	const id = "00000000-0000-4000-8000-000000000020"
	_, err = c.Pool.Exec(t.Context(), `INSERT INTO documents(id,content,status) VALUES($1,'existing text','completed')`, id)
	require.NoError(t, err)
	require.NoError(t, c.RunMigrations(t.Context()))
	var source, content, state string
	require.NoError(t, c.Pool.QueryRow(t.Context(), `SELECT text_source,content,status FROM documents WHERE id=$1`, id).Scan(&source, &content, &state))
	require.Equal(t, "parser", source)
	require.Equal(t, "existing text", content)
	require.Equal(t, "completed", state)
	require.NoError(t, c.RunMigrations(t.Context()))
}

func TestOCRTextRequiresLiveLease(t *testing.T) {
	c := jobsDatabase(t)
	req := jobsRequest("")
	req.Document.TextSource = "ocr_pending"
	key := "synthetic-object"
	req.Document.S3Key = &key
	_, err := c.AcceptDocument(t.Context(), req)
	require.NoError(t, err)
	j, err := c.ClaimJob(t.Context(), "current", time.Minute, 5)
	require.NoError(t, err)
	require.Equal(t, "ocr_pending", j.Document.TextSource)
	stale := *j
	stale.LeaseOwner = "stale"
	require.ErrorIs(t, c.StoreOCRText(t.Context(), stale, "stale text"), domain.ErrJobNotOwned)
	var source, content, preview string
	require.NoError(t, c.Pool.QueryRow(t.Context(), `SELECT text_source,content,content_preview FROM documents WHERE id=$1`, string(j.Document.ID)).Scan(&source, &content, &preview))
	require.Equal(t, "ocr_pending", source)
	require.Empty(t, content)
	text := strings.Repeat("界", 130)
	require.NoError(t, c.StoreOCRText(t.Context(), *j, text))
	require.NoError(t, c.Pool.QueryRow(t.Context(), `SELECT text_source,content,content_preview FROM documents WHERE id=$1`, string(j.Document.ID)).Scan(&source, &content, &preview))
	require.Equal(t, "ocr", source)
	require.Equal(t, text, content)
	require.Equal(t, strings.Repeat("界", 120), preview)
	require.NoError(t, c.ReleaseJob(t.Context(), *j))
	next, err := c.ClaimJob(t.Context(), "next", time.Minute, 5)
	require.NoError(t, err)
	require.Equal(t, "ocr", next.Document.TextSource)
	require.Equal(t, text, next.Document.Content)
	require.Equal(t, req.Fingerprint, next.Document.Fingerprint)
	_, err = c.Pool.Exec(t.Context(), `UPDATE ingest_jobs SET lease_until=NOW()-interval '1 second'`)
	require.NoError(t, err)
	require.ErrorIs(t, c.StoreOCRText(t.Context(), *next, "expired text"), domain.ErrJobNotOwned)
}

func TestOCRTextRejectsLeaseExpiredWhileWaitingForDocument(t *testing.T) {
	c := jobsDatabase(t)
	id := jobsAccept(t, c, "pending text")
	j, err := c.ClaimJob(t.Context(), "owner", time.Minute, 5)
	require.NoError(t, err)
	lock, err := c.Pool.Begin(t.Context())
	require.NoError(t, err)
	defer func() { _ = lock.Rollback(t.Context()) }()
	_, err = lock.Exec(t.Context(), `SELECT id FROM documents WHERE id=$1 FOR UPDATE`, string(id))
	require.NoError(t, err)
	_, err = lock.Exec(t.Context(), `UPDATE ingest_jobs SET lease_until=clock_timestamp()+interval '500 milliseconds' WHERE document_id=$1`, string(id))
	require.NoError(t, err)
	// Keep only the document locked while the original lease expires.
	require.NoError(t, lock.Commit(t.Context()))
	lock, err = c.Pool.Begin(t.Context())
	require.NoError(t, err)
	_, err = lock.Exec(t.Context(), `SELECT id FROM documents WHERE id=$1 FOR UPDATE`, string(id))
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() { done <- c.StoreOCRText(t.Context(), *j, "late text") }()
	require.Eventually(t, func() bool {
		var waiting bool
		queryErr := c.Pool.QueryRow(t.Context(), `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE wait_event_type='Lock' AND query LIKE 'SELECT d.id::text FROM documents d JOIN ingest_jobs j%')`).Scan(&waiting)
		return queryErr == nil && waiting
	}, time.Second, time.Millisecond)
	require.Eventually(t, func() bool {
		var expired bool
		queryErr := c.Pool.QueryRow(t.Context(), `SELECT lease_until<clock_timestamp() FROM ingest_jobs WHERE document_id=$1`, string(id)).Scan(&expired)
		return queryErr == nil && expired
	}, time.Second, time.Millisecond)
	require.NoError(t, lock.Commit(t.Context()))
	require.ErrorIs(t, <-done, domain.ErrJobNotOwned)
	var content string
	require.NoError(t, c.Pool.QueryRow(t.Context(), `SELECT content FROM documents WHERE id=$1`, string(id)).Scan(&content))
	require.Equal(t, "pending text", content)
}
