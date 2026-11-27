package postgres

import (
	"context"
	"math"
	"slices"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDecodeSourceLineBoundaries(t *testing.T) {
	const body = "é a\n\nb"
	valid := []domain.SourceLineBoundary{{Offset: 2, Kind: domain.SourceLineBreak}, {Offset: 4, Kind: domain.SourceParagraphBreak}}
	arr := func(offsets ...int32) pgtype.Array[pgtype.Int4] {
		v := pgtype.Array[pgtype.Int4]{Valid: true}
		length := len(offsets)
		if length > math.MaxInt32 {
			t.Fatal("fixture array dimension exceeds int32")
			return v
		}
		if length > 0 {
			v.Dims = []pgtype.ArrayDimension{{Length: int32(length), LowerBound: 1}}
		}
		for _, off := range offsets {
			v.Elements = append(v.Elements, pgtype.Int4{Int32: off, Valid: true})
		}
		return v
	}
	require.Nil(t, decodeSourceLineBoundaries(body, pgtype.Array[pgtype.Int4]{}))
	require.Equal(t, []domain.SourceLineBoundary{}, decodeSourceLineBoundaries(body, arr()))
	require.Equal(t, valid, decodeSourceLineBoundaries(body, arr(2, -4)))
	require.Equal(t, []int32{2, -4}, encodeSourceLineBoundaries(body, valid))
	require.Equal(t, []int32{}, encodeSourceLineBoundaries(body, []domain.SourceLineBoundary{}))
	require.Nil(t, encodeSourceLineBoundaries(body, []domain.SourceLineBoundary{{Offset: -1, Kind: domain.SourceLineBreak}}))
	wrongLower := arr(2)
	wrongLower.Dims[0].LowerBound = 0
	wrongDim := arr(2)
	wrongDim.Dims = append(wrongDim.Dims, wrongDim.Dims[0])
	wrongCardinality := arr(2)
	wrongCardinality.Dims[0].Length = 2
	nullElement := arr(2)
	nullElement.Elements[0].Valid = false
	for _, bad := range []pgtype.Array[pgtype.Int4]{
		wrongLower, wrongDim, wrongCardinality, nullElement, arr(0), arr(math.MinInt32), arr(2, 2), arr(-4, 2), arr(-2), arr(4), arr(1), arr(99), arr(-5), arr(2, -4, -5), arr(2, 2, 2, 2, 2, 2, 2, 2),
	} {
		require.Nil(t, decodeSourceLineBoundaries(body, bad), "%+v", bad)
	}
	for _, tc := range []struct {
		text   string
		offset int32
	}{
		{"a\n\n\n", -1}, {"a \u2001", 1}, {"\t a", 1},
	} {
		require.Nil(t, decodeSourceLineBoundaries(tc.text, arr(tc.offset)), "%q", tc.text)
		require.Equal(t, []domain.SourceLineBoundary{}, decodeSourceLineBoundaries(tc.text, arr()))
	}
}

func TestCompleteJobSourceLineBoundaries(t *testing.T) {
	c, _ := vectorDatabase(t)
	ctx := t.Context()
	jobsAccept(t, c, "Synthetic source line boundary completion.")
	job, err := c.ClaimJob(ctx, "boundaries", time.Minute, 3)
	require.NoError(t, err)
	require.NotNil(t, job)
	marked := []domain.SourceLineBoundary{{Offset: 2, Kind: domain.SourceLineBreak}, {Offset: 4, Kind: domain.SourceParagraphBreak}}
	chunks := make([]domain.Chunk, 3)
	for i, sidecar := range [][]domain.SourceLineBoundary{nil, {}, marked} {
		chunks[i] = domain.Chunk{ID: domain.ChunkID(uuid.NewString()), DocumentID: job.Document.ID, Index: i, Text: "é a\n\nb", Embedding: []float32{1, 0, 0}, SourceLineBoundaries: sidecar}
	}
	// A later vector failure rolls back the earlier chunk inserts.
	chunks[2].Embedding = []float32{1, 0}
	ok, err := c.CompleteJob(ctx, *job, chunks, domain.IndexObservation{})
	require.Error(t, err)
	require.False(t, ok)
	var count int
	require.NoError(t, c.Pool.QueryRow(ctx, `SELECT count(*) FROM chunks WHERE document_id=$1`, job.Document.ID).Scan(&count))
	require.Zero(t, count)
	chunks[2].Embedding = []float32{1, 0, 0}
	ok, err = c.CompleteJob(ctx, *job, chunks, domain.IndexObservation{})
	require.NoError(t, err)
	require.True(t, ok)
	check := func(ch domain.Chunk, want []domain.SourceLineBoundary) {
		t.Helper()
		var stored pgtype.Array[pgtype.Int4]
		require.NoError(t, c.Pool.QueryRow(ctx, `SELECT source_line_boundaries FROM chunks WHERE id=$1`, ch.ID).Scan(&stored))
		require.Equal(t, want, decodeSourceLineBoundaries(ch.Text, stored))
	}
	for _, ch := range chunks {
		check(ch, ch.SourceLineBoundaries)
	}
	stale := slices.Clone(chunks)
	stale[2].SourceLineBoundaries = []domain.SourceLineBoundary{}
	ok, err = c.CompleteJob(ctx, *job, stale, domain.IndexObservation{})
	require.NoError(t, err)
	require.False(t, ok)
	check(chunks[2], marked)
	serving, _ := publishedServingClient(t, c)
	// A pre-upgrade upsert preserves same-body provenance and clears stale
	// coordinates when its replacement text has the same separator bytes.
	ch := chunks[2]
	ch.SourceLineBoundaries = nil
	require.NoError(t, serving.InsertChunks(ctx, []domain.Chunk{ch}))
	check(ch, marked)
	ch.Text = "è z\n\nc"
	require.NoError(t, serving.InsertChunks(ctx, []domain.Chunk{ch}))
	check(ch, nil)
	ch.SourceLineBoundaries = marked
	require.NoError(t, serving.InsertChunks(ctx, []domain.Chunk{ch}))
	check(ch, marked)
	ch.Text = "ê x\n\ny"
	require.NoError(t, serving.InsertChunks(ctx, []domain.Chunk{ch}))
	check(ch, marked)
	_, err = c.Pool.Exec(ctx, `DELETE FROM documents WHERE id=$1`, job.Document.ID)
	require.NoError(t, err)
	require.NoError(t, c.Pool.QueryRow(ctx, `SELECT count(*) FROM chunks WHERE document_id=$1`, job.Document.ID).Scan(&count))
	require.Zero(t, count)

	// Bulk cancellation and completion can contend for the same document.
	// Any publication that wins the row lock still cascades on deletion.
	req := jobsRequest("Synthetic replacement with the original identifier.")
	req.Document.ID = job.Document.ID
	req.CallerSupplied = true
	result, err := c.AcceptDocument(ctx, req)
	require.NoError(t, err)
	require.Equal(t, domain.AcceptAccepted, result.Outcome)
	replacement, err := c.ClaimJob(ctx, "replacement", time.Minute, 3)
	require.NoError(t, err)
	require.NotNil(t, replacement)
	require.NotEqual(t, job.Incarnation, replacement.Incarnation)
	ch = chunks[2]
	ok, err = c.CompleteJob(ctx, *job, []domain.Chunk{ch}, domain.IndexObservation{})
	require.NoError(t, err)
	require.False(t, ok)
	queuedID := jobsAccept(t, c, "Synthetic queued work for bulk cancellation.")
	raceCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	completed := make(chan error, 1)
	go func() {
		_, completeErr := c.CompleteJob(raceCtx, *replacement, []domain.Chunk{ch}, domain.IndexObservation{})
		completed <- completeErr
	}()
	_, deleteErr := c.Pool.Exec(raceCtx, `DELETE FROM documents WHERE id=ANY($1::uuid[])`,
		[]string{string(replacement.Document.ID), string(queuedID)})
	completeErr := <-completed
	require.NoError(t, deleteErr)
	require.NoError(t, completeErr)
	require.NoError(t, c.Pool.QueryRow(ctx, `SELECT
		(SELECT count(*) FROM chunks WHERE document_id=ANY($1::uuid[]))
		+ (SELECT count(*) FROM ingest_jobs WHERE document_id=ANY($1::uuid[]))`,
		[]string{string(replacement.Document.ID), string(queuedID)}).Scan(&count))
	require.Zero(t, count)
}

func TestSourceLineBoundaryMigrationKeepsLegacyReadersAndRows(t *testing.T) {
	c := migrationTestClient(t, startTestDatabase(t))
	applyMigrationsExcept(t, c, 46)
	ctx := t.Context()
	doc := domain.Document{ID: domain.DocumentID(uuid.NewString())}
	require.NoError(t, c.InsertDocument(ctx, doc))
	require.NoError(t, c.UpdateDocumentStatus(ctx, doc.ID, "completed"))
	chunk := domain.Chunk{ID: domain.ChunkID(uuid.NewString()), DocumentID: doc.ID, Text: "first line"}
	require.NoError(t, c.InsertChunks(ctx, []domain.Chunk{chunk}))
	vis := Visibility{Pool: c.Pool}
	candidates := []domain.ScoredChunk{{Chunk: chunk}}
	before, err := vis.VisibleEvidence(ctx, candidates)
	require.NoError(t, err)
	require.Len(t, before, 1)
	var raw pgtype.Array[pgtype.Int4]
	err = c.Pool.QueryRow(ctx, `SELECT source_line_boundaries FROM chunks WHERE id=$1`, chunk.ID).Scan(&raw)
	require.Error(t, err)
	t.Run("populated table needs no validation scan", func(t *testing.T) {
		lockCtx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()
		ddl, readErr := migrationFiles.ReadFile("migrations/046_chunk_source_line_boundaries.sql")
		require.NoError(t, readErr)
		reader, acquireErr := c.Pool.Acquire(lockCtx)
		require.NoError(t, acquireErr)
		defer reader.Release()
		var readerPID int32
		require.NoError(t, reader.QueryRow(lockCtx, `SELECT pg_backend_pid()`).Scan(&readerPID))
		tx, beginErr := c.Pool.Begin(lockCtx)
		require.NoError(t, beginErr)
		defer func() { _ = tx.Rollback(context.WithoutCancel(lockCtx)) }()
		_, ddlErr := tx.Exec(lockCtx, string(ddl))
		require.NoError(t, ddlErr)

		// The lock is still exclusive until commit. An unvalidated constraint
		// proves the ALTER did not use that lock to validate existing rows.
		var validated, exclusive bool
		require.NoError(t, tx.QueryRow(lockCtx, `SELECT convalidated, EXISTS (
			SELECT 1 FROM pg_locks WHERE relation=conrelid AND pid=pg_backend_pid()
			AND mode='AccessExclusiveLock' AND granted)
			FROM pg_constraint WHERE conrelid='chunks'::regclass AND conname='chunks_source_line_boundaries_size'`).Scan(&validated, &exclusive))
		require.False(t, validated)
		require.True(t, exclusive)

		type evidenceRead struct {
			hits []domain.ScoredChunk
			err  error
		}
		done := make(chan evidenceRead, 1)
		go func() {
			defer close(done)
			hits, _, visibilityErr := vis.visibleEvidence(lockCtx, reader, candidates, nil)
			done <- evidenceRead{hits, visibilityErr}
		}()
		defer func() {
			cancel()
			// Keep the connection checked out until its query returns.
			<-done
		}()
		require.EventuallyWithT(t, func(collect *assert.CollectT) {
			var blocked bool
			require.NoError(collect, tx.QueryRow(lockCtx, `SELECT pg_backend_pid()=ANY(pg_blocking_pids($1))`, readerPID).Scan(&blocked))
			require.True(collect, blocked)
		}, 5*time.Second, 10*time.Millisecond)
		// Roll the manual execution back so the normal runner can apply 046.
		require.NoError(t, tx.Rollback(lockCtx))
		got := <-done
		require.NoError(t, got.err)
		require.Equal(t, before, got.hits)
	})
	require.NoError(t, c.RunMigrations(ctx))
	after, err := vis.VisibleEvidence(ctx, candidates)
	require.NoError(t, err)
	require.Equal(t, before, after)
	require.NoError(t, c.Pool.QueryRow(ctx, `SELECT source_line_boundaries FROM chunks WHERE id=$1`, chunk.ID).Scan(&raw))
	require.Nil(t, decodeSourceLineBoundaries(chunk.Text, raw))
	_, err = c.Pool.Exec(ctx, `UPDATE chunks SET source_line_boundaries=array_fill(1, ARRAY[100]) WHERE id=$1`, chunk.ID)
	requireSQLState(t, err, "23514")
	_, err = c.Pool.Exec(ctx, `INSERT INTO chunks (id,document_id,chunk_index,text,token_count,source_line_boundaries)
		VALUES ($1,$2,1,$3,1,array_fill(1, ARRAY[100]))`, uuid.NewString(), doc.ID, chunk.Text)
	requireSQLState(t, err, "23514")
}
