package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/chunker"
	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	pgstore "github.com/Victoroide/quarry-rag/internal/store/postgres"
	"github.com/Victoroide/quarry-rag/internal/testutil/pgtest"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pinecone-io/go-pinecone/v5/pinecone"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestSupersededReconcileAfterManifestRetry(t *testing.T) {
	ctx := t.Context()
	dsn := pgtest.Start(t)
	pg, err := pgstore.NewClient(ctx, dsn)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, pg.Close(context.Background())) })
	require.NoError(t, pg.RunMigrations(ctx))
	cfg := &config.Config{
		VectorStore: "pinecone", PineconeIndexName: "bound", EmbeddingProvider: domain.EmbeddingProviderOpenAI,
		OpenAIEmbeddingModel: "text-embedding-3-small", EmbeddingModelRevision: "rev-1",
		ExtractionChunkVersion: "chunk-1", EmbeddingDimensions: 3, ChunkTokenSize: 512, ChunkOverlap: 64,
	}
	const host = "bound-host"
	require.NoError(t, pg.PinVectorGeneration(ctx, pgstore.LegacyGeneration, "pinecone", "bound", host, cfg.EmbeddingSettings()))

	partialID, reuseID, legacyID := fmt.Sprintf("%032x", 1), fmt.Sprintf("%032x", 2), fmt.Sprintf("%032x", 3)
	doc, reused, legacy := uuid.NewString(), uuid.NewString(), uuid.NewString()
	exec := func(t *testing.T, sql string, args ...any) {
		t.Helper()
		_, execErr := pg.Pool.Exec(ctx, sql, args...)
		require.NoError(t, execErr)
	}
	addDocument := func(id, content string) {
		t.Helper()
		accepted, acceptErr := pg.AcceptDocument(ctx, domain.AcceptRequest{
			Document: domain.Document{ID: domain.DocumentID(id), Content: content}, CallerSupplied: true,
			Fingerprint: chunker.Fingerprint(content), MaxPending: 1000,
		})
		require.NoError(t, acceptErr)
		require.Equal(t, domain.AcceptAccepted, accepted.Outcome)
	}
	index := &supersededTestIndex{vectors: make(map[string]*pinecone.Vector)}
	index.deleteHook = index.deleteVectors
	var listCalls int
	index.list = func(_ context.Context, req *pinecone.ListVectorsRequest) (*pinecone.ListVectorsResponse, error) {
		listCalls++
		keys := slices.Sorted(maps.Keys(index.vectors))
		start := 0
		if req.PaginationToken != nil {
			start, _ = slices.BinarySearch(keys, *req.PaginationToken)
			for start < len(keys) && keys[start] <= *req.PaginationToken {
				start++
			}
		}
		end := min(len(keys), start+int(*req.Limit))
		page := keys[start:end]
		next := ""
		if end < len(keys) {
			next = page[len(page)-1]
		}
		return supersededTestList(next, page...), nil
	}
	bare := func(t *testing.T, key, owner, incarnation string) {
		t.Helper()
		index.vectors[key] = supersededTestVector(t, key, map[string]any{"doc_id": owner, domain.VectorIncarnationKey: incarnation})
	}

	addDocument(doc, "partial bare write")
	first, err := pg.ClaimJob(ctx, "bare-worker", time.Minute, 5)
	require.NoError(t, err)
	require.NotNil(t, first)
	require.Equal(t, domain.DocumentID(doc), first.Document.ID)
	originalIncarnation := first.Incarnation
	require.NoError(t, uuid.Validate(originalIncarnation))
	bare(t, partialID, doc, originalIncarnation)
	require.NoError(t, pg.RetryJob(ctx, *first, 0, "partial write"))

	pg.EnableHostedVectorManifests(true)
	publish := func(id string) (chunk domain.Chunk, vectorID, incarnation string) {
		t.Helper()
		job, claimErr := pg.ClaimJob(ctx, "manifest-worker", time.Minute, 5)
		require.NoError(t, claimErr)
		require.NotNil(t, job)
		require.Equal(t, domain.DocumentID(id), job.Document.ID)
		job.RevisionID = uuid.NewString()
		chunk = domain.Chunk{ID: domain.ChunkID(uuid.NewString()), DocumentID: job.Document.ID, RevisionID: job.RevisionID, Text: "published text", TokenCount: 2}
		prefix := domain.RevisionVectorPrefix(job.Document.ID, job.RevisionID)
		vectorID = domain.FormatRevisionVectorID(job.Document.ID, job.RevisionID, 0)
		require.NoError(t, pg.PersistVectorManifest(ctx, *job, domain.VectorManifest{
			RevisionID: job.RevisionID, DocumentID: job.Document.ID, Incarnation: job.Incarnation, Attempt: job.Attempts,
			LeaseToken: job.LeaseToken, IDPrefix: prefix, VectorIDs: []string{vectorID}, ChunkIDs: []string{string(chunk.ID)},
		}))
		index.vectors[vectorID] = supersededTestVector(t, vectorID, map[string]any{"doc_id": id, "revision_id": job.RevisionID})
		ok, completeErr := pg.CompleteJob(ctx, *job, []domain.Chunk{chunk}, domain.IndexObservation{})
		require.NoError(t, completeErr)
		require.True(t, ok)
		return chunk, vectorID, job.Incarnation
	}
	published, qualified, retryIncarnation := publish(doc)
	require.Equal(t, originalIncarnation, retryIncarnation)
	exec(t, `INSERT INTO documents(id,content,status) VALUES($1,'previous document','completed')`, reused)
	var retiredIncarnation string
	require.NoError(t, pg.Pool.QueryRow(ctx, `SELECT incarnation::text FROM documents WHERE id=$1`, reused).Scan(&retiredIncarnation))
	bare(t, reuseID, reused, retiredIncarnation)
	exec(t, `DELETE FROM documents WHERE id=$1`, reused)
	addDocument(reused, "new document with reused UUID")
	_, reusedQualified, newIncarnation := publish(reused)
	require.NotEqual(t, retiredIncarnation, newIncarnation)
	exec(t, `INSERT INTO documents(id,content,status) VALUES($1,'legacy document','completed')`, legacy)
	index.vectors[legacyID] = supersededTestVector(t, legacyID, map[string]any{"doc_id": legacy})
	failed := uuid.NewString()
	exec(t, `INSERT INTO documents(id,content,status) VALUES($1,'failed document','failed')`, failed)

	// A pointer alone cannot prove that the current incarnation has published.
	for _, tc := range []struct{ name, change, undo string }{
		{"processing", "UPDATE documents SET status='processing' WHERE id=$1", "UPDATE documents SET status='completed' WHERE id=$1"},
		{"deleting", "UPDATE documents SET status='deleting' WHERE id=$1", "UPDATE documents SET status='completed' WHERE id=$1"},
		{"mutation", "UPDATE documents SET vector_mutation_pending=true WHERE id=$1", "UPDATE documents SET vector_mutation_pending=false WHERE id=$1"},
		{"job", "INSERT INTO ingest_jobs(document_id) VALUES($1)", "DELETE FROM ingest_jobs WHERE document_id=$1"},
		{"mismatched incarnation", "UPDATE hosted_vector_revisions SET incarnation=gen_random_uuid() WHERE document_id=$1", "UPDATE hosted_vector_revisions r SET incarnation=d.incarnation FROM documents d WHERE d.id=$1 AND r.document_id=d.id"},
		{"writing manifest", "UPDATE hosted_vector_manifests SET state='writing' WHERE document_id=$1", "UPDATE hosted_vector_manifests SET state='active' WHERE document_id=$1"},
		{"held UUID", "INSERT INTO document_deletions(document_id,incarnation,kind,vector_mode,held,finalized) SELECT id,incarnation,'quarantine','legacy',true,true FROM documents WHERE id=$1", "DELETE FROM document_deletions WHERE document_id=$1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			exec(t, tc.change, doc)
			defer exec(t, tc.undo, doc)
			found, lookupErr := lookupSupersededDocuments(ctx, pg, []string{doc, reused, legacy, failed})
			require.NoError(t, lookupErr)
			require.Equal(t, map[string]bool{reused: true}, found)
		})
	}

	opts := options{SupersededUnrevisioned: true, Page: 1, Limit: 2, ScanLimit: 3}
	dry, err := runSuperseded(ctx, cfg, index, pg, host, opts, io.Discard)
	require.NoError(t, err)
	require.Equal(t, []string{partialID, reuseID}, dry.Sample)
	require.Zero(t, dry.Deleted)
	require.Empty(t, index.deleted)
	opts.Apply = true
	_, err = runSuperseded(ctx, cfg, index, pg, host, opts, io.Discard)
	require.ErrorContains(t, err, "freeze writers")
	require.NoError(t, pg.FreezeWriters(ctx, pgstore.LegacyGeneration))
	_, err = runSuperseded(ctx, cfg, index, pg, host, opts, io.Discard)
	require.ErrorContains(t, err, "frozen for at least")
	exec(t, `UPDATE vector_state SET frozen_at=now()-interval '2 minutes'`)
	require.ErrorIs(t, pg.WritersOpen(ctx), domain.ErrWritersFrozen)
	claim, err := pg.ClaimJob(ctx, "old-worker", time.Minute, 5)
	require.NoError(t, err)
	require.Nil(t, claim)
	// An outstanding receipt keeps even an old enough freeze from settling.
	exec(t, `INSERT INTO document_deletions(document_id,incarnation,kind,vector_mode)
		SELECT id,incarnation,'quarantine','legacy' FROM documents WHERE id=$1`, legacy)
	_, err = runSuperseded(ctx, cfg, index, pg, host, opts, io.Discard)
	require.ErrorContains(t, err, "1 deletions")
	exec(t, `DELETE FROM document_deletions WHERE document_id=$1`, legacy)
	exec(t, `INSERT INTO ingest_jobs(document_id,state,lease_owner,lease_until) VALUES($1,'running','older-writer',now()+interval '2 minutes')`, legacy)
	_, err = runSuperseded(ctx, cfg, index, pg, host, opts, io.Discard)
	require.ErrorContains(t, err, "1 jobs")
	exec(t, `DELETE FROM ingest_jobs WHERE document_id=$1`, legacy)
	_, err = runSuperseded(ctx, cfg, index, pg, "different-host", opts, io.Discard)
	require.ErrorIs(t, err, pgstore.ErrGenerationDrift)
	require.Empty(t, index.deleted)

	// A newer command refuses an unmigrated database before the first delete.
	exec(t, `ALTER TABLE vector_state RENAME COLUMN active_cleanup_ids TO hidden_cleanup_ids`)
	_, err = runSuperseded(ctx, cfg, index, pg, host, opts, io.Discard)
	require.ErrorContains(t, err, "active_cleanup_ids")
	require.Empty(t, index.deleted)
	exec(t, `ALTER TABLE vector_state RENAME COLUMN hidden_cleanup_ids TO active_cleanup_ids`)

	applied, err := runSuperseded(ctx, cfg, index, pg, host, opts, io.Discard)
	require.NoError(t, err)
	require.Equal(t, [][]string{{partialID}, {reuseID}}, index.deleted)
	require.Equal(t, 2, applied.Deleted)
	require.Equal(t, 2, applied.Scanned)
	require.NotEmpty(t, applied.NextPageToken)
	for _, key := range []string{qualified, reusedQualified, legacyID} {
		require.Contains(t, index.vectors, key)
	}
	visible, err := (pgstore.Visibility{Pool: pg.Pool}).VisibleRevisionChunks(ctx, []domain.ScoredChunk{
		{Chunk: domain.Chunk{ID: domain.ChunkID(partialID), DocumentID: domain.DocumentID(doc)}}, {Chunk: published},
	})
	require.NoError(t, err)
	require.Len(t, visible, 1)
	require.Equal(t, published.RevisionID, visible[0].Chunk.RevisionID)
	repeat, err := runSuperseded(ctx, cfg, index, pg, host, opts, io.Discard)
	require.NoError(t, err)
	require.Zero(t, repeat.Selected)

	state, err := pg.VectorState(ctx)
	require.NoError(t, err)
	require.NotNil(t, state.FrozenAt)
	readBatch := func(t *testing.T) *pgstore.ActiveCleanupBatch {
		t.Helper()
		fence, fenceErr := pg.FenceGeneration(ctx, pgstore.LegacyGeneration)
		require.NoError(t, fenceErr)
		defer fence.Close(ctx)
		batch, readErr := fence.ActiveCleanupBatch(ctx, *state.FrozenAt)
		require.NoError(t, readErr)
		return batch
	}
	confirm := func(t *testing.T) options {
		t.Helper()
		batch := readBatch(t)
		require.NotNil(t, batch)
		_, cleanupErr := runSuperseded(ctx, cfg, index, pg, host, opts, io.Discard)
		require.ErrorContains(t, cleanupErr, "-confirm-target-drained="+batch.Reservation.String())
		recovery := opts
		recovery.ConfirmTargetDrained = batch.Reservation
		return recovery
	}

	for n, fail := range []error{context.Canceled, context.DeadlineExceeded, status.Error(codes.PermissionDenied, "rejected"), status.Error(codes.NotFound, "missing index")} {
		t.Run(strconv.Itoa(n), func(t *testing.T) {
			bare(t, partialID, doc, originalIncarnation)
			index.deleteErr = fail
			before := len(index.deleted)
			result, cleanupErr := runSuperseded(ctx, cfg, index, pg, host, opts, io.Discard)
			require.ErrorIs(t, cleanupErr, fail)
			require.Equal(t, 1, result.Selected)
			require.Zero(t, result.Deleted)
			require.ErrorContains(t, pg.ThawWriters(ctx), "pending active vector cleanup")
			_, cleanupErr = runSuperseded(ctx, cfg, index, pg, host, opts, io.Discard)
			require.ErrorContains(t, cleanupErr, partialID)
			require.Equal(t, [][]string{{partialID}}, index.deleted[before:])
			if errors.Is(fail, context.DeadlineExceeded) {
				// A delayed provider request takes effect after the client timed out.
				delete(index.vectors, partialID)
			}
			index.deleteErr = nil
			recovery := confirm(t)
			result, cleanupErr = runSuperseded(ctx, cfg, index, pg, host, recovery, io.Discard)
			require.NoError(t, cleanupErr)
			if errors.Is(fail, context.DeadlineExceeded) {
				require.Zero(t, result.Deleted)
			} else {
				require.Equal(t, 1, result.Deleted)
			}
		})
	}
	bare(t, partialID, doc, originalIncarnation)
	applyCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	index.onDelete = cancel
	acknowledged, err := runSuperseded(applyCtx, cfg, index, pg, host, opts, io.Discard)
	require.ErrorContains(t, err, "record acknowledgment")
	require.Equal(t, 1, acknowledged.Deleted)
	index.onDelete = nil
	require.ErrorContains(t, pg.ThawWriters(ctx), "pending active vector cleanup")
	recovery := confirm(t)
	acknowledged, err = runSuperseded(ctx, cfg, index, pg, host, recovery, io.Discard)
	require.NoError(t, err)
	require.Zero(t, acknowledged.Deleted)

	bare(t, partialID, doc, originalIncarnation)
	index.onDelete = func() {
		// The fence is the only advisory lock held in this disposable database.
		var terminated bool
		terminateErr := pg.Pool.QueryRow(ctx, `SELECT pg_terminate_backend(pid, 5000) FROM pg_locks
			WHERE locktype='advisory' AND granted
				AND database=(SELECT oid FROM pg_database WHERE datname=current_database())`).Scan(&terminated)
		require.NoError(t, terminateErr)
		require.True(t, terminated)
	}
	acknowledged, err = runSuperseded(ctx, cfg, index, pg, host, opts, io.Discard)
	require.ErrorContains(t, err, "record acknowledgment")
	require.Equal(t, 1, acknowledged.Deleted)
	index.onDelete = nil
	require.ErrorContains(t, pg.ThawWriters(ctx), "pending active vector cleanup")
	recovery = confirm(t)
	acknowledged, err = runSuperseded(ctx, cfg, index, pg, host, recovery, io.Discard)
	require.NoError(t, err)
	require.Zero(t, acknowledged.Deleted)

	for _, tc := range []struct {
		name           string
		nextID         string
		loseClearReply bool
	}{
		{name: "delayed confirmation with different IDs", nextID: reuseID},
		{name: "delayed confirmation with identical IDs", nextID: partialID},
		{name: "duplicate confirmation after an ambiguous clear", nextID: partialID, loseClearReply: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bare(t, partialID, doc, originalIncarnation)
			index.deleteErr = context.DeadlineExceeded
			firstDispatch := len(index.deleted)
			_, err := runSuperseded(ctx, cfg, index, pg, host, opts, io.Discard)
			require.ErrorIs(t, err, context.DeadlineExceeded)
			operatorA := confirm(t)
			operatorB := operatorA

			// The provider has settled the first request. The restart can
			// discover another candidate or still list the same bare key.
			if tc.nextID != partialID {
				delete(index.vectors, partialID)
				bare(t, tc.nextID, reused, retiredIncarnation)
			}
			if tc.loseClearReply {
				var armed atomic.Bool
				faulted, connectErr := pgstore.NewClient(ctx, dsn, func(pool *pgxpool.Config) {
					pool.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeExec
					pool.ConnConfig.BuildFrontend = func(r io.Reader, w io.Writer) *pgproto3.Frontend {
						return pgproto3.NewFrontend(&lostCleanupClearReply{Reader: r, armed: &armed}, w)
					}
				})
				require.NoError(t, connectErr)
				t.Cleanup(func() { require.NoError(t, faulted.Close(context.WithoutCancel(ctx))) })
				armAfterDiagnostic := cleanupDiagnosticFunc(func(p []byte) (int, error) {
					armed.Store(true)
					return len(p), nil
				})
				beforeList := listCalls
				_, err = runSuperseded(ctx, cfg, index, faulted, host, operatorA, armAfterDiagnostic)
				require.ErrorContains(t, err, "clear active cleanup")
				require.False(t, armed.Load(), "the clear response must have been dropped")
				require.Eventually(t, func() bool {
					var locked bool
					probeErr := pg.Pool.QueryRow(ctx, `SELECT EXISTS (
						SELECT FROM pg_locks WHERE locktype='advisory' AND granted
							AND database=(SELECT oid FROM pg_database WHERE datname=current_database())
					)`).Scan(&locked)
					return probeErr == nil && !locked
				}, 5*time.Second, 10*time.Millisecond, "the disconnected clear must release the fence")
				require.Nil(t, readBatch(t), "the clear committed despite the lost response")
				_, err = runSuperseded(ctx, cfg, index, pg, host, operatorB, io.Discard)
				require.ErrorContains(t, err, "no batch is pending")
				require.Equal(t, beforeList, listCalls)
				require.Equal(t, [][]string{{partialID}}, index.deleted[firstDispatch:])
				operatorA = opts
			}

			_, err = runSuperseded(ctx, cfg, index, pg, host, operatorA, io.Discard)
			require.ErrorIs(t, err, context.DeadlineExceeded)
			batchB := readBatch(t)
			require.NotNil(t, batchB)
			require.NotEqual(t, operatorB.ConfirmTargetDrained, batchB.Reservation)
			require.Equal(t, []string{tc.nextID}, batchB.IDs)

			// The provider may omit a record even with its delete outstanding.
			delete(index.vectors, tc.nextID)
			beforeList := listCalls
			_, err = runSuperseded(ctx, cfg, index, pg, host, operatorB, io.Discard)
			require.ErrorContains(t, err, "stale active cleanup confirmation")
			require.ErrorContains(t, err, batchB.Reservation.String())
			require.Equal(t, beforeList, listCalls)
			require.Equal(t, [][]string{{partialID}, {tc.nextID}}, index.deleted[firstDispatch:])
			require.Equal(t, batchB, readBatch(t))
			require.ErrorContains(t, pg.ThawWriters(ctx), "pending active vector cleanup")
			_, err = pg.Pool.Exec(ctx, `UPDATE vector_state SET active_generation='replacement'`)
			require.ErrorContains(t, err, "pending active vector cleanup")

			index.deleteErr = nil
			recovery := confirm(t)
			result, err := runSuperseded(ctx, cfg, index, pg, host, recovery, io.Discard)
			require.NoError(t, err)
			require.Zero(t, result.Deleted)
			require.Nil(t, readBatch(t))
		})
	}
	require.NoError(t, pg.ThawWriters(ctx))
	require.NoError(t, pg.WritersOpen(ctx))
}

type cleanupDiagnosticFunc func([]byte) (int, error)

func (f cleanupDiagnosticFunc) Write(p []byte) (int, error) { return f(p) }

// lostCleanupClearReply drops ReadyForQuery after the autocommit clear commits.
type lostCleanupClearReply struct {
	io.Reader
	armed *atomic.Bool
	tail  []byte
}

func (r *lostCleanupClearReply) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	if n == 0 || !r.armed.Load() {
		return n, err
	}
	ready := []byte{'Z', 0, 0, 0, 5, 'I'}
	r.tail = append(r.tail, p[:n]...)
	if bytes.Contains(r.tail, ready) && r.armed.CompareAndSwap(true, false) {
		return 0, io.ErrUnexpectedEOF
	}
	r.tail = append(r.tail[:0], r.tail[max(0, len(r.tail)-len(ready)+1):]...)
	return n, err
}
