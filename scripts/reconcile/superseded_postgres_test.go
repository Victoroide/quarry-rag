package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"strconv"
	"strings"
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
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pinecone-io/go-pinecone/v5/pinecone"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func supersededTestDatabase(t *testing.T) (dsn string, pg *pgstore.Client, cfg *config.Config, host string) {
	t.Helper()
	ctx := t.Context()
	dsn = pgtest.Start(t)
	var err error
	pg, err = pgstore.NewClient(ctx, dsn)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, pg.Close(context.Background())) })
	require.NoError(t, pg.RunMigrations(ctx))
	cfg = &config.Config{
		VectorStore: "pinecone", PineconeIndexName: "bound", EmbeddingProvider: domain.EmbeddingProviderOpenAI,
		OpenAIEmbeddingModel: "text-embedding-3-small", EmbeddingModelRevision: "rev-1",
		ExtractionChunkVersion: "chunk-1", EmbeddingDimensions: 3, ChunkTokenSize: 512, ChunkOverlap: 64,
	}
	host = "bound-host"
	require.NoError(t, pg.PinVectorGeneration(ctx, pgstore.LegacyGeneration, "pinecone", "bound", host, cfg.EmbeddingSettings()))
	return dsn, pg, cfg, host
}

func TestSupersededRecoveryRequiresStoppedSender(t *testing.T) {
	_, pg, cfg, host := supersededTestDatabase(t)
	ctx := t.Context()
	doc, revision := uuid.New(), uuid.New()
	_, err := pg.Pool.Exec(ctx, `INSERT INTO documents(id,content,status) VALUES($1,'published','completed')`, doc)
	require.NoError(t, err)
	_, err = pg.Pool.Exec(ctx, `WITH manifest AS (
		INSERT INTO hosted_vector_manifests(revision_id,document_id,incarnation,attempt,id_prefix,vector_ids,chunk_ids,state)
		SELECT $2::uuid,id,COALESCE(incarnation,id),1,$2::uuid::text,ARRAY[$2::uuid::text],ARRAY[$2::uuid],'active' FROM documents WHERE id=$1
		RETURNING revision_id, document_id, incarnation
	) INSERT INTO hosted_vector_revisions(document_id,incarnation,active_revision_id)
		SELECT document_id,incarnation,revision_id FROM manifest`, doc, revision)
	require.NoError(t, err)
	require.NoError(t, pg.FreezeWriters(ctx, pgstore.LegacyGeneration))
	_, err = pg.Pool.Exec(ctx, `UPDATE vector_state SET frozen_at=now()-interval '2 minutes'`)
	require.NoError(t, err)
	var tokens []string
	index := &supersededTestIndex{vectors: map[string]*pinecone.Vector{
		bareTestID: supersededTestVector(t, bareTestID, map[string]any{"doc_id": doc.String()}),
	}}
	index.deleteHook = index.deleteVectors
	index.list = func(_ context.Context, req *pinecone.ListVectorsRequest) (*pinecone.ListVectorsResponse, error) {
		if req.PaginationToken == nil {
			tokens = append(tokens, "")
			return supersededTestList("next", bareTestID), nil
		}
		tokens = append(tokens, *req.PaginationToken)
		return supersededTestList(""), nil
	}
	opts := options{SupersededUnrevisioned: true, Apply: true, Page: 1, Limit: 2, ScanLimit: 3}
	supervisor, stop := context.WithCancel(ctx)
	paused := make(chan string, 1)
	done := make(chan struct{})
	t.Cleanup(func() {
		stop()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("sender still running after cancellation")
		}
	})
	var invocation string
	var senderErr error
	go func() {
		defer close(done)
		_, senderErr = runSuperseded(supervisor, cfg, index, pg, host, opts, cleanupDiagnosticFunc(func(p []byte) (int, error) {
			if strings.Contains(string(p), "exact IDs") {
				paused <- string(p)
				<-supervisor.Done()
			} else {
				invocation = string(p)
			}
			return len(p), nil
		}))
	}()
	select {
	case <-time.After(10 * time.Second):
		t.Fatal("sender did not pause after commit")
	case <-done:
		t.Fatalf("sender exited before dispatch boundary: %v", senderErr)
	case diagnostic := <-paused:
		require.Contains(t, diagnostic, bareTestID)
	}
	terminateCleanupFence(t, pg)
	state, err := pg.VectorState(ctx)
	require.NoError(t, err)
	require.NotNil(t, state.FrozenAt)
	readBatch := func() *pgstore.ActiveCleanupBatch {
		t.Helper()
		fence, fenceErr := pg.FenceGeneration(ctx, pgstore.LegacyGeneration)
		require.NoError(t, fenceErr)
		defer fence.Close(ctx)
		batch, readErr := fence.ActiveCleanupBatch(ctx, *state.FrozenAt)
		require.NoError(t, readErr)
		return batch
	}
	batch := readBatch()
	require.NotNil(t, batch)
	require.Equal(t, []string{bareTestID}, batch.IDs)
	require.Contains(t, invocation, batch.Sender.String())
	require.Empty(t, index.deleted)
	recovery := opts
	recovery.ConfirmTargetDrained = batch.Reservation
	mismatch := recovery
	mismatch.ConfirmSenderStopped = uuid.New()
	for _, blocked := range []options{opts, recovery, mismatch} {
		_, err = runSuperseded(ctx, cfg, index, pg, host, blocked, io.Discard)
		require.ErrorContains(t, err, batch.Sender.String())
		require.Equal(t, batch, readBatch())
		require.Equal(t, []string{""}, tokens)
		require.Empty(t, index.deleted)
		require.ErrorContains(t, pg.ThawWriters(ctx), "pending active vector cleanup")
	}
	stop()
	select {
	case <-done:
		require.ErrorIs(t, senderErr, context.Canceled)
	case <-time.After(10 * time.Second):
		t.Fatal("sender did not exit")
	}
	require.Empty(t, index.deleted)
	// No request was submitted, so the stopped sender has no outstanding effect.
	recovery.ConfirmSenderStopped = batch.Sender
	_, err = runSuperseded(ctx, cfg, index, pg, "wrong-host", recovery, io.Discard)
	require.ErrorIs(t, err, pgstore.ErrGenerationDrift)
	require.Equal(t, batch, readBatch())
	require.Equal(t, []string{""}, tokens)
	report, err := runSuperseded(ctx, cfg, index, pg, host, recovery, io.Discard)
	require.NoError(t, err)
	require.Equal(t, 1, report.Deleted)
	require.Empty(t, report.NextPageToken)
	require.Equal(t, []string{"", "", "next"}, tokens)
	require.Equal(t, [][]string{{bareTestID}}, index.deleted)
	require.Nil(t, readBatch())
	require.NoError(t, pg.ThawWriters(ctx))
	require.NoError(t, pg.WritersOpen(ctx))
}

func terminateCleanupFence(t *testing.T, pg *pgstore.Client) {
	t.Helper()
	var terminated bool
	err := pg.Pool.QueryRow(t.Context(), `SELECT pg_terminate_backend(pid, 5000) FROM pg_locks
		WHERE locktype='advisory' AND granted AND objsubid=2
			AND objid::bigint=(hashtext($1)::bigint & 4294967295)
			AND database=(SELECT oid FROM pg_database WHERE datname=current_database())`, pgstore.LegacyGeneration).Scan(&terminated)
	require.NoError(t, err)
	require.True(t, terminated)
}

func TestSupersededReconcileAfterManifestRetry(t *testing.T) {
	ctx := t.Context()
	dsn, pg, cfg, host := supersededTestDatabase(t)

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
		chunk, vectorID, incarnation = publishSupersededTestDocument(t, pg, id)
		index.vectors[vectorID] = supersededTestVector(t, vectorID, map[string]any{"doc_id": id, "revision_id": chunk.RevisionID})
		return chunk, vectorID, incarnation
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
		recovery.ConfirmSenderStopped = batch.Sender
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

	t.Run("failed diagnostics before dispatch", func(t *testing.T) {
		bare(t, partialID, doc, originalIncarnation)
		before := len(index.deleted)
		diagnosticErr := errors.New("diagnostics unavailable")
		_, cleanupErr := runSuperseded(ctx, cfg, index, pg, host, opts, cleanupDiagnosticFunc(func(p []byte) (int, error) {
			if strings.Contains(string(p), "exact IDs") {
				return 0, diagnosticErr
			}
			return len(p), nil
		}))
		require.ErrorIs(t, cleanupErr, diagnosticErr)
		require.Len(t, index.deleted, before)
		require.ErrorContains(t, pg.ThawWriters(ctx), "pending active vector cleanup")
		_, cleanupErr = runSuperseded(ctx, cfg, index, pg, host, confirm(t), io.Discard)
		require.NoError(t, cleanupErr)
		require.Equal(t, [][]string{{partialID}}, index.deleted[before:])
	})

	t.Run("reservation commit reply lost", func(t *testing.T) {
		bare(t, partialID, doc, originalIncarnation)
		before := len(index.deleted)
		var readyToReserve, armed atomic.Bool
		originalList := index.list
		index.list = func(ctx context.Context, req *pinecone.ListVectorsRequest) (*pinecone.ListVectorsResponse, error) {
			readyToReserve.Store(true)
			return originalList(ctx, req)
		}
		defer func() { index.list = originalList }()
		faulted, connectErr := pgstore.NewClient(ctx, dsn, func(pool *pgxpool.Config) {
			pool.ConnConfig.Tracer = cleanupQueryTrace(func(sql string) {
				if sql == "commit" && readyToReserve.CompareAndSwap(true, false) {
					armed.Store(true)
				}
			})
			pool.ConnConfig.BuildFrontend = func(r io.Reader, w io.Writer) *pgproto3.Frontend {
				return pgproto3.NewFrontend(&lostCleanupReply{Reader: r, armed: &armed}, w)
			}
		})
		require.NoError(t, connectErr)
		t.Cleanup(faulted.Pool.Close)
		var diagnostics bytes.Buffer
		_, cleanupErr := runSuperseded(ctx, cfg, index, faulted, host, opts, &diagnostics)
		require.ErrorContains(t, cleanupErr, "commit active cleanup reservation")
		require.False(t, armed.Load())
		waitForCleanupFenceRelease(ctx, t, pg)
		batch := readBatch(t)
		require.NotNil(t, batch)
		require.Contains(t, diagnostics.String(), batch.Sender.String())
		require.NotContains(t, diagnostics.String(), batch.Reservation.String())
		require.Len(t, index.deleted, before)
		require.ErrorContains(t, pg.ThawWriters(ctx), "pending active vector cleanup")
		_, cleanupErr = runSuperseded(ctx, cfg, index, pg, host, confirm(t), io.Discard)
		require.NoError(t, cleanupErr)
		require.Equal(t, [][]string{{partialID}}, index.deleted[before:])
	})

	t.Run("concurrent recovery", func(t *testing.T) {
		bare(t, partialID, doc, originalIncarnation)
		index.deleteErr = context.DeadlineExceeded
		_, cleanupErr := runSuperseded(ctx, cfg, index, pg, host, opts, io.Discard)
		require.ErrorIs(t, cleanupErr, context.DeadlineExceeded)
		confirmed := confirm(t)
		// The first request settled before recovery. No retry should be dispatched.
		delete(index.vectors, partialID)
		index.deleteErr = nil
		before := len(index.deleted)
		clearing, allowClear, done := make(chan struct{}, 1), make(chan struct{}), make(chan struct{})
		recoverCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		t.Cleanup(func() {
			cancel()
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Error("recovery still running after cancellation")
			}
		})
		var recoveryErr error
		go func() {
			defer close(done)
			_, recoveryErr = runSuperseded(recoverCtx, cfg, index, pg, host, confirmed, cleanupDiagnosticFunc(func(p []byte) (int, error) {
				if strings.Contains(string(p), "restarting the scan") {
					clearing <- struct{}{}
					select {
					case <-allowClear:
					case <-recoverCtx.Done():
						return 0, recoverCtx.Err()
					}
				}
				return len(p), nil
			}))
		}()
		select {
		case <-clearing:
		case <-done:
			t.Fatalf("recovery exited before clear: %v", recoveryErr)
		case <-recoverCtx.Done():
			t.Fatal("recovery did not reach clear")
		}
		_, cleanupErr = runSuperseded(recoverCtx, cfg, index, pg, host, confirmed, io.Discard)
		require.ErrorContains(t, cleanupErr, "another operation owns")
		close(allowClear)
		select {
		case <-done:
			require.NoError(t, recoveryErr)
		case <-recoverCtx.Done():
			t.Fatal("recovery did not finish")
		}
		_, cleanupErr = runSuperseded(ctx, cfg, index, pg, host, confirmed, io.Discard)
		require.ErrorContains(t, cleanupErr, "no batch is pending")
		require.Len(t, index.deleted, before)
	})

	bare(t, partialID, doc, originalIncarnation)
	index.onDelete = func() {
		terminateCleanupFence(t, pg)
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
						return pgproto3.NewFrontend(&lostCleanupReply{Reader: r, armed: &armed}, w)
					}
				})
				require.NoError(t, connectErr)
				t.Cleanup(func() { require.NoError(t, faulted.Close(context.WithoutCancel(ctx))) })
				armAfterDiagnostic := cleanupDiagnosticFunc(func(p []byte) (int, error) {
					if strings.Contains(string(p), "restarting the scan") {
						armed.Store(true)
					}
					return len(p), nil
				})
				beforeList := listCalls
				_, err = runSuperseded(ctx, cfg, index, faulted, host, operatorA, armAfterDiagnostic)
				require.ErrorContains(t, err, "clear active cleanup")
				require.False(t, armed.Load(), "the clear response must have been dropped")
				waitForCleanupFenceRelease(ctx, t, pg)
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
			require.NotEqual(t, operatorB.ConfirmSenderStopped, batchB.Sender)
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

func publishSupersededTestDocument(t *testing.T, pg *pgstore.Client, id string) (chunk domain.Chunk, vectorID, incarnation string) {
	t.Helper()
	ctx := t.Context()
	job, err := pg.ClaimJob(ctx, "manifest-worker", time.Minute, 5)
	require.NoError(t, err)
	require.NotNil(t, job)
	require.Equal(t, domain.DocumentID(id), job.Document.ID)
	job.RevisionID = uuid.NewString()
	chunk = domain.Chunk{ID: domain.ChunkID(uuid.NewString()), DocumentID: job.Document.ID, RevisionID: job.RevisionID, Text: "published text", TokenCount: 2}
	vectorID = domain.FormatRevisionVectorID(job.Document.ID, job.RevisionID, 0)
	require.NoError(t, pg.PersistVectorManifest(ctx, *job, domain.VectorManifest{
		RevisionID: job.RevisionID, DocumentID: job.Document.ID, Incarnation: job.Incarnation, Attempt: job.Attempts,
		LeaseToken: job.LeaseToken, IDPrefix: domain.RevisionVectorPrefix(job.Document.ID, job.RevisionID),
		VectorIDs: []string{vectorID}, ChunkIDs: []string{string(chunk.ID)},
	}))
	ok, err := pg.CompleteJob(ctx, *job, []domain.Chunk{chunk}, domain.IndexObservation{})
	require.NoError(t, err)
	require.True(t, ok)
	return chunk, vectorID, job.Incarnation
}

func TestSupersededSplitFetchDoesNotReservePartialPage(t *testing.T) {
	ctx := t.Context()
	pg := newDatabase(t)
	const host = "bound-host"
	cfg := &config.Config{
		VectorStore: "pinecone", PineconeIndexName: "bound", EmbeddingProvider: domain.EmbeddingProviderOpenAI,
		OpenAIEmbeddingModel: "text-embedding-3-small", EmbeddingModelRevision: "rev-1",
		ExtractionChunkVersion: "chunk-1", EmbeddingDimensions: 3, ChunkTokenSize: 512, ChunkOverlap: 64,
	}
	require.NoError(t, pg.PinVectorGeneration(ctx, pgstore.LegacyGeneration, "pinecone", "bound", host, cfg.EmbeddingSettings()))
	pg.EnableHostedVectorManifests(true)
	owner, content := uuid.NewString(), "manifested document"
	accepted, err := pg.AcceptDocument(ctx, domain.AcceptRequest{
		Document: domain.Document{ID: domain.DocumentID(owner), Content: content}, CallerSupplied: true,
		Fingerprint: chunker.Fingerprint(content), MaxPending: 1000,
	})
	require.NoError(t, err)
	require.Equal(t, domain.AcceptAccepted, accepted.Outcome)
	chunk, qualified, _ := publishSupersededTestDocument(t, pg, owner)
	require.NoError(t, pg.FreezeWriters(ctx, pgstore.LegacyGeneration))
	_, err = pg.Pool.Exec(ctx, `UPDATE vector_state SET frozen_at=now()-interval '2 minutes'`)
	require.NoError(t, err)

	first, second := strings.Repeat("f", 32), bareTestID
	revisioned, unowned, absent := strings.Repeat("a", 32), strings.Repeat("b", 32), strings.Repeat("c", 32)
	eligible := []string{first, second}
	page := []string{first, unowned, second, revisioned, absent}
	index := &supersededTestIndex{vectors: map[string]*pinecone.Vector{
		first:      supersededTestVector(t, first, map[string]any{"doc_id": owner}),
		second:     supersededTestVector(t, second, map[string]any{"doc_id": owner}),
		revisioned: supersededTestVector(t, revisioned, map[string]any{"doc_id": owner, domain.VectorRevisionKey: chunk.RevisionID}),
		qualified:  supersededTestVector(t, qualified, map[string]any{"doc_id": owner, domain.VectorRevisionKey: chunk.RevisionID}),
		unowned:    supersededTestVector(t, unowned, map[string]any{"doc_id": uuid.NewString()}),
	}}
	index.deleteHook = index.deleteVectors
	var listCalls int
	index.list = func(_ context.Context, req *pinecone.ListVectorsRequest) (*pinecone.ListVectorsResponse, error) {
		listCalls++
		switch {
		case req.PaginationToken == nil:
			return supersededTestList("start", qualified), nil
		case *req.PaginationToken == "start":
			return supersededTestList("after", page...), nil
		default:
			require.Equal(t, "after", *req.PaginationToken)
			return supersededTestList(""), nil
		}
	}
	journal := func() ([]string, uuid.NullUUID, uuid.NullUUID) {
		t.Helper()
		var ids []string
		var reservation, sender uuid.NullUUID
		require.NoError(t, pg.Pool.QueryRow(ctx, `SELECT active_cleanup_ids, active_cleanup_reservation, active_cleanup_sender FROM vector_state`).
			Scan(&ids, &reservation, &sender))
		return ids, reservation, sender
	}
	requireEmptyJournal := func() {
		t.Helper()
		ids, reservation, sender := journal()
		require.Empty(t, ids)
		require.False(t, reservation.Valid)
		require.False(t, sender.Valid)
	}
	start, err := runSuperseded(ctx, cfg, index, pg, host, options{SupersededUnrevisioned: true, Page: 1, Limit: 1, ScanLimit: 1}, io.Discard)
	require.NoError(t, err)
	require.True(t, strings.HasSuffix(start.NextPageToken, ".c3RhcnQ"))
	preview := options{SupersededUnrevisioned: true, Page: 5, Limit: 5, ScanLimit: 5, PageToken: start.NextPageToken}
	fitting, err := runSuperseded(ctx, cfg, index, pg, host, preview, io.Discard)
	require.NoError(t, err)
	require.Equal(t, eligible, fitting.Sample)

	dsn := pg.Pool.Config().ConnString()
	role := pgx.Identifier{pgtest.Role(t, dsn, "superseded_no_lookup")}.Sanitize()
	_, err = pg.Pool.Exec(ctx, `CREATE ROLE `+role+`; GRANT USAGE ON SCHEMA public TO `+role)
	require.NoError(t, err)
	var denyNextLookup atomic.Bool
	denied, err := pgstore.NewClient(ctx, dsn, func(pool *pgxpool.Config) {
		pool.PrepareConn = func(ctx context.Context, conn *pgx.Conn) (bool, error) {
			if !denyNextLookup.CompareAndSwap(true, false) {
				return true, nil
			}
			_, roleErr := conn.Exec(ctx, `SET ROLE `+role)
			return roleErr == nil, roleErr
		}
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, denied.Close(context.WithoutCancel(ctx))) })
	laterFailure := status.Error(codes.Unavailable, "later fetch unavailable")
	var rightError error
	var denyLookup bool
	var fetches [][]string
	index.fetch = func(_ context.Context, ids []string) (*pinecone.FetchVectorsResponse, error) {
		fetches = append(fetches, slices.Clone(ids))
		requireEmptyJournal()
		switch {
		case len(ids) == len(page):
			return nil, supersededTestSizeError()
		case ids[0] == second:
			if rightError != nil {
				return nil, rightError
			}
			if denyLookup {
				denyNextLookup.Store(true)
			}
		}
		return index.fetchListedVectors(ids), nil
	}
	split, err := runSuperseded(ctx, cfg, index, pg, host, preview, io.Discard)
	require.NoError(t, err)
	fittingJSON, err := json.Marshal(fitting)
	require.NoError(t, err)
	splitJSON, err := json.Marshal(split)
	require.NoError(t, err)
	require.JSONEq(t, string(fittingJSON), string(splitJSON))
	for _, next := range []string{fitting.NextPageToken, split.NextPageToken} {
		resume := preview
		resume.PageToken = next
		result, resumeErr := runSuperseded(ctx, cfg, index, pg, host, resume, io.Discard)
		require.NoError(t, resumeErr)
		require.Equal(t, supersededReport{}, result)
	}

	apply := preview
	apply.Apply = true
	t.Run("exclusive generation fence", func(t *testing.T) {
		fence, fenceErr := pg.FenceGeneration(ctx, pgstore.LegacyGeneration)
		require.NoError(t, fenceErr)
		defer fence.Close(ctx)
		fetches = nil
		_, fenceErr = runSuperseded(ctx, cfg, index, pg, host, apply, io.Discard)
		require.ErrorContains(t, fenceErr, "another operation owns generation")
		require.Empty(t, fetches)
		require.Empty(t, index.deleted)
		requireEmptyJournal()
	})
	_, err = runSuperseded(ctx, cfg, index, pg, "different-host", apply, io.Discard)
	require.ErrorIs(t, err, pgstore.ErrGenerationDrift)
	rightError = laterFailure
	fetches = nil
	failed, err := runSuperseded(ctx, cfg, index, pg, host, apply, io.Discard)
	require.ErrorIs(t, err, laterFailure)
	require.Equal(t, supersededReport{Scanned: 5, NextPageToken: start.NextPageToken}, failed)
	require.Equal(t, [][]string{page, page[:2], page[2:]}, fetches)
	require.Empty(t, index.deleted)
	requireEmptyJournal()

	rightError, denyLookup, fetches = nil, true, nil
	failed, err = runSuperseded(ctx, cfg, index, denied, host, apply, io.Discard)
	var deniedLookup *pgconn.PgError
	require.ErrorAs(t, err, &deniedLookup)
	require.Equal(t, "42501", deniedLookup.Code)
	require.ErrorContains(t, err, "superseded vector owners")
	require.False(t, denyNextLookup.Load())
	require.Equal(t, supersededReport{Scanned: 5, NextPageToken: start.NextPageToken}, failed)
	require.Equal(t, [][]string{page, page[:2], page[2:]}, fetches)
	require.Empty(t, index.deleted)
	requireEmptyJournal()

	var lastReservation, lastSender uuid.UUID
	index.onDelete = func() {
		ids, reservation, sender := journal()
		require.Equal(t, eligible, ids)
		require.True(t, reservation.Valid)
		require.NotEqual(t, lastReservation, reservation.UUID)
		require.True(t, sender.Valid)
		require.NotEqual(t, lastSender, sender.UUID)
		lastReservation = reservation.UUID
		lastSender = sender.UUID
		require.ErrorIs(t, pg.WritersOpen(ctx), domain.ErrWritersFrozen)
	}
	denyLookup, fetches = false, nil
	resumed, err := runSuperseded(ctx, cfg, index, pg, host, apply, io.Discard)
	require.NoError(t, err)
	want := fitting
	want.Deleted = len(eligible)
	require.Equal(t, want, resumed)
	require.Equal(t, [][]string{page, page[:2], page[2:]}, fetches)
	require.Equal(t, [][]string{eligible}, index.deleted)
	require.NotContains(t, index.vectors, first)
	require.NotContains(t, index.vectors, second)
	requireEmptyJournal()

	for _, id := range eligible {
		index.vectors[id] = supersededTestVector(t, id, map[string]any{"doc_id": owner})
	}
	index.deleteErr = context.DeadlineExceeded
	ambiguous, err := runSuperseded(ctx, cfg, index, pg, host, apply, io.Discard)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Zero(t, ambiguous.Deleted)
	require.Equal(t, start.NextPageToken, ambiguous.NextPageToken)
	ids, pending, sender := journal()
	require.True(t, pending.Valid)
	require.True(t, sender.Valid)
	require.Equal(t, eligible, ids)
	require.Equal(t, lastReservation, pending.UUID)
	require.Equal(t, lastSender, sender.UUID)
	requireHeldJournal := func() {
		t.Helper()
		actualIDs, actualReservation, actualSender := journal()
		require.Equal(t, eligible, actualIDs)
		require.Equal(t, pending, actualReservation)
		require.Equal(t, sender, actualSender)
		require.ErrorContains(t, pg.ThawWriters(ctx), "pending active vector cleanup")
	}
	requireHeldJournal()
	recovery := apply
	recovery.PageToken = ""
	recovery.ScanLimit++
	listCallsBeforeRecovery := listCalls
	fetches = nil
	_, err = runSuperseded(ctx, cfg, index, pg, host, recovery, io.Discard)
	require.ErrorContains(t, err, "unconfirmed active cleanup for generation")
	require.ErrorContains(t, err, "-confirm-sender-stopped="+sender.UUID.String()+" -confirm-target-drained="+pending.UUID.String())
	requireHeldJournal()
	recovery.ConfirmTargetDrained = pending.UUID
	_, err = runSuperseded(ctx, cfg, index, pg, host, recovery, io.Discard)
	require.ErrorContains(t, err, "stale active cleanup confirmation")
	requireHeldJournal()
	require.Equal(t, listCallsBeforeRecovery, listCalls)
	require.Empty(t, fetches)
	require.Equal(t, [][]string{eligible, eligible}, index.deleted)
	for _, id := range eligible {
		delete(index.vectors, id)
	}
	index.deleteErr = nil
	recovery.ConfirmSenderStopped = sender.UUID
	recovered, err := runSuperseded(ctx, cfg, index, pg, host, recovery, io.Discard)
	require.NoError(t, err)
	require.Equal(t, len(page)+1, recovered.Scanned)
	require.Zero(t, recovered.Selected)
	require.Zero(t, recovered.Deleted)
	require.Equal(t, [][]string{eligible, eligible}, index.deleted)
	requireEmptyJournal()
	require.ErrorIs(t, pg.WritersOpen(ctx), domain.ErrWritersFrozen)
	for _, id := range []string{revisioned, qualified, unowned} {
		require.Contains(t, index.vectors, id)
	}
}

type cleanupDiagnosticFunc func([]byte) (int, error)

func (f cleanupDiagnosticFunc) Write(p []byte) (int, error) { return f(p) }

func waitForCleanupFenceRelease(ctx context.Context, t *testing.T, pg *pgstore.Client) {
	t.Helper()
	require.Eventually(t, func() bool {
		var locked bool
		err := pg.Pool.QueryRow(ctx, `SELECT EXISTS (
			SELECT FROM pg_locks WHERE locktype='advisory' AND granted
			AND database=(SELECT oid FROM pg_database WHERE datname=current_database())
		)`).Scan(&locked)
		return err == nil && !locked
	}, 5*time.Second, 10*time.Millisecond)
}

type cleanupQueryTrace func(string)

func (trace cleanupQueryTrace) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	trace(data.SQL)
	return ctx
}

func (cleanupQueryTrace) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

// lostCleanupReply drops the idle ReadyForQuery after the statement commits.
type lostCleanupReply struct {
	io.Reader
	armed *atomic.Bool
	tail  []byte
}

func (r *lostCleanupReply) Read(p []byte) (int, error) {
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
