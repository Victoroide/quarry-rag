package postgres

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/testutil/pgtest"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
)

func scopedFileCostFixture(t testing.TB) (*ownedFixture, domain.DocumentID) {
	t.Helper()
	dsn := pgtest.StartIsolated(t, testcontainers.WithCmd("postgres",
		"-c", "autovacuum=off", "-c", "checkpoint_timeout=1h",
		"-c", "full_page_writes=off", "-c", "fsync=on", "-c", "synchronous_commit=on"))
	c := migrationTestClient(t, dsn)
	require.NoError(t, c.RunMigrations(t.Context()))
	f := provisionOwnedFixture(t, c)
	id := domain.DocumentID(f.acceptOwned(t, f.owner, "", "documents/original.pdf"))
	return f, id
}

type fileSigningCounters struct {
	walRecords, walBytes, fullPageImages          int64
	multixactBlocksWritten, multixactBlocksZeroed int64
}

func readFileSigningCounters(ctx context.Context, t *testing.T, c *Client) fileSigningCounters {
	t.Helper()
	var out fileSigningCounters
	require.NoError(t, c.Pool.QueryRow(ctx, `SELECT wal_records,wal_bytes::bigint,wal_fpi,
		(SELECT sum(blks_written)::bigint FROM pg_stat_slru WHERE name IN ('MultiXactMember','MultiXactOffset')),
		(SELECT sum(blks_zeroed)::bigint FROM pg_stat_slru WHERE name IN ('MultiXactMember','MultiXactOffset'))
		FROM pg_stat_wal`).Scan(&out.walRecords, &out.walBytes, &out.fullPageImages, &out.multixactBlocksWritten, &out.multixactBlocksZeroed))
	return out
}

func fileSigningTupleMarkers(ctx context.Context, t *testing.T, f *ownedFixture, id domain.DocumentID) [2]string {
	t.Helper()
	var markers [2]string
	require.NoError(t, f.c.Pool.QueryRow(ctx, `SELECT d.xmax::text,p.xmax::text
		FROM documents d CROSS JOIN grant_catalog_principals p
		WHERE d.id=$1 AND p.principal_id=$2`, string(id), f.owner.ID).Scan(&markers[0], &markers[1]))
	return markers
}

func TestScopedFileSigningAuditAndLockCost(t *testing.T) {
	f, id := scopedFileCostFixture(t)
	for _, checkpointed := range []bool{true, false} {
		for _, readers := range []int{1, 8} {
			t.Run(fmt.Sprintf("checkpointed=%t/readers=%d", checkpointed, readers), func(t *testing.T) {
				ctx := scopedFileTestContext(t)
				_, err := f.c.Pool.Exec(ctx, `UPDATE document_deletion_activation
					SET activated_at=CASE WHEN $1::boolean THEN clock_timestamp() ELSE NULL END`, checkpointed)
				require.NoError(t, err)
				// Warm the audit INSERT, then flush setup and the previous phase
				// before sampling the isolated server's cumulative counters.
				_, err = f.c.WithScopedDocumentFile(ctx, f.owner, id, noopFileSign)
				require.NoError(t, err)
				_, err = f.c.Pool.Exec(ctx, `SELECT pg_stat_force_next_flush()`)
				require.NoError(t, err)
				_, err = f.c.Pool.Exec(ctx, `CHECKPOINT`)
				require.NoError(t, err)
				audits, err := scopedFileAuditCount(ctx, f.c, id)
				require.NoError(t, err)
				before := readFileSigningCounters(ctx, t, f.c)
				markers := fileSigningTupleMarkers(ctx, t, f, id)
				ready, release, signed := launchFileCostSigners(ctx, f, id, readers)
				requireFileCostReady(ctx, t, ready, readers, checkpointed)
				during := fileSigningTupleMarkers(ctx, t, f, id)
				if checkpointed {
					require.Equal(t, markers, during)
				} else {
					require.NotEqual(t, markers, during)
					if readers > 1 {
						requireFileMultixactMembers(ctx, t, f.c, during, readers)
					}
				}
				close(release)
				for range readers {
					require.NoError(t, awaitScopedFile(ctx, t, signed))
				}
				after := readFileSigningCounters(ctx, t, f.c)
				// Read WAL before the measurement checkpoint, then measure SLRU
				// after dirty multixact buffers have had a chance to flush.
				_, err = f.c.Pool.Exec(ctx, `CHECKPOINT`)
				require.NoError(t, err)
				slru := readFileSigningCounters(ctx, t, f.c)
				n, err := scopedFileAuditCount(ctx, f.c, id)
				require.NoError(t, err)
				require.Equal(t, audits+readers, n)
				t.Logf("full transaction including allow audit: %.2f WAL records/op, %.2f WAL bytes/op, full page images=%d, multixact SLRU blocks written=%d zeroed=%d, tuple xmax before=%v during=%v",
					float64(after.walRecords-before.walRecords)/float64(readers), float64(after.walBytes-before.walBytes)/float64(readers),
					after.fullPageImages-before.fullPageImages,
					slru.multixactBlocksWritten-before.multixactBlocksWritten, slru.multixactBlocksZeroed-before.multixactBlocksZeroed, markers, during)

				// Row probes run outside the sample because a successful probe
				// writes tuple-lock records of its own.
				ready, release, signed = launchFileCostSigners(ctx, f, id, readers)
				requireFileCostReady(ctx, t, ready, readers, checkpointed)
				probeFileSigningRows(ctx, t, f, id, checkpointed)
				close(release)
				for range readers {
					require.NoError(t, awaitScopedFile(ctx, t, signed))
				}
				n, err = scopedFileAuditCount(ctx, f.c, id)
				require.NoError(t, err)
				require.Equal(t, audits+2*readers, n)
			})
		}
	}
}

func requireFileMultixactMembers(ctx context.Context, t *testing.T, c *Client, markers [2]string, readers int) {
	t.Helper()
	for _, xmax := range markers {
		var members int
		require.NoError(t, c.Pool.QueryRow(ctx, `SELECT count(*) FROM pg_get_multixact_members($1::text::xid)`, xmax).Scan(&members))
		require.Equal(t, readers, members)
	}
}

func probeFileSigningRows(ctx context.Context, t *testing.T, f *ownedFixture, id domain.DocumentID, checkpointed bool) {
	t.Helper()
	conn, err := f.c.Pool.Acquire(ctx)
	require.NoError(t, err)
	defer conn.Release()
	for _, probe := range []struct{ sql, id string }{
		{`SELECT id FROM documents WHERE id=$1 FOR UPDATE NOWAIT`, string(id)},
		{`SELECT principal_id FROM grant_catalog_principals WHERE principal_id=$1 FOR UPDATE NOWAIT`, f.owner.ID},
	} {
		_, probeErr := conn.Exec(ctx, probe.sql, probe.id)
		if checkpointed {
			require.NoError(t, probeErr)
		} else {
			requireSQLState(t, probeErr, "55P03")
		}
	}
	_, err = conn.Exec(ctx, `SELECT pg_stat_force_next_flush()`)
	require.NoError(t, err)
	_, err = conn.Exec(ctx, `SELECT 1`)
	require.NoError(t, err)
}

type fileCostReady struct {
	xid *string
	err error
}

func launchFileCostSigners(ctx context.Context, f *ownedFixture, id domain.DocumentID, readers int) (ready <-chan fileCostReady, release chan struct{}, done <-chan error) {
	signersReady := make(chan fileCostReady, readers)
	allowAudit := make(chan struct{})
	signersDone := make(chan error, readers)
	for range readers {
		go func() {
			signersDone <- runFileCostSigner(ctx, f, id, signersReady, allowAudit)
		}()
	}
	return signersReady, allowAudit, signersDone
}

func requireFileCostReady(ctx context.Context, t *testing.T, ready <-chan fileCostReady, readers int, checkpointed bool) {
	t.Helper()
	for range readers {
		signer := awaitScopedFile(ctx, t, ready)
		require.NoError(t, signer.err)
		if checkpointed {
			require.Nil(t, signer.xid, "authorization must not assign an XID before audit")
		} else {
			require.NotNil(t, signer.xid, "the row-locked control assigns an XID")
		}
	}
}

// Use the production authorization and audit functions and expose the
// transaction to the XID probe immediately before the allow audit.
func runFileCostSigner(ctx context.Context, f *ownedFixture, id domain.DocumentID, ready chan<- fileCostReady, release <-chan struct{}) error {
	conn, err := f.c.Pool.Acquire(ctx)
	if err != nil {
		ready <- fileCostReady{err: err}
		return err
	}
	defer conn.Release()
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		ready <- fileCostReady{err: err}
		return err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	var checkpointed bool
	if activationErr := tx.QueryRow(ctx, `SELECT document_deletion_active()`).Scan(&checkpointed); activationErr != nil {
		ready <- fileCostReady{err: activationErr}
		return activationErr
	}
	active, source, err := readScopedSignableSource(ctx, tx, f.owner, id, checkpointed)
	if err == nil && (!active || source == nil) {
		err = fmt.Errorf("missing source for authorized signing")
	}
	if err == nil {
		err = noopFileSign(ctx, *source)
	}
	var xid *string
	if err == nil {
		err = tx.QueryRow(ctx, `SELECT pg_current_xact_id_if_assigned()::text`).Scan(&xid)
	}
	ready <- fileCostReady{xid, err}
	if err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-release:
	}
	if auditErr := recordSecurityAuditTx(ctx, tx, domain.SecurityAuditEvent{
		TenantID: f.owner.TenantID, PrincipalID: f.owner.ID, DocumentID: string(id),
		Action: "file_sign", Outcome: "allow", Reason: "ok",
	}); auditErr != nil {
		return auditErr
	}
	if _, flushErr := tx.Exec(ctx, `SELECT pg_stat_force_next_flush()`); flushErr != nil {
		return flushErr
	}
	// The next statement on this connection cannot begin until the server
	// returns to idle and flushes the signing transaction's statistics.
	if commitErr := tx.Commit(ctx); commitErr != nil {
		return commitErr
	}
	_, err = conn.Exec(ctx, `SELECT 1`)
	return err
}

func BenchmarkScopedFileSigning(b *testing.B) {
	f, id := scopedFileCostFixture(b)
	for _, checkpointed := range []bool{false, true} {
		for _, readers := range []int{1, 8} {
			b.Run(fmt.Sprintf("checkpointed=%t/readers=%d", checkpointed, readers), func(b *testing.B) {
				_, err := f.c.Pool.Exec(b.Context(), `UPDATE document_deletion_activation
					SET activated_at=CASE WHEN $1::boolean THEN clock_timestamp() ELSE NULL END`, checkpointed)
				require.NoError(b, err)
				benchmarkScopedFileSigning(b, f, id, readers)
			})
		}
	}
}

func benchmarkScopedFileSigning(b *testing.B, f *ownedFixture, id domain.DocumentID, readers int) {
	ctx, cancel := context.WithTimeout(b.Context(), 5*time.Minute)
	defer cancel()
	work := make(chan struct{})
	results := make(chan error, readers)
	for range readers {
		go func() {
			var firstErr error
			for range work {
				res, err := f.c.WithScopedDocumentFile(ctx, f.owner, id, noopFileSign)
				if err == nil && (!res.Active || !res.Found) {
					err = fmt.Errorf("benchmark signer returned %+v", res)
				}
				if firstErr == nil {
					firstErr = err
				}
			}
			results <- firstErr
		}()
	}
	b.ResetTimer()
	for range b.N {
		work <- struct{}{}
	}
	close(work)
	var firstErr error
	for range readers {
		if err := <-results; firstErr == nil {
			firstErr = err
		}
	}
	b.StopTimer()
	require.NoError(b, firstErr)
}
