package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

const (
	documentAdvisorySpace  = 1
	principalAdvisorySpace = 2
)

func TestScopedFileAdvisoryOrdering(t *testing.T) {
	f := newOwnedFixture(t)
	grantExec(t, f.c, `UPDATE document_deletion_activation SET activated_at=clock_timestamp()`)
	for _, scoped := range []bool{false, true} {
		name := "service"
		if scoped {
			name = "scoped"
		}
		t.Run(name+" intent", func(t *testing.T) {
			t.Run("sign first", func(t *testing.T) { scopedFileSignBeforeIntent(t, f, scoped) })
			t.Run("intent first", func(t *testing.T) { scopedFileIntentBeforeSign(t, f, scoped) })
		})
	}
	t.Run("operator import", func(t *testing.T) { testScopedFileImportOrdering(t, f) })
	t.Run("ordered overlapping imports", func(t *testing.T) { testScopedFileOverlappingImports(t, f) })
	t.Run("canceled signing", func(t *testing.T) { testScopedFileLockCancellation(t, f) })
}

func beginFileTestIntent(ctx context.Context, c *Client, p domain.Principal, id domain.DocumentID, incarnation string, scoped bool) error {
	ops := newDeletionRecorder().ops(incarnation)
	ops.InMemory, ops.MemoryOwner = true, uuid.NewString()
	if scoped {
		_, _, err := c.BeginScopedDeletion(ctx, p, time.Now().Add(time.Minute), id, ops)
		return err
	}
	_, _, err := c.beginDeletion(ctx, id, ops)
	return err
}

type scopedFileAttempt struct {
	result domain.ScopedFileResult
	err    error
}

func startScopedFileAttempt(ctx context.Context, c *Client, p domain.Principal, id domain.DocumentID, sign func(context.Context, string) error) <-chan scopedFileAttempt {
	done := make(chan scopedFileAttempt, 1)
	go func() {
		result, err := c.WithScopedDocumentFile(ctx, p, id, sign)
		done <- scopedFileAttempt{result, err}
	}()
	return done
}

func awaitScopedFile[T any](ctx context.Context, t *testing.T, done <-chan T) T {
	t.Helper()
	select {
	case result := <-done:
		return result
	case <-ctx.Done():
		t.Fatalf("waiting for scoped signing test: %v", ctx.Err())
		var zero T
		return zero
	}
}

func scopedFileTestContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func waitForScopedFileLock(ctx context.Context, t *testing.T, c *Client, space int, mode string, granted bool) {
	t.Helper()
	require.Eventually(t, func() bool {
		var found bool
		err := c.Pool.QueryRow(ctx, `SELECT EXISTS (
			SELECT FROM pg_locks WHERE locktype='advisory' AND objsubid=$1 AND mode=$2 AND granted=$3
			AND database=(SELECT oid FROM pg_database WHERE datname=current_database())
			AND ($1<>2 OR classid::bigint=$4))`, space, mode, granted, scopedFilePrincipalLockClass).Scan(&found)
		return err == nil && found
	}, 10*time.Second, 10*time.Millisecond, "advisory key space %d mode %s granted=%v", space, mode, granted)
}

func gatedScopedFileSigner(inside chan<- struct{}, release <-chan struct{}) func(context.Context, string) error {
	return func(ctx context.Context, _ string) error {
		inside <- struct{}{}
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func scopedFileAuditCount(ctx context.Context, c *Client, id domain.DocumentID) (int, error) {
	var n int
	err := c.Pool.QueryRow(ctx, `SELECT count(*) FROM grant_catalog_security_audit
		WHERE action='file_sign' AND outcome='allow' AND document_id=$1`, string(id)).Scan(&n)
	return n, err
}

func scopedFileSignBeforeIntent(t *testing.T, f *ownedFixture, scoped bool) {
	ctx := scopedFileTestContext(t)
	id := domain.DocumentID(f.acceptOwned(t, f.owner, "", "documents/original.pdf"))
	inc := storedIncarnation(t, f.c, id)
	inside, release := make(chan struct{}, 1), make(chan struct{})
	// The writer uses the regular UUID spelling while the signer uses upper case.
	signing := startScopedFileAttempt(ctx, f.c, f.owner, domain.DocumentID(strings.ToUpper(string(id))), gatedScopedFileSigner(inside, release))
	awaitScopedFile(ctx, t, inside)
	writing := make(chan error, 1)
	go func() { writing <- beginFileTestIntent(ctx, f.c, f.owner, id, inc, scoped) }()
	waitForScopedFileLock(ctx, t, f.c, documentAdvisorySpace, "ExclusiveLock", false)
	require.NotEqual(t, "deleting", documentStatus(t, f.c, id))
	close(release)
	res := awaitScopedFile(ctx, t, signing)
	require.NoError(t, res.err)
	require.True(t, res.result.Active && res.result.Found)
	require.NoError(t, awaitScopedFile(ctx, t, writing))
	require.Equal(t, "deleting", documentStatus(t, f.c, id))
	n, err := scopedFileAuditCount(ctx, f.c, id)
	require.NoError(t, err)
	require.Equal(t, 1, n)
}

func scopedFileIntentBeforeSign(t *testing.T, f *ownedFixture, scoped bool) {
	ctx := scopedFileTestContext(t)
	id := domain.DocumentID(f.acceptOwned(t, f.owner, "", "documents/original.pdf"))
	inc := storedIncarnation(t, f.c, id)
	latch, err := f.c.Pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = latch.Rollback(context.WithoutCancel(ctx)) }()
	_, err = latch.Exec(ctx, `SELECT id FROM documents WHERE id=$1 FOR UPDATE`, string(id))
	require.NoError(t, err)
	writing := make(chan error, 1)
	go func() { writing <- beginFileTestIntent(ctx, f.c, f.owner, id, inc, scoped) }()
	waitForScopedFileLock(ctx, t, f.c, documentAdvisorySpace, "ExclusiveLock", true)
	signing := startScopedFileAttempt(ctx, f.c, f.owner, id, unexpectedScopedFileSignature)
	waitForScopedFileLock(ctx, t, f.c, documentAdvisorySpace, "ShareLock", false)
	require.NoError(t, latch.Commit(ctx))
	require.NoError(t, awaitScopedFile(ctx, t, writing))
	res := awaitScopedFile(ctx, t, signing)
	require.NoError(t, res.err)
	require.Equal(t, domain.ScopedFileResult{Active: true}, res.result)
	n, err := scopedFileAuditCount(ctx, f.c, id)
	require.NoError(t, err)
	require.Zero(t, n)
}

func fileGrant(p domain.Principal, id domain.DocumentID, incarnation string, read, download bool) grantEntry {
	return grantEntry{
		"tenant_id": p.TenantID, "principal_id": p.ID, "document_id": string(id),
		"incarnation": incarnation, "can_read": read, "can_download": download,
	}
}

func testScopedFileImportOrdering(t *testing.T, f *ownedFixture) {
	for _, mutation := range []string{"disable", "downgrade", "revoke", "enable", "rollback"} {
		t.Run(mutation, func(t *testing.T) {
			id := domain.DocumentID(f.acceptOwned(t, f.owner, "", "documents/original.pdf"))
			inc := storedIncarnation(t, f.c, id)
			f.setEnabled(t, f.peer, "peer", mutation != "enable")
			f.grant(t, f.peer, string(id), inc, true, true)
			changes := map[string]any{"principals": []grantEntry{grantPrincipal(f.tenant, f.peer.ID, "peer", mutation == "enable")}}
			if mutation == "downgrade" || mutation == "revoke" {
				changes = map[string]any{"grants": []grantEntry{fileGrant(f.peer, id, inc, mutation == "downgrade", false)}}
			}
			if mutation != "enable" && mutation != "rollback" {
				t.Run("sign first", func(t *testing.T) {
					scopedFileSignBeforeImport(t, f, id, fileImportBody(t, f, changes))
				})
				f.setEnabled(t, f.peer, "peer", true)
				f.grant(t, f.peer, string(id), inc, true, true)
			}
			t.Run("import first", func(t *testing.T) {
				ctx := scopedFileTestContext(t)
				writer, err := f.c.Pool.Begin(ctx)
				require.NoError(t, err)
				defer func() { _ = writer.Rollback(context.WithoutCancel(ctx)) }()
				_, err = writer.Exec(ctx, `SELECT * FROM grant_catalog_import_batch($1,false)`, fileImportBody(t, f, changes))
				require.NoError(t, err)
				sign := unexpectedScopedFileSignature
				want := domain.ScopedFileResult{Active: mutation != "disable"}
				if mutation == "enable" || mutation == "rollback" {
					want.Found = true
					sign = func(context.Context, string) error { return nil }
				}
				signing := startScopedFileAttempt(ctx, f.c, f.peer, id, sign)
				waitForScopedFileLock(ctx, t, f.c, principalAdvisorySpace, "ShareLock", false)
				if mutation == "rollback" {
					require.NoError(t, writer.Rollback(ctx))
				} else {
					require.NoError(t, writer.Commit(ctx))
				}
				res := awaitScopedFile(ctx, t, signing)
				require.NoError(t, res.err)
				require.Equal(t, want, res.result)
			})
		})
	}
}

func scopedFileSignBeforeImport(t *testing.T, f *ownedFixture, id domain.DocumentID, body string) {
	ctx := scopedFileTestContext(t)
	const readers = 4
	inside, release := make(chan struct{}, readers), make(chan struct{})
	signing := make([]<-chan scopedFileAttempt, readers)
	for i := range signing {
		signing[i] = startScopedFileAttempt(ctx, f.c, f.peer, id, gatedScopedFileSigner(inside, release))
	}
	for range signing {
		awaitScopedFile(ctx, t, inside)
	}
	written := make(chan error, 1)
	go func() {
		tx, err := f.c.Pool.Begin(ctx)
		if err != nil {
			written <- err
			return
		}
		defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
		_, err = tx.Exec(ctx, `SELECT * FROM grant_catalog_import_batch($1,false)`, body)
		if err == nil {
			var n int
			err = tx.QueryRow(ctx, `SELECT count(*) FROM grant_catalog_security_audit
				WHERE action='file_sign' AND outcome='allow' AND principal_id=$1 AND document_id=$2`, f.peer.ID, string(id)).Scan(&n)
			if err == nil && n != readers {
				err = fmt.Errorf("import observed %d committed signing audits, want %d", n, readers)
			}
		}
		if err == nil {
			err = tx.Commit(ctx)
		}
		written <- err
	}()
	waitForScopedFileLock(ctx, t, f.c, principalAdvisorySpace, "ExclusiveLock", false)

	// These writes leave authorization unchanged and need no advisory key.
	_, err := f.c.Pool.Exec(ctx, `UPDATE documents SET updated_at=clock_timestamp() WHERE id=$1`, string(id))
	require.NoError(t, err)
	unrelated, err := f.c.WithScopedDocumentFile(ctx, f.owner, id, func(context.Context, string) error { return nil })
	require.NoError(t, err)
	require.True(t, unrelated.Found)
	close(release)
	for _, done := range signing {
		res := awaitScopedFile(ctx, t, done)
		require.NoError(t, res.err)
		require.True(t, res.result.Found)
	}
	require.NoError(t, awaitScopedFile(ctx, t, written))
}

func testScopedFileOverlappingImports(t *testing.T, f *ownedFixture) {
	ctx := scopedFileTestContext(t)
	id := domain.DocumentID(f.acceptOwned(t, f.owner, "", "documents/original.pdf"))
	inc := storedIncarnation(t, f.c, id)
	first := fileGrant(f.owner, id, inc, true, true)
	second := fileGrant(f.peer, id, inc, true, true)
	var lower, higher int32
	require.NoError(t, f.c.Pool.QueryRow(ctx, `SELECT grant_catalog_file_principal_key($1,$2), grant_catalog_file_principal_key($1,$3)`,
		f.tenant, f.owner.ID, f.peer.ID).Scan(&lower, &higher))
	require.NotEqual(t, lower, higher)
	if lower > higher {
		lower, higher = higher, lower
		first, second = second, first
	}
	forward := fileImportBody(t, f, map[string]any{"grants": []grantEntry{first, second}})
	reverse := fileImportBody(t, f, map[string]any{"grants": []grantEntry{second, first}})

	// Hold the larger key. The import must hold the smaller one before it
	// queues here, irrespective of the input order and overlapping document.
	latch, err := f.c.Pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = latch.Rollback(context.WithoutCancel(ctx)) }()
	_, err = latch.Exec(ctx, `SELECT pg_advisory_xact_lock($1,$2)`, scopedFilePrincipalLockClass, higher)
	require.NoError(t, err)
	apply := func(raw string) <-chan error {
		done := make(chan error, 1)
		go func() {
			_, importErr := f.c.Pool.Exec(ctx, `SELECT * FROM grant_catalog_import_batch($1,false)`, raw)
			done <- importErr
		}()
		return done
	}
	one := apply(reverse)
	waitForScopedFileLock(ctx, t, f.c, principalAdvisorySpace, "ExclusiveLock", false)
	var acquired bool
	require.NoError(t, latch.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock($1,$2)`, scopedFilePrincipalLockClass, lower).Scan(&acquired))
	require.False(t, acquired, "import must already hold the smaller key")
	_, err = f.c.Pool.Exec(ctx, `SELECT * FROM grant_catalog_import_batch($1,true)`, forward)
	require.NoError(t, err, "preview does not wait for the principal key")
	two := apply(forward)
	require.NoError(t, latch.Commit(ctx))
	require.NoError(t, awaitScopedFile(ctx, t, one))
	requireSQLState(t, awaitScopedFile(ctx, t, two), "QG003")

	latch, err = f.c.Pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = latch.Rollback(context.WithoutCancel(ctx)) }()
	_, err = latch.Exec(ctx, `SELECT pg_advisory_xact_lock($1,$2)`, scopedFilePrincipalLockClass, higher)
	require.NoError(t, err)
	var replayed bool
	require.NoError(t, f.c.Pool.QueryRow(ctx, `SELECT replayed FROM grant_catalog_import_batch($1,false)`, reverse).Scan(&replayed))
	require.True(t, replayed, "committed replay does not wait for the principal key")
	newBody := fileImportBody(t, f, map[string]any{"grants": []grantEntry{second, first}})
	writerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	canceled := make(chan error, 1)
	go func() {
		_, err := f.c.Pool.Exec(writerCtx, `SELECT * FROM grant_catalog_import_batch($1,false)`, newBody)
		canceled <- err
	}()
	waitForScopedFileLock(ctx, t, f.c, principalAdvisorySpace, "ExclusiveLock", false)
	cancel()
	require.ErrorIs(t, awaitScopedFile(ctx, t, canceled), context.Canceled)
	require.Eventually(t, func() bool {
		err := latch.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock($1,$2)`, scopedFilePrincipalLockClass, lower).Scan(&acquired)
		return err == nil && acquired
	}, 10*time.Second, 10*time.Millisecond, "canceled import releases the first key")
}

func testScopedFileLockCancellation(t *testing.T, f *ownedFixture) {
	for _, space := range []int{documentAdvisorySpace, principalAdvisorySpace} {
		t.Run(fmt.Sprint(space), func(t *testing.T) {
			ctx := scopedFileTestContext(t)
			id := domain.DocumentID(f.acceptOwned(t, f.owner, "", "documents/original.pdf"))
			writer, err := f.c.Pool.Begin(ctx)
			require.NoError(t, err)
			defer func() { _ = writer.Rollback(context.WithoutCancel(ctx)) }()
			if space == documentAdvisorySpace {
				err = lockDocumentIntent(ctx, writer, id)
			} else {
				_, err = writer.Exec(ctx, `SELECT pg_advisory_xact_lock($1, grant_catalog_file_principal_key($2,$3))`,
					scopedFilePrincipalLockClass, f.tenant, f.owner.ID)
			}
			require.NoError(t, err)
			signCtx, cancel := context.WithCancel(ctx)
			defer cancel()
			signed := startScopedFileAttempt(signCtx, f.c, f.owner, id, unexpectedScopedFileSignature)
			waitForScopedFileLock(ctx, t, f.c, space, "ShareLock", false)
			cancel()
			res := awaitScopedFile(ctx, t, signed)
			require.ErrorIs(t, res.err, context.Canceled)
			require.Empty(t, res.result)
			require.NoError(t, writer.Rollback(ctx))
			requireScopedFileLocksReleased(ctx, t, f, id)
			n, err := scopedFileAuditCount(ctx, f.c, id)
			require.NoError(t, err)
			require.Zero(t, n)
		})
	}
}

func requireScopedFileLocksReleased(ctx context.Context, t *testing.T, f *ownedFixture, id domain.DocumentID) {
	t.Helper()
	// A canceled pgx connection can finish closing asynchronously.
	require.Eventually(t, func() bool {
		var available bool
		err := f.c.Pool.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock(`+documentIntentLockKey+`)
			AND pg_try_advisory_xact_lock($2, grant_catalog_file_principal_key($3,$4))`,
			string(id), scopedFilePrincipalLockClass, f.tenant, f.owner.ID).Scan(&available)
		return err == nil && available
	}, 10*time.Second, 10*time.Millisecond)
}

func TestScopedFileSigningFailures(t *testing.T) {
	f := newOwnedFixture(t)
	grantExec(t, f.c, `UPDATE document_deletion_activation SET activated_at=clock_timestamp()`)
	id := domain.DocumentID(f.acceptOwned(t, f.owner, "", "documents/original.pdf"))
	grantExec(t, f.c, `CREATE FUNCTION refuse_file_audit() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN RAISE check_violation USING MESSAGE = 'file audit rejected'; END;
		$$`)
	for _, trigger := range []struct {
		name, definition string
	}{
		{"audit", "CREATE TRIGGER fail_file_audit AFTER INSERT ON grant_catalog_security_audit"},
		{"commit", "CREATE CONSTRAINT TRIGGER fail_file_audit AFTER INSERT ON grant_catalog_security_audit DEFERRABLE INITIALLY DEFERRED"},
	} {
		t.Run(trigger.name, func(t *testing.T) {
			grantExec(t, f.c, trigger.definition+` FOR EACH ROW EXECUTE FUNCTION refuse_file_audit()`)
			t.Cleanup(func() {
				_, err := f.c.Pool.Exec(context.Background(), `DROP TRIGGER fail_file_audit ON grant_catalog_security_audit`)
				require.NoError(t, err)
			})
			ctx := scopedFileTestContext(t)
			called := false
			res, err := f.c.WithScopedDocumentFile(ctx, f.owner, id, func(context.Context, string) error {
				called = true
				return nil
			})
			require.True(t, called)
			requireSQLState(t, err, "23514")
			require.Empty(t, res)
			requireScopedFileLocksReleased(ctx, t, f, id)
			n, err := scopedFileAuditCount(ctx, f.c, id)
			require.NoError(t, err)
			require.Zero(t, n)
		})
	}
	t.Run("provider error", func(t *testing.T) {
		ctx := scopedFileTestContext(t)
		providerErr := errors.New("provider refused signature")
		res, err := f.c.WithScopedDocumentFile(ctx, f.owner, id, func(context.Context, string) error { return providerErr })
		require.ErrorIs(t, err, providerErr)
		require.Equal(t, domain.ScopedFileResult{Active: true}, res)
		requireScopedFileLocksReleased(ctx, t, f, id)
	})
	t.Run("callback canceled before audit", func(t *testing.T) {
		ctx := scopedFileTestContext(t)
		signCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		res, err := f.c.WithScopedDocumentFile(signCtx, f.owner, id, func(context.Context, string) error {
			cancel()
			return nil
		})
		require.ErrorIs(t, err, context.Canceled)
		require.Empty(t, res)
		requireScopedFileLocksReleased(ctx, t, f, id)
	})
	t.Run("batch error skips callback", func(t *testing.T) {
		ctx := scopedFileTestContext(t)
		broken := domain.Principal{TenantID: "invalid UUID", ID: f.owner.ID}
		res, err := f.c.WithScopedDocumentFile(ctx, broken, id, unexpectedScopedFileSignature)
		requireSQLState(t, err, "22P02")
		require.Empty(t, res)
		requireScopedFileLocksReleased(ctx, t, f, id)
	})
	n, err := scopedFileAuditCount(t.Context(), f.c, id)
	require.NoError(t, err)
	require.Zero(t, n)
}

type fileBatchCloseErrorTx struct {
	pgx.Tx
	err error
}

func (tx fileBatchCloseErrorTx) SendBatch(context.Context, *pgx.Batch) pgx.BatchResults {
	return fileBatchCloseError{err: tx.err}
}

type fileBatchCloseError struct {
	pgx.BatchResults
	err error
}

func (b fileBatchCloseError) Close() error { return b.err }

func TestScopedFileReturnsBatchCloseFailure(t *testing.T) {
	closeErr := errors.New("batch close failed")
	active, key, err := readScopedSignableSource(t.Context(), fileBatchCloseErrorTx{err: closeErr},
		domain.Principal{}, "", true)
	require.ErrorIs(t, err, closeErr)
	require.False(t, active)
	require.Nil(t, key)
}
