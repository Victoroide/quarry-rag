package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

const fixtureWriterPassword = "fixture-password"

func writerLogin(t *testing.T, dsn, login string, searchPath ...string) *Client {
	t.Helper()
	client, err := NewClient(t.Context(), dsn, func(config *pgxpool.Config) {
		config.ConnConfig.User = login
		config.ConnConfig.Password = fixtureWriterPassword
		if len(searchPath) != 0 {
			config.ConnConfig.RuntimeParams["search_path"] = searchPath[0]
		}
	})
	require.NoError(t, err)
	t.Cleanup(client.Pool.Close)
	return client
}

func writerFixtureSearchPath(path string) Option {
	return func(config *pgxpool.Config) {
		config.ConnConfig.RuntimeParams["search_path"] = path
	}
}

func TestWriterOperationBindsTheEffectiveCorpus(t *testing.T) {
	dsn := startTestDatabase(t)
	admin := migrationTestClient(t, dsn)
	roles := newGrantRoles(t, dsn, "schema_writer", "meta_owner")
	grantExec(t, admin, roles.sql(`CREATE EXTENSION IF NOT EXISTS pgcrypto;
		CREATE ROLE {schema_writer} LOGIN PASSWORD 'fixture-password';
		CREATE ROLE {meta_owner} LOGIN PASSWORD 'fixture-password';
		CREATE SCHEMA {schema_writer} AUTHORIZATION {schema_writer};
		CREATE SCHEMA "Mixed Metadata" AUTHORIZATION {meta_owner};
		GRANT USAGE ON SCHEMA "Mixed Metadata" TO {schema_writer}`))
	owner := writerLogin(t, dsn, roles.name("meta_owner"), `"Mixed Metadata",public`)
	require.NoError(t, owner.RunMigrations(t.Context()))
	grantExec(t, owner, roles.sql(`GRANT SELECT ON documents, schema_migrations TO {schema_writer};
		GRANT EXECUTE ON FUNCTION vector_writer_state() TO {schema_writer};
		GRANT EXECUTE ON FUNCTION vector_writer_operation_begin(uuid,uuid,text,text,text),
			vector_writer_operation_mark(uuid,uuid), vector_writer_operation_finish(uuid,uuid),
			vector_writer_operation_drain_status() TO {schema_writer};
		UPDATE vector_state SET writer_operations_enabled=TRUE`))
	writer := writerLogin(t, dsn, roles.name("schema_writer"), `"$user","Mixed Metadata",public`)
	callbacks := 0
	work := func(ctx context.Context) error {
		callbacks++
		var reserved bool
		err := owner.Pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM vector_writer_operations)`).Scan(&reserved)
		require.NoError(t, err)
		require.True(t, reserved, "the front schema must not hide an enabled corpus")
		return nil
	}
	run := func() error {
		return writer.RunWriterOperation(t.Context(), domain.WriterOperationReconcile, "scoped", work)
	}
	require.NoError(t, run())
	grantExec(t, writer, `CREATE FUNCTION vector_writer_operations_enabled() RETURNS boolean LANGUAGE sql AS 'SELECT false';
		CREATE FUNCTION vector_writer_operation_mark(uuid,uuid) RETURNS boolean LANGUAGE sql AS 'SELECT false';
		CREATE FUNCTION vector_writer_operation_finish(uuid,uuid) RETURNS boolean LANGUAGE sql AS 'SELECT false';
		CREATE FUNCTION vector_writer_operation_drain_status() RETURNS TABLE(pending_count bigint, operation_ids uuid[])
		LANGUAGE sql AS 'SELECT 0::bigint, ARRAY[]::uuid[]'`)
	require.NoError(t, run())
	require.Equal(t, 2, callbacks)
	require.NoError(t, writer.RunWriterOperation(t.Context(), domain.WriterOperationReconcile, "scoped", func(ctx context.Context) error {
		return domain.DispatchExternalWriter(ctx, func(context.Context) (bool, error) { return false, nil })
	}))
	control := migrationTestClient(t, dsn, writerFixtureSearchPath(roles.name("schema_writer")+`,"Mixed Metadata",public`))
	var pending *ErrWriterOperationsPending
	require.ErrorAs(t, control.FreezeWriters(t.Context(), "next"), &pending)
	require.Len(t, pending.OperationIDs, 1)

	grantExec(t, writer, `CREATE TABLE vector_state (singleton boolean)`)
	require.ErrorContains(t, run(), "ambiguous vector writer schema")
	require.ErrorContains(t, control.FreezeWriters(t.Context(), "next"), "ambiguous vector writer schema")
	grantExec(t, writer, `DROP TABLE vector_state;
		CREATE FUNCTION vector_writer_state() RETURNS TABLE(active text, frozen boolean, profile_hash text)
		LANGUAGE sql AS 'SELECT ''legacy''::text,false,''''::text'`)
	require.ErrorContains(t, run(), "ambiguous vector writer schema")
	grantExec(t, writer, `CREATE TABLE vector_state (singleton boolean);
		DROP FUNCTION vector_writer_state();
		CREATE FUNCTION vector_writer_state() RETURNS boolean LANGUAGE sql
		BEGIN ATOMIC SELECT singleton FROM vector_state; END`)
	require.ErrorContains(t, run(), "ambiguous vector writer schema")
	require.ErrorContains(t, control.FreezeWriters(t.Context(), "next"), "ambiguous vector writer schema")
	// Give the counterfeit gate the actual serving pin and binding. Neither
	// a view through to the real documents nor a copy can define the corpus.
	grantExec(t, writer, `DROP FUNCTION vector_writer_state(); DROP TABLE vector_state;
		DROP FUNCTION vector_writer_operations_enabled();
		CREATE TABLE vector_state (active text, frozen boolean, profile_hash text);
		INSERT INTO vector_state VALUES ('legacy', false, '');
		CREATE FUNCTION vector_writer_state() RETURNS TABLE(active text, frozen boolean, profile_hash text)
		LANGUAGE sql SECURITY DEFINER
		BEGIN ATOMIC SELECT s.active, s.frozen, s.profile_hash FROM vector_state s FOR SHARE; END;
		CREATE VIEW documents AS SELECT * FROM "Mixed Metadata".documents`)
	genuine := writerLogin(t, dsn, roles.name("schema_writer"), `"Mixed Metadata","$user",public`)
	for _, enabled := range []bool{true, false} {
		grantExec(t, owner, `DELETE FROM vector_writer_operations`)
		grantExec(t, owner, `UPDATE vector_state SET writer_operations_enabled=$1`, enabled)
		require.ErrorContains(t, run(), "ambiguous vector writer schema")
		require.ErrorContains(t, control.FreezeWriters(t.Context(), "next"), "ambiguous vector writer schema")
		require.NoError(t, genuine.RunWriterOperation(t.Context(), domain.WriterOperationReconcile, "explicit", func(ctx context.Context) error {
			require.Equal(t, enabled, domain.HasExternalWriterDispatch(ctx))
			return nil
		}))
		if enabled {
			grantExec(t, writer, `DROP VIEW documents; CREATE TABLE documents AS TABLE "Mixed Metadata".documents;
				CREATE TABLE schema_migrations AS TABLE "Mixed Metadata".schema_migrations;
				CREATE FUNCTION grant_catalog_corpus_authz_mode() RETURNS text
				LANGUAGE sql SECURITY DEFINER AS 'SELECT ''service''::text'`)
			require.ErrorContains(t, run(), "ambiguous vector writer schema")
			grantExec(t, writer, `DROP TABLE schema_migrations; DROP FUNCTION grant_catalog_corpus_authz_mode()`)
			require.ErrorContains(t, run(), "ambiguous vector writer schema")
		}
	}
	grantExec(t, writer, `DROP FUNCTION vector_writer_state(); DROP TABLE vector_state, documents`)
	grantExec(t, owner, `DROP FUNCTION vector_writer_operation_mark(uuid,uuid)`)
	require.ErrorContains(t, run(), "incomplete writer operation schema")
	require.Equal(t, 2, callbacks)
}

func TestWriterOperationSelectsEitherMigratedCorpus(t *testing.T) {
	ctx := t.Context()
	dsn := startTestDatabase(t)
	admin := migrationTestClient(t, dsn)
	roles := newGrantRoles(t, dsn, "corpus_writer")
	grantExec(t, admin, roles.sql(`CREATE ROLE {corpus_writer} LOGIN PASSWORD 'fixture-password';
		CREATE SCHEMA "First Corpus"; CREATE SCHEMA second_corpus; CREATE TABLE public.vector_state (sentinel integer);
		GRANT USAGE ON SCHEMA "First Corpus", second_corpus TO {corpus_writer}`))
	first := migrationTestClient(t, dsn, writerFixtureSearchPath(`"First Corpus",second_corpus,public`))
	second := migrationTestClient(t, dsn, writerFixtureSearchPath(`second_corpus,"First Corpus",public`))
	require.NoError(t, first.RunMigrations(ctx))
	require.NoError(t, second.RunMigrations(ctx))
	for _, corpus := range []*Client{first, second} {
		grantExec(t, corpus, roles.sql(`GRANT EXECUTE ON FUNCTION vector_writer_state(),
			vector_writer_operation_begin(uuid,uuid,text,text,text), vector_writer_operation_mark(uuid,uuid),
			vector_writer_operation_finish(uuid,uuid) TO {corpus_writer}`))
	}
	for _, test := range []struct {
		selected, other *Client
		path            string
	}{
		{first, second, `"First Corpus",second_corpus,public`},
		{second, first, `second_corpus,"First Corpus",public`},
	} {
		writer := writerLogin(t, dsn, roles.name("corpus_writer"), test.path)
		require.NoError(t, writer.RunWriterOperation(ctx, domain.WriterOperationReconcile, "disabled", func(ctx context.Context) error {
			require.False(t, domain.HasExternalWriterDispatch(ctx))
			return nil
		}))
		require.NoError(t, test.selected.FreezeWriters(ctx, "next"))
		require.NoError(t, test.selected.ThawWriters(ctx))
		grantExec(t, test.selected, `UPDATE vector_state SET writer_operations_enabled=TRUE`)
		require.NoError(t, writer.RunWriterOperation(ctx, domain.WriterOperationReconcile, "settled", func(ctx context.Context) error {
			return domain.DispatchExternalWriter(ctx, func(ctx context.Context) (bool, error) {
				var marked bool
				require.NoError(t, test.selected.Pool.QueryRow(ctx, `SELECT pending_provider FROM vector_writer_operations`).Scan(&marked))
				require.True(t, marked)
				return true, nil
			})
		}))
		require.NoError(t, writer.RunWriterOperation(ctx, domain.WriterOperationReconcile, "pending", func(ctx context.Context) error {
			return domain.DispatchExternalWriter(ctx, func(context.Context) (bool, error) { return false, nil })
		}))
		var pending *ErrWriterOperationsPending
		require.ErrorAs(t, test.selected.FreezeWriters(ctx, "next"), &pending)
		require.Len(t, pending.OperationIDs, 1)
		require.NoError(t, test.other.FreezeWriters(ctx, "next"))
		require.NoError(t, test.other.ThawWriters(ctx))
		grantExec(t, test.selected, `DELETE FROM vector_writer_operations WHERE operation_id=$1`, pending.OperationIDs[0])
		grantExec(t, test.selected, `UPDATE vector_state SET writer_operations_enabled=FALSE`)
	}
}

func TestWriterOperationDirectCutoverIsolation(t *testing.T) {
	const activate = `SELECT * FROM grant_catalog_activate_grants(false)`
	for _, isolation := range []pgx.TxIsoLevel{pgx.ReadCommitted, pgx.RepeatableRead, pgx.Serializable} {
		t.Run(string(isolation), func(t *testing.T) {
			ctx := t.Context()
			owner, _ := sessionTestClientWithPool(t, 2)
			grantExec(t, owner, `UPDATE document_deletion_activation SET activated_at=clock_timestamp()`)
			dsn := owner.Pool.Config().ConnString()
			roles := newGrantRoles(t, dsn, "cutover")
			grantExec(t, owner, roles.sql(`CREATE ROLE {cutover} LOGIN PASSWORD 'fixture-password';
				GRANT EXECUTE ON FUNCTION grant_catalog_activate_grants(boolean) TO {cutover}`))
			operator := writerLogin(t, dsn, roles.name("cutover"))
			tx, err := operator.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: isolation})
			require.NoError(t, err)
			defer rollbackFencedWriterTx(ctx, tx)
			_, err = tx.Exec(ctx, activate)
			require.NoError(t, err)
			require.NoError(t, tx.Rollback(ctx))

			tx, err = operator.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: isolation})
			require.NoError(t, err)
			defer rollbackFencedWriterTx(ctx, tx)
			var snapshot string
			require.NoError(t, tx.QueryRow(ctx, `SELECT pg_current_snapshot()::text`).Scan(&snapshot))
			require.NotEmpty(t, snapshot)
			grantExec(t, owner, `UPDATE vector_state SET writer_operations_enabled=TRUE`)
			require.NoError(t, owner.RunWriterOperation(ctx, domain.WriterOperationDelete, "document", func(ctx context.Context) error {
				return domain.DispatchExternalWriter(ctx, func(context.Context) (bool, error) { return false, nil })
			}))
			_, err = tx.Exec(ctx, activate)
			if isolation == pgx.ReadCommitted {
				requireSQLState(t, err, "QG007")
			} else {
				requireSQLState(t, err, "40001")
			}
			require.NoError(t, tx.Rollback(ctx))
			fresh, err := operator.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: isolation})
			require.NoError(t, err)
			defer rollbackFencedWriterTx(ctx, fresh)
			_, err = fresh.Exec(ctx, activate)
			requireSQLState(t, err, "QG007")
			require.NoError(t, fresh.Rollback(ctx))
			grantExec(t, owner, `DELETE FROM vector_writer_operations; UPDATE vector_state SET writer_operations_enabled=FALSE`)
			final, err := operator.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: isolation})
			require.NoError(t, err)
			defer rollbackFencedWriterTx(ctx, final)
			_, err = final.Exec(ctx, activate)
			require.NoError(t, err)
			require.NoError(t, final.Commit(ctx))
		})
	}
}

func TestWriterOperationModeRequiresAuthenticatedOwner(t *testing.T) {
	dsn := startTestDatabase(t)
	admin := migrationTestClient(t, dsn)
	roles := newGrantRoles(t, dsn, "meta_owner", "reindex")
	grantExec(t, admin, roles.sql(`CREATE EXTENSION IF NOT EXISTS pgcrypto;
		CREATE ROLE {meta_owner} LOGIN PASSWORD 'fixture-password';
		CREATE ROLE {reindex} LOGIN PASSWORD 'fixture-password';
		CREATE SCHEMA metadata AUTHORIZATION {meta_owner};
		GRANT USAGE ON SCHEMA metadata TO {reindex}`))
	owner := writerLogin(t, dsn, roles.name("meta_owner"), "metadata,public")
	require.NoError(t, owner.RunMigrations(t.Context()))
	grantExec(t, owner, roles.sql(`GRANT UPDATE (writer_operations_enabled) ON vector_state TO {reindex}`))
	reindex := writerLogin(t, dsn, roles.name("reindex"), "metadata,public")
	ctx := t.Context()
	_, err := reindex.Pool.Exec(ctx, `UPDATE vector_state SET writer_operations_enabled=TRUE`)
	requireSQLState(t, err, "42501")
	require.ErrorContains(t, err, "requires the metadata schema owner")
	grantExec(t, admin, roles.sql(`GRANT {meta_owner} TO {reindex} WITH INHERIT FALSE, SET FALSE`))
	_, err = reindex.Pool.Exec(ctx, "SET ROLE "+pgx.Identifier{roles.name("meta_owner")}.Sanitize())
	requireSQLState(t, err, "42501")
	_, err = reindex.Pool.Exec(ctx, `UPDATE vector_state SET writer_operations_enabled=TRUE`)
	requireSQLState(t, err, "42501")
	require.ErrorContains(t, err, "requires the metadata schema owner")
	grantExec(t, owner, `UPDATE vector_state SET writer_operations_enabled=TRUE`)
	_, err = reindex.Pool.Exec(ctx, `UPDATE vector_state SET writer_operations_enabled=FALSE`)
	requireSQLState(t, err, "42501")
	require.ErrorContains(t, err, "requires the metadata schema owner")
	grantExec(t, admin, roles.sql(`GRANT {meta_owner} TO {reindex} WITH INHERIT FALSE, SET TRUE`))
	tx, err := reindex.Pool.Begin(ctx)
	require.NoError(t, err)
	defer rollbackFencedWriterTx(ctx, tx)
	_, err = tx.Exec(ctx, roles.sql(`SET LOCAL ROLE {meta_owner}`))
	require.NoError(t, err)
	_, err = tx.Exec(ctx, `UPDATE grant_catalog_corpus_mode SET writer_operations_enabled=FALSE`)
	requireSQLState(t, err, "42501")
	require.ErrorContains(t, err, "requires the metadata schema owner")
	require.NoError(t, tx.Rollback(ctx))

	grantExec(t, owner, `UPDATE vector_state SET writer_operations_enabled=FALSE`)
}

func TestWriterOperationSerializesWithLegacyCorpusCutover(t *testing.T) {
	for _, strict := range []bool{false, true} {
		name := "legacy barrier"
		if strict {
			name = "durable reservation"
		}
		t.Run(name, func(t *testing.T) {
			owner, observer := sessionTestClientWithPool(t, 2)
			ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
			defer cancel()
			owner.SetAuthzMode("service")
			grantExec(t, owner, `UPDATE document_deletion_activation SET activated_at=clock_timestamp()`)
			grantExec(t, owner, `UPDATE vector_state SET writer_operations_enabled=$1`, strict)
			dsn := owner.Pool.Config().ConnString()
			roles := newGrantRoles(t, dsn, "cutover", "unaffiliated")
			grantExec(t, owner, roles.sql(`CREATE ROLE {cutover} LOGIN PASSWORD 'fixture-password';
				CREATE ROLE {unaffiliated} LOGIN PASSWORD 'fixture-password';
				GRANT EXECUTE ON FUNCTION grant_catalog_activate_grants(boolean) TO {cutover}`))
			operator := writerLogin(t, dsn, roles.name("cutover"))
			unaffiliated := writerLogin(t, dsn, roles.name("unaffiliated"))
			const activate = `SELECT * FROM grant_catalog_activate_grants(false)`
			_, err := unaffiliated.Pool.Exec(ctx, activate)
			requireSQLState(t, err, "42501")

			attempt := make(chan struct{})
			release := make(chan struct{})
			writerDone := make(chan error, 1)
			go func() {
				writerDone <- owner.RunWriterOperation(ctx, domain.WriterOperationDelete, "document", func(ctx context.Context) error {
					return domain.DispatchExternalWriter(ctx, func(ctx context.Context) (bool, error) {
						close(attempt)
						select {
						case <-release:
							return true, nil
						case <-ctx.Done():
							return true, ctx.Err()
						}
					})
				})
			}()
			select {
			case <-attempt:
			case writerErr := <-writerDone:
				t.Fatalf("writer did not enter the provider attempt: %v", writerErr)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			if strict {
				_, err = operator.Pool.Exec(ctx, activate)
				requireSQLState(t, err, "QG007")
			} else {
				waitCtx, stop := context.WithTimeout(ctx, 100*time.Millisecond)
				_, err = operator.Pool.Exec(waitCtx, activate)
				stop()
				require.ErrorIs(t, err, context.DeadlineExceeded)
			}
			mode, err := owner.CorpusAuthzMode(ctx)
			require.NoError(t, err)
			require.Equal(t, "service", mode)
			close(release)
			require.NoError(t, <-writerDone)

			tx, err := operator.Pool.Begin(ctx)
			require.NoError(t, err)
			defer rollbackFencedWriterTx(ctx, tx)
			_, err = tx.Exec(ctx, activate)
			require.NoError(t, err, "the old activation entry point can retry after drain")
			activatePID := tx.Conn().PgConn().PID()
			go func() {
				writerDone <- owner.RunWriterOperation(ctx, domain.WriterOperationDelete, "document", func(context.Context) error {
					return errors.New("callback entered after cutover")
				})
			}()
			require.Eventually(t, func() bool {
				var blocked bool
				err := observer.QueryRow(ctx, `SELECT EXISTS(
					SELECT 1 FROM pg_stat_activity WHERE datname=current_database()
					AND $1::integer=ANY(pg_blocking_pids(pid))
					AND query LIKE '%grant_catalog_corpus_authz_mode_shared()%')`, activatePID).Scan(&blocked)
				return err == nil && blocked
			}, 5*time.Second, 20*time.Millisecond)
			require.NoError(t, tx.Commit(ctx))
			require.ErrorIs(t, <-writerDone, domain.ErrAuthzModeMismatch)
		})
	}
}

func TestWriterOperationMirrorRequiresAuthenticatedOwnerAndConsistency(t *testing.T) {
	ctx := t.Context()
	dsn := startTestDatabase(t)
	owner := migrationTestClient(t, dsn)
	applyMigrationPrefix(t, owner, 44)
	roles := newGrantRoles(t, dsn, "legacy_writer", "column_writer", "inherited_writer", "marker_access", "operator")
	grantExec(t, owner, roles.sql(`
		CREATE ROLE {legacy_writer} LOGIN PASSWORD 'fixture-password';
		CREATE ROLE {column_writer} LOGIN PASSWORD 'fixture-password';
		CREATE ROLE {inherited_writer} LOGIN PASSWORD 'fixture-password';
		CREATE ROLE {marker_access};
		CREATE ROLE {operator} LOGIN PASSWORD 'fixture-password';
		GRANT UPDATE ON grant_catalog_corpus_mode TO {legacy_writer};
		GRANT SELECT ON grant_catalog_corpus_mode TO {legacy_writer}, {column_writer}, {marker_access};
		GRANT {marker_access} TO {inherited_writer};
		GRANT EXECUTE ON FUNCTION grant_catalog_activate_grants(boolean) TO {operator};
		UPDATE document_deletion_activation SET activated_at=clock_timestamp()`))
	require.NoError(t, owner.RunMigrations(ctx))
	grantExec(t, owner, roles.sql(`
		GRANT UPDATE (writer_operations_enabled, authz_mode) ON grant_catalog_corpus_mode TO {column_writer}, {marker_access};
		GRANT UPDATE (writer_operations_enabled) ON vector_state TO {legacy_writer}, {column_writer}, {marker_access};
		UPDATE vector_state SET writer_operations_enabled=TRUE`))
	require.NoError(t, owner.RunWriterOperation(ctx, domain.WriterOperationDelete, "document", func(ctx context.Context) error {
		return domain.DispatchExternalWriter(ctx, func(context.Context) (bool, error) { return false, nil })
	}))
	for _, login := range []string{"legacy_writer", "column_writer", "inherited_writer"} {
		t.Run(login, func(t *testing.T) {
			caseCtx := t.Context()
			restricted := writerLogin(t, dsn, roles.name(login))
			tx, err := restricted.Pool.Begin(caseCtx)
			require.NoError(t, err)
			defer rollbackFencedWriterTx(caseCtx, tx)
			if login == "inherited_writer" {
				// Exercise the inherited grant before setting the effective role.
				_, err = tx.Exec(caseCtx, `UPDATE grant_catalog_corpus_mode SET writer_operations_enabled=FALSE`)
				requireSQLState(t, err, "42501")
				require.ErrorContains(t, err, "requires the metadata schema owner")
				require.NoError(t, tx.Rollback(caseCtx))
				tx, err = restricted.Pool.Begin(caseCtx)
				require.NoError(t, err)
				defer rollbackFencedWriterTx(caseCtx, tx)
				_, err = tx.Exec(caseCtx, roles.sql(`SET LOCAL ROLE {marker_access}`))
				require.NoError(t, err)
			}
			_, err = tx.Exec(caseCtx, `UPDATE grant_catalog_corpus_mode SET writer_operations_enabled=FALSE, authz_mode='grants'`)
			requireSQLState(t, err, "42501")
			require.ErrorContains(t, err, "requires the metadata schema owner")
			require.NoError(t, tx.Rollback(caseCtx))

			_, err = restricted.Pool.Exec(caseCtx, `UPDATE vector_state SET writer_operations_enabled=FALSE`)
			requireSQLState(t, err, "42501")
			require.ErrorContains(t, err, "requires the metadata schema owner")
		})
	}
	_, err := owner.Pool.Exec(ctx, `UPDATE grant_catalog_corpus_mode SET writer_operations_enabled=FALSE`)
	requireSQLState(t, err, "23514")
	require.ErrorContains(t, err, "must match vector state")
	operator := writerLogin(t, dsn, roles.name("operator"))
	_, err = operator.Pool.Exec(ctx, `SELECT * FROM grant_catalog_activate_grants(false)`)
	requireSQLState(t, err, "QG007")

	grantExec(t, owner, `DELETE FROM vector_writer_operations; UPDATE vector_state SET writer_operations_enabled=FALSE`)
	var enabled bool
	require.NoError(t, owner.Pool.QueryRow(ctx, `SELECT writer_operations_enabled FROM grant_catalog_corpus_mode`).Scan(&enabled))
	require.False(t, enabled)
	_, err = operator.Pool.Exec(ctx, `SELECT * FROM grant_catalog_activate_grants(false)`)
	require.NoError(t, err)
}
