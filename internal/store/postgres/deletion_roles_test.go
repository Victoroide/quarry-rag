package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

func TestRestrictedServingLoginReadsDeletionStateThroughOwnerPredicates(t *testing.T) {
	owner, serving, _ := restrictedDatabase(t)
	ctx := t.Context()
	_, err := serving.Pool.Exec(ctx, `SELECT 1 FROM document_deletion_activation`)
	requireSQLState(t, err, "42501")

	// Ordinary traffic on an inactive database needs no privilege on the marker.
	active, err := serving.DeletionActive(ctx)
	require.NoError(t, err)
	require.False(t, active)
	admitted, err := serving.AcceptDocument(ctx, jobsRequest("restricted admission"))
	require.NoError(t, err)
	require.Equal(t, domain.AcceptAccepted, admitted.Outcome)

	_, err = owner.Pool.Exec(ctx, `UPDATE document_deletion_activation SET activated_at=clock_timestamp()`)
	require.NoError(t, err)
	active, err = serving.DeletionActive(ctx)
	require.NoError(t, err)
	require.True(t, active)
}

func TestRestrictedServingLoginRunsTheCheckpointedCoordinator(t *testing.T) {
	owner, serving, _ := restrictedDatabase(t)
	ctx := t.Context()
	_, err := owner.Pool.Exec(ctx, `UPDATE document_deletion_activation SET activated_at=clock_timestamp()`)
	require.NoError(t, err)
	doc := acceptDocument(t, owner, "restricted coordinator", true, true)
	job, err := serving.ClaimJob(ctx, "worker", time.Minute, 3)
	require.NoError(t, err)
	require.NotNil(t, job)

	var signedKey string
	found, err := serving.WithDocumentFile(ctx, doc.id, func(_ context.Context, key string) error {
		signedKey = key
		return nil
	})
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, doc.key, signedKey)

	recorder := newDeletionRecorder()
	require.NoError(t, serving.DeleteWithCleanup(ctx, doc.id, recorder.ops(doc.incarnation)))
	require.Equal(t, []string{"revision:" + string(doc.id) + "/" + doc.incarnation, "source:" + doc.key}, recorder.all())
	require.True(t, readReceipt(t, owner, doc.id).finalized)
	require.Empty(t, documentStatus(t, owner, doc.id))
	require.NoError(t, serving.DeleteWithCleanup(ctx, doc.id, recorder.ops(doc.incarnation)), "the receipt acknowledges the retry")

	reused, err := serving.AcceptDocument(ctx, func() domain.AcceptRequest {
		req := jobsRequest("replacement under the restricted login")
		req.Document.ID, req.CallerSupplied = doc.id, true
		return req
	}())
	require.NoError(t, err)
	require.Equal(t, domain.AcceptAccepted, reused.Outcome, "the owner-defined predicate sees the released receipt")
}

func TestPublishedGrantsCoverTheDeletionPredicates(t *testing.T) {
	owner, _, roles := restrictedDatabase(t)
	var executable bool
	require.NoError(t, owner.Pool.QueryRow(t.Context(), `SELECT
		has_function_privilege($1,'document_deletion_active()','EXECUTE')
		AND has_function_privilege($1,'document_deletion_reserved(uuid)','EXECUTE')
		AND has_table_privilege($1,'document_deletions','SELECT,INSERT,UPDATE')
		AND NOT has_table_privilege($1,'document_deletion_activation','SELECT,INSERT,UPDATE,DELETE')`,
		roles.name("quarry_service")).Scan(&executable))
	require.True(t, executable)
}

func TestFileSigningNeedsOnlySelect(t *testing.T) {
	owner := activatedDatabase(t)
	ctx := t.Context()
	doc := acceptDocument(t, owner, "select only signing", true, true)
	roles := newGrantRoles(t, owner.Pool.Config().ConnString(), "file_reader")
	role := pgx.Identifier{roles.name("file_reader")}.Sanitize()
	grantExec(t, owner, "CREATE ROLE "+role)
	grantExec(t, owner, "GRANT USAGE ON SCHEMA public TO "+role)
	grantExec(t, owner, "GRANT SELECT ON public.documents TO "+role)
	reader := roleClient(t, owner, roles.name("file_reader"))

	var signedKey string
	found, err := reader.WithDocumentFile(ctx, doc.id, func(_ context.Context, key string) error {
		signedKey = key
		return nil
	})
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, doc.key, signedKey)

	_, err = reader.Pool.Exec(ctx, `UPDATE documents SET updated_at=NOW() WHERE id=$1`, string(doc.id))
	requireSQLState(t, err, "42501")
}
