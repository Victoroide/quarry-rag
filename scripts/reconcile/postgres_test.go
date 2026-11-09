package main

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"testing"

	pgstore "github.com/Victoroide/quarry-rag/internal/store/postgres"
	"github.com/Victoroide/quarry-rag/internal/testutil/pgtest"
	"github.com/stretchr/testify/require"
)

func TestDocumentLookupPreservesRetryJobs(t *testing.T) {
	if testing.Short() {
		t.Skip("requires a Docker daemon")
	}
	pg, err := pgstore.NewClient(t.Context(), pgtest.Start(t))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, pg.Close(t.Context())) })
	require.NoError(t, pg.RunMigrations(t.Context()))
	_, err = pg.Pool.Exec(t.Context(), `INSERT INTO documents(id, content, status) VALUES
        ($1, 'retryable', 'failed'), ($2, 'terminal', 'failed')`, storedID, missingID)
	require.NoError(t, err)
	_, err = pg.Pool.Exec(t.Context(), `INSERT INTO ingest_jobs(document_id) VALUES ($1)`, storedID)
	require.NoError(t, err)
	states, err := lookupDocuments(t.Context(), pg, []string{storedID, missingID, "invalid", "00000000-0000-4000-8000-000000000099"})
	require.NoError(t, err)
	require.Equal(t, map[string]documentState{storedID: {Status: "failed", HasJob: true}, missingID: {Status: "failed"}}, states)
	require.Equal(t, []string{"terminal"}, selectOrphanVectors([]vectorRef{{"retry", storedID}, {"terminal", missingID}}, states, 100))
}

func TestCollectionBackfillSelectsLabeledDocuments(t *testing.T) {
	pg, err := pgstore.NewClient(t.Context(), pgtest.Start(t))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, pg.Close(t.Context())) })
	require.NoError(t, pg.RunMigrations(t.Context()))
	_, err = pg.Pool.Exec(t.Context(), `INSERT INTO documents(id,content,collection,status) VALUES($1,'synthetic','archive','completed'),($2,'synthetic','','completed'),('00000000-0000-4000-8000-000000000003','synthetic','private','processing')`, storedID, missingID)
	require.NoError(t, err)
	docs, err := listCollectionDocuments(t.Context(), pg, "", 10)
	require.NoError(t, err)
	require.Equal(t, []collectionDocument{{ID: storedID, Collection: "archive"}, {ID: "00000000-0000-4000-8000-000000000003", Collection: "private"}}, docs)
	docs, err = listCollectionDocuments(t.Context(), pg, "", 1)
	require.NoError(t, err)
	require.Equal(t, []collectionDocument{{ID: storedID, Collection: "archive"}}, docs)
	docs, err = listCollectionDocuments(t.Context(), pg, storedID, 10)
	require.NoError(t, err)
	require.Equal(t, []collectionDocument{{ID: "00000000-0000-4000-8000-000000000003", Collection: "private"}}, docs)
}

func TestMakeCollectionBackfillDryRun(t *testing.T) {
	pg, err := pgstore.NewClient(t.Context(), pgtest.Start(t))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, pg.Close(t.Context())) })
	require.NoError(t, pg.RunMigrations(t.Context()))
	_, err = pg.Pool.Exec(t.Context(), `INSERT INTO documents(id,content,collection,status) VALUES($1,'synthetic','archive','completed')`, storedID)
	require.NoError(t, err)
	command := exec.CommandContext(t.Context(), "make", "-s", "reconcile", "ARGS=-backfill-collections -limit=1")
	command.Dir = "../.."
	command.Env = append(os.Environ(), "GO_RAG_FAKE_MODE=true", "DATABASE_URL="+pg.Pool.Config().ConnString())
	var output, logs bytes.Buffer
	command.Stdout = &output
	command.Stderr = &logs
	require.NoError(t, command.Run(), logs.String())
	var result collectionReport
	require.NoError(t, json.Unmarshal(output.Bytes(), &result))
	require.Equal(t, []collectionDocument{{ID: storedID, Collection: "archive"}}, result.Documents)
	require.Zero(t, result.Updated)
}
