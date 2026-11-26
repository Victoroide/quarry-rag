package postgres

import (
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

var manifestTables = []string{"hosted_vector_manifests", "hosted_vector_revisions", "hosted_vector_cleanups"}

// A serving login holding only the published grants has no privilege on the
// manifest tables, and every ordinary path still works while the feature is off.
func TestServingLoginWithoutManifestsNeedsNoManifestTablePrivileges(t *testing.T) {
	owner, serving, _ := restrictedDatabase(t)
	ctx := t.Context()
	for _, table := range manifestTables {
		_, err := serving.Pool.Exec(ctx, `SELECT 1 FROM `+table)
		requireSQLState(t, err, "42501")
	}

	docID := acceptManifested(t, serving, "restricted admission")
	first, err := serving.ClaimJob(ctx, "w1", time.Minute, 5)
	require.NoError(t, err)
	require.NotNil(t, first)
	expireJobLease(t, owner, docID)
	retry, err := serving.ClaimJob(ctx, "w2", time.Minute, 5)
	require.NoError(t, err, "a retry claim must not touch tables the login cannot read")
	require.NotNil(t, retry)
	require.Equal(t, 2, retry.Attempts)

	chunk := domain.Chunk{ID: domain.ChunkID(uuid.NewString()), DocumentID: docID, Index: 0, Text: "restricted evidence", TokenCount: 3}
	completed, err := serving.CompleteJob(ctx, *retry, []domain.Chunk{chunk}, domain.IndexObservation{})
	require.NoError(t, err)
	require.True(t, completed)
	visible, err := Visibility{Pool: serving.Pool}.VisibleEvidence(ctx, []domain.ScoredChunk{{Chunk: chunk}})
	require.NoError(t, err, "the read path consults revisions through the owner-defined predicate")
	require.Len(t, visible, 1)
	revVisible, err := Visibility{Pool: serving.Pool}.VisibleRevisionChunks(ctx, []domain.ScoredChunk{{Chunk: chunk}})
	require.NoError(t, err, "revision chunk visibility uses the owner-defined predicate")
	require.Len(t, revVisible, 1)
	listed, err := serving.ListDocumentVectorManifests(ctx, docID, "")
	require.NoError(t, err, "listing manifests with the flag off must not query the manifest table")
	require.Empty(t, listed)

	doc, err := serving.GetDocument(ctx, docID)
	require.NoError(t, err)
	require.NoError(t, serving.RemoveLiveDocument(ctx, doc), "the delete trigger runs with its owner's privileges")

	failing := acceptManifested(t, serving, "restricted failure")
	job, err := serving.ClaimJob(ctx, "w3", time.Minute, 5)
	require.NoError(t, err)
	require.NotNil(t, job)
	require.NoError(t, serving.FailJob(ctx, *job, "permanent"), "the failure trigger runs with its owner's privileges")
	require.NoError(t, serving.DeleteDocument(ctx, failing))
}

// A node that enables manifests needs the one extra grant block, and nothing else.
func TestServingLoginWithManifestsNeedsOnlyThePublishedManifestGrants(t *testing.T) {
	owner, serving, roles := restrictedDatabase(t)
	ctx := t.Context()
	grantExec(t, owner, publishedServingSQL(t, roles, "grant-catalog.md", "public.hosted_vector_manifests"))
	serving.EnableHostedVectorManifests(true)

	docID := acceptManifested(t, serving, "restricted manifests")
	startManifestAttempt(t, serving, docID, "w1")
	expireJobLease(t, owner, docID)
	attempt := startManifestAttempt(t, serving, docID, "w2")
	ok, err := serving.CompleteJob(ctx, attempt.job, attempt.chunks(docID, "restricted manifests"), domain.IndexObservation{})
	require.NoError(t, err)
	require.True(t, ok)
	visible, err := Visibility{Pool: serving.Pool}.VisibleEvidence(ctx, []domain.ScoredChunk{{
		Chunk: domain.Chunk{ID: domain.ChunkID(attempt.chunkID), DocumentID: docID, Index: 0, Text: "restricted manifests", RevisionID: attempt.revision},
	}})
	require.NoError(t, err)
	require.Len(t, visible, 1)

	doc, err := serving.GetDocument(ctx, docID)
	require.NoError(t, err)
	require.NoError(t, serving.RemoveLiveDocument(ctx, doc))
	claimed, err := serving.ClaimVectorCleanup(ctx, "sweeper", time.Minute, 0)
	require.NoError(t, err)
	require.NotNil(t, claimed)
	done, err := serving.CompleteVectorCleanup(ctx, *claimed)
	require.NoError(t, err)
	require.True(t, done)
}
