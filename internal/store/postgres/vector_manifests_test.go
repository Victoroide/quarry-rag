package postgres

import (
	"context"
	"errors"
	"maps"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

func manifestDatabase(t *testing.T) *Client {
	t.Helper()
	c := jobsDatabase(t)
	c.EnableHostedVectorManifests(true)
	return c
}

type manifestAttempt struct {
	job      domain.Job
	revision string
	chunkID  string
	vectorID string
}

// startManifestAttempt leases the next attempt of the document's job and
// records the write intent of its revision.
func startManifestAttempt(t *testing.T, c *Client, docID domain.DocumentID, owner string) manifestAttempt {
	t.Helper()
	job, err := c.ClaimJob(t.Context(), owner, time.Minute, 5)
	require.NoError(t, err)
	require.NotNil(t, job)
	return persistManifestAttempt(t, c, docID, *job)
}

// persistManifestAttempt records the write intent of a revision for a leased job.
func persistManifestAttempt(t *testing.T, c *Client, docID domain.DocumentID, job domain.Job) manifestAttempt {
	t.Helper()
	a := manifestAttempt{job: job, revision: uuid.NewString(), chunkID: uuid.NewString()}
	a.job.RevisionID = a.revision
	a.vectorID = domain.FormatRevisionVectorID(docID, a.revision, 0)
	require.NoError(t, c.PersistVectorManifest(t.Context(), a.job, domain.VectorManifest{
		RevisionID: a.revision, DocumentID: docID, Incarnation: job.Incarnation, Attempt: job.Attempts, LeaseToken: job.LeaseToken,
		IDPrefix: domain.RevisionVectorPrefix(docID, a.revision), VectorIDs: []string{a.vectorID}, ChunkIDs: []string{a.chunkID},
	}))
	return a
}

func (a manifestAttempt) chunks(docID domain.DocumentID, text string) []domain.Chunk {
	return []domain.Chunk{{ID: domain.ChunkID(a.chunkID), DocumentID: docID, Index: 0, Text: text, TokenCount: 3}}
}

func expireJobLease(t *testing.T, c *Client, docID domain.DocumentID) {
	t.Helper()
	_, err := c.Pool.Exec(t.Context(), `UPDATE ingest_jobs SET lease_until = NOW() - interval '1 second' WHERE document_id = $1`, string(docID))
	require.NoError(t, err)
}

// manifestState returns the stored state of a manifest, or "" once it is gone.
func manifestState(t *testing.T, c *Client, revision string) string {
	t.Helper()
	var state string
	err := c.Pool.QueryRow(t.Context(), `SELECT state FROM hosted_vector_manifests WHERE revision_id = $1`, revision).Scan(&state)
	if errors.Is(err, pgx.ErrNoRows) {
		return ""
	}
	require.NoError(t, err)
	return state
}

// cleanupReason returns why a revision awaits cleanup, or "" when it does not.
func cleanupReason(t *testing.T, c *Client, revision string) string {
	t.Helper()
	var reason string
	err := c.Pool.QueryRow(t.Context(), `SELECT reason FROM hosted_vector_cleanups WHERE revision_id = $1`, revision).Scan(&reason)
	if errors.Is(err, pgx.ErrNoRows) {
		return ""
	}
	require.NoError(t, err)
	return reason
}

func acceptManifested(t *testing.T, c *Client, content string) domain.DocumentID {
	t.Helper()
	req := jobsRequest(content)
	req.CallerSupplied = true
	res, err := c.AcceptDocument(t.Context(), req)
	require.NoError(t, err)
	require.Equal(t, domain.AcceptAccepted, res.Outcome)
	return req.Document.ID
}

func TestVectorManifestLifecycleAndUUIDReuse(t *testing.T) {
	c := manifestDatabase(t)
	ctx := t.Context()

	docID := domain.DocumentID(uuid.NewString())
	req := jobsRequest("manifested content v1")
	req.Document.ID = docID
	req.CallerSupplied = true
	res, err := c.AcceptDocument(ctx, req)
	require.NoError(t, err)
	require.Equal(t, domain.AcceptAccepted, res.Outcome)

	first := startManifestAttempt(t, c, docID, "w1")
	expireJobLease(t, c, docID)
	// The expired attempt can no longer record another intent.
	err = c.PersistVectorManifest(ctx, first.job, domain.VectorManifest{
		RevisionID: uuid.NewString(), DocumentID: docID, Incarnation: first.job.Incarnation, Attempt: first.job.Attempts,
		LeaseToken: first.job.LeaseToken, IDPrefix: domain.RevisionVectorPrefix(docID, first.revision),
		VectorIDs: []string{first.vectorID}, ChunkIDs: []string{first.chunkID},
	})
	require.ErrorIs(t, err, domain.ErrJobNotOwned)

	second := startManifestAttempt(t, c, docID, "w2")
	require.Equal(t, "writing", manifestState(t, c, first.revision), "a retry claim leaves the earlier intent for activation to retire")
	ok, err := c.CompleteJob(ctx, second.job, second.chunks(docID, "manifested content v1"), domain.IndexObservation{})
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "active", manifestState(t, c, second.revision))
	require.Equal(t, "retired", manifestState(t, c, first.revision))
	require.Equal(t, "superseded", cleanupReason(t, c, first.revision))

	// Only the active revision is visible; a superseded revision and an unrevisioned hit are not.
	vis := Visibility{Pool: c.Pool}
	candidates := []domain.ScoredChunk{
		{Chunk: domain.Chunk{ID: domain.ChunkID(first.chunkID), DocumentID: docID, Index: 0, Text: "manifested content v1", RevisionID: first.revision}, Score: 0.99},
		{Chunk: domain.Chunk{ID: domain.ChunkID(second.chunkID), DocumentID: docID, Index: 0, Text: "manifested content v1"}, Score: 0.95},
		{Chunk: domain.Chunk{ID: domain.ChunkID(second.chunkID), DocumentID: docID, Index: 0, Text: "manifested content v1", RevisionID: second.revision}, Score: 0.90},
	}
	gotEv, previews, err := vis.VisibleEvidenceWithPreviews(ctx, candidates, []string{string(docID)})
	require.NoError(t, err)
	require.Len(t, gotEv, 1)
	require.Equal(t, second.revision, gotEv[0].Chunk.RevisionID)
	require.NotEmpty(t, previews[string(docID)])
	gotRev, err := vis.VisibleRevisionChunks(ctx, candidates)
	require.NoError(t, err)
	require.Len(t, gotRev, 1)
	require.Equal(t, second.revision, gotRev[0].Chunk.RevisionID)

	// Completing the superseded revision's cleanup drops its manifest and leaves the active one.
	superseded, err := c.ClaimVectorCleanup(ctx, "sweeper", time.Minute, 0)
	require.NoError(t, err)
	require.NotNil(t, superseded)
	require.Equal(t, first.revision, superseded.RevisionID)
	done, err := c.CompleteVectorCleanup(ctx, *superseded)
	require.NoError(t, err)
	require.True(t, done)
	require.Empty(t, manifestState(t, c, first.revision), "a verified cleanup leaves no retired manifest behind")
	require.Equal(t, "active", manifestState(t, c, second.revision))

	// Delete the document and immediately reuse its UUID. The tombstone outlives both.
	require.NoError(t, c.DeleteDocument(ctx, docID))
	resReuse, err := c.AcceptDocument(ctx, req)
	require.NoError(t, err)
	require.Equal(t, domain.AcceptAccepted, resReuse.Outcome)
	pending, err := c.ListPendingVectorCleanups(ctx, 10)
	require.NoError(t, err)
	require.Len(t, pending, 1)
	require.Equal(t, second.revision, pending[0].RevisionID)
	require.Equal(t, "deleted", pending[0].Reason)

	deleted, err := c.ClaimVectorCleanup(ctx, "sweeper", time.Minute, 0)
	require.NoError(t, err)
	require.NotNil(t, deleted)
	advanced, err := c.AdvanceVectorCleanup(ctx, *deleted, 1)
	require.NoError(t, err)
	require.True(t, advanced)
	deferred, err := c.DeferVectorCleanup(ctx, *deleted, "verifying", 0, 1, "")
	require.NoError(t, err)
	require.True(t, deferred)
	reclaimed, err := c.ClaimVectorCleanup(ctx, "sweeper", time.Minute, 0)
	require.NoError(t, err)
	require.NotNil(t, reclaimed)
	require.Equal(t, "verifying", reclaimed.State)
	require.Equal(t, 1, reclaimed.ConvergenceChecks)
	completed, err := c.CompleteVectorCleanup(ctx, *reclaimed)
	require.NoError(t, err)
	require.True(t, completed)
	require.Empty(t, manifestState(t, c, second.revision))
	// A finished claim cannot complete again, and a release after cancellation still works.
	completed, err = c.CompleteVectorCleanup(ctx, *reclaimed)
	require.NoError(t, err)
	require.False(t, completed)
	require.NoError(t, c.ReleaseVectorCleanup(context.Background(), *reclaimed))
}

func TestDeletionDrainIsEnforcedWhereCleanupIsClaimed(t *testing.T) {
	for name, remove := range map[string]func(*testing.T, *Client, domain.DocumentID){
		"plain delete": func(t *testing.T, c *Client, id domain.DocumentID) {
			require.NoError(t, c.DeleteDocument(t.Context(), id))
		},
		"guarded removal": func(t *testing.T, c *Client, id domain.DocumentID) {
			doc, err := c.GetDocument(t.Context(), id)
			require.NoError(t, err)
			require.NotNil(t, doc)
			require.NoError(t, c.RemoveLiveDocument(t.Context(), doc))
		},
	} {
		t.Run(name, func(t *testing.T) {
			c := manifestDatabase(t)
			ctx := t.Context()
			docID := acceptManifested(t, c, "drained "+name)
			attempt := startManifestAttempt(t, c, docID, "w1")
			ok, err := c.CompleteJob(ctx, attempt.job, attempt.chunks(docID, "drained "+name), domain.IndexObservation{})
			require.NoError(t, err)
			require.True(t, ok)

			remove(t, c, docID)
			require.Equal(t, "deleted", cleanupReason(t, c, attempt.revision))

			held, err := c.ClaimVectorCleanup(ctx, "sweeper", time.Minute, time.Hour)
			require.NoError(t, err)
			require.Nil(t, held, "a write in flight at deletion may still land inside the drain window")

			_, err = c.Pool.Exec(ctx, `UPDATE hosted_vector_cleanups SET retired_at = retired_at - interval '2 hours' WHERE revision_id = $1`, attempt.revision)
			require.NoError(t, err)
			claimed, err := c.ClaimVectorCleanup(ctx, "sweeper", time.Minute, time.Hour)
			require.NoError(t, err)
			require.NotNil(t, claimed)
			require.Equal(t, attempt.revision, claimed.RevisionID)
		})
	}
}

func TestActivationSupersedesAnActiveRevision(t *testing.T) {
	c := manifestDatabase(t)
	ctx := t.Context()
	docID := acceptManifested(t, c, "reingested content")

	first := startManifestAttempt(t, c, docID, "w1")
	// The document already published its first revision when the next attempt starts.
	_, err := c.Pool.Exec(ctx, `UPDATE hosted_vector_manifests SET state='active', activated_at=NOW() WHERE revision_id=$1`, first.revision)
	require.NoError(t, err)
	_, err = c.Pool.Exec(ctx, `INSERT INTO hosted_vector_revisions(document_id, incarnation, active_revision_id) VALUES ($1,$2,$3)`,
		string(docID), first.job.Incarnation, first.revision)
	require.NoError(t, err)
	expireJobLease(t, c, docID)
	second := startManifestAttempt(t, c, docID, "w2")

	ok, err := c.CompleteJob(ctx, second.job, second.chunks(docID, "reingested content"), domain.IndexObservation{})
	require.NoError(t, err, "a unique active manifest per document must not block the swap")
	require.True(t, ok)
	require.Equal(t, "active", manifestState(t, c, second.revision))
	require.Equal(t, "retired", manifestState(t, c, first.revision))
	require.Equal(t, "superseded", cleanupReason(t, c, first.revision))
	var active string
	require.NoError(t, c.Pool.QueryRow(ctx, `SELECT active_revision_id::text FROM hosted_vector_revisions WHERE document_id=$1`, string(docID)).Scan(&active))
	require.Equal(t, second.revision, active)
}

func TestTerminalFailureRetiresEveryManifestWhateverTheEntryPoint(t *testing.T) {
	t.Run("exceeded attempts at claim", func(t *testing.T) {
		c := manifestDatabase(t)
		ctx := t.Context()
		docID := acceptManifested(t, c, "crashed last attempt")
		crashed := startManifestAttempt(t, c, docID, "w1")
		expireJobLease(t, c, docID)

		// The claim fails the job without knowing any revision.
		job, err := c.ClaimJob(ctx, "w2", time.Minute, 1)
		require.NoError(t, err)
		require.Nil(t, job)
		require.Equal(t, "retired", manifestState(t, c, crashed.revision))
		require.Equal(t, "failed", cleanupReason(t, c, crashed.revision))
	})
	t.Run("failure on a node without manifests", func(t *testing.T) {
		c := manifestDatabase(t)
		ctx := t.Context()
		docID := acceptManifested(t, c, "failed elsewhere")
		attempt := startManifestAttempt(t, c, docID, "w1")
		_, err := c.Pool.Exec(ctx, `UPDATE hosted_vector_manifests SET state='active', activated_at=NOW() WHERE revision_id=$1`, attempt.revision)
		require.NoError(t, err)
		_, err = c.Pool.Exec(ctx, `INSERT INTO hosted_vector_revisions(document_id, incarnation, active_revision_id) VALUES ($1,$2,$3)`,
			string(docID), attempt.job.Incarnation, attempt.revision)
		require.NoError(t, err)

		unflagged := &Client{Pool: c.Pool}
		unrevisioned := attempt.job
		unrevisioned.RevisionID = ""
		require.NoError(t, unflagged.FailJob(ctx, unrevisioned, "permanent"))
		require.Equal(t, "retired", manifestState(t, c, attempt.revision))
		require.Equal(t, "failed", cleanupReason(t, c, attempt.revision))
		var rows int
		require.NoError(t, c.Pool.QueryRow(ctx, `SELECT count(*) FROM hosted_vector_revisions WHERE document_id=$1`, string(docID)).Scan(&rows))
		require.Zero(t, rows, "a failed document keeps no active revision")
	})
	t.Run("status update outside the job lifecycle", func(t *testing.T) {
		c := manifestDatabase(t)
		docID := acceptManifested(t, c, "failed by status")
		attempt := startManifestAttempt(t, c, docID, "w1")
		require.NoError(t, c.UpdateDocumentStatus(t.Context(), docID, "failed"))
		require.Equal(t, "retired", manifestState(t, c, attempt.revision))
		require.Equal(t, "failed", cleanupReason(t, c, attempt.revision))
	})
}

func TestFixtureRemovalHandsRevisionsToTheDrainedQueue(t *testing.T) {
	c := manifestDatabase(t)
	ctx := t.Context()
	id, receipt := fixtureShapedID(), uuid.NewString()
	accepted, err := c.AcceptDocument(ctx, fixtureRequest(id, receipt, "pinecone:bound"))
	require.NoError(t, err)
	require.Equal(t, domain.AcceptAccepted, accepted.Outcome)
	job, err := c.ClaimJobForFixtureStore(ctx, "bound-worker", time.Minute, 3, "pinecone:bound")
	require.NoError(t, err)
	require.NotNil(t, job)
	attempt := persistManifestAttempt(t, c, id, *job)
	ok, err := c.CompleteJob(ctx, attempt.job, attempt.chunks(id, "fixture body"), domain.IndexObservation{})
	require.NoError(t, err)
	require.True(t, ok)

	state, err := c.DeleteEvaluationFixture(ctx, id, receipt, accepted.Incarnation, "pinecone:bound", func(context.Context) error { return nil })
	require.NoError(t, err)
	require.Equal(t, domain.FixtureDeleted, state)
	require.Equal(t, "deleted", cleanupReason(t, c, attempt.revision))
	held, err := c.ClaimVectorCleanup(ctx, "sweeper", time.Minute, time.Hour)
	require.NoError(t, err)
	require.Nil(t, held, "fixture removal honors the same drain window as every other deletion")
}

func TestParseManifestVectorID(t *testing.T) {
	docID := uuid.NewString()
	revID := uuid.NewString()
	valid := domain.FormatRevisionVectorID(domain.DocumentID(docID), revID, 12)
	gotDoc, gotRev, gotIdx, ok := parseManifestVectorID(valid)
	require.True(t, ok)
	require.Equal(t, docID, gotDoc)
	require.Equal(t, revID, gotRev)
	require.Equal(t, int32(12), gotIdx)

	for _, invalid := range []string{
		"",
		docID,
		docID + "#" + revID,
		docID + "#" + revID + "#-1",
		docID + "#" + revID + "#abc",
		docID + "#" + revID + "#0#extra",
		uuid.NewString() + "#not-a-uuid#0",
		docID + "#" + revID + "#00",
	} {
		_, _, _, ok := parseManifestVectorID(invalid)
		require.False(t, ok, invalid)
	}
}

func TestVisibleRevisionChunksEnforcesSubjectGrants(t *testing.T) {
	c, _ := vectorDatabase(t)
	c.EnableHostedVectorManifests(true)
	f := &retrievalFixture{ownedFixture: provisionOwnedFixture(t, c)}

	publishManifested := func(owner domain.Principal, text string) (docID, revID, chunkID string) {
		t.Helper()
		req := ownedRequest(owner, "", text)
		require.Equal(t, domain.AcceptAccepted, f.accept(t, req).Outcome)
		attempt := startManifestAttempt(t, c, req.Document.ID, "worker")
		chunks := attempt.chunks(req.Document.ID, text)
		for i := range chunks {
			chunks[i].Embedding = []float32{1, 0, 0}
		}
		ok, err := c.CompleteJob(t.Context(), attempt.job, chunks, domain.IndexObservation{})
		require.NoError(t, err)
		require.True(t, ok)
		return string(req.Document.ID), attempt.revision, attempt.chunkID
	}

	ownDoc, ownRev, ownChunk := publishManifested(f.owner, "own manifested text")
	sharedDoc, sharedRev, sharedChunk := publishManifested(f.peer, "shared manifested text")
	peerDoc, peerRev, peerChunk := publishManifested(f.peer, "peer private manifested text")
	otherDoc, otherRev, otherChunk := publishManifested(f.outsider, "other tenant manifested text")
	f.grantRead(t, f.peer, sharedDoc, f.owner, true)

	candidates := []domain.ScoredChunk{
		{Chunk: domain.Chunk{ID: domain.ChunkID(ownChunk), DocumentID: domain.DocumentID(ownDoc), Text: "own manifested text", RevisionID: ownRev}, Score: 0.9},
		{Chunk: domain.Chunk{ID: domain.ChunkID(ownChunk), DocumentID: domain.DocumentID(ownDoc), Text: "stale rev", RevisionID: uuid.NewString()}, Score: 0.95},
		{Chunk: domain.Chunk{ID: domain.ChunkID(sharedChunk), DocumentID: domain.DocumentID(sharedDoc), Text: "shared manifested text", RevisionID: sharedRev}, Score: 0.8},
		{Chunk: domain.Chunk{ID: domain.ChunkID(peerChunk), DocumentID: domain.DocumentID(peerDoc), Text: "peer private manifested text", RevisionID: peerRev}, Score: 0.99},
		{Chunk: domain.Chunk{ID: domain.ChunkID(otherChunk), DocumentID: domain.DocumentID(otherDoc), Text: "other tenant manifested text", RevisionID: otherRev}, Score: 0.99},
	}

	vis := Visibility{Pool: c.Pool, Subject: &f.owner}
	visible, err := vis.VisibleRevisionChunks(t.Context(), candidates)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{ownDoc, sharedDoc}, documentIDs(visible))
	for _, sc := range visible {
		if string(sc.Chunk.DocumentID) == ownDoc {
			require.Equal(t, ownRev, sc.Chunk.RevisionID)
		}
	}

	f.grantRead(t, f.peer, sharedDoc, f.owner, false)
	visible, err = vis.VisibleRevisionChunks(t.Context(), candidates)
	require.NoError(t, err)
	require.Equal(t, []string{ownDoc}, documentIDs(visible))
}

func grantsManifestFixture(t *testing.T, c *Client) *ownedFixture {
	t.Helper()
	c.EnableHostedVectorManifests(true)
	f := provisionOwnedFixture(t, c)
	_, err := c.ActivateGrants(t.Context(), false)
	require.NoError(t, err)
	c.SetAuthzMode("grants")
	return f
}

func admitHostedOwned(t *testing.T, f *ownedFixture, p domain.Principal, text string, scoped bool) domain.DocumentID {
	t.Helper()
	req := ownedRequest(p, "", text)
	req.SupportsScopedVectors = scoped
	require.Equal(t, domain.AcceptAccepted, f.accept(t, req).Outcome)
	return req.Document.ID
}

func completeManifestHit(t *testing.T, c *Client, a manifestAttempt, score float64) domain.ScoredChunk {
	t.Helper()
	chunks := a.chunks(a.job.Document.ID, a.job.Document.Content)
	ok, err := c.CompleteJob(t.Context(), a.job, chunks, domain.IndexObservation{})
	require.NoError(t, err)
	require.True(t, ok)
	hit := domain.ScoredChunk{Chunk: chunks[0], Score: score}
	hit.Chunk.RevisionID = a.revision
	hit.Chunk.VectorID = a.vectorID
	return hit
}

func expectedManifestEvidence(hit domain.ScoredChunk, incarnation string) domain.ScoredChunk {
	hit.ObservedIncarnation = incarnation
	metadata := make(map[string]any, len(hit.Chunk.Metadata)+1)
	maps.Copy(metadata, hit.Chunk.Metadata)
	metadata["doc_type"] = domain.DefaultDocType
	hit.Chunk.Metadata = metadata
	return hit
}

func TestVisibleEvidenceScopedManifestPublication(t *testing.T) {
	c := jobsDatabase(t)
	f := grantsManifestFixture(t, c)
	ctx := t.Context()
	const body = "scoped manifest publication"
	id := admitHostedOwned(t, f, f.owner, body, true)
	first := startManifestAttempt(t, c, id, "expired")
	expireJobLease(t, c, id)
	second := startManifestAttempt(t, c, id, "published")
	require.NotEqual(t, first.job.LeaseToken, second.job.LeaseToken)
	active := completeManifestHit(t, c, second, 0.95)
	require.Equal(t, "retired", manifestState(t, c, first.revision))
	require.Equal(t, "active", manifestState(t, c, second.revision))
	var scoped bool
	var attempt string
	require.NoError(t, c.Pool.QueryRow(ctx, `SELECT scoped_vectors, vector_attempt::text FROM documents WHERE id=$1`, id).
		Scan(&scoped, &attempt))
	require.True(t, scoped)
	require.Equal(t, second.job.LeaseToken, attempt)

	otherID := admitHostedOwned(t, f, f.owner, "another published document", true)
	other := startManifestAttempt(t, c, otherID, "other")
	otherHit := completeManifestHit(t, c, other, 0.4)
	pendingID := admitHostedOwned(t, f, f.owner, "unpublished document", true)
	pending := startManifestAttempt(t, c, pendingID, "pending")
	require.Equal(t, "writing", manifestState(t, c, pending.revision))

	// Keep the current chunk ID, index and text to isolate the revision predicate.
	candidates := []domain.ScoredChunk{otherHit}
	for _, revision := range []string{first.revision, pending.revision, uuid.NewString(), "", "not-a-uuid", other.revision} {
		rejected := active
		rejected.Chunk.RevisionID = revision
		candidates = append(candidates, rejected)
	}
	tagged := active
	tagged.Chunk.RevisionID = ""
	tagged.Chunk.Metadata = map[string]any{
		domain.VectorRevisionKey: second.job.Incarnation,
		domain.VectorAttemptKey:  second.job.LeaseToken,
	}
	candidates = append(candidates, tagged)
	candidates = append(candidates, active)
	vis := Visibility{Pool: c.Pool, Subject: &f.owner}
	revisions, err := vis.VisibleRevisionChunks(ctx, candidates)
	require.NoError(t, err)
	require.Equal(t, []domain.ScoredChunk{otherHit, active}, revisions)
	want := []domain.ScoredChunk{
		expectedManifestEvidence(otherHit, other.job.Incarnation),
		expectedManifestEvidence(active, second.job.Incarnation),
	}
	evidence, err := vis.VisibleEvidence(ctx, candidates)
	require.NoError(t, err)
	require.Equal(t, want, evidence)
	evidence, previews, err := vis.VisibleEvidenceWithPreviews(ctx, candidates, []string{string(id), string(pendingID)})
	require.NoError(t, err)
	require.Equal(t, want, evidence)
	require.Equal(t, map[string]string{string(id): body}, previews)
}
