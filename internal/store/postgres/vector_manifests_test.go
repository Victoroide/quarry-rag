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
	return persistManifestAttemptWithChunk(t.Context(), t, c, docID, job, uuid.NewString())
}

func persistManifestAttemptWithChunk(ctx context.Context, t *testing.T, c *Client, docID domain.DocumentID, job domain.Job, chunkID string) manifestAttempt {
	t.Helper()
	a := manifestAttempt{job: job, revision: uuid.NewString(), chunkID: chunkID}
	a.job.RevisionID = a.revision
	a.vectorID = domain.FormatRevisionVectorID(docID, a.revision, 0)
	require.NoError(t, c.PersistVectorManifest(ctx, a.job, domain.VectorManifest{
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

const unresolvedPublication = "vector mutation is unresolved for the publication target"

func restrictedManifestDatabase(t *testing.T) (owner, serving *Client) {
	t.Helper()
	owner, serving, roles := restrictedDatabase(t)
	grantExec(t, owner, publishedServingSQL(t, roles, "grant-catalog.md", "GRANT SELECT, INSERT, UPDATE, DELETE ON public.hosted_vector_manifests"))
	serving.EnableHostedVectorManifests(true)
	return owner, serving
}

func legacyMutation(t *testing.T, c *Client, job domain.Job, deletes bool, outcome error) {
	t.Helper()
	err := c.MutateJobVectors(t.Context(), job, func(ctx context.Context) error {
		return domain.DispatchVectorMutation(ctx, deletes, func(context.Context) error { return outcome })
	})
	if outcome != nil {
		require.ErrorIs(t, err, outcome)
	} else {
		require.NoError(t, err)
	}
}

// publicationState includes the old chunks, ownership, and retirement queue as
// well as the pointer so that rejected completions cannot hide partial writes.
func publicationState(t *testing.T, c *Client, id domain.DocumentID) string {
	t.Helper()
	var state string
	require.NoError(t, c.Pool.QueryRow(t.Context(), `SELECT jsonb_build_object(
		'document', (SELECT to_jsonb(d) FROM documents d WHERE id=$1),
		'job', (SELECT to_jsonb(j) FROM ingest_jobs j WHERE document_id=$1),
		'pointer', (SELECT to_jsonb(r) FROM hosted_vector_revisions r WHERE document_id=$1),
		'manifests', (SELECT jsonb_agg(to_jsonb(m) ORDER BY revision_id) FROM hosted_vector_manifests m WHERE document_id=$1),
		'cleanups', (SELECT jsonb_agg(to_jsonb(q) ORDER BY revision_id) FROM hosted_vector_cleanups q WHERE document_id=$1),
		'chunks', (SELECT jsonb_agg(to_jsonb(ch) ORDER BY id) FROM chunks ch WHERE document_id=$1)
	)::text`, id).Scan(&state))
	return state
}

func requirePublishedManifest(t *testing.T, c *Client, a manifestAttempt, uncertain bool) {
	t.Helper()
	var status, revision string
	var pending, unknown, deleteUnknown bool
	var withoutClaim bool
	var active, jobs int
	var chunks []string
	require.NoError(t, c.Pool.QueryRow(t.Context(), `SELECT d.status, r.active_revision_id::text,
		d.vector_mutation_pending, d.vector_mutation_uncertain, d.vector_delete_uncertain,
		d.vector_mutation_claim IS NULL AND d.vector_mutation_until IS NULL AND NOT d.vector_mutation_deletes,
		(SELECT count(*) FROM hosted_vector_manifests m WHERE m.document_id=d.id AND m.state='active'),
		(SELECT array_agg(ch.id::text) FROM chunks ch WHERE ch.document_id=d.id),
		(SELECT count(*) FROM ingest_jobs j WHERE j.document_id=d.id)
		FROM documents d JOIN hosted_vector_revisions r ON r.document_id=d.id WHERE d.id=$1`,
		a.job.Document.ID).Scan(&status, &revision, &pending, &unknown, &deleteUnknown, &withoutClaim, &active, &chunks, &jobs))
	require.Equal(t, "completed", status)
	require.Equal(t, a.revision, revision)
	require.False(t, pending)
	require.Equal(t, uncertain, unknown)
	require.False(t, deleteUnknown)
	require.True(t, withoutClaim)
	require.Equal(t, 1, active)
	require.Equal(t, []string{a.chunkID}, chunks)
	require.Zero(t, jobs)
}

func TestManifestCompletionWithLegacyMutationUncertainty(t *testing.T) {
	owner, serving := restrictedManifestDatabase(t)
	for _, tc := range []struct {
		name                                        string
		upsert, erase, claim, expired, claimDeletes bool
		legacy, fixture, scoped, allowed            bool
	}{
		{name: "clean", allowed: true},
		{name: "lost upsert acknowledgement", upsert: true, allowed: true},
		{name: "expired upsert claim", claim: true, expired: true, allowed: true},
		{name: "expired upsert claim on fixture route", claim: true, expired: true, fixture: true, allowed: true},
		{name: "live upsert claim", claim: true},
		{name: "live delete claim", claim: true, claimDeletes: true},
		{name: "lost delete acknowledgement", erase: true},
		{name: "expired delete claim", claim: true, expired: true, claimDeletes: true},
		{name: "unresolved upsert and delete", upsert: true, erase: true},
		{name: "expired upsert cannot mask uncertain delete", erase: true, claim: true, expired: true},
		{name: "legacy retry after acknowledged cleanup", upsert: true, legacy: true},
		{name: "scoped manifest keeps pending guard", claim: true, expired: true, scoped: true},
		{name: "scoped manifest keeps existing uncertainty rule", scoped: true, upsert: true, allowed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			req := jobsRequest(tc.name)
			req.CallerSupplied = true
			if tc.fixture {
				req = fixtureRequest(fixtureShapedID(), uuid.NewString(), "pinecone:bound")
			}
			if tc.scoped {
				grantExec(t, owner, "UPDATE document_deletion_activation SET activated_at=clock_timestamp()")
				req.SupportsScopedVectors = true
			}
			res, err := owner.AcceptDocument(ctx, req)
			require.NoError(t, err)
			require.Equal(t, domain.AcceptAccepted, res.Outcome)
			id := req.Document.ID
			t.Cleanup(func() {
				cleanupCtx := context.WithoutCancel(ctx)
				if tc.fixture {
					// The reservation references the live document.
					_, cleanupErr := owner.Pool.Exec(cleanupCtx, `DELETE FROM evaluation_fixtures WHERE document_id=$1 AND receipt=$2`, id, req.EvaluationReceipt)
					require.NoError(t, cleanupErr)
				}
				_, cleanupErr := owner.Pool.Exec(cleanupCtx, `DELETE FROM documents WHERE id=$1`, id)
				require.NoError(t, cleanupErr)
			})

			claim := func(worker string) domain.Job {
				t.Helper()
				var job *domain.Job
				var claimErr error
				if tc.fixture {
					job, claimErr = serving.ClaimJobForFixtureStore(ctx, worker, time.Minute, 5, req.EvaluationVectorIdentity)
				} else {
					job, claimErr = serving.ClaimJob(ctx, worker, time.Minute, 5)
				}
				require.NoError(t, claimErr)
				require.NotNil(t, job)
				require.Equal(t, id, job.Document.ID)
				require.Equal(t, tc.scoped, job.ScopedVectors)
				return *job
			}
			first := claim("legacy")
			if tc.upsert {
				legacyMutation(t, serving, first, false, context.DeadlineExceeded)
			}
			if tc.erase {
				legacyMutation(t, serving, first, true, context.DeadlineExceeded)
			}
			recordClaim := func(seconds float64) {
				t.Helper()
				_, recordErr := owner.Pool.Exec(ctx, `UPDATE documents SET vector_mutation_pending=TRUE,
					vector_mutation_claim=$2, vector_mutation_until=clock_timestamp()+make_interval(secs=>$3),
					vector_mutation_deletes=$4 WHERE id=$1`, id, uuid.NewString(), seconds, tc.claimDeletes)
				require.NoError(t, recordErr)
			}
			if tc.expired {
				recordClaim(-time.Hour.Seconds())
			}
			expireJobLease(t, owner, id)
			job := claim("retry")
			if tc.claim && !tc.expired {
				// An unexpired claim would prevent the selector from issuing a retry lease.
				recordClaim(time.Hour.Seconds())
			}
			if tc.legacy {
				legacyMutation(t, serving, job, true, nil)
			}
			attempt := manifestAttempt{job: job, chunkID: uuid.NewString()}
			if !tc.legacy {
				attempt = persistManifestAttempt(t, serving, id, job)
			}
			before := publicationState(t, owner, id)
			ok, err := serving.CompleteJob(ctx, attempt.job, attempt.chunks(id, tc.name), domain.IndexObservation{})
			if tc.allowed {
				require.NoError(t, err)
				require.True(t, ok)
				requirePublishedManifest(t, owner, attempt, tc.upsert || tc.expired)
			} else {
				require.EqualError(t, err, unresolvedPublication)
				require.False(t, ok)
				require.Equal(t, before, publicationState(t, owner, id))
			}
		})
	}
}

func TestManifestLegacyMutationPublicationIsAtomic(t *testing.T) {
	owner, serving := restrictedManifestDatabase(t)
	for _, name := range []string{
		"missing manifest", "malformed manifest", "another lease's manifest", "retired manifest",
		"stale lease", "reused UUID", "cancellation during activation", "activation SQL failure",
		"concurrent completion and replay",
	} {
		t.Run(name, func(t *testing.T) {
			ctx := t.Context()
			id := acceptManifested(t, owner, name)
			t.Cleanup(func() {
				_, err := owner.Pool.Exec(context.WithoutCancel(ctx), `DELETE FROM documents WHERE id=$1`, id)
				require.NoError(t, err)
			})
			first := startManifestAttempt(t, serving, id, "legacy")
			_, err := owner.Pool.Exec(ctx, `UPDATE documents SET vector_mutation_pending=TRUE,
				vector_mutation_claim=$2, vector_mutation_until=clock_timestamp()-interval '1 hour' WHERE id=$1`, id, uuid.NewString())
			require.NoError(t, err)
			_, err = owner.Pool.Exec(ctx, `INSERT INTO chunks(id,document_id,chunk_index,text,token_count) VALUES($1,$2,0,'prior chunk',2)`, first.chunkID, id)
			require.NoError(t, err)
			expireJobLease(t, owner, id)
			next := startManifestAttempt(t, serving, id, "retry")
			job := next.job
			chunks := next.chunks(id, name)
			complete := func(ctx context.Context) (bool, error) {
				return serving.CompleteJob(ctx, job, chunks, domain.IndexObservation{})
			}

			switch name {
			case "missing manifest":
				job.RevisionID = uuid.NewString()
			case "malformed manifest":
				job.RevisionID = "invalid"
			case "another lease's manifest":
				job.RevisionID = first.revision
			case "retired manifest":
				_, err := owner.Pool.Exec(ctx, `UPDATE hosted_vector_manifests SET state='retired', retired_at=now() WHERE revision_id=$1`, next.revision)
				require.NoError(t, err)
			case "stale lease":
				job = first.job
			case "reused UUID":
				require.NoError(t, owner.DeleteDocument(ctx, id))
				res, err := owner.AcceptDocument(ctx, shapedRequest(id, "replacement"))
				require.NoError(t, err)
				require.Equal(t, domain.AcceptAccepted, res.Outcome)
				replacement := startManifestAttempt(t, serving, id, "replacement")
				require.NotEqual(t, job.Incarnation, replacement.job.Incarnation)
			case "activation SQL failure":
				grantExec(t, owner, `ALTER TABLE hosted_vector_revisions ADD CONSTRAINT reject_publication CHECK (false) NOT VALID`)
				defer grantExec(t, owner, `ALTER TABLE hosted_vector_revisions DROP CONSTRAINT reject_publication`)
			}

			before := publicationState(t, owner, id)
			switch name {
			case "cancellation during activation":
				requireCanceledManifestActivation(t, owner, next, complete)
			case "concurrent completion and replay":
				start := make(chan struct{})
				results := make(chan error, 2)
				for range cap(results) {
					go func() {
						<-start
						ok, err := complete(ctx)
						if err == nil && !ok {
							err = domain.ErrJobNotOwned
						}
						results <- err
					}()
				}
				close(start)
				require.ElementsMatch(t, []error{nil, domain.ErrJobNotOwned}, []error{<-results, <-results})
				requirePublishedManifest(t, owner, next, true)
				// Replay without relying on a delivered success response.
				published := publicationState(t, owner, id)
				ok, err := complete(ctx)
				require.NoError(t, err)
				require.False(t, ok)
				require.Equal(t, published, publicationState(t, owner, id))
				return
			default:
				ok, err := complete(ctx)
				require.False(t, ok)
				switch name {
				case "stale lease", "reused UUID":
					require.NoError(t, err)
				case "activation SQL failure":
					require.ErrorContains(t, err, "set active vector revision")
					requireSQLState(t, err, "23514")
				default:
					require.ErrorContains(t, err, "vector manifest")
				}
			}
			require.Equal(t, before, publicationState(t, owner, id))
		})
	}
}

func requireCanceledManifestActivation(t *testing.T, owner *Client, a manifestAttempt, complete func(context.Context) (bool, error)) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	lock, err := owner.Pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, lock.Rollback(context.WithoutCancel(ctx))) }()
	var blocker int32
	require.NoError(t, lock.QueryRow(ctx, `SELECT pg_backend_pid() FROM hosted_vector_manifests WHERE revision_id=$1 FOR UPDATE`, a.revision).Scan(&blocker))
	result := make(chan error, 1)
	go func() {
		ok, err := complete(ctx)
		if ok {
			err = errors.New("completion committed during cancellation")
		}
		result <- err
	}()
	require.Eventually(t, func() bool {
		var blocked bool
		err := owner.Pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_stat_activity
			WHERE query LIKE '%SET state = ''active''%' AND $1=ANY(pg_blocking_pids(pid)))`, blocker).Scan(&blocked)
		return err == nil && blocked
	}, 5*time.Second, 10*time.Millisecond)
	cancel()
	require.ErrorIs(t, <-result, context.Canceled)
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
	_, err := c.ActivateDeletion(t.Context())
	require.NoError(t, err)
	_, err = c.ActivateGrants(t.Context(), false)
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

func TestVisibleEvidenceManifestScopeAndEvidenceFences(t *testing.T) {
	c, serving, _ := restrictedDatabase(t)
	f := grantsManifestFixture(t, c)
	ctx := t.Context()
	const body = "identical private passage in both tenants"
	admit := func(p domain.Principal) domain.DocumentID {
		req := ownedRequest(p, "", body)
		title := "quarterly summary"
		req.Document.FileName = &title
		req.SupportsScopedVectors = true
		require.Equal(t, domain.AcceptAccepted, f.accept(t, req).Outcome)
		return req.Document.ID
	}
	ownerID := admit(f.owner)
	ownerAttempt := startManifestAttempt(t, c, ownerID, "owner")
	valid := completeManifestHit(t, c, ownerAttempt, 0.8)
	outsiderID := admit(f.outsider)
	outsider := completeManifestHit(t, c, startManifestAttempt(t, c, outsiderID, "outsider"), 0.9)
	var directAccess bool
	require.NoError(t, serving.Pool.QueryRow(ctx, `SELECT has_table_privilege('hosted_vector_revisions', 'SELECT')`).Scan(&directAccess))
	require.False(t, directAccess)
	vis := Visibility{Pool: serving.Pool, Subject: &f.owner, DocType: domain.DefaultDocType}
	candidates := []domain.ScoredChunk{outsider}
	for _, mutate := range []func(*domain.ScoredChunk){
		func(h *domain.ScoredChunk) { h.Chunk.ID = outsider.Chunk.ID },
		func(h *domain.ScoredChunk) { h.Chunk.Index++ },
		func(h *domain.ScoredChunk) { h.Chunk.Text += " modified" },
		func(h *domain.ScoredChunk) { h.ObservedIncarnation = uuid.NewString() },
		func(h *domain.ScoredChunk) { h.Chunk.VectorIncarnation = uuid.NewString() },
		func(h *domain.ScoredChunk) {
			h.Chunk.Metadata = map[string]any{domain.VectorRevisionKey: ownerAttempt.job.Incarnation}
		},
		func(h *domain.ScoredChunk) {
			h.Chunk.Metadata = map[string]any{
				domain.VectorRevisionKey: ownerAttempt.job.Incarnation,
				domain.VectorAttemptKey:  ownerAttempt.job.LeaseToken,
			}
		},
		func(h *domain.ScoredChunk) {
			h.Chunk.Metadata = map[string]any{domain.VectorAttemptKey: ownerAttempt.job.LeaseToken}
		},
	} {
		rejected := valid
		mutate(&rejected)
		candidates = append(candidates, rejected)
	}
	candidates = append(candidates, valid)
	probes := []string{string(ownerID), string(outsiderID)}
	got, previews, err := vis.VisibleEvidenceWithPreviews(ctx, candidates, probes)
	require.NoError(t, err)
	require.Equal(t, []domain.ScoredChunk{expectedManifestEvidence(valid, ownerAttempt.job.Incarnation)}, got)
	require.Equal(t, map[string]string{string(ownerID): body}, previews)
	revisions, err := vis.VisibleRevisionChunks(ctx, []domain.ScoredChunk{outsider, valid})
	require.NoError(t, err)
	require.Equal(t, []domain.ScoredChunk{valid}, revisions)

	for _, tc := range []struct {
		name string
		vis  Visibility
	}{
		{"wrong principal", Visibility{Pool: serving.Pool, Subject: &f.peer}},
		{"wrong tenant", Visibility{Pool: serving.Pool, Subject: &f.outsider}},
		{"wrong collection", Visibility{Pool: serving.Pool, Subject: &f.owner, Collection: "team"}},
		{"wrong type", Visibility{Pool: serving.Pool, Subject: &f.owner, DocType: "other-type"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			evidence, previews, err := tc.vis.VisibleEvidenceWithPreviews(t.Context(), []domain.ScoredChunk{valid}, probes)
			require.NoError(t, err)
			require.Empty(t, evidence)
			require.Empty(t, previews)
		})
	}
	for _, status := range []string{"processing", "deleting"} {
		t.Run(status, func(t *testing.T) {
			grantExec(t, c, `UPDATE documents SET status=$2 WHERE id=$1`, ownerID, status)
			// Exercise the reader's status guard without the grants view's status filter.
			unscoped := Visibility{Pool: serving.Pool}
			evidence, previews, err := unscoped.VisibleEvidenceWithPreviews(t.Context(), []domain.ScoredChunk{valid}, probes)
			require.NoError(t, err)
			require.Empty(t, evidence)
			require.Empty(t, previews)
			revisions, err := unscoped.VisibleRevisionChunks(t.Context(), []domain.ScoredChunk{valid})
			require.NoError(t, err)
			require.Empty(t, revisions)
		})
	}
}

func completeUnrevisionedHit(t *testing.T, c *Client, job domain.Job, chunkID domain.ChunkID) domain.ScoredChunk {
	t.Helper()
	chunk := domain.Chunk{ID: chunkID, DocumentID: job.Document.ID, Text: job.Document.Content, TokenCount: 3}
	ok, err := c.CompleteJob(t.Context(), job, []domain.Chunk{chunk}, domain.IndexObservation{})
	require.NoError(t, err)
	require.True(t, ok)
	if job.ScopedVectors {
		chunk.Metadata = map[string]any{domain.VectorRevisionKey: job.Incarnation, domain.VectorAttemptKey: job.LeaseToken}
	}
	return domain.ScoredChunk{Chunk: chunk, Score: 0.9}
}

func TestVisibleEvidenceTaggedAndPlainRegression(t *testing.T) {
	c := jobsDatabase(t)
	f := grantsManifestFixture(t, c)
	ctx := t.Context()
	const body = "body shared across document lifetimes"
	req := ownedRequest(f.owner, "", body)
	req.SupportsScopedVectors = true
	require.Equal(t, domain.AcceptAccepted, f.accept(t, req).Outcome)
	first := f.claim(t)
	expireJobLease(t, c, req.Document.ID)
	current := f.claim(t)
	require.True(t, current.ScopedVectors)
	require.NotEqual(t, first.LeaseToken, current.LeaseToken)
	tagged := completeUnrevisionedHit(t, c, *current, domain.ChunkID(uuid.NewString()))
	manifestID := admitHostedOwned(t, f, f.owner, "manifest without scoped vectors", false)
	manifest := startManifestAttempt(t, c, manifestID, "manifest")
	require.False(t, manifest.job.ScopedVectors)
	manifestHit := completeManifestHit(t, c, manifest, 0.7)
	plainID := admitHostedOwned(t, f, f.owner, "plain unrevisioned content", false)
	plain := f.claim(t)
	require.Equal(t, plainID, plain.Document.ID)
	require.False(t, plain.ScopedVectors)
	plainHit := completeUnrevisionedHit(t, c, *plain, domain.ChunkID(uuid.NewString()))

	stale := tagged
	stale.Chunk.Metadata = map[string]any{domain.VectorRevisionKey: current.Incarnation, domain.VectorAttemptKey: first.LeaseToken}
	missing := tagged
	missing.Chunk.Metadata = map[string]any{domain.VectorRevisionKey: current.Incarnation}
	badAttempt := tagged
	badAttempt.Chunk.Metadata = map[string]any{domain.VectorRevisionKey: current.Incarnation, domain.VectorAttemptKey: "not-a-uuid"}
	badRevision := plainHit
	badRevision.Chunk.RevisionID = "not-a-uuid"
	unpublishedRevision := plainHit
	unpublishedRevision.Chunk.RevisionID = uuid.NewString()
	vis := Visibility{Pool: c.Pool, Subject: &f.owner}
	got, err := vis.VisibleEvidence(ctx, []domain.ScoredChunk{stale, missing, manifestHit, badAttempt, tagged, badRevision, unpublishedRevision, plainHit})
	require.NoError(t, err)
	require.Equal(t, []domain.ScoredChunk{
		expectedManifestEvidence(manifestHit, manifest.job.Incarnation),
		expectedManifestEvidence(tagged, current.Incarnation),
		expectedManifestEvidence(plainHit, plain.Incarnation),
	}, got)

	require.NoError(t, c.DeleteDocument(ctx, req.Document.ID))
	req.Owner.Incarnation = uuid.NewString()
	require.Equal(t, domain.AcceptAccepted, f.accept(t, req).Outcome)
	replacement := f.claim(t)
	require.NotEqual(t, current.Incarnation, replacement.Incarnation)
	currentHit := completeUnrevisionedHit(t, c, *replacement, tagged.Chunk.ID)
	got, err = vis.VisibleEvidence(ctx, []domain.ScoredChunk{tagged, currentHit})
	require.NoError(t, err)
	require.Equal(t, []domain.ScoredChunk{expectedManifestEvidence(currentHit, replacement.Incarnation)}, got)
}

func TestVisibleEvidenceManifestAfterPublicationRace(t *testing.T) {
	c := jobsDatabase(t)
	f := grantsManifestFixture(t, c)
	ctx := t.Context()
	id := admitHostedOwned(t, f, f.owner, "same chunk before and after publication", true)
	first := startManifestAttempt(t, c, id, "first")
	saved := completeManifestHit(t, c, first, 0.9)
	vis := Visibility{Pool: c.Pool, Subject: &f.owner}
	ev, err := vis.VisibleEvidence(ctx, []domain.ScoredChunk{saved})
	require.NoError(t, err)
	require.Equal(t, []domain.ScoredChunk{expectedManifestEvidence(saved, first.job.Incarnation)}, ev)

	// Reopen the fixture while preserving its active pointer, chunk identity and incarnation.
	grantExec(t, c, `WITH reopened AS (UPDATE documents SET status='processing' WHERE id=$1 RETURNING id)
		INSERT INTO ingest_jobs (document_id) SELECT id FROM reopened`, id)
	next := persistManifestAttemptWithChunk(ctx, t, c, id, *f.claim(t), first.chunkID)
	require.NotEqual(t, first.job.LeaseToken, next.job.LeaseToken)
	current := completeManifestHit(t, c, next, 0.8)
	require.Equal(t, "retired", manifestState(t, c, first.revision))
	candidates := []domain.ScoredChunk{saved, current}
	ev, err = vis.VisibleEvidence(ctx, candidates)
	require.NoError(t, err)
	require.Equal(t, []domain.ScoredChunk{expectedManifestEvidence(current, next.job.Incarnation)}, ev)
	revisions, err := vis.VisibleRevisionChunks(ctx, candidates)
	require.NoError(t, err)
	require.Equal(t, []domain.ScoredChunk{current}, revisions)

	lock, err := c.Pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = lock.Rollback(context.WithoutCancel(ctx)) }()
	_, err = lock.Exec(ctx, `LOCK TABLE chunks IN ACCESS EXCLUSIVE MODE`)
	require.NoError(t, err)
	waitCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	type result struct {
		hits     []domain.ScoredChunk
		previews map[string]string
		err      error
	}
	done := make(chan result, 1)
	go func() {
		hits, previews, err := vis.VisibleEvidenceWithPreviews(waitCtx, candidates, []string{string(id)})
		done <- result{hits, previews, err}
	}()
	require.Eventually(t, func() bool {
		var waiting bool
		err := c.Pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_locks
			WHERE relation='chunks'::regclass AND mode='AccessShareLock' AND NOT granted
			AND database=(SELECT oid FROM pg_database WHERE datname=current_database()))`).Scan(&waiting)
		return err == nil && waiting
	}, 5*time.Second, 10*time.Millisecond)
	cancel()
	select {
	case out := <-done:
		require.ErrorIs(t, out.err, context.Canceled)
		require.Nil(t, out.hits)
		require.Nil(t, out.previews)
	case <-time.After(5 * time.Second):
		t.Fatal("evidence query did not return after cancellation")
	}
	require.NoError(t, lock.Rollback(ctx))
}
