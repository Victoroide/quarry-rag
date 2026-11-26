package postgres

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/chunker"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// activatedDatabase returns a disposable database whose marker was set by the
// fixture itself; the coordinator under test never sets it.
func activatedDatabase(t *testing.T) *Client {
	t.Helper()
	c := jobsDatabase(t)
	_, err := c.Pool.Exec(t.Context(), `UPDATE document_deletion_activation SET activated_at=clock_timestamp()`)
	require.NoError(t, err)
	return c
}

// otherConnection opens an independent pool on the same database.
func otherConnection(t *testing.T, c *Client) *Client {
	t.Helper()
	other, err := NewClient(t.Context(), c.Pool.Config().ConnString())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, other.Close(context.Background())) })
	return other
}

type liveDocument struct {
	id          domain.DocumentID
	incarnation string
	key         string
}

func storedIncarnation(t *testing.T, c *Client, id domain.DocumentID) string {
	t.Helper()
	var incarnation string
	require.NoError(t, c.Pool.QueryRow(t.Context(), `SELECT COALESCE(incarnation,id)::text FROM documents WHERE id=$1`, string(id)).Scan(&incarnation))
	return incarnation
}

// acceptDocument admits a caller-supplied document. A scoped document records
// the tagged vector format, which the active marker permits.
func acceptDocument(t *testing.T, c *Client, content string, scoped, withSource bool) liveDocument {
	t.Helper()
	req := jobsRequest(content)
	req.CallerSupplied, req.SupportsScopedVectors = true, scoped
	doc := liveDocument{id: req.Document.ID}
	if withSource {
		doc.key = "source/" + string(doc.id) + "/" + uuid.NewString()
		req.Document.S3Key = &doc.key
	}
	result, err := c.AcceptDocument(t.Context(), req)
	require.NoError(t, err)
	require.Equal(t, domain.AcceptAccepted, result.Outcome)
	doc.incarnation = storedIncarnation(t, c, doc.id)
	var stored bool
	require.NoError(t, c.Pool.QueryRow(t.Context(), `SELECT scoped_vectors FROM documents WHERE id=$1`, string(doc.id)).Scan(&stored))
	require.Equal(t, scoped, stored, "the tagged format is recorded only once the database is activated; use markTagged before activation")
	return doc
}

// markTagged records the tagged vector format on a live document of a database
// that has not been activated, where admission never selects it.
func markTagged(ctx context.Context, t *testing.T, c *Client, id domain.DocumentID) {
	t.Helper()
	tag, err := c.Pool.Exec(ctx, `UPDATE documents SET scoped_vectors=TRUE WHERE id=$1`, string(id))
	require.NoError(t, err)
	require.EqualValues(t, 1, tag.RowsAffected())
}

// deletionRecorder stands in for the vector and object stores. A hook runs
// after the call is recorded, so an error models an applied operation whose
// reply was lost.
type deletionRecorder struct {
	mu    sync.Mutex
	calls []string
	hooks map[string]func(context.Context) error
}

func newDeletionRecorder() *deletionRecorder {
	return &deletionRecorder{hooks: map[string]func(context.Context) error{}}
}

func (r *deletionRecorder) do(ctx context.Context, kind, argument string) error {
	r.mu.Lock()
	r.calls = append(r.calls, kind+":"+argument)
	hook := r.hooks[kind]
	r.mu.Unlock()
	if hook != nil {
		return hook(ctx)
	}
	return nil
}

func (r *deletionRecorder) hook(kind string, hook func(context.Context) error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.hooks[kind] = hook
}

func (r *deletionRecorder) count(kind string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, call := range r.calls {
		if strings.HasPrefix(call, kind+":") {
			n++
		}
	}
	return n
}

func (r *deletionRecorder) all() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.calls...)
}

func (r *deletionRecorder) ops(incarnation string) domain.DeleteOperations {
	return domain.DeleteOperations{
		ExpectedIncarnation: incarnation,
		LegacyNamespace:     func(ctx context.Context, id string) error { return r.do(ctx, "namespace", id) },
		LegacyVectors:       func(ctx context.Context, id string) error { return r.do(ctx, "vectors", id) },
		RevisionVectors: func(ctx context.Context, id, revision string) error {
			return r.do(ctx, "revision", id+"/"+revision)
		},
		Source: func(ctx context.Context, key string) error { return r.do(ctx, "source", key) },
	}
}

func failFirst(message string) func(context.Context) error {
	var calls atomic.Int32
	return func(context.Context) error {
		if calls.Add(1) == 1 {
			return errors.New(message)
		}
		return nil
	}
}

type deletionReceipt struct {
	kind, mode string
	source     *string
	step       int
	held       bool
	finalized  bool
	claim      *string
}

func readReceipt(t *testing.T, c *Client, id domain.DocumentID) deletionReceipt {
	t.Helper()
	var r deletionReceipt
	require.NoError(t, c.Pool.QueryRow(t.Context(), `SELECT kind,vector_mode,source_key,step,held,finalized,claim::text
		FROM document_deletions WHERE document_id=$1`, string(id)).Scan(&r.kind, &r.mode, &r.source, &r.step, &r.held, &r.finalized, &r.claim))
	return r
}

func documentStatus(t *testing.T, c *Client, id domain.DocumentID) string {
	t.Helper()
	var status string
	err := c.Pool.QueryRow(t.Context(), `SELECT status FROM documents WHERE id=$1`, string(id)).Scan(&status)
	if err != nil {
		return ""
	}
	return status
}

func TestDeletionIntentFencesTheDocumentAtCommit(t *testing.T) {
	c := activatedDatabase(t)
	ctx := t.Context()
	doc := acceptDocument(t, c, "content that must stop matching", true, true)
	job, err := c.ClaimJob(ctx, "worker", time.Minute, 5)
	require.NoError(t, err)
	require.NotNil(t, job)
	require.True(t, job.ScopedVectors)
	visibility := Visibility{Pool: c.Pool}
	var fingerprints int
	require.NoError(t, c.Pool.QueryRow(ctx, `SELECT count(*) FROM document_fingerprints WHERE document_id=$1`, string(doc.id)).Scan(&fingerprints))
	require.Equal(t, 1, fingerprints)

	// A completed document is visible to retrieval until its intent commits.
	published := acceptDocument(t, c, "published document", true, false)
	_, err = c.Pool.Exec(ctx, `UPDATE documents SET status='completed' WHERE id=$1`, string(published.id))
	require.NoError(t, err)
	visible, err := visibility.VisibleDocumentIDs(ctx, []string{string(published.id)})
	require.NoError(t, err)
	require.Contains(t, visible, string(published.id))

	recorder := newDeletionRecorder()
	incarnation, complete, err := c.beginDeletion(ctx, doc.id, recorder.ops(doc.incarnation))
	require.NoError(t, err)
	require.False(t, complete)
	require.Equal(t, doc.incarnation, incarnation)
	require.Empty(t, recorder.all(), "recording the intent must not touch an external store")
	_, _, err = c.beginDeletion(ctx, published.id, recorder.ops(published.incarnation))
	require.NoError(t, err)

	receipt := readReceipt(t, c, doc.id)
	require.Equal(t, "qualified_intent", receipt.kind)
	require.Equal(t, revisionVectors, receipt.mode)
	require.NotNil(t, receipt.source)
	require.Equal(t, doc.key, *receipt.source)
	require.Zero(t, receipt.step)
	require.False(t, receipt.held)
	require.False(t, receipt.finalized)
	require.Nil(t, receipt.claim)
	require.Equal(t, "deleting", documentStatus(t, c, doc.id))
	var jobs int
	require.NoError(t, c.Pool.QueryRow(ctx, `SELECT count(*) FROM ingest_jobs WHERE document_id=$1`, string(doc.id)).Scan(&jobs))
	require.Zero(t, jobs, "the intent cancels the job")
	require.NoError(t, c.Pool.QueryRow(ctx, `SELECT count(*) FROM document_fingerprints WHERE document_id=$1`, string(doc.id)).Scan(&fingerprints))
	require.Zero(t, fingerprints, "shared content must stop matching")

	summary, err := c.GetDocumentSummary(ctx, doc.id)
	require.NoError(t, err)
	require.Nil(t, summary)
	stored, err := c.GetDocument(ctx, doc.id)
	require.NoError(t, err)
	require.Nil(t, stored)
	key, err := c.GetDocumentS3Key(ctx, doc.id)
	require.NoError(t, err)
	require.Nil(t, key)
	page, err := c.ListDocumentsPage(ctx, "", 10)
	require.NoError(t, err)
	require.Empty(t, page)
	refs, err := c.ListDocuments(ctx)
	require.NoError(t, err)
	require.Empty(t, refs)
	var streamed int
	require.NoError(t, c.StreamDocuments(ctx, func(domain.DocumentSummary) error { streamed++; return nil }))
	require.Zero(t, streamed)
	signed, err := c.WithDocumentFile(ctx, doc.id, func(context.Context, string) error {
		t.Error("signed a source after the intent committed")
		return nil
	})
	require.NoError(t, err)
	require.False(t, signed)
	visible, err = visibility.VisibleDocumentIDs(ctx, []string{string(doc.id), string(published.id)})
	require.NoError(t, err)
	require.Empty(t, visible, "retired documents leave retrieval at the intent commit")
	_, matched, err := c.CheckFingerprint(ctx, jobsRequest("content that must stop matching").Fingerprint)
	require.NoError(t, err)
	require.False(t, matched)

	// The retired worker can neither renew, complete nor mutate vectors.
	owned, err := c.HeartbeatJob(ctx, *job, time.Minute)
	require.NoError(t, err)
	require.False(t, owned)
	done, err := c.CompleteJob(ctx, *job, nil, domain.IndexObservation{})
	require.NoError(t, err)
	require.False(t, done)
	require.ErrorIs(t, c.mutateJobVectors(ctx, *job, false, func(context.Context) error {
		t.Error("a retired worker reached the vector store")
		return nil
	}), domain.ErrJobNotOwned)

	// The identity cannot be admitted again while the intent is unfinished.
	req := jobsRequest("replacement")
	req.Document.ID, req.CallerSupplied = doc.id, true
	conflict, err := c.AcceptDocument(ctx, req)
	require.NoError(t, err)
	require.Equal(t, domain.AcceptDeletionConflict, conflict.Outcome)

	// Repeating the request resumes the same intent without changing it.
	again, complete, err := c.beginDeletion(ctx, doc.id, recorder.ops(doc.incarnation))
	require.NoError(t, err)
	require.False(t, complete)
	require.Equal(t, doc.incarnation, again)
	require.Equal(t, receipt, readReceipt(t, c, doc.id))
}

func TestDeletionIntentIsRefusedBeforeItCommits(t *testing.T) {
	t.Run("inactive database", func(t *testing.T) {
		c := jobsDatabase(t)
		doc := acceptDocument(t, c, "inactive", false, false)
		_, _, err := c.beginDeletion(t.Context(), doc.id, newDeletionRecorder().ops(doc.incarnation))
		require.ErrorIs(t, err, domain.ErrDeletionInactive)
		require.Equal(t, "processing", documentStatus(t, c, doc.id))
		var receipts int
		require.NoError(t, c.Pool.QueryRow(t.Context(), `SELECT count(*) FROM document_deletions`).Scan(&receipts))
		require.Zero(t, receipts)
		active, err := c.DeletionActive(t.Context())
		require.NoError(t, err)
		require.False(t, active, "the coordinator must never set the marker")
	})
	c := activatedDatabase(t)
	scoped := acceptDocument(t, c, "scoped with source", true, true)
	legacy := acceptDocument(t, c, "legacy", false, false)
	for _, tc := range []struct {
		name   string
		doc    liveDocument
		mutate func(*domain.DeleteOperations)
		want   error
	}{
		{name: "bare request", doc: scoped, mutate: func(o *domain.DeleteOperations) { o.ExpectedIncarnation = "" }, want: domain.ErrDeletionPreconditionRequired},
		{name: "other lifetime", doc: scoped, mutate: func(o *domain.DeleteOperations) { o.ExpectedIncarnation = uuid.NewString() }, want: domain.ErrDeletionPreconditionFailed},
		{name: "malformed incarnation", doc: scoped, mutate: func(o *domain.DeleteOperations) { o.ExpectedIncarnation = "not-a-uuid" }, want: domain.ErrDeletionPreconditionFailed},
		{name: "source storage missing", doc: scoped, mutate: func(o *domain.DeleteOperations) { o.Source = nil }, want: domain.ErrFileStorageUnavailable},
		{name: "scoped vectors missing", doc: scoped, mutate: func(o *domain.DeleteOperations) { o.RevisionVectors = nil }, want: domain.ErrVectorStorageUnavailable},
		{name: "half a legacy pair", doc: legacy, mutate: func(o *domain.DeleteOperations) { o.LegacyNamespace = nil }, want: domain.ErrVectorStorageUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ops := newDeletionRecorder().ops(tc.doc.incarnation)
			tc.mutate(&ops)
			_, _, err := c.beginDeletion(t.Context(), tc.doc.id, ops)
			require.ErrorIs(t, err, tc.want)
			require.NotEqual(t, "deleting", documentStatus(t, c, tc.doc.id))
			var receipts int
			require.NoError(t, c.Pool.QueryRow(t.Context(), `SELECT count(*) FROM document_deletions WHERE document_id=$1`, string(tc.doc.id)).Scan(&receipts))
			require.Zero(t, receipts, "a refused request must leave no receipt")
		})
	}
	_, _, err := c.beginDeletion(t.Context(), domain.DocumentID(uuid.NewString()), newDeletionRecorder().ops(""))
	require.ErrorIs(t, err, domain.ErrDocumentNotFound)
}

func TestDeletionResumesAtEveryExternalBoundary(t *testing.T) {
	for _, tc := range []struct {
		name    string
		scoped  bool
		failing string
		step    int
		want    map[string]int
	}{
		{name: "historical namespace", failing: "namespace", step: stepNamespace, want: map[string]int{"namespace": 2, "vectors": 1, "revision": 0, "source": 1}},
		{name: "historical document vectors", failing: "vectors", step: stepVectors, want: map[string]int{"namespace": 1, "vectors": 2, "revision": 0, "source": 1}},
		{name: "incarnation vectors", scoped: true, failing: "revision", step: stepVectors, want: map[string]int{"namespace": 0, "vectors": 0, "revision": 2, "source": 1}},
		{name: "source object", scoped: true, failing: "source", step: stepSource, want: map[string]int{"namespace": 0, "vectors": 0, "revision": 1, "source": 2}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := activatedDatabase(t)
			retry := otherConnection(t, c)
			doc := acceptDocument(t, c, "boundary "+tc.name, tc.scoped, true)
			recorder := newDeletionRecorder()
			recorder.hook(tc.failing, failFirst("provider reply lost"))
			ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
			defer cancel()

			err := c.DeleteWithCleanup(ctx, doc.id, recorder.ops(doc.incarnation))
			require.ErrorContains(t, err, "provider reply lost")
			receipt := readReceipt(t, c, doc.id)
			require.Equal(t, tc.step, receipt.step, "the failed step must stay pending")
			require.False(t, receipt.finalized)
			require.Nil(t, receipt.claim, "a failed attempt releases its claim")
			require.NotNil(t, receipt.source)
			require.Equal(t, doc.key, *receipt.source, "the exact source key stays recorded")
			require.Equal(t, "deleting", documentStatus(t, c, doc.id))

			require.NoError(t, retry.DeleteWithCleanup(ctx, doc.id, recorder.ops(doc.incarnation)))
			for kind, want := range tc.want {
				require.Equal(t, want, recorder.count(kind), kind)
			}
			require.Empty(t, documentStatus(t, c, doc.id))
			receipt = readReceipt(t, retry, doc.id)
			require.True(t, receipt.finalized)
			require.Equal(t, stepDone, receipt.step)
			require.Equal(t, !tc.scoped, receipt.held, "only historical vectors keep the identity held")
			if tc.scoped {
				require.Nil(t, receipt.source, "a completed clean receipt drops the source key")
			}
			// A matching retry acknowledges the completed deletion without effects.
			before := len(recorder.all())
			require.NoError(t, c.DeleteWithCleanup(ctx, doc.id, recorder.ops(doc.incarnation)))
			require.Len(t, recorder.all(), before)
		})
	}
}

func TestDeletionSkipsInapplicableSteps(t *testing.T) {
	c := activatedDatabase(t)
	doc := acceptDocument(t, c, "transactional vectors without a source", false, false)
	recorder := newDeletionRecorder()
	ops := recorder.ops(doc.incarnation)
	ops.LegacyNamespace, ops.LegacyVectors, ops.RevisionVectors = nil, nil, nil
	require.NoError(t, c.DeleteWithCleanup(t.Context(), doc.id, ops))
	require.Empty(t, recorder.all())
	receipt := readReceipt(t, c, doc.id)
	require.Equal(t, localVectors, receipt.mode)
	require.True(t, receipt.finalized)
	require.False(t, receipt.held, "a transactional delete permits reuse")
	reused, err := c.AcceptDocument(t.Context(), func() domain.AcceptRequest {
		req := jobsRequest("replacement")
		req.Document.ID, req.CallerSupplied = doc.id, true
		return req
	}())
	require.NoError(t, err)
	require.Equal(t, domain.AcceptAccepted, reused.Outcome)
}

func TestDeletionRetainsCheckpointsAfterFinalCommitRollsBack(t *testing.T) {
	c := activatedDatabase(t)
	ctx := t.Context()
	doc := acceptDocument(t, c, "commit boundary", true, true)
	_, err := c.Pool.Exec(ctx, `
		CREATE FUNCTION reject_document_delete() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN RAISE EXCEPTION 'final commit rejected'; END $$;
		CREATE CONSTRAINT TRIGGER reject_document_delete AFTER DELETE ON documents
		DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION reject_document_delete()`)
	require.NoError(t, err)
	recorder := newDeletionRecorder()
	require.ErrorContains(t, c.DeleteWithCleanup(ctx, doc.id, recorder.ops(doc.incarnation)), "final commit rejected")
	receipt := readReceipt(t, c, doc.id)
	require.Equal(t, stepDone, receipt.step)
	require.False(t, receipt.finalized)
	require.Nil(t, receipt.claim)
	require.Equal(t, "deleting", documentStatus(t, c, doc.id))
	require.Equal(t, 1, recorder.count("revision"))
	require.Equal(t, 1, recorder.count("source"))

	_, err = c.Pool.Exec(ctx, `DROP TRIGGER reject_document_delete ON documents`)
	require.NoError(t, err)
	require.NoError(t, otherConnection(t, c).DeleteWithCleanup(ctx, doc.id, recorder.ops(doc.incarnation)))
	require.Equal(t, 1, recorder.count("revision"), "acknowledged vector cleanup is not repeated")
	require.Equal(t, 1, recorder.count("source"), "acknowledged source cleanup is not repeated")
	require.True(t, readReceipt(t, c, doc.id).finalized)
	require.Empty(t, documentStatus(t, c, doc.id))
}

func TestDuplicateRequestsCoalesceOnOneClaim(t *testing.T) {
	c := activatedDatabase(t)
	second := otherConnection(t, c)
	doc := acceptDocument(t, c, "single owner", true, true)
	recorder := newDeletionRecorder()
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	recorder.hook("revision", func(ctx context.Context) error {
		once.Do(func() { close(entered) })
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	first := make(chan error, 1)
	go func() { first <- c.DeleteWithCleanup(ctx, doc.id, recorder.ops(doc.incarnation)) }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	receipt := readReceipt(t, c, doc.id)
	require.NotNil(t, receipt.claim, "the owner holds a durable claim during the external call")

	// A duplicate neither repeats the call nor advances the receipt.
	require.ErrorIs(t, second.DeleteWithCleanup(ctx, doc.id, recorder.ops(doc.incarnation)), domain.ErrDeletionBusy)
	require.Equal(t, 1, recorder.count("revision"))
	require.Zero(t, recorder.count("source"))
	require.Equal(t, stepVectors, readReceipt(t, c, doc.id).step)

	close(release)
	require.NoError(t, <-first)
	require.NoError(t, second.DeleteWithCleanup(ctx, doc.id, recorder.ops(doc.incarnation)), "the receipt acknowledges the finished deletion")
	require.Equal(t, 1, recorder.count("revision"))
	require.Equal(t, 1, recorder.count("source"))
}

func TestExpiredClaimLetsAnotherClaimantResumeAndFencesTheFirst(t *testing.T) {
	c := activatedDatabase(t)
	ctx := t.Context()
	doc := acceptDocument(t, c, "expired claim", true, true)
	_, _, err := c.beginDeletion(ctx, doc.id, newDeletionRecorder().ops(doc.incarnation))
	require.NoError(t, err)
	stale := uuid.NewString()
	_, err = c.Pool.Exec(ctx, `UPDATE document_deletions SET claim=$2,claim_until=clock_timestamp()+interval '1 hour' WHERE document_id=$1`, string(doc.id), stale)
	require.NoError(t, err)

	recorder := newDeletionRecorder()
	require.ErrorIs(t, c.DeleteWithCleanup(ctx, doc.id, recorder.ops(doc.incarnation)), domain.ErrDeletionBusy)
	require.Empty(t, recorder.all(), "a live claim must not be bypassed")

	_, err = c.Pool.Exec(ctx, `UPDATE document_deletions SET claim_until=clock_timestamp()-interval '1 second' WHERE document_id=$1`, string(doc.id))
	require.NoError(t, err)
	// The elapsed claim permits a new claimant to resume, and the late
	// checkpoint of the first claimant cannot move the receipt.
	require.ErrorIs(t, c.checkpointDeletion(ctx, doc.id, doc.incarnation, stale, stepNamespace), domain.ErrDeletionBusy)
	require.NoError(t, c.DeleteWithCleanup(ctx, doc.id, recorder.ops(doc.incarnation)))
	require.Equal(t, 1, recorder.count("revision"))
	require.Equal(t, 1, recorder.count("source"))
	require.ErrorIs(t, c.checkpointDeletion(ctx, doc.id, doc.incarnation, stale, stepNamespace), domain.ErrDeletionBusy)
	require.True(t, readReceipt(t, c, doc.id).finalized)
}

func TestDeletionWaitsForALiveWorkerMutation(t *testing.T) {
	c := activatedDatabase(t)
	other := otherConnection(t, c)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	doc := acceptDocument(t, c, "worker is writing", true, true)
	job, err := c.ClaimJob(ctx, "worker", time.Minute, 5)
	require.NoError(t, err)
	require.NotNil(t, job)
	entered, release := make(chan struct{}), make(chan struct{})
	written := make(chan error, 1)
	go func() {
		written <- c.mutateJobVectors(ctx, *job, false, func(context.Context) error {
			close(entered)
			<-release
			return nil
		})
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	recorder := newDeletionRecorder()
	require.ErrorIs(t, other.DeleteWithCleanup(ctx, doc.id, recorder.ops(doc.incarnation)), domain.ErrDeletionBusy)
	require.Empty(t, recorder.all(), "cleanup must not race a vector write that is still in flight")
	require.Equal(t, "deleting", documentStatus(t, c, doc.id), "the intent still fenced the document")
	close(release)
	require.NoError(t, <-written)
	require.NoError(t, other.DeleteWithCleanup(ctx, doc.id, recorder.ops(doc.incarnation)))
	require.False(t, readReceipt(t, c, doc.id).held, "a confirmed write alone leaves no uncertainty")
}

func TestScopedUncertainMutationAloneHoldsIdentity(t *testing.T) {
	for _, scenario := range []string{"clean", "unknown reply", "interrupted"} {
		t.Run(scenario, func(t *testing.T) {
			c := activatedDatabase(t)
			observer := otherConnection(t, c)
			ctx := t.Context()
			doc := acceptDocument(t, c, scenario, true, false)
			job, err := c.ClaimJob(ctx, scenario, time.Minute, 5)
			require.NoError(t, err)
			require.NotNil(t, job)
			switch scenario {
			case "clean":
				require.NoError(t, c.mutateJobVectors(ctx, *job, false, func(context.Context) error { return nil }))
			case "unknown reply":
				require.ErrorIs(t, c.mutateJobVectors(ctx, *job, false, func(context.Context) error { return context.DeadlineExceeded }), context.DeadlineExceeded)
			case "interrupted":
				require.Panics(t, func() {
					_ = c.mutateJobVectors(ctx, *job, false, func(context.Context) error { panic("process interrupted") })
				})
			}
			recorder := newDeletionRecorder()
			ops := recorder.ops(doc.incarnation)
			_, _, err = c.beginDeletion(ctx, doc.id, ops)
			require.NoError(t, err)
			require.False(t, readReceipt(t, observer, doc.id).held, "the intent alone never holds a scoped identity")
			var pending, uncertain bool
			require.NoError(t, observer.Pool.QueryRow(ctx, `SELECT vector_mutation_pending,vector_mutation_uncertain FROM documents WHERE id=$1`, string(doc.id)).Scan(&pending, &uncertain))
			require.Equal(t, scenario == "interrupted", pending)
			require.Equal(t, scenario == "unknown reply", uncertain)
			if scenario == "interrupted" {
				// The abandoned claim expires, which lets cleanup begin.
				_, err = observer.Pool.Exec(ctx, `UPDATE documents SET vector_mutation_until=clock_timestamp() WHERE id=$1`, string(doc.id))
				require.NoError(t, err)
			}
			require.NoError(t, c.DeleteWithCleanup(ctx, doc.id, ops))
			require.Equal(t, scenario != "clean", readReceipt(t, observer, doc.id).held)

			req := jobsRequest("replacement " + scenario)
			req.Document.ID, req.CallerSupplied, req.SupportsScopedVectors = doc.id, true, true
			reused, err := c.AcceptDocument(ctx, req)
			require.NoError(t, err)
			if scenario == "clean" {
				require.Equal(t, domain.AcceptAccepted, reused.Outcome)
				replacement := storedIncarnation(t, c, doc.id)
				require.NotEqual(t, doc.incarnation, replacement)
				require.NoError(t, c.DeleteWithCleanup(ctx, doc.id, recorder.ops(replacement)))
				return
			}
			require.Equal(t, domain.AcceptDeletionConflict, reused.Outcome)
			require.NoError(t, c.DeleteWithCleanup(ctx, doc.id, ops), "the completed receipt still acknowledges a retry")
		})
	}
}

func TestHistoricalVectorsKeepTheIdentityHeld(t *testing.T) {
	c := activatedDatabase(t)
	doc := acceptDocument(t, c, "historical", false, false)
	recorder := newDeletionRecorder()
	require.NoError(t, c.DeleteWithCleanup(t.Context(), doc.id, recorder.ops(doc.incarnation)))
	receipt := readReceipt(t, c, doc.id)
	require.True(t, receipt.finalized)
	require.True(t, receipt.held)
	require.Equal(t, legacyVectors, receipt.mode)
	req := jobsRequest("replacement")
	req.Document.ID, req.CallerSupplied = doc.id, true
	held, err := c.AcceptDocument(t.Context(), req)
	require.NoError(t, err)
	require.Equal(t, domain.AcceptDeletionConflict, held.Outcome)
	require.Empty(t, documentStatus(t, c, doc.id))
	require.NoError(t, c.DeleteWithCleanup(t.Context(), doc.id, recorder.ops(doc.incarnation)), "the held receipt still acknowledges a retry")
	require.ErrorIs(t, c.DeleteWithCleanup(t.Context(), doc.id, recorder.ops("")), domain.ErrDocumentNotFound)
}

func TestQualifiedRetryAfterReuseTargetsOnlyItsOwnLifetime(t *testing.T) {
	c := activatedDatabase(t)
	ctx := t.Context()
	first := acceptDocument(t, c, "first lifetime", true, true)
	recorder := newDeletionRecorder()
	require.NoError(t, c.DeleteWithCleanup(ctx, first.id, recorder.ops(first.incarnation)))
	require.NoError(t, c.DeleteWithCleanup(ctx, first.id, recorder.ops(first.incarnation)))
	require.ErrorIs(t, c.DeleteWithCleanup(ctx, first.id, recorder.ops("")), domain.ErrDocumentNotFound)
	require.ErrorIs(t, c.DeleteWithCleanup(ctx, first.id, recorder.ops(uuid.NewString())), domain.ErrDocumentNotFound)

	replacementKey := "source/replacement/" + uuid.NewString()
	req := jobsRequest("second lifetime")
	req.Document.ID, req.Document.S3Key, req.CallerSupplied, req.SupportsScopedVectors = first.id, &replacementKey, true, true
	accepted, err := c.AcceptDocument(ctx, req)
	require.NoError(t, err)
	require.Equal(t, domain.AcceptAccepted, accepted.Outcome)
	replacement := storedIncarnation(t, c, first.id)
	require.NotEqual(t, first.incarnation, replacement)
	before := recorder.all()

	require.ErrorIs(t, c.DeleteWithCleanup(ctx, first.id, recorder.ops(first.incarnation)), domain.ErrDeletionPreconditionFailed)
	require.ErrorIs(t, c.DeleteWithCleanup(ctx, first.id, recorder.ops("")), domain.ErrDeletionPreconditionRequired)
	require.Equal(t, before, recorder.all(), "a stale retry must not touch the replacement's stores")
	stored, err := c.GetDocumentS3Key(ctx, first.id)
	require.NoError(t, err)
	require.NotNil(t, stored)
	require.Equal(t, replacementKey, *stored)

	require.NoError(t, c.DeleteWithCleanup(ctx, first.id, recorder.ops(replacement)))
	require.Contains(t, recorder.all(), "source:"+replacementKey)
	require.Contains(t, recorder.all(), "revision:"+string(first.id)+"/"+replacement)
	require.ErrorIs(t, c.DeleteWithCleanup(ctx, first.id, recorder.ops(first.incarnation)), domain.ErrDocumentNotFound,
		"the replacement's receipt replaced the retired one")
}

func TestIntentFencesAnExpiredJobAcrossReuse(t *testing.T) {
	c := activatedDatabase(t)
	ctx := t.Context()
	first := acceptDocument(t, c, "first source", true, true)
	retired, err := c.ClaimJob(ctx, "same-worker", time.Minute, 5)
	require.NoError(t, err)
	require.NotNil(t, retired)
	_, err = c.Pool.Exec(ctx, `UPDATE ingest_jobs SET lease_until=clock_timestamp()-interval '1 second' WHERE document_id=$1`, string(first.id))
	require.NoError(t, err)

	recorder := newDeletionRecorder()
	ops := recorder.ops(first.incarnation)
	_, _, err = c.beginDeletion(ctx, first.id, ops)
	require.NoError(t, err)
	require.ErrorIs(t, c.mutateJobVectors(ctx, *retired, false, func(context.Context) error {
		t.Error("an expired job used the vector store")
		return nil
	}), domain.ErrJobNotOwned)
	require.NoError(t, c.DeleteWithCleanup(ctx, first.id, ops))

	replacementKey := "replacement-source"
	req := jobsRequest("replacement content")
	req.Document.ID, req.Document.S3Key, req.CallerSupplied, req.SupportsScopedVectors = first.id, &replacementKey, true, true
	accepted, err := c.AcceptDocument(ctx, req)
	require.NoError(t, err)
	require.Equal(t, domain.AcceptAccepted, accepted.Outcome)
	fresh, err := c.ClaimJob(ctx, "same-worker", time.Minute, 5)
	require.NoError(t, err)
	require.NotNil(t, fresh)
	require.Equal(t, retired.Attempts, fresh.Attempts, "the owner name and attempt repeat across the reuse")
	require.NotEqual(t, retired.Incarnation, fresh.Incarnation)
	done, err := c.CompleteJob(ctx, *retired, nil, domain.IndexObservation{})
	require.NoError(t, err)
	require.False(t, done)
	require.ErrorIs(t, c.mutateJobVectors(ctx, *retired, false, func(context.Context) error {
		t.Error("a retired job used the vector store")
		return nil
	}), domain.ErrJobNotOwned)
	require.Equal(t, []string{"revision:" + string(first.id) + "/" + first.incarnation, "source:" + first.key}, recorder.all())
	stored, err := c.GetDocumentS3Key(ctx, first.id)
	require.NoError(t, err)
	require.Equal(t, &replacementKey, stored)
}

func TestAdmissionTreatsSharedContentOfADeletingDocumentAsAbsent(t *testing.T) {
	c := activatedDatabase(t)
	ctx := t.Context()
	const body = "shared by two uploads"
	first := jobsRequest(body)
	key := "first-source"
	first.Document.S3Key = &key
	accepted, err := c.AcceptDocument(ctx, first)
	require.NoError(t, err)
	require.Equal(t, domain.AcceptAccepted, accepted.Outcome)
	incarnation := storedIncarnation(t, c, accepted.DocumentID)
	_, err = c.Pool.Exec(ctx, `UPDATE documents SET status='completed' WHERE id=$1`, string(accepted.DocumentID))
	require.NoError(t, err)

	duplicate := jobsRequest(body)
	duplicate.RequireSource = true
	result, err := c.AcceptDocument(ctx, duplicate)
	require.NoError(t, err)
	require.Equal(t, domain.AcceptDuplicate, result.Outcome, "control: a live document matches")

	recorder := newDeletionRecorder()
	ops := recorder.ops(incarnation)
	_, _, err = c.beginDeletion(ctx, accepted.DocumentID, ops)
	require.NoError(t, err)
	result, err = c.AcceptDocument(ctx, duplicate)
	require.NoError(t, err)
	require.Equal(t, domain.AcceptNeedsSource, result.Outcome, "the handler must stage a new source before admitting")
	replacementKey := "replacement-source"
	duplicate.Document.S3Key = &replacementKey
	result, err = c.AcceptDocument(ctx, duplicate)
	require.NoError(t, err)
	require.Equal(t, domain.AcceptAccepted, result.Outcome)
	require.NotEqual(t, accepted.DocumentID, result.DocumentID)

	require.NoError(t, c.DeleteWithCleanup(ctx, accepted.DocumentID, ops))
	stored, err := c.GetDocumentS3Key(ctx, result.DocumentID)
	require.NoError(t, err)
	require.Equal(t, &replacementKey, stored, "the new document keeps its own source")
	require.Contains(t, recorder.all(), "source:"+key)
	require.NotContains(t, recorder.all(), "source:"+replacementKey)
}

func TestFileSigningIsSerializedAgainstTheIntent(t *testing.T) {
	c := activatedDatabase(t)
	other := otherConnection(t, c)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	doc := acceptDocument(t, c, "signing boundary", true, true)
	inside, finish := make(chan struct{}), make(chan struct{})
	signed := make(chan string, 2)
	signing := make(chan error, 1)
	go func() {
		_, err := c.WithDocumentFile(ctx, doc.id, func(_ context.Context, key string) error {
			signed <- key
			close(inside)
			<-finish
			return nil
		})
		signing <- err
	}()
	select {
	case <-inside:
	case err := <-signing:
		t.Fatalf("signing ended before its callback ran: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	begun := make(chan error, 1)
	go func() {
		_, _, err := other.beginDeletion(ctx, doc.id, newDeletionRecorder().ops(doc.incarnation))
		begun <- err
	}()
	select {
	case err := <-begun:
		t.Fatalf("the intent committed while a signature was being prepared: %v", err)
	case <-time.After(300 * time.Millisecond):
	}
	close(finish)
	require.NoError(t, <-signing)
	require.NoError(t, <-begun)
	require.Equal(t, doc.key, <-signed, "the link issued before the boundary used the recorded source")
	found, err := c.WithDocumentFile(ctx, doc.id, func(context.Context, string) error {
		t.Error("signed after the intent committed")
		return nil
	})
	require.NoError(t, err)
	require.False(t, found)
}

func TestDeletionReceiptReplacementIsGuarded(t *testing.T) {
	for _, tc := range []struct {
		name  string
		state string
		busy  bool
	}{
		{name: "released receipt", state: "TRUE, FALSE, NULL, NULL"},
		{name: "held receipt", state: "TRUE, TRUE, NULL, NULL", busy: true},
		{name: "unfinished receipt", state: "FALSE, FALSE, NULL, NULL", busy: true},
		{name: "live claim", state: "TRUE, FALSE, gen_random_uuid(), clock_timestamp()+interval '1 hour'", busy: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := activatedDatabase(t)
			doc := acceptDocument(t, c, tc.name, true, false)
			// A receipt of another incarnation already owns the public UUID.
			_, err := c.Pool.Exec(t.Context(), `INSERT INTO document_deletions(document_id,incarnation,kind,vector_mode,finalized,held,claim,claim_until)
				VALUES($1,gen_random_uuid(),'quarantine','revision',`+tc.state+`)`, string(doc.id))
			require.NoError(t, err)
			recorder := newDeletionRecorder()
			_, _, err = c.beginDeletion(t.Context(), doc.id, recorder.ops(doc.incarnation))
			if tc.busy {
				require.ErrorIs(t, err, domain.ErrDeletionBusy)
				require.Equal(t, "processing", documentStatus(t, c, doc.id), "a refused intent leaves the document untouched")
				require.Equal(t, "quarantine", readReceipt(t, c, doc.id).kind)
				return
			}
			require.NoError(t, err)
			require.Equal(t, "qualified_intent", readReceipt(t, c, doc.id).kind)
			require.Equal(t, "deleting", documentStatus(t, c, doc.id))
		})
	}
}

func TestDeletionRefusesStoresThatDifferFromTheRecordedTarget(t *testing.T) {
	c := activatedDatabase(t)
	ctx := t.Context()
	doc := acceptDocument(t, c, "recorded target", true, true)
	recorder := newDeletionRecorder()
	ops := recorder.ops(doc.incarnation)
	ops.SourceBucket, ops.VectorIndex = "originals", "index"
	_, _, err := c.beginDeletion(ctx, doc.id, ops)
	require.NoError(t, err)
	var bucket, index *string
	require.NoError(t, c.Pool.QueryRow(ctx, `SELECT source_bucket,vector_index FROM document_deletions WHERE document_id=$1`, string(doc.id)).Scan(&bucket, &index))
	require.Equal(t, "originals", deref(bucket))
	require.Equal(t, "index", deref(index))

	for name, mutate := range map[string]func(*domain.DeleteOperations){
		"other bucket":   func(o *domain.DeleteOperations) { o.SourceBucket = "elsewhere" },
		"unnamed bucket": func(o *domain.DeleteOperations) { o.SourceBucket = "" },
		"other index":    func(o *domain.DeleteOperations) { o.VectorIndex = "elsewhere" },
		"process local":  func(o *domain.DeleteOperations) { o.InMemory = true },
	} {
		t.Run(name, func(t *testing.T) {
			mismatched := recorder.ops(doc.incarnation)
			mismatched.SourceBucket, mismatched.VectorIndex = "originals", "index"
			mutate(&mismatched)
			require.ErrorIs(t, c.DeleteWithCleanup(t.Context(), doc.id, mismatched), domain.ErrDeletionTargetMismatch)
			require.Empty(t, recorder.all(), "no store may be touched through a different target")
			require.Nil(t, readReceipt(t, c, doc.id).claim, "the refused attempt releases its claim")
		})
	}
	require.NoError(t, c.DeleteWithCleanup(ctx, doc.id, ops))
	require.True(t, readReceipt(t, c, doc.id).finalized)
}

func TestDeletionOfAFailedAndOfAProcessingDocument(t *testing.T) {
	c := activatedDatabase(t)
	ctx := t.Context()
	for _, state := range []string{"processing", "failed", "completed"} {
		doc := acceptDocument(t, c, "state "+state, true, true)
		_, err := c.Pool.Exec(ctx, `UPDATE documents SET status=$2 WHERE id=$1`, string(doc.id), state)
		require.NoError(t, err)
		recorder := newDeletionRecorder()
		require.NoError(t, c.DeleteWithCleanup(ctx, doc.id, recorder.ops(doc.incarnation)), state)
		require.Equal(t, 1, recorder.count("source"), state)
		require.Empty(t, documentStatus(t, c, doc.id), state)
	}
}

func TestDeletionDoesNotChangeTheMarker(t *testing.T) {
	c := jobsDatabase(t)
	doc := acceptDocument(t, c, "inactive", false, false)
	require.Error(t, c.DeleteWithCleanup(t.Context(), doc.id, newDeletionRecorder().ops(doc.incarnation)))
	var marker *time.Time
	require.NoError(t, c.Pool.QueryRow(t.Context(), `SELECT activated_at FROM document_deletion_activation`).Scan(&marker))
	require.Nil(t, marker)
	// Reading the mode is also free of effects.
	_, err := c.DeletionActive(t.Context())
	require.NoError(t, err)
	require.NoError(t, c.Pool.QueryRow(t.Context(), `SELECT activated_at FROM document_deletion_activation`).Scan(&marker))
	require.Nil(t, marker)
}

func TestSharedFingerprintsSurviveOtherDocumentDeletion(t *testing.T) {
	c := activatedDatabase(t)
	ctx := t.Context()
	keep := acceptDocument(t, c, "unrelated document that stays", true, false)
	doomed := acceptDocument(t, c, "document being removed", true, false)
	require.NoError(t, c.DeleteWithCleanup(ctx, doomed.id, newDeletionRecorder().ops(doomed.incarnation)))
	id, found, err := c.CheckFingerprint(ctx, chunker.Fingerprint("unrelated document that stays"))
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, keep.id, id)
	require.Equal(t, "processing", documentStatus(t, c, keep.id))
}

func TestDeletionAddressesTheCanonicalUUID(t *testing.T) {
	c := activatedDatabase(t)
	doc := acceptDocument(t, c, "canonical identifier", true, true)
	recorder := newDeletionRecorder()
	upper := domain.DocumentID(strings.ToUpper(string(doc.id)))
	require.NoError(t, c.DeleteWithCleanup(t.Context(), upper, recorder.ops(strings.ToUpper(doc.incarnation))))
	require.Equal(t, []string{"revision:" + string(doc.id) + "/" + doc.incarnation, "source:" + doc.key}, recorder.all(),
		"external stores key records by the canonical form")
	require.ErrorIs(t, c.DeleteWithCleanup(t.Context(), "not-a-uuid", recorder.ops("")), domain.ErrDocumentNotFound)
	require.True(t, readReceipt(t, c, doc.id).finalized)
}

func TestIntentSerializesWithJobCompletion(t *testing.T) {
	c := activatedDatabase(t)
	other := otherConnection(t, c)
	for i := range 8 {
		doc := acceptDocument(t, c, "completion race "+uuid.NewString(), true, false)
		job, err := c.ClaimJob(t.Context(), "worker", time.Minute, 5)
		require.NoError(t, err)
		require.NotNil(t, job)
		require.Equal(t, doc.id, job.Document.ID)
		var wg sync.WaitGroup
		var completed bool
		var completeErr, beginErr error
		wg.Go(func() {
			completed, completeErr = c.CompleteJob(t.Context(), *job, nil, domain.IndexObservation{})
		})
		wg.Go(func() {
			_, _, beginErr = other.beginDeletion(t.Context(), doc.id, newDeletionRecorder().ops(doc.incarnation))
		})
		wg.Wait()
		require.NoError(t, completeErr, i)
		require.NoError(t, beginErr, i)
		require.Equal(t, "deleting", documentStatus(t, c, doc.id), "a completion must never overwrite the intent (iteration %d, completed=%v)", i, completed)
		done, err := c.CompleteJob(t.Context(), *job, nil, domain.IndexObservation{})
		require.NoError(t, err)
		require.False(t, done, "no completion after the intent committed")
	}
}

func TestCompatibilityRemovalNeverCompletesACheckpointedDeletion(t *testing.T) {
	c := activatedDatabase(t)
	ctx := t.Context()
	doc := acceptDocument(t, c, "intent before compatibility removal", true, true)
	_, _, err := c.beginDeletion(ctx, doc.id, newDeletionRecorder().ops(doc.incarnation))
	require.NoError(t, err)
	observed := &domain.Document{ID: doc.id, Incarnation: doc.incarnation}
	require.ErrorIs(t, c.CheckLiveVectorRemoval(ctx, observed), domain.ErrLiveVectorMutation)
	require.ErrorIs(t, c.RemoveLiveDocument(ctx, observed), domain.ErrLiveVectorMutation)
	require.Equal(t, "deleting", documentStatus(t, c, doc.id), "the row stays for the coordinator")
	require.False(t, readReceipt(t, c, doc.id).finalized)
	require.NoError(t, c.DeleteWithCleanup(ctx, doc.id, newDeletionRecorder().ops(doc.incarnation)))
}
