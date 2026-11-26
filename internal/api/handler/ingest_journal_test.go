package handler

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/extractor"
	"github.com/stretchr/testify/require"
)

// journalTrace records the order of durable and object-store effects.
type journalTrace struct {
	mu     sync.Mutex
	events []string
}

func (j *journalTrace) add(event string) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.events = append(j.events, event)
}

func (j *journalTrace) names() []string {
	j.mu.Lock()
	defer j.mu.Unlock()
	return append([]string(nil), j.events...)
}

type fakeJournal struct {
	trace    *journalTrace
	staged   []domain.SourceUpload
	acked    map[string]string
	deadline time.Time
	delay    time.Duration
	stageErr error
	beginErr error
	ackErr   error
}

func (j *fakeJournal) StageSourceUpload(ctx context.Context, upload domain.SourceUpload) error {
	j.trace.add("stage")
	if j.stageErr != nil {
		return j.stageErr
	}
	j.staged = append(j.staged, upload)
	j.deadline, _ = ctx.Deadline()
	time.Sleep(j.delay)
	return nil
}

func (j *fakeJournal) BeginSourceUpload(context.Context, string) error {
	j.trace.add("begin")
	return j.beginErr
}

func (j *fakeJournal) RecordSourceUploadResult(_ context.Context, id string, result domain.SourceUploadResult) error {
	j.trace.add("ack")
	if j.ackErr != nil {
		return j.ackErr
	}
	if j.acked == nil {
		j.acked = map[string]string{}
	}
	j.acked[id] = result.VersionID
	return nil
}

type journaledObjects struct {
	trace       *journalTrace
	bucket      string
	version     string
	putErr      error
	putDeadline time.Time
	written     map[string][]byte
}

func (o *journaledObjects) Upload(context.Context, string, string, string, extractor.FileType, io.Reader) (string, error) {
	o.trace.add("legacy-upload")
	return "legacy-key", nil
}
func (o *journaledObjects) Delete(context.Context, string) error {
	o.trace.add("legacy-delete")
	return nil
}
func (o *journaledObjects) Bucket() string { return o.bucket }
func (o *journaledObjects) NewKey(documentID, uploadID, docType, fileName string) string {
	return "documents/" + docType + "/" + documentID + "/" + uploadID + "/" + fileName
}
func (*journaledObjects) NewOwnedKey(documentID, incarnation, uploadID, docType, fileName string) string {
	return "documents/" + docType + "/" + documentID + "/" + incarnation + "/" + uploadID + "/" + fileName
}

func (o *journaledObjects) PutSource(ctx context.Context, key string, _ extractor.FileType, r io.Reader) (domain.SourceUploadResult, error) {
	o.trace.add("put")
	o.putDeadline, _ = ctx.Deadline()
	data, err := io.ReadAll(r)
	if err != nil {
		return domain.SourceUploadResult{Settled: true}, err
	}
	if o.written == nil {
		o.written = map[string][]byte{}
	}
	o.written[key] = data
	if o.putErr != nil {
		return domain.SourceUploadResult{}, o.putErr
	}
	return domain.SourceUploadResult{VersionID: o.version, Acknowledged: true, Settled: true}, nil
}

type journalStore struct {
	validationAcceptStore
	trace     *journalTrace
	outcome   domain.AcceptOutcome
	acceptErr error
}

func (s *journalStore) AcceptDocument(ctx context.Context, req domain.AcceptRequest) (domain.AcceptResult, error) {
	s.trace.add("accept")
	_, _ = s.validationAcceptStore.AcceptDocument(ctx, req)
	if s.acceptErr != nil {
		return domain.AcceptResult{}, s.acceptErr
	}
	return domain.AcceptResult{DocumentID: req.Document.ID, Outcome: s.outcome}, nil
}

func (s *journalStore) GetDocumentS3Key(context.Context, domain.DocumentID) (*string, error) {
	s.trace.add("lookup")
	return nil, nil
}

func (s *journalStore) CheckFingerprint(context.Context, string) (domain.DocumentID, bool, error) {
	return "", false, nil
}

type journalRig struct {
	trace   *journalTrace
	journal *fakeJournal
	objects *journaledObjects
	store   *journalStore
}

func newJournalRig() *journalRig {
	trace := &journalTrace{}
	return &journalRig{
		trace:   trace,
		journal: &fakeJournal{trace: trace},
		objects: &journaledObjects{trace: trace, bucket: "originals", version: "v1"},
		store:   &journalStore{trace: trace, outcome: domain.AcceptAccepted},
	}
}

func (r *journalRig) post(t *testing.T, opts ...IngestOption) *httptest.ResponseRecorder {
	t.Helper()
	options := append([]IngestOption{WithSourceUploadJournal(r.journal)}, opts...)
	rec := httptest.NewRecorder()
	IngestHandler(nil, r.store, r.objects, 1<<20, options...)(rec, ocrUpload(t, "scan.txt", "archive", []byte("journaled original")))
	return rec
}

func TestJournalStagesOwnershipBeforeTheObjectWrite(t *testing.T) {
	rig := newJournalRig()
	rec := rig.post(t)
	require.Equal(t, http.StatusAccepted, rec.Code, rec.Body.String())
	require.Equal(t, []string{"stage", "begin", "put", "ack", "accept"}, rig.trace.names())

	require.Len(t, rig.journal.staged, 1)
	staged := rig.journal.staged[0]
	require.Len(t, rig.store.accepted, 1)
	accepted := rig.store.accepted[0]
	require.Equal(t, "originals", staged.Bucket)
	require.Equal(t, "archive", staged.Collection)
	require.Equal(t, accepted.Document.ID, staged.DocumentID)
	require.Equal(t, *accepted.Document.S3Key, staged.Key)
	require.Contains(t, staged.Key, "/"+staged.ID+"/", "the key carries the upload incarnation")
	require.Equal(t, &domain.SourceUploadLink{ID: staged.ID, VersionID: "v1"}, accepted.SourceUpload)
	require.Equal(t, map[string]string{staged.ID: "v1"}, rig.journal.acked)
	require.Equal(t, []byte("journaled original"), rig.objects.written[staged.Key])
}

func TestJournalUsesDistinctKeysForRepeatedUploads(t *testing.T) {
	rig := newJournalRig()
	require.Equal(t, http.StatusAccepted, rig.post(t).Code)
	require.Equal(t, http.StatusAccepted, rig.post(t).Code)
	require.Len(t, rig.journal.staged, 2)
	require.NotEqual(t, rig.journal.staged[0].Key, rig.journal.staged[1].Key)
	require.NotEqual(t, rig.journal.staged[0].ID, rig.journal.staged[1].ID)
}

func TestJournalFailureToStageWritesNothing(t *testing.T) {
	rig := newJournalRig()
	rig.journal.stageErr = errors.New("database unavailable")
	rec := rig.post(t)
	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
	require.Equal(t, "30", rec.Header().Get("Retry-After"))
	require.JSONEq(t, `{"error":"storage","message":"source file storage is unavailable"}`, rec.Body.String())
	require.Equal(t, []string{"stage"}, rig.trace.names())
	require.Empty(t, rig.objects.written)
	require.Empty(t, rig.store.accepted)
}

func TestJournalKeepsTheRecordWhenTheWriteOutcomeIsUnknown(t *testing.T) {
	rig := newJournalRig()
	rig.objects.putErr = context.DeadlineExceeded
	rec := rig.post(t)
	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
	require.Equal(t, []string{"stage", "begin", "put"}, rig.trace.names())
	require.Len(t, rig.journal.staged, 1, "the object may exist, so reclamation owns the record")
	require.Empty(t, rig.store.accepted)
}

func TestJournalRefusesAPutWithoutADurableDispatch(t *testing.T) {
	rig := newJournalRig()
	rig.journal.beginErr = errors.New("dispatch commit status unknown")
	require.Equal(t, http.StatusServiceUnavailable, rig.post(t).Code)
	require.Equal(t, []string{"stage", "begin"}, rig.trace.names())
	require.Empty(t, rig.objects.written)
}

func TestJournalBoundsTheObjectWrite(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rig := newJournalRig()
		rig.journal.delay = time.Minute
		start := time.Now()
		require.Equal(t, http.StatusAccepted, rig.post(t).Code)
		require.Equal(t, start.Add(domain.SourceUploadPutTimeout), rig.journal.deadline)
		require.Equal(t, rig.journal.deadline, rig.objects.putDeadline)
		require.Less(t, domain.SourceUploadPutTimeout, domain.SourceUploadLease)
	})
}

func TestJournalDoesNotStartAPutAfterStagingDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rig := newJournalRig()
		rig.journal.delay = domain.SourceUploadLease
		rec := rig.post(t)
		require.Equal(t, http.StatusServiceUnavailable, rec.Code)
		require.Equal(t, "30", rec.Header().Get("Retry-After"))
		require.Equal(t, []string{"stage"}, rig.trace.names())
		require.Empty(t, rig.objects.written)
	})
}

func TestJournalToleratesAFailedAcknowledgement(t *testing.T) {
	rig := newJournalRig()
	rig.journal.ackErr = errors.New("connection reset")
	rec := rig.post(t)
	require.Equal(t, http.StatusAccepted, rec.Code)
	require.Equal(t, "v1", rig.store.accepted[0].SourceUpload.VersionID, "acceptance still records the version")
}

func TestJournalLeavesUnadoptedUploadsForReconciliation(t *testing.T) {
	for _, outcome := range []domain.AcceptOutcome{domain.AcceptDuplicate, domain.AcceptInProgress, domain.AcceptConflict, domain.AcceptQueueFull, domain.AcceptUploadExpired} {
		rig := newJournalRig()
		rig.store.outcome = outcome
		rig.post(t)
		require.Equal(t, []string{"stage", "begin", "put", "ack", "accept"}, rig.trace.names(), "outcome %d", outcome)
	}
}

func TestJournalRetainsTheObjectWhenAcceptanceIsUncertain(t *testing.T) {
	rig := newJournalRig()
	rig.store.acceptErr = errors.New("commit status unknown")
	require.Equal(t, http.StatusServiceUnavailable, rig.post(t).Code)
	require.Equal(t, []string{"stage", "begin", "put", "ack", "accept"}, rig.trace.names())
}

type failingResponse struct{ *httptest.ResponseRecorder }

func (failingResponse) Write([]byte) (int, error) { return 0, errors.New("client disconnected") }

func TestJournalPreservesAnAcceptedObjectWhenTheResponseIsLost(t *testing.T) {
	rig := newJournalRig()
	rec := failingResponse{httptest.NewRecorder()}
	IngestHandler(nil, rig.store, rig.objects, 1<<20, WithSourceUploadJournal(rig.journal))(rec, ocrUpload(t, "scan.txt", "archive", []byte("journaled original")))
	require.Equal(t, []string{"stage", "begin", "put", "ack", "accept"}, rig.trace.names())
}

func TestJournalAnswersAnExpiredUploadWithARetry(t *testing.T) {
	rig := newJournalRig()
	rig.store.outcome = domain.AcceptUploadExpired
	rec := rig.post(t)
	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
	require.Equal(t, "30", rec.Header().Get("Retry-After"))
	require.JSONEq(t, `{"error":"storage","message":"source file upload expired before acceptance; retry the upload"}`, rec.Body.String())
}

func TestJournalIsOffByDefault(t *testing.T) {
	rig := newJournalRig()
	rec := httptest.NewRecorder()
	IngestHandler(nil, rig.store, rig.objects, 1<<20)(rec, ocrUpload(t, "scan.txt", "archive", []byte("plain original")))
	require.Equal(t, http.StatusAccepted, rec.Code)
	require.Equal(t, []string{"legacy-upload", "accept"}, rig.trace.names())
	require.Nil(t, rig.store.accepted[0].SourceUpload)
}

type plainObjects struct{ uploaded bool }

func (o *plainObjects) Upload(context.Context, string, string, string, extractor.FileType, io.Reader) (string, error) {
	o.uploaded = true
	return "key", nil
}
func (*plainObjects) Delete(context.Context, string) error { return nil }

func TestJournalFailsClosedWithoutAJournalingStore(t *testing.T) {
	trace := &journalTrace{}
	journal := &fakeJournal{trace: trace}
	objects := &plainObjects{}
	store := &journalStore{trace: trace}
	rec := httptest.NewRecorder()
	IngestHandler(nil, store, objects, 1<<20, WithSourceUploadJournal(journal))(rec, ocrUpload(t, "scan.txt", "archive", []byte("journaled original")))
	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
	require.False(t, objects.uploaded)
	require.Empty(t, trace.names())
}

func TestJournalIgnoresUploadsWithoutObjectStorage(t *testing.T) {
	rig := newJournalRig()
	rec := httptest.NewRecorder()
	body := new(bytes.Buffer)
	body.WriteString(`{"content":"json content"}`)
	r := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/ingest", strings.NewReader(body.String()))
	r.Header.Set("Content-Type", "application/json")
	IngestHandler(nil, rig.store, rig.objects, 1<<20, WithSourceUploadJournal(rig.journal))(rec, r)
	require.Equal(t, http.StatusAccepted, rec.Code)
	require.Equal(t, []string{"accept"}, rig.trace.names())
}
