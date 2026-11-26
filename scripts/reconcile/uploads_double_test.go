package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/api/handler"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/extractor"
	pgstore "github.com/Victoroide/quarry-rag/internal/store/postgres"
	s3store "github.com/Victoroide/quarry-rag/internal/store/s3"
	"github.com/Victoroide/quarry-rag/internal/testutil/pgtest"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/require"
)

var errCrash = errors.New("process stopped")

type putMode int

const (
	putStores            putMode = iota // the object is stored and acknowledged
	putStopsBefore                      // the process stops before the request reaches the bucket
	putStoresButFail                    // the object is stored but the caller sees a failure
	putLandsLater                       // the caller sees a timeout and the object lands afterwards
	putRetryAcknowledged                // a retry succeeds while an older PUT can still land
	putRefused                          // the bucket refused every attempt and stored nothing
)

type storedVersion struct {
	id     string
	data   []byte
	marker bool
}

// bucketDouble is an in-memory object store with optional versioning, holds and fault injection.
type bucketDouble struct {
	mu         sync.Mutex
	name       string
	versioned  bool
	next       int
	objects    map[string][]storedVersion
	landing    map[string][]byte
	holds      map[string]bool
	retention  map[string]time.Time
	putMode    func(key string) putMode
	failDelete func(key, version string) error
	failList   error
	failLock   error
	writes     int
	deletes    int
	plain      int
	reads      int
	removed    []string
}

func newBucket(name string, versioned bool) *bucketDouble {
	return &bucketDouble{name: name, versioned: versioned, objects: map[string][]storedVersion{}, landing: map[string][]byte{}, holds: map[string]bool{}, retention: map[string]time.Time{}}
}

func (b *bucketDouble) Bucket() string { return b.name }
func (b *bucketDouble) NewKey(documentID, uploadID, docType, fileName string) string {
	return "documents/" + docType + "/" + documentID + "/" + uploadID + "/" + fileName
}

func (b *bucketDouble) Upload(context.Context, string, string, string, extractor.FileType, io.Reader) (string, error) {
	return "", errors.New("unjournaled uploads are not expected")
}
func (b *bucketDouble) Delete(ctx context.Context, key string) error {
	return b.DeleteVersion(ctx, key, "")
}

func (b *bucketDouble) storeLocked(key string, data []byte) string {
	b.writes++
	id := "null"
	if b.versioned {
		b.next++
		id = fmt.Sprintf("v%d", b.next)
		b.objects[key] = append(b.objects[key], storedVersion{id: id, data: data})
		return id
	}
	b.objects[key] = []storedVersion{{id: id, data: data}}
	return ""
}

func (b *bucketDouble) PutSource(_ context.Context, key string, _ extractor.FileType, r io.Reader) (domain.SourceUploadResult, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return domain.SourceUploadResult{Settled: true}, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	mode := putStores
	if b.putMode != nil {
		mode = b.putMode(key)
	}
	switch mode {
	case putStopsBefore:
		return domain.SourceUploadResult{}, errCrash
	case putRefused:
		return domain.SourceUploadResult{Settled: true}, &smithy.GenericAPIError{Code: "AccessDenied", Message: "refused"}
	case putLandsLater:
		b.landing[key] = data
		return domain.SourceUploadResult{}, context.DeadlineExceeded
	case putStoresButFail:
		b.storeLocked(key, data)
		return domain.SourceUploadResult{}, context.DeadlineExceeded
	case putRetryAcknowledged:
		b.landing[key] = data
	}
	return domain.SourceUploadResult{
		VersionID: b.storeLocked(key, data), Acknowledged: true, Settled: mode == putStores,
	}, nil
}

// land completes a write that the caller already gave up on.
func (b *bucketDouble) land(key string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if data, ok := b.landing[key]; ok {
		delete(b.landing, key)
		b.storeLocked(key, data)
	}
}

// addVersion stores an extra version, as a retried write would in a versioned bucket.
func (b *bucketDouble) addVersion(key string, data []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.storeLocked(key, data)
}

func (b *bucketDouble) DeleteVersion(_ context.Context, key, version string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.failDelete != nil {
		if err := b.failDelete(key, version); err != nil {
			return err
		}
	}
	if version == "" {
		b.deletes++
		b.plain++
		if b.versioned {
			b.next++
			b.objects[key] = append(b.objects[key], storedVersion{id: fmt.Sprintf("v%d", b.next), marker: true})
		} else {
			delete(b.objects, key)
		}
		return nil
	}
	if b.holds[key+"@"+version] || b.retention[key+"@"+version].After(time.Now()) {
		return &smithy.GenericAPIError{Code: "AccessDenied", Message: "object is protected"}
	}
	b.deletes++
	b.removed = append(b.removed, key+"@"+version)
	b.objects[key] = slices.DeleteFunc(b.objects[key], func(v storedVersion) bool { return v.id == version })
	if len(b.objects[key]) == 0 {
		delete(b.objects, key)
	}
	return nil
}

func (b *bucketDouble) ListVersions(_ context.Context, key string, limit int) ([]s3store.ObjectVersion, bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.reads++
	if b.failList != nil {
		return nil, false, b.failList
	}
	var out []s3store.ObjectVersion
	for _, v := range slices.Backward(b.objects[key]) {
		if len(out) == limit {
			return out, false, nil
		}
		out = append(out, s3store.ObjectVersion{ID: v.id, Size: int64(len(v.data)), DeleteMarker: v.marker})
	}
	return out, true, nil
}

func (b *bucketDouble) VersionLock(_ context.Context, key, version string) (s3store.ObjectLock, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.reads++
	if b.failLock != nil {
		return s3store.ObjectLock{}, b.failLock
	}
	for _, v := range b.objects[key] {
		if v.id == version {
			return s3store.ObjectLock{LegalHold: b.holds[key+"@"+version], RetainUntil: b.retention[key+"@"+version]}, nil
		}
	}
	return s3store.ObjectLock{}, s3store.ErrVersionNotFound
}

func (b *bucketDouble) landAll() {
	b.mu.Lock()
	keys := make([]string, 0, len(b.landing))
	for key := range b.landing {
		keys = append(keys, key)
	}
	b.mu.Unlock()
	for _, key := range keys {
		b.land(key)
	}
}

func (b *bucketDouble) versionCount(key string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.objects[key])
}

func (b *bucketDouble) keys() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	keys := make([]string, 0, len(b.objects))
	for key := range b.objects {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}

func (b *bucketDouble) put(key string, data []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.storeLocked(key, data)
}

func (b *bucketDouble) removedCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.removed)
}

func (b *bucketDouble) mutations() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.writes + b.deletes
}

// faultStore adds failure points around the document store the handler uses.
type faultStore struct {
	*pgstore.Client
	mu          sync.Mutex
	staged      func(domain.SourceUpload)
	stopBefore  func(domain.AcceptRequest) bool
	commitThen  error
	acknowledge error
}

func (s *faultStore) StageSourceUpload(ctx context.Context, upload domain.SourceUpload) error {
	if err := s.Client.StageSourceUpload(ctx, upload); err != nil {
		return err
	}
	if s.staged != nil {
		s.staged(upload)
	}
	return nil
}

func (s *faultStore) AcceptDocument(ctx context.Context, req domain.AcceptRequest) (domain.AcceptResult, error) {
	s.mu.Lock()
	stop, after := s.stopBefore, s.commitThen
	s.mu.Unlock()
	if stop != nil && stop(req) {
		return domain.AcceptResult{}, errCrash
	}
	result, err := s.Client.AcceptDocument(ctx, req)
	if err == nil && after != nil && result.Outcome == domain.AcceptAccepted {
		return domain.AcceptResult{}, after
	}
	return result, err
}

func (s *faultStore) RecordSourceUploadResult(ctx context.Context, id string, result domain.SourceUploadResult) error {
	if s.acknowledge != nil {
		return s.acknowledge
	}
	return s.Client.RecordSourceUploadResult(ctx, id, result)
}

type rig struct {
	t      *testing.T
	pg     *pgstore.Client
	bucket *bucketDouble
	store  *faultStore
}

func newDatabase(t *testing.T) *pgstore.Client {
	t.Helper()
	pg, err := pgstore.NewClient(t.Context(), pgtest.Start(t))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, pg.Close(context.WithoutCancel(t.Context()))) })
	require.NoError(t, pg.RunMigrations(t.Context()))
	return pg
}

func newRig(t *testing.T, pg *pgstore.Client, bucket string, versioned bool) *rig {
	t.Helper()
	return &rig{t: t, pg: pg, bucket: newBucket(bucket, versioned), store: &faultStore{Client: pg}}
}

func (r *rig) upload(collection, content string, fields ...string) *httptest.ResponseRecorder {
	r.t.Helper()
	return r.uploadWithContext(r.t.Context(), collection, content, fields...)
}

func (r *rig) uploadWithContext(ctx context.Context, collection, content string, fields ...string) *httptest.ResponseRecorder {
	r.t.Helper()
	var body bytes.Buffer
	w := multipart.NewWriter(&body)
	part, err := w.CreateFormFile("file", "note.txt")
	require.NoError(r.t, err)
	_, err = part.Write([]byte(content))
	require.NoError(r.t, err)
	require.NoError(r.t, w.WriteField("collection", collection))
	for i := 0; i+1 < len(fields); i += 2 {
		require.NoError(r.t, w.WriteField(fields[i], fields[i+1]))
	}
	require.NoError(r.t, w.Close())
	req := httptest.NewRequestWithContext(ctx, http.MethodPost, "/ingest", &body)
	req.Header.Set("Content-Type", w.FormDataContentType())
	rec := httptest.NewRecorder()
	handler.IngestHandler(nil, r.store, r.bucket, 1<<20, handler.WithSourceUploadJournal(r.store))(rec, req)
	return rec
}

// expire moves every staged upload of the bucket past its lease and minimum age.
func (r *rig) expire() {
	r.t.Helper()
	_, err := r.pg.Pool.Exec(r.t.Context(), `UPDATE source_uploads SET created_at=NOW()-interval '3 hours',expires_at=NOW()-interval '2 hours' WHERE bucket=$1 AND state='staged'`, r.bucket.name)
	require.NoError(r.t, err)
}

func (r *rig) states() map[string]int {
	r.t.Helper()
	rows, err := r.pg.Pool.Query(r.t.Context(), `SELECT state,count(*) FROM source_uploads WHERE bucket=$1 GROUP BY 1`, r.bucket.name)
	require.NoError(r.t, err)
	defer rows.Close()
	counts := map[string]int{}
	for rows.Next() {
		var state string
		var n int
		require.NoError(r.t, rows.Scan(&state, &n))
		counts[state] = n
	}
	require.NoError(r.t, rows.Err())
	return counts
}

func (r *rig) reconcile(opts options) uploadReport {
	r.t.Helper()
	report, err := reconcileSourceUploads(r.t.Context(), r.pg, r.bucket, r.bucket.name, opts)
	require.NoError(r.t, err)
	return report
}

// quiesce applies cleanup until no unlinked upload remains pending.
func (r *rig) quiesce(opts options) uploadReport {
	r.t.Helper()
	var report uploadReport
	for range 8 {
		report = r.reconcile(opts)
		if report.Pending == 0 && r.states()["claimed"] == 0 && r.states()["staged"] == 0 {
			return report
		}
	}
	r.t.Fatalf("cleanup did not settle: %v, report %+v", r.states(), report)
	return report
}

func applyOptions() options {
	return options{SourceUploads: true, Apply: true, Limit: 1000, Batch: 100, MaxBytes: 1 << 30}
}

func inventoryOptions() options {
	opts := applyOptions()
	opts.Apply = false
	return opts
}

// loseRace makes the next upload lose to a concurrent identical upload that
// reaches acceptance first, leaving the losing original for reconciliation.
func (r *rig) loseRace(collection, content string) {
	r.t.Helper()
	var mu sync.Mutex
	raced := false
	r.store.stopBefore = func(domain.AcceptRequest) bool {
		mu.Lock()
		first := !raced
		raced = true
		mu.Unlock()
		if first {
			require.Equal(r.t, http.StatusAccepted, r.upload(collection, content).Code)
		}
		return false
	}
}
