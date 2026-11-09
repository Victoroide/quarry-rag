package worker

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/ocr"
	"github.com/Victoroide/quarry-rag/internal/pipeline"
	"github.com/stretchr/testify/require"
)

type ocrLeaseStore struct {
	*leaseTestStore
	content    string
	failure    string
	heartbeats int
	storeErr   error
}

func (s *ocrLeaseStore) StoreOCRText(_ context.Context, _ domain.Job, text string) error {
	s.content = text
	return s.storeErr
}
func (s *ocrLeaseStore) FailJob(_ context.Context, _ domain.Job, message string) error {
	s.failure = message
	s.events <- "failed"
	return nil
}
func (s *ocrLeaseStore) HeartbeatJob(ctx context.Context, j domain.Job, d time.Duration) (bool, error) {
	s.mu.Lock()
	s.heartbeats++
	s.mu.Unlock()
	return s.leaseTestStore.HeartbeatJob(ctx, j, d)
}

type ocrFunc func(context.Context, string) (string, error)

func (f ocrFunc) Extract(ctx context.Context, key string) (string, error) { return f(ctx, key) }

type ocrStage struct {
	run func(*pipeline.IngestContext) error
}

func (ocrStage) ID() pipeline.StageID                                      { return pipeline.StageChunk }
func (s ocrStage) Run(_ context.Context, ic *pipeline.IngestContext) error { return s.run(ic) }
func ocrJob() domain.Job {
	key := "synthetic-object"
	return domain.Job{Document: domain.Document{ID: "doc", TextSource: "ocr_pending", S3Key: &key}, Attempts: 1, LeaseOwner: "owner"}
}
func TestOCRProcessingPersistsBeforePipelineWhileHeartbeating(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := &ocrLeaseStore{leaseTestStore: leaseStore(0)}
		run := false
		w := New(s, []pipeline.Stage{ocrStage{func(ic *pipeline.IngestContext) error {
			run = true
			require.Equal(t, "Detected text", s.content)
			require.Equal(t, s.content, ic.RawText)
			return nil
		}}}, nil, Options{Lease: 30 * time.Millisecond, JobTimeout: time.Minute, OCR: ocrFunc(func(_ context.Context, key string) (string, error) {
			require.Equal(t, "synthetic-object", key)
			time.Sleep(100 * time.Millisecond)
			return "Detected text", nil
		})})
		w.process(t.Context(), ocrJob())
		require.True(t, run)
		require.Greater(t, s.heartbeats, 0)
		leaseEvent(t, s.leaseTestStore, "completed")
	})
}
func TestOCRFailureStopsPipeline(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		extractErr, storeErr error
		want                 string
	}{
		{"no text", domain.Permanent(ocr.ErrNoText), nil, "failed"},
		{"page limit", domain.Permanent(ocr.ErrPageLimit), nil, "failed"},
		{"temporary", errors.New("temporary"), nil, "retry"},
		{"lost lease", nil, domain.ErrJobNotOwned, "retry"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &ocrLeaseStore{leaseTestStore: leaseStore(0), storeErr: tc.storeErr}
			run := false
			w := New(s, []pipeline.Stage{ocrStage{func(*pipeline.IngestContext) error { run = true; return nil }}}, nil, Options{OCR: ocrFunc(func(context.Context, string) (string, error) { return "text", tc.extractErr })})
			w.process(t.Context(), ocrJob())
			require.False(t, run)
			leaseEvent(t, s.leaseTestStore, tc.want)
			if tc.name == "no text" {
				require.Equal(t, "no_text", s.failure)
			}
		})
	}
}
func TestOCRSavedTextSkipsDetectionOnRetry(t *testing.T) {
	s := &ocrLeaseStore{leaseTestStore: leaseStore(0)}
	j := ocrJob()
	j.Document.TextSource = "ocr"
	j.Document.Content = "already detected"
	run := false
	w := New(s, []pipeline.Stage{ocrStage{func(ic *pipeline.IngestContext) error {
		run = true
		require.Equal(t, j.Document.Content, ic.RawText)
		return nil
	}}}, nil, Options{OCR: ocrFunc(func(context.Context, string) (string, error) { t.Fatal("OCR repeated"); return "", nil })})
	w.process(t.Context(), j)
	require.True(t, run)
	leaseEvent(t, s.leaseTestStore, "completed")
}
