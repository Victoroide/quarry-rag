package worker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/ocr"
	"github.com/Victoroide/quarry-rag/internal/pipeline"
	"github.com/stretchr/testify/require"
)

type profileClaimCall struct {
	method         string
	vectorIdentity string
	mixed          bool
	layout         bool
}

type profileCapableStore struct {
	*pdfTestStore
	mu    sync.Mutex
	calls []profileClaimCall
}

func newProfileCapableStore(base *leaseTestStore) *profileCapableStore {
	return &profileCapableStore{pdfTestStore: &pdfTestStore{ocrLeaseStore: &ocrLeaseStore{leaseTestStore: base}}}
}

func (s *profileCapableStore) ClaimPDFProfilesForFixtureStore(_ context.Context, owner string, _ time.Duration, _ int, vectorIdentity string, mixed, layout bool) (*domain.Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, profileClaimCall{method: "profiles", vectorIdentity: vectorIdentity, mixed: mixed, layout: layout})
	for i, j := range s.jobs {
		isLayout := j.PDF != nil && j.PDF.Profile == domain.LayoutPDFProfile
		isMixed := j.PDF != nil && j.PDF.Profile == domain.MixedPDFProfile
		if (isLayout && !layout) || (isMixed && !mixed) {
			continue
		}
		j.LeaseOwner = owner
		s.jobs = append(s.jobs[:i], s.jobs[i+1:]...)
		return &j, nil
	}
	return nil, nil
}

func (s *profileCapableStore) ClaimPDFJobForFixtureStore(_ context.Context, owner string, _ time.Duration, _ int, vectorIdentity string) (*domain.Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, profileClaimCall{method: "pdf_fixture", vectorIdentity: vectorIdentity, mixed: true})
	return s.claimLocked(owner)
}

func (s *profileCapableStore) ClaimJobForFixtureStore(_ context.Context, owner string, _ time.Duration, _ int, vectorIdentity string) (*domain.Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, profileClaimCall{method: "fixture", vectorIdentity: vectorIdentity})
	return s.claimLocked(owner)
}

func (s *profileCapableStore) ClaimJob(_ context.Context, owner string, _ time.Duration, _ int) (*domain.Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, profileClaimCall{method: "claim"})
	return s.claimLocked(owner)
}

func (s *profileCapableStore) claimLocked(owner string) (*domain.Job, error) {
	if len(s.jobs) == 0 {
		return nil, nil
	}
	j := s.jobs[0]
	j.LeaseOwner = owner
	s.jobs = s.jobs[1:]
	return &j, nil
}

type legacyPDFFixtureStore struct {
	*leaseTestStore
	mu    sync.Mutex
	calls []profileClaimCall
}

func (s *legacyPDFFixtureStore) ClaimPDFJobForFixtureStore(ctx context.Context, owner string, lease time.Duration, maxAttempts int, vectorIdentity string) (*domain.Job, error) {
	s.mu.Lock()
	s.calls = append(s.calls, profileClaimCall{method: "pdf_fixture", vectorIdentity: vectorIdentity, mixed: true})
	s.mu.Unlock()
	return s.ClaimJob(ctx, owner, lease, maxAttempts)
}

func (s *legacyPDFFixtureStore) ClaimJobForFixtureStore(ctx context.Context, owner string, lease time.Duration, maxAttempts int, vectorIdentity string) (*domain.Job, error) {
	s.mu.Lock()
	s.calls = append(s.calls, profileClaimCall{method: "fixture", vectorIdentity: vectorIdentity})
	s.mu.Unlock()
	return s.ClaimJob(ctx, owner, lease, maxAttempts)
}

type legacyPDFOnlyStore struct {
	*leaseTestStore
	mu    sync.Mutex
	calls []profileClaimCall
}

func (s *legacyPDFOnlyStore) ClaimPDFJob(ctx context.Context, owner string, lease time.Duration, maxAttempts int) (*domain.Job, error) {
	s.mu.Lock()
	s.calls = append(s.calls, profileClaimCall{method: "pdf", mixed: true})
	s.mu.Unlock()
	return s.leaseTestStore.ClaimJob(ctx, owner, lease, maxAttempts)
}

func (s *legacyPDFOnlyStore) ClaimJob(ctx context.Context, owner string, lease time.Duration, maxAttempts int) (*domain.Job, error) {
	s.mu.Lock()
	s.calls = append(s.calls, profileClaimCall{method: "claim"})
	s.mu.Unlock()
	return s.leaseTestStore.ClaimJob(ctx, owner, lease, maxAttempts)
}

type analyzerOnlyOCR struct {
	fn func(context.Context, string, string, string, int, func(context.Context) error) (ocr.LayoutResults, error)
}

func (analyzerOnlyOCR) Extract(context.Context, string) (string, error) {
	return "", errors.New("legacy detection called")
}

func (a analyzerOnlyOCR) AnalyzePages(ctx context.Context, key, profile, window string, count int, accepted func(context.Context) error) (ocr.LayoutResults, error) {
	res, err := a.fn(ctx, key, profile, window, count, accepted)
	if err == nil && accepted != nil {
		err = accepted(ctx)
	}
	return res, err
}

func TestWorkerClaimsPDFProfilesFromOCRCapabilities(t *testing.T) {
	detector := pdfTestDetector(func(context.Context, string, string, int) (ocr.PageResults, error) {
		return ocr.PageResults{Pages: map[int]ocr.Page{1: {Present: true, Text: "Mixed OCR text"}}}, nil
	})
	analyzer := analyzerOnlyOCR{fn: func(context.Context, string, string, string, int, func(context.Context) error) (ocr.LayoutResults, error) {
		return ocr.LayoutResults{}, nil
	}}
	both := layoutTestAnalyzer{fn: func(context.Context, string, string, string, int, func(context.Context) error) (ocr.LayoutResults, error) {
		return ocr.LayoutResults{}, nil
	}}

	capabilities := []struct {
		name      string
		extractor TextExtractor
		mixed     bool
		layout    bool
	}{
		{name: "nil", extractor: nil, mixed: false, layout: false},
		{name: "text-only", extractor: ocrFunc(func(context.Context, string) (string, error) { return "", nil }), mixed: false, layout: false},
		{name: "detector-only", extractor: detector, mixed: true, layout: false},
		{name: "analyzer-only", extractor: analyzer, mixed: false, layout: true},
		{name: "both", extractor: both, mixed: true, layout: true},
	}

	for _, tc := range capabilities {
		t.Run("profile-store/"+tc.name, func(t *testing.T) {
			s := newProfileCapableStore(leaseStore(0))
			w := New(s, nil, nil, Options{Owner: "w", FixtureVectorIdentity: "postgres", OCR: tc.extractor})
			job, err := w.claim(t.Context())
			require.NoError(t, err)
			require.Nil(t, job)
			require.Equal(t, []profileClaimCall{{
				method:         "profiles",
				vectorIdentity: "postgres",
				mixed:          tc.mixed,
				layout:         tc.layout,
			}}, s.calls)
		})

		t.Run("legacy-fixture-store/"+tc.name, func(t *testing.T) {
			s := &legacyPDFFixtureStore{leaseTestStore: leaseStore(0)}
			w := New(s, nil, nil, Options{Owner: "w", FixtureVectorIdentity: "postgres", OCR: tc.extractor})
			job, err := w.claim(t.Context())
			require.NoError(t, err)
			require.Nil(t, job)
			wantMethod := "fixture"
			if tc.mixed {
				wantMethod = "pdf_fixture"
			}
			require.Equal(t, []profileClaimCall{{
				method:         wantMethod,
				vectorIdentity: "postgres",
				mixed:          tc.mixed,
			}}, s.calls)
		})

		t.Run("legacy-pdf-only-store/"+tc.name, func(t *testing.T) {
			s := &legacyPDFOnlyStore{leaseTestStore: leaseStore(0)}
			w := New(s, nil, nil, Options{Owner: "w", OCR: tc.extractor})
			job, err := w.claim(t.Context())
			require.NoError(t, err)
			require.Nil(t, job)
			wantMethod := "claim"
			if tc.mixed {
				wantMethod = "pdf"
			}
			require.Equal(t, []profileClaimCall{{
				method: wantMethod,
				mixed:  tc.mixed,
			}}, s.calls)
		})

		t.Run("basic-job-store/"+tc.name, func(t *testing.T) {
			s := leaseStore(0)
			w := New(s, nil, nil, Options{Owner: "w", OCR: tc.extractor})
			job, err := w.claim(t.Context())
			require.NoError(t, err)
			require.Nil(t, job)
		})
	}

	t.Run("mixed consumer never touches layout job or calls layout provider", func(t *testing.T) {
		key := "objects/doc.pdf"
		layoutJob := domain.Job{
			Document: domain.Document{ID: "layout-doc", TextSource: "parser", S3Key: &key},
			Attempts: 1,
			PDF: &domain.PDFExtraction{
				Profile:  domain.LayoutPDFProfile,
				Coverage: domain.PDFPending,
				Pages:    domain.PDFPages{{Number: 1, Evidence: domain.PDFEmbedded, Text: "Embedded"}},
			},
		}
		mixedJob := domain.Job{
			Document: domain.Document{ID: "mixed-doc", TextSource: "parser", S3Key: &key},
			Attempts: 1,
			PDF: &domain.PDFExtraction{
				Profile:  domain.MixedPDFProfile,
				Coverage: domain.PDFPending,
				Pages:    domain.PDFPages{{Number: 1, Evidence: domain.PDFImage}},
			},
		}
		s := newProfileCapableStore(leaseStore(0))
		s.jobs = []domain.Job{layoutJob, mixedJob}
		var processed []domain.DocumentID
		w := New(s, []pipeline.Stage{ocrStage{func(ic *pipeline.IngestContext) error {
			processed = append(processed, ic.DocumentID)
			return nil
		}}}, nil, Options{
			Workers: 1, MaxAttempts: 5, Lease: time.Second, JobTimeout: time.Second, PollInterval: 5 * time.Millisecond,
			Owner: "mixed-only",
			OCR: pdfTestDetector(func(context.Context, string, string, int) (ocr.PageResults, error) {
				return ocr.PageResults{Pages: map[int]ocr.Page{1: {Present: true, Text: "Mixed scanned text"}}}, nil
			}),
		})
		w.Start(t.Context())
		leaseEvent(t, s.leaseTestStore, "completed")
		w.Shutdown()
		require.Equal(t, []domain.DocumentID{"mixed-doc"}, processed)
		require.Len(t, s.jobs, 1)
		require.Equal(t, domain.DocumentID("layout-doc"), s.jobs[0].Document.ID)
		require.Empty(t, s.failure)
	})

	t.Run("analyzer retries provider, releases on cancel, and resumes checkpoint", func(t *testing.T) {
		key := "objects/layout.pdf"
		baseJob := domain.Job{
			Document: domain.Document{ID: "layout-doc", TextSource: "parser", S3Key: &key},
			Attempts: 1,
			PDF: &domain.PDFExtraction{
				Profile:  domain.LayoutPDFProfile,
				Coverage: domain.PDFPending,
				Pages:    domain.PDFPages{{Number: 1, Evidence: domain.PDFEmbedded, Text: "Embedded"}},
			},
		}
		s := &pdfTestStore{ocrLeaseStore: &ocrLeaseStore{leaseTestStore: leaseStore(0)}}
		providerCalls := 0
		var stageSections []domain.LayoutSection
		wk := New(s, []pipeline.Stage{ocrStage{func(ic *pipeline.IngestContext) error {
			stageSections = ic.LayoutSections
			return nil
		}}}, nil, Options{
			Owner: "analyzer", MaxAttempts: 5, Lease: time.Second, JobTimeout: time.Second,
			OCR: analyzerOnlyOCR{fn: func(ctx context.Context, _ string, _ string, _ string, _ int, _ func(context.Context) error) (ocr.LayoutResults, error) {
				providerCalls++
				if providerCalls == 1 {
					return ocr.LayoutResults{}, errors.New("transient textract error")
				}
				if providerCalls == 2 {
					<-ctx.Done()
					return ocr.LayoutResults{}, ctx.Err()
				}
				return ocr.LayoutResults{Pages: map[int]ocr.LayoutPage{
					1: {Present: true, Text: "Header: Value", Sections: []domain.LayoutSection{{Page: 1, TableID: "t1", RowID: "r1", Text: "Header: Value"}}},
				}}, nil
			}},
		})

		// 1. Transient provider failure triggers retry without running pipeline.
		wk.process(t.Context(), baseJob)
		leaseEvent(t, s.leaseTestStore, "retry")
		require.Nil(t, s.saved)

		// 2. Context cancellation releases lease without consuming attempt.
		cancelCtx, cancel := context.WithCancel(t.Context())
		go func() {
			time.Sleep(10 * time.Millisecond)
			cancel()
		}()
		wk.process(cancelCtx, baseJob)
		leaseEvent(t, s.leaseTestStore, "released")
		require.Nil(t, s.saved)

		// 3. Subsequent attempt checkpoints and completes.
		wk.process(t.Context(), baseJob)
		leaseEvent(t, s.leaseTestStore, "completed")
		require.NotNil(t, s.saved)
		require.Equal(t, []domain.LayoutSection{{Page: 1, TableID: "t1", RowID: "r1", Text: "Header: Value"}}, stageSections)

		// 4. Resume from committed layout checkpoint makes zero provider calls and carries sections.
		resumed := baseJob
		resumed.Document.Content = s.saved.Text
		resumed.PDF = &domain.PDFExtraction{
			Profile:     domain.LayoutPDFProfile,
			Coverage:    s.saved.Coverage,
			PageResults: s.saved.Pages,
		}
		stageSections = nil
		wk.process(t.Context(), resumed)
		leaseEvent(t, s.leaseTestStore, "completed")
		require.Equal(t, 3, providerCalls)
		require.Equal(t, []domain.LayoutSection{{Page: 1, TableID: "t1", RowID: "r1", Text: "Header: Value"}}, stageSections)
	})
}
