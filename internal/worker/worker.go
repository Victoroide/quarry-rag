// Package worker processes durable ingestion jobs under renewable leases.
package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/metrics"
	"github.com/Victoroide/quarry-rag/internal/ocr"
	"github.com/Victoroide/quarry-rag/internal/pipeline"
	"github.com/Victoroide/quarry-rag/internal/tracing"
	"github.com/aws/smithy-go"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Options bounds parallel processing, retries, and lease lifetimes.
type Options struct {
	FixtureVectorIdentity           string
	HostedVectorManifest            bool
	Workers, MaxAttempts            int
	Lease, JobTimeout, PollInterval time.Duration
	Owner                           string
	Metrics                         *metrics.Service
	OCR                             TextExtractor
	RedactTelemetry                 bool
}

// TextExtractor detects text in an original object stored in S3.
type TextExtractor interface {
	Extract(context.Context, string) (string, error)
}

var errLeaseLost = errors.New("ingestion lease lost")
var errShutdown = errors.New("ingestion shutdown")

// Worker claims and processes persisted ingestion jobs.
type Worker struct {
	store            domain.JobStore
	stages           []pipeline.Stage
	vectors          domain.VectorDeleter
	opts             Options
	wake, stop       chan struct{}
	wg               sync.WaitGroup
	mu               sync.Mutex
	cancel           context.CancelCauseFunc
	started, stopped bool
}

// New constructs a durable ingestion processor.
func New(store domain.JobStore, stages []pipeline.Stage, vectors domain.VectorDeleter, opts Options) *Worker {
	if _, transactional := vectors.(domain.TransactionalVectorStore); transactional {
		vectors = nil
	}
	if opts.Metrics == nil {
		opts.Metrics = metrics.Default
	}
	if opts.Workers < 1 {
		opts.Workers = 2
	}
	if opts.MaxAttempts < 1 {
		opts.MaxAttempts = 5
	}
	if opts.Lease <= 0 {
		opts.Lease = 2 * time.Minute
	}
	if opts.JobTimeout <= 0 {
		opts.JobTimeout = 15 * time.Minute
	}
	if opts.PollInterval <= 0 {
		opts.PollInterval = time.Second
	}
	return &Worker{store: store, stages: stages, vectors: vectors, opts: opts, wake: make(chan struct{}, 1), stop: make(chan struct{})}
}

// Notify wakes a claim loop; polling also discovers work accepted by other replicas.
func (w *Worker) Notify() {
	select {
	case w.wake <- struct{}{}:
	default:
	}
}

// Start launches the configured number of claim loops once.
func (w *Worker) Start(ctx context.Context) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.started || w.stopped {
		return
	}
	w.started = true
	jobCtx, cancel := context.WithCancelCause(ctx)
	w.cancel = cancel
	for range w.opts.Workers {
		w.wg.Go(func() { w.loop(jobCtx) })
	}
}
func (w *Worker) loop(ctx context.Context) {
	ticker := time.NewTicker(w.opts.PollInterval)
	defer ticker.Stop()
	for {
		for {
			select {
			case <-w.stop:
				return
			case <-ctx.Done():
				return
			default:
			}
			job, err := w.claim(ctx)
			if err != nil {
				slog.ErrorContext(ctx, "claim ingestion job", "error", err)
				break
			}
			if job == nil {
				break
			}
			w.process(ctx, *job)
		}
		select {
		case <-w.stop:
			return
		case <-ctx.Done():
			return
		case <-w.wake:
		case <-ticker.C:
		}
	}
}

func (w *Worker) claim(ctx context.Context) (*domain.Job, error) {
	_, hasDetector := w.opts.OCR.(pdfDetector)
	_, hasAnalyzer := w.opts.OCR.(pdfAnalyzer)
	if hasDetector || hasAnalyzer {
		if store, ok := w.store.(interface {
			ClaimPDFJobForFixtureStore(context.Context, string, time.Duration, int, string) (*domain.Job, error)
		}); ok {
			return store.ClaimPDFJobForFixtureStore(ctx, w.opts.Owner, w.opts.Lease, w.opts.MaxAttempts, w.opts.FixtureVectorIdentity)
		}
	}
	if store, ok := w.store.(domain.FixtureJobStore); ok {
		return store.ClaimJobForFixtureStore(ctx, w.opts.Owner, w.opts.Lease, w.opts.MaxAttempts, w.opts.FixtureVectorIdentity)
	}
	return w.store.ClaimJob(ctx, w.opts.Owner, w.opts.Lease, w.opts.MaxAttempts)
}

// Shutdown waits for current jobs to finish.
func (w *Worker) Shutdown() { _ = w.ShutdownContext(context.Background()) }

// ShutdownContext stops claiming and releases unfinished leases at the deadline.
func (w *Worker) ShutdownContext(ctx context.Context) error {
	w.mu.Lock()
	if !w.stopped {
		w.stopped = true
		close(w.stop)
	}
	cancel := w.cancel
	w.mu.Unlock()
	done := make(chan struct{})
	go func() { w.wg.Wait(); close(done) }()
	select {
	case <-done:
		if cancel != nil {
			cancel(errShutdown)
		}
		return nil
	case <-ctx.Done():
		if cancel != nil {
			cancel(errShutdown)
		}
		<-done
		return ctx.Err()
	}
}

var manifestRevisionNamespace = uuid.MustParse("c3d59c98-994f-46b0-9f5c-0c4a12437a10")

func (w *Worker) hostedManifestEnabled() bool {
	if !w.opts.HostedVectorManifest {
		return false
	}
	_, transactional := w.vectors.(domain.TransactionalVectorStore)
	return !transactional
}

func revisionIDForAttempt(job domain.Job, owner string) string {
	if job.RevisionID != "" {
		return job.RevisionID
	}
	token := owner
	if job.LeaseToken != "" {
		token = job.LeaseToken
	}
	seed := fmt.Sprintf("%s\x00%s\x00%d\x00%s", job.Document.ID, job.Incarnation, job.Attempts, token)
	return uuid.NewSHA1(manifestRevisionNamespace, []byte(seed)).String()
}

func persistAttemptManifest(ctx context.Context, ms domain.VectorManifestStore, job domain.Job, chunks []domain.Chunk) error {
	vectorIDs := make([]string, len(chunks))
	chunkIDs := make([]string, len(chunks))
	for i, ch := range chunks {
		vectorIDs[i] = ch.VectorID
		if vectorIDs[i] == "" {
			vectorIDs[i] = domain.FormatRevisionVectorID(job.Document.ID, job.RevisionID, ch.Index)
		}
		chunkIDs[i] = string(ch.ID)
	}
	return ms.PersistVectorManifest(ctx, job, domain.VectorManifest{
		RevisionID:  job.RevisionID,
		DocumentID:  job.Document.ID,
		Incarnation: job.Incarnation,
		Attempt:     job.Attempts,
		LeaseToken:  job.LeaseToken,
		Collection:  job.Collection,
		IDPrefix:    domain.RevisionVectorPrefix(job.Document.ID, job.RevisionID),
		VectorIDs:   vectorIDs,
		ChunkIDs:    chunkIDs,
	})
}

func (w *Worker) process(parent context.Context, job domain.Job) {
	if w.hostedManifestEnabled() {
		job.RevisionID = revisionIDForAttempt(job, w.opts.Owner)
	}
	redact := w.opts.RedactTelemetry || job.Tenant != ""
	attrs := []attribute.KeyValue{attribute.String(tracing.LangSmithKind, "chain"), attribute.Int("quarry.job.attempt", job.Attempts)}
	if !redact {
		attrs = append(tracing.DocumentAttributes(string(job.Document.ID)), attrs...)
	} else {
		parent = tracing.WithRedactIdentifiers(parent, true)
	}
	parent, span := otel.Tracer("github.com/Victoroide/quarry-rag/internal/worker").Start(parent, "ingest.job", trace.WithNewRoot(), trace.WithAttributes(attrs...))
	defer span.End()
	parent = metrics.WithService(parent, w.opts.Metrics)
	timeoutCtx, stop := context.WithTimeout(parent, w.opts.JobTimeout)
	defer stop()
	ctx, cancel := context.WithCancelCause(timeoutCtx)
	defer cancel(nil)
	leaseCtx, cancelLease := context.WithCancelCause(parent)
	hbDone, hbStop := make(chan struct{}), make(chan struct{})
	defer func() { close(hbStop); cancelLease(nil); <-hbDone }()
	go func() {
		defer close(hbDone)
		ticker := time.NewTicker(w.opts.Lease / 3)
		defer ticker.Stop()
		for {
			select {
			case <-leaseCtx.Done():
				return
			case <-hbStop:
				return
			case <-ticker.C:
				ok, err := w.store.HeartbeatJob(leaseCtx, job, w.opts.Lease)
				if err != nil {
					if redact {
						slog.ErrorContext(leaseCtx, "renew ingestion lease", "error", err)
					} else {
						slog.ErrorContext(leaseCtx, "renew ingestion lease", "document_id", job.Document.ID, "error", err)
					}
				}
				if err != nil || !ok {
					cancelLease(errLeaseLost)
					cancel(errLeaseLost)
					return
				}
			}
		}
	}()
	ic := pipeline.NewIngestContext(job.Document.ID, job.Document.Content, job.Document.Metadata)
	ic.FileName = job.Document.FileName
	ic.FileType = job.Document.FileType
	ic.S3Key = job.Document.S3Key
	ic.DocType = job.DocType
	ic.Collection = job.Collection
	ic.Tenant = job.Tenant
	ic.Stopping = w.stop
	if w.hostedManifestEnabled() {
		ic.RevisionID = job.RevisionID
		if ms, ok := w.store.(domain.VectorManifestStore); ok {
			ic.PersistManifest = func(mCtx context.Context, chunks []domain.Chunk) error {
				return persistAttemptManifest(mCtx, ms, job, chunks)
			}
		}
	} else {
		if job.ScopedVectors {
			ic.VectorRevision = job.Incarnation
			ic.VectorAttempt = job.LeaseToken
		}
		if guard, ok := w.store.(domain.JobVectorGuard); ok {
			ic.MutateVectors = func(ctx context.Context, mutate func(context.Context) error) error {
				return guard.MutateJobVectors(ctx, job, mutate)
			}
		}
	}
	err := w.run(ctx, job, ic)
	tracing.RecordError(span, err)
	if parent.Err() != nil {
		w.release(parent, job)
		return
	}
	if errors.Is(context.Cause(leaseCtx), errLeaseLost) {
		if w.hostedManifestEnabled() {
			w.compensateDeleted(parent, job, ic.Chunks)
		}
		return
	}
	if err == nil || errors.Is(err, domain.ErrJobNotOwned) {
		return
	}
	if w.hostedManifestEnabled() {
		w.compensateDeleted(parent, job, ic.Chunks)
	}
	if permanent(err) || job.Attempts >= w.opts.MaxAttempts {
		confirmCtx, confirmCancel := context.WithTimeout(leaseCtx, 5*time.Second)
		owned, confirmErr := w.store.HeartbeatJob(confirmCtx, job, w.opts.Lease)
		confirmCancel()
		if confirmErr != nil || !owned {
			w.logStateError(redact, job, confirmErr)
			if w.hostedManifestEnabled() {
				w.compensateDeleted(parent, job, ic.Chunks)
			}
			return
		}
		w.deleteVectors(leaseCtx, job, ic.Chunks)
		if parent.Err() != nil {
			w.release(parent, job)
			return
		}
		if errors.Is(context.Cause(leaseCtx), errLeaseLost) {
			if w.hostedManifestEnabled() {
				w.compensateDeleted(parent, job, ic.Chunks)
			}
			return
		}
		stateCtx, stateCancel := context.WithTimeout(context.WithoutCancel(parent), 5*time.Second)
		defer stateCancel()
		message := err.Error()
		switch {
		case errors.Is(err, ocr.ErrNoText):
			message = ocr.ErrNoText.Error()
		case errors.Is(err, domain.ErrPDFOCRExpired):
			message = domain.ErrPDFOCRExpired.Error()
		}
		failErr := w.store.FailJob(stateCtx, job, message)
		w.recordOutcome(parent, job, "failed", failErr)
		if w.hostedManifestEnabled() && errors.Is(failErr, domain.ErrJobNotOwned) {
			w.compensateDeleted(parent, job, ic.Chunks)
		}
		return
	}
	stateCtx, stateCancel := context.WithTimeout(context.WithoutCancel(parent), 5*time.Second)
	defer stateCancel()
	delay := 5 * time.Second
	for i := 1; i < job.Attempts && delay < 5*time.Minute; i++ {
		delay *= 2
	}
	delay = min(delay, 5*time.Minute) + time.Duration(rand.Int64N(int64(time.Second)+1)) // #nosec G404 -- Retry jitter does not require cryptographic randomness.
	w.recordOutcome(parent, job, "retried", w.store.RetryJob(stateCtx, job, delay, err.Error()))
}

func (w *Worker) release(ctx context.Context, job domain.Job) {
	releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	w.recordOutcome(ctx, job, "released", w.store.ReleaseJob(releaseCtx, job))
}

func (w *Worker) run(ctx context.Context, job domain.Job, ic *pipeline.IngestContext) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("ingestion panic: %v", recovered)
		}
	}()
	switch {
	case job.PDF != nil && job.PDF.Coverage == domain.PDFPending:
		if extractErr := w.extractPDFText(ctx, job, ic); extractErr != nil {
			return extractErr
		}
	case job.PDF != nil && job.PDF.Coverage == domain.PDFPartial && len(ic.RawText) < domain.MinPDFTextBytes:
		return domain.Permanent(ocr.ErrNoText)
	case job.PDF != nil && job.PDF.Profile == domain.LayoutPDFProfile && len(job.PDF.PageResults) > 0:
		ic.LayoutSections = layoutSectionsFromResults(job.PDF.PageResults)
	case job.Document.TextSource == "ocr_pending":
		if extractErr := w.extractText(ctx, job, ic); extractErr != nil {
			return extractErr
		}
	}
	if contextErr := ctx.Err(); contextErr != nil {
		return contextErr
	}
	if w.vectors != nil {
		var delErr error
		if w.hostedManifestEnabled() {
			delErr = w.deleteAttemptVectors(ctx, job, nil)
		} else {
			delErr = w.removeJobVectors(ctx, job)
		}
		if delErr != nil {
			return fmt.Errorf("remove previous vectors: %w", delErr)
		}
	}
	if _, err = pipeline.RunIngestionPipeline(ctx, ic, w.stages); err != nil {
		return err
	}
	ok, err := w.store.CompleteJob(ctx, job, ic.Chunks, ic.IndexObservation)
	if err != nil {
		return err
	}
	if !ok {
		if w.hostedManifestEnabled() {
			w.compensateDeleted(ctx, job, ic.Chunks)
		}
	} else {
		w.opts.Metrics.IngestJobs.WithLabelValues("completed").Inc()
		trace.SpanFromContext(ctx).SetAttributes(attribute.String("quarry.job.result", "completed"))
	}
	return nil
}

func (w *Worker) extractText(ctx context.Context, job domain.Job, ic *pipeline.IngestContext) error {
	if w.opts.OCR == nil {
		return errors.New("ocr: text detection is unavailable")
	}
	if job.Document.S3Key == nil || *job.Document.S3Key == "" {
		return domain.Permanent(errors.New("ocr: original document is missing"))
	}
	store, ok := w.store.(interface {
		StoreOCRText(context.Context, domain.Job, string) error
	})
	if !ok {
		return errors.New("ocr: detected text storage is unavailable")
	}
	owned, err := w.store.HeartbeatJob(ctx, job, w.opts.Lease)
	if err != nil {
		return err
	}
	if !owned {
		return domain.ErrJobNotOwned
	}
	text, err := w.opts.OCR.Extract(ctx, *job.Document.S3Key)
	if err != nil {
		return err
	}
	if err := store.StoreOCRText(ctx, job, text); err != nil {
		return err
	}
	ic.RawText = text
	return nil
}
func permanent(err error) bool {
	if errors.Is(err, domain.ErrPermanent) || errors.Is(err, domain.ErrEmbeddingModelLimit) || errors.Is(err, pipeline.ErrNoChunks) || status.Code(err) == codes.InvalidArgument {
		return true
	}
	var apiErr smithy.APIError
	return errors.As(err, &apiErr) && apiErr.ErrorCode() == "ValidationException"
}
func (w *Worker) compensateDeleted(ctx context.Context, job domain.Job, chunks []domain.Chunk) {
	ds, ok := w.store.(interface {
		DocumentExists(context.Context, domain.DocumentID) (bool, error)
	})
	if !ok {
		return
	}
	checkCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	exists, err := ds.DocumentExists(checkCtx, job.Document.ID)
	if err != nil {
		w.logStateError(w.opts.RedactTelemetry || job.Tenant != "", job, err)
		return
	}
	if !exists {
		w.deleteVectors(context.WithoutCancel(ctx), job, chunks)
	}
}
func (w *Worker) deleteVectors(ctx context.Context, job domain.Job, chunks []domain.Chunk) {
	if w.vectors == nil {
		return
	}
	cleanupCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	redact := w.opts.RedactTelemetry || job.Tenant != ""
	if w.hostedManifestEnabled() {
		w.logStateError(redact, job, w.deleteAttemptVectors(cleanupCtx, job, chunks))
		return
	}
	w.logStateError(redact, job, w.removeJobVectors(cleanupCtx, job))
}

func (w *Worker) removeJobVectors(ctx context.Context, job domain.Job) error {
	remove := func(ctx context.Context) error {
		if job.ScopedVectors {
			vectors, ok := w.vectors.(domain.AttemptVectorDeleter)
			if !ok {
				return errors.New("revision vector cleanup is unavailable")
			}
			return vectors.DeleteByAttempt(ctx, string(job.Document.ID), job.Incarnation, job.LeaseToken)
		}
		return w.vectors.DeleteByDocID(ctx, string(job.Document.ID))
	}
	if guard, ok := w.store.(domain.JobVectorGuard); ok {
		return guard.MutateJobVectors(ctx, job, func(ctx context.Context) error {
			if store, managed := w.vectors.(domain.VectorDispatchStore); managed && store.ManagesVectorDispatch() {
				return remove(ctx)
			}
			return domain.DispatchVectorMutation(ctx, true, remove)
		})
	}
	if job.ScopedVectors {
		return errors.New("job vector guard is unavailable")
	}
	return remove(ctx)
}

func (w *Worker) deleteAttemptVectors(ctx context.Context, job domain.Job, chunks []domain.Chunk) error {
	if w.vectors == nil {
		return nil
	}
	revDeleter, ok := w.vectors.(domain.ManifestRevisionVectorDeleter)
	if !ok {
		return errors.New("worker: revision-targeted vector deletion is unavailable")
	}
	var knownIDs []string
	if len(chunks) > 0 {
		knownIDs = make([]string, len(chunks))
		for i, ch := range chunks {
			knownIDs[i] = ch.VectorID
			if knownIDs[i] == "" {
				knownIDs[i] = domain.FormatRevisionVectorID(job.Document.ID, job.RevisionID, ch.Index)
			}
		}
	} else if ms, hasMS := w.store.(domain.VectorManifestStore); hasMS && job.RevisionID != "" {
		if m, err := ms.GetVectorManifest(ctx, job.RevisionID); err == nil && m != nil {
			knownIDs = m.VectorIDs
		}
	}
	return revDeleter.DeleteByManifestRevision(ctx, string(job.Document.ID), job.RevisionID, knownIDs)
}

func (w *Worker) logStateError(redact bool, job domain.Job, err error) {
	if err != nil {
		if redact {
			slog.Error("update ingestion state", "error", err)
			return
		}
		slog.Error("update ingestion state", "document_id", job.Document.ID, "error", err)
	}
}

func (w *Worker) recordOutcome(ctx context.Context, job domain.Job, result string, err error) {
	w.logStateError(w.opts.RedactTelemetry || job.Tenant != "", job, err)
	tracing.RecordError(trace.SpanFromContext(ctx), err)
	if err == nil {
		w.opts.Metrics.IngestJobs.WithLabelValues(result).Inc()
		trace.SpanFromContext(ctx).SetAttributes(attribute.String("quarry.job.result", result))
	}
}
