package worker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/pipeline"
	"github.com/Victoroide/quarry-rag/internal/store/pinecone"
	"github.com/Victoroide/quarry-rag/internal/tracing"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func TestIngestionJobRootAndOutcomes(t *testing.T) {
	for _, result := range []string{"completed", "retried", "failed", "released"} {
		t.Run(result, func(t *testing.T) {
			previous := otel.GetTracerProvider()
			recorder := tracetest.NewSpanRecorder()
			provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
			otel.SetTracerProvider(provider)
			t.Cleanup(func() {
				require.NoError(t, provider.Shutdown(context.WithoutCancel(t.Context())))
				otel.SetTracerProvider(previous)
			})
			ctx, parent := otel.Tracer("test").Start(t.Context(), "ingestion-request")
			defer parent.End()
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			store := metricJobStore{leaseTestStore: leaseStore(0), complete: true}
			stage := leaseTestStage{func(context.Context) error {
				switch result {
				case "retried":
					return errors.New("temporary")
				case "failed":
					return domain.Permanent(errors.New("invalid"))
				case "released":
					cancel()
					return context.Canceled
				}
				return nil
			}}
			w := New(store, []pipeline.Stage{stage}, nil, Options{})
			w.process(ctx, domain.Job{Document: domain.Document{ID: "fixture-doc", Content: "fixture-content"}, Attempts: 2})
			var jobSpan sdktrace.ReadOnlySpan
			for _, span := range recorder.Ended() {
				if span.Name() == "ingest.job" {
					jobSpan = span
				}
			}
			require.NotNil(t, jobSpan)
			require.False(t, jobSpan.Parent().IsValid())
			require.Equal(t, trace.SpanKindInternal, jobSpan.SpanKind())
			require.Len(t, recorder.Ended(), 2)
			require.Equal(t, "ingest.embed", recorder.Ended()[0].Name())
			require.Equal(t, jobSpan.SpanContext().SpanID(), recorder.Ended()[0].Parent().SpanID())
			require.NotEqual(t, parent.SpanContext().TraceID(), jobSpan.SpanContext().TraceID())
			attrs := map[string]attribute.Value{}
			for _, attr := range jobSpan.Attributes() {
				attrs[string(attr.Key)] = attr.Value
				require.NotContains(t, attr.Value.String(), "fixture-content")
			}
			require.Equal(t, "fixture-doc", attrs["quarry.document_id"].AsString())
			require.Equal(t, "fixture-doc", attrs["langsmith.metadata.document_id"].AsString())
			require.EqualValues(t, 2, attrs["quarry.job.attempt"].AsInt64())
			require.Equal(t, result, attrs["quarry.job.result"].AsString())
			if result == "failed" || result == "retried" {
				require.Equal(t, codes.Error, jobSpan.Status().Code)
			}
		})
	}
}

type telemetryLogBuffer struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

func (b *telemetryLogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.Write(p)
}

func (b *telemetryLogBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.String()
}

func captureTelemetryLogs(t *testing.T) *telemetryLogBuffer {
	t.Helper()
	buf := &telemetryLogBuffer{}
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return buf
}

type telemetryJobStore struct {
	*leaseTestStore
	claimErr, heartbeatErr, completionErr, stateErr error
	claimCalled                                     chan struct{}
	result, reason                                  string
}

func (s *telemetryJobStore) ClaimJob(ctx context.Context, owner string, lease time.Duration, attempts int) (*domain.Job, error) {
	if s.claimErr != nil {
		close(s.claimCalled)
		return nil, s.claimErr
	}
	return s.leaseTestStore.ClaimJob(ctx, owner, lease, attempts)
}

func (s *telemetryJobStore) HeartbeatJob(context.Context, domain.Job, time.Duration) (bool, error) {
	return s.heartbeatErr == nil, s.heartbeatErr
}

func (s *telemetryJobStore) CompleteJob(ctx context.Context, _ domain.Job, _ []domain.Chunk, _ domain.IndexObservation) (bool, error) {
	if s.completionErr == nil {
		s.result = "completed"
		return true, nil
	}
	db := tracing.NewQueryTracer()
	ctx = db.TraceQueryStart(ctx, nil, pgx.TraceQueryStartData{SQL: "UPDATE documents SET content = 'synthetic-content'"})
	db.TraceQueryEnd(ctx, nil, pgx.TraceQueryEndData{Err: s.completionErr})
	return false, s.completionErr
}

func (s *telemetryJobStore) RetryJob(_ context.Context, _ domain.Job, _ time.Duration, reason string) error {
	s.result, s.reason = "retried", reason
	return s.stateErr
}

func (s *telemetryJobStore) FailJob(_ context.Context, _ domain.Job, reason string) error {
	s.result, s.reason = "failed", reason
	return s.stateErr
}

func (s *telemetryJobStore) ReleaseJob(context.Context, domain.Job) error {
	s.result = "released"
	return s.stateErr
}

func TestOwnedIngestionFailureRedactsAllTelemetry(t *testing.T) {
	const (
		doc     = "25ba39fe-0a4f-4cad-8941-0f26d677ceb2"
		chunk   = "synthetic-chunk-key"
		object  = "synthetic-object-key"
		content = "synthetic-content"
	)
	providerErr := errors.New(strings.Join([]string{doc, chunk, object, content}, "\n"))
	dbErr := &pgconn.PgError{Code: "22000", Message: doc + " " + chunk, Detail: object + " " + content}
	stateErr := fmt.Errorf("state update: %w", providerErr)
	for _, tc := range []struct {
		name, tenant, result, category        string
		global, shutdown, deadline, leaseLoss bool
		completion                            error
	}{
		{name: "owned Pinecone preflight", tenant: "tenant", result: "retried", category: "operation failed"},
		{name: "grants global PostgreSQL completion", global: true, completion: domain.Permanent(dbErr), result: "failed", category: "operation failed"},
		{name: "cancellation", tenant: "tenant", shutdown: true, result: "released", category: "operation canceled"},
		{name: "deadline", global: true, deadline: true, result: "retried", category: "operation timed out"},
		{name: "lease lost", tenant: "tenant", leaseLoss: true, category: "operation canceled"},
		{name: "service control", result: "retried"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logs := captureTelemetryLogs(t)
			prev := otel.GetTracerProvider()
			rec := tracetest.NewSpanRecorder()
			tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
			otel.SetTracerProvider(tp)
			t.Cleanup(func() {
				require.NoError(t, tp.Shutdown(context.WithoutCancel(t.Context())))
				otel.SetTracerProvider(prev)
			})

			parent, cancel := context.WithCancel(tracing.WithContentCapture(t.Context(), true))
			defer cancel()
			store := &telemetryJobStore{leaseTestStore: leaseStore(0), completionErr: tc.completion, stateErr: stateErr}
			opts := Options{RedactTelemetry: tc.global, MaxAttempts: 3, Lease: time.Hour}
			if tc.deadline {
				opts.JobTimeout = 20 * time.Millisecond
			}
			if tc.leaseLoss {
				opts.Lease = 30 * time.Millisecond
				store.heartbeatErr = providerErr
			}
			// The real adapter rejects oversized metadata before issuing any remote call.
			vectors := tracing.WrapVectorStore(pinecone.NewDriver(&pinecone.Client{}), "pinecone")
			fail := true
			stage := leaseTestStage{func(ctx context.Context) error {
				if !fail {
					return nil
				}
				if tc.tenant != "" || tc.global {
					require.False(t, tracing.ContentCapture(ctx))
				}
				switch {
				case tc.result == "failed":
					return nil
				case tc.shutdown:
					cancel()
					return errors.Join(providerErr, ctx.Err())
				case tc.deadline || tc.leaseLoss:
					<-ctx.Done()
					return errors.Join(providerErr, ctx.Err())
				default:
					return vectors.Upsert(ctx, doc, []domain.Chunk{{
						ID: chunk, DocumentID: doc, Embedding: []float32{1}, Text: strings.Repeat(content, 4000),
					}})
				}
			}}
			job := domain.Job{Document: domain.Document{ID: doc, Content: content}, Tenant: tc.tenant, Attempts: 1}
			New(store, []pipeline.Stage{stage}, nil, opts).process(parent, job)
			require.Equal(t, tc.result, store.result)
			if tc.result == "retried" || tc.result == "failed" {
				switch {
				case tc.completion != nil:
					require.Equal(t, tc.completion.Error(), store.reason)
				case tc.deadline:
					require.Contains(t, store.reason, context.DeadlineExceeded.Error())
				default:
					require.Contains(t, store.reason, chunk)
				}
			}
			require.Contains(t, logs.String(), "error")
			if tc.category == "" {
				require.Contains(t, logs.String(), doc)
				require.Contains(t, logs.String(), object)
				require.Contains(t, rec.Ended()[0].Status().Description, chunk)
				return
			}
			require.Contains(t, logs.String(), "operation failed")
			if tc.leaseLoss {
				require.Contains(t, logs.String(), "renew ingestion lease")
			} else {
				require.Contains(t, logs.String(), "update ingestion state")
			}
			failedSpans := map[string]bool{}
			for _, span := range rec.Ended() {
				if span.Status().Code == codes.Error {
					failedSpans[span.Name()] = true
					require.Contains(t, []string{tc.category, "operation failed"}, span.Status().Description)
					for _, event := range span.Events() {
						require.Equal(t, "exception", event.Name)
						for _, attr := range event.Attributes {
							require.Contains(t, []string{tc.category, "operation failed", "*errors.errorString"}, attr.Value.AsString())
						}
					}
				}
				dump := fmt.Sprint(span.Name(), span.Status(), span.Attributes(), span.Events())
				for _, canary := range []string{doc, chunk, object, content} {
					require.NotContains(t, logs.String(), canary)
					require.NotContains(t, dump, canary)
				}
			}
			require.True(t, failedSpans["ingest.job"])
			switch {
			case tc.completion != nil:
				require.True(t, failedSpans["db.query"])
			case !tc.shutdown && !tc.deadline && !tc.leaseLoss:
				require.True(t, failedSpans["vector.upsert"])
			default:
				require.True(t, failedSpans["ingest.embed"])
			}

			if tc.result == "retried" || tc.leaseLoss {
				fail = false
				store.stateErr, store.heartbeatErr = nil, nil
				job.Attempts++
				New(store, []pipeline.Stage{stage}, nil, opts).process(t.Context(), job)
				require.Equal(t, "completed", store.result)
			}
		})
	}

	t.Run("claim before a job exists", func(t *testing.T) {
		logs := captureTelemetryLogs(t)
		store := &telemetryJobStore{leaseTestStore: leaseStore(0), claimErr: providerErr, claimCalled: make(chan struct{})}
		w := New(store, nil, nil, Options{Workers: 1, PollInterval: time.Hour, RedactTelemetry: true})
		w.Start(t.Context())
		t.Cleanup(w.Shutdown)
		select {
		case <-store.claimCalled:
		case <-time.After(5 * time.Second):
			t.Fatal("worker never attempted a claim")
		}
		w.Shutdown()
		require.Contains(t, logs.String(), "claim ingestion job")
		require.Contains(t, logs.String(), "operation failed")
		require.NotContains(t, logs.String(), doc)
	})
}
