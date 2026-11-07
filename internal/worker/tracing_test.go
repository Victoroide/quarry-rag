package worker

import (
	"context"
	"errors"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/pipeline"
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
