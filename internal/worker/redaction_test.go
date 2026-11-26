package worker

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/logctx"
	"github.com/Victoroide/quarry-rag/internal/pipeline"
	"github.com/Victoroide/quarry-rag/internal/tracing"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestWorkerRedactsDocumentIDsAndPropagatesFlag(t *testing.T) {
	const secretDocID = "88888888-7777-4666-8555-444444444444"
	var logBuf bytes.Buffer
	prevLog := slog.Default()
	slog.SetDefault(slog.New(logctx.NewHandler(slog.NewJSONHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))))
	t.Cleanup(func() { slog.SetDefault(prevLog) })

	prevTracer := otel.GetTracerProvider()
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	otel.SetTracerProvider(provider)
	t.Cleanup(func() {
		require.NoError(t, provider.Shutdown(context.WithoutCancel(t.Context())))
		otel.SetTracerProvider(prevTracer)
	})

	var stageRedacted bool
	store := metricJobStore{leaseTestStore: leaseStore(0), complete: true}
	stage := leaseTestStage{func(ctx context.Context) error {
		stageRedacted = tracing.RedactIdentifiers(ctx)
		return errors.New("temporary failure")
	}}
	w := New(store, []pipeline.Stage{stage}, nil, Options{RedactTelemetry: true})
	w.process(t.Context(), domain.Job{
		Document: domain.Document{ID: domain.DocumentID(secretDocID), Content: "classified text"},
		Tenant:   "tenant-a",
		Attempts: 1,
	})

	require.True(t, stageRedacted)
	require.NotContains(t, logBuf.String(), secretDocID)
	require.NotContains(t, logBuf.String(), "classified text")
	for _, span := range recorder.Ended() {
		for _, attr := range span.Attributes() {
			require.NotEqual(t, "quarry.document_id", string(attr.Key))
			require.NotEqual(t, "langsmith.metadata.document_id", string(attr.Key))
			require.NotContains(t, attr.Value.String(), secretDocID)
		}
	}
}
