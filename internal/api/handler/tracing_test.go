package handler

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestIngestionTracesInProcessExtractionAndDocumentIdentity(t *testing.T) {
	previous := otel.GetTracerProvider()
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	otel.SetTracerProvider(provider)
	t.Cleanup(func() {
		require.NoError(t, provider.Shutdown(context.WithoutCancel(t.Context())))
		otel.SetTracerProvider(previous)
	})
	for _, format := range []string{"text", "docx"} {
		t.Run(format, func(t *testing.T) {
			before := len(recorder.Ended())
			ctx, parent := otel.Tracer("test").Start(t.Context(), "POST /ingest")
			request := detectionUpload(t, "fixture.txt", []byte("fixture-document"))
			if format == "docx" {
				request = identityRequest(t, "fixture-document", "", true)
			}
			store := &detectionAcceptStore{}
			rec := httptest.NewRecorder()
			IngestHandler(nil, store, nil, 20<<20).ServeHTTP(rec, request.WithContext(ctx))
			require.Equal(t, 202, rec.Code, rec.Body.String())
			parent.End()
			spans := recorder.Ended()[before:]
			require.Len(t, spans, 2)
			require.Equal(t, "extract "+format, spans[0].Name())
			require.Equal(t, parent.SpanContext().SpanID(), spans[0].Parent().SpanID())
			attributes := map[string]string{}
			for _, attr := range spans[1].Attributes() {
				attributes[string(attr.Key)] = attr.Value.AsString()
			}
			require.Equal(t, string(store.accepted.Document.ID), attributes["quarry.document_id"])
			require.Equal(t, string(store.accepted.Document.ID), attributes["langsmith.metadata.document_id"])
		})
	}
}
