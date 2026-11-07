package pipeline

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/tracing"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func pipelineRecorder(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()
	previous := otel.GetTracerProvider()
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	otel.SetTracerProvider(provider)
	t.Cleanup(func() {
		require.NoError(t, provider.Shutdown(context.WithoutCancel(t.Context())))
		otel.SetTracerProvider(previous)
	})
	return recorder
}

func tracingAttributes(span sdktrace.ReadOnlySpan) map[string]attribute.Value {
	attrs := map[string]attribute.Value{}
	for _, attr := range span.Attributes() {
		attrs[string(attr.Key)] = attr.Value
	}
	return attrs
}

func TestQueryStageSpansAndContentPolicy(t *testing.T) {
	for _, capture := range []bool{false, true} {
		t.Run(fmt.Sprint(capture), func(t *testing.T) {
			recorder := pipelineRecorder(t)
			ctx, parent := otel.Tracer("test").Start(tracing.WithContentCapture(t.Context(), capture), "request")
			qc := &QueryContext{OriginalQuery: "fixture-question", SubQueries: []string{"fixture-question"}, RetrievalMode: "LOCAL", RetrievedChunks: []domain.ScoredChunk{{Chunk: domain.Chunk{DocumentID: "doc", Text: strings.Repeat("界", 4001), Index: 2, Metadata: map[string]any{"doc_type": "manual"}}, Score: 0.9}}, TopScore: 0.9, ContextChunks: 1, ContextTokens: 12, FinishReason: "end_turn", FinalAnswer: "fixture-answer"}
			stages := []QueryStage{}
			for _, id := range []StageID{StageQueryDecomposition, StageRetrievalRouting, StageRetrieval, StageSynthesis} {
				stages = append(stages, metricQueryStage{id, func(*QueryContext) error { return nil }})
			}
			_, err := RunRetrievalPipeline(ctx, qc, stages)
			require.NoError(t, err)
			spans := recorder.Ended()
			require.Len(t, spans, 4)
			for i, name := range []string{"query.decompose", "query.routing", "query.retrieval", "query.synthesis"} {
				require.Equal(t, name, spans[i].Name())
				require.Equal(t, trace.SpanKindInternal, spans[i].SpanKind())
				require.Equal(t, parent.SpanContext().SpanID(), spans[i].Parent().SpanID())
				attrs := tracingAttributes(spans[i])
				require.Equal(t, string(stages[i].ID()), attrs["quarry.stage"].AsString())
				require.EqualValues(t, 1, attrs["quarry.query.sub_queries"].AsInt64())
				require.Equal(t, "LOCAL", attrs["quarry.retrieval.mode"].AsString())
				require.EqualValues(t, 1, attrs["quarry.retrieval.chunks"].AsInt64())
				require.Equal(t, 0.9, attrs["quarry.retrieval.top_score"].AsFloat64())
				require.EqualValues(t, 1, attrs["quarry.synthesis.context_chunks"].AsInt64())
				require.EqualValues(t, 12, attrs["quarry.synthesis.context_tokens"].AsInt64())
				require.Equal(t, "end_turn", attrs["quarry.synthesis.finish_reason"].AsString())
				require.False(t, attrs["quarry.synthesis.early_exit"].AsBool())
				if !capture {
					require.EqualValues(t, 16, attrs["quarry.query.bytes"].AsInt64())
					require.Equal(t, "c09d7b1a01be", attrs["quarry.query.digest"].AsString())
					for _, attr := range spans[i].Attributes() {
						require.False(t, strings.HasPrefix(string(attr.Key), "retrieval.documents."))
						require.NotContains(t, attr.Value.String(), "fixture-question")
						require.NotContains(t, attr.Value.String(), "fixture-answer")
						require.NotContains(t, attr.Value.String(), "界")
					}
				}
			}
			require.Equal(t, "retriever", tracingAttributes(spans[2])["langsmith.span.kind"].AsString())
			if capture {
				retrieval := tracingAttributes(spans[2])
				require.Len(t, []rune(retrieval["retrieval.documents.0.document.content"].AsString()), 4000)
				require.JSONEq(t, `{"doc_id":"doc","doc_type":"manual","chunk_index":2,"score":0.9}`, retrieval["retrieval.documents.0.document.metadata"].AsString())
				require.NotContains(t, retrieval, "output.value")
				require.JSONEq(t, `{"question":"fixture-question"}`, tracingAttributes(spans[3])["input.value"].AsString())
				require.JSONEq(t, `{"answer":"fixture-answer"}`, tracingAttributes(spans[3])["output.value"].AsString())
			}
			parent.End()
		})
	}
}

type tracingIngestStage struct{ id StageID }

func (s tracingIngestStage) ID() StageID { return s.id }
func (tracingIngestStage) Run(_ context.Context, ic *IngestContext) error {
	ic.Chunks = []domain.Chunk{{Text: "fixture-chunk"}}
	return nil
}

func TestIngestStageSpansAndQueryFailure(t *testing.T) {
	recorder := pipelineRecorder(t)
	ctx, parent := otel.Tracer("test").Start(t.Context(), "parent")
	defer parent.End()
	_, err := RunIngestionPipeline(ctx, NewIngestContext("doc", "fixture-document", nil), []Stage{tracingIngestStage{StageChunk}, tracingIngestStage{StageEmbed}, tracingIngestStage{StagePersist}})
	require.NoError(t, err)
	_, err = RunRetrievalPipeline(ctx, &QueryContext{}, []QueryStage{metricQueryStage{StageRetrieval, func(*QueryContext) error { return errors.New("retrieval unavailable") }}})
	require.Error(t, err)
	spans := recorder.Ended()
	require.Len(t, spans, 4)
	for i, name := range []string{"ingest.chunk", "ingest.embed", "ingest.persist"} {
		require.Equal(t, name, spans[i].Name())
		require.Equal(t, trace.SpanKindInternal, spans[i].SpanKind())
		require.Equal(t, parent.SpanContext().SpanID(), spans[i].Parent().SpanID())
		require.EqualValues(t, 1, tracingAttributes(spans[i])["quarry.ingest.chunks"].AsInt64())
	}
	require.Equal(t, codes.Error, spans[3].Status().Code)
	require.Len(t, spans[3].Events(), 1)
}

func TestPanickingStageEndsErrorSpan(t *testing.T) {
	recorder := pipelineRecorder(t)
	ctx, parent := otel.Tracer("test").Start(t.Context(), "parent")
	defer parent.End()
	require.Panics(t, func() {
		_, _ = RunRetrievalPipeline(ctx, &QueryContext{}, []QueryStage{metricQueryStage{StageSynthesis, func(*QueryContext) error { panic("unavailable") }}})
	})
	require.Len(t, recorder.Ended(), 1)
	require.Equal(t, codes.Error, recorder.Ended()[0].Status().Code)
}
