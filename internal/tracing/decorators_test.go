package tracing

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/embedder"
	"github.com/Victoroide/quarry-rag/internal/extractor"
	"github.com/Victoroide/quarry-rag/internal/llm"
	"github.com/Victoroide/quarry-rag/internal/store/memory"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
	brtypes "github.com/aws/aws-sdk-go-v2/service/bedrockruntime/types"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func recordSpans(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()
	resetGlobals(t)
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	otel.SetTracerProvider(provider)
	t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.WithoutCancel(t.Context()))) })
	return recorder
}

func spanAttrs(span sdktrace.ReadOnlySpan) map[string]attribute.Value {
	attrs := make(map[string]attribute.Value)
	for _, attr := range span.Attributes() {
		attrs[string(attr.Key)] = attr.Value
	}
	return attrs
}

type fixtureConverser struct{}

func (fixtureConverser) Converse(_ context.Context, messages []brtypes.Message) (*bedrockruntime.ConverseOutput, error) {
	if len(messages) != 1 {
		return nil, errors.New("unexpected messages")
	}
	return &bedrockruntime.ConverseOutput{
		Output:     &brtypes.ConverseOutputMemberMessage{Value: brtypes.Message{Role: brtypes.ConversationRoleAssistant, Content: []brtypes.ContentBlock{&brtypes.ContentBlockMemberText{Value: "fixture-answer"}}}},
		Usage:      &brtypes.TokenUsage{InputTokens: aws.Int32(17), OutputTokens: aws.Int32(9), TotalTokens: aws.Int32(26)},
		StopReason: brtypes.StopReasonEndTurn,
	}, nil
}

type fixtureModelConverser struct{ fixtureConverser }

func (c fixtureModelConverser) ConverseWithModelAndSystem(ctx context.Context, model, system string, messages []brtypes.Message, temperature float32, maxTokens int) (*bedrockruntime.ConverseOutput, error) {
	if model != "fixture-model" || system != "fixture-system" || temperature != 0.25 || maxTokens != 80 {
		return nil, errors.New("request settings changed")
	}
	return c.Converse(ctx, messages)
}

func TestModelDecoratorsPreserveCallsAndCapturePolicy(t *testing.T) {
	for _, capture := range []bool{false, true} {
		t.Run(fmt.Sprint(capture), func(t *testing.T) {
			recorder := recordSpans(t)
			ctx, parent := otel.Tracer("test").Start(WithContentCapture(t.Context(), capture), "query.synthesis")
			inner := embedder.NewFakeEmbedder(256)
			emb := WrapEmbedder(inner, "offline", "fixture-embed-model")
			vector, err := emb.Embed(ctx, "fixture-question")
			require.NoError(t, err)
			expected, err := inner.Embed(ctx, "fixture-question")
			require.NoError(t, err)
			require.Equal(t, expected, vector)
			converser := WrapConverser(fixtureModelConverser{}, "aws.bedrock", "default-model")
			modelConverser, ok := converser.(llm.ModelConverser)
			require.True(t, ok)
			messages := []brtypes.Message{{Role: brtypes.ConversationRoleUser, Content: []brtypes.ContentBlock{&brtypes.ContentBlockMemberText{Value: "fixture-question"}}}}
			output, err := modelConverser.ConverseWithModelAndSystem(ctx, "fixture-model", "fixture-system", messages, 0.25, 80)
			require.NoError(t, err)
			require.Equal(t, "fixture-answer", llm.ExtractText(output))
			plain := WrapConverser(fixtureConverser{}, "offline", "default-model")
			_, ok = plain.(llm.ModelConverser)
			require.False(t, ok)
			_, err = plain.Converse(ctx, messages)
			require.NoError(t, err)
			parent.End()
			spans := recorder.Ended()
			require.Len(t, spans, 4)
			for i, name := range []string{"embeddings fixture-embed-model", "chat fixture-model", "chat default-model"} {
				require.Equal(t, name, spans[i].Name())
				require.Equal(t, trace.SpanKindClient, spans[i].SpanKind())
				require.Equal(t, parent.SpanContext().SpanID(), spans[i].Parent().SpanID())
			}
			attrs := spanAttrs(spans[1])
			for key, want := range map[string]string{"langsmith.span.kind": "llm", "gen_ai.system": "aws.bedrock", "gen_ai.provider.name": "aws.bedrock", "gen_ai.request.model": "fixture-model", "gen_ai.operation.name": "chat"} {
				require.Equal(t, want, attrs[key].AsString(), key)
			}
			require.Equal(t, 0.25, attrs["gen_ai.request.temperature"].AsFloat64())
			require.EqualValues(t, 80, attrs["gen_ai.request.max_tokens"].AsInt64())
			require.Equal(t, []string{"end_turn"}, attrs["gen_ai.response.finish_reasons"].AsStringSlice())
			for key, want := range map[string]int64{"gen_ai.usage.input_tokens": 17, "gen_ai.usage.prompt_tokens": 17, "gen_ai.usage.output_tokens": 9, "gen_ai.usage.completion_tokens": 9} {
				require.Equal(t, want, attrs[key].AsInt64(), key)
			}
			require.Equal(t, "embedding", spanAttrs(spans[0])["langsmith.span.kind"].AsString())
			if capture {
				require.Equal(t, "system", attrs["gen_ai.prompt.0.role"].AsString())
				require.Equal(t, "fixture-system", attrs["gen_ai.prompt.0.content"].AsString())
				require.Equal(t, "user", attrs["gen_ai.prompt.1.role"].AsString())
				require.Equal(t, "fixture-question", attrs["gen_ai.prompt.1.content"].AsString())
				require.Equal(t, "assistant", attrs["gen_ai.completion.0.role"].AsString())
				require.Equal(t, "fixture-answer", attrs["gen_ai.completion.0.content"].AsString())
			} else {
				for _, span := range spans {
					for _, attr := range span.Attributes() {
						require.NotContains(t, attr.Value.String(), "fixture-question")
						require.NotContains(t, attr.Value.String(), "fixture-system")
						require.NotContains(t, attr.Value.String(), "fixture-answer")
					}
				}
			}
		})
	}
}

func TestModelDecoratorRecordsEffectiveTokenLimit(t *testing.T) {
	for _, limit := range []int{0, -1, 80} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			recorder := recordSpans(t)
			converser := WrapConverser(llm.NewFakeConverser(), "offline", "fixture-model")
			modelConverser, ok := converser.(llm.ModelConverser)
			require.True(t, ok)
			_, err := modelConverser.ConverseWithModelAndSystem(t.Context(), "fixture-model", "", nil, 0, limit)
			require.NoError(t, err)
			require.Len(t, recorder.Ended(), 1)
			want := int64(4096)
			if limit > 0 {
				want = 80
			}
			require.Equal(t, want, spanAttrs(recorder.Ended()[0])["gen_ai.request.max_tokens"].AsInt64())
		})
	}
}

type limitedVectorStore struct{ domain.VectorStore }

func TestVectorDecoratorPreservesFilteringAndCounts(t *testing.T) {
	recorder := recordSpans(t)
	ctx, parent := otel.Tracer("test").Start(t.Context(), "query.retrieval")
	defer parent.End()
	store := WrapVectorStore(memory.NewFakeVectorStore(), "memory")
	filtered, ok := store.(domain.FilteredVectorStore)
	require.True(t, ok)
	deleter, ok := store.(domain.VectorDeleter)
	require.True(t, ok)
	chunks := []domain.Chunk{{ID: "chunk", DocumentID: "doc", Text: "fixture-excerpt", Embedding: []float32{1, 0}}, {ID: "other-chunk", DocumentID: "other-doc", Text: "fixture-excerpt", Embedding: []float32{1, 0}}}
	require.NoError(t, store.Upsert(ctx, "", chunks))
	results, err := filtered.QueryFiltered(ctx, []float32{1, 0}, 3, domain.VectorFilter{DocumentIDs: []string{"doc"}})
	require.NoError(t, err)
	require.Len(t, results, 1)
	results, err = store.Query(ctx, "", []float32{1, 0}, 4, "")
	require.NoError(t, err)
	require.Len(t, results, 2)
	_, err = store.QueryAllNamespaces(ctx, []float32{1, 0}, 5, "")
	require.NoError(t, err)
	require.NoError(t, deleter.DeleteByDocID(ctx, "doc"))
	require.NoError(t, deleter.DeleteAllVectorsInNamespace(ctx, ""))
	require.NoError(t, store.Ping(ctx))
	spans := recorder.Ended()
	require.Len(t, spans, 6)
	for i, name := range []string{"vector.upsert", "vector.query", "vector.query", "vector.query", "vector.delete", "vector.delete"} {
		require.Equal(t, name, spans[i].Name())
		require.Equal(t, trace.SpanKindClient, spans[i].SpanKind())
		require.Equal(t, parent.SpanContext().SpanID(), spans[i].Parent().SpanID())
		require.Equal(t, "memory", spanAttrs(spans[i])["db.system.name"].AsString())
		for _, attr := range spans[i].Attributes() {
			require.NotContains(t, attr.Value.String(), "fixture-excerpt")
		}
	}
	require.EqualValues(t, 2, spanAttrs(spans[0])["quarry.vector.records"].AsInt64())
	require.EqualValues(t, 3, spanAttrs(spans[1])["quarry.vector.top_k"].AsInt64())
	require.EqualValues(t, 1, spanAttrs(spans[1])["quarry.vector.results"].AsInt64())
	require.Equal(t, "retriever", spanAttrs(spans[1])["langsmith.span.kind"].AsString())
	limited := limitedVectorStore{memory.NewFakeVectorStore()}
	require.Equal(t, limited, WrapVectorStore(limited, "memory"))
}

type fixtureObjects struct{}

func (fixtureObjects) Upload(_ context.Context, _, _, _ string, _ extractor.FileType, r io.Reader) (string, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return "", err
	}
	if !bytes.Equal(data, []byte("fixture-file")) {
		return "", errors.New("unexpected file")
	}
	return "stored-key", nil
}
func (fixtureObjects) Delete(_ context.Context, key string) error {
	if key != "stored-key" {
		return errors.New("storage unavailable")
	}
	return nil
}

func TestStorageAndExtractionDecorators(t *testing.T) {
	recorder := recordSpans(t)
	ctx, parent := otel.Tracer("test").Start(t.Context(), "ingest")
	defer parent.End()
	objects := WrapObjectStore(fixtureObjects{})
	key, err := objects.Upload(ctx, "doc", "file", "file", extractor.FileTypeText, strings.NewReader("fixture-file"))
	require.NoError(t, err)
	require.Equal(t, "stored-key", key)
	require.NoError(t, objects.Delete(ctx, key))
	require.Error(t, objects.Delete(ctx, "missing"))
	extract := WrapExtract(func(ctx context.Context, ft extractor.FileType, data []byte) (string, error) {
		return extractor.New(ft).Extract(ctx, bytes.NewReader(data))
	})
	text, err := extract(ctx, extractor.FileTypeText, []byte("fixture-file"))
	require.NoError(t, err)
	require.Equal(t, "fixture-file", text)
	spans := recorder.Ended()
	require.Len(t, spans, 4)
	for i, name := range []string{"storage.upload", "storage.delete", "storage.delete", "extract text"} {
		require.Equal(t, name, spans[i].Name())
		require.Equal(t, parent.SpanContext().SpanID(), spans[i].Parent().SpanID())
	}
	require.Equal(t, "s3", spanAttrs(spans[0])["quarry.storage.system"].AsString())
	require.Equal(t, codes.Error, spans[2].Status().Code)
	require.Len(t, spans[2].Events(), 1)
	require.EqualValues(t, 12, spanAttrs(spans[3])["quarry.extract.bytes"].AsInt64())
	require.Equal(t, "text", spanAttrs(spans[3])["quarry.extract.file_type"].AsString())
}
