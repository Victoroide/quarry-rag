package tracing

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/llm"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
	brtypes "github.com/aws/aws-sdk-go-v2/service/bedrockruntime/types"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
)

func TestStreamingDecoratorUsageAndCapture(t *testing.T) {
	for _, capture := range []bool{false, true} {
		t.Run(map[bool]string{false: "private", true: "captured"}[capture], func(t *testing.T) {
			recorder := recordSpans(t)
			ctx, parent := otel.Tracer("test").Start(WithContentCapture(t.Context(), capture), "query.synthesis")
			defer parent.End()
			wrapped := WrapConverser(llm.NewFakeConverser(), "offline", "default")
			stream, ok := wrapped.(llm.StreamConverser)
			require.True(t, ok, "decorator must preserve streaming")
			_, ok = wrapped.(llm.ModelConverser)
			require.True(t, ok)
			messages := []brtypes.Message{{Role: brtypes.ConversationRoleUser, Content: []brtypes.ContentBlock{&brtypes.ContentBlockMemberText{Value: "Archive evidence"}}}}
			var text strings.Builder
			out, err := stream.ConverseStreamWithModelAndSystem(ctx, "selected", "instructions", messages, .2, 80, func(delta string) error {
				require.Empty(t, recorder.Ended(), "span ended before stream completion")
				text.WriteString(delta)
				return nil
			})
			require.NoError(t, err)
			require.Len(t, recorder.Ended(), 1)
			span := recorder.Ended()[0]
			require.Equal(t, parent.SpanContext().SpanID(), span.Parent().SpanID())
			attrs := spanAttrs(span)
			require.True(t, attrs["gen_ai.request.streaming"].AsBool())
			require.Equal(t, "selected", attrs["gen_ai.request.model"].AsString())
			require.EqualValues(t, *out.Usage.OutputTokens, attrs["gen_ai.usage.output_tokens"].AsInt64())
			if capture {
				require.Equal(t, text.String(), attrs["gen_ai.completion.0.content"].AsString())
			} else {
				for _, a := range span.Attributes() {
					require.NotContains(t, a.Value.String(), "Archive evidence")
				}
			}
		})
	}
}

type failingStreamConverser struct{ llm.FakeConverser }

func (failingStreamConverser) ConverseStreamWithModelAndSystem(ctx context.Context, _, _ string, _ []brtypes.Message, _ float32, _ int, fn func(string) error) (*bedrockruntime.ConverseOutput, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return nil, fn("partial")
}

func TestStreamingDecoratorRecordsCallbackFailure(t *testing.T) {
	recorder := recordSpans(t)
	stream, ok := WrapConverser(&failingStreamConverser{}, "offline", "default").(llm.StreamConverser)
	require.True(t, ok)
	failed := errors.New("client stopped")
	_, err := stream.ConverseStreamWithModelAndSystem(t.Context(), "selected", "", nil, 0, 80, func(string) error { return failed })
	require.ErrorIs(t, err, failed)
	require.Len(t, recorder.Ended(), 1)
	require.Equal(t, codes.Error, recorder.Ended()[0].Status().Code)
}
