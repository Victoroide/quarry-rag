package llm

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	brtypes "github.com/aws/aws-sdk-go-v2/service/bedrockruntime/types"
	"github.com/stretchr/testify/require"
)

type streamReader struct {
	events chan brtypes.ConverseStreamOutput
	err    error
	closed bool
}

func (s *streamReader) Events() <-chan brtypes.ConverseStreamOutput { return s.events }
func (s *streamReader) Err() error                                  { return s.err }
func (s *streamReader) Close() error                                { s.closed = true; return nil }

func textDelta(text string) brtypes.ConverseStreamOutput {
	return &brtypes.ConverseStreamOutputMemberContentBlockDelta{Value: brtypes.ContentBlockDeltaEvent{Delta: &brtypes.ContentBlockDeltaMemberText{Value: text}}}
}

func TestReadConverseStream(t *testing.T) {
	s := &streamReader{events: make(chan brtypes.ConverseStreamOutput, 5)}
	s.events <- textDelta("Archive ")
	s.events <- textDelta("evidence")
	s.events <- &brtypes.ConverseStreamOutputMemberMessageStop{Value: brtypes.MessageStopEvent{StopReason: brtypes.StopReasonMaxTokens}}
	s.events <- &brtypes.ConverseStreamOutputMemberMetadata{Value: brtypes.ConverseStreamMetadataEvent{Usage: &brtypes.TokenUsage{InputTokens: aws.Int32(11), OutputTokens: aws.Int32(3), TotalTokens: aws.Int32(14)}}}
	close(s.events)
	var parts []string
	out, err := readConverseStream(t.Context(), s, func(text string) error { parts = append(parts, text); return nil })
	require.NoError(t, err)
	require.True(t, s.closed)
	require.Equal(t, []string{"Archive ", "evidence"}, parts)
	require.Equal(t, strings.Join(parts, ""), ExtractText(out))
	require.Equal(t, brtypes.StopReasonMaxTokens, out.StopReason)
	require.EqualValues(t, 14, *out.Usage.TotalTokens)
}

func TestReadConverseStreamFailuresCloseReader(t *testing.T) {
	failed := errors.New("stream failed")
	for _, name := range []string{"upstream", "callback", "canceled", "incomplete"} {
		t.Run(name, func(t *testing.T) {
			s := &streamReader{events: make(chan brtypes.ConverseStreamOutput, 1)}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			fn := func(string) error { return nil }
			switch name {
			case "upstream":
				s.err = failed
				close(s.events)
			case "callback":
				s.events <- textDelta("partial")
				fn = func(string) error { return failed }
			case "canceled":
				cancel()
			case "incomplete":
				close(s.events)
			}
			_, err := readConverseStream(ctx, s, fn)
			require.Error(t, err)
			if name == "upstream" || name == "callback" {
				require.ErrorIs(t, err, failed)
			}
			if name == "canceled" {
				require.ErrorIs(t, err, context.Canceled)
			}
			require.True(t, s.closed)
		})
	}
}

func TestFakeAndAmbassadorStream(t *testing.T) {
	fake := NewFakeConverser()
	var _ StreamConverser = fake
	amb := NewAmbassador(nil)
	amb.SetClient(fake)
	messages := []brtypes.Message{{Role: brtypes.ConversationRoleUser, Content: []brtypes.ContentBlock{&brtypes.ContentBlockMemberText{Value: "Archive evidence"}}}}
	var parts []string
	out, err := amb.ConverseStreamRoutedPlain(t.Context(), "offline", "", messages, .2, 100, func(text string) error { parts = append(parts, text); return nil })
	require.NoError(t, err)
	require.Greater(t, len(parts), 2)
	require.Equal(t, out.Text, strings.Join(parts, ""))
	require.Equal(t, brtypes.StopReasonEndTurn, out.Output.StopReason)
	require.Positive(t, *out.Output.Usage.TotalTokens)
	failed := errors.New("stop reading")
	_, err = fake.ConverseStreamWithModelAndSystem(t.Context(), "offline", "", messages, .2, 100, func(string) error { return failed })
	require.ErrorIs(t, err, failed)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = fake.ConverseStreamWithModelAndSystem(ctx, "offline", "", messages, .2, 100, func(string) error { t.Error("canceled callback"); return nil })
	require.ErrorIs(t, err, context.Canceled)
	_, err = amb.ConverseStreamRoutedPlain(t.Context(), "offline", "", messages, .2, 100, nil)
	require.Error(t, err)
}
