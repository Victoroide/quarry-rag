package tracing

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type unsafeTelemetryError struct{}

func (*unsafeTelemetryError) Error() string          { panic("rendered original Error") }
func (*unsafeTelemetryError) Format(fmt.State, rune) { panic("rendered original formatter") }
func (*unsafeTelemetryError) As(any) bool            { panic("classified with As") }
func (*unsafeTelemetryError) LogValue() slog.Value   { panic("rendered original LogValuer") }

func TestTelemetryErrorRedaction(t *testing.T) {
	const canary = "foreign-chunk-key"
	serviceCtx := t.Context()
	ctx := WithRedactIdentifiers(serviceCtx, true)
	recorder := recordSpans(t)
	require.Nil(t, ErrorForTelemetry(ctx, nil))
	for _, tc := range []struct {
		name string
		err  error
		want string
	}{
		{"opaque", &unsafeTelemetryError{}, "operation failed"},
		{"provider", errors.New(canary + "\n" + strings.Repeat("内容", 65536)), "operation failed"},
		{"postgres", &pgconn.PgError{Message: canary, Detail: canary}, "operation failed"},
		{"wrapped cancel", fmt.Errorf("provider %s: %w", canary, context.Canceled), "operation canceled"},
		{"joined deadline wins", errors.Join(context.Canceled, fmt.Errorf("%s: %w", canary, context.DeadlineExceeded)), "operation timed out"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Same(t, tc.err, ErrorForTelemetry(serviceCtx, tc.err))
			safe := ErrorForTelemetry(ctx, tc.err)
			require.Equal(t, tc.want, safe.Error())
			var logs strings.Builder
			slog.New(slog.NewJSONHandler(&logs, nil)).ErrorContext(ctx, "fixture", "error", safe)
			require.Contains(t, logs.String(), `"error":"`+tc.want+`"`)
			spanCtx, span := start(ctx, "fixture", "", trace.SpanKindInternal)
			RecordError(spanCtx, span, tc.err)
			span.End()
			ended := recorder.Ended()
			result := ended[len(ended)-1]
			require.Equal(t, codes.Error, result.Status().Code)
			require.Equal(t, tc.want, result.Status().Description)
			require.Len(t, result.Events(), 1)
			event := result.Events()[0]
			require.Equal(t, "exception", event.Name)
			attrs := make(map[string]string)
			for _, attr := range event.Attributes {
				attrs[string(attr.Key)] = attr.Value.AsString()
			}
			require.Equal(t, map[string]string{
				"exception.message": tc.want, "exception.type": "*errors.errorString",
			}, attrs)
		})
	}
}
