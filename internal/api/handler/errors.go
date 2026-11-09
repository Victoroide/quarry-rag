package handler

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/Victoroide/quarry-rag/internal/pipeline"
	"github.com/aws/smithy-go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var errQueryOverloaded = errors.New("query admission queue timed out")

func acquireQuerySlot(ctx context.Context, slots chan struct{}, timeout time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case slots <- struct{}{}:
		return nil
	default:
	}
	if timeout <= 0 {
		return errQueryOverloaded
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case slots <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return errQueryOverloaded
	}
}

func writeQueryError(w http.ResponseWriter, r *http.Request, err error) {
	slog.ErrorContext(r.Context(), "query processing failed", "error", err)
	code, retry, body := queryErrorResponse(err)
	if code == 0 {
		return
	}
	if retry != "" {
		w.Header().Set("Retry-After", retry)
	}
	writeJSONError(w, code, body.Error, body.Message)
}

func queryErrorResponse(err error) (code int, retry string, body APIError) {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return http.StatusGatewayTimeout, "", APIError{Error: "timeout", Message: "query processing timed out"}
	case errors.Is(err, context.Canceled):
		return 0, "", APIError{}
	case errors.Is(err, errQueryOverloaded):
		return http.StatusServiceUnavailable, "1", APIError{Error: "overloaded", Message: "too many concurrent queries"}
	case isUpstreamThrottled(err):
		return http.StatusServiceUnavailable, "2", APIError{Error: "upstream_throttled", Message: "upstream service is busy"}
	case errors.Is(err, pipeline.ErrSynthesisEmpty):
		return http.StatusBadGateway, "", APIError{Error: "synthesis", Message: "answer synthesis returned no text"}
	default:
		return http.StatusInternalServerError, "", APIError{Error: "pipeline", Message: "query processing failed"}
	}
}

func isUpstreamThrottled(err error) bool {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "ThrottlingException", "ServiceQuotaExceededException", "TooManyRequestsException":
			return true
		}
	}
	return status.Code(err) == codes.ResourceExhausted
}
