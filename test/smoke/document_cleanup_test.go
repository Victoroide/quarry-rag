package smoke

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	qclient "github.com/Victoroide/quarry-rag/client"
	"github.com/google/uuid"
)

// cleanupRetryDelay spaces repeated attempts of one cleanup step.
var cleanupRetryDelay = 500 * time.Millisecond

// smokeService is the part of the service client the smoke helpers use. Every
// call goes to the service under test over its authenticated HTTP API.
type smokeService interface {
	GetDeletionMode(ctx context.Context, opts ...qclient.RequestOption) (*qclient.DeletionModeResponse, error)
	GetDocumentWithIncarnation(ctx context.Context, id string) (*qclient.DocumentIncarnationSummary, error)
	DeleteDocumentIncarnation(ctx context.Context, id, incarnation string) (*qclient.DeleteResponse, error)
}

var _ smokeService = (*qclient.Client)(nil)

// errTargetInactive reports a target whose database has not activated
// checkpointed deletion.
var errTargetInactive = errors.New("the target service has not activated checkpointed deletion, so smoke tests that write documents cannot clean up after themselves; activate it with the administrative command first (see the staging procedure in the deployment guide)")

// checkActivatedTarget reads the mode recorded in the target service's own
// database. It writes nothing and treats an unreadable or unknown mode as inactive.
func checkActivatedTarget(ctx context.Context, api smokeService) error {
	mode, err := api.GetDeletionMode(ctx)
	if err != nil {
		return fmt.Errorf("read the deletion mode of the target service: %w", err)
	}
	if !mode.Active() {
		return errTargetInactive
	}
	return nil
}

// transientFailure reports an outcome that repeating the identical request can
// resolve: a transport failure, a busy or unavailable service or a rate limit.
func transientFailure(err error) bool {
	var apiErr *qclient.APIError
	if errors.As(err, &apiErr) {
		return apiErr.StatusCode >= http.StatusInternalServerError || apiErr.StatusCode == http.StatusTooManyRequests
	}
	return !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded)
}

func pause(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func canonicalUUID(value string) (string, error) {
	parsed, err := uuid.Parse(value)
	if err != nil || len(value) != len(parsed.String()) {
		return "", fmt.Errorf("%q is not one canonical UUID", value)
	}
	return parsed.String(), nil
}

// readAdmittedIncarnation reads the incarnation of a document that the test's
// own request was just accepted for. It runs once per document. Only a
// transient failure repeats the read. An absent document is an error and never
// cleanup, so an ambiguous creation cannot lead to a destructive request.
func readAdmittedIncarnation(ctx context.Context, api smokeService, id string) (string, error) {
	id, err := canonicalUUID(id)
	if err != nil {
		return "", fmt.Errorf("document id: %w", err)
	}
	for {
		summary, readErr := api.GetDocumentWithIncarnation(ctx, id)
		if readErr == nil {
			incarnation, parseErr := canonicalUUID(summary.Incarnation)
			if parseErr != nil {
				return "", fmt.Errorf("incarnation of %s: %w", id, parseErr)
			}
			return incarnation, nil
		}
		if !transientFailure(readErr) {
			return "", fmt.Errorf("read the incarnation of %s: %w", id, readErr)
		}
		if waitErr := pause(ctx, cleanupRetryDelay); waitErr != nil {
			return "", fmt.Errorf("read the incarnation of %s: %w", id, errors.Join(waitErr, readErr))
		}
	}
}

// deleteSavedIncarnation removes the document lifetime the test admitted. Every
// attempt carries the incarnation saved at admission. It never reads a current
// token: a failure that repeating cannot resolve, including a different
// lifetime under the same identifier (412), ends the cleanup without touching
// anything else. A document that is already absent needs no deletion.
func deleteSavedIncarnation(ctx context.Context, api smokeService, id, incarnation string) error {
	for {
		_, err := api.DeleteDocumentIncarnation(ctx, id, incarnation)
		if err == nil {
			return nil
		}
		var apiErr *qclient.APIError
		if errors.As(err, &apiErr) && apiErr.StatusCode == http.StatusNotFound {
			return nil
		}
		if !transientFailure(err) {
			return err
		}
		delay := cleanupRetryDelay
		if apiErr != nil && apiErr.RetryAfter > 0 {
			delay = min(apiErr.RetryAfter, 5*time.Second)
		}
		if waitErr := pause(ctx, delay); waitErr != nil {
			return errors.Join(waitErr, err)
		}
	}
}
