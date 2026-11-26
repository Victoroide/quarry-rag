package handler

import (
	"errors"
	"fmt"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/stretchr/testify/require"
)

func TestOwnedIngestAuditsEveryAcceptanceOutcome(t *testing.T) {
	type pair struct{ outcome, reason string }
	expected := map[domain.AcceptOutcome]*pair{
		domain.AcceptAccepted:            nil,
		domain.AcceptDuplicate:           nil,
		domain.AcceptInProgress:          nil,
		domain.AcceptRequeued:            nil,
		domain.AcceptConflict:            {"deny", "conflict"},
		domain.AcceptDeletionConflict:    {"deny", "conflict"},
		domain.AcceptNotFound:            {"deny", "not_found"},
		domain.AcceptForbidden:           {"deny", "access_denied"},
		domain.AcceptUnauthorized:        {"deny", "credential_expired"},
		domain.AcceptNeedsSource:         {"deny", "precondition_failed"},
		domain.AcceptOCRRetryRequired:    {"deny", "precondition_failed"},
		domain.AcceptNeedsPDFText:        {"deny", "precondition_failed"},
		domain.AcceptOCRRetryAfterExpiry: {"deny", "precondition_failed"},
		domain.AcceptUploadExpired:       {"deny", "precondition_failed"},
		domain.AcceptQueueFull:           {"error", "queue_full"},
		domain.AcceptFrozen:              {"error", "writers_frozen"},
	}
	// Every declared outcome must be listed, so a new one cannot fall into a
	// generic failure reason unnoticed.
	for outcome := domain.AcceptAccepted; outcome <= domain.AcceptFrozen; outcome++ {
		_, listed := expected[outcome]
		require.True(t, listed, "acceptance outcome %d has no expected audit event", outcome)
		_, _, _, known := acceptAudit(outcome)
		require.True(t, known, "acceptance outcome %d has no audit mapping", outcome)
	}
	for outcome, want := range expected {
		t.Run(fmt.Sprint(outcome), func(t *testing.T) {
			audit := &auditMock{}
			ingest := ownedIngest{opts: ingestOptions{audit: audit}}
			ingest.recordAcceptFailure(t.Context(), outcome, nil, "doc")
			if want == nil {
				require.Empty(t, audit.events, "the acceptance transaction already recorded this outcome")
				return
			}
			require.Len(t, audit.events, 1)
			require.Equal(t, "ingest", audit.events[0].Action)
			require.Equal(t, want.outcome, audit.events[0].Outcome)
			require.Equal(t, want.reason, audit.events[0].Reason)
		})
	}

	for name, tc := range map[string]struct {
		err          error
		want, reason string
	}{
		"store failure":      {errors.New("db down"), "error", "accept_failed"},
		"expired credential": {domain.ErrUnauthorized, "error", "accept_failed"},
		"mode mismatch":      {domain.ErrAuthzModeMismatch, "error", "accept_failed"},
	} {
		t.Run(name, func(t *testing.T) {
			audit := &auditMock{}
			ingest := ownedIngest{opts: ingestOptions{audit: audit}}
			// Handlers report AcceptConflict alongside every error.
			ingest.recordAcceptFailure(t.Context(), domain.AcceptConflict, tc.err, "doc")
			require.Len(t, audit.events, 1)
			require.Equal(t, tc.want, audit.events[0].Outcome)
			require.Equal(t, tc.reason, audit.events[0].Reason)
		})
	}
}
