package domain

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestVectorOutcomeRecordsOnlyAttemptsThatMayHaveExecuted(t *testing.T) {
	ctx, outcome := ObserveVectorOutcome(t.Context())
	require.True(t, ObservingVectorOutcome(ctx))
	require.False(t, ObservingVectorOutcome(t.Context()))
	require.False(t, outcome.Uncertain())

	NoteVectorAttempt(ctx, nil)
	NoteVectorAttempt(ctx, RejectedVectorMutation(errors.New("refused before any send")))
	require.False(t, outcome.Uncertain(), "a success and a definite rejection leave nothing outstanding")

	NoteVectorAttempt(ctx, context.DeadlineExceeded)
	require.True(t, outcome.Uncertain())
	// A later success does not clear what an earlier attempt may have left.
	NoteVectorAttempt(ctx, nil)
	require.True(t, outcome.Uncertain())
}

func TestVectorOutcomeWithoutObserverIsInert(t *testing.T) {
	require.NotPanics(t, func() { NoteVectorAttempt(t.Context(), context.DeadlineExceeded) })
}

func TestVectorOutcomesAreIndependent(t *testing.T) {
	first, firstOutcome := ObserveVectorOutcome(t.Context())
	_, secondOutcome := ObserveVectorOutcome(t.Context())
	NoteVectorAttempt(first, errors.New("unknown"))
	require.True(t, firstOutcome.Uncertain())
	require.False(t, secondOutcome.Uncertain())
}
