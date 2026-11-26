package pipeline

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/stretchr/testify/require"
)

type setAuthority struct {
	stateAuthority
	live      bool
	setState  domain.ReadState
	setErr    error
	setCalls  int
	lastCheck domain.DocumentSetRef
}

func (a *setAuthority) DocumentSetState(_ context.Context, _ domain.Principal, set domain.DocumentSetRef) (domain.ReadState, bool, error) {
	a.setCalls++
	a.lastCheck = set
	return a.setState, a.live, a.setErr
}

func (a *setAuthority) EvidenceInSet(domain.Principal, domain.DocumentSetRef, string, string) domain.SubjectEvidence {
	return domain.SubjectEvidence{}
}

func setAuthorization(now *time.Time, authority domain.ReadAuthority, expires time.Time) *ReadAuthorization {
	a := authorizationAt(now, now.Add(time.Hour), authority, 3)
	a.DocumentSet = &domain.DocumentSetRef{ID: "set", Revision: "revision", ExpiresAt: expires}
	return a
}

func TestReadAuthorizationChecksTheSetWithThePrincipalInOneRead(t *testing.T) {
	now := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	active := domain.ReadState{Active: true, Revision: 3}
	for _, tc := range []struct {
		name      string
		authority *setAuthority
		expires   time.Time
		want      error
	}{
		{"live set", &setAuthority{setState: active, live: true}, now.Add(time.Minute), nil},
		{"set no longer live", &setAuthority{setState: active, live: false}, now.Add(time.Minute), domain.ErrDocumentSetNotFound},
		{"set expired by the local clock", &setAuthority{setState: active, live: true}, now, domain.ErrDocumentSetNotFound},
		{"set expired a nanosecond ago", &setAuthority{setState: active, live: true}, now.Add(-time.Nanosecond), domain.ErrDocumentSetNotFound},
		{"principal deactivated", &setAuthority{setState: domain.ReadState{Active: false, Revision: 3}, live: true}, now.Add(time.Minute), ErrReadAuthorityChanged},
		{"database unavailable", &setAuthority{setErr: errors.New("down")}, now.Add(time.Minute), ErrAuthorizationUnavailable},
		{"policy revision regressed", &setAuthority{setState: domain.ReadState{Active: true, Revision: 2}, live: true}, now.Add(time.Minute), ErrAuthorizationUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			at := now
			err := setAuthorization(&at, tc.authority, tc.expires).Check(t.Context())
			if tc.want == nil {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, tc.want)
			}
			require.Equal(t, 1, tc.authority.setCalls, "one statement answers both questions")
			require.Zero(t, tc.authority.calls, "the principal-only read is not used for a set query")
			require.Equal(t, "revision", tc.authority.lastCheck.Revision)
		})
	}
}

func TestReadAuthorizationWithoutASetKeepsItsPrincipalOnlyRead(t *testing.T) {
	now := time.Now()
	authority := &setAuthority{stateAuthority: stateAuthority{state: domain.ReadState{Active: true, Revision: 3}}}
	require.NoError(t, authorizationAt(&now, now.Add(time.Hour), authority, 3).Check(t.Context()))
	require.Equal(t, 1, authority.calls)
	require.Zero(t, authority.setCalls)
}

func TestAuthorityThatCannotCheckSetsIsUnavailableNeverSkipped(t *testing.T) {
	now := time.Now()
	authority := &stateAuthority{state: domain.ReadState{Active: true, Revision: 3}}
	a := authorizationAt(&now, now.Add(time.Hour), authority, 3)
	a.DocumentSet = &domain.DocumentSetRef{ID: "set", Revision: "revision", ExpiresAt: now.Add(time.Hour)}
	require.ErrorIs(t, a.Check(t.Context()), ErrAuthorizationUnavailable)
	require.Zero(t, authority.calls)
}

func TestSetExpiryAnswersTheSameErrorEverywhere(t *testing.T) {
	qc := &QueryContext{}
	require.NoError(t, qc.authorizeEvidenceError(t.Context(), nil))
	qc.Authorization = &ReadAuthorization{}
	require.ErrorIs(t, qc.authorizeEvidenceError(t.Context(), domain.ErrDocumentSetNotFound), domain.ErrDocumentSetNotFound,
		"an expired set is not rewritten into an unavailable authority")
}
