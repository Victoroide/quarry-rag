package pipeline

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/subjectauth"
	"github.com/stretchr/testify/require"
)

type stateAuthority struct {
	domain.ReadAuthority
	state domain.ReadState
	err   error
	calls int
	onRun func()
}

func (a *stateAuthority) ReadState(context.Context, domain.Principal) (domain.ReadState, error) {
	a.calls++
	if a.onRun != nil {
		a.onRun()
	}
	return a.state, a.err
}

func authorizationAt(now *time.Time, expires time.Time, authority domain.ReadAuthority, revision int64) *ReadAuthorization {
	a := NewReadAuthorization(domain.Principal{ID: "principal", TenantID: "tenant"}, expires, authority, domain.AuthorizedScope{TenantID: "tenant", PrincipalID: "principal"}, revision)
	a.Now = func() time.Time { return *now }
	return a
}

func TestReadAuthorizationBoundsEveryDisclosureByTheExactExpiry(t *testing.T) {
	expires := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	for _, tc := range []struct {
		name string
		now  time.Time
		zero bool
		want error
	}{
		{"a second before", expires.Add(-time.Second), false, nil},
		{"a nanosecond before", expires.Add(-time.Nanosecond), false, nil},
		{"at expiry", expires, false, ErrCredentialExpired},
		{"after expiry", expires.Add(time.Nanosecond), false, ErrCredentialExpired},
		{"unset expiry", expires.Add(-time.Hour), true, ErrCredentialExpired},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := tc.now
			deadline := expires
			if tc.zero {
				deadline = time.Time{}
			}
			authority := &stateAuthority{state: domain.ReadState{Active: true, Revision: 3}}
			err := authorizationAt(&now, deadline, authority, 3).Check(t.Context())
			if tc.want == nil {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, tc.want)
			require.Zero(t, authority.calls, "an expired credential reads nothing")
		})
	}
}

func TestReadAuthorizationCredentialCanExpireWhileTheDatabaseAnswers(t *testing.T) {
	now := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	expires := now.Add(time.Second)
	authority := &stateAuthority{state: domain.ReadState{Active: true, Revision: 1}, onRun: func() { now = expires }}
	require.ErrorIs(t, authorizationAt(&now, expires, authority, 1).Check(t.Context()), ErrCredentialExpired)
}

func TestReadAuthorizationClassifiesStateFailures(t *testing.T) {
	now := time.Now()
	expires := now.Add(time.Hour)
	dbErr := errors.New("connection refused")
	for _, tc := range []struct {
		name      string
		authority *stateAuthority
		trust     func() error
		revision  int64
		want      error
	}{
		{"inactive principal", &stateAuthority{state: domain.ReadState{Revision: 5}}, nil, 5, ErrReadAuthorityChanged},
		{"database failure", &stateAuthority{err: dbErr}, nil, 5, ErrAuthorizationUnavailable},
		{"lagging replica", &stateAuthority{state: domain.ReadState{Active: true, Revision: 4}}, nil, 5, ErrAuthorizationUnavailable},
		{"newer revision is acceptable", &stateAuthority{state: domain.ReadState{Active: true, Revision: 6}}, nil, 5, nil},
		{"trust data unavailable", &stateAuthority{state: domain.ReadState{Active: true, Revision: 5}}, func() error { return subjectauth.ErrAuthorityUnavailable }, 5, ErrAuthorizationUnavailable},
		{"signing key no longer trusted", &stateAuthority{state: domain.ReadState{Active: true, Revision: 5}}, func() error { return subjectauth.ErrInvalidCredential }, 5, ErrCredentialExpired},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := authorizationAt(&now, expires, tc.authority, tc.revision)
			a.RecheckTrust = tc.trust
			err := a.Check(t.Context())
			if tc.want == nil {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, tc.want)
			}
			if errors.Is(tc.want, ErrAuthorizationUnavailable) && tc.authority.err != nil {
				require.ErrorIs(t, err, dbErr)
			}
		})
	}
}

func TestReadAuthorizationNeverMovesItsRevisionBackward(t *testing.T) {
	now := time.Now()
	authority := &stateAuthority{state: domain.ReadState{Active: true, Revision: 9}}
	a := authorizationAt(&now, now.Add(time.Hour), authority, 5)
	require.NoError(t, a.Check(t.Context()))
	authority.state.Revision = 8
	require.ErrorIs(t, a.Check(t.Context()), ErrAuthorizationUnavailable, "an older read after a newer one is a lagging replica")
	authority.state.Revision = 9
	require.NoError(t, a.Check(t.Context()))
}

func TestReadAuthorizationReportsCancellationWithoutNamingAnOutcome(t *testing.T) {
	now := time.Now()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	authority := &stateAuthority{state: domain.ReadState{Active: true}}
	require.ErrorIs(t, authorizationAt(&now, now.Add(time.Hour), authority, 0).Check(ctx), context.Canceled)
	require.Zero(t, authority.calls)
	canceling := &stateAuthority{err: context.Canceled}
	ctx, cancel = context.WithCancel(t.Context())
	canceling.onRun = cancel
	require.ErrorIs(t, authorizationAt(&now, now.Add(time.Hour), canceling, 0).Check(ctx), context.Canceled)
}

type scriptedChecks struct {
	err error
}

func (c scriptedChecks) VisibleEvidence(context.Context, []domain.ScoredChunk) ([]domain.ScoredChunk, error) {
	return nil, c.err
}

func (c scriptedChecks) VisibleEvidenceWithPreviews(context.Context, []domain.ScoredChunk, []string) ([]domain.ScoredChunk, map[string]string, error) {
	return nil, nil, c.err
}

func (c scriptedChecks) VisibleDocumentIDs(context.Context, []string) (map[string]struct{}, error) {
	return nil, c.err
}

func (c scriptedChecks) DocumentPreviews(context.Context, []string) (map[string]string, error) {
	return nil, c.err
}

func TestFailClosedChecksTurnStorageFailuresIntoUnavailableAuthority(t *testing.T) {
	dbErr := errors.New("connection refused")
	inner := scriptedChecks{err: dbErr}
	checks := FailClosedChecks(domain.SubjectEvidence{Visibility: inner, Documents: inner, Previews: inner})
	_, err := checks.Visibility.VisibleEvidence(t.Context(), nil)
	require.ErrorIs(t, err, ErrAuthorizationUnavailable)
	require.ErrorIs(t, err, dbErr)
	_, _, err = checks.Visibility.VisibleEvidenceWithPreviews(t.Context(), nil, nil)
	require.ErrorIs(t, err, ErrAuthorizationUnavailable)
	_, err = checks.Documents.VisibleDocumentIDs(t.Context(), nil)
	require.ErrorIs(t, err, ErrAuthorizationUnavailable)
	_, err = checks.Previews.DocumentPreviews(t.Context(), nil)
	require.ErrorIs(t, err, ErrAuthorizationUnavailable)

	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = checks.Previews.DocumentPreviews(canceled, nil)
	require.NotErrorIs(t, err, ErrAuthorizationUnavailable, "a canceled request is not a failed authority")

	ok := FailClosedChecks(domain.SubjectEvidence{Visibility: scriptedChecks{}, Documents: scriptedChecks{}, Previews: scriptedChecks{}})
	_, err = ok.Visibility.VisibleEvidence(t.Context(), nil)
	require.NoError(t, err)
	observer, isObserver := ok.Visibility.(domain.EvidenceIncarnationObserver)
	require.True(t, isObserver)
	incarnations, err := observer.ObserveUntaggedIncarnations(t.Context(), nil)
	require.NoError(t, err)
	require.Nil(t, incarnations, "a store without historical lifetimes has nothing to observe")
}

func TestAuthorizedHitsMustAgreeWithTheScope(t *testing.T) {
	scope := &domain.AuthorizedScope{TenantID: "00000000-0000-4000-8000-0000000000a0"}
	hit := func(doc, tenant, revision string) domain.ScoredChunk {
		metadata := map[string]any{}
		if tenant != "" {
			metadata[domain.VectorTenantKey] = tenant
		}
		if revision != "" {
			metadata[domain.VectorRevisionKey] = revision
		}
		return domain.ScoredChunk{Chunk: domain.Chunk{ID: domain.ChunkID(doc + tenant + revision), DocumentID: domain.DocumentID(doc), Metadata: metadata}}
	}
	const doc, other = "00000000-0000-4000-8000-000000000001", "00000000-0000-4000-8000-000000000002"
	hits := []domain.ScoredChunk{
		hit(doc, scope.TenantID, "inc"), hit(doc, "00000000-0000-4000-8000-0000000000b0", "inc"), hit(doc, scope.TenantID, "stale"),
		hit(other, scope.TenantID, "inc"), hit(doc, "", "inc"), hit(doc, "", ""),
	}
	// Relation stores carry no membership: only a contradicting tenant label rejects a hit.
	require.Len(t, filterAuthorizedHits(scope, hits), 5)
	scope.Incarnations = map[string]string{doc: "inc"}
	kept := filterAuthorizedHits(scope, hits)
	require.Len(t, kept, 2, "a membership store requires its own incarnation tag on a listed document")
	for _, sc := range kept {
		require.Equal(t, domain.DocumentID(doc), sc.Chunk.DocumentID)
		require.Equal(t, "inc", sc.Chunk.Metadata[domain.VectorRevisionKey])
	}
}

func TestVectorFilterCarriesTheAuthorizedScopeOnlyUnderAuthorization(t *testing.T) {
	qc := &QueryContext{Collection: "archive", AllowedDocumentIDs: []string{"doc"}}
	require.Nil(t, qc.vectorFilter().Authorized)
	now := time.Now()
	qc.Authorization = authorizationAt(&now, now.Add(time.Hour), &stateAuthority{}, 0)
	qc.Authorization.Scope.Incarnations = map[string]string{"doc": "inc"}
	filter := qc.vectorFilter()
	require.Equal(t, "tenant", filter.Authorized.TenantID)
	require.Equal(t, map[string]string{"doc": "inc"}, filter.Authorized.Incarnations)
	require.True(t, qc.checksCurrentEvidence(), "authorization alone requires current-evidence checks")
}

func TestEvidenceScopeCheckBoundsAResponseWithoutEvidence(t *testing.T) {
	now := time.Now()
	authority := &stateAuthority{state: domain.ReadState{Active: true, Revision: 1}}
	qc := &QueryContext{Authorization: authorizationAt(&now, now.Add(time.Hour), authority, 1)}
	require.NoError(t, qc.CheckEvidenceScope(t.Context()))
	require.Equal(t, 1, authority.calls, "a no-evidence response still checks the principal")
	authority.state.Active = false
	require.ErrorIs(t, qc.CheckEvidenceScope(t.Context()), ErrReadAuthorityChanged)

	service := &QueryContext{}
	require.NoError(t, service.CheckEvidenceScope(t.Context()))
	require.Equal(t, 2, authority.calls, "service mode reads no authority")
}

func TestAuthorizedEvidenceFailuresNameTheirConsequence(t *testing.T) {
	now := time.Now()
	qc := &QueryContext{Authorization: authorizationAt(&now, now.Add(time.Hour), &stateAuthority{}, 0)}
	dbErr := errors.New("boom")
	require.ErrorIs(t, qc.authorizeEvidenceError(t.Context(), ErrEvidenceScopeChanged), ErrReadAuthorityChanged)
	require.ErrorIs(t, qc.authorizeEvidenceError(t.Context(), dbErr), ErrAuthorizationUnavailable)
	require.ErrorIs(t, qc.authorizeEvidenceError(t.Context(), context.DeadlineExceeded), context.DeadlineExceeded)
	require.NoError(t, qc.authorizeEvidenceError(t.Context(), nil))
	service := &QueryContext{}
	require.ErrorIs(t, service.authorizeEvidenceError(t.Context(), ErrEvidenceScopeChanged), ErrEvidenceScopeChanged,
		"service mode keeps its existing error")
}
