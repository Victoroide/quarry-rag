package pipeline

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/subjectauth"
)

var (
	// ErrReadAuthorityChanged indicates that the principal, a document it read
	// or a preview that shaped routing no longer satisfies the decision the
	// request was admitted under. Nothing derived from that decision is released.
	ErrReadAuthorityChanged = errors.New("read authority changed during query")
	// ErrCredentialExpired indicates that the subject credential reached its
	// exact expiry. No later disclosure may start.
	ErrCredentialExpired = errors.New("subject credential expired during query")
	// ErrAuthorizationUnavailable indicates that authority could not be
	// established. The request fails; it never continues unscoped.
	ErrAuthorizationUnavailable = errors.New("read authorization unavailable")
)

// ReadAuthorization binds one query to a verified principal. Every disclosure
// boundary calls Check, so a decision never outlives its immediately preceding check.
type ReadAuthorization struct {
	Principal domain.Principal
	// ExpiresAt is the credential's exact expiry, without clock skew.
	ExpiresAt time.Time
	Authority domain.ReadAuthority
	// Scope is attached to every vector search. A relation store joins the live
	// readable relation. A membership store receives its incarnation map.
	Scope domain.AuthorizedScope
	// RecheckTrust confirms the signing key is still trusted. It may be nil.
	RecheckTrust func() error
	// Now supplies the clock. Nil means time.Now.
	Now func() time.Time

	mu       sync.Mutex
	revision int64
}

// NewReadAuthorization records the policy revision the scope was derived at.
func NewReadAuthorization(principal domain.Principal, expiresAt time.Time, authority domain.ReadAuthority, scope domain.AuthorizedScope, revision int64) *ReadAuthorization {
	return &ReadAuthorization{Principal: principal, ExpiresAt: expiresAt, Authority: authority, Scope: scope, revision: revision}
}

func (a *ReadAuthorization) now() time.Time {
	if a.Now != nil {
		return a.Now()
	}
	return time.Now()
}

// Expired reports whether the credential has reached its exact expiry.
func (a *ReadAuthorization) Expired() bool { return a.expired() }

func (a *ReadAuthorization) expired() bool {
	return a.ExpiresAt.IsZero() || !a.now().Before(a.ExpiresAt)
}

// Check verifies the credential lifetime and key trust, then reads the
// principal's standing and the policy revision from the primary database.
// A revision lower than one already observed means a lagging read, which is
// treated as unavailable authority rather than as a grant.
func (a *ReadAuthorization) Check(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if a.expired() {
		return ErrCredentialExpired
	}
	if a.RecheckTrust != nil {
		if err := a.RecheckTrust(); err != nil {
			if errors.Is(err, subjectauth.ErrAuthorityUnavailable) {
				return fmt.Errorf("%w: trust data", ErrAuthorizationUnavailable)
			}
			return ErrCredentialExpired
		}
	}
	state, err := a.Authority.ReadState(ctx, a.Principal)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return fmt.Errorf("%w: %w", ErrAuthorizationUnavailable, err)
	}
	if !state.Active {
		return ErrReadAuthorityChanged
	}
	a.mu.Lock()
	stale := state.Revision < a.revision
	if !stale {
		a.revision = state.Revision
	}
	a.mu.Unlock()
	if stale {
		return fmt.Errorf("%w: policy revision regressed", ErrAuthorizationUnavailable)
	}
	if a.expired() {
		return ErrCredentialExpired
	}
	return ctx.Err()
}

// FailClosedChecks makes every storage failure of a principal's evidence,
// visibility and preview checks report unavailable authority. A failed read
// then ends the request instead of leaving a path that could widen the scope.
func FailClosedChecks(checks domain.SubjectEvidence) domain.SubjectEvidence {
	wrapped := failClosedChecks{evidence: checks.Visibility, documents: checks.Documents, previews: checks.Previews}
	return domain.SubjectEvidence{Visibility: wrapped, Documents: wrapped, Previews: wrapped}
}

type failClosedChecks struct {
	evidence  domain.EvidenceVisibility
	documents domain.DocumentVisibility
	previews  domain.DocumentPreviewer
}

func unavailable(ctx context.Context, err error) error {
	if err == nil || ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return fmt.Errorf("%w: %w", ErrAuthorizationUnavailable, err)
}

func (f failClosedChecks) VisibleEvidence(ctx context.Context, candidates []domain.ScoredChunk) ([]domain.ScoredChunk, error) {
	visible, err := f.evidence.VisibleEvidence(ctx, candidates)
	return visible, unavailable(ctx, err)
}

func (f failClosedChecks) VisibleEvidenceWithPreviews(ctx context.Context, candidates []domain.ScoredChunk, probeIDs []string) ([]domain.ScoredChunk, map[string]string, error) {
	visible, previews, err := f.evidence.VisibleEvidenceWithPreviews(ctx, candidates, probeIDs)
	return visible, previews, unavailable(ctx, err)
}

func (f failClosedChecks) VisibleDocumentIDs(ctx context.Context, ids []string) (map[string]struct{}, error) {
	visible, err := f.documents.VisibleDocumentIDs(ctx, ids)
	return visible, unavailable(ctx, err)
}

func (f failClosedChecks) DocumentPreviews(ctx context.Context, ids []string) (map[string]string, error) {
	previews, err := f.previews.DocumentPreviews(ctx, ids)
	return previews, unavailable(ctx, err)
}

// ObserveUntaggedIncarnations forwards to a store that observes historical
// lifetimes. Without one there is nothing to observe, which a nil result expresses.
func (f failClosedChecks) ObserveUntaggedIncarnations(ctx context.Context, allowedIDs []string) (map[string]string, error) {
	observer, ok := f.evidence.(domain.EvidenceIncarnationObserver)
	if !ok {
		return nil, nil
	}
	incarnations, err := observer.ObserveUntaggedIncarnations(ctx, allowedIDs)
	return incarnations, unavailable(ctx, err)
}
