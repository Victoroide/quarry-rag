package middleware

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/subjectauth"
)

// SubjectResolver resolves a verified external identity in the active catalog.
type SubjectResolver interface {
	ResolvePrincipal(ctx context.Context, issuer, subject string) (domain.Principal, bool, error)
}

// VerifiedSubject is the request principal and the credential's exact deadline.
type VerifiedSubject struct {
	Principal domain.Principal
	ExpiresAt time.Time
	// RecheckTrust, when set, confirms the signing key is still trusted. A
	// long-running request calls it at every disclosure boundary. The error is
	// subjectauth.ErrAuthorityUnavailable when trust data cannot be used.
	RecheckTrust func() error
}

// ValidAt checks the credential deadline without admission clock skew. Call it
// immediately before any evidence disclosure during a long-running request.
func (s VerifiedSubject) ValidAt(now time.Time) bool {
	return !s.ExpiresAt.IsZero() && now.Before(s.ExpiresAt)
}

type subjectContextKey struct{}

// ContextWithSubject carries a principal that a gate has already verified.
// Only code that performed the verification may call it.
func ContextWithSubject(ctx context.Context, subject VerifiedSubject) context.Context {
	return context.WithValue(ctx, subjectContextKey{}, subject)
}

// SubjectFromContext returns the authenticated principal, when present.
func SubjectFromContext(ctx context.Context) (VerifiedSubject, bool) {
	subject, ok := ctx.Value(subjectContextKey{}).(VerifiedSubject)
	return subject, ok
}

// RequireSubject guards a handler with independent service and delegated subject
// credentials. Only handlers that enforce the associated grants may use it.
func RequireSubject(tokens []string, verifier *subjectauth.Verifier, resolver SubjectResolver, next http.HandlerFunc) http.HandlerFunc {
	return RequireSubjectWithAudit(tokens, verifier, resolver, nil, next)
}

// RequireSubjectWithAudit guards a handler with independent service and delegated
// subject credentials and records credential and principal denials in auditor.
func RequireSubjectWithAudit(tokens []string, verifier *subjectauth.Verifier, resolver SubjectResolver, auditor domain.SecurityAuditRecorder, next http.HandlerFunc) http.HandlerFunc {
	digests := serviceTokenDigests(tokens)
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "private, no-store")
		// Nothing touches the authority or the audit trail before the service
		// token matches, so anonymous traffic costs no database round trip.
		if len(digests) == 0 {
			subjectUnavailable(w)
			return
		}
		service := r.Header.Values("X-Service-Token")
		if len(service) != 1 || strings.Contains(service[0], ",") || !matchesServiceToken(service[0], digests) {
			subjectUnauthorized(w)
			return
		}
		if verifier == nil || resolver == nil {
			recordSubjectAudit(r.Context(), auditor, "", "", "error", "authority_unavailable")
			subjectUnavailable(w)
			return
		}
		token := bearerToken(r.Header.Values("Authorization"))
		if token == "" {
			recordSubjectAudit(r.Context(), auditor, "", "", "deny", "missing_subject_credential")
			subjectUnauthorized(w)
			return
		}
		identity, err := verifier.Verify(token)
		if errors.Is(err, subjectauth.ErrAuthorityUnavailable) {
			recordSubjectAudit(r.Context(), auditor, "", "", "error", "verifier_unavailable")
			subjectUnavailable(w)
			return
		}
		if err != nil || !time.Now().Before(identity.ExpiresAt) {
			recordSubjectAudit(r.Context(), auditor, "", "", "deny", "invalid_or_expired_token")
			subjectUnauthorized(w)
			return
		}
		ctx, cancel := context.WithDeadline(r.Context(), identity.ExpiresAt)
		defer cancel()
		principal, found, err := resolver.ResolvePrincipal(ctx, identity.Issuer, identity.Subject)
		subject := VerifiedSubject{Principal: principal, ExpiresAt: identity.ExpiresAt,
			RecheckTrust: func() error { return verifier.CheckTrust(identity) }}
		if !subject.ValidAt(time.Now()) {
			recordSubjectAudit(r.Context(), auditor, principal.TenantID, principal.ID, "deny", "credential_expired")
			subjectUnauthorized(w)
			return
		}
		if err != nil {
			recordSubjectAudit(r.Context(), auditor, "", "", "error", "principal_lookup_unavailable")
			subjectUnavailable(w)
			return
		}
		if !found {
			recordSubjectAudit(r.Context(), auditor, "", "", "deny", "principal_missing_or_disabled")
			writeJSONErr(w, http.StatusForbidden, "forbidden", "access denied")
			return
		}
		verified := r.Clone(ContextWithSubject(ctx, subject))
		verified.Header.Del("Authorization")
		verified.Header.Del("X-Service-Token")
		err = verifier.CheckTrust(identity)
		if errors.Is(err, subjectauth.ErrAuthorityUnavailable) {
			recordSubjectAudit(r.Context(), auditor, principal.TenantID, principal.ID, "error", "trust_check_unavailable")
			subjectUnavailable(w)
			return
		}
		if err != nil || !subject.ValidAt(time.Now()) {
			recordSubjectAudit(r.Context(), auditor, principal.TenantID, principal.ID, "deny", "trust_check_failed")
			subjectUnauthorized(w)
			return
		}
		next(w, verified)
	}
}

func recordSubjectAudit(ctx context.Context, auditor domain.SecurityAuditRecorder, tenantID, principalID, outcome, reason string) {
	if auditor == nil {
		return
	}
	auditCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()
	_ = auditor.RecordSecurityAudit(auditCtx, domain.SecurityAuditEvent{
		TenantID:    tenantID,
		PrincipalID: principalID,
		Action:      "subject_auth",
		Outcome:     outcome,
		Reason:      reason,
	})
}

func bearerToken(values []string) string {
	if len(values) != 1 {
		return ""
	}
	scheme, token, ok := strings.Cut(values[0], " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") {
		return ""
	}
	return strings.TrimLeft(token, " ")
}

func subjectUnauthorized(w http.ResponseWriter) {
	writeJSONErr(w, http.StatusUnauthorized, "unauthorized", "invalid or missing credentials")
}

func subjectUnavailable(w http.ResponseWriter) {
	writeJSONErr(w, http.StatusServiceUnavailable, "unavailable", "authorization unavailable")
}
