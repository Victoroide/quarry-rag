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
}

// ValidAt checks the credential deadline without admission clock skew. Call it
// immediately before any evidence disclosure during a long-running request.
func (s VerifiedSubject) ValidAt(now time.Time) bool {
	return !s.ExpiresAt.IsZero() && now.Before(s.ExpiresAt)
}

type subjectContextKey struct{}

// SubjectFromContext returns the authenticated principal, when present.
func SubjectFromContext(ctx context.Context) (VerifiedSubject, bool) {
	subject, ok := ctx.Value(subjectContextKey{}).(VerifiedSubject)
	return subject, ok
}

// RequireSubject guards a handler with independent service and delegated subject
// credentials. Only handlers that enforce the associated grants may use it.
func RequireSubject(tokens []string, verifier *subjectauth.Verifier, resolver SubjectResolver, next http.HandlerFunc) http.HandlerFunc {
	digests := serviceTokenDigests(tokens)
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "private, no-store")
		if len(digests) == 0 || verifier == nil || resolver == nil {
			subjectUnavailable(w)
			return
		}
		service := r.Header.Values("X-Service-Token")
		token := bearerToken(r.Header.Values("Authorization"))
		if len(service) != 1 || strings.Contains(service[0], ",") || !matchesServiceToken(service[0], digests) || token == "" {
			subjectUnauthorized(w)
			return
		}
		identity, err := verifier.Verify(token)
		if errors.Is(err, subjectauth.ErrAuthorityUnavailable) {
			subjectUnavailable(w)
			return
		}
		if err != nil || !time.Now().Before(identity.ExpiresAt) {
			subjectUnauthorized(w)
			return
		}
		ctx, cancel := context.WithDeadline(r.Context(), identity.ExpiresAt)
		defer cancel()
		principal, found, err := resolver.ResolvePrincipal(ctx, identity.Issuer, identity.Subject)
		subject := VerifiedSubject{Principal: principal, ExpiresAt: identity.ExpiresAt}
		if !subject.ValidAt(time.Now()) {
			subjectUnauthorized(w)
			return
		}
		if err != nil {
			subjectUnavailable(w)
			return
		}
		if !found {
			writeJSONErr(w, http.StatusForbidden, "forbidden", "access denied")
			return
		}
		verified := r.Clone(context.WithValue(ctx, subjectContextKey{}, subject))
		verified.Header.Del("Authorization")
		verified.Header.Del("X-Service-Token")
		err = verifier.CheckTrust(identity)
		if errors.Is(err, subjectauth.ErrAuthorityUnavailable) {
			subjectUnavailable(w)
			return
		}
		if err != nil || !subject.ValidAt(time.Now()) {
			subjectUnauthorized(w)
			return
		}
		next(w, verified)
	}
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
