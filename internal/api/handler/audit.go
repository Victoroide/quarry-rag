package handler

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/Victoroide/quarry-rag/internal/api/middleware"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/logctx"
)

func recordSecurityAudit(ctx context.Context, recorder domain.SecurityAuditRecorder, subject *middleware.VerifiedSubject, rev *int64, action, outcome, reason, docID, incarnation string) error {
	if recorder == nil {
		return nil
	}
	auditCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()
	event := domain.SecurityAuditEvent{
		RequestID:      logctx.RequestID(ctx),
		PolicyRevision: rev,
		Action:         action,
		Outcome:        outcome,
		Reason:         reason,
		DocumentID:     docID,
		Incarnation:    incarnation,
	}
	if subject != nil {
		event.TenantID = subject.Principal.TenantID
		event.PrincipalID = subject.Principal.ID
	}
	return recorder.RecordSecurityAudit(auditCtx, event)
}

func recordSecurityAuditBestEffort(ctx context.Context, recorder domain.SecurityAuditRecorder, subject *middleware.VerifiedSubject, rev *int64, action, outcome, reason, docID, incarnation string) {
	if err := recordSecurityAudit(ctx, recorder, subject, rev, action, outcome, reason, docID, incarnation); err != nil {
		slog.WarnContext(ctx, "record security audit event", "action", action, "outcome", outcome, "error", err)
	}
}

func requireAuditedAllow(ctx context.Context, w http.ResponseWriter, recorder domain.SecurityAuditRecorder, subject *middleware.VerifiedSubject, rev *int64, action, reason, docID, incarnation string) bool {
	if err := recordSecurityAudit(ctx, recorder, subject, rev, action, "allow", reason, docID, incarnation); err != nil {
		slog.ErrorContext(ctx, "record allow security audit event", "action", action, "error", err)
		writeJSONError(w, http.StatusServiceUnavailable, "unavailable", "audit logging unavailable")
		return false
	}
	return true
}

func statusAuditOutcome(status int) string {
	switch status {
	case http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound, http.StatusUnprocessableEntity:
		return "deny"
	default:
		return "error"
	}
}

func requireAuditedScopedSubject(w http.ResponseWriter, r *http.Request, now func() time.Time, recorder domain.SecurityAuditRecorder, action string) (middleware.VerifiedSubject, bool) {
	subject, ok := requireScopedSubject(w, r, now)
	if !ok {
		var auditSubject *middleware.VerifiedSubject
		if raw, exists := middleware.SubjectFromContext(r.Context()); exists {
			auditSubject = &raw
		}
		recordSecurityAuditBestEffort(r.Context(), recorder, auditSubject, nil, action, "deny", "credential_expired", "", "")
		return middleware.VerifiedSubject{}, false
	}
	return subject, true
}
