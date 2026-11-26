// Package handler contains HTTP handler functions that translate between
// the transport layer and domain logic. No business logic belongs here.
package handler

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/Victoroide/quarry-rag/internal/tracing"
)

// HealthStatus represents the health check response body.
type HealthStatus struct {
	Status   string `json:"status"`
	Pinecone string `json:"pinecone"`
	Postgres string `json:"postgres"`
	Neon     string `json:"neon"`
	S3       string `json:"s3"`
}

// HealthHandler returns an HTTP handler for GET /health.
func HealthHandler(p *Probes) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		status := p.Snapshot(r.Context())

		w.Header().Set("Content-Type", "application/json")
		if status.Status == "ok" {
			w.WriteHeader(http.StatusOK)
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		if err := json.NewEncoder(w).Encode(status); err != nil {
			slog.ErrorContext(r.Context(), "encode health response", "error", tracing.ErrorForTelemetry(r.Context(), err))
		}
	}
}

// LivenessHandler reports process availability without probing dependencies.
func LivenessHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		writeProbeStatus(r.Context(), w, http.StatusOK, "ok")
	}
}

// ReadinessHandler reports database availability and shutdown readiness.
func ReadinessHandler(p *Probes) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if p.Draining() {
			writeProbeStatus(r.Context(), w, http.StatusServiceUnavailable, "draining")
			return
		}
		status := p.Snapshot(r.Context())
		switch {
		case p.Draining():
			writeProbeStatus(r.Context(), w, http.StatusServiceUnavailable, "draining")
		case status.Postgres != "ok":
			writeProbeStatus(r.Context(), w, http.StatusServiceUnavailable, "unavailable")
		case !p.authzReady(r.Context()):
			writeProbeStatus(r.Context(), w, http.StatusServiceUnavailable, "unavailable")
		default:
			writeProbeStatus(r.Context(), w, http.StatusOK, "ready")
		}
	}
}

func writeProbeStatus(ctx context.Context, w http.ResponseWriter, code int, status string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(struct {
		Status string `json:"status"`
	}{Status: status}); err != nil {
		slog.ErrorContext(ctx, "encode probe response", "error", tracing.ErrorForTelemetry(ctx, err))
	}
}
