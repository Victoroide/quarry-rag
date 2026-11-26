package handler

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
)

// Deletion modes reported by GET /deletion-mode.
const (
	DeletionModeInactive = "inactive"
	DeletionModeActive   = "active"
)

// deletionModeTimeout bounds the one database lookup of a mode check.
const deletionModeTimeout = 5 * time.Second

// DeletionModeResponse is the GET /deletion-mode response body.
type DeletionModeResponse struct {
	DeletionMode string `json:"deletion_mode"`
}

// DeletionModeHandler reports the deletion mode recorded in the service's own
// database. It only reads: no request, replica or caller can activate checkpointed
// deletion through it. A lookup that fails returns 503 and never an assumed mode.
func DeletionModeHandler(cs domain.ChunkStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		deleter, ok := cs.(domain.DocumentDeleter)
		if !ok {
			writeJSONError(w, http.StatusServiceUnavailable, "unavailable", "document deletion mode is unavailable")
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), deletionModeTimeout)
		defer cancel()
		active, err := deleter.DeletionActive(ctx)
		if err != nil {
			slog.ErrorContext(r.Context(), "read deletion mode", "error", err)
			writeJSONError(w, http.StatusServiceUnavailable, "unavailable", "document deletion mode is unavailable")
			return
		}
		mode := DeletionModeInactive
		if active {
			mode = DeletionModeActive
		}
		w.Header().Set("Content-Type", "application/json")
		if err = json.NewEncoder(w).Encode(DeletionModeResponse{DeletionMode: mode}); err != nil {
			slog.ErrorContext(r.Context(), "encode deletion mode response", "error", err)
		}
	}
}
