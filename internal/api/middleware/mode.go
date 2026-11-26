package middleware

import (
	"net/http"

	"github.com/Victoroide/quarry-rag/internal/domain"
)

// RequireAuthzMode refuses traffic whenever the corpus authorization mode
// recorded in PostgreSQL disagrees with expected or cannot be verified.
func RequireAuthzMode(expected string, reader domain.CorpusModeReader, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if expected == "grants" {
			w.Header().Set("Cache-Control", "private, no-store")
		}
		if reader == nil {
			if expected == "grants" {
				subjectUnavailable(w)
				return
			}
			next(w, r)
			return
		}
		actual, err := reader.CorpusAuthzMode(r.Context())
		if err != nil {
			w.Header().Set("Cache-Control", "private, no-store")
			subjectUnavailable(w)
			return
		}
		if actual != expected {
			w.Header().Set("Cache-Control", "private, no-store")
			writeJSONErr(w, http.StatusServiceUnavailable, "unavailable", "authorization mode mismatch")
			return
		}
		next(w, r)
	}
}
