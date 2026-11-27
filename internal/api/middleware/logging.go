// Package middleware provides HTTP middleware for logging and recovery.
package middleware

import (
	"log/slog"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
)

// Logging wraps an http.Handler and logs each request's method, path,
// status code, bytes written, duration, and remote address.
func Logging(next http.Handler) http.Handler {
	return loggingHandler(next, false)
}

// LoggingWithRouteTemplates wraps an http.Handler and logs the matched route
// template (or "unmatched") in place of the raw request path so path
// identifiers never reach ordinary access logs.
func LoggingWithRouteTemplates(next http.Handler) http.Handler {
	return loggingHandler(next, true)
}

func loggingHandler(next http.Handler, routeTemplates bool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rw := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}
		var evidenceFailed atomic.Bool
		ctx := domain.WithWriterEvidenceFailureObserver(r.Context(), func() { evidenceFailed.Store(true) })
		rt := r.WithContext(ctx)
		next.ServeHTTP(rw, rt)
		r.Pattern = rt.Pattern
		if evidenceFailed.Load() {
			return
		}
		path := r.URL.Path
		if routeTemplates {
			path = routeTemplate(r.Pattern)
		}
		slog.InfoContext(r.Context(), "http request", "method", r.Method, "path", path, "status", rw.statusCode, "bytes", rw.bytes, "duration_ms", float64(time.Since(start))/float64(time.Millisecond), "remote_addr", r.RemoteAddr)
	})
}

func routeTemplate(pattern string) string {
	if pattern == "" {
		return "unmatched"
	}
	if _, path, ok := strings.Cut(pattern, " "); ok && path != "" {
		return path
	}
	return pattern
}

type responseWriter struct {
	http.ResponseWriter
	statusCode int
	written    bool
	bytes      int
}

func (rw *responseWriter) WriteHeader(code int) {
	if rw.written {
		return
	}
	rw.written = true
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

func (rw *responseWriter) Write(p []byte) (int, error) {
	if !rw.written {
		rw.WriteHeader(http.StatusOK)
	}
	n, err := rw.ResponseWriter.Write(p)
	rw.bytes += n
	return n, err
}

// Written reports whether response headers have been sent.
func (rw *responseWriter) Written() bool { return rw.written }

func (rw *responseWriter) Unwrap() http.ResponseWriter { return rw.ResponseWriter }
