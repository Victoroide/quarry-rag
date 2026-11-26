package handler

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"net/url"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/tracing"
	"github.com/google/uuid"
)

// DocumentStatusHandler returns a document's ingestion summary.
func DocumentStatusHandler(cs domain.ChunkStore, opts ...DocumentStatusOption) http.HandlerFunc {
	var options documentStatusOptions
	for _, opt := range opts {
		opt(&options)
	}
	if options.scoped {
		return scopedDocumentStatusHandler(cs, options)
	}
	return func(w http.ResponseWriter, r *http.Request) {
		id, err := uuid.Parse(r.PathValue("doc_id"))
		if err != nil {
			writeJSONError(r.Context(), w, http.StatusBadRequest, "validation", "doc_id must be a valid UUID")
			return
		}
		query, err := url.ParseQuery(r.URL.RawQuery)
		var projection documentSummaryProjection
		if err == nil {
			projection = documentSummaryOptions(query)
		}
		lister, ok := selectDocumentLister(cs, projection.incarnation)
		if !ok {
			writeJSONError(r.Context(), w, http.StatusServiceUnavailable, "unavailable", "document status is unavailable")
			return
		}
		doc, err := lister.GetDocumentSummary(r.Context(), domain.DocumentID(id.String()))
		if err != nil {
			slog.ErrorContext(r.Context(), "look up document summary", "document_id", id, "error", tracing.ErrorForTelemetry(r.Context(), err))
			writeJSONError(r.Context(), w, http.StatusInternalServerError, "internal", "failed to look up document")
			return
		}
		if doc == nil {
			writeJSONError(r.Context(), w, http.StatusNotFound, "not_found", "document not found")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(projection.body(*doc)); err != nil {
			slog.ErrorContext(r.Context(), "encode document summary", "error", tracing.ErrorForTelemetry(r.Context(), err))
		}
	}
}

type documentSummaryProjection struct {
	incarnation     bool
	indexVisibility bool
	pdfCoverage     bool
}

func documentSummaryOptions(query url.Values) documentSummaryProjection {
	enabled := func(key string) bool {
		values := query[key]
		return len(values) == 1 && values[0] == "true"
	}
	return documentSummaryProjection{
		incarnation:     enabled("include_incarnation"),
		indexVisibility: enabled("include_index_visibility"),
		pdfCoverage:     enabled("include_pdf_coverage"),
	}
}

func selectDocumentLister(cs domain.ChunkStore, include bool) (domain.DocumentLister, bool) {
	if include {
		lister, ok := cs.(domain.DocumentIncarnationLister)
		if !ok {
			return nil, false
		}
		return lister.WithDocumentIncarnations(), true
	}
	lister, ok := cs.(domain.DocumentLister)
	return lister, ok
}

func (p documentSummaryProjection) body(doc domain.DocumentSummary) any {
	if !p.indexVisibility {
		doc.IndexVisibility = ""
		doc.IndexVisibilityObservedAt = nil
	} else if doc.IndexVisibility == "" {
		doc.IndexVisibility = domain.VisibilityUnknown
	}
	if !p.pdfCoverage {
		doc.PDFCoverage = ""
	}
	if p.incarnation {
		return doc.WithIncarnation()
	}
	return doc
}

func scopedDocumentStatusHandler(cs domain.ChunkStore, opts documentStatusOptions) http.HandlerFunc {
	now := clockOrDefault(opts.now)
	return func(w http.ResponseWriter, r *http.Request) {
		subject, ok := requireAuditedScopedSubject(w, r, now, opts.audit, "document_status")
		if !ok {
			return
		}
		id, err := uuid.Parse(r.PathValue("doc_id"))
		if err != nil {
			writeJSONError(r.Context(), w, http.StatusBadRequest, "validation", "doc_id must be a valid UUID")
			return
		}
		query, err := url.ParseQuery(r.URL.RawQuery)
		var projection documentSummaryProjection
		if err == nil {
			projection = documentSummaryOptions(query)
		}
		lister, capable := cs.(domain.ScopedDocumentLister)
		if !capable {
			recordSecurityAuditBestEffort(r.Context(), opts.audit, &subject, nil, "document_status", "error", "status_unavailable", id.String(), "")
			writeJSONError(r.Context(), w, http.StatusServiceUnavailable, "unavailable", "document status is unavailable")
			return
		}
		summary, fetchErr := fetchVerifiedScopedSummary(r.Context(), lister, subject, now, domain.DocumentID(id.String()), projection.incarnation)
		if fetchErr != nil {
			if fetchErr.cause != nil {
				slog.ErrorContext(r.Context(), "look up scoped document summary", "error", tracing.ErrorForTelemetry(r.Context(), fetchErr.cause))
			}
			recordSecurityAuditBestEffort(r.Context(), opts.audit, &subject, nil, "document_status", statusAuditOutcome(fetchErr.status), fetchErr.errCode, id.String(), "")
			writeJSONError(r.Context(), w, fetchErr.status, fetchErr.errCode, fetchErr.message)
			return
		}
		rev := summary.CatalogRevision
		if summary.Document == nil {
			recordSecurityAuditBestEffort(r.Context(), opts.audit, &subject, &rev, "document_status", "deny", "not_found", id.String(), "")
			writeJSONError(r.Context(), w, http.StatusNotFound, "not_found", "document not found")
			return
		}
		docIncarnation := summary.Document.Incarnation
		if !requireAuditedAllow(r.Context(), w, opts.audit, &subject, &rev, "document_status", "authorized", id.String(), docIncarnation) {
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(projection.body(redactScopedSummary(*summary.Document))); err != nil {
			slog.ErrorContext(r.Context(), "encode scoped document summary", "error", tracing.ErrorForTelemetry(r.Context(), err))
		}
	}
}
