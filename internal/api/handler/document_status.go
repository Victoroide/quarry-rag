package handler

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"net/url"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/google/uuid"
)

// DocumentStatusHandler returns a document's ingestion summary.
func DocumentStatusHandler(cs domain.ChunkStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, err := uuid.Parse(r.PathValue("doc_id"))
		if err != nil {
			writeJSONError(w, http.StatusBadRequest, "validation", "doc_id must be a valid UUID")
			return
		}
		query, err := url.ParseQuery(r.URL.RawQuery)
		var projection documentSummaryProjection
		if err == nil {
			projection = documentSummaryOptions(query)
		}
		lister, ok := selectDocumentLister(cs, projection.incarnation)
		if !ok {
			writeJSONError(w, http.StatusServiceUnavailable, "unavailable", "document status is unavailable")
			return
		}
		doc, err := lister.GetDocumentSummary(r.Context(), domain.DocumentID(id.String()))
		if err != nil {
			slog.ErrorContext(r.Context(), "look up document summary", "document_id", id, "error", err)
			writeJSONError(w, http.StatusInternalServerError, "internal", "failed to look up document")
			return
		}
		if doc == nil {
			writeJSONError(w, http.StatusNotFound, "not_found", "document not found")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(projection.body(*doc)); err != nil {
			slog.ErrorContext(r.Context(), "encode document summary", "error", err)
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
