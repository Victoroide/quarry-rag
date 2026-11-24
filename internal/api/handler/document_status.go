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
		include := err == nil && includeDocumentIncarnation(query)
		lister, ok := selectDocumentLister(cs, include)
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
		if err := json.NewEncoder(w).Encode(documentSummaryBody(*doc, include)); err != nil {
			slog.ErrorContext(r.Context(), "encode document summary", "error", err)
		}
	}
}

func includeDocumentIncarnation(query url.Values) bool {
	values := query["include_incarnation"]
	return len(values) == 1 && values[0] == "true"
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

func documentSummaryBody(doc domain.DocumentSummary, include bool) any {
	if include {
		return doc.WithIncarnation()
	}
	return doc
}
