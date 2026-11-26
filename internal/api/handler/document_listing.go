package handler

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/Victoroide/quarry-rag/internal/api/middleware"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/google/uuid"
)

func listDocumentSummaries(w http.ResponseWriter, r *http.Request, lister domain.DocumentLister, query url.Values, collection string, projection documentSummaryProjection) {
	scoped, ok := lister.(domain.CollectionDocumentLister)
	if collection != "" && !ok {
		writeJSONError(w, http.StatusInternalServerError, "internal", "collection filtering is unavailable")
		return
	}

	if !query.Has("limit") && !query.Has("after") {
		streamDocumentSummaries(w, r, lister, scoped, collection, projection)
		return
	}
	limit, err := strconv.Atoi(query.Get("limit"))
	if err != nil || limit < 1 || limit > 1000 || len(query["limit"]) != 1 {
		writeJSONError(w, http.StatusBadRequest, "validation", "limit must be between 1 and 1000")
		return
	}
	after := ""
	if query.Has("after") {
		id, parseErr := uuid.Parse(query.Get("after"))
		if parseErr != nil || len(query["after"]) != 1 {
			writeJSONError(w, http.StatusBadRequest, "validation", "after must be a valid UUID")
			return
		}
		after = id.String()
	}
	var docs []domain.DocumentSummary
	if collection != "" {
		docs, err = scoped.ListDocumentsPageInCollection(r.Context(), after, limit, collection)
	} else {
		docs, err = lister.ListDocumentsPage(r.Context(), after, limit)
	}
	if err != nil {
		slog.ErrorContext(r.Context(), "list documents page", "error", err)
		writeJSONError(w, http.StatusInternalServerError, "internal", "failed to list documents")
		return
	}
	var next *string
	if len(docs) == limit {
		next = &docs[len(docs)-1].ID
	}
	documents := make([]any, len(docs))
	for i, doc := range docs {
		documents[i] = projection.body(doc)
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(struct {
		Documents []any   `json:"documents"`
		Count     int     `json:"count"`
		NextAfter *string `json:"next_after"`
	}{documents, len(docs), next}); err != nil {
		slog.ErrorContext(r.Context(), "encode document list", "error", err)
	}
}

func streamDocumentSummaries(w http.ResponseWriter, r *http.Request, lister domain.DocumentLister, scoped domain.CollectionDocumentLister, collection string, projection documentSummaryProjection) {
	count := 0
	started := false
	visit := func(doc domain.DocumentSummary) error {
		encoded, err := json.Marshal(projection.body(doc))
		if err != nil {
			return err
		}
		prefix := ","
		if !started {
			w.Header().Set("Content-Type", "application/json")
			started = true
			prefix = `{"documents":[`
		}
		if _, err := io.WriteString(w, prefix); err != nil {
			return err
		}
		if _, err := w.Write(encoded); err != nil {
			return err
		}
		count++
		return nil
	}
	var err error
	if collection != "" {
		err = scoped.StreamDocumentsInCollection(r.Context(), collection, visit)
	} else {
		err = lister.StreamDocuments(r.Context(), visit)
	}
	if err != nil {
		slog.ErrorContext(r.Context(), "stream documents", "error", err)
		if !started {
			writeJSONError(w, http.StatusInternalServerError, "internal", "failed to list documents")
		}
		return
	}
	if !started {
		w.Header().Set("Content-Type", "application/json")
		if _, err := io.WriteString(w, `{"documents":[`); err != nil {
			slog.ErrorContext(r.Context(), "encode document list", "error", err)
			return
		}
	}
	if _, err := fmt.Fprintf(w, `],"count":%d}`+"\n", count); err != nil {
		slog.ErrorContext(r.Context(), "encode document list", "error", err)
	}
}

func listingCollection(query url.Values) (string, *APIError) {
	if len(query["collection"]) > 1 {
		return "", &APIError{Error: "validation", Message: "collection must occur at most once"}
	}
	return validateLabel("collection", query.Get("collection"))
}

func scopedListDocumentsHandler(cs domain.ChunkStore, opts documentListOptions) http.HandlerFunc {
	now := clockOrDefault(opts.now)
	batchSize := opts.streamBatchSize
	if batchSize <= 0 {
		batchSize = defaultScopedStreamBatchSize
	}
	return func(w http.ResponseWriter, r *http.Request) {
		subject, ok := requireAuditedScopedSubject(w, r, now, opts.audit, "document_list")
		if !ok {
			return
		}
		query, err := url.ParseQuery(r.URL.RawQuery)
		if err != nil {
			writeJSONError(w, http.StatusBadRequest, "validation", "invalid query parameters")
			return
		}
		collection, apiErr := listingCollection(query)
		if apiErr != nil {
			writeJSONError(w, http.StatusBadRequest, apiErr.Error, apiErr.Message)
			return
		}
		projection := documentSummaryOptions(query)
		lister, capable := cs.(domain.ScopedDocumentLister)
		if !capable {
			recordSecurityAuditBestEffort(r.Context(), opts.audit, &subject, nil, "document_list", "error", "inventory_unavailable", "", "")
			writeJSONError(w, http.StatusServiceUnavailable, "unavailable", "document inventory is unavailable")
			return
		}
		if !query.Has("limit") && !query.Has("after") {
			streamScopedDocumentSummaries(w, r, lister, subject, now, batchSize, collection, projection, opts.audit)
			return
		}
		limit, err := strconv.Atoi(query.Get("limit"))
		if err != nil || limit < 1 || limit > 1000 || len(query["limit"]) != 1 {
			writeJSONError(w, http.StatusBadRequest, "validation", "limit must be between 1 and 1000")
			return
		}
		after := ""
		if query.Has("after") {
			id, parseErr := uuid.Parse(query.Get("after"))
			if parseErr != nil || len(query["after"]) != 1 {
				writeJSONError(w, http.StatusBadRequest, "validation", "after must be a valid UUID")
				return
			}
			after = id.String()
		}
		page, fetchErr := fetchVerifiedScopedPage(r.Context(), lister, subject, now, after, limit, collection, projection.incarnation)
		if fetchErr != nil {
			if fetchErr.cause != nil {
				slog.ErrorContext(r.Context(), "list scoped documents page", "error", fetchErr.cause)
			}
			recordSecurityAuditBestEffort(r.Context(), opts.audit, &subject, nil, "document_list", statusAuditOutcome(fetchErr.status), fetchErr.errCode, "", "")
			writeJSONError(w, fetchErr.status, fetchErr.errCode, fetchErr.message)
			return
		}
		rev := page.CatalogRevision
		if !requireAuditedAllow(r.Context(), w, opts.audit, &subject, &rev, "document_list", "authorized", "", "") {
			return
		}
		var next *string
		if len(page.Documents) == limit {
			next = &page.Documents[len(page.Documents)-1].ID
		}
		documents := make([]any, len(page.Documents))
		for i, doc := range page.Documents {
			documents[i] = projection.body(redactScopedSummary(doc))
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(struct {
			Documents []any   `json:"documents"`
			Count     int     `json:"count"`
			NextAfter *string `json:"next_after"`
		}{documents, len(page.Documents), next}); err != nil {
			slog.ErrorContext(r.Context(), "encode scoped document list", "error", err)
		}
	}
}

func streamScopedDocumentSummaries(
	w http.ResponseWriter,
	r *http.Request,
	lister domain.ScopedDocumentLister,
	subject middleware.VerifiedSubject,
	now func() time.Time,
	batchSize int,
	collection string,
	projection documentSummaryProjection,
	audit domain.SecurityAuditRecorder,
) {
	after := ""
	count := 0
	started := false
	var streamRevision int64
	for r.Context().Err() == nil {
		page, fetchErr := fetchVerifiedScopedPage(r.Context(), lister, subject, now, after, batchSize, collection, projection.incarnation)
		if fetchErr != nil {
			if fetchErr.cause != nil {
				slog.ErrorContext(r.Context(), "stream scoped documents", "error", fetchErr.cause)
			}
			recordSecurityAuditBestEffort(r.Context(), audit, &subject, nil, "document_list", statusAuditOutcome(fetchErr.status), fetchErr.errCode, "", "")
			if !started {
				writeJSONError(w, fetchErr.status, fetchErr.errCode, fetchErr.message)
			}
			return
		}
		if started && page.Revision != streamRevision {
			rev := page.CatalogRevision
			recordSecurityAuditBestEffort(r.Context(), audit, &subject, &rev, "document_list", "deny", "policy_changed", "", "")
			return
		}
		if !started {
			rev := page.CatalogRevision
			if !requireAuditedAllow(r.Context(), w, audit, &subject, &rev, "document_list", "authorized", "", "") {
				return
			}
			streamRevision = page.Revision
		}
		if len(page.Documents) == 0 {
			break
		}
		for _, doc := range page.Documents {
			if r.Context().Err() != nil {
				return
			}
			if !subject.ValidAt(now()) {
				recordSecurityAuditBestEffort(r.Context(), audit, &subject, nil, "document_list", "deny", "credential_expired", "", "")
				if !started {
					writeJSONError(w, http.StatusUnauthorized, "unauthorized", "invalid or missing credentials")
				}
				return
			}
			encoded, err := json.Marshal(projection.body(redactScopedSummary(doc)))
			if err != nil {
				slog.ErrorContext(r.Context(), "encode scoped document summary", "error", err)
				if !started {
					writeJSONError(w, http.StatusInternalServerError, "internal", "failed to list documents")
				}
				return
			}
			prefix := ","
			if !started {
				w.Header().Set("Content-Type", "application/json")
				started = true
				prefix = `{"documents":[`
			}
			if _, err := io.WriteString(w, prefix); err != nil {
				return
			}
			if _, err := w.Write(encoded); err != nil {
				return
			}
			count++
		}
		if len(page.Documents) < batchSize {
			break
		}
		after = page.Documents[len(page.Documents)-1].ID
	}
	if r.Context().Err() != nil {
		return
	}
	if !subject.ValidAt(now()) {
		recordSecurityAuditBestEffort(r.Context(), audit, &subject, nil, "document_list", "deny", "credential_expired", "", "")
		if !started {
			writeJSONError(w, http.StatusUnauthorized, "unauthorized", "invalid or missing credentials")
		}
		return
	}
	if !started {
		w.Header().Set("Content-Type", "application/json")
		if _, err := io.WriteString(w, `{"documents":[`); err != nil {
			slog.ErrorContext(r.Context(), "encode scoped document list", "error", err)
			return
		}
	}
	if _, err := fmt.Fprintf(w, `],"count":%d}`+"\n", count); err != nil {
		slog.ErrorContext(r.Context(), "encode scoped document list", "error", err)
	}
}
