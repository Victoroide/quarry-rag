package handler

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/google/uuid"
)

func listDocumentSummaries(w http.ResponseWriter, r *http.Request, lister domain.DocumentLister, query url.Values, collection string, include bool) {
	scoped, ok := lister.(domain.CollectionDocumentLister)
	if collection != "" && !ok {
		writeJSONError(w, http.StatusInternalServerError, "internal", "collection filtering is unavailable")
		return
	}

	if !query.Has("limit") && !query.Has("after") {
		streamDocumentSummaries(w, r, lister, scoped, collection, include)
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
	if docs == nil {
		docs = []domain.DocumentSummary{}
	}
	var next *string
	if len(docs) == limit {
		next = &docs[len(docs)-1].ID
	}
	var documents any = docs
	if include {
		projected := make([]domain.DocumentIncarnationSummary, len(docs))
		for i, doc := range docs {
			projected[i] = doc.WithIncarnation()
		}
		documents = projected
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(struct {
		Documents any     `json:"documents"`
		Count     int     `json:"count"`
		NextAfter *string `json:"next_after"`
	}{documents, len(docs), next}); err != nil {
		slog.ErrorContext(r.Context(), "encode document list", "error", err)
	}
}

func streamDocumentSummaries(w http.ResponseWriter, r *http.Request, lister domain.DocumentLister, scoped domain.CollectionDocumentLister, collection string, include bool) {
	count := 0
	started := false
	visit := func(doc domain.DocumentSummary) error {
		encoded, err := json.Marshal(documentSummaryBody(doc, include))
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
