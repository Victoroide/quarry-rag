package handler

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/evaluation"
	"github.com/Victoroide/quarry-rag/internal/worker"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// EvaluationFixtureOptions configures staged, receipt-qualified cleanup.
type EvaluationFixtureOptions struct {
	Prefix         string
	LocalVectors   bool
	VectorIdentity string
}

// EvaluationFixtureIngestHandler accepts only receipt-qualified JSON uploads.
func EvaluationFixtureIngestHandler(wk *worker.Worker, cs domain.ChunkStore, opts ...IngestOption) http.HandlerFunc {
	ingest := IngestHandler(wk, cs, nil, 0, opts...)
	return func(w http.ResponseWriter, r *http.Request) {
		if len(r.Header.Values(evaluation.ReceiptHeader)) != 1 {
			fixtureUnavailable(w)
			return
		}
		ingest.ServeHTTP(w, r)
	}
}

// EvaluationFixtureStatusHandler reads a receipt and its current document.
func EvaluationFixtureStatusHandler(cs domain.ChunkStore, prefix string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, err := uuid.Parse(r.PathValue("doc_id"))
		if err != nil {
			writeJSONError(w, http.StatusBadRequest, "validation", "doc_id must be a valid UUID")
			return
		}
		fixtureStatus(w, r, cs, domain.DocumentID(id.String()), prefix)
	}
}

// EvaluationFixtureDeleteHandler removes exactly the supplied fixture revision.
func EvaluationFixtureDeleteHandler(cs domain.ChunkStore, vectors domain.VectorDeleter, opts EvaluationFixtureOptions) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, err := uuid.Parse(r.PathValue("doc_id"))
		if err != nil {
			writeJSONError(w, http.StatusBadRequest, "validation", "doc_id must be a valid UUID")
			return
		}
		deleteFixture(w, r, cs, vectors, id.String(), opts)
	}
}

func fixtureRequest(r *http.Request, prefix, id string) (evaluation.Namespace, string, evaluation.Document, bool) {
	values := r.Header.Values(evaluation.ReceiptHeader)
	namespaces := r.Header.Values(evaluation.FixtureHeader)
	if len(values) != 1 || len(namespaces) != 1 {
		return evaluation.Namespace{}, "", evaluation.Document{}, false
	}
	ns, ok := evaluation.ParseNamespace(prefix, namespaces[0])
	if !ok || !evaluation.ValidReceipt(values[0]) {
		return evaluation.Namespace{}, "", evaluation.Document{}, false
	}
	for _, doc := range evaluation.Documents() {
		if ns.DocumentID(doc.Key) == id {
			return ns, values[0], doc, true
		}
	}
	return evaluation.Namespace{}, "", evaluation.Document{}, false
}

func fixtureHeaders(w http.ResponseWriter, state, incarnation string) {
	w.Header().Set(evaluation.StateHeader, state)
	if incarnation != "" {
		w.Header().Set(evaluation.IncarnationHeader, incarnation)
	}
	w.Header().Set("Cache-Control", "no-store")
}

func fixtureUnavailable(w http.ResponseWriter) {
	writeJSONError(w, http.StatusPreconditionFailed, "precondition", "evaluation fixture ownership is unavailable")
}

func fixtureStatus(w http.ResponseWriter, r *http.Request, cs domain.ChunkStore, id domain.DocumentID, prefix string) {
	_, receipt, _, valid := fixtureRequest(r, prefix, string(id))
	store, ok := cs.(domain.EvaluationFixtureStore)
	lister, listOK := selectDocumentLister(cs, true)
	if !valid || !ok || !listOK {
		fixtureUnavailable(w)
		return
	}
	fixture, err := store.ReadEvaluationFixture(r.Context(), id, receipt)
	if err != nil {
		slog.ErrorContext(r.Context(), "read fixture receipt", "error", err)
		writeJSONError(w, http.StatusInternalServerError, "internal", "failed to read fixture receipt")
		return
	}
	switch fixture.State {
	case domain.FixtureMissing, domain.FixtureDeleted, domain.FixtureRetired:
		fixtureHeaders(w, fixture.State, fixture.Incarnation)
		writeJSONError(w, http.StatusNotFound, "not_found", "fixture document is absent")
	case domain.FixtureBorrowed:
		fixtureHeaders(w, fixture.State, "")
		writeJSONError(w, http.StatusConflict, "conflict", "fixture receipt does not own this document")
	case domain.FixtureWrongStore:
		fixtureStoreUnavailable(w, fixture.Incarnation)
	case domain.FixtureOwned:
		doc, err := lister.GetDocumentSummary(r.Context(), id)
		if err != nil {
			slog.ErrorContext(r.Context(), "read fixture status", "error", err)
			writeJSONError(w, http.StatusInternalServerError, "internal", "failed to read fixture status")
			return
		}
		if doc == nil || doc.Incarnation != fixture.Incarnation {
			fixtureUnavailable(w)
			return
		}
		fixtureHeaders(w, fixture.State, fixture.Incarnation)
		w.Header().Set(evaluation.DatabaseTimeHeader, fixture.DatabaseTime.UTC().Format(time.RFC3339Nano))
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(doc.WithIncarnation()); err != nil {
			slog.ErrorContext(r.Context(), "encode fixture status", "error", err)
		}
	default:
		fixtureUnavailable(w)
	}
}

func deleteFixture(w http.ResponseWriter, r *http.Request, cs domain.ChunkStore, vectors domain.VectorDeleter, id string, opts EvaluationFixtureOptions) {
	_, receipt, _, valid := fixtureRequest(r, opts.Prefix, id)
	revisions := r.Header.Values(evaluation.IncarnationHeader)
	reconcile := r.Header.Values(evaluation.ReconcileHeader)
	store, ok := cs.(domain.EvaluationFixtureStore)
	if !valid || !ok {
		fixtureUnavailable(w)
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), 60*time.Second)
	defer cancel()
	if len(reconcile) == 1 && reconcile[0] == "absent" && len(revisions) == 0 {
		reconcileFixture(ctx, w, store, id, receipt)
		return
	}
	if len(reconcile) != 0 || len(revisions) != 1 || !evaluation.ValidIncarnation(revisions[0]) {
		fixtureUnavailable(w)
		return
	}
	state, err := store.DeleteEvaluationFixture(ctx, domain.DocumentID(id), receipt, revisions[0], opts.VectorIdentity, func(sweepCtx context.Context) error {
		if opts.LocalVectors {
			// The document transaction cascades to PostgreSQL vectors.
			return nil
		}
		if vectors == nil {
			return errors.New("fixture vector store is unavailable")
		}
		if ms, hasMS := cs.(domain.VectorManifestStore); hasMS {
			manifests, listErr := ms.ListDocumentVectorManifests(sweepCtx, domain.DocumentID(id), revisions[0])
			if listErr != nil {
				return listErr
			}
			if len(manifests) > 0 {
				revDeleter, canRev := vectors.(domain.ManifestRevisionVectorDeleter)
				if !canRev {
					return errors.New("fixture revision vector deleter is unavailable")
				}
				for _, m := range manifests {
					if delErr := revDeleter.DeleteByManifestRevision(sweepCtx, id, m.RevisionID, m.VectorIDs); delErr != nil && status.Code(delErr) != codes.NotFound {
						return delErr
					}
				}
				return nil
			}
		}
		// Admission wrote only to the default namespace, so the receipt does not
		// cover a namespace named after the document.
		err := vectors.DeleteByDocID(sweepCtx, id)
		if status.Code(err) == codes.NotFound {
			return nil
		}
		return err
	})
	if err != nil {
		if fixtureWritePaused(w, err) {
			return
		}
		slog.ErrorContext(ctx, "delete fixture", "document_id", id, "error", err)
		writeJSONError(w, http.StatusInternalServerError, "internal", "failed to delete fixture")
		return
	}
	switch state {
	case domain.FixtureDeleted:
		writeFixtureRemoval(ctx, w, id, state, revisions[0])
	case domain.FixtureBusy:
		fixtureHeaders(w, state, revisions[0])
		writeJSONError(w, http.StatusConflict, "fixture_busy", "fixture ingestion is still running")
	case domain.FixtureWrongStore:
		fixtureStoreUnavailable(w, revisions[0])
	default:
		fixtureHeaders(w, domain.FixtureBorrowed, "")
		writeJSONError(w, http.StatusPreconditionFailed, "precondition", "fixture receipt or incarnation does not match")
	}
}

func reconcileFixture(ctx context.Context, w http.ResponseWriter, store domain.EvaluationFixtureStore, id, receipt string) {
	s, err := store.ReconcileEvaluationFixture(ctx, domain.DocumentID(id), receipt)
	if err != nil {
		if fixtureWritePaused(w, err) {
			return
		}
		slog.ErrorContext(ctx, "reconcile fixture", "document_id", id, "error", err)
		writeJSONError(w, http.StatusInternalServerError, "internal", "failed to reconcile fixture")
		return
	}
	switch s.State {
	case domain.FixtureRetired, domain.FixtureDeleted:
		writeFixtureRemoval(ctx, w, id, s.State, s.Incarnation)
	case domain.FixtureWrongStore:
		fixtureStoreUnavailable(w, s.Incarnation)
	default:
		fixtureHeaders(w, s.State, s.Incarnation)
		writeJSONError(w, http.StatusConflict, "conflict", "fixture reconciliation requires the current receipt and incarnation")
	}
}

func fixtureWritePaused(w http.ResponseWriter, err error) bool {
	switch {
	case errors.Is(err, domain.ErrWritersFrozen):
		writeFrozen(w)
	case errors.Is(err, domain.ErrGenerationMismatch):
		w.Header().Set("Retry-After", "30")
		writeJSONError(w, http.StatusServiceUnavailable, "unavailable", "this replica does not serve the active vector generation")
	default:
		return false
	}
	return true
}

func fixtureStoreUnavailable(w http.ResponseWriter, incarnation string) {
	fixtureHeaders(w, domain.FixtureWrongStore, incarnation)
	writeJSONError(w, http.StatusPreconditionFailed, "fixture_store", "original evaluation fixture vector store is required")
}

func writeFixtureRemoval(ctx context.Context, w http.ResponseWriter, id, state, incarnation string) {
	fixtureHeaders(w, state, incarnation)
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(DeleteResponse{Deleted: state == domain.FixtureDeleted, DocID: id}); err != nil {
		slog.ErrorContext(ctx, "encode fixture removal", "error", err)
	}
}
