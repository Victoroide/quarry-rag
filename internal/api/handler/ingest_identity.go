package handler

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/Victoroide/quarry-rag/internal/chunker"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/extractor"
	"github.com/Victoroide/quarry-rag/internal/tracing"
	"github.com/Victoroide/quarry-rag/internal/worker"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"
)

func ingestDocumentID(rw http.ResponseWriter, supplied string) (string, bool) {
	if supplied == "" {
		return uuid.NewString(), true
	}
	parsed, err := uuid.Parse(supplied)
	if err != nil {
		writeJSONError(rw, http.StatusBadRequest, "validation", "document_id must be a UUID")
		return "", false
	}
	return parsed.String(), true
}

func mixedPDFFingerprint(collection, original string) string {
	return chunker.ScopedFingerprint(collection, domain.MixedPDFProfile+"\x00"+original)
}

func setPDFOriginalFingerprints(req *domain.AcceptRequest, file []byte) {
	original := string(file)
	req.Fingerprint = mixedPDFFingerprint(req.Collection, original)
	req.LegacyOCRFingerprint = chunker.ScopedFingerprint(req.Collection, original)
}

// acceptMixedPDF reports whether it wrote the response. A clean anonymous
// upload may duplicate a historical parser-only document, so its identity needs
// the compatibility parser's text. An inconclusive parse stops the request
// before any source object or document is created; only a conclusive absence of
// text lets the upload continue, because no such historical row can exist.
func acceptMixedPDF(rw http.ResponseWriter, r *http.Request, wk *worker.Worker, cs domain.ChunkStore, opts ingestOptions, file []byte, req *domain.AcceptRequest) bool {
	outcome, _ := acceptIngest(rw, r, wk, cs, *req)
	req.TryLegacyPDFText = false
	if outcome != domain.AcceptNeedsPDFText {
		return outcome != domain.AcceptNeedsSource
	}
	text, err := opts.extract(r.Context(), extractor.FileTypePDF, file)
	switch {
	case err != nil && !extractionErrorIs(err, extractor.ErrNoText):
		writeExtractionError(rw, r, err)
		return true
	case err == nil:
		if text = strings.ReplaceAll(text, "\x00", ""); len(text) >= domain.MinPDFTextBytes {
			req.LegacyParserFingerprint = chunker.ScopedFingerprint(req.Collection, text)
		}
	}
	outcome, _ = acceptIngest(rw, r, wk, cs, *req)
	return outcome != domain.AcceptNeedsSource
}

func acceptExistingPDFUpload(rw http.ResponseWriter, r *http.Request, wk *worker.Worker, cs domain.ChunkStore, opts ingestOptions, file []byte, req domain.AcceptRequest) bool {
	acceptor, ok := cs.(domain.ExistingPDFAcceptor)
	if !ok {
		return false
	}
	accept := func(ctx context.Context, request domain.AcceptRequest) (domain.AcceptResult, error) {
		return acceptor.AcceptExistingPDF(ctx, request, opts.extractPages != nil)
	}
	outcome, _ := acceptIngestWith(rw, r, wk, accept, req)
	if outcome != domain.AcceptNeedsPDFText {
		return outcome != domain.AcceptNeedsSource
	}
	text, err := opts.extract(r.Context(), extractor.FileTypePDF, file)
	if err != nil {
		if extractionErrorIs(err, extractor.ErrNoText) {
			writeJSONError(rw, http.StatusConflict, "conflict", "document_id already exists with different content")
		} else {
			writeExtractionError(rw, r, err)
		}
		return true
	}
	req.LegacyParserFingerprint = chunker.ScopedFingerprint(req.Collection, strings.ReplaceAll(text, "\x00", ""))
	outcome, _ = acceptIngestWith(rw, r, wk, accept, req)
	return outcome != domain.AcceptNeedsSource
}

func acceptIngest(rw http.ResponseWriter, r *http.Request, wk *worker.Worker, cs domain.ChunkStore, req domain.AcceptRequest) (domain.AcceptOutcome, error) {
	acceptor, ok := cs.(domain.DocumentAcceptor)
	if !ok {
		writeJSONError(rw, http.StatusServiceUnavailable, "unavailable", "document store unavailable")
		return domain.AcceptConflict, errors.New("durable acceptance is unavailable")
	}
	return acceptIngestWith(rw, r, wk, acceptor.AcceptDocument, req)
}

func acceptIngestWith(rw http.ResponseWriter, r *http.Request, wk *worker.Worker, accept func(context.Context, domain.AcceptRequest) (domain.AcceptResult, error), req domain.AcceptRequest) (domain.AcceptOutcome, error) {
	acceptCtx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), 10*time.Second)
	defer cancel()
	result, err := accept(acceptCtx, req)
	if err != nil {
		slog.ErrorContext(r.Context(), "accept document", "error", err)
		writeJSONError(rw, http.StatusServiceUnavailable, "unavailable", "document store unavailable")
		return domain.AcceptConflict, err
	}
	code, state := http.StatusAccepted, "processing"
	switch result.Outcome {
	case domain.AcceptAccepted, domain.AcceptInProgress, domain.AcceptRequeued:
		if wk != nil {
			wk.Notify()
		}
	case domain.AcceptDuplicate:
		code, state = http.StatusOK, "already_ingested"
	case domain.AcceptConflict:
		writeJSONError(rw, http.StatusConflict, "conflict", "document_id already exists with different content")
		return result.Outcome, nil
	case domain.AcceptOCRRetryRequired:
		writeJSONError(rw, http.StatusConflict, "conflict", "retry the mixed PDF with its document_id")
		return result.Outcome, nil
	case domain.AcceptOCRRetryAfterExpiry:
		writeJSONError(rw, http.StatusConflict, "conflict", "the mixed PDF result can be retried after it expires")
		return result.Outcome, nil
	case domain.AcceptQueueFull:
		rw.Header().Set("Retry-After", "30")
		writeJSONError(rw, http.StatusServiceUnavailable, "queue", "ingest queue is full")
		return result.Outcome, nil
	case domain.AcceptNeedsSource, domain.AcceptNeedsPDFText:
		return result.Outcome, nil
	default:
		writeJSONError(rw, http.StatusServiceUnavailable, "unavailable", "document store unavailable")
		return result.Outcome, nil
	}
	trace.SpanFromContext(r.Context()).SetAttributes(tracing.DocumentAttributes(string(result.DocumentID))...)
	if req.EvaluationReceipt != "" && result.Outcome == domain.AcceptAccepted && result.Incarnation != "" {
		fixtureHeaders(rw, domain.FixtureOwned, result.Incarnation)
	}
	rw.Header().Set("Content-Type", "application/json")
	rw.WriteHeader(code)
	if err = json.NewEncoder(rw).Encode(IngestResponse{DocumentID: string(result.DocumentID), Status: state}); err != nil {
		slog.ErrorContext(r.Context(), "encode ingestion response", "error", err)
	}
	return result.Outcome, nil
}
