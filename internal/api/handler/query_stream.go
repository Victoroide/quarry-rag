package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"mime"
	"net/http"
	"strconv"
	"strings"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/pipeline"
)

// QueryStreamContext describes the evidence selected for the query.
type QueryStreamContext struct {
	Mode          string                `json:"mode"`
	ChunksUsed    int                   `json:"chunks_used"`
	ContextChunks int                   `json:"context_chunks"`
	ScoredChunks  []ScoredChunkResponse `json:"scored_chunks"`
}

// QueryStreamDelta carries the next part of the streamed answer.
type QueryStreamDelta struct {
	Text string `json:"text"`
}

// QueryStreamDone contains final synthesis diagnostics and optional citations.
type QueryStreamDone struct {
	FinishReason     string             `json:"finish_reason"`
	TokenUsage       domain.TokenUsage  `json:"token_usage"`
	ContextTokens    int                `json:"context_tokens"`
	CountMethod      string             `json:"count_method,omitempty"`
	ModelInputTokens int                `json:"model_input_tokens,omitempty"`
	StageTimings     map[string]int64   `json:"stage_timings_ms"`
	EarlyExit        bool               `json:"early_exit,omitempty"`
	Citations        []CitationResponse `json:"citations,omitzero"`
	CitationCoverage *CitationCoverage  `json:"citation_coverage,omitempty"`
}

type queryStream struct {
	w        http.ResponseWriter
	r        *http.Request
	ctx      context.Context
	qc       *pipeline.QueryContext
	started  bool
	writeErr error
}

func acceptsQueryStream(r *http.Request) bool {
	for _, item := range strings.Split(strings.Join(r.Header.Values("Accept"), ","), ",") {
		mediaType, params, err := mime.ParseMediaType(strings.TrimSpace(item))
		if err != nil || mediaType != "text/event-stream" {
			continue
		}
		if q, ok := params["q"]; ok {
			quality, err := strconv.ParseFloat(q, 64)
			if err != nil || !(quality > 0 && quality <= 1) {
				continue
			}
		}
		return true
	}
	return false
}

func newQueryStream(ctx context.Context, w http.ResponseWriter, r *http.Request, qc *pipeline.QueryContext) *queryStream {
	s := &queryStream{w: w, r: r, ctx: ctx, qc: qc}
	qc.Stream = &pipeline.QueryStream{
		Context: func(stageCtx context.Context) error {
			return s.event(stageCtx, "context", QueryStreamContext{Mode: qc.RetrievalMode, ChunksUsed: len(qc.RetrievedChunks), ContextChunks: qc.ContextChunks, ScoredChunks: queryScoredChunks(qc)})
		},
		Delta: func(stageCtx context.Context, text string) error {
			return s.event(stageCtx, "delta", QueryStreamDelta{Text: text})
		},
	}
	return s
}

func (s *queryStream) event(ctx context.Context, name string, value any) error {
	if err := s.r.Context().Err(); err != nil {
		return err
	}
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if name != "error" {
		if scopeErr := s.qc.CheckEvidenceScope(ctx); scopeErr != nil {
			return scopeErr
		}
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	if !s.started {
		s.w.Header().Set("Content-Type", "text/event-stream")
		s.w.Header().Set("Cache-Control", "no-cache")
		s.w.Header().Set("X-Accel-Buffering", "no")
		setQueryLanguageHeaders(s.w.Header(), s.qc)
		s.w.WriteHeader(http.StatusOK)
		s.started = true
	}
	if _, err = fmt.Fprintf(s.w, "event: %s\ndata: %s\n\n", name, data); err == nil {
		err = http.NewResponseController(s.w).Flush()
	}
	s.writeErr = err
	return err
}

func (s *queryStream) finish(ctx context.Context, err error) {
	if err == nil {
		err = s.done()
	}
	if err != nil {
		if !s.started {
			writeQueryError(s.w, s.r, err)
			return
		}
		slog.ErrorContext(ctx, "query processing failed", "error", err)
		if s.writeErr != nil || s.r.Context().Err() != nil {
			return
		}
		code, _, body := queryErrorResponse(err)
		if code == 0 {
			return
		}
		if writeErr := s.event(context.WithoutCancel(ctx), "error", body); writeErr != nil {
			slog.DebugContext(ctx, "write query stream error", "error", writeErr)
		}
	}
}

func (s *queryStream) done() error {
	done := QueryStreamDone{
		FinishReason:     s.qc.FinishReason,
		TokenUsage:       s.qc.TokenUsage,
		ContextTokens:    s.qc.ContextTokens,
		CountMethod:      s.qc.CountMethod,
		ModelInputTokens: s.qc.ModelInputTokens,
		StageTimings:     s.qc.StageTimingsMS,
		EarlyExit:        s.qc.EarlyExit,
	}
	if s.qc.IncludeCitations {
		done.Citations = citationResponses(s.qc.Citations)
		done.CitationCoverage = (*CitationCoverage)(s.qc.CitationCoverage)
	}
	return s.event(s.ctx, "done", done)
}
