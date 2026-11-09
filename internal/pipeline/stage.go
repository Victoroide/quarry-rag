// Package pipeline implements the decomposed ingestion and retrieval
// pipeline as a sequence of discrete, composable stages.
package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/metrics"
	"github.com/Victoroide/quarry-rag/internal/tracing"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// StageID identifies a pipeline stage.
type StageID string

// Stage identifiers label pipeline execution results.
const (
	StageChunk              StageID = "chunk"
	StageEmbed              StageID = "embed"
	StagePersist            StageID = "persist"
	StageQueryDecomposition StageID = "query_decomposition"
	StageRetrievalRouting   StageID = "retrieval_routing"
	StageRetrieval          StageID = "retrieval"
	StageSynthesis          StageID = "synthesis"
)

// StageResult carries the output of a single pipeline stage and any
// error that terminated execution.
type StageResult struct {
	StageID StageID
	Err     error
}

// IngestContext accumulates state as a document flows through
// the ingestion pipeline stages.
type IngestContext struct {
	DocumentID domain.DocumentID
	RawText    string
	Chunks     []domain.Chunk
	Metadata   map[string]any
	FileName   *string
	FileType   *string
	S3Key      *string
	DocType    string
	// Stopping is closed when the worker stops claiming jobs; optional waits
	// end early so the attempt can still complete.
	Stopping <-chan struct{}

	// Collection is written to chunk metadata when non-empty.
	Collection string
}

// NewIngestContext creates an initialized IngestContext.
func NewIngestContext(docID domain.DocumentID, text string, metadata map[string]any) *IngestContext {
	return &IngestContext{
		DocumentID: docID,
		RawText:    text,
		Metadata:   metadata,
	}
}

// QueryContext accumulates state as a query flows through
// the retrieval pipeline stages.
type QueryContext struct {
	OriginalQuery string
	SubQueries    []string
	MaxChunks     int
	RequestedMode string
	FilterDocType string
	// IncludeCitations asks synthesis for inline citation markers and verifies them.
	IncludeCitations bool
	// AllowedDocumentIDs, when non-nil, restricts retrieved chunks to this
	// allowlist BEFORE synthesis (tenant/role scoping). Nil = no restriction;
	// non-nil empty = matches nothing.
	AllowedDocumentIDs []string

	// AcceptLanguage selects the fixed answer when no evidence remains.
	AcceptLanguage string

	// Populated by routing stage.
	NamedEntity      string
	EntityDocumentID string
	LocalDocumentID  string
	Candidates       []domain.ScoredChunk
	SkipRetrieval    bool

	// Populated by routing stage.
	RetrievalMode  string
	QueryEmbedding []float32
	TopScore       float64

	// Populated by retrieval stage.
	RetrievedChunks []domain.ScoredChunk

	// Populated by synthesis stage.
	Excerpts      []domain.ScoredChunk
	Truncated     []bool
	Citations     []Citation
	FinishReason  string
	FinalAnswer   string
	TokenUsage    domain.TokenUsage
	ContextTokens int  // estimated synthesis input tokens (after compression)
	ContextChunks int  // chunks actually sent to the LLM (after adaptive trim)
	EarlyExit     bool // true if synthesis skipped (cache hit / no chunks)

	// ContentLanguage identifies the language of the fixed no-evidence answer.
	ContentLanguage string

	// Per-stage timing in milliseconds — observability for tail-latency
	// analysis. Filled by each stage and surfaced in the query response.
	StageTimingsMS map[string]int64

	// Collection restricts routing, retrieval and visibility to one label; empty is unrestricted.
	Collection string
}

// Stage is the interface for a single ingestion pipeline stage.
type Stage interface {
	ID() StageID
	Run(ctx context.Context, ic *IngestContext) error
}

// QueryStage is the interface for a single retrieval pipeline stage.
type QueryStage interface {
	ID() StageID
	Run(ctx context.Context, qc *QueryContext) error
}

// RunIngestionPipeline executes the ingestion stages in order, short-circuiting
// on the first error.
func RunIngestionPipeline(ctx context.Context, ic *IngestContext, stages []Stage) ([]StageResult, error) {
	results := make([]StageResult, 0, len(stages))
	for _, s := range stages {
		if err := ctx.Err(); err != nil {
			return results, fmt.Errorf("pipeline: context canceled before stage %s: %w", s.ID(), err)
		}
		start := time.Now()
		stageCtx, span := otel.Tracer("github.com/Victoroide/quarry-rag/internal/pipeline").Start(ctx, "ingest."+string(s.ID()), trace.WithAttributes(attribute.String(tracing.LangSmithKind, "chain"), attribute.String("quarry.stage", string(s.ID()))))
		err := func() error {
			defer finishStageSpan(span)
			err := s.Run(stageCtx, ic)
			span.SetAttributes(attribute.Int("quarry.ingest.chunks", len(ic.Chunks)))
			tracing.RecordError(span, err)
			return err
		}()
		metrics.FromContext(ctx).StageDuration.WithLabelValues(string(s.ID())).Observe(time.Since(start).Seconds())
		results = append(results, StageResult{StageID: s.ID(), Err: err})
		if err != nil {
			return results, fmt.Errorf("pipeline: stage %s failed: %w", s.ID(), err)
		}
	}
	return results, nil
}

// RunRetrievalPipeline executes the retrieval stages in order, short-circuiting
// on the first error. Records per-stage wall-clock time into
// qc.StageTimingsMS so callers can attribute tail-latency costs.
func RunRetrievalPipeline(ctx context.Context, qc *QueryContext, stages []QueryStage) ([]StageResult, error) {
	defer func() {
		switch qc.RetrievalMode {
		case "LOCAL", "GLOBAL":
			metrics.FromContext(ctx).QueryMode.WithLabelValues(qc.RetrievalMode).Inc()
		}
	}()
	if qc.StageTimingsMS == nil {
		qc.StageTimingsMS = make(map[string]int64, len(stages))
	}
	results := make([]StageResult, 0, len(stages))
	for _, s := range stages {
		if err := ctx.Err(); err != nil {
			return results, fmt.Errorf("pipeline: context canceled before stage %s: %w", s.ID(), err)
		}
		t0 := time.Now()
		name, kind := querySpanName(s.ID()), "chain"
		if s.ID() == StageRetrieval {
			kind = "retriever"
		}
		stageCtx, span := otel.Tracer("github.com/Victoroide/quarry-rag/internal/pipeline").Start(ctx, name, trace.WithAttributes(attribute.String(tracing.LangSmithKind, kind), attribute.String("quarry.stage", string(s.ID()))))
		err := func() error {
			defer finishStageSpan(span)
			err := s.Run(stageCtx, qc)
			if span.IsRecording() {
				setQueryAttributes(stageCtx, span, s.ID(), qc)
			}
			tracing.RecordError(span, err)
			return err
		}()
		elapsed := time.Since(t0)
		qc.StageTimingsMS[string(s.ID())] = elapsed.Milliseconds()
		metrics.FromContext(ctx).StageDuration.WithLabelValues(string(s.ID())).Observe(elapsed.Seconds())
		results = append(results, StageResult{StageID: s.ID(), Err: err})
		if err != nil {
			return results, fmt.Errorf("pipeline: stage %s failed: %w", s.ID(), err)
		}
		// Early-exit hook: a stage may set qc.EarlyExit=true to skip the
		// remaining stages (used by retrieval-cache hits).
		if qc.EarlyExit {
			break
		}
	}
	return results, nil
}

func querySpanName(id StageID) string {
	switch id {
	case StageQueryDecomposition:
		return "query.decompose"
	case StageRetrievalRouting:
		return "query.routing"
	default:
		return "query." + string(id)
	}
}

func finishStageSpan(span trace.Span) {
	defer span.End()
	if recovered := recover(); recovered != nil {
		tracing.RecordError(span, fmt.Errorf("pipeline stage panicked"))
		panic(recovered)
	}
}

func setQueryAttributes(ctx context.Context, span trace.Span, id StageID, qc *QueryContext) {
	span.SetAttributes(
		attribute.Int("quarry.query.sub_queries", len(qc.SubQueries)), attribute.String("quarry.retrieval.mode", qc.RetrievalMode),
		attribute.Int("quarry.retrieval.chunks", len(qc.RetrievedChunks)), attribute.Float64("quarry.retrieval.top_score", qc.TopScore),
		attribute.Int("quarry.synthesis.context_chunks", qc.ContextChunks), attribute.Int("quarry.synthesis.context_tokens", qc.ContextTokens),
		attribute.String("quarry.synthesis.finish_reason", qc.FinishReason), attribute.Bool("quarry.synthesis.early_exit", qc.EarlyExit))
	if !tracing.ContentCapture(ctx) {
		span.SetAttributes(attribute.Int("quarry.query.bytes", len(qc.OriginalQuery)), attribute.String("quarry.query.digest", queryDigest(qc.OriginalQuery)))
		return
	}
	switch id {
	case StageRetrieval:
		for i, result := range qc.RetrievedChunks {
			text := []rune(result.Chunk.Text)
			if len(text) > 4000 {
				text = text[:4000]
			}
			docType, _ := result.Chunk.Metadata["doc_type"].(string)
			if docType == "" {
				docType = domain.DefaultDocType
			}
			metadata, err := json.Marshal(map[string]any{"doc_id": string(result.Chunk.DocumentID), "doc_type": docType, "chunk_index": result.Chunk.Index, "score": result.Score})
			if err != nil {
				continue
			}
			prefix := fmt.Sprintf("retrieval.documents.%d.document.", i)
			span.SetAttributes(attribute.String(prefix+"content", string(text)), attribute.String(prefix+"metadata", string(metadata)))
		}
	case StageSynthesis:
		input, _ := json.Marshal(map[string]string{"question": qc.OriginalQuery})
		output, _ := json.Marshal(map[string]string{"answer": qc.FinalAnswer})
		span.SetAttributes(attribute.String("input.value", string(input)), attribute.String("output.value", string(output)))
	}
}
