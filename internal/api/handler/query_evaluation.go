package handler

import (
	"context"
	"net/http"
	"slices"

	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/evaluation"
	"github.com/Victoroide/quarry-rag/internal/pipeline"
)

func evaluationScope(r *http.Request, prefix string, qc *pipeline.QueryContext) (evaluation.Namespace, bool) {
	if prefix == "" || qc.AnswerMode != domain.AnswerModeGenerated || qc.Authorization != nil {
		return evaluation.Namespace{}, false
	}
	values := r.Header.Values(evaluation.FixtureHeader)
	if len(values) != 1 {
		return evaluation.Namespace{}, false
	}
	ns, ok := evaluation.ParseNamespace(prefix, values[0])
	return ns, ok && evaluation.OwnsScope(ns, qc.Collection, qc.AllowedDocumentIDs)
}

func evaluationExecution(cfg *config.Config, qc *pipeline.QueryContext) string {
	if cfg.FakeMode {
		return "offline"
	}
	if qc.EarlyExit || len(qc.QueryEmbedding) == 0 || len(qc.Excerpts) == 0 || qc.FinalAnswer == "" {
		return ""
	}
	return evaluation.ProviderExecution(cfg.VectorStore, cfg.EmbeddingProvider, cfg.LLMProvider)
}

func evaluationProof(ctx context.Context, r *http.Request, prefix string, qc *pipeline.QueryContext) string {
	ns, ok := evaluationScope(r, prefix, qc)
	if !ok || r.Header.Get(evaluation.ObserveHeader) == "off" {
		return ""
	}
	if len(qc.AllowedDocumentIDs) != 0 {
		receipt := r.Header.Values(evaluation.ReceiptHeader)
		visibility, supported := qc.EvidenceVisibility.(domain.EvaluationEvidenceVisibility)
		if !supported || len(receipt) != 1 || !evaluation.ValidReceipt(receipt[0]) {
			return ""
		}
		// Recheck the original candidate bodies. Synthesis may trim or sanitize
		// their excerpts, so those bodies cannot be compared to storage.
		selected := make([]domain.ScoredChunk, 0, len(qc.Excerpts))
		for _, candidate := range qc.RetrievedChunks {
			if slices.ContainsFunc(qc.Excerpts, func(excerpt domain.ScoredChunk) bool {
				return excerpt.Chunk.DocumentID == candidate.Chunk.DocumentID &&
					excerpt.Chunk.ID == candidate.Chunk.ID && excerpt.Chunk.Index == candidate.Chunk.Index
			}) {
				selected = append(selected, candidate)
			}
		}
		if len(selected) != len(qc.Excerpts) {
			return ""
		}
		verified, err := visibility.VerifyEvaluationEvidence(ctx, receipt[0], qc.AllowedDocumentIDs, selected)
		if err != nil || !verified {
			return ""
		}
	}
	value, err := evaluation.EncodeMatches(evaluation.MatchExcerpts(ns, qc.AllowedDocumentIDs, qc.Excerpts))
	if err != nil {
		return ""
	}
	return value
}
