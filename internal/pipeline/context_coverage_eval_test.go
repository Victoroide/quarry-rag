package pipeline

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/llm"
	"github.com/Victoroide/quarry-rag/internal/retriever"
	"github.com/stretchr/testify/require"
)

type contextCoverageEvalQuery struct {
	ID                 string            `json:"id"`
	Query              string            `json:"query"`
	Collection         string            `json:"collection"`
	AllowedDocumentIDs []string          `json:"allowed_document_ids"`
	RerankOrder        []string          `json:"rerank_order,omitempty"`
	RequiredDocs       []string          `json:"required_docs"`
	RequiredFacts      []routingEvalFact `json:"required_facts"`
}

type contextCoverageOutOfScopeCheck struct {
	ID                 string   `json:"id"`
	Query              string   `json:"query"`
	Collection         string   `json:"collection"`
	AllowedDocumentIDs []string `json:"allowed_document_ids"`
	ForbiddenDocs      []string `json:"forbidden_docs"`
}

type contextCoverageEvalFixture struct {
	CorpusVersion     string                           `json:"corpus_version"`
	ChunkTokens       int                              `json:"chunk_tokens"`
	ChunkOverlap      int                              `json:"chunk_overlap"`
	MaxChunks         int                              `json:"max_chunks"`
	LocalThreshold    float64                          `json:"local_threshold"`
	ContextTokens     int                              `json:"synthesis_context_tokens"`
	RelativeFloor     float64                          `json:"synthesis_relative_score_floor"`
	Documents         []routingCorpusDoc               `json:"documents"`
	OverlapQueries    []contextCoverageEvalQuery       `json:"overlap_queries"`
	SingleFactQueries []contextCoverageEvalQuery       `json:"single_fact_queries"`
	OutOfScopeChecks  []contextCoverageOutOfScopeCheck `json:"out_of_scope_checks"`
}

func loadContextCoverageFixture(t *testing.T) contextCoverageEvalFixture {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", "context_coverage_eval.json"))
	require.NoError(t, err)
	require.Equal(t, "c3a3153cee60c87a5f0bc7122a2bc17c3601ddcc579dc44bde0056db44bb5b32", fmt.Sprintf("%x", sha256.Sum256(data)), "frozen context coverage manifest changed")
	var fix contextCoverageEvalFixture
	require.NoError(t, json.Unmarshal(data, &fix))
	return fix
}

type contextEvalRunResult struct {
	qc             *QueryContext
	allFactsFound  float64
	outOfScopeHits int
}

func candidateChunkIDs(chunks []domain.ScoredChunk) []domain.ChunkID {
	ids := make([]domain.ChunkID, len(chunks))
	for i, sc := range chunks {
		ids[i] = sc.Chunk.ID
	}
	return ids
}

func evalReranker(order []string) rerankerFunc {
	rankByKey := make(map[string]int, len(order))
	for i, key := range order {
		rankByKey[key] = i
	}
	return func(_ context.Context, _ string, in []domain.ScoredChunk) ([]domain.ScoredChunk, error) {
		out := slices.Clone(in)
		slices.SortStableFunc(out, func(a, b domain.ScoredChunk) int {
			ka := fmt.Sprintf("%s#%d", a.Chunk.DocumentID, a.Chunk.Index)
			kb := fmt.Sprintf("%s#%d", b.Chunk.DocumentID, b.Chunk.Index)
			ra, okA := rankByKey[ka]
			rb, okB := rankByKey[kb]
			switch {
			case okA && okB:
				return ra - rb
			case okA:
				return -1
			case okB:
				return 1
			default:
				return 0
			}
		})
		for i := range out {
			score := max(0.01, 1.0-0.01*float64(i))
			out[i].RerankScore = &score
		}
		return out, nil
	}
}

func runContextCoverageQuery(
	t *testing.T,
	cs *evalCorpusStore,
	fix contextCoverageEvalFixture,
	policy string,
	answerMode domain.AnswerMode,
	query, collection string,
	allowed []string,
	facts []routingEvalFact,
) contextEvalRunResult {
	t.Helper()
	return runContextCoverageQueryWithRerank(t, cs, fix, policy, answerMode, query, collection, allowed, nil, facts)
}

func runContextCoverageQueryWithRerank(
	t *testing.T,
	cs *evalCorpusStore,
	fix contextCoverageEvalFixture,
	policy string,
	answerMode domain.AnswerMode,
	query, collection string,
	allowed, rerankOrder []string,
	facts []routingEvalFact,
) contextEvalRunResult {
	t.Helper()
	ret := retriever.NewRetriever(cs.vs, cs.emb, fix.LocalThreshold, fix.MaxChunks)
	scopedPreviews := &evalScopedPreviews{visible: cs.vis.visible, all: cs.previews.values}
	routingCfg := RoutingConfig{Policy: retriever.RoutingPolicyEvidence}
	routing := NewRoutingStageWithConfig(ret, cs.vis, scopedPreviews, routingCfg)
	retrieval := NewRetrievalStageWithVisibility(ret, cs.vis)

	cfg := &config.Config{
		BedrockFallbackModel:        "fallback",
		SynthesisContextTokens:      fix.ContextTokens,
		SynthesisRelativeScoreFloor: fix.RelativeFloor,
		SynthesisContextPolicy:      policy,
		SynthesisTimeout:            5 * time.Second,
	}
	amb := llm.NewAmbassador(cfg)
	amb.SetClient(llm.NewFakeConverser())
	synthesis := NewSynthesisStage(amb, cfg)

	qc := &QueryContext{
		OriginalQuery:      query,
		MaxChunks:          fix.MaxChunks,
		Collection:         collection,
		AllowedDocumentIDs: allowed,
		AnswerMode:         answerMode,
	}
	stages := []QueryStage{NewDecomposeStage(), routing, retrieval}
	if len(rerankOrder) > 0 {
		stages = append(stages, NewRerankStage(evalReranker(rerankOrder), time.Second))
	}
	stages = append(stages, synthesis)
	_, err := RunRetrievalPipeline(t.Context(), qc, stages)
	require.NoError(t, err)

	outOfScope := 0
	for _, sc := range append(append([]domain.ScoredChunk(nil), qc.RetrievedChunks...), qc.Excerpts...) {
		if _, visible := cs.vis.visible[string(sc.Chunk.DocumentID)]; !visible {
			outOfScope++
		}
		if col, _ := sc.Chunk.Metadata["collection"].(string); collection != "" && col != collection {
			outOfScope++
		}
		if allowed != nil {
			matched := false
			for _, id := range allowed {
				if id == string(sc.Chunk.DocumentID) {
					matched = true
					break
				}
			}
			if !matched {
				outOfScope++
			}
		}
	}

	var combined strings.Builder
	for _, ex := range qc.Excerpts {
		combined.WriteString(ex.Chunk.Text)
		combined.WriteByte('\n')
	}
	excerptText := combined.String()
	found := 0
	for _, fact := range facts {
		if strings.Contains(excerptText, fact.Excerpt) {
			found++
		}
	}
	allFound := 0.0
	if len(facts) > 0 && found == len(facts) {
		allFound = 1.0
	}
	return contextEvalRunResult{qc: qc, allFactsFound: allFound, outOfScopeHits: outOfScope}
}

func TestContextCoverageHeldOutPairedEvaluation(t *testing.T) {
	fix := loadContextCoverageFixture(t)
	docByID := make(map[string]routingCorpusDoc, len(fix.Documents))
	for _, d := range fix.Documents {
		docByID[d.ID] = d
	}
	for _, q := range append(append([]contextCoverageEvalQuery(nil), fix.OverlapQueries...), fix.SingleFactQueries...) {
		require.NotEmpty(t, q.RequiredFacts, q.ID)
		for _, fact := range q.RequiredFacts {
			doc, ok := docByID[fact.DocumentID]
			require.True(t, ok, q.ID)
			require.True(t, doc.Visible, q.ID)
			require.Contains(t, doc.Content, fact.Excerpt, q.ID)
		}
	}

	cs := buildEvalCorpusStore(t, fix.Documents, fix.ChunkTokens, fix.ChunkOverlap, 1024)

	// Equal budget and candidate count for both policies across overlap-heavy queries.
	var rankedOverlapSum, diverseOverlapSum float64
	overlapDiffs := make([]float64, 0, len(fix.OverlapQueries))
	overlapDocs := make([][]string, 0, len(fix.OverlapQueries))
	for _, q := range fix.OverlapQueries {
		rRes := runContextCoverageQueryWithRerank(t, cs, fix, config.SynthesisContextPolicyRanked, domain.AnswerModeGenerated, q.Query, q.Collection, q.AllowedDocumentIDs, q.RerankOrder, q.RequiredFacts)
		dRes := runContextCoverageQueryWithRerank(t, cs, fix, config.SynthesisContextPolicyDiverse, domain.AnswerModeGenerated, q.Query, q.Collection, q.AllowedDocumentIDs, q.RerankOrder, q.RequiredFacts)
		eRes := runContextCoverageQueryWithRerank(t, cs, fix, config.SynthesisContextPolicyDiverse, domain.AnswerModeEvidence, q.Query, q.Collection, q.AllowedDocumentIDs, q.RerankOrder, q.RequiredFacts)

		require.Zero(t, rRes.outOfScopeHits, q.ID)
		require.Zero(t, dRes.outOfScopeHits, q.ID)
		require.Zero(t, eRes.outOfScopeHits, q.ID)
		require.Equal(t, candidateChunkIDs(rRes.qc.RetrievedChunks), candidateChunkIDs(dRes.qc.RetrievedChunks), q.ID)
		require.Equal(t, dRes.qc.Excerpts, eRes.qc.Excerpts, q.ID)
		require.Equal(t, dRes.qc.Truncated, eRes.qc.Truncated, q.ID)

		rankedOverlapSum += rRes.allFactsFound
		diverseOverlapSum += dRes.allFactsFound
		overlapDiffs = append(overlapDiffs, dRes.allFactsFound-rRes.allFactsFound)
		overlapDocs = append(overlapDocs, q.RequiredDocs)
	}

	rankedOverlapRate := rankedOverlapSum / float64(len(fix.OverlapQueries))
	diverseOverlapRate := diverseOverlapSum / float64(len(fix.OverlapQueries))
	gainMean, gainLCL, gainUCL := pairedInterval95(overlapDiffs, overlapDocs)

	// Single-fact control queries under identical budget and candidate count.
	var rankedSingleSum, diverseSingleSum float64
	singleLossDiffs := make([]float64, 0, len(fix.SingleFactQueries))
	singleLossDocs := make([][]string, 0, len(fix.SingleFactQueries))
	for _, q := range fix.SingleFactQueries {
		rRes := runContextCoverageQueryWithRerank(t, cs, fix, config.SynthesisContextPolicyRanked, domain.AnswerModeGenerated, q.Query, q.Collection, q.AllowedDocumentIDs, q.RerankOrder, q.RequiredFacts)
		dRes := runContextCoverageQueryWithRerank(t, cs, fix, config.SynthesisContextPolicyDiverse, domain.AnswerModeGenerated, q.Query, q.Collection, q.AllowedDocumentIDs, q.RerankOrder, q.RequiredFacts)
		if strings.HasPrefix(q.ID, "sq-ext-") {
			factRank := -1
			for idx, sc := range rRes.qc.RetrievedChunks {
				if strings.Contains(sc.Chunk.Text, q.RequiredFacts[0].Excerpt) {
					factRank = idx
					break
				}
			}
			require.Greater(t, factRank, 0, "%s must place its required fact after rank 0", q.ID)
		}
		require.Zero(t, rRes.outOfScopeHits, q.ID)
		require.Zero(t, dRes.outOfScopeHits, q.ID)
		require.Equal(t, candidateChunkIDs(rRes.qc.RetrievedChunks), candidateChunkIDs(dRes.qc.RetrievedChunks), q.ID)
		rankedSingleSum += rRes.allFactsFound
		diverseSingleSum += dRes.allFactsFound
		singleLossDiffs = append(singleLossDiffs, rRes.allFactsFound-dRes.allFactsFound)
		singleLossDocs = append(singleLossDocs, q.RequiredDocs)
	}
	rankedSingleRate := rankedSingleSum / float64(len(fix.SingleFactQueries))
	diverseSingleRate := diverseSingleSum / float64(len(fix.SingleFactQueries))
	lossMean, lossLCL, lossUCL := pairedInterval95(singleLossDiffs, singleLossDocs)

	// Explicit out-of-scope checks for hidden documents, other collections and allowlists.
	for _, oos := range fix.OutOfScopeChecks {
		for _, policy := range []string{config.SynthesisContextPolicyRanked, config.SynthesisContextPolicyDiverse} {
			res := runContextCoverageQuery(t, cs, fix, policy, domain.AnswerModeGenerated, oos.Query, oos.Collection, oos.AllowedDocumentIDs, nil)
			require.Zero(t, res.outOfScopeHits, oos.ID)
			for _, sc := range append(append([]domain.ScoredChunk(nil), res.qc.RetrievedChunks...), res.qc.Excerpts...) {
				require.NotContains(t, oos.ForbiddenDocs, string(sc.Chunk.DocumentID), oos.ID)
			}
		}
	}

	t.Logf("overlap-heavy (n=%d, budget=%d, max_chunks=%d): ranked=%.3f diverse=%.3f gain=%.3f 95%%CI=[%.3f, %.3f]",
		len(fix.OverlapQueries), fix.ContextTokens, fix.MaxChunks, rankedOverlapRate, diverseOverlapRate, gainMean, gainLCL, gainUCL)
	t.Logf("single-fact controls (n=%d): ranked=%.3f diverse=%.3f loss=%.3f 95%%CI=[%.3f, %.3f]",
		len(fix.SingleFactQueries), rankedSingleRate, diverseSingleRate, lossMean, lossLCL, lossUCL)

	require.Greater(t, diverseOverlapRate, rankedOverlapRate, "diverse policy must improve all-required-facts coverage on overlap-heavy queries")
	require.Greater(t, gainLCL, 0.0, "lower bound of 95%% paired CI on coverage gain must be positive")
	require.LessOrEqual(t, lossMean, 0.02, "single-fact recall loss exceeded 2pp tolerance")
}
