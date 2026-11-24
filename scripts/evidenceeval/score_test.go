package main

import (
	"encoding/json"
	"testing"

	"github.com/Victoroide/quarry-rag/client"
	"github.com/Victoroide/quarry-rag/internal/evaluation"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestGeneratedOptionalFinishReason(t *testing.T) {
	ns := evaluation.Namespace{Prefix: "staging", ID: uuid.New()}
	q := evaluation.Questions()[0]
	response := &client.QueryResponse{Answer: "Four lamps.", ScoredChunks: []client.ScoredChunkResponse{{DocID: ns.DocumentID("atlas")}}}
	s := observe(q, ns, response, nil, 3, false)
	require.Empty(t, s.Error)
	require.Equal(t, "unknown", s.Finish)
	require.Equal(t, 1, s.RecallHits)
	response.FinishReason = "unexpected remote diagnostic"
	s = observe(q, ns, response, nil, 3, false)
	require.Equal(t, "invalid_response", s.Error)
	require.Empty(t, s.Finish)
}

func TestSynthesisUsagePartialCountersAreUnknown(t *testing.T) {
	ns := evaluation.Namespace{Prefix: "staging", ID: uuid.New()}
	q := evaluation.Questions()[0]
	for _, envelope := range []string{
		`{"output_tokens":9,"total_tokens":9}`,
		`{"input_tokens":9,"total_tokens":9}`,
		`{"input_tokens":9,"output_tokens":9}`,
		`null`,
	} {
		var u client.TokenUsage
		require.NoError(t, json.Unmarshal([]byte(envelope), &u))
		r := &client.QueryResponse{Answer: "Four lamps.", FinishReason: "stop", TokenUsage: u, ScoredChunks: []client.ScoredChunkResponse{{DocID: ns.DocumentID("atlas")}}}
		require.Nil(t, observe(q, ns, r, nil, 3, false).SynthesisUsage, envelope)
	}
	noEvidence := &client.QueryResponse{Answer: "No evidence.", FinishReason: "no_evidence", ScoredChunks: []client.ScoredChunkResponse{}}
	require.Equal(t, &client.TokenUsage{}, observe(q, ns, noEvidence, nil, 3, false).SynthesisUsage)
}

func TestMatchedHumanAndProofRatesUseCommonQueries(t *testing.T) {
	one, zero := 1, 0
	a := []sample{{ID: "atlas-en", GoldSpans: 1, CoverageHits: &one, Review: &review{Claims: 1, Accepted: true}}, {ID: "garden-es", GoldSpans: 1}}
	b := []sample{{ID: "atlas-en", GoldSpans: 1}, {ID: "garden-es", GoldSpans: 1, CoverageHits: &zero, Review: &review{Claims: 1, Unsupported: 1}}}
	disjoint := matchSlice(a, b)
	require.Equal(t, 2, disjoint.Matched)
	require.Zero(t, disjoint.Baseline.Accepted.Total)
	require.Zero(t, disjoint.Candidate.Unsupported.Total)
	require.Zero(t, disjoint.Baseline.Coverage.Total)
	require.Zero(t, disjoint.Candidate.Coverage.Total)
	require.Zero(t, disjoint.MatchedCoverage)
	require.Zero(t, disjoint.MatchedReviews)
	b[0].Review = &review{Claims: 1, Accepted: true}
	b[0].CoverageHits = &zero
	partial := matchSlice(a, b)
	require.Equal(t, 2, partial.Matched)
	require.Equal(t, 1, partial.MatchedCoverage)
	require.Equal(t, 1, partial.MatchedReviews)
	require.Equal(t, 1, partial.MatchedClaims)
	require.Equal(t, proportion(1, 1), partial.Baseline.Accepted)
	require.Equal(t, proportion(1, 1), partial.Candidate.Accepted)
	require.Equal(t, proportion(1, 1), partial.Baseline.Coverage)
	require.Equal(t, proportion(0, 1), partial.Candidate.Coverage)
}

func TestReplayCountsAndCensoring(t *testing.T) {
	r := replay(false)
	require.Equal(t, "replay", r.Source)
	require.Zero(t, r.HTTP.Used)
	m := r.Slices["all"]
	require.Equal(t, 6, m.Selected)
	require.Equal(t, 6, m.Results)
	require.Equal(t, proportion(3, 4), m.Recall)
	require.Equal(t, proportion(1, 4), m.PreviewRecall)
	require.Equal(t, proportion(0, 0), m.Coverage)
	require.Equal(t, proportion(1, 5), m.Unsupported)
	require.Equal(t, proportion(1, 6), m.Empty)
	require.Equal(t, 2, r.VisibilityStats.Observed)
	require.Equal(t, 1, r.VisibilityStats.Censored)
	require.Equal(t, 1, r.VisibilityStats.Failed)
	require.NotNil(t, m.Recall.CI95)
	require.Equal(t, 20.0, *m.Latency.P50)
	require.Equal(t, 50.0, *m.Latency.P95)
	for _, sample := range r.Queries {
		require.Nil(t, sample.Raw)
	}
}

func TestRankTiesAndProofBeyondPreview(t *testing.T) {
	ns := evaluation.Namespace{Prefix: "staging", ID: uuid.New()}
	q := evaluation.Questions()[0]
	atlas := ns.DocumentID("atlas")
	archive := ns.DocumentID("archive")
	response := &client.QueryResponse{Answer: "Four lamps.", FinishReason: "stop", ScoredChunks: []client.ScoredChunkResponse{
		{DocID: archive, Score: .8, Excerpt: 2, ChunkIndex: 3},
		{DocID: atlas, Score: .8, Excerpt: 1, ChunkIndex: 2},
	}}
	hits := []evaluation.Match{{Span: q.Gold[0], Excerpt: 1, Runes: 800}}
	s := observe(q, ns, response, hits, 1, false)
	require.Empty(t, s.Error)
	require.Zero(t, s.RecallHits)
	require.Equal(t, 1, *s.CoverageHits)
	require.False(t, s.Reranked)
	s = observe(q, ns, response, nil, 2, false)
	require.Equal(t, 1, s.RecallHits)
	require.Nil(t, s.CoverageHits)
	response.ScoredChunks[0].DocID = ns.DocumentID("distractor")
	s = observe(q, ns, response, hits, 2, true)
	require.Equal(t, "out_of_scope", s.Error)
	require.Nil(t, s.Raw)
}

func TestMatchedQueriesRequireUsableResults(t *testing.T) {
	r := report{Target: "a", Queries: []sample{{ID: "failed", Error: "throttled"}, {ID: "pass", RecallHits: 1}}}
	b := report{Target: "b", Queries: []sample{{ID: "failed", Error: "timeout"}, {ID: "pass", RecallHits: 0}}}
	comparison := compare(r, b)
	require.Equal(t, 2, comparison.Planned)
	require.Equal(t, 1, comparison.Matched)
	require.Equal(t, 1, comparison.Baseline.Recall.Hits)
	require.Zero(t, comparison.Candidate.Recall.Hits)
	b.Queries = b.Queries[:1]
	require.Zero(t, compare(r, b).Matched)
}
