package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"math"
	"slices"
	"strings"

	"github.com/Victoroide/quarry-rag/client"
	"github.com/Victoroide/quarry-rag/internal/evaluation"
)

type review struct {
	Claims      int  `json:"claims"`
	Unsupported int  `json:"unsupported"`
	Accepted    bool `json:"accepted"`
}

type rawResponse struct {
	Answer    string                       `json:"answer"`
	Chunks    []client.ScoredChunkResponse `json:"chunks"`
	Citations []client.CitationResponse    `json:"citations"`
}

type sample struct {
	ID               string             `json:"id"`
	Groups           []string           `json:"groups"`
	Milliseconds     float64            `json:"milliseconds"`
	Error            string             `json:"error,omitempty"`
	HTTPStatus       int                `json:"http_status,omitempty"`
	Finish           string             `json:"finish_reason,omitempty"`
	Gold             int                `json:"gold_documents"`
	RecallHits       int                `json:"recalled_documents"`
	PreviewHits      int                `json:"preview_spans"`
	GoldSpans        int                `json:"gold_spans"`
	CoverageHits     *int               `json:"covered_spans"`
	ObservationError string             `json:"observation_error,omitempty"`
	Matches          []evaluation.Match `json:"matches,omitempty"`
	Empty            bool               `json:"empty_answer"`
	NoEvidence       bool               `json:"no_evidence"`
	Reranked         bool               `json:"reranked"`
	Execution        string             `json:"provider_execution,omitempty"`
	SynthesisUsage   *client.TokenUsage `json:"synthesis_usage"`
	Digest           string             `json:"response_sha256,omitempty"`
	Review           *review            `json:"human_review,omitempty"`
	Raw              *rawResponse       `json:"raw,omitempty"`
}

func newSample(q evaluation.Question) sample {
	s := sample{ID: q.ID, GoldSpans: len(q.Gold), Groups: []string{"all", "language/" + q.Language}}
	spanDocs := goldDocuments(q)
	s.Gold = len(spanDocs)
	if s.Gold == 0 {
		s.Groups = append(s.Groups, "evidence/no_answer")
		return s
	}
	s.Groups = append(s.Groups, "evidence/answerable", "pair/"+q.Pair)
	cross := false
	for _, doc := range evaluation.Documents() {
		if slices.Contains(spanDocs, doc.Key) && doc.Language != q.Language {
			cross = true
		}
	}
	if cross {
		s.Groups = append(s.Groups, "language/cross")
	}
	return s
}

func goldDocuments(q evaluation.Question) []string {
	var keys []string
	for _, doc := range evaluation.Documents() {
		for _, span := range doc.Spans {
			if slices.Contains(q.Gold, span.ID) {
				keys = append(keys, doc.Key)
				break
			}
		}
	}
	return keys
}

func observe(q evaluation.Question, ns evaluation.Namespace, response *client.QueryResponse, matches []evaluation.Match, k int, includeRaw bool) sample {
	s := newSample(q)
	s.Empty = strings.TrimSpace(response.Answer) == ""
	s.NoEvidence = response.FinishReason == "no_evidence"
	switch response.FinishReason {
	case "":
		s.Finish = "unknown"
	case "stop", "max_tokens", "no_evidence":
		s.Finish = response.FinishReason
	case "filtered", "context_exceeded":
		s.Finish = response.FinishReason
		s.Error = response.FinishReason
		return s
	default:
		s.Error = "invalid_response"
		return s
	}
	if response.ScoredChunks == nil || (s.NoEvidence && len(response.ScoredChunks) != 0) || (s.Finish == "unknown" && s.Empty) {
		s.Error = "invalid_response"
		return s
	}
	allowed := make(map[string]bool, len(q.Allowed))
	for _, key := range q.Allowed {
		allowed[ns.DocumentID(key)] = true
	}
	for _, chunk := range response.ScoredChunks {
		if !allowed[chunk.DocID] {
			s.Error = "out_of_scope"
			return s
		}
		s.Reranked = s.Reranked || chunk.RerankScore != nil
	}
	for _, citation := range response.Citations {
		if !allowed[citation.DocID] {
			s.Error = "out_of_scope"
			return s
		}
	}
	ranked := response.ScoredChunks[:min(k, len(response.ScoredChunks))]
	for _, key := range goldDocuments(q) {
		id := ns.DocumentID(key)
		if slices.ContainsFunc(ranked, func(chunk client.ScoredChunkResponse) bool { return chunk.DocID == id }) {
			s.RecallHits++
		}
	}
	for _, doc := range evaluation.Documents() {
		for _, span := range doc.Spans {
			if slices.Contains(q.Gold, span.ID) && slices.ContainsFunc(ranked, func(chunk client.ScoredChunkResponse) bool {
				return chunk.DocID == ns.DocumentID(doc.Key) && strings.Contains(chunk.Text, span.Text)
			}) {
				s.PreviewHits++
			}
		}
	}
	if matches != nil {
		s.recordMatches(q, ns, response.ScoredChunks, matches)
	}
	usage := response.TokenUsage
	// This envelope only accounts for synthesis. A zero envelope on a
	// generated answer cannot distinguish missing usage from actual zero.
	if s.NoEvidence && usage == (client.TokenUsage{}) ||
		usage.InputTokens > 0 && usage.OutputTokens > 0 &&
			int64(usage.InputTokens)+int64(usage.OutputTokens) == int64(usage.TotalTokens) {
		s.SynthesisUsage = &usage
	}
	raw := &rawResponse{Answer: response.Answer, Chunks: response.ScoredChunks, Citations: response.Citations}
	if includeRaw {
		s.Raw = raw
	}
	body, err := json.Marshal(raw)
	if err == nil {
		sum := sha256.Sum256(body)
		s.Digest = hex.EncodeToString(sum[:])
	}
	return s
}

func (s *sample) recordMatches(q evaluation.Question, ns evaluation.Namespace, chunks []client.ScoredChunkResponse, matches []evaluation.Match) {
	count := 0
	for _, match := range matches {
		found := false
		for _, doc := range evaluation.Documents() {
			if slices.Contains(q.Allowed, doc.Key) && slices.ContainsFunc(doc.Spans, func(span evaluation.Span) bool {
				return span.ID == match.Span
			}) && slices.ContainsFunc(chunks, func(chunk client.ScoredChunkResponse) bool {
				return chunk.DocID == ns.DocumentID(doc.Key) && chunk.Excerpt == match.Excerpt
			}) {
				found = true
				break
			}
		}
		if !found {
			s.ObservationError = "unmatched_excerpt"
			return
		}
		if slices.Contains(q.Gold, match.Span) {
			count++
		}
	}
	s.CoverageHits = &count
	s.Matches = matches
}

type rate struct {
	Hits   int         `json:"numerator"`
	Total  int         `json:"denominator"`
	Status string      `json:"status"`
	Value  *float64    `json:"rate"`
	CI95   *[2]float64 `json:"wilson_95"`
}

func proportion(hits, total int) rate {
	r := rate{Hits: hits, Total: total, Status: "unmeasured"}
	if total == 0 {
		return r
	}
	p, n := float64(hits)/float64(total), float64(total)
	const z = 1.959963984540054
	factor := 1 + z*z/n
	center := (p + z*z/(2*n)) / factor
	half := z * math.Sqrt(p*(1-p)/n+z*z/(4*n*n)) / factor
	r.Value = &p
	r.Status = "measured"
	r.CI95 = &[2]float64{max(0, center-half), min(1, center+half)}
	return r
}

type latency struct {
	Count int      `json:"count"`
	P50   *float64 `json:"p50_ms"`
	P95   *float64 `json:"p95_ms"`
}

func percentiles(ms []float64) latency {
	if len(ms) == 0 {
		return latency{}
	}
	sorted := slices.Clone(ms)
	slices.Sort(sorted)
	p50 := sorted[int(math.Ceil(.50*float64(len(sorted))))-1]
	p95 := sorted[int(math.Ceil(.95*float64(len(sorted))))-1]
	return latency{Count: len(sorted), P50: &p50, P95: &p95}
}

type sliceResult struct {
	Selected              int            `json:"selected"`
	Results               int            `json:"results"`
	Errors                map[string]int `json:"errors"`
	ObservationErrors     map[string]int `json:"observation_errors"`
	Recall                rate           `json:"recall_at_k"`
	PreviewRecall         rate           `json:"preview_span_recall_at_k"`
	Coverage              rate           `json:"post_trim_coverage"`
	Unsupported           rate           `json:"unsupported_claims"`
	Accepted              rate           `json:"accepted_answers"`
	Empty                 rate           `json:"empty_answers"`
	NoEvidence            rate           `json:"no_evidence"`
	NoAnswerFalsePositive rate           `json:"no_answer_false_positive"`
	EmptySynthesisErrors  int            `json:"empty_synthesis_errors"`
	EmptyOrAbstained      rate           `json:"empty_or_abstained"`
	Reranked              int            `json:"reranked"`
	Latency               latency        `json:"result_latency"`
}

func summarize(samples []sample) sliceResult {
	r := sliceResult{Selected: len(samples), Errors: map[string]int{}, ObservationErrors: map[string]int{}}
	var durations []float64
	var recall, gold, previewHits, previewGold, covered, spans, unsupported, claims, accepted, reviews, empty, noEvidence, abstained, falsePositive, noAnswerReviews int
	for _, s := range samples {
		if s.Error != "" {
			r.Errors[s.Error]++
			if s.Error == "empty_synthesis" {
				r.EmptySynthesisErrors++
			}
			continue
		}
		r.Results++
		durations = append(durations, s.Milliseconds)
		recall += s.RecallHits
		gold += s.Gold
		previewHits += s.PreviewHits
		previewGold += s.GoldSpans
		if s.CoverageHits != nil {
			covered += *s.CoverageHits
			spans += s.GoldSpans
		}
		if s.ObservationError != "" {
			r.ObservationErrors[s.ObservationError]++
		}
		if s.Review != nil {
			unsupported += s.Review.Unsupported
			claims += s.Review.Claims
			reviews++
			if s.GoldSpans == 0 {
				noAnswerReviews++
				if s.Review.Unsupported > 0 {
					falsePositive++
				}
			}
			if s.Review.Accepted {
				accepted++
			}
		}
		if s.Empty {
			empty++
		}
		if s.NoEvidence {
			noEvidence++
		}
		if s.Empty || s.NoEvidence || s.Review != nil && s.Review.Accepted && s.Gold == 0 {
			abstained++
		}
		if s.Reranked {
			r.Reranked++
		}
	}
	r.Recall = proportion(recall, gold)
	r.PreviewRecall = proportion(previewHits, previewGold)
	r.Coverage = proportion(covered, spans)
	r.Unsupported = proportion(unsupported, claims)
	r.Accepted = proportion(accepted, reviews)
	r.Empty = proportion(empty, r.Results)
	r.NoEvidence = proportion(noEvidence, r.Results)
	r.NoAnswerFalsePositive = proportion(falsePositive, noAnswerReviews)
	r.EmptyOrAbstained = proportion(abstained, r.Results)
	r.Latency = percentiles(durations)
	return r
}

func byGroup(samples []sample) map[string][]sample {
	groups := map[string][]sample{"all": samples}
	for _, s := range samples {
		for _, group := range s.Groups {
			if group != "all" {
				groups[group] = append(groups[group], s)
			}
		}
	}
	return groups
}

type matchedSlice struct {
	Planned         int         `json:"planned"`
	Matched         int         `json:"matched"`
	MatchedCoverage int         `json:"matched_coverage_queries"`
	MatchedReviews  int         `json:"matched_reviewed_queries"`
	MatchedClaims   int         `json:"matched_claim_queries"`
	MatchedNoAnswer int         `json:"matched_reviewed_no_answer_queries"`
	Baseline        sliceResult `json:"baseline"`
	Candidate       sliceResult `json:"candidate"`
}

type comparison struct {
	BaselineTarget  string `json:"baseline_target"`
	CandidateTarget string `json:"candidate_target"`
	matchedSlice
	Slices map[string]matchedSlice `json:"slices"`
}

func matchSlice(baseline, candidate []sample) matchedSlice {
	byID := make(map[string]sample, len(candidate))
	for _, s := range candidate {
		byID[s.ID] = s
	}
	var left, right []sample
	pair := matchedSlice{Planned: len(baseline)}
	var leftClaims, leftUnsupported, rightClaims, rightUnsupported int
	for _, b := range baseline {
		if c, ok := byID[b.ID]; ok && b.Error == "" && c.Error == "" {
			if b.CoverageHits == nil || c.CoverageHits == nil {
				b.CoverageHits, c.CoverageHits = nil, nil
			} else if b.GoldSpans > 0 && c.GoldSpans > 0 {
				pair.MatchedCoverage++
			}
			if b.Review == nil || c.Review == nil {
				b.Review, c.Review = nil, nil
			} else {
				pair.MatchedReviews++
				if b.GoldSpans == 0 && c.GoldSpans == 0 {
					pair.MatchedNoAnswer++
				}
				if b.Review.Claims > 0 && c.Review.Claims > 0 {
					pair.MatchedClaims++
					leftClaims += b.Review.Claims
					leftUnsupported += b.Review.Unsupported
					rightClaims += c.Review.Claims
					rightUnsupported += c.Review.Unsupported
				}
			}
			left, right = append(left, b), append(right, c)
		}
	}
	pair.Matched = len(left)
	pair.Baseline, pair.Candidate = summarize(left), summarize(right)
	pair.Baseline.Unsupported = proportion(leftUnsupported, leftClaims)
	pair.Candidate.Unsupported = proportion(rightUnsupported, rightClaims)
	return pair
}

func compare(baseline, candidate report) comparison {
	groups := byGroup(candidate.Queries)
	c := comparison{BaselineTarget: baseline.Target, CandidateTarget: candidate.Target, Slices: map[string]matchedSlice{}}
	for group, samples := range byGroup(baseline.Queries) {
		matched := matchSlice(samples, groups[group])
		if group == "all" {
			c.matchedSlice = matched
		} else {
			c.Slices[group] = matched
		}
	}
	return c
}
