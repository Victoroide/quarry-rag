package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/client"
	"github.com/Victoroide/quarry-rag/internal/evaluation"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestOptedInComparisonReportsBackendSlicesAndBaseline(t *testing.T) {
	baseline := comparisonServer(t, false)
	candidate := comparisonServer(t, true)
	targets := []targetConfig{
		{Name: "baseline", BaseURL: baseline.URL, deployment: deployment{Backend: "postgres", Dimensions: 8, Reranker: "off"}},
		{Name: "candidate", BaseURL: candidate.URL, deployment: deployment{Backend: "pinecone", Dimensions: 8, Reranker: "off"}},
	}
	o := options{Live: true, Create: true, Delete: true, Instrument: true, Collection: "staging", Baseline: "baseline",
		LedgerDir: t.TempDir(), MaxRequests: 60, K: 3, PollDeadline: time.Second, PollInterval: time.Millisecond, RequestTimeout: time.Second}
	result, err := evaluateTargets(t.Context(), o, output{Version: evaluation.Version}, targets)
	require.ErrorContains(t, err, "candidate")
	require.Equal(t, 60, result.HTTP.Used)
	require.Len(t, result.Targets, 2)
	require.Equal(t, "postgres", result.Targets[0].Backend)
	require.Equal(t, "pinecone", result.Targets[1].Backend)
	for _, target := range result.Targets {
		require.Equal(t, "live_unverified", target.Source)
		require.Empty(t, target.RemainingIDs)
		observed := 4
		if target.Target == "candidate" {
			observed = 3
		}
		require.Equal(t, observed, target.VisibilityStats.Observed)
		require.NotNil(t, target.Slices["language/es"].Recall.CI95)
		require.NotNil(t, target.Slices["language/en"].Latency.P95)
		require.Zero(t, target.Slices["all"].Unsupported.Total)
		require.Nil(t, target.HTTP.ProviderCalls)
	}
	require.Equal(t, 1, result.Targets[1].Slices["all"].Errors["throttled"])
	require.Zero(t, result.Targets[1].Slices["all"].Errors["fixture_unavailable"])
	require.Equal(t, proportion(4, 4), result.Targets[1].Slices["all"].Recall)
	require.Len(t, result.Comparisons, 1)
	matched := result.Comparisons[0]
	require.Equal(t, 6, matched.Planned)
	require.Equal(t, 5, matched.Matched)
	require.Equal(t, 2, matched.Slices["language/cross"].Matched)
	require.Equal(t, proportion(2, 2), result.Targets[0].Slices["pair/atlas"].Coverage)
}

func comparisonServer(t *testing.T, throttle bool) *httptest.Server {
	t.Helper()
	var mu sync.Mutex
	docs := map[string]client.IngestRequest{}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /evaluation/fixtures/{doc_id}", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("doc_id")
		mu.Lock()
		doc, ok := docs[id]
		mu.Unlock()
		if !ok {
			w.Header().Set(evaluation.StateHeader, "missing")
			w.WriteHeader(http.StatusNotFound)
			return
		}
		now := time.Now()
		w.Header().Set(evaluation.StateHeader, "owned")
		writeFixtureStatus(w, r, doc.Collection, comparisonRevision(id), now)
	})
	mux.HandleFunc("POST /evaluation/fixtures", func(w http.ResponseWriter, r *http.Request) {
		var req client.IngestRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decode document: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		mu.Lock()
		docs[req.DocumentID] = req
		mu.Unlock()
		w.Header().Set(evaluation.StateHeader, "owned")
		w.Header().Set(evaluation.IncarnationHeader, comparisonRevision(req.DocumentID))
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(client.IngestResponse{DocumentID: req.DocumentID, Status: "processing"})
	})
	mux.HandleFunc("POST /query", func(w http.ResponseWriter, r *http.Request) {
		var req client.QueryRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decode query: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		ans := client.QueryResponse{Answer: "Fixture response", FinishReason: "no_evidence", ScoredChunks: []client.ScoredChunkResponse{}}
		if req.AnswerMode == client.AnswerModeEvidence {
			id := req.AllowedDocumentIDs[0]
			mu.Lock()
			doc := docs[id]
			mu.Unlock()
			if throttle && doc.Metadata["title"] == evaluation.Documents()[0].Title &&
				!strings.HasSuffix(doc.Collection, "-other") {
				w.WriteHeader(http.StatusTooManyRequests)
				return
			}
			ans.FinishReason = client.FinishReasonEvidenceOnly
			ans.ScoredChunks = append(ans.ScoredChunks, client.ScoredChunkResponse{DocID: id, Text: doc.Content, Excerpt: 1})
		} else {
			generatedComparisonResponse(t, w, r, req, &ans, throttle)
			if ans.FinishReason == "throttled" {
				return
			}
		}
		_ = json.NewEncoder(w).Encode(ans)
	})
	mux.HandleFunc("DELETE /evaluation/fixtures/{doc_id}", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("doc_id")
		if r.Header.Get(evaluation.IncarnationHeader) != comparisonRevision(id) {
			t.Error("cleanup did not send the pinned fixture revision")
		}
		mu.Lock()
		delete(docs, id)
		mu.Unlock()
		w.Header().Set(evaluation.StateHeader, "deleted")
		w.Header().Set(evaluation.IncarnationHeader, comparisonRevision(id))
		_ = json.NewEncoder(w).Encode(client.DeleteResponse{Deleted: true, DocID: id})
	})
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	return server
}

func comparisonRevision(id string) string {
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte(id)).String()
}

func generatedComparisonResponse(t *testing.T, w http.ResponseWriter, r *http.Request, req client.QueryRequest, ans *client.QueryResponse, throttle bool) {
	t.Helper()
	questions := evaluation.Questions()
	idx := slices.IndexFunc(questions, func(q evaluation.Question) bool {
		return q.Text == req.Query && len(q.Allowed) == len(req.AllowedDocumentIDs)
	})
	if idx < 0 {
		t.Errorf("unexpected fixture query %q", req.Query)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	q := questions[idx]
	if throttle && q.ID == "missing" {
		w.WriteHeader(http.StatusTooManyRequests)
		ans.FinishReason = "throttled"
		return
	}
	ns, ok := evaluation.ParseNamespace("staging", r.Header.Get(evaluation.FixtureHeader))
	if !ok || !evaluation.OwnsScope(ns, req.Collection, req.AllowedDocumentIDs) {
		t.Error("generated query did not use the dedicated fixture scope")
	}
	ans.Answer = q.Accepted[0]
	matches := []evaluation.Match{}
	if len(q.Gold) != 0 {
		ans.FinishReason = "stop"
		ans.ScoredChunks = append(ans.ScoredChunks, client.ScoredChunkResponse{DocID: req.AllowedDocumentIDs[0], Excerpt: 1})
		matches = append(matches, evaluation.Match{Span: q.Gold[0], Excerpt: 1, Runes: 850})
	}
	proof, err := evaluation.EncodeMatches(matches)
	if err != nil {
		t.Errorf("encode evaluation observation: %v", err)
	}
	w.Header().Set(evaluation.MatchesHeader, proof)
}

func TestAnnotateJoinsReviewByTargetQueryAndDigest(t *testing.T) {
	fixture := replay(true)
	s := &fixture.Queries[0]
	s.Review = nil
	dir := t.TempDir()
	reportPath, labelPath := filepath.Join(dir, "report.json"), filepath.Join(dir, "reviews.json")
	writeJSON := func(path string, value any) {
		t.Helper()
		body, err := json.Marshal(value)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(path, body, 0o600))
	}
	writeJSON(reportPath, output{Version: evaluation.Version, Targets: []report{fixture}})
	labels := []map[string]any{{
		"target": fixture.Target, "query": s.ID, "response_sha256": s.Digest,
		"claims": 2, "unsupported": 1, "accepted": false,
	}}
	writeJSON(labelPath, labels)
	out, err := annotate(options{Annotate: reportPath, Reviews: labelPath})
	require.NoError(t, err)
	require.Equal(t, proportion(2, 6), out.Targets[0].Slices["all"].Unsupported)
	require.Nil(t, out.Targets[0].Queries[0].Raw)
	fixture.Source = "hosted"
	fixture.Deployment = &deployment{EmbeddingProvider: "offline", AnswerProvider: "offline"}
	writeJSON(reportPath, output{Version: evaluation.Version, Targets: []report{fixture}})
	out, err = annotate(options{Annotate: reportPath, Reviews: labelPath})
	require.NoError(t, err)
	require.Equal(t, "offline", out.Targets[0].Source)
	labels[0]["response_sha256"] = "stale"
	writeJSON(labelPath, labels)
	_, err = annotate(options{Annotate: reportPath, Reviews: labelPath})
	require.ErrorContains(t, err, "does not match")
}
