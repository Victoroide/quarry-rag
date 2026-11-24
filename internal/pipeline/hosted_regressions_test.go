package pipeline

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/llm"
	"github.com/Victoroide/quarry-rag/internal/retriever"
	"github.com/stretchr/testify/require"
)

func TestHostedRefillRestoresOriginalHitThatCompletes(t *testing.T) {
	pending := refillHit("pending", "record-a", "Alpha.", .95)
	steady := refillHit("steady", "record-b", "Beta.", .9)
	completed := false
	var sizes []int
	store := refillIndex{search: func(call hostedSearch) ([]domain.ScoredChunk, int, error) {
		sizes = append(sizes, call.topK)
		if call.budget == 0 {
			return []domain.ScoredChunk{pending, steady}, 0, nil
		}
		completed = true
		return []domain.ScoredChunk{steady}, 1, nil
	}}
	check := currentChunks{check: func(hits []domain.ScoredChunk) ([]domain.ScoredChunk, error) {
		var out []domain.ScoredChunk
		for _, h := range hits {
			if h.Chunk.ID != pending.Chunk.ID || completed {
				out = append(out, h)
			}
		}
		return out, nil
	}}
	qc := &QueryContext{RetrievalMode: "GLOBAL", QueryEmbedding: []float32{1}, MaxChunks: 2,
		EvidenceVisibility: check, HostedVisibilityRefill: true, HostedVisibilityExtraRequests: 1}
	require.NoError(t, NewRetrievalStage(retriever.NewRetriever(store, nil, .7, 2)).Run(t.Context(), qc))
	require.Equal(t, []int{2, 4}, sizes)
	var ids []domain.ChunkID
	for _, h := range qc.RetrievedChunks {
		ids = append(ids, h.Chunk.ID)
	}
	require.Equal(t, []domain.ChunkID{"pending", "steady"}, ids)
}

func TestHostedRefillWidensAfterLocalUnionRemoval(t *testing.T) {
	a := refillHit("a", "local", "Alpha.", .93)
	b := refillHit("b", "other", "Beta.", .9)
	c := refillHit("c", "third", "Gamma.", .87)
	var calls []hostedSearch
	filter := domain.VectorFilter{Collection: "archive", DocType: "guide", DocumentIDs: []string{"local", "other", "third"}}
	store := refillIndex{search: func(call hostedSearch) ([]domain.ScoredChunk, int, error) {
		calls = append(calls, call)
		if len(call.filter.DocumentIDs) == 1 {
			return []domain.ScoredChunk{a}, 0, nil
		}
		if call.budget == 0 {
			return []domain.ScoredChunk{a, b}, 0, nil
		}
		return []domain.ScoredChunk{a, b, c}, 1, nil
	}}
	reads := 0
	check := currentChunks{check: func(hits []domain.ScoredChunk) ([]domain.ScoredChunk, error) {
		reads++
		var out []domain.ScoredChunk
		for _, h := range hits {
			if reads < 3 || h.Chunk.ID != b.Chunk.ID {
				out = append(out, h)
			}
		}
		return out, nil
	}}
	qc := &QueryContext{RetrievalMode: "LOCAL", LocalDocumentID: "local", Collection: filter.Collection, FilterDocType: filter.DocType, AllowedDocumentIDs: filter.DocumentIDs, QueryEmbedding: []float32{1}, MaxChunks: 2,
		EvidenceVisibility: check, HostedVisibilityRefill: true, HostedVisibilityExtraRequests: 1}
	require.NoError(t, NewRetrievalStage(retriever.NewRetriever(store, nil, .7, 2)).Run(t.Context(), qc))
	var ids []domain.ChunkID
	for _, h := range qc.RetrievedChunks {
		ids = append(ids, h.Chunk.ID)
	}
	localFilter := filter
	localFilter.DocumentIDs = []string{"local"}
	require.Equal(t, []hostedSearch{{filter: localFilter, topK: 2}, {filter: filter, topK: 2}, {filter: filter, topK: 4, budget: 1}}, calls)
	require.Equal(t, []domain.ChunkID{"a", "c"}, ids)
}

func TestHostedEvidenceChecksEachProviderSend(t *testing.T) {
	for _, name := range []string{"rerank page", "rerank retry", "rerank retry SQL failure", "generate retry", "stream retry"} {
		t.Run(name, func(t *testing.T) {
			var revoked atomic.Bool
			requests := make(chan string, 4)
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, err := io.ReadAll(r.Body)
				if err != nil {
					t.Error(err)
					return
				}
				requests <- string(body)
				revoked.Store(true)
				w.Header().Set("Content-Type", "application/json")
				if name == "rerank page" {
					_, _ = io.WriteString(w, `{"results":[{"index":0,"relevanceScore":0.9}],"nextToken":"next"}`)
					return
				}
				w.Header().Set("X-Amzn-Errortype", "InternalServerException")
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = io.WriteString(w, `{"message":"temporary provider failure"}`)
			}))
			t.Cleanup(upstream.Close)
			t.Setenv("AWS_ENDPOINT_URL", upstream.URL)
			t.Setenv("AWS_ENDPOINT_URL_BEDROCK_AGENT_RUNTIME", upstream.URL)
			t.Setenv("AWS_ENDPOINT_URL_BEDROCK_RUNTIME", upstream.URL)
			cfg := &config.Config{AWSRegion: "us-east-1", AWSAccessKeyID: "fixture", AWSSecretKey: "fixture",
				AWSMaxAttempts: 2, RerankModelARN: "fixture", BedrockFallbackModel: "fixture"}
			want := ErrEvidenceScopeChanged
			var lookupErr error
			if name == "rerank retry SQL failure" {
				lookupErr = errors.New("visibility lookup failed")
				want = lookupErr
			}
			visibility := currentChunks{check: func(hits []domain.ScoredChunk) ([]domain.ScoredChunk, error) {
				if revoked.Load() {
					return nil, lookupErr
				}
				return hits, nil
			}}
			qc := &QueryContext{OriginalQuery: "hours?", HostedVisibilityRefill: true, EvidenceVisibility: visibility,
				RetrievedChunks: []domain.ScoredChunk{refillHit("a", "doc", "Alpha.", .95), refillHit("b", "doc", "Beta.", .9)}}
			var err error
			if strings.HasPrefix(name, "rerank") {
				ranker, buildErr := retriever.NewBedrockReranker(t.Context(), cfg)
				require.NoError(t, buildErr)
				err = NewRerankStage(ranker, 0).Run(t.Context(), qc)
			} else {
				converser, buildErr := llm.NewClient(t.Context(), cfg)
				require.NoError(t, buildErr)
				amb := llm.NewAmbassador(cfg)
				amb.SetClient(converser)
				if name == "stream retry" {
					qc.Stream = &QueryStream{Context: func() error { return nil }, Delta: func(string) error { return nil }}
				}
				err = NewSynthesisStage(amb, cfg).Run(t.Context(), qc)
			}
			require.ErrorIs(t, err, want)
			require.ErrorIs(t, err, domain.ErrEvidenceCheck)
			require.Len(t, requests, 1)
			body := <-requests
			require.Contains(t, body, "Alpha.")
			require.Contains(t, body, "Beta.")
		})
	}
}

func TestHostedRefillKeepsBothLocalBodiesUntilFinalCheck(t *testing.T) {
	old := refillHit("same", "local", "Previous.", .99)
	fresh := refillHit("same", "local", "Current.", .90)
	stale := refillHit("stale", "local", "Unavailable.", .98)
	completedTopUp := false
	store := refillIndex{search: func(call hostedSearch) ([]domain.ScoredChunk, int, error) {
		switch {
		case call.topK == 4:
			return []domain.ScoredChunk{fresh}, 1, nil
		case call.filter.DocumentIDs == nil:
			completedTopUp = true
			return nil, 0, nil
		default:
			return []domain.ScoredChunk{old, stale}, 0, nil
		}
	}}
	check := currentChunks{check: func(hits []domain.ScoredChunk) ([]domain.ScoredChunk, error) {
		var out []domain.ScoredChunk
		for _, hit := range hits {
			if (!completedTopUp && hit.Chunk.Text == old.Chunk.Text) || (completedTopUp && hit.Chunk.Text == fresh.Chunk.Text) {
				out = append(out, hit)
			}
		}
		return out, nil
	}}
	qc := &QueryContext{RetrievalMode: "LOCAL", LocalDocumentID: "local", QueryEmbedding: []float32{1}, MaxChunks: 2,
		EvidenceVisibility: check, HostedVisibilityRefill: true, HostedVisibilityExtraRequests: 1}
	require.NoError(t, NewRetrievalStage(retriever.NewRetriever(store, nil, .7, 2)).Run(t.Context(), qc))
	require.Equal(t, []domain.ScoredChunk{fresh}, qc.RetrievedChunks)
}

func TestHostedRefillFailedExpansionUsesOnlyPreviouslyVerifiedHits(t *testing.T) {
	pending := refillHit("pending", "local", "Finishes during expansion.", .95)
	steady := refillHit("steady", "other", "Already current.", .9)
	completed := false
	store := refillIndex{search: func(call hostedSearch) ([]domain.ScoredChunk, int, error) {
		if call.budget > 0 {
			completed = true
			return nil, 1, errors.New("provider failed")
		}
		return []domain.ScoredChunk{pending, steady}, 0, nil
	}}
	check := currentChunks{check: func(hits []domain.ScoredChunk) ([]domain.ScoredChunk, error) {
		if !completed {
			return hits[1:], nil
		}
		return hits, nil
	}}
	qc := &QueryContext{RetrievalMode: "GLOBAL", QueryEmbedding: []float32{1}, MaxChunks: 2,
		EvidenceVisibility: check, HostedVisibilityRefill: true, HostedVisibilityExtraRequests: 1}
	require.NoError(t, NewRetrievalStage(retriever.NewRetriever(store, nil, .7, 2)).Run(t.Context(), qc))
	require.Equal(t, []domain.ScoredChunk{steady}, qc.RetrievedChunks)
}
