package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/api"
	"github.com/Victoroide/quarry-rag/internal/api/handler"
	"github.com/Victoroide/quarry-rag/internal/chunker"
	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/embedder"
	"github.com/Victoroide/quarry-rag/internal/llm"
	"github.com/Victoroide/quarry-rag/internal/pipeline"
	"github.com/Victoroide/quarry-rag/internal/retriever"
	"github.com/Victoroide/quarry-rag/internal/store/memory"
	pgstore "github.com/Victoroide/quarry-rag/internal/store/postgres"
	"github.com/Victoroide/quarry-rag/internal/worker"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestOfflineQueryEndToEnd(t *testing.T) {
	if testing.Short() {
		t.Skip("requires a Docker daemon")
	}
	db, err := pgstore.NewClient(t.Context(), startPostgres(t))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close(context.Background())) })
	require.NoError(t, db.RunMigrations(t.Context()))

	vectors := memory.NewFakeVectorStore()
	emb := embedder.NewFakeEmbedder(256)
	cfg := &config.Config{
		FakeMode: true, BedrockFallbackModel: "offline", MaxUploadBytes: 1 << 20,
		MaxChunksPerQuery: 10, WorkerQueueSize: 10, ExtractConcurrency: 1,
	}
	amb := llm.NewAmbassador(cfg)
	amb.SetClient(llm.NewFakeConverser())
	w := worker.New(db, []pipeline.Stage{
		pipeline.NewChunkStage(chunker.NewChunker(128, 16)),
		pipeline.NewEmbedStage(emb, 2), pipeline.NewPersistStage(vectors),
	}, vectors, worker.Options{Workers: 1, Owner: "offline-query", PollInterval: 10 * time.Millisecond})
	w.Start(t.Context())
	t.Cleanup(w.Shutdown)
	server := httptest.NewServer(api.NewRouter(&api.RouterDeps{
		VectorStore: vectors, ChunkStore: db, Retriever: retriever.NewRetriever(vectors, emb, 0.7, 10),
		Ambassador: amb, Config: cfg, Worker: w, PineconeDriver: vectors, PGPool: db.Pool,
	}))
	t.Cleanup(server.Close)
	client := server.Client()
	client.Timeout = 5 * time.Second

	const content = "The archive retains reports for seven years."
	var ingested handler.IngestResponse
	offlineQueryHTTP(t, client, http.MethodPost, server.URL+"/ingest", map[string]any{"content": content}, http.StatusAccepted, &ingested)
	require.NotEmpty(t, ingested.DocumentID)
	require.Eventually(t, func() bool {
		var status domain.DocumentSummary
		offlineQueryHTTP(t, client, http.MethodGet, server.URL+"/documents/"+ingested.DocumentID, nil, http.StatusOK, &status)
		require.NotEqual(t, "failed", status.Status, "%+v", status)
		return status.Status == "completed" && status.ChunkCount > 0
	}, 10*time.Second, 10*time.Millisecond)

	var answer handler.QueryResponse
	offlineQueryHTTP(t, client, http.MethodPost, server.URL+"/query", handler.QueryRequest{Query: content}, http.StatusOK, &answer)
	require.Positive(t, answer.ChunksUsed)
	require.Contains(t, answer.Answer, `id="1"`)
	require.Contains(t, answer.Answer, content)
	require.NotEmpty(t, answer.ScoredChunks)
	for _, chunk := range answer.ScoredChunks {
		require.Equal(t, ingested.DocumentID, chunk.DocID)
		require.Equal(t, content, chunk.Text)
	}

	var restricted handler.QueryResponse
	offlineQueryHTTP(t, client, http.MethodPost, server.URL+"/query", handler.QueryRequest{
		Query: content, AllowedDocumentIDs: []string{uuid.NewString()},
	}, http.StatusOK, &restricted)
	require.Equal(t, "No relevant information was found to answer this question.", restricted.Answer)
	require.Empty(t, restricted.ScoredChunks)
	require.Zero(t, restricted.ChunksUsed)
	require.Zero(t, restricted.TokenUsage.TotalTokens)
}

func TestStrictBudgetQueryIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("requires a Docker daemon")
	}
	db, err := pgstore.NewClient(t.Context(), startPostgres(t))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close(context.Background())) })
	require.NoError(t, db.RunMigrations(t.Context()))

	type providerRequest struct {
		Stream    bool `json:"stream"`
		MaxTokens int  `json:"max_tokens"`
		Messages  []struct {
			Role    string `json:"role"`
			Content string `json:"content"`
		} `json:"messages"`
	}
	requests := make(chan providerRequest, 3)
	oaiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req providerRequest
		if decodeErr := json.NewDecoder(r.Body).Decode(&req); decodeErr != nil {
			t.Error(decodeErr)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		requests <- req
		if req.Stream {
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = fmt.Fprint(w, "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Archive answer.\"}}]}\n\n")
			_, _ = fmt.Fprint(w, "data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":7000,\"completion_tokens\":5}}\n\n")
			_, _ = fmt.Fprint(w, "data: [DONE]\n\n")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"choices":[{"index":0,"message":{"content":"Archive answer."},"finish_reason":"stop"}],"usage":{"prompt_tokens":7000,"completion_tokens":5}}`)
	}))
	defer oaiServer.Close()

	vectors := memory.NewFakeVectorStore()
	emb := embedder.NewFakeEmbedder(256)
	cfg := &config.Config{
		LLMProvider:               "openai",
		OpenAIBaseURL:             oaiServer.URL,
		OpenAIChatModel:           "gpt-4-0613",
		SynthesisOpenAIDeployment: "chatml-v1|gpt-4-0613|" + oaiServer.URL,
		SynthesisInputProfile:     "strict",
		SynthesisContextTokens:    20_000,
		SynthesisTimeout:          5 * time.Second,
		QueryTimeout:              10 * time.Second,
		MaxUploadBytes:            1 << 20,
		MaxChunksPerQuery:         10,
		WorkerQueueSize:           10,
		ExtractConcurrency:        1,
	}
	oaiClient, err := llm.NewOpenAIClient(cfg)
	require.NoError(t, err)
	amb := llm.NewAmbassador(cfg)
	amb.SetGenerator(oaiClient)
	w := worker.New(db, []pipeline.Stage{
		pipeline.NewChunkStage(chunker.NewChunker(3200, 0)),
		pipeline.NewEmbedStage(emb, 2), pipeline.NewPersistStage(vectors),
	}, vectors, worker.Options{Workers: 1, Owner: "strict-budget", PollInterval: 10 * time.Millisecond})
	w.Start(t.Context())
	t.Cleanup(w.Shutdown)

	server := httptest.NewServer(api.NewRouter(&api.RouterDeps{
		VectorStore: vectors, ChunkStore: db, Retriever: retriever.NewRetriever(vectors, emb, 0.0, 10),
		Ambassador: amb, Config: cfg, Worker: w, PineconeDriver: vectors, PGPool: db.Pool,
	}))
	t.Cleanup(server.Close)
	httpClient := server.Client()
	httpClient.Timeout = 10 * time.Second

	for _, phrase := range []string{"archive records alpha ", "archive records beta ", "archive records gamma "} {
		var ing handler.IngestResponse
		offlineQueryHTTP(t, httpClient, http.MethodPost, server.URL+"/ingest", map[string]any{"content": strings.Repeat(phrase, 1100)}, http.StatusAccepted, &ing)
		require.Eventually(t, func() bool {
			var status domain.DocumentSummary
			offlineQueryHTTP(t, httpClient, http.MethodGet, server.URL+"/documents/"+ing.DocumentID, nil, http.StatusOK, &status)
			return status.Status == "completed" && status.ChunkCount > 0
		}, 10*time.Second, 10*time.Millisecond)
	}

	var jsonResp handler.QueryResponse
	offlineQueryHTTP(t, httpClient, http.MethodPost, server.URL+"/query", handler.QueryRequest{Query: "Summarize archive records", Mode: "global"}, http.StatusOK, &jsonResp)
	require.Equal(t, string(llm.CountMethodOpenAICl100kChat), jsonResp.CountMethod)
	require.Positive(t, jsonResp.ModelInputTokens)
	require.LessOrEqual(t, jsonResp.ModelInputTokens+1024, 8192)
	require.Positive(t, jsonResp.ContextTokens)
	require.GreaterOrEqual(t, len(jsonResp.ScoredChunks), 2)
	require.True(t, jsonResp.ContextChunks < len(jsonResp.ScoredChunks) || jsonResp.ScoredChunks[jsonResp.ContextChunks-1].Truncated)

	reqBody, err := json.Marshal(handler.QueryRequest{Query: "Summarize archive records", Mode: "global"})
	require.NoError(t, err)
	sseReq, err := http.NewRequestWithContext(t.Context(), http.MethodPost, server.URL+"/query", bytes.NewReader(reqBody))
	require.NoError(t, err)
	sseReq.Header.Set("Content-Type", "application/json")
	sseReq.Header.Set("Accept", "text/event-stream")
	sseResp, err := httpClient.Do(sseReq)
	require.NoError(t, err)
	defer func() { require.NoError(t, sseResp.Body.Close()) }()
	require.Equal(t, http.StatusOK, sseResp.StatusCode)
	var rawSSE bytes.Buffer
	_, err = rawSSE.ReadFrom(sseResp.Body)
	require.NoError(t, err)
	var ctxEv handler.QueryStreamContext
	var doneEv handler.QueryStreamDone
	for _, frame := range strings.Split(strings.TrimSuffix(rawSSE.String(), "\n\n"), "\n\n") {
		lines := strings.Split(frame, "\n")
		if len(lines) != 2 {
			continue
		}
		switch lines[0] {
		case "event: context":
			require.NoError(t, json.Unmarshal([]byte(strings.TrimPrefix(lines[1], "data: ")), &ctxEv))
		case "event: done":
			require.NoError(t, json.Unmarshal([]byte(strings.TrimPrefix(lines[1], "data: ")), &doneEv))
		}
	}
	require.Equal(t, jsonResp.ScoredChunks, ctxEv.ScoredChunks)
	require.Equal(t, jsonResp.CountMethod, doneEv.CountMethod)
	require.Equal(t, jsonResp.ModelInputTokens, doneEv.ModelInputTokens)
	require.LessOrEqual(t, doneEv.ModelInputTokens+1024, 8192)
	syncWire, streamWire := <-requests, <-requests
	require.False(t, syncWire.Stream)
	require.True(t, streamWire.Stream)
	require.Equal(t, 1024, syncWire.MaxTokens)
	require.Equal(t, 1024, streamWire.MaxTokens)
	require.Len(t, syncWire.Messages, 2)
	require.Len(t, streamWire.Messages, 2)
	marker := regexp.MustCompile(`(?s)<excerpt-\d{21} id="(\d+)">\n(.*?)\n</excerpt-\d{21}>`)
	syncBodies := marker.FindAllStringSubmatch(syncWire.Messages[1].Content, -1)
	streamBodies := marker.FindAllStringSubmatch(streamWire.Messages[1].Content, -1)
	require.Len(t, syncBodies, jsonResp.ContextChunks)
	require.Len(t, streamBodies, jsonResp.ContextChunks)
	for i := range syncBodies {
		require.Equal(t, syncBodies[i][1:], streamBodies[i][1:])
	}

	// Stay within the 10,000-byte API request limit while overflowing gpt-4's 8,192-token budget.
	var qBuilder strings.Builder
	for i := range 3300 {
		qBuilder.WriteRune(rune(0x3400 + i))
	}
	tooLargeQuestion := qBuilder.String()
	var errResp handler.APIError
	offlineQueryHTTP(t, httpClient, http.MethodPost, server.URL+"/query", handler.QueryRequest{Query: tooLargeQuestion, Mode: "global"}, http.StatusRequestEntityTooLarge, &errResp)
	require.Equal(t, "input_too_large", errResp.Error)
	require.Empty(t, requests)
}

func offlineQueryHTTP(t *testing.T, client *http.Client, method, address string, body any, status int, response any) {
	t.Helper()
	encoded, err := json.Marshal(body)
	require.NoError(t, err)
	req, err := http.NewRequestWithContext(t.Context(), method, address, bytes.NewReader(encoded))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	require.NoError(t, err)
	defer func() { require.NoError(t, resp.Body.Close()) }()
	require.Equal(t, status, resp.StatusCode)
	require.NoError(t, json.NewDecoder(resp.Body).Decode(response))
}
