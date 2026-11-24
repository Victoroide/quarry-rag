package handler

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/client"
	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/embedder"
	"github.com/Victoroide/quarry-rag/internal/embeddinginput"
	"github.com/Victoroide/quarry-rag/internal/retriever"
	"github.com/Victoroide/quarry-rag/internal/store/memory"
	"github.com/Victoroide/quarry-rag/internal/testutil"
	"github.com/stretchr/testify/require"
)

type embeddingQueryTransport func(*http.Request) (*http.Response, error)

func (f embeddingQueryTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestExactUnicodeQueryLimitStopsBeforeTransport(t *testing.T) {
	profile := testutil.OpenAIProfile(t, "../../embeddinginput/testdata/cl100k_base.tiktoken.gz", 4)
	var calls atomic.Int32
	savedTransport := http.DefaultTransport
	t.Cleanup(func() { http.DefaultTransport = savedTransport })
	http.DefaultTransport = embeddingQueryTransport(func(r *http.Request) (*http.Response, error) {
		calls.Add(1)
		return &http.Response{StatusCode: http.StatusBadRequest, Header: make(http.Header),
			Body: io.NopCloser(strings.NewReader(`{"error":{"code":"context_length_exceeded"}}`)), Request: r}, nil
	})
	emb, err := embedder.NewOpenAIEmbedder(&config.Config{
		OpenAIBaseURL: embeddinginput.OpenAIEndpoint, OpenAIEmbeddingModel: "text-embedding-3-small",
		EmbeddingDimensions: 3, EmbedConcurrency: 1, EmbedTimeout: time.Second, EmbeddingInputProfile: profile,
	})
	require.NoError(t, err)
	ret := retriever.NewRetriever(memory.NewFakeVectorStore(), emb, .7, 2, retriever.WithEmbeddingInputProfile(profile))
	h := QueryHandler(ret, nil, streamQueryConfig(), nil)
	for _, accept := range []string{"application/json", "text/event-stream"} {
		for _, tc := range []struct{ body, category string }{
			{`{"query":"\u088f.com","answer_mode":"evidence"}`, "embedding_input_too_long"},
			{`{"query":"` + strings.Repeat("a", 10_001) + `","answer_mode":"evidence"}`, "validation"},
		} {
			before := calls.Load()
			req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/query", strings.NewReader(tc.body))
			req.Header.Set("Accept", accept)
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			require.Equal(t, before, calls.Load(), "accept %s", accept)
			require.Equal(t, http.StatusBadRequest, rec.Code)
			require.Equal(t, "application/json", rec.Header().Get("Content-Type"))
			require.Contains(t, rec.Body.String(), `"error":"`+tc.category+`"`)
		}
	}
}

func TestQueryEmbeddingLimitJSONAndSSE(t *testing.T) {
	var calls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":{"code":"context_length_exceeded","message":"private provider message"}}`))
	}))
	defer upstream.Close()

	path := filepath.Join(t.TempDir(), "embedding.json")
	require.NoError(t, os.WriteFile(path, []byte(`{"provider":"openai","model":"embed","revision":"r1","max_characters":8}`), 0o600))
	profile, err := embeddinginput.Load(path, embeddinginput.Target{Provider: "openai", Model: "embed", BaseURL: upstream.URL})
	require.NoError(t, err)
	emb, err := embedder.NewOpenAIEmbedder(&config.Config{
		OpenAIBaseURL: upstream.URL, OpenAIEmbeddingModel: "embed", EmbeddingDimensions: 3, EmbedConcurrency: 1, EmbedTimeout: time.Second,
		EmbeddingInputProfile: profile,
	})
	require.NoError(t, err)
	ret := retriever.NewRetriever(memory.NewFakeVectorStore(), emb, .7, 10, retriever.WithEmbeddingInputProfile(profile))
	h := QueryHandler(ret, nil, streamQueryConfig(), nil)

	invoke := func(accept, body string) *httptest.ResponseRecorder {
		r := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/query", strings.NewReader(body))
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("Accept", accept)
		w := httptest.NewRecorder()
		h(w, r)
		return w
	}
	for _, accept := range []string{"application/json", "text/event-stream"} {
		t.Run(accept, func(t *testing.T) {
			before := calls.Load()
			for _, allowlist := range []string{"", `,"allowed_document_ids":null`, `,"allowed_document_ids":["00000000-0000-4000-8000-000000000001"]`} {
				w := invoke(accept, `{"query":"oversized"`+allowlist+`}`)
				require.Equal(t, http.StatusBadRequest, w.Code)
				require.Equal(t, "application/json", w.Header().Get("Content-Type"))
				require.JSONEq(t, `{"error":"embedding_input_too_long","message":"query exceeds the embedding input profile limit"}`, w.Body.String())
			}
			w := invoke(accept, `{"query":"oversized","allowed_document_ids":[],"answer_mode":"evidence"}`)
			require.Equal(t, http.StatusOK, w.Code)
			require.Equal(t, before, calls.Load())
			w = invoke(accept, `{"query":"fits"}`)
			require.Equal(t, http.StatusBadGateway, w.Code)
			require.Equal(t, "application/json", w.Header().Get("Content-Type"))
			require.JSONEq(t, `{"error":"embedding_model_limit","message":"embedding model rejected the input length"}`, w.Body.String())
			require.Equal(t, before+1, calls.Load())
		})
	}

	server := httptest.NewServer(h)
	defer server.Close()
	c, err := client.New(server.URL)
	require.NoError(t, err)
	query := client.QueryRequest{Query: "oversized"}
	before := calls.Load()
	_, err = c.Query(t.Context(), query)
	var jsonError *client.APIError
	require.ErrorAs(t, err, &jsonError)
	err = c.QueryStream(t.Context(), query, func(client.Event) error {
		t.Error("received a stream event after a pre-embedding rejection")
		return nil
	})
	var streamError *client.APIError
	require.ErrorAs(t, err, &streamError)
	require.Equal(t, jsonError, streamError)
	require.Equal(t, "embedding_input_too_long", streamError.Code)
	require.Equal(t, before, calls.Load())
}
