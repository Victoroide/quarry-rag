package embedder

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/embeddinginput"
	"github.com/stretchr/testify/require"
)

func TestOpenAIProfileSeparatesProviderFailures(t *testing.T) {
	t.Setenv("EMBEDDING_INPUT_PROFILE", "")
	profile := testInputProfile(t, `{"provider":"openai","model":"embed","revision":"r1","max_characters":6}`,
		embeddinginput.Target{Provider: "openai", Model: "embed"})
	for _, tc := range []struct {
		name, body string
		status     int
		want       error
	}{
		{"window code", `{"error":{"code":"context_length_exceeded","message":"private upstream text"}}`, http.StatusBadRequest, domain.ErrEmbeddingModelLimit},
		{"window message", `{"error":{"code":null,"type":"invalid_request_error","message":"This model's maximum context length is 4 tokens, however you requested 5 tokens. private upstream text"}}`, http.StatusBadRequest, domain.ErrEmbeddingModelLimit},
		{"unknown validation", `{"error":{"code":"invalid_model","message":"private upstream text"}}`, http.StatusBadRequest, nil},
		{"denial", `{"error":{"code":"context_length_exceeded","message":"private upstream text"}}`, http.StatusForbidden, domain.ErrEmbeddingDenied},
		{"throttling", `{"error":{"code":"context_length_exceeded","message":"private upstream text"}}`, http.StatusTooManyRequests, domain.ErrUpstreamThrottled},
		{"dimension mismatch", `{"data":[{"index":0,"embedding":[1]}]}`, http.StatusOK, ErrInvalidEmbedding},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				calls.Add(1)
				w.WriteHeader(tc.status)
				_, _ = io.WriteString(w, tc.body)
			}))
			t.Cleanup(server.Close)
			cfg := config.Config{OpenAIBaseURL: server.URL, OpenAIEmbeddingModel: "embed", EmbeddingDimensions: 3,
				EmbedConcurrency: 1, EmbedTimeout: time.Second, EmbeddingInputProfile: profile}
			e, err := NewOpenAIEmbedder(&cfg)
			require.NoError(t, err)
			_, err = e.Embed(t.Context(), "toolong")
			require.ErrorIs(t, err, embeddinginput.ErrInputLimit)
			require.Zero(t, calls.Load())
			_, err = e.Embed(t.Context(), "sample")
			require.Equal(t, int32(1), calls.Load())
			require.Error(t, err)
			require.NotContains(t, err.Error(), "private upstream text")
			if tc.want == nil {
				require.NotErrorIs(t, err, domain.ErrEmbeddingModelLimit)
			} else {
				require.ErrorIs(t, err, tc.want)
			}
			if tc.want == domain.ErrEmbeddingModelLimit {
				cfg.EmbeddingInputProfile = nil
				legacy, err := NewOpenAIEmbedder(&cfg)
				require.NoError(t, err)
				_, err = legacy.Embed(t.Context(), "toolong")
				require.Equal(t, int32(2), calls.Load())
				require.EqualError(t, err, "openai: HTTP status 400")
			}
		})
	}
}

func TestBedrockProfileSeparatesProviderFailures(t *testing.T) {
	t.Setenv("EMBEDDING_INPUT_PROFILE", "")
	profile := testInputProfile(t, `{"provider":"bedrock","model":"amazon.titan-embed-text-v2:0","revision":"0","max_characters":6}`,
		embeddinginput.Target{Provider: "bedrock", Model: "amazon.titan-embed-text-v2:0"})
	for _, tc := range []struct {
		code, message string
		status        int
		want          error
	}{
		{"ValidationException", "400 Bad Request: Too many input tokens. Max input tokens: 4", http.StatusBadRequest, domain.ErrEmbeddingModelLimit},
		{"ValidationException", "Input is too long for requested model.", http.StatusBadRequest, domain.ErrEmbeddingModelLimit},
		{"ValidationException", "The model identifier is invalid.", http.StatusBadRequest, nil},
		{"AccessDeniedException", "Too many input tokens.", http.StatusForbidden, domain.ErrEmbeddingDenied},
		{"ThrottlingException", "Too many input tokens.", http.StatusTooManyRequests, nil},
	} {
		t.Run(tc.code+"/"+tc.message, func(t *testing.T) {
			var calls atomic.Int32
			e := bedrockTestEmbedder(t, func(w http.ResponseWriter, _ *http.Request) {
				calls.Add(1)
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("X-Amzn-Errortype", tc.code)
				w.WriteHeader(tc.status)
				_, _ = io.WriteString(w, `{"message":"`+tc.message+`"}`)
			})
			e.profile = profile
			_, err := e.Embed(t.Context(), "toolong")
			require.ErrorIs(t, err, embeddinginput.ErrInputLimit)
			require.Zero(t, calls.Load())
			_, err = e.Embed(t.Context(), "sample")
			require.Equal(t, int32(1), calls.Load())
			require.Error(t, err)
			if tc.want == nil {
				require.NotErrorIs(t, err, domain.ErrEmbeddingModelLimit)
				require.ErrorContains(t, err, tc.code)
			} else {
				require.ErrorIs(t, err, tc.want)
				require.NotContains(t, err.Error(), tc.message)
			}
		})
	}
}

func testInputProfile(t *testing.T, definition string, target embeddinginput.Target) *embeddinginput.Profile {
	t.Helper()
	path := filepath.Join(t.TempDir(), "embedding.json")
	require.NoError(t, os.WriteFile(path, []byte(definition), 0o600))
	profile, err := embeddinginput.Load(path, target)
	require.NoError(t, err)
	return profile
}
