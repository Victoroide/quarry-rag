package embedder

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
)

func TestOpenAIEmbedding(t *testing.T) {
	for _, tc := range []struct {
		name, response string
		invalid        bool
	}{
		{"valid", `{"data":[{"index":0,"embedding":[0.1,0.2,0.3]}]}`, false},
		{"dimension mismatch", `{"data":[{"index":0,"embedding":[0.1]}]}`, true},
		{"missing data", `{"data":[]}`, true},
		{"wrong index", `{"data":[{"index":1,"embedding":[0.1,0.2,0.3]}]}`, true},
		{"null element", `{"data":[{"index":0,"embedding":[0.1,null,0.3]}]}`, true},
		{"multiple embeddings", `{"data":[{"index":0,"embedding":[0.1,0.2,0.3]},{"index":1,"embedding":[0.1,0.2,0.3]}]}`, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/v1/embeddings" || r.Method != http.MethodPost || r.Header.Get("Authorization") != "Bearer synthetic-key" {
					t.Error("incorrect embedding request")
				}
				var body struct {
					Model, Input, EncodingFormat string
				}
				var raw map[string]json.RawMessage
				if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
					t.Error(err)
				}
				if err := json.Unmarshal(raw["model"], &body.Model); err != nil {
					t.Error(err)
				}
				if err := json.Unmarshal(raw["input"], &body.Input); err != nil {
					t.Error(err)
				}
				if _, ok := raw["dimensions"]; ok {
					t.Error("dimensions must not be requested")
				}
				if err := json.Unmarshal(raw["encoding_format"], &body.EncodingFormat); err != nil {
					t.Error(err)
				}
				if body.Model != "embed" || body.Input != "synthetic document" || body.EncodingFormat != "float" {
					t.Error("incorrect embedding body")
				}
				_, _ = fmt.Fprint(w, tc.response)
			}))
			defer server.Close()
			client, err := NewOpenAIEmbedder(&config.Config{OpenAIBaseURL: server.URL + "/v1", OpenAIAPIKey: "synthetic-key", OpenAIEmbeddingModel: "embed", EmbeddingDimensions: 3, EmbedConcurrency: 1, EmbedTimeout: time.Second})
			if err != nil {
				t.Fatal(err)
			}
			vector, err := client.Embed(context.Background(), "synthetic document")
			if tc.invalid {
				if !errors.Is(err, ErrInvalidEmbedding) {
					t.Fatalf("expected invalid embedding, got %v", err)
				}
				return
			}
			if err != nil || len(vector) != 3 || vector[1] != float32(.2) {
				t.Fatalf("unexpected embedding: %v %v", vector, err)
			}
		})
	}
}

func TestOpenAIEmbeddingThrottling(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(429); _, _ = fmt.Fprint(w, "private") }))
	defer server.Close()
	client, err := NewOpenAIEmbedder(&config.Config{OpenAIBaseURL: server.URL, OpenAIEmbeddingModel: "embed", EmbeddingDimensions: 3, EmbedConcurrency: 1, EmbedTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Embed(context.Background(), "synthetic")
	if !errors.Is(err, domain.ErrUpstreamThrottled) || strings.Contains(err.Error(), "private") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestOpenAIEmbeddingConcurrencyAndCancellation(t *testing.T) {
	started := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		started <- struct{}{}
		<-r.Context().Done()
	}))
	defer server.Close()
	client, err := NewOpenAIEmbedder(&config.Config{OpenAIBaseURL: server.URL, OpenAIEmbeddingModel: "embed", EmbeddingDimensions: 1, EmbedConcurrency: 1, EmbedTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	firstCtx, cancel := context.WithCancel(t.Context())
	defer cancel()
	result := make(chan error, 1)
	go func() { _, callErr := client.Embed(firstCtx, "synthetic"); result <- callErr }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("embedding did not start")
	}
	waitCtx, waitCancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer waitCancel()
	_, err = client.Embed(waitCtx, "synthetic")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waiting call ignored cancellation: %v", err)
	}
	cancel()
	select {
	case err = <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("active call ignored cancellation: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("active call did not cancel")
	}
}
