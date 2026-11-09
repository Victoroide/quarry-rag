package embedder

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/llm"
	"golang.org/x/sync/semaphore"
)

// OpenAIEmbedder generates bounded embeddings from an OpenAI-compatible endpoint.
type OpenAIEmbedder struct {
	http       *llm.OpenAIHTTPClient
	model      string
	dimensions int
	sem        *semaphore.Weighted
	timeout    time.Duration
}

// NewOpenAIEmbedder creates an embedder with the configured concurrency and deadline.
func NewOpenAIEmbedder(cfg *config.Config) (*OpenAIEmbedder, error) {
	transport, err := llm.NewOpenAIHTTPClient(cfg.OpenAIBaseURL, cfg.OpenAIAPIKey)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(cfg.OpenAIEmbeddingModel) == "" || cfg.EmbeddingDimensions < 1 || cfg.EmbeddingDimensions > 4096 || cfg.EmbedConcurrency < 1 || cfg.EmbedTimeout <= 0 {
		return nil, errors.New("openai: invalid embedding configuration")
	}
	return &OpenAIEmbedder{transport, cfg.OpenAIEmbeddingModel, cfg.EmbeddingDimensions, semaphore.NewWeighted(int64(cfg.EmbedConcurrency)), cfg.EmbedTimeout}, nil
}

// Embed validates dimensions, cardinality and finite values before returning a vector.
func (e *OpenAIEmbedder) Embed(ctx context.Context, text string) ([]float32, error) {
	if err := e.sem.Acquire(ctx, 1); err != nil {
		return nil, err
	}
	defer e.sem.Release(1)
	ctx, cancel := context.WithTimeout(ctx, e.timeout)
	defer cancel()
	request := struct {
		Model          string `json:"model"`
		Input          string `json:"input"`
		EncodingFormat string `json:"encoding_format"`
	}{e.model, text, "float"}
	var response struct {
		Data []struct {
			Index     int        `json:"index"`
			Embedding []*float32 `json:"embedding"`
		} `json:"data"`
	}
	if err := e.http.Decode(ctx, "/embeddings", request, &response); err != nil {
		return nil, err
	}
	if len(response.Data) != 1 || response.Data[0].Index != 0 || len(response.Data[0].Embedding) != e.dimensions {
		return nil, fmt.Errorf("%w: unexpected dimensions or response cardinality", ErrInvalidEmbedding)
	}
	vector := make([]float32, e.dimensions)
	for i, value := range response.Data[0].Embedding {
		if value == nil || math.IsNaN(float64(*value)) || math.IsInf(float64(*value), 0) {
			return nil, fmt.Errorf("%w: non-finite or missing value", ErrInvalidEmbedding)
		}
		vector[i] = *value
	}
	return vector, nil
}
