package llm

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"unicode"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/evaluation"
)

const maxOpenAIResponseBytes = 4 << 20

const maxOpenAIErrorBytes = 8 << 10

// The embeddings API can return a null code for this error.
var embeddingWindowMessage = regexp.MustCompile(`^This model's maximum context length is [0-9]+ tokens, however you requested [0-9]+ tokens\b`)

// OpenAIHTTPClient shares bounded, credential-safe HTTP transport between providers.
type OpenAIHTTPClient struct {
	base   string
	key    string
	client *http.Client
}

// NewOpenAIHTTPClient creates a transport without implicit endpoints or redirects.
func NewOpenAIHTTPClient(base, key string) (*OpenAIHTTPClient, error) {
	address, err := url.Parse(base)
	if err != nil || address.Hostname() == "" || (address.Scheme != "http" && address.Scheme != "https") || address.User != nil || address.RawQuery != "" || address.ForceQuery || address.Fragment != "" || strings.ContainsAny(base, "\r\n\t ") {
		return nil, errors.New("openai: invalid base address")
	}
	if strings.ContainsFunc(key, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }) {
		return nil, errors.New("openai: invalid API key")
	}
	return &OpenAIHTTPClient{base: normalizeOpenAIBase(base), key: key, client: &http.Client{CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}

// Post sends a JSON request; the caller must close the successful response body.
func (c *OpenAIHTTPClient) Post(ctx context.Context, path string, payload any) (*http.Response, error) {
	return c.post(ctx, path, payload, false)
}

func (c *OpenAIHTTPClient) post(ctx context.Context, path string, payload any, embeddingErrors bool) (*http.Response, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, errors.New("openai: invalid request")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+path, bytes.NewReader(body))
	if err != nil {
		return nil, errors.New("openai: invalid request address")
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json, text/event-stream")
	if c.key != "" {
		request.Header.Set("Authorization", "Bearer "+c.key)
	}
	if scopeErr := domain.CheckEvidenceBeforeSend(ctx); scopeErr != nil {
		return nil, scopeErr
	}
	if domain.HasEvidenceCheck(ctx) {
		request.GetBody = nil
	}
	response, err := c.client.Do(request)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, errors.New("openai: HTTP request failed")
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		defer func() { _ = response.Body.Close() }()
		return nil, openAIStatusError(ctx, response, embeddingErrors)
	}
	switch path {
	case "/embeddings":
		evaluation.ObserveUpstream(ctx, evaluation.EmbeddingCall, "openai", response)
	case "/chat/completions":
		evaluation.ObserveUpstream(ctx, evaluation.SynthesisCall, "openai", response)
	}
	return response, nil
}

func openAIStatusError(ctx context.Context, response *http.Response, embeddingErrors bool) error {
	code := response.StatusCode
	if code == http.StatusTooManyRequests || code == http.StatusServiceUnavailable {
		return fmt.Errorf("openai: HTTP status %d: %w", code, domain.ErrUpstreamThrottled)
	}
	if embeddingErrors {
		switch code {
		case http.StatusUnauthorized, http.StatusForbidden:
			return domain.ErrEmbeddingDenied
		case http.StatusBadRequest:
			body, err := io.ReadAll(io.LimitReader(response.Body, maxOpenAIErrorBytes+1))
			if ctx.Err() != nil {
				return ctx.Err()
			}
			var payload struct {
				Error struct {
					Code    string `json:"code"`
					Type    string `json:"type"`
					Message string `json:"message"`
				} `json:"error"`
			}
			if err == nil && len(body) <= maxOpenAIErrorBytes && json.Unmarshal(body, &payload) == nil &&
				(payload.Error.Code == "context_length_exceeded" ||
					(payload.Error.Code == "" && payload.Error.Type == "invalid_request_error" && embeddingWindowMessage.MatchString(payload.Error.Message))) {
				return domain.ErrEmbeddingModelLimit
			}
		}
	}
	return fmt.Errorf("openai: HTTP status %d", code)
}

// Decode reads one bounded JSON response without exposing upstream response content.
func (c *OpenAIHTTPClient) Decode(ctx context.Context, path string, payload, result any) error {
	return c.decode(ctx, path, payload, result, false)
}

// DecodeEmbedding reads an embedding response and optionally classifies provider window errors.
func (c *OpenAIHTTPClient) DecodeEmbedding(ctx context.Context, payload, result any, embeddingErrors bool) error {
	return c.decode(ctx, "/embeddings", payload, result, embeddingErrors)
}

func (c *OpenAIHTTPClient) decode(ctx context.Context, path string, payload, result any, embeddingErrors bool) error {
	response, err := c.post(ctx, path, payload, embeddingErrors)
	if err != nil {
		return err
	}
	defer func() { _ = response.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(response.Body, maxOpenAIResponseBytes+1))
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return errors.New("openai: response read failed")
	}
	if len(body) > maxOpenAIResponseBytes {
		return errors.New("openai: response exceeds size limit")
	}
	if json.Unmarshal(body, result) != nil {
		return errors.New("openai: invalid JSON response")
	}
	return nil
}
