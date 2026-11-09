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
	"strings"
	"unicode"

	"github.com/Victoroide/quarry-rag/internal/domain"
)

const maxOpenAIResponseBytes = 4 << 20

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
	return &OpenAIHTTPClient{base: strings.TrimRight(base, "/"), key: key, client: &http.Client{CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}

// Post sends a JSON request; the caller must close the successful response body.
func (c *OpenAIHTTPClient) Post(ctx context.Context, path string, payload any) (*http.Response, error) {
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
	response, err := c.client.Do(request)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, errors.New("openai: HTTP request failed")
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		_ = response.Body.Close()
		if response.StatusCode == 429 || response.StatusCode == 503 {
			return nil, fmt.Errorf("openai: HTTP status %d: %w", response.StatusCode, domain.ErrUpstreamThrottled)
		}
		return nil, fmt.Errorf("openai: HTTP status %d", response.StatusCode)
	}
	return response, nil
}

// Decode reads one bounded JSON response without exposing upstream response content.
func (c *OpenAIHTTPClient) Decode(ctx context.Context, path string, payload, result any) error {
	response, err := c.Post(ctx, path, payload)
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
