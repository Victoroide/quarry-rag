package client

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
	"time"
)

// Client calls the HTTP API without retries and is safe for concurrent use.
type Client struct {
	baseURL    string
	token      string
	httpClient *http.Client
}

type options struct {
	token      string
	httpClient *http.Client
	timeout    *time.Duration
}

// Option configures a Client.
type Option func(*options) error

// WithServiceToken sets the X-Service-Token header on API requests.
func WithServiceToken(token string) Option {
	return func(o *options) error { o.token = token; return nil }
}

// WithHTTPClient uses a copy of the supplied client, preserving its transport and timeout.
// Redirects are disabled to confine service credentials to the configured endpoint.
func WithHTTPClient(c *http.Client) Option {
	return func(o *options) error {
		if c == nil {
			return errors.New("HTTP client is required")
		}
		o.httpClient = c
		return nil
	}
}

// WithTimeout overrides the HTTP timeout; zero disables it. Context deadlines still apply.
func WithTimeout(timeout time.Duration) Option {
	return func(o *options) error {
		if timeout < 0 {
			return errors.New("timeout must not be negative")
		}
		o.timeout = &timeout
		return nil
	}
}

// New constructs a client for an absolute HTTP or HTTPS base URL, including any path prefix.
// The default timeout is 90 seconds. Redirects are returned as API errors.
func New(baseURL string, opts ...Option) (*Client, error) {
	u, err := url.Parse(baseURL)
	if err != nil || u == nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Opaque != "" {
		return nil, errors.New("base URL must be absolute HTTP or HTTPS without credentials, query or fragment")
	}
	o := options{httpClient: &http.Client{Timeout: 90 * time.Second}}
	for _, opt := range opts {
		if opt == nil {
			return nil, errors.New("client option is required")
		}
		if err := opt(&o); err != nil {
			return nil, err
		}
	}
	hc := *o.httpClient
	if o.timeout != nil {
		hc.Timeout = *o.timeout
	}
	hc.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &Client{baseURL: strings.TrimRight(u.String(), "/"), token: o.token, httpClient: &hc}, nil
}

func (c *Client) jsonRequest(ctx context.Context, method, path string, body, output any) error {
	var reader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("encode request: %w", err)
		}
		reader = bytes.NewReader(data)
	}
	contentType := ""
	if body != nil {
		contentType = "application/json"
	}
	return c.request(ctx, method, path, reader, contentType, output)
}

func (c *Client) request(ctx context.Context, method, path string, body io.Reader, contentType string, output any) error {
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, body)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	if c.token != "" {
		req.Header.Set("X-Service-Token", c.token)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("perform request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return decodeAPIError(resp)
	}
	decoder := json.NewDecoder(resp.Body)
	if err := decoder.Decode(output); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return errors.New("response contains trailing data")
	}
	return nil
}
