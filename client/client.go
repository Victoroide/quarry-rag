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

	"github.com/Victoroide/quarry-rag/internal/credential"
)

// Client calls the HTTP API without retries and is safe for concurrent use.
type Client struct {
	baseURL    string
	token      string
	subject    SubjectCredentialProvider
	httpClient *http.Client
}

type options struct {
	token      string
	subject    SubjectCredentialProvider
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
// Redirects are disabled to confine credentials to the configured endpoint.
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
	if o.subject != nil && o.token == "" {
		return nil, errors.New("service token is required with a subject credential provider")
	}
	hc := *o.httpClient
	if o.timeout != nil {
		hc.Timeout = *o.timeout
	}
	hc.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &Client{baseURL: strings.TrimRight(u.String(), "/"), token: o.token, subject: o.subject, httpClient: &hc}, nil
}

func (c *Client) jsonRequest(ctx context.Context, method, path string, body, output any, opts ...RequestOption) error {
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
	req, credentials, err := c.newRequest(ctx, method, path, reader, contentType, "application/json", opts...)
	if err != nil {
		return err
	}
	return c.doJSON(req, credentials, output)
}

func (c *Client) newRequest(ctx context.Context, method, path string, body io.Reader, contentType, accept string, opts ...RequestOption) (*http.Request, requestCredentials, error) {
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, body)
	credentials := requestCredentials{service: c.token}
	if err != nil {
		return nil, credentials, fmt.Errorf("create request: %w", credentials.safeError(err))
	}
	scheme, host := req.URL.Scheme, req.URL.Host
	req.Header.Set("Accept", accept)
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	if c.subject == nil && c.token != "" {
		req.Header.Set("X-Service-Token", c.token)
	}
	for _, opt := range opts {
		if opt == nil {
			return nil, credentials, errors.New("request option is required")
		}
		opt(req)
	}
	if c.subject == nil {
		return req, credentials, nil
	}
	if req.URL == nil || req.URL.Scheme != scheme || req.URL.Host != host || req.URL.User != nil || req.URL.Opaque != "" || req.Host != host {
		return nil, credentials, errors.New("request option changed the credential destination")
	}
	subject, err := c.subjectCredential(req)
	if err != nil {
		return nil, credentials, err
	}
	credentials.subject = subject
	for name := range req.Header {
		if strings.EqualFold(name, "Authorization") || strings.EqualFold(name, "X-Service-Token") {
			delete(req.Header, name)
		}
	}
	if req.Header == nil {
		req.Header = make(http.Header)
	}
	req.Header.Set("X-Service-Token", c.token)
	req.Header.Set("Authorization", "Bearer "+subject.Token)
	return req, credentials, nil
}

func (c *Client) subjectCredential(req *http.Request) (SubjectCredential, error) {
	ctx := req.Context()
	if err := ctx.Err(); err != nil {
		return SubjectCredential{}, err
	}
	subject, err := c.subject(ctx)
	if contextErr := ctx.Err(); contextErr != nil {
		return SubjectCredential{}, contextErr
	}
	if err != nil || !time.Now().Before(subject.ExpiresAt) {
		return SubjectCredential{}, ErrSubjectCredential
	}
	if _, _, ok := credential.CompactJWT(subject.Token); !ok {
		return SubjectCredential{}, ErrSubjectCredential
	}
	return subject, nil
}

func (c *Client) perform(req *http.Request, credentials requestCredentials) (*http.Response, error) {
	if credentials.subject.Token != "" {
		if err := req.Context().Err(); err != nil {
			return nil, err
		}
		if !time.Now().Before(credentials.subject.ExpiresAt) {
			return nil, ErrSubjectCredential
		}
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("perform request: %w", credentials.safeError(err))
	}
	return resp, nil
}

func (c *Client) doJSON(req *http.Request, credentials requestCredentials, output any) error {
	resp, err := c.perform(req, credentials)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return decodeAPIError(resp, credentials)
	}
	decoder := json.NewDecoder(resp.Body)
	if err := decoder.Decode(output); err != nil {
		return fmt.Errorf("decode response: %w", credentials.safeError(err))
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return errors.New("response contains trailing data")
	}
	return nil
}
