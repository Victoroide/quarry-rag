package main

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"time"

	"github.com/Victoroide/quarry-rag/client"
	"github.com/Victoroide/quarry-rag/internal/evaluation"
)

var (
	errRequestBudget = errors.New("HTTP operation budget exhausted")
	errHardBudget    = errors.New("provider token or spend ceiling cannot be guaranteed")
	errBodyLimit     = errors.New("response body exceeds limit")
)

type requestBudget struct {
	limit      int
	used       int
	hardBudget bool
}

type exchangeKey struct{}

type exchange struct {
	namespace, receipt, incarnation    string
	noProof                            bool
	started                            time.Time
	state, revision, proof             string
	execution, upstreams, databaseTime string
}

func fixtureExchange(ctx context.Context, l *ledger, incarnation string) (context.Context, *exchange) {
	e := &exchange{namespace: l.NamespaceID, receipt: l.Receipt, incarnation: incarnation}
	return context.WithValue(ctx, exchangeKey{}, e), e
}

func singleHeader(headers http.Header, name string) string {
	values := headers.Values(name)
	if len(values) != 1 {
		return ""
	}
	return values[0]
}

type budgetTransport struct {
	base   http.RoundTripper
	budget *requestBudget
}

func (t budgetTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if err := r.Context().Err(); err != nil {
		return nil, err
	}
	if t.budget.hardBudget {
		return nil, errHardBudget
	}
	if t.budget.used >= t.budget.limit {
		return nil, errRequestBudget
	}
	capture, _ := r.Context().Value(exchangeKey{}).(*exchange)
	if capture != nil {
		r = r.Clone(r.Context())
		r.Header.Set(evaluation.FixtureHeader, capture.namespace)
		if capture.receipt != "" {
			r.Header.Set(evaluation.ReceiptHeader, capture.receipt)
		}
		if capture.incarnation != "" {
			r.Header.Set(evaluation.IncarnationHeader, capture.incarnation)
		}
		if capture.noProof {
			r.Header.Set(evaluation.ObserveHeader, "off")
		}
		capture.started = time.Now()
	}
	t.budget.used++
	resp, err := t.base.RoundTrip(r)
	if err != nil {
		return nil, err
	}
	if capture != nil {
		capture.state = singleHeader(resp.Header, evaluation.StateHeader)
		capture.revision = singleHeader(resp.Header, evaluation.IncarnationHeader)
		capture.proof = singleHeader(resp.Header, evaluation.MatchesHeader)
		capture.databaseTime = singleHeader(resp.Header, evaluation.DatabaseTimeHeader)
		execution := singleHeader(resp.Header, evaluation.ExecutionHeader)
		if resp.TLS != nil && len(resp.TLS.VerifiedChains) > 0 {
			capture.execution = execution
			capture.upstreams = singleHeader(resp.Header, evaluation.UpstreamsHeader)
		}
		if execution == "offline" {
			capture.execution = execution
		}
	}
	resp.Body = &boundedBody{ReadCloser: resp.Body, remaining: 2 << 20}
	return resp, nil
}

func rejectedIngest(err error) bool {
	var apiErr *client.APIError
	if !errors.As(err, &apiErr) {
		return false
	}
	switch apiErr.Code {
	case "unauthorized":
		return apiErr.StatusCode == http.StatusUnauthorized
	case "conflict":
		return apiErr.StatusCode == http.StatusConflict
	case "queue":
		return apiErr.StatusCode == http.StatusServiceUnavailable
	case "validation":
		return apiErr.StatusCode == http.StatusBadRequest || apiErr.StatusCode == http.StatusUnsupportedMediaType ||
			apiErr.StatusCode == http.StatusRequestEntityTooLarge
	case "precondition":
		return apiErr.StatusCode == http.StatusPreconditionFailed
	default:
		return false
	}
}

type boundedBody struct {
	io.ReadCloser
	remaining int
}

func (b *boundedBody) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if b.remaining == 0 {
		var byteAtLimit [1]byte
		n, err := b.ReadCloser.Read(byteAtLimit[:])
		if n != 0 {
			return 0, errBodyLimit
		}
		return 0, err
	}
	n, err := b.ReadCloser.Read(p[:min(len(p), b.remaining)])
	b.remaining -= n
	return n, err
}

func newLiveClient(target targetConfig, token string, budget *requestBudget, timeout time.Duration) (*client.Client, error) {
	protocols := new(http.Protocols)
	protocols.SetHTTP1(true)
	dialer := &net.Dialer{Timeout: timeout}
	transport := &http.Transport{
		DialContext: dialer.DialContext, Protocols: protocols, DisableKeepAlives: true,
		TLSHandshakeTimeout: timeout, MaxResponseHeaderBytes: 32 << 10,
	}
	// Fresh HTTP/1 connections keep automatic retries out of the operation cap.
	hc := &http.Client{Timeout: timeout, Transport: budgetTransport{base: transport, budget: budget}}
	return client.New(target.BaseURL, client.WithServiceToken(token), client.WithHTTPClient(hc))
}

func failure(err error) (reason string, status int) {
	var apiErr *client.APIError
	switch {
	case errors.Is(err, errRequestBudget):
		return "request_budget", 0
	case errors.Is(err, errHardBudget):
		return "unverifiable_provider_budget", 0
	case errors.Is(err, context.Canceled):
		return "canceled", 0
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline", 0
	case errors.Is(err, errBodyLimit):
		return "response_too_large", 0
	case errors.As(err, &apiErr):
		switch {
		case apiErr.StatusCode == http.StatusTooManyRequests || apiErr.Code == "upstream_throttled":
			return "throttled", apiErr.StatusCode
		case apiErr.StatusCode == http.StatusNotFound:
			return "not_found", apiErr.StatusCode
		// The deployed API uses the same status and code for input-count failures.
		case apiErr.StatusCode == http.StatusBadGateway && apiErr.Code == "synthesis" &&
			apiErr.Message == "answer synthesis returned no text":
			return "empty_synthesis", apiErr.StatusCode
		default:
			return "http_error", apiErr.StatusCode
		}
	default:
		return "client_error", 0
	}
}
