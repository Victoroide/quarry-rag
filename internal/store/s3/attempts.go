package s3

import (
	"errors"
	"net/http"
	"net/http/httptrace"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
)

// putAttempts wraps the HTTP client of one PUT operation and records what each
// attempt that reached the transport could have delivered. Failures before
// the transport, such as credentials and signing, never reach it.
type putAttempts struct {
	inner    aws.HTTPClient
	mu       sync.Mutex
	attempts []*attemptEvidence
}

type attemptEvidence struct {
	connRequested, connObtained bool
	responded, failed           bool
	status                      int
	s3RequestID                 bool
}

func (p *putAttempts) Do(req *http.Request) (*http.Response, error) {
	a := &attemptEvidence{}
	p.mu.Lock()
	p.attempts = append(p.attempts, a)
	p.mu.Unlock()
	if p.inner == nil {
		p.update(func() { a.failed = true })
		return nil, errors.New("s3: missing http client")
	}
	trace := &httptrace.ClientTrace{
		GetConn: func(string) { p.update(func() { a.connRequested = true }) },
		GotConn: func(httptrace.GotConnInfo) { p.update(func() { a.connObtained = true }) },
	}
	resp, err := p.inner.Do(req.WithContext(httptrace.WithClientTrace(req.Context(), trace)))
	p.update(func() {
		a.failed = err != nil
		if resp != nil {
			a.responded, a.status = true, resp.StatusCode
			a.s3RequestID = resp.Header.Get("X-Amz-Request-Id") != ""
		}
	})
	return resp, err
}

func (p *putAttempts) update(change func()) {
	p.mu.Lock()
	defer p.mu.Unlock()
	change()
}

// accounted reports whether the attempt can no longer store the object.
func (a *attemptEvidence) accounted() bool {
	switch {
	case a.responded && !a.failed && a.status >= 200 && a.status < 300:
		return true // S3 stored the object and answered.
	case a.responded && !a.failed && a.status >= 300 && a.status < 500 && a.s3RequestID:
		return true // S3 answered and refused this request.
	case !a.responded && a.failed && a.connRequested && !a.connObtained:
		return true // No connection carried the request.
	default:
		return false
	}
}

// settled reports whether no attempt of the operation can still store the
// object. A successful operation must have passed through the recorder.
func (p *putAttempts) settled(succeeded bool) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if succeeded && len(p.attempts) == 0 {
		return false
	}
	for _, a := range p.attempts {
		if !a.accounted() {
			return false
		}
	}
	return true
}
