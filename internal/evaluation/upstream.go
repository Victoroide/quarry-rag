package evaluation

import (
	"context"
	"net/http"
	"regexp"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
)

// UpstreamsHeader carries the authenticated embedding and synthesis origins.
const UpstreamsHeader = "X-Quarry-Evaluation-Upstreams"

// UpstreamCall identifies one stage of a scoped evaluation query.
type UpstreamCall int

const (
	// EmbeddingCall tracks the request that produces the query vector.
	EmbeddingCall UpstreamCall = iota
	// SynthesisCall tracks the request that produces the answer.
	SynthesisCall
)

type upstreamKey struct{}

// UpstreamProof records the actual HTTP peer for each provider operation.
type UpstreamProof struct {
	mu          sync.Mutex
	host        [2]string
	conflicting [2]bool
}

// TrackUpstreams enables transport observations for the lifetime of one query.
func TrackUpstreams(ctx context.Context) (context.Context, *UpstreamProof) {
	proof := new(UpstreamProof)
	return context.WithValue(ctx, upstreamKey{}, proof), proof
}

var bedrockOrigin = regexp.MustCompile(`^bedrock-runtime\.[a-z]{2}(?:-[a-z]+)+-[0-9]+\.amazonaws\.com(?:\.cn)?$`)

func isProviderOrigin(provider, host string) bool {
	switch provider {
	case "openai":
		return host == "api.openai.com"
	case "bedrock":
		return bedrockOrigin.MatchString(host)
	default:
		return false
	}
}

// ValidUpstreams requires a known hosted origin for each configured provider.
func ValidUpstreams(value, embedding, synthesis string) bool {
	left, right, ok := strings.Cut(value, ",")
	return ok && isProviderOrigin(embedding, left) && isProviderOrigin(synthesis, right)
}

// ObserveUpstream records successful calls to a recognized authenticated peer.
// A successful call to any other peer keeps the stage unverified.
func ObserveUpstream(ctx context.Context, call UpstreamCall, provider string, response *http.Response) {
	proof, _ := ctx.Value(upstreamKey{}).(*UpstreamProof)
	if proof == nil || response == nil || response.StatusCode < 200 || response.StatusCode >= 300 {
		return
	}
	host := verifiedOrigin(provider, response)
	proof.mu.Lock()
	defer proof.mu.Unlock()
	if host == "" || proof.host[call] != "" && proof.host[call] != host {
		proof.conflicting[call] = true
	}
	proof.host[call] = host
}

func verifiedOrigin(provider string, response *http.Response) string {
	if response.Request == nil || response.Request.URL == nil || response.TLS == nil {
		return ""
	}
	u, tls := response.Request.URL, response.TLS
	host := u.Hostname()
	if u.Scheme != "https" || u.Port() != "" && u.Port() != "443" ||
		!isProviderOrigin(provider, host) || len(tls.VerifiedChains) == 0 ||
		len(tls.VerifiedChains[0]) == 0 || tls.VerifiedChains[0][0].VerifyHostname(host) != nil {
		return ""
	}
	return host
}

// Header returns both authenticated origins, when all calls were verified.
func (p *UpstreamProof) Header() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.conflicting[0] || p.conflicting[1] || p.host[0] == "" || p.host[1] == "" {
		return ""
	}
	return p.host[0] + "," + p.host[1]
}

type bedrockObserver struct {
	base aws.HTTPClient
	call UpstreamCall
}

// ObserveBedrockHTTP wraps the SDK's configured transport without changing its defaults.
func ObserveBedrockHTTP(base aws.HTTPClient, call UpstreamCall) aws.HTTPClient {
	return bedrockObserver{base: base, call: call}
}

func (o bedrockObserver) Do(req *http.Request) (*http.Response, error) {
	resp, err := o.base.Do(req)
	if err == nil {
		ObserveUpstream(req.Context(), o.call, "bedrock", resp)
	}
	return resp, err
}
