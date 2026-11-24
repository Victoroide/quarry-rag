package evaluation

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUpstreamProofRequiresProviderHostnameAndVerifiedCertificate(t *testing.T) {
	const openAI = "api.openai.com"
	const bedrock = "bedrock-runtime.us-east-1.amazonaws.com"
	observe := func(ctx context.Context, call UpstreamCall, provider, host, certificateName string) {
		response := &http.Response{
			StatusCode: http.StatusOK,
			Request:    &http.Request{URL: &url.URL{Scheme: "https", Host: host}},
		}
		if certificateName != "" {
			response.TLS = &tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{{DNSNames: []string{certificateName}}}}}
		}
		ObserveUpstream(ctx, call, provider, response)
	}
	for _, test := range []struct {
		name, host, certificateName, want string
	}{
		{"known peers", bedrock, bedrock, openAI + "," + bedrock},
		{"compatible local", "127.0.0.1", "127.0.0.1", ""},
		{"unrelated certificate", bedrock, "local.example", ""},
		{"provider lookalike", bedrock + ".local.example", bedrock + ".local.example", ""},
		{"unexpected port", bedrock + ":8443", bedrock, ""},
		{"missing TLS verification", bedrock, "", ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, proof := TrackUpstreams(t.Context())
			observe(ctx, EmbeddingCall, "openai", openAI, openAI)
			require.Empty(t, proof.Header())
			observe(ctx, SynthesisCall, "bedrock", test.host, test.certificateName)
			require.Equal(t, test.want, proof.Header())
			require.Equal(t, test.want != "", ValidUpstreams(proof.Header(), "openai", "bedrock"))
			observe(ctx, SynthesisCall, "bedrock", "local.example", "local.example")
			require.Empty(t, proof.Header())
		})
	}
}
