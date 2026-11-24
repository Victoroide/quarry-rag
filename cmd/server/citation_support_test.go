package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/client"
	"github.com/Victoroide/quarry-rag/internal/citationsupport"
	"github.com/Victoroide/quarry-rag/internal/testutil/pgtest"
	"github.com/stretchr/testify/require"
)

func TestOfflineServerMakesNoCitationVerifierRequests(t *testing.T) {
	const child = "QUARRY_TEST_OFFLINE_CITATION_CHILD"
	if os.Getenv(child) == "1" {
		require.NoError(t, run())
		return
	}
	if testing.Short() {
		t.Skip("requires a Docker daemon")
	}
	const model = "fixture@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	const token = "fixture-service-token"
	profile := citationsupport.PairEvaluation{
		ClaimLanguage: "en", SourceLanguage: "en", EvidenceMode: citationsupport.Single,
		Thresholds: &citationsupport.Scores{Supported: .85, Contradicted: .85, Insufficient: .85},
	}
	for _, row := range []struct {
		label    citationsupport.Status
		category string
		scores   citationsupport.Scores
	}{
		{citationsupport.Supported, "", citationsupport.Scores{Supported: .98, Contradicted: .01, Insufficient: .01}},
		{citationsupport.Contradicted, citationsupport.BenignContrast, citationsupport.Scores{Supported: .01, Contradicted: .98, Insufficient: .01}},
		{citationsupport.Insufficient, "", citationsupport.Scores{Supported: .01, Contradicted: .01, Insufficient: .98}},
	} {
		for i := range 30 {
			profile.HeldOut = append(profile.HeldOut, citationsupport.Sample{
				Documents: []string{fmt.Sprintf("%s-%d", row.label, i)}, EvidenceCount: 1, Label: row.label, Category: row.category,
				Prediction: &citationsupport.Prediction{ClaimLanguage: "en", EvidenceLanguages: []string{"en"}, Scores: &row.scores},
			})
		}
	}
	calibration := citationsupport.Calibration{
		Model: model, BlindedNativeLabels: true, DevelopmentDocuments: []string{"development"}, Pairs: []citationsupport.PairEvaluation{profile},
	}
	reports, err := calibration.Evaluate()
	require.NoError(t, err)
	require.True(t, reports[0].Enabled)
	data, err := json.Marshal(calibration)
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "fixture-calibration.json")
	require.NoError(t, os.WriteFile(path, data, 0o600))
	var calls atomic.Int32
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	t.Cleanup(endpoint.Close)
	l, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	_, port, err := net.SplitHostPort(addr)
	require.NoError(t, err)
	require.NoError(t, l.Close())

	t.Setenv("GO_RAG_FAKE_MODE", "true")
	t.Setenv("DATABASE_URL", pgtest.Start(t))
	t.Setenv("SERVER_PORT", port)
	t.Setenv("METRICS_ADDR", "")
	t.Setenv("SERVICE_TOKENS", "")
	t.Setenv("SERVICE_TOKEN", token)
	t.Setenv("INGEST_POLL_INTERVAL", "100ms")
	t.Setenv("SHUTDOWN_DRAIN_DELAY", "0s")
	t.Setenv("CITATION_SUPPORT_MODEL", model)
	t.Setenv("CITATION_SUPPORT_ENDPOINT", endpoint.URL)
	t.Setenv("CITATION_SUPPORT_TOKEN", "")
	t.Setenv("CITATION_SUPPORT_CALIBRATION_FILE", path)
	t.Setenv(child, "1")

	ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), 30*time.Second)
	t.Cleanup(cancel)
	cmd := testBinaryCommand(ctx, t, "TestOfflineServerMakesNoCitationVerifierRequests")
	cmd.Stderr, cmd.Stdout = os.Stderr, os.Stdout
	require.NoError(t, cmd.Start())
	t.Cleanup(func() {
		if signalErr := cmd.Process.Signal(syscall.SIGTERM); signalErr != nil {
			cancel()
		}
		require.NoError(t, cmd.Wait())
	})

	base := (&url.URL{Scheme: "http", Host: addr}).String()
	api, err := client.New(base, client.WithServiceToken(token), client.WithTimeout(500*time.Millisecond))
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		_, listErr := api.ListDocuments(ctx, client.ListOptions{})
		return listErr == nil
	}, 10*time.Second, 50*time.Millisecond, "server readiness")
	doc, err := api.Ingest(ctx, client.IngestRequest{Content: "The library opens on Friday [1]."})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		summary, statusErr := api.GetDocument(ctx, doc.DocumentID)
		return statusErr == nil && summary.Status == "completed"
	}, 10*time.Second, 50*time.Millisecond, "fixture document ingestion")
	response, err := api.Query(ctx, client.QueryRequest{
		Query: "When does the library open?", Mode: "global", AllowedDocumentIDs: []string{doc.DocumentID}, IncludeCitations: true,
	})
	require.NoError(t, err)
	require.NotEmpty(t, response.Citations)
	require.Zero(t, calls.Load(), "offline mode sent requests to the inherited verifier")
	require.Nil(t, response.CitationCoverage)
	for _, citation := range response.Citations {
		require.Nil(t, citation.Support)
	}
}
