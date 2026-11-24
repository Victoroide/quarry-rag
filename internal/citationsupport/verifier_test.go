package citationsupport

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestVerifierBatchCalibrationAndAbstention(t *testing.T) {
	c := testCalibration()
	set := testCalibration().Pairs[0]
	set.EvidenceMode = Set
	for i := range set.HeldOut {
		row := &set.HeldOut[i]
		doc := row.Documents[0]
		row.Documents = []string{doc + "-joint-a", doc + "-joint-b"}
		row.EvidenceCount = 2
		row.Prediction.EvidenceLanguages = []string{"en", "en"}
	}
	c.Pairs = append(c.Pairs, set)
	batch := Batch{Pairs: []Pair{
		{Claim: "保管期間は十年です", Body: "Files must be kept for a decade."},
		{Claim: "保管期間は十年です", Body: "Files must be kept for two years."},
		{Claim: "保管期間は十年です", Body: "Files must not be kept for a decade."},
		{Claim: "保管期間は十年です", Body: "Files can be kept for a decade in the first archive."},
		{Claim: "保管期間は十年です", Body: "The first archive has a mandatory policy."},
	}, Claims: [][]int{{0}, {1}, {2}, {3, 4}, {4}, {0, 4}}}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "Bearer operator-token", r.Header.Get("Authorization"))
		require.Equal(t, http.MethodPost, r.Method)
		var req verifierRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		require.Equal(t, verifierRequest{Model: testModel, Batch: batch}, req)
		require.NoError(t, json.NewEncoder(w).Encode(verifierResponse{Model: testModel, Results: []Prediction{
			{ClaimLanguage: "ja", EvidenceLanguages: []string{"en"}, Scores: &Scores{Supported: .98, Contradicted: .01, Insufficient: .01}},
			{ClaimLanguage: "ja", EvidenceLanguages: []string{"en"}, Scores: &Scores{Supported: .01, Contradicted: .98, Insufficient: .01}},
			{ClaimLanguage: "ja", EvidenceLanguages: []string{"en"}, Scores: &Scores{Supported: .01, Contradicted: .98, Insufficient: .01}},
			{ClaimLanguage: "ja", EvidenceLanguages: []string{"en", "en"}, Scores: &Scores{Supported: .97, Contradicted: .02, Insufficient: .01}},
			{ClaimLanguage: "ja", EvidenceLanguages: []string{"en"}, Scores: &Scores{Supported: .01, Contradicted: .01, Insufficient: .98}},
			{ClaimLanguage: "ja", EvidenceLanguages: []string{"en", "de"}, Scores: &Scores{Supported: .98, Contradicted: .01, Insufficient: .01}},
		}}))
	}))
	defer server.Close()
	v, err := newVerifier(testModel, server.URL, "operator-token", &c, server.Client().Transport)
	require.NoError(t, err)
	statuses, err := v.Verify(t.Context(), batch)
	require.NoError(t, err)
	require.Equal(t, []Status{Supported, Contradicted, Contradicted, Supported, Insufficient, Unchecked}, statuses)
}

func TestVerifierAbstainsOnSpecialLanguageTags(t *testing.T) {
	scores := &Scores{Supported: .98, Contradicted: .01, Insufficient: .01}
	thresholds := Scores{Supported: .85, Contradicted: .85, Insufficient: .85}
	for _, tag := range []string{"und", "und-Latn", "mul", "mul-Latn", "zxx", "zxx-Latn", "mis", "mis-Latn", "x-test"} {
		for _, key := range []profileKey{{tag, "en-GB", Single}, {"ja", tag, Single}} {
			result := &Prediction{ClaimLanguage: key.claimLanguage, EvidenceLanguages: []string{key.sourceLanguage}, Scores: scores}
			_, status := assessPrediction(map[profileKey]Scores{key: thresholds}, result, 1)
			require.Equal(t, Unchecked, status, "profile: %+v", key)
		}
	}
	key := profileKey{"ja", "en-GB", Single}
	result := &Prediction{ClaimLanguage: "ja", EvidenceLanguages: []string{"en-GB"}, Scores: scores}
	_, status := assessPrediction(map[profileKey]Scores{key: thresholds}, result, 1)
	require.Equal(t, Supported, status)
}

func TestVerifierRejectsInvalidTrafficAndDoesNotRedirect(t *testing.T) {
	c := testCalibration()
	var redirects atomic.Int32
	destination := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { redirects.Add(1) }))
	defer destination.Close()
	for _, tc := range []struct {
		name string
		send func(http.ResponseWriter, *http.Request)
	}{
		{"unavailable", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusServiceUnavailable) }},
		{"redirect", func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, destination.URL, http.StatusTemporaryRedirect)
		}},
		{"oversize", func(w http.ResponseWriter, _ *http.Request) {
			_, _ = io.WriteString(w, strings.Repeat(" ", MaxBatchBytes+1))
		}},
		{"trailing value", func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "{}{}") }},
		{"wrong model", func(w http.ResponseWriter, _ *http.Request) {
			_, _ = io.WriteString(w, `{"model":"other","results":[{}]}`)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(tc.send))
			defer server.Close()
			v, err := newVerifier(testModel, server.URL, "", &c, nil)
			require.NoError(t, err)
			_, err = v.Verify(t.Context(), Batch{Pairs: []Pair{{Claim: "事実", Body: "Fact"}}, Claims: [][]int{{0}}})
			require.Error(t, err)
		})
	}
	require.Zero(t, redirects.Load())
}

func TestVerifierCancelsRequest(t *testing.T) {
	c := testCalibration()
	started, ended := make(chan struct{}), make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		close(started)
		<-r.Context().Done()
		close(ended)
	}))
	defer server.Close()
	v, err := newVerifier(testModel, server.URL, "", &c, nil)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	errs := make(chan error, 1)
	go func() {
		_, err := v.Verify(ctx, Batch{Pairs: []Pair{{Claim: "事実", Body: "Fact"}}, Claims: [][]int{{0}}})
		errs <- err
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("verifier did not receive the request")
	}
	cancel()
	require.ErrorIs(t, <-errs, context.Canceled)
	select {
	case <-ended:
	case <-time.After(5 * time.Second):
		t.Fatal("verifier request survived cancellation")
	}
}

func TestVerifierWithoutCalibrationDoesNotSendData(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { calls.Add(1) }))
	defer server.Close()
	v, err := New(testModel, server.URL, "", "")
	require.NoError(t, err)
	statuses, err := v.Verify(t.Context(), Batch{Pairs: []Pair{{Claim: "Statement", Body: "Evidence"}}, Claims: [][]int{{0}}})
	require.NoError(t, err)
	require.Equal(t, []Status{Unchecked}, statuses)
	require.Zero(t, calls.Load())
}
