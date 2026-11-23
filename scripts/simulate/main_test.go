package main

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

type ingestTransport func(*http.Request) (*http.Response, error)

func (f ingestTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

func TestRepeatedGuideIngest(t *testing.T) {
	originalTransport := http.DefaultTransport
	originalGuide, originalPassed, originalTotal, originalResults := guideID, passed, total, results
	t.Cleanup(func() {
		http.DefaultTransport = originalTransport
		guideID, passed, total, results = originalGuide, originalPassed, originalTotal, originalResults
	})
	guideID = uuid.NewString()
	privateID := uuid.NewString()
	for _, tc := range []struct {
		name        string
		policy      string
		code        int
		reply       ingestResp
		wantDeletes []string
	}{
		{name: "default completed match", code: http.StatusOK, reply: ingestResp{guideID, "already_ingested"}},
		{name: "shared processing match", policy: "collection", code: http.StatusAccepted, reply: ingestResp{guideID, "processing"}},
		{name: "private document", policy: "off", code: http.StatusAccepted, reply: ingestResp{privateID, "processing"}, wantDeletes: []string{privateID}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var deletes []string
			http.DefaultTransport = ingestTransport(func(r *http.Request) (*http.Response, error) {
				status := tc.code
				var reply any = tc.reply
				switch r.Method {
				case http.MethodPost:
					require.Equal(t, "/ingest", r.URL.Path)
				case http.MethodDelete:
					id, found := strings.CutPrefix(r.URL.Path, "/documents/")
					require.True(t, found)
					deletes = append(deletes, id)
					status = http.StatusOK
					reply = deleteResp{Deleted: true, DocID: id}
				default:
					t.Fatalf("unexpected method %q", r.Method)
				}
				body, err := json.Marshal(reply)
				require.NoError(t, err)
				return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(string(body)))}, nil
			})
			policy, err := expectedIngestPolicy(tc.policy)
			require.NoError(t, err)
			scenario4(policy)
			require.Equal(t, tc.wantDeletes, deletes)
		})
	}
}

func TestRepeatedGuideIdentityMustMatchPolicy(t *testing.T) {
	prior, different := uuid.NewString(), uuid.NewString()
	for _, tc := range []struct {
		name   string
		policy domain.ContentDedup
		code   int
		reply  ingestResp
	}{
		{"shared accepts only the matched UUID", domain.ContentDedupCollection, http.StatusOK, ingestResp{different, "already_ingested"}},
		{"shared status and body must agree", domain.ContentDedupCollection, http.StatusAccepted, ingestResp{prior, "already_ingested"}},
		{"private requires a different UUID", domain.ContentDedupOff, http.StatusAccepted, ingestResp{strings.ToUpper(prior), "processing"}},
		{"private requires a valid UUID", domain.ContentDedupOff, http.StatusAccepted, ingestResp{"invalid", "processing"}},
		{"private requires accepted status", domain.ContentDedupOff, http.StatusOK, ingestResp{different, "processing"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.False(t, matchesRepeatIngest(tc.policy, tc.code, tc.reply, prior))
		})
	}
	_, err := expectedIngestPolicy("invalid")
	require.ErrorContains(t, err, "INGEST_CONTENT_DEDUP")
}
