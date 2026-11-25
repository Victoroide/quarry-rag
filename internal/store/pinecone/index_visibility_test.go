package pinecone

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	pc "github.com/pinecone-io/go-pinecone/v5/pinecone"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWriteVisibilityWatermarks(t *testing.T) {
	for _, tt := range []struct {
		name, first, second, namespace string
		watermarks                     []string
		want                           []domain.IndexVisibility
	}{
		{"maximum across batches", "12", "15", "archive", []string{"14", "15"}, []domain.IndexVisibility{"", domain.VisibilityWritesIndexed}},
		{"out of order receipts", "15", "12", "archive", []string{"14", "15"}, []domain.IndexVisibility{"", domain.VisibilityWritesIndexed}},
		{"missing first receipt", "", "15", "archive", nil, nil},
		{"missing second receipt", "12", "", "archive", nil, nil},
		{"invalid receipt", "12", "15, 16", "archive", nil, nil},
		{"missing query watermark", "12", "15", "archive", []string{""}, []domain.IndexVisibility{domain.VisibilityUnknown}},
		{"different namespace", "12", "15", "other", []string{"99"}, []domain.IndexVisibility{domain.VisibilityUnknown}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			writes, reads := 0, 0
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, "2026-07", r.Header.Get("X-Pinecone-Api-Version"))
				assert.Equal(t, "test-key", r.Header.Get("Api-Key"))
				assert.Equal(t, http.MethodPost, r.Method)
				var req struct {
					Namespace string `json:"namespace"`
					Vectors   []struct {
						ID       string         `json:"id"`
						Metadata map[string]any `json:"metadata"`
					} `json:"vectors"`
					TopK            int  `json:"topK"`
					IncludeMetadata bool `json:"includeMetadata"`
				}
				if !assert.NoError(t, json.NewDecoder(r.Body).Decode(&req)) {
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				assert.Equal(t, "archive", req.Namespace)
				switch r.URL.Path {
				case "/vectors/upsert":
					if assert.Less(t, writes, 2) && assert.Len(t, req.Vectors, 1) {
						assert.Equal(t, fmt.Sprintf("chunk-%d", writes), req.Vectors[0].ID)
						assert.Equal(t, "archive", req.Vectors[0].Metadata["collection"])
						if receipt := []string{tt.first, tt.second}[writes]; receipt != "" {
							w.Header().Set("x-pinecone-request-lsn", receipt)
						}
					}
					writes++
					_, _ = io.WriteString(w, `{"upsertedCount":1}`)
				case "/query":
					assert.Equal(t, 1, req.TopK)
					assert.False(t, req.IncludeMetadata)
					if assert.Less(t, reads, len(tt.watermarks)) && tt.watermarks[reads] != "" {
						w.Header().Set("x-pinecone-max-indexed-lsn", tt.watermarks[reads])
					}
					reads++
					// The first probe sees a stale copy with the same ID. The second sees a tied record.
					match := "chunk-1"
					if reads > 1 {
						match = "tied-record"
					}
					_, _ = fmt.Fprintf(w, `{"namespace":%q,"matches":[{"id":%q,"score":1}]}`, tt.namespace, match)
				default:
					t.Errorf("unexpected path %q", r.URL.Path)
				}
			}))
			t.Cleanup(server.Close)
			c := visibilityClient(t, server)
			chunks := []domain.Chunk{
				{ID: "chunk-0", Embedding: []float32{1, 0}, Metadata: map[string]any{"collection": "archive"}},
				{ID: "chunk-1", Embedding: []float32{1, 0}, Metadata: map[string]any{"collection": "archive"}},
			}
			probe, err := c.UpsertWithVisibility(t.Context(), "archive", chunks)
			require.NoError(t, err)
			if tt.want == nil {
				require.Nil(t, probe)
			} else {
				require.NotNil(t, probe)
				for _, want := range tt.want {
					got, probeErr := probe(t.Context(), []float32{1, 0})
					require.NoError(t, probeErr)
					require.Equal(t, want, got)
				}
			}
			server.Close()
			require.Equal(t, 2, writes)
			require.Equal(t, len(tt.watermarks), reads)
		})
	}
}

func visibilityClient(t *testing.T, server *httptest.Server) *Client {
	t.Helper()
	plane, err := newVisibilityPlane(&pc.Index{Host: strings.TrimPrefix(server.URL, "https://"), Spec: &pc.IndexSpec{Serverless: &pc.ServerlessSpec{}}}, "test-key")
	require.NoError(t, err)
	plane.http.Transport = server.Client().Transport
	return &Client{visibility: plane, batchSize: 1, timeout: time.Second, maxAttempts: 1}
}

func TestWriteVisibilityPartialUpsertDiscardsReceipts(t *testing.T) {
	for _, code := range []int{http.StatusOK, http.StatusServiceUnavailable} {
		t.Run(http.StatusText(code), func(t *testing.T) {
			writes := 0
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, "/vectors/upsert", r.URL.Path)
				writes++
				w.Header().Set("x-pinecone-request-lsn", "15")
				if writes == 1 {
					_, _ = io.WriteString(w, `{"upsertedCount":1}`)
					return
				}
				w.WriteHeader(code)
				_, _ = io.WriteString(w, `{"upsertedCount":0}`)
			}))
			t.Cleanup(server.Close)
			probe, err := visibilityClient(t, server).UpsertWithVisibility(t.Context(), "", testChunks(2, 2, 10))
			require.Error(t, err)
			require.Nil(t, probe)
			server.Close()
			require.Equal(t, 2, writes)
		})
	}
}

func TestWriteVisibilityPinsIndexEpoch(t *testing.T) {
	replacement := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		t.Error("visibility probe reached the recreated index")
		w.Header().Set("x-pinecone-max-indexed-lsn", "100")
		_, _ = io.WriteString(w, `{"namespace":"archive"}`)
	}))
	t.Cleanup(replacement.Close)
	old := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/vectors/upsert" {
			w.Header().Set("x-pinecone-request-lsn", "15")
			_, _ = io.WriteString(w, `{"upsertedCount":1}`)
			return
		}
		w.Header().Set("Location", replacement.URL+"/query")
		w.WriteHeader(http.StatusTemporaryRedirect)
	}))
	t.Cleanup(old.Close)
	client := visibilityClient(t, old)
	probe, err := client.UpsertWithVisibility(t.Context(), "archive", testChunks(1, 2, 10))
	require.NoError(t, err)
	require.NotNil(t, probe)
	client.visibility = visibilityClient(t, replacement).visibility
	got, probeErr := probe(t.Context(), []float32{1, 0})
	require.NoError(t, probeErr)
	require.Equal(t, domain.VisibilityUnknown, got)
}

func TestWriteVisibilityRejectsUnsupportedIndex(t *testing.T) {
	for _, spec := range []*pc.IndexSpec{nil, {}, {Pod: &pc.PodSpec{}}, {BYOC: &pc.BYOCSpec{}}} {
		_, err := newVisibilityPlane(&pc.Index{Host: "index.example", Spec: spec}, "test-key")
		require.ErrorContains(t, err, "serverless")
	}
}

func TestWriteVisibilityUsesSDKDefaultNamespace(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload struct {
			Namespace string `json:"namespace"`
		}
		assert.NoError(t, json.NewDecoder(r.Body).Decode(&payload))
		assert.Equal(t, "__default__", payload.Namespace)
		if r.URL.Path == "/vectors/upsert" {
			w.Header().Set("x-pinecone-request-lsn", "15")
			_, _ = io.WriteString(w, `{"upsertedCount":1}`)
			return
		}
		w.Header().Set("x-pinecone-max-indexed-lsn", "15")
		_, _ = io.WriteString(w, `{"namespace":"__default__","matches":[]}`)
	}))
	t.Cleanup(server.Close)
	probe, err := visibilityClient(t, server).UpsertWithVisibility(t.Context(), "", testChunks(1, 2, 10))
	require.NoError(t, err)
	require.NotNil(t, probe)
	got, err := probe(t.Context(), []float32{1, 0})
	require.NoError(t, err)
	require.Equal(t, domain.VisibilityWritesIndexed, got)
}

func FuzzParseVisibilityLSN(f *testing.F) {
	for _, seed := range []string{"", "0", "15", "-1", "+1", " 1", "2, 3", "18446744073709551615", "18446744073709551616"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, raw string) {
		header := http.Header{"X-Lsn": {raw}}
		parsed, ok := parseVisibilityLSN(header, "X-Lsn")
		var expected uint64
		// A decimal header may contain leading zeroes but must contain only ASCII digits.
		if raw == "" || strings.Trim(raw, "0123456789") != "" {
			require.False(t, ok)
			return
		}
		var err error
		// Decimal scanning of normalized text avoids octal interpretation of leading zeroes.
		if trimmed := strings.TrimLeft(raw, "0"); trimmed != "" {
			_, err = fmt.Sscan(trimmed, &expected)
		} else {
			expected, err = 0, nil
		}
		require.Equal(t, err == nil, ok)
		if ok {
			require.Equal(t, expected, parsed)
		}
		header.Add("X-Lsn", raw)
		_, ok = parseVisibilityLSN(header, "X-Lsn")
		require.False(t, ok)
	})
}
