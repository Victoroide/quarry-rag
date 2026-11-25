package pinecone

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/tracing"
	sdk "github.com/pinecone-io/go-pinecone/v5/pinecone"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Ingestion receipts and query-time refill are separate settings. The traced
// store that the server hands to both paths must expose both capabilities for
// every combination of them.
func TestTracedDriverKeepsBothVisibilityCapabilities(t *testing.T) {
	for _, tt := range []struct {
		name   string
		mode   string
		refill bool
	}{
		{"sample without refill", "sample", false},
		{"sample with refill", "sample", true},
		{"receipts without refill", "lsn", false},
		{"receipts with refill", "lsn", true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var server *httptest.Server
			server = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch r.URL.Path {
				case "/indexes/fixture":
					assert.NoError(t, json.NewEncoder(w).Encode(map[string]any{
						"name": "fixture", "dimension": 2, "metric": "cosine", "host": strings.TrimPrefix(server.URL, "https://"),
						"status": map[string]any{"ready": true, "state": "Ready"},
						"spec":   map[string]any{"serverless": map[string]string{"cloud": "aws", "region": "us-east-1"}},
					}))
				case "/vectors/upsert":
					w.Header().Set("x-pinecone-request-lsn", "15")
					_, _ = io.WriteString(w, `{"upsertedCount":1}`)
				case "/query":
					w.Header().Set("x-pinecone-max-indexed-lsn", "15")
					_, _ = io.WriteString(w, `{"namespace":"__default__","matches":[]}`)
				default:
					t.Errorf("unexpected path %q", r.URL.Path)
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			t.Cleanup(server.Close)
			pc, err := sdk.NewClient(sdk.NewClientParams{ApiKey: "fixture", Host: server.URL, RestClient: server.Client()})
			require.NoError(t, err)
			cfg := &config.Config{
				PineconeIndexName: "fixture", PineconeAPIKey: "fixture", PineconeTimeout: 5 * time.Second, PineconeMaxAttempts: 1,
				PineconeUpsertBatchSize: 1, EmbeddingDimensions: 2, IndexVisibilityMode: tt.mode, HostedVisibilityRefill: tt.refill,
			}
			client, err := newPineconeClient(t.Context(), cfg, pc)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, client.Close()) })

			store := tracing.WrapVectorStore(NewDriver(client), "pinecone")
			writer, canWrite := store.(domain.VisibilityWriter)
			require.True(t, canWrite, "ingestion must still reach the receipt writer")
			_, canRefill := store.(domain.VisibilityRefillStore)
			require.True(t, canRefill, "retrieval must still reach the refill search")
			if tt.mode != "lsn" {
				require.Nil(t, client.visibility)
				return
			}
			require.NotNil(t, client.visibility, "the binding is created from the validated host")
			client.visibility.http.Transport = server.Client().Transport
			probe, err := writer.UpsertWithVisibility(t.Context(), "", testChunks(1, 2, 10))
			require.NoError(t, err)
			require.NotNil(t, probe)
			got, err := probe(t.Context(), []float32{1, 0})
			require.NoError(t, err)
			require.Equal(t, domain.VisibilityWritesIndexed, got)
		})
	}
}
