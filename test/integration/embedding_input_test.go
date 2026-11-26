package integration

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/Victoroide/quarry-rag/internal/api/handler"
	"github.com/Victoroide/quarry-rag/internal/chunker"
	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/embedder"
	"github.com/Victoroide/quarry-rag/internal/embeddinginput"
	"github.com/Victoroide/quarry-rag/internal/pipeline"
	pgstore "github.com/Victoroide/quarry-rag/internal/store/postgres"
	"github.com/Victoroide/quarry-rag/internal/testutil/pgtest"
	"github.com/Victoroide/quarry-rag/internal/worker"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEmbeddingModelRejectionLeavesNoCompletedChunksOrVectors(t *testing.T) {
	c, err := pgstore.NewClient(t.Context(), pgtest.StartVector(t))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, c.Close(context.Background())) })
	require.NoError(t, c.RunMigrations(t.Context()))
	vectors, err := pgstore.NewVectorStore(t.Context(), c, 3)
	require.NoError(t, err)

	for _, tc := range []struct {
		name, code, firstWord string
		attempts              int
		message               string
	}{
		{"model window shrank", "context_length_exceeded", "firstword", 3, "embedding model rejected the input length"},
		{"unknown provider rejection", "invalid_model", "otherword", 1, "openai: HTTP status 400"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			inputs := make(chan string, 20)
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var body struct{ Input string }
				if !assert.NoError(t, json.NewDecoder(r.Body).Decode(&body)) {
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				inputs <- body.Input
				if utf8.RuneCountInString(body.Input) > 9 {
					w.WriteHeader(http.StatusBadRequest)
					_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]string{"code": tc.code, "message": "private model detail"}})
					return
				}
				_, _ = io.WriteString(w, `{"data":[{"index":0,"embedding":[1,0,0]}]}`)
			}))
			t.Cleanup(upstream.Close)
			path := filepath.Join(t.TempDir(), "embedding.json")
			require.NoError(t, os.WriteFile(path, []byte(`{"provider":"openai","model":"embed","revision":"r1","max_characters":10}`), 0o600))
			profile, err := embeddinginput.Load(path, embeddinginput.Target{Provider: "openai", Model: "embed", BaseURL: upstream.URL})
			require.NoError(t, err)
			provider, err := embedder.NewOpenAIEmbedder(&config.Config{
				OpenAIBaseURL: upstream.URL, OpenAIEmbeddingModel: "embed", EmbeddingDimensions: 3,
				EmbedConcurrency: 1, EmbedTimeout: time.Second, EmbeddingInputProfile: profile,
			})
			require.NoError(t, err)
			chk := chunker.NewChunker(32, 4)
			w := worker.New(c, []pipeline.Stage{
				pipeline.NewChunkStageWithEmbeddingProfile(chk, profile),
				pipeline.NewEmbedStage(provider, 1),
				pipeline.NewPersistStage(vectors),
			}, vectors, worker.Options{Workers: 1, MaxAttempts: tc.attempts, Lease: time.Second,
				JobTimeout: time.Minute, PollInterval: 10 * time.Millisecond, Owner: "profile-rejection"})
			w.Start(t.Context())
			t.Cleanup(w.Shutdown)
			rec := durableSubmit(t, w, c, `{"content":"`+tc.firstWord+` secondword"}`)
			require.Equal(t, http.StatusAccepted, rec.Code, rec.Body.String())
			var accepted handler.IngestResponse
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &accepted))
			durableStatus(t, c, accepted.DocumentID, "failed")
			require.Len(t, inputs, 2)
			require.Equal(t, tc.firstWord, strings.TrimSpace(<-inputs))
			require.Equal(t, "secondword", strings.TrimSpace(<-inputs))

			var chunks, vectorCount, jobs int
			var failure string
			require.NoError(t, c.Pool.QueryRow(t.Context(), `
				SELECT last_error,
					(SELECT count(*) FROM chunks WHERE document_id=$1),
					(SELECT count(*) FROM chunk_vectors WHERE document_id=$1),
					(SELECT count(*) FROM ingest_jobs WHERE document_id=$1)
				FROM documents WHERE id=$1`, accepted.DocumentID).Scan(&failure, &chunks, &vectorCount, &jobs))
			require.Contains(t, failure, tc.message)
			require.NotContains(t, failure, "private model detail")
			require.Zero(t, chunks)
			require.Zero(t, vectorCount)
			require.Zero(t, jobs)

			fits := tc.firstWord + " smallword"
			rec = durableSubmit(t, w, c, `{"content":"`+fits+`"}`)
			require.Equal(t, http.StatusAccepted, rec.Code, rec.Body.String())
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &accepted))
			durableStatus(t, c, accepted.DocumentID, "completed")
			expected, err := chk.ChunkForEmbedding(t.Context(), domain.DocumentID(accepted.DocumentID), fits, profile)
			require.NoError(t, err)
			require.Len(t, expected, 2)
			filter := domain.VectorFilter{DocumentIDs: []string{accepted.DocumentID}}
			matches, err := vectors.QueryFiltered(t.Context(), []float32{1, 0, 0}, 2, filter)
			require.NoError(t, err)
			require.Len(t, matches, 2)
			for _, match := range matches {
				require.Equal(t, uuid.MustParse(string(expected[match.Chunk.Index].ID)), uuid.MustParse(string(match.Chunk.ID)))
				require.Equal(t, expected[match.Chunk.Index].Text, match.Chunk.Text)
			}
		})
	}
}
