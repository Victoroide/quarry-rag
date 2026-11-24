package integration

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/api/handler"
	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/llm"
	"github.com/Victoroide/quarry-rag/internal/retriever"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type deadlineRefillReranker func(context.Context, string, []domain.ScoredChunk) ([]domain.ScoredChunk, error)

func (f deadlineRefillReranker) Rerank(ctx context.Context, text string, hits []domain.ScoredChunk) ([]domain.ScoredChunk, error) {
	return f(ctx, text, hits)
}

func TestHostedRefillRerankDeadlineDistinguishesVisibilityAndProvider(t *testing.T) {
	if testing.Short() {
		t.Skip("requires a Docker daemon")
	}
	db := visibilityClient(t)
	for _, tc := range []struct {
		name            string
		blockVisibility bool
	}{
		{name: "SQL visibility timeout", blockVisibility: true},
		{name: "provider timeout"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			live := refillFixture("The fixture reading room closes at seven.", .95)
			insertRefillDocument(t, db, live.Chunk, "archive", "completed")
			var rerankCalls, modelCalls atomic.Int32
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				b, err := io.ReadAll(r.Body)
				if err != nil {
					t.Error(err)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				if strings.Contains(r.URL.Path, "/converse") {
					if !strings.Contains(string(b), live.Chunk.Text) {
						t.Error("expected the fixture context in synthesis")
					}
					modelCalls.Add(1)
					_, _ = io.WriteString(w, `{"output":{"message":{"role":"assistant","content":[{"text":"The fixture answer."}]}},"stopReason":"end_turn","usage":{"inputTokens":3,"outputTokens":2,"totalTokens":5}}`)
					return
				}
				rerankCalls.Add(1)
				if !tc.blockVisibility {
					<-r.Context().Done()
					return
				}
				_, _ = io.WriteString(w, `{"results":[{"index":0,"relevanceScore":0.95}]}`)
			}))
			t.Cleanup(upstream.Close)
			for _, k := range []string{"AWS_ENDPOINT_URL", "AWS_ENDPOINT_URL_BEDROCK_AGENT_RUNTIME", "AWS_ENDPOINT_URL_BEDROCK_RUNTIME"} {
				t.Setenv(k, upstream.URL)
			}
			cfg := &config.Config{
				AWSRegion: "us-east-1", AWSAccessKeyID: "fixture", AWSSecretKey: "fixture", AWSMaxAttempts: 1,
				RerankModelARN: "fixture", BedrockFallbackModel: "fixture", HostedVisibilityRefill: true,
				HostedVisibilityExtraRequests: 1, MaxChunksPerQuery: 2, QueryTimeout: 12 * time.Second, RerankTimeout: 2 * time.Second,
			}
			ranker, err := retriever.NewBedrockReranker(t.Context(), cfg)
			require.NoError(t, err)
			client, err := llm.NewClient(t.Context(), cfg)
			require.NoError(t, err)
			amb := llm.NewAmbassador(cfg)
			amb.SetClient(client)
			idx := hostedRefillDouble{query: func(context.Context, []float32, int, domain.VectorFilter, int) ([]domain.ScoredChunk, error) {
				return []domain.ScoredChunk{live}, nil
			}}
			var returned, recorded error
			rank := deadlineRefillReranker(func(ctx context.Context, text string, hits []domain.ScoredChunk) ([]domain.ScoredChunk, error) {
				if tc.blockVisibility {
					// The stage preflight has passed. Block the next SQL read,
					// which runs inside the Bedrock HTTP-attempt guard.
					tx, beginErr := db.Pool.Begin(ctx)
					require.NoError(t, beginErr)
					defer func() {
						cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
						defer cancel()
						require.NoError(t, tx.Rollback(cleanupCtx))
					}()
					_, lockErr := tx.Exec(ctx, "LOCK TABLE chunks IN ACCESS EXCLUSIVE MODE")
					require.NoError(t, lockErr)
				}
				result, rankErr := ranker.Rerank(ctx, text, hits)
				returned = rankErr
				recorded = domain.RecordedEvidenceCheckError(ctx)
				return result, rankErr
			})
			h := handler.QueryHandler(retriever.NewRetriever(idx, &refillEmbedder{}, .7, 2), amb, cfg, db.Pool, handler.WithReranker(rank))
			w := scopeQuery(t, h, `{"query":"reading room","mode":"global","collection":"archive","max_chunks":2}`)
			require.ErrorIs(t, returned, context.DeadlineExceeded)
			if tc.blockVisibility {
				require.NotErrorIs(t, returned, domain.ErrEvidenceCheck)
				require.ErrorIs(t, recorded, domain.ErrEvidenceCheck)
				require.ErrorIs(t, recorded, context.DeadlineExceeded)
				require.ErrorContains(t, recorded, "postgres: visible evidence:")
				assert.Zero(t, rerankCalls.Load())
				assert.Zero(t, modelCalls.Load())
				assert.Equal(t, http.StatusGatewayTimeout, w.Code, w.Body.String())
				assert.Contains(t, w.Body.String(), `"error":"timeout"`)
				return
			}
			require.NoError(t, recorded)
			require.Equal(t, http.StatusOK, w.Code, w.Body.String())
			require.Contains(t, w.Body.String(), "The fixture answer.")
			require.EqualValues(t, 1, modelCalls.Load())
			require.EqualValues(t, 1, rerankCalls.Load())
		})
	}
}
