package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/api/handler"
	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/llm"
	"github.com/Victoroide/quarry-rag/internal/metrics"
	"github.com/Victoroide/quarry-rag/internal/retriever"
	pgstore "github.com/Victoroide/quarry-rag/internal/store/postgres"
	"github.com/Victoroide/quarry-rag/internal/tracing"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

type hostedRefillDouble struct {
	domain.VectorStore
	domain.VectorDeleter
	query func(context.Context, []float32, int, domain.VectorFilter, int) ([]domain.ScoredChunk, error)
}

func (s hostedRefillDouble) QueryFiltered(ctx context.Context, v []float32, k int, f domain.VectorFilter) ([]domain.ScoredChunk, error) {
	return s.query(ctx, v, k, f, 0)
}

func (s hostedRefillDouble) QueryCandidates(ctx context.Context, v []float32, k int, f domain.VectorFilter) ([]domain.ScoredChunk, error) {
	return s.QueryFiltered(ctx, v, k, f)
}

func (s hostedRefillDouble) QueryFilteredBudgeted(ctx context.Context, v []float32, k int, f domain.VectorFilter, budget int) ([]domain.ScoredChunk, int, error) {
	groups := max(1, (len(f.DocumentIDs)+9999)/10000)
	if budget < groups {
		return nil, 0, domain.ErrVectorQueryBudget
	}
	chunks, err := s.query(ctx, v, k, f, groups)
	return chunks, groups, err
}

func (hostedRefillDouble) VisibilityRefillLimit() int { return 90 }

type refillEmbedder struct{ calls int }

func (e *refillEmbedder) Embed(context.Context, string) ([]float32, error) {
	e.calls++
	return []float32{1}, nil
}

type refillReranker func([]domain.ScoredChunk)

func (f refillReranker) Rerank(_ context.Context, _ string, chunks []domain.ScoredChunk) ([]domain.ScoredChunk, error) {
	f(chunks)
	return chunks, nil
}

func insertRefillDocument(t *testing.T, c *pgstore.Client, ch domain.Chunk, collection, status string) {
	t.Helper()
	_, err := c.Pool.Exec(t.Context(), `INSERT INTO documents (id, content, content_preview, status, collection, doc_type, metadata)
		VALUES ($1, $2, $2, $3, $4, 'guide', '{"title":"Shared Archive"}')`, ch.DocumentID, ch.Text, status, collection)
	require.NoError(t, err)
	require.NoError(t, c.InsertChunks(t.Context(), []domain.Chunk{ch}))
}

func refillFixture(text string, score float64) domain.ScoredChunk {
	return domain.ScoredChunk{Score: score, Chunk: domain.Chunk{
		ID: domain.ChunkID(uuid.NewString()), DocumentID: domain.DocumentID(uuid.NewString()), Text: text,
		Metadata: map[string]any{"collection": "archive", "doc_type": "guide"},
	}}
}

func TestHostedRefillWithPostgreSQLVisibility(t *testing.T) {
	if testing.Short() {
		t.Skip("requires a Docker daemon")
	}
	c := visibilityClient(t)
	for _, tc := range []struct {
		name         string
		enabled      bool
		stream       bool
		remove       bool
		sqlFailure   bool
		cancel       bool
		evidenceOnly bool
		reuseID      bool
		want         bool
	}{
		{name: "default path"},
		{name: "rank three recovered", enabled: true, want: true},
		{name: "streamed recovery", enabled: true, stream: true, want: true, evidenceOnly: true},
		{name: "deleted during refill", enabled: true, stream: true, remove: true},
		{name: "SQL failure after refill", enabled: true, stream: true, sqlFailure: true},
		{name: "parent deadline during refill", enabled: true, stream: true, cancel: true},
		{name: "reused public IDs", enabled: true, stream: true, reuseID: true, want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			missing := refillFixture("Missing document.", .99)
			wrong := refillFixture("Other collection text.", .98)
			live := refillFixture("The reading room closes at seven.", .9)
			private := refillFixture(live.Chunk.Text, 1)
			private.Chunk.Metadata = map[string]any{"doc_type": "guide"}
			insertRefillDocument(t, c, wrong.Chunk, "private", "completed")
			insertRefillDocument(t, c, live.Chunk, "archive", "completed")
			insertRefillDocument(t, c, private.Chunk, "", "completed")
			stale := missing
			if tc.reuseID {
				stale.Chunk = live.Chunk
				stale.Chunk.Text = "Superseded text."
				_, err := c.Pool.Exec(t.Context(), `DELETE FROM documents WHERE id=$1`, live.Chunk.DocumentID)
				require.NoError(t, err)
				insertRefillDocument(t, c, live.Chunk, "archive", "completed")
			}

			filter := domain.VectorFilter{Collection: "archive", DocType: "guide", DocumentIDs: []string{
				string(missing.Chunk.DocumentID), string(wrong.Chunk.DocumentID), string(live.Chunk.DocumentID),
			}}
			var renamedChunks bool
			if tc.sqlFailure {
				t.Cleanup(func() {
					if renamedChunks {
						_, err := c.Pool.Exec(context.Background(), `ALTER TABLE hidden_chunks RENAME TO chunks`)
						require.NoError(t, err)
					}
				})
			}
			var topKs []int
			idx := hostedRefillDouble{query: func(ctx context.Context, embedding []float32, topK int, gotFilter domain.VectorFilter, cost int) ([]domain.ScoredChunk, error) {
				require.Equal(t, filter, gotFilter)
				require.Equal(t, []float32{1}, embedding)
				topKs = append(topKs, topK)
				if topK == 2 {
					require.Zero(t, cost)
					if tc.remove {
						return []domain.ScoredChunk{stale, live}, nil
					}
					return []domain.ScoredChunk{stale, wrong}, nil
				}
				require.Equal(t, 1, cost)
				switch {
				case tc.remove:
					_, err := c.Pool.Exec(ctx, `DELETE FROM documents WHERE id=$1`, live.Chunk.DocumentID)
					require.NoError(t, err)
				case tc.sqlFailure:
					_, err := c.Pool.Exec(ctx, `ALTER TABLE chunks RENAME TO hidden_chunks`)
					require.NoError(t, err)
					renamedChunks = true
				case tc.cancel:
					<-ctx.Done()
					return nil, ctx.Err()
				}
				return []domain.ScoredChunk{stale, wrong, live, private}, nil
			}}
			emb := &refillEmbedder{}
			cfg := &config.Config{
				BedrockFallbackModel: "fallback", MaxChunksPerQuery: 10,
				HostedVisibilityRefill: tc.enabled, HostedVisibilityExtraRequests: 1,
			}
			if tc.cancel {
				cfg.QueryTimeout = 3 * time.Second
			}
			amb := llm.NewAmbassador(cfg)
			amb.SetClient(llm.NewFakeConverser())
			reranks := 0
			checkRerank := refillReranker(func(hits []domain.ScoredChunk) {
				reranks++
				require.Equal(t, []domain.ScoredChunk{live}, hits)
			})
			ret := retriever.NewRetriever(tracing.WrapVectorStore(idx, "pinecone"), emb, .7, 10)
			h := handler.QueryHandler(ret, amb, cfg, c.Pool, handler.WithReranker(checkRerank))

			answerMode := string(domain.AnswerModeGenerated)
			if tc.evidenceOnly {
				answerMode = "evidence"
			}
			body := fmt.Sprintf(`{"query":"reading room","mode":"global","answer_mode":%q,"max_chunks":2,"collection":"archive","filter_doc_type":"guide","allowed_document_ids":[%q,%q,%q]}`,
				answerMode, filter.DocumentIDs[0], filter.DocumentIDs[1], filter.DocumentIDs[2])
			service := metrics.NewService(nil)
			request := httptest.NewRequestWithContext(metrics.WithService(t.Context(), service), http.MethodPost, "/query", strings.NewReader(body))
			request.Header.Set("Content-Type", "application/json")
			if tc.stream {
				request.Header.Set("Accept", "text/event-stream")
			}
			response := httptest.NewRecorder()
			h(response, request)

			wantStatus := http.StatusOK
			if tc.sqlFailure {
				wantStatus = http.StatusInternalServerError
			}
			if tc.cancel {
				wantStatus = http.StatusGatewayTimeout
			}
			require.Equal(t, wantStatus, response.Code, response.Body.String())
			require.Equal(t, 1, emb.calls)
			require.NotContains(t, response.Body.String(), string(private.Chunk.DocumentID))
			for _, absent := range []domain.ScoredChunk{stale, wrong} {
				require.NotContains(t, response.Body.String(), absent.Chunk.Text)
			}
			wantTopKs := []int{2}
			if tc.enabled {
				wantTopKs = append(wantTopKs, 4)
			}
			require.Equal(t, wantTopKs, topKs)
			if tc.want {
				require.Contains(t, response.Body.String(), live.Chunk.Text)
				if !tc.evidenceOnly {
					require.Equal(t, 1, reranks)
				}
			} else {
				require.NotContains(t, response.Body.String(), live.Chunk.Text)
				require.Zero(t, reranks)
			}
		})
	}

	t.Run("local baseline top-up refills within its original scope", func(t *testing.T) {
		local := refillFixture("The reading room closes at seven.", .9)
		other := refillFixture("The catalog desk closes at eight.", .8)
		wrong := refillFixture("Other collection text.", .99)
		insertRefillDocument(t, c, local.Chunk, "archive", "completed")
		insertRefillDocument(t, c, other.Chunk, "archive", "completed")
		insertRefillDocument(t, c, wrong.Chunk, "private", "completed")
		old := local
		old.Chunk.Text = "Superseded preview source."
		old.Score = 1

		ids := []string{string(local.Chunk.DocumentID), string(other.Chunk.DocumentID), string(wrong.Chunk.DocumentID)}
		scope := domain.VectorFilter{Collection: "archive", DocType: "guide", DocumentIDs: ids}
		localScope := scope
		localScope.DocumentIDs = ids[:1]
		calls := 0
		idx := hostedRefillDouble{query: func(_ context.Context, embedding []float32, k int, filter domain.VectorFilter, budget int) ([]domain.ScoredChunk, error) {
			require.Equal(t, []float32{1}, embedding)
			calls++
			switch calls {
			case 1:
				require.Equal(t, 5, k)
				require.Equal(t, scope, filter)
				require.Zero(t, budget)
				return []domain.ScoredChunk{old, wrong, local, other}, nil
			case 2:
				require.Equal(t, 2, k)
				require.Equal(t, localScope, filter)
				require.Zero(t, budget)
				return []domain.ScoredChunk{local}, nil
			case 3:
				require.Equal(t, 2, k)
				require.Equal(t, scope, filter)
				require.Zero(t, budget)
				return []domain.ScoredChunk{wrong, other}, nil
			case 4:
				require.Equal(t, 4, k)
				require.Equal(t, scope, filter)
				require.Equal(t, 1, budget)
				return []domain.ScoredChunk{wrong, local, other}, nil
			default:
				t.Fatal("unexpected query")
				return nil, nil
			}
		}}
		emb := &refillEmbedder{}
		cfg := &config.Config{MaxChunksPerQuery: 10, HostedVisibilityRefill: true, HostedVisibilityExtraRequests: 1}
		query := handler.QueryHandler(retriever.NewRetriever(idx, emb, .7, 10), nil, cfg, c.Pool)
		body := fmt.Sprintf(`{"query":"reading room","mode":"local","answer_mode":"evidence","collection":"archive","filter_doc_type":"guide","max_chunks":2,"allowed_document_ids":[%q,%q,%q]}`,
			ids[0], ids[1], ids[2])
		response := scopeQuery(t, query, body)
		require.Equal(t, http.StatusOK, response.Code, response.Body.String())
		require.Contains(t, response.Body.String(), `"mode":"LOCAL"`)
		require.Contains(t, response.Body.String(), local.Chunk.Text)
		require.Contains(t, response.Body.String(), other.Chunk.Text)
		require.NotContains(t, response.Body.String(), wrong.Chunk.Text)
		require.NotContains(t, response.Body.String(), old.Chunk.Text)
		require.Equal(t, 4, calls)
		require.Equal(t, 1, emb.calls)
	})

	t.Run("mixed probe IDs", func(t *testing.T) { testHostedRefillMixedProbeIDs(t, c) })
	t.Run("revocation during context flush", func(t *testing.T) { testHostedRefillRevocationDuringContextFlush(t, c) })
	t.Run("allowlist policy", func(t *testing.T) { testHostedRefillAllowlistPolicy(t, c) })
	t.Run("grouped allowlist exceeds allowance", func(t *testing.T) { testHostedRefillSkipsGroupedAllowlistOverBudget(t, c) })
}

func testHostedRefillAllowlistPolicy(t *testing.T, c *pgstore.Client) {
	t.Helper()
	for _, tc := range []struct {
		name     string
		ids      string
		required bool
		status   int
		calls    int
	}{
		{name: "omitted", status: 200, calls: 1},
		{name: "null", ids: `,"allowed_document_ids":null`, status: 200, calls: 1},
		{name: "required omitted", required: true, status: 400},
		{name: "required null", ids: `,"allowed_document_ids":null`, required: true, status: 400},
		{name: "empty", ids: `,"allowed_document_ids":[]`, required: true, status: 200},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls []int
			idx := hostedRefillDouble{query: func(_ context.Context, _ []float32, k int, f domain.VectorFilter, budget int) ([]domain.ScoredChunk, error) {
				require.Nil(t, f.DocumentIDs)
				require.Zero(t, budget)
				calls = append(calls, k)
				return nil, nil
			}}
			emb := &refillEmbedder{}
			cfg := &config.Config{RequireDocumentAllowlist: tc.required, HostedVisibilityRefill: true, HostedVisibilityExtraRequests: 1}
			h := handler.QueryHandler(retriever.NewRetriever(idx, emb, .7, 10), nil, cfg, c.Pool)
			resp := scopeQuery(t, h, `{"query":"archive","mode":"global"`+tc.ids+`}`)
			require.Equal(t, tc.status, resp.Code, resp.Body.String())
			require.Len(t, calls, tc.calls)
			require.Equal(t, tc.calls, emb.calls)
		})
	}
}

func testHostedRefillSkipsGroupedAllowlistOverBudget(t *testing.T, c *pgstore.Client) {
	t.Helper()
	stale := refillFixture("Removed document", 1)
	ids := make([]string, 10001)
	ids[0] = string(stale.Chunk.DocumentID)
	for i := range ids[1:] {
		ids[i+1] = uuid.NewString()
	}
	calls := 0
	idx := hostedRefillDouble{query: func(_ context.Context, _ []float32, k int, f domain.VectorFilter, budget int) ([]domain.ScoredChunk, error) {
		calls++
		require.Equal(t, 2, k)
		require.Zero(t, budget)
		require.True(t, slices.Equal(ids, f.DocumentIDs))
		return []domain.ScoredChunk{stale}, nil
	}}
	emb := &refillEmbedder{}
	cfg := &config.Config{MaxChunksPerQuery: 10, HostedVisibilityRefill: true, HostedVisibilityExtraRequests: 1, MaxAllowedDocumentIDs: len(ids)}
	service := metrics.NewService(nil)
	body, err := json.Marshal(handler.QueryRequest{Query: "archive", Mode: "global", MaxChunks: 2, AllowedDocumentIDs: ids})
	require.NoError(t, err)
	req := httptest.NewRequestWithContext(metrics.WithService(t.Context(), service), http.MethodPost, "/query", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler.QueryHandler(retriever.NewRetriever(idx, emb, .7, 10), nil, cfg, c.Pool)(w, req)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.NotContains(t, w.Body.String(), stale.Chunk.Text)
	require.Equal(t, 1, calls)
	require.Equal(t, 1, emb.calls)
	var exposition strings.Builder
	require.NoError(t, service.Registry.WriteText(&exposition))
	require.Contains(t, exposition.String(), `quarry_hosted_visibility_refill_total{result="budget_exhausted"} 1`)
	require.Contains(t, exposition.String(), `quarry_hosted_visibility_refill_total{result="underfill"} 1`)
}

func testHostedRefillMixedProbeIDs(t *testing.T, db *pgstore.Client) {
	t.Helper()
	live := refillFixture("The room opens at eight.", .9)
	insertRefillDocument(t, db, live.Chunk, "archive", "completed")
	missing := refillFixture("", .8)
	missing.Chunk.DocumentID = ""
	for _, enabled := range []bool{false, true} {
		calls := 0
		idx := hostedRefillDouble{query: func(_ context.Context, _ []float32, _ int, _ domain.VectorFilter, _ int) ([]domain.ScoredChunk, error) {
			calls++
			if calls == 1 {
				return []domain.ScoredChunk{live, missing}, nil
			}
			return []domain.ScoredChunk{live}, nil
		}}
		cfg := &config.Config{MaxChunksPerQuery: 10, HostedVisibilityRefill: enabled, HostedVisibilityExtraRequests: 1}
		emb := &refillEmbedder{}
		h := handler.QueryHandler(retriever.NewRetriever(idx, emb, .7, 2), nil, cfg, db.Pool)
		w := scopeQuery(t, h, `{"query":"opening","mode":"local","answer_mode":"evidence","max_chunks":2}`)
		t.Logf("refill=%t: status=%d searches=%d", enabled, w.Code, calls)
		require.Equal(t, http.StatusOK, w.Code, w.Body.String())
		require.Contains(t, w.Body.String(), `"mode":"LOCAL"`)
		require.Equal(t, 1, emb.calls)
		require.Contains(t, w.Body.String(), live.Chunk.Text)
	}
}

type refillFlushRecorder struct {
	*httptest.ResponseRecorder
	afterContext func()
}

func (w *refillFlushRecorder) Flush() {
	w.ResponseRecorder.Flush()
	if callback := w.afterContext; callback != nil {
		w.afterContext = nil
		callback()
	}
}

type countingRefillGenerator struct {
	calls int
}

func (g *countingRefillGenerator) Generate(context.Context, llm.Request) (*llm.Response, error) {
	g.calls++
	return &llm.Response{Text: "An answer.", StopReason: llm.StopEndTurn}, nil
}

func (g *countingRefillGenerator) Stream(ctx context.Context, r llm.Request, _ func(string) error) (*llm.Response, error) {
	return g.Generate(ctx, r)
}

func testHostedRefillRevocationDuringContextFlush(t *testing.T, db *pgstore.Client) {
	t.Helper()
	live := refillFixture("The reading room closes at seven.", .9)
	insertRefillDocument(t, db, live.Chunk, "archive", "completed")
	idx := hostedRefillDouble{query: func(context.Context, []float32, int, domain.VectorFilter, int) ([]domain.ScoredChunk, error) {
		return []domain.ScoredChunk{live}, nil
	}}
	cfg := &config.Config{MaxChunksPerQuery: 10, HostedVisibilityRefill: true, HostedVisibilityExtraRequests: 1, BedrockFallbackModel: "fixture"}
	amb := llm.NewAmbassador(cfg)
	gen := &countingRefillGenerator{}
	amb.SetGenerator(gen)
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/query", strings.NewReader(`{"query":"reading room","mode":"global","collection":"archive"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream")
	w := &refillFlushRecorder{ResponseRecorder: httptest.NewRecorder(), afterContext: func() {
		_, err := db.Pool.Exec(req.Context(), `DELETE FROM documents WHERE id=$1`, live.Chunk.DocumentID)
		require.NoError(t, err)
	}}
	h := handler.QueryHandler(retriever.NewRetriever(idx, &refillEmbedder{}, .7, 10), amb, cfg, db.Pool)
	h(w, req)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.Zero(t, gen.calls)
	require.Contains(t, w.Body.String(), "event: context\n")
	require.Contains(t, w.Body.String(), "event: error\n")
	require.NotContains(t, w.Body.String(), "event: delta\n")
	require.NotContains(t, w.Body.String(), "event: done\n")
}
