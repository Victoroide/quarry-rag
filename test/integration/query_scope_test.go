package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/api/handler"
	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/embedder"
	"github.com/Victoroide/quarry-rag/internal/llm"
	"github.com/Victoroide/quarry-rag/internal/retriever"
	"github.com/Victoroide/quarry-rag/internal/store/memory"
	pgstore "github.com/Victoroide/quarry-rag/internal/store/postgres"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
	brtypes "github.com/aws/aws-sdk-go-v2/service/bedrockruntime/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

type visibilityCountingConverser struct {
	llm.FakeConverser
	count     func(context.Context, []brtypes.Message) (llm.CountResult, error)
	generated int
}

func (*visibilityCountingConverser) AWSRegion() string { return "us-east-1" }

func (c *visibilityCountingConverser) CountTokensWithMaxAttempts(ctx context.Context, _, _ string, messages []brtypes.Message, _ int) (llm.CountResult, error) {
	return c.count(ctx, messages)
}

func (c *visibilityCountingConverser) ConverseStreamWithModelAndSystem(ctx context.Context, model, system string, messages []brtypes.Message, temperature float32, maxTokens int, delta func(string) error) (*bedrockruntime.ConverseOutput, error) {
	c.generated++
	return c.FakeConverser.ConverseStreamWithModelAndSystem(ctx, model, system, messages, temperature, maxTokens, delta)
}

func TestStrictSynthesisRevalidatesOriginalEvidenceBeforeRecountAndGeneration(t *testing.T) {
	if testing.Short() {
		t.Skip("requires a Docker daemon")
	}
	db := visibilityClient(t)
	for _, tc := range []struct {
		name       string
		firstCount int
		mutate     string
	}{
		{"unchanged", 100, ""},
		{"withdrawn before recount", 299100, "UPDATE documents SET status='processing' WHERE id=$1"},
		{"withdrawn before generation", 100, "UPDATE documents SET collection='withdrawn' WHERE id=$1"},
		{"same id with a different body", 299100, "UPDATE chunks SET text='replacement' WHERE document_id=$1"},
		{"visibility lookup failure", 100, "ALTER TABLE chunks RENAME TO unavailable_chunks"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const collection = "archive"
			emb := embedder.NewFakeEmbedder(256)
			vector, err := emb.Embed(t.Context(), "archive")
			require.NoError(t, err)
			vectors := memory.NewFakeVectorStore()
			var ids []string
			for _, group := range []string{collection, "other"} {
				id := uuid.NewString()
				ids = append(ids, id)
				_, err := db.Pool.Exec(t.Context(), `INSERT INTO documents (id,content,status,collection,doc_type) VALUES ($1,'Shared title','completed',$2,'report')`, id, group)
				require.NoError(t, err)
				chunk := domain.Chunk{
					ID: domain.ChunkID(uuid.NewString()), DocumentID: domain.DocumentID(id),
					Text: strings.Repeat("Archive\u200b records are retained. ", 25), Embedding: vector,
					Metadata: map[string]any{"collection": group, "doc_type": "report"},
				}
				require.NoError(t, db.InsertChunks(t.Context(), []domain.Chunk{chunk}))
				require.NoError(t, vectors.Upsert(t.Context(), "", []domain.Chunk{chunk}))
			}
			cfg := &config.Config{AWSRegion: "us-east-1", BedrockFallbackModel: "us.anthropic.claude-haiku-4-5-20251001-v1:0", SynthesisInputProfile: "strict"}
			amb := llm.NewAmbassador(cfg)
			calls := 0
			provider := &visibilityCountingConverser{count: func(ctx context.Context, messages []brtypes.Message) (llm.CountResult, error) {
				calls++
				body, ok := llm.BlockText(messages[0].Content[0])
				require.True(t, ok)
				require.Contains(t, body, "Archive records")
				require.NotContains(t, body, "\u200b")
				if tc.mutate == "ALTER TABLE chunks RENAME TO unavailable_chunks" {
					_, err := db.Pool.Exec(ctx, tc.mutate)
					require.NoError(t, err)
					restoreCtx := context.WithoutCancel(ctx)
					t.Cleanup(func() {
						_, err := db.Pool.Exec(restoreCtx, "ALTER TABLE unavailable_chunks RENAME TO chunks")
						require.NoError(t, err)
					})
				} else if tc.mutate != "" {
					_, err := db.Pool.Exec(ctx, tc.mutate, ids[0])
					require.NoError(t, err)
				}
				return llm.CountResult{InputTokens: tc.firstCount, Attempts: 1, Method: llm.CountMethodBedrockCountTokens}, nil
			}}
			amb.SetClient(provider)
			query := handler.QueryHandler(retriever.NewRetriever(vectors, emb, 0.7, 10), amb, cfg, db.Pool)
			body := fmt.Sprintf(`{"query":"archive","mode":"global","collection":"archive","filter_doc_type":"report","allowed_document_ids":[%q]}`, ids[0])
			req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/query", strings.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Accept", "text/event-stream")
			rec := httptest.NewRecorder()
			query(rec, req)
			require.Equal(t, 1, calls)
			if tc.mutate == "" {
				require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
				require.Equal(t, 1, provider.generated)
				require.Contains(t, rec.Body.String(), "event: done")
			} else {
				require.Equal(t, http.StatusInternalServerError, rec.Code, rec.Body.String())
				require.Zero(t, provider.generated)
				require.NotContains(t, rec.Body.String(), "event:")
				require.NotContains(t, rec.Body.String(), "Archive")
			}
		})
	}
}

func scopeQuery(t *testing.T, h http.HandlerFunc, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/query", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h(rec, req)
	return rec
}

func TestAllowlistIsAppliedInsideVectorQuery(t *testing.T) {
	cfg := &config.Config{BedrockFallbackModel: "fallback"}
	amb := llm.NewAmbassador(cfg)
	amb.SetClient(llm.NewFakeConverser())
	emb := embedder.NewFakeEmbedder(256)
	vector, err := emb.Embed(t.Context(), "archive")
	require.NoError(t, err)
	vs := memory.NewFakeVectorStore()
	chunks := make([]domain.Chunk, 0, 31)
	for i := range 30 {
		chunks = append(chunks, domain.Chunk{ID: domain.ChunkID(fmt.Sprint(i)), DocumentID: domain.DocumentID(uuid.NewString()), Text: "private", Embedding: vector})
	}
	id := uuid.NewString()
	allowed := make([]float32, len(vector))
	for i, v := range vector {
		allowed[i] = -v
	}
	chunks = append(chunks, domain.Chunk{ID: "allowed", DocumentID: domain.DocumentID(id), Text: "The archive opens at nine.", Embedding: allowed})
	require.NoError(t, vs.Upsert(t.Context(), "", chunks))
	h := handler.QueryHandler(retriever.NewRetriever(vs, emb, 0.7, 10), amb, cfg, nil)
	rec := scopeQuery(t, h, fmt.Sprintf(`{"query":"archive","mode":"global","max_chunks":10,"allowed_document_ids":[%q,%q]}`, strings.ToUpper(id), "urn:uuid:"+strings.ToUpper(id)))
	require.Equal(t, 200, rec.Code, rec.Body.String())
	var resp handler.QueryResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	require.Equal(t, 1, resp.ChunksUsed)
	require.Equal(t, id, resp.ScoredChunks[0].DocID)
}

func TestAllowlistEntriesMustBeUUIDs(t *testing.T) {
	h := scopeQueryHandler(&config.Config{})
	rec := scopeQuery(t, h, `{"query":"archive","allowed_document_ids":["not-a-uuid"]}`)
	require.Equal(t, 400, rec.Code, rec.Body.String())
	require.Contains(t, rec.Body.String(), "allowed_document_ids must contain UUIDs")
}

func scopeQueryHandler(cfg *config.Config) http.HandlerFunc {
	amb := llm.NewAmbassador(cfg)
	amb.SetClient(llm.NewFakeConverser())
	return handler.QueryHandler(retriever.NewRetriever(memory.NewFakeVectorStore(), embedder.NewFakeEmbedder(256), 0.7, 10), amb, cfg, nil)
}

func TestAllowlistRequiredWhenConfigured(t *testing.T) {
	h := scopeQueryHandler(&config.Config{RequireDocumentAllowlist: true})
	for _, body := range []string{`{"query":"archive"}`, `{"query":"archive","allowed_document_ids":null}`} {
		rec := scopeQuery(t, h, body)
		require.Equal(t, 400, rec.Code, rec.Body.String())
		require.Contains(t, rec.Body.String(), "allowed_document_ids is required")
	}
	require.Equal(t, 200, scopeQuery(t, h, `{"query":"archive","allowed_document_ids":[]}`).Code)
}

func TestAllowlistLimitAndCanonicalUUIDs(t *testing.T) {
	cfg := &config.Config{MaxAllowedDocumentIDs: 1}
	h := scopeQueryHandler(cfg)
	id := uuid.NewString()
	rec := scopeQuery(t, h, fmt.Sprintf(`{"query":"archive","allowed_document_ids":[%q,%q]}`, id, id))
	require.Equal(t, 400, rec.Code, rec.Body.String())
	cfg.MaxAllowedDocumentIDs = 2
	rec = scopeQuery(t, h, fmt.Sprintf(`{"query":"archive","allowed_document_ids":[%q,%q]}`, id, strings.ToUpper(id)))
	require.Equal(t, 200, rec.Code, rec.Body.String())
}

func TestDocumentPreviewsAreRestrictedToCandidates(t *testing.T) {
	if testing.Short() {
		t.Skip("requires a Docker daemon")
	}
	client := visibilityClient(t)
	allowed, private, noPreview := uuid.NewString(), uuid.NewString(), uuid.NewString()
	for _, row := range []struct{ id, preview string }{{allowed, "Cedar Archive stores records."}, {private, "Falcon Station stores records."}} {
		_, err := client.Pool.Exec(t.Context(), `INSERT INTO documents (id, content, content_preview, status) VALUES ($1, 'text', $2, 'completed')`, row.id, row.preview)
		require.NoError(t, err)
	}
	_, err := client.Pool.Exec(t.Context(), `INSERT INTO documents (id,content,status) VALUES ($1,'text','completed')`, noPreview)
	require.NoError(t, err)
	previews := pgstore.Previews{Pool: client.Pool}
	got, err := previews.DocumentPreviews(t.Context(), []string{allowed, noPreview, uuid.NewString(), allowed})
	require.NoError(t, err)
	require.Equal(t, map[string]string{allowed: "Cedar Archive stores records."}, got)
	got, err = (pgstore.Previews{}).DocumentPreviews(t.Context(), nil)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Empty(t, got)
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = previews.DocumentPreviews(canceled, []string{allowed})
	require.ErrorIs(t, err, context.Canceled)

	_, err = client.Pool.Exec(t.Context(), `UPDATE documents SET collection='archive',doc_type='guide' WHERE id=$1`, allowed)
	require.NoError(t, err)
	previews.Collection, previews.DocType = "archive", "guide"
	got, err = previews.DocumentPreviews(t.Context(), []string{private, allowed})
	require.NoError(t, err)
	require.Equal(t, map[string]string{allowed: "Cedar Archive stores records."}, got)
	_, err = client.Pool.Exec(t.Context(), `UPDATE documents SET status='processing' WHERE id=$1`, allowed)
	require.NoError(t, err)
	got, err = previews.DocumentPreviews(t.Context(), []string{allowed})
	require.NoError(t, err)
	require.Empty(t, got)
}
