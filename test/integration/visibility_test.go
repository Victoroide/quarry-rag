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
	"github.com/Victoroide/quarry-rag/internal/testutil/pgtest"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func visibilityClient(t *testing.T) *pgstore.Client {
	t.Helper()
	return visibilityClientAt(t, startPostgres(t))
}

func visibilityClientAt(t *testing.T, dsn string) *pgstore.Client {
	t.Helper()
	client, err := pgstore.NewClient(t.Context(), dsn)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close(context.Background())) })
	require.NoError(t, client.RunMigrations(t.Context()))
	return client
}

func TestVisibilityRules(t *testing.T) {
	if testing.Short() {
		t.Skip("requires a Docker daemon")
	}
	client := visibilityClient(t)
	completed, legacy, processing, failed, missing := uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString()
	for _, row := range []struct{ id, status string }{
		{completed, "completed"}, {legacy, "processing"}, {processing, "processing"}, {failed, "failed"},
	} {
		_, err := client.Pool.Exec(t.Context(), `INSERT INTO documents (id, content, status) VALUES ($1, 'text', $2)`, row.id, row.status)
		require.NoError(t, err)
	}
	require.NoError(t, client.StoreFingerprint(t.Context(), "legacy", legacy))
	require.NoError(t, client.StoreFingerprint(t.Context(), "failed", failed))
	vis := pgstore.Visibility{Pool: client.Pool}
	got, err := vis.VisibleDocumentIDs(t.Context(), []string{completed, strings.ToUpper(completed), completed, legacy, processing, failed, missing, "malformed", ""})
	require.NoError(t, err)
	require.Equal(t, map[string]struct{}{completed: {}}, got)
	for _, ids := range [][]string{nil, {}, {"malformed", ""}} {
		got, err = (pgstore.Visibility{}).VisibleDocumentIDs(t.Context(), ids)
		require.NoError(t, err)
		require.NotNil(t, got)
		require.Empty(t, got)
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = vis.VisibleDocumentIDs(canceled, []string{completed})
	require.ErrorIs(t, err, context.Canceled)
}

type visibilityLaggingVectors struct{ *memory.FakeVectorStore }

func (visibilityLaggingVectors) DeleteAllVectorsInNamespace(context.Context, string) error {
	return nil
}
func (visibilityLaggingVectors) DeleteByDocID(context.Context, string) error { return nil }

func TestOrphanVectorsAreNotCited(t *testing.T) {
	if testing.Short() {
		t.Skip("requires a Docker daemon")
	}
	client := visibilityClient(t)
	completed, failed, processing, missing := uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString()
	for _, row := range []struct{ id, status string }{{completed, "completed"}, {failed, "failed"}, {processing, "processing"}} {
		_, err := client.Pool.Exec(t.Context(), `INSERT INTO documents (id, content, status) VALUES ($1, 'text', $2)`, row.id, row.status)
		require.NoError(t, err)
	}
	vs := visibilityLaggingVectors{memory.NewFakeVectorStore()}
	emb := embedder.NewFakeEmbedder(256)
	vector, err := emb.Embed(t.Context(), "policy")
	require.NoError(t, err)
	for _, row := range []struct{ id, text string }{
		{completed, "Completed policy lasts three years."}, {failed, "Failed policy is confidential."},
		{processing, "Unfinished policy is confidential."}, {missing, "Orphan policy is confidential."},
	} {
		chunks := []domain.Chunk{{ID: domain.ChunkID(uuid.NewString()), DocumentID: domain.DocumentID(row.id), Text: row.text, Embedding: vector}}
		if row.id != missing {
			require.NoError(t, client.InsertChunks(t.Context(), chunks))
		}
		require.NoError(t, vs.Upsert(t.Context(), "", chunks))
	}
	cfg := &config.Config{BedrockFallbackModel: "fallback"}
	amb := llm.NewAmbassador(cfg)
	amb.SetClient(llm.NewFakeConverser())
	query := handler.QueryHandler(retriever.NewRetriever(vs, emb, 0.7, 10), amb, cfg, client.Pool)
	for _, deleted := range []bool{false, true} {
		if deleted {
			req := httptest.NewRequestWithContext(t.Context(), http.MethodDelete, "/documents/"+completed, http.NoBody)
			req.SetPathValue("doc_id", completed)
			rec := httptest.NewRecorder()
			handler.DeleteHandler(client, vs, nil)(rec, req)
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		}
		req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/query", strings.NewReader(`{"query":"policy"}`))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		query(rec, req)
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		var response handler.QueryResponse
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
		require.NotContains(t, response.Answer, "confidential")
		if deleted {
			require.Zero(t, response.ChunksUsed)
			require.Empty(t, response.ScoredChunks)
			require.True(t, response.EarlyExit)
			require.Equal(t, domain.TokenUsage{}, response.TokenUsage)
		} else {
			require.Equal(t, 1, response.ChunksUsed)
			require.Len(t, response.ScoredChunks, 1)
			require.Equal(t, completed, response.ScoredChunks[0].DocID)
			require.Contains(t, response.Answer, "Completed policy")
		}
	}
}

func TestEvidenceRoutingVisibilityAndPreviewWithPostgres(t *testing.T) {
	if testing.Short() {
		t.Skip("requires a Docker daemon")
	}
	client := visibilityClientAt(t, pgtest.StartVector(t))
	vs, err := pgstore.NewVectorStore(t.Context(), client, 4)
	require.NoError(t, err)

	docLate, docCompetitor := uuid.NewString(), uuid.NewString()
	docHarborN, docHarborS, docHidden := uuid.NewString(), uuid.NewString(), uuid.NewString()
	lateContent := "Operational storage manual covering baseline environmental inspections across regional compliance archives without naming specific vaults yet. Alpine Vault dual custody required."
	latePreview := string([]rune(lateContent)[:120])
	require.NotContains(t, latePreview, "Alpine Vault")

	docs := []struct {
		id, collection, preview, text string
		vec                           []float32
	}{
		{docLate, "archive", latePreview, lateContent, []float32{1, 0, 0, 0}},
		{docCompetitor, "archive", "Regional Records.", "Regional Records cover backup custody.", []float32{.95, 0, .312, 0}},
		{docHarborN, "archive", "Harbor Registry North Report.", "Harbor Registry North preserves import manifests seven years.", []float32{0, 1, 0, 0}},
		{docHarborS, "archive", "Harbor Registry South Report.", "Harbor Registry South preserves export manifests ten years.", []float32{0, .65, 0, .76}},
		{docHidden, "private", "Harbor Registry West Report.", "Harbor Registry West holds private manifests.", []float32{0, 1, 0, 0}},
	}
	for _, d := range docs {
		_, insertErr := client.Pool.Exec(t.Context(),
			`INSERT INTO documents (id, content, content_preview, status, collection) VALUES ($1, $2, $3, 'completed', $4)`,
			d.id, d.text, d.preview, d.collection)
		require.NoError(t, insertErr)
		chunks := []domain.Chunk{{
			ID: domain.ChunkID(uuid.NewString()), DocumentID: domain.DocumentID(d.id), Index: 0,
			Text: d.text, Embedding: d.vec, Metadata: map[string]any{"collection": d.collection},
		}}
		require.NoError(t, client.InsertChunks(t.Context(), chunks))
		require.NoError(t, vs.Upsert(t.Context(), "", chunks))
	}

	emb := visibilityQueryEmbedder(func(_ context.Context, query string) ([]float32, error) {
		if strings.Contains(query, "Alpine") {
			return []float32{1, 0, 0, 0}, nil
		}
		return []float32{0, 1, 0, 0}, nil
	})

	margin := 0.20
	cfg := &config.Config{
		BedrockFallbackModel:      "fallback",
		MaxChunksPerQuery:         6,
		RoutingPolicy:             config.RoutingPolicyEvidence,
		RoutingMargin:             &margin,
		RoutingCalibrationMatched: true,
		EffectiveRoutingMargin:    margin,
	}
	amb := llm.NewAmbassador(cfg)
	amb.SetClient(llm.NewFakeConverser())
	query := handler.QueryHandler(retriever.NewRetriever(vs, emb, 0.70, 6), amb, cfg, client.Pool)
	run := func(body string) handler.QueryResponse {
		rec := scopeQuery(t, query, body)
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		var response handler.QueryResponse
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
		for _, sc := range response.ScoredChunks {
			require.NotEqual(t, docHidden, sc.DocID)
		}
		return response
	}

	const alpine = `{"query":"Alpine Vault dual custody","collection":"archive","max_chunks":1}`
	withoutName := run(alpine)
	require.Equal(t, "GLOBAL", withoutName.Mode)
	require.Len(t, withoutName.ScoredChunks, 1)
	require.Equal(t, docLate, withoutName.ScoredChunks[0].DocID)
	_, err = client.Pool.Exec(t.Context(), `UPDATE documents SET content=$2, content_preview=left($2,120) WHERE id=$1`,
		docLate, "Alpine Vault. "+lateContent)
	require.NoError(t, err)
	withName := run(alpine)
	require.Equal(t, "LOCAL", withName.Mode)
	require.Len(t, withName.ScoredChunks, 1)
	require.Equal(t, docLate, withName.ScoredChunks[0].DocID)

	scoresOnly := run(`{"query":"manifest retention","collection":"archive"}`)
	require.Equal(t, "LOCAL", scoresOnly.Mode)
	require.NotEmpty(t, scoresOnly.ScoredChunks)
	require.Equal(t, docHarborN, scoresOnly.ScoredChunks[0].DocID)
	homonym := run(`{"query":"Harbor Registry manifest retention","collection":"archive"}`)
	require.Equal(t, "GLOBAL", homonym.Mode)
	got := make(map[string]bool)
	for _, sc := range homonym.ScoredChunks {
		got[sc.DocID] = true
	}
	require.True(t, got[docHarborN] && got[docHarborS])
	scoped := run(fmt.Sprintf(`{"query":"Harbor Registry manifest retention","collection":"archive","allowed_document_ids":[%q,%q]}`, docHarborN, docHidden))
	require.Equal(t, "LOCAL", scoped.Mode)
	require.Len(t, scoped.ScoredChunks, 1)
	require.Equal(t, docHarborN, scoped.ScoredChunks[0].DocID)
}

type visibilityQueryEmbedder func(context.Context, string) ([]float32, error)

func (e visibilityQueryEmbedder) Embed(ctx context.Context, query string) ([]float32, error) {
	return e(ctx, query)
}

type visibilityFixedEmbedder struct {
	domain.EmbeddingProvider
	vec []float32
}

func (e visibilityFixedEmbedder) Embed(context.Context, string) ([]float32, error) {
	return e.vec, nil
}
