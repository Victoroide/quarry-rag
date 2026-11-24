package handler

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/evaluation"
	"github.com/Victoroide/quarry-rag/internal/llm"
	"github.com/Victoroide/quarry-rag/internal/pipeline"
	"github.com/Victoroide/quarry-rag/internal/retriever"
	"github.com/Victoroide/quarry-rag/internal/store/postgres"
	"github.com/Victoroide/quarry-rag/internal/testutil/pgtest"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestGeneratedEvaluationProofUsesFinalExcerpts(t *testing.T) {
	ns := evaluation.Namespace{Prefix: "staging", ID: uuid.New()}
	atlas := evaluation.Documents()[0]
	collection := ns.Collection(false)
	docID := ns.DocumentID(atlas.Key)
	db, err := postgres.NewClient(t.Context(), pgtest.Start(t))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close(context.Background())) })
	require.NoError(t, db.RunMigrations(t.Context()))
	ch := domain.Chunk{ID: domain.ChunkID(uuid.NewString()), DocumentID: domain.DocumentID(docID), Text: atlas.Text}
	receipt := uuid.NewString()
	admit := func(spec evaluation.Document) domain.AcceptResult {
		result, acceptErr := db.AcceptDocument(t.Context(), domain.AcceptRequest{
			Document:   domain.Document{ID: domain.DocumentID(ns.DocumentID(spec.Key)), Content: spec.Text},
			Collection: collection, CallerSupplied: true, ContentDedup: domain.ContentDedupOff,
			DocType: domain.DefaultDocType, Fingerprint: spec.Key, MaxPending: 10,
			EvaluationReceipt: receipt, EvaluationVectorIdentity: "postgres",
		})
		require.NoError(t, acceptErr)
		require.Equal(t, domain.AcceptAccepted, result.Outcome)
		return result
	}
	admit(atlas)
	job, err := db.ClaimJobForFixtureStore(t.Context(), "proof", time.Minute, 2, "postgres")
	require.NoError(t, err)
	require.NotNil(t, job)
	complete, err := db.CompleteJob(t.Context(), *job, []domain.Chunk{ch}, domain.IndexObservation{})
	require.NoError(t, err)
	require.True(t, complete)

	garden, archive := evaluation.Documents()[1], evaluation.Documents()[2]
	ordinary := domain.DocumentID(ns.DocumentID(garden.Key))
	require.NoError(t, db.InsertDocument(t.Context(), domain.Document{ID: ordinary, Content: garden.Text}))
	_, err = db.Pool.Exec(t.Context(), `UPDATE documents SET collection=$1,status='completed' WHERE id=$2`, collection, string(ordinary))
	require.NoError(t, err)
	removed := admit(archive)
	state, err := db.DeleteEvaluationFixture(t.Context(), removed.DocumentID, receipt, removed.Incarnation, "postgres", func(context.Context) error { return nil })
	require.NoError(t, err)
	require.Equal(t, domain.FixtureDeleted, state)
	vector := evidenceQueryVector{search: func(context.Context, []float32, int, domain.VectorFilter) ([]domain.ScoredChunk, error) {
		return []domain.ScoredChunk{
			{Chunk: domain.Chunk{DocumentID: domain.DocumentID(ns.DocumentID("distractor")), Text: atlas.Text}, Score: .9},
			{Chunk: ch, Score: .9},
		}, nil
	}}
	for _, tc := range []struct {
		name, prefix, collection, header, receipt string
		ids                                       any
		budget                                    int
		measured, covered                         bool
	}{
		{"ordinary matching document", ns.Prefix, collection, ns.ID.String(), receipt, []string{docID, string(ordinary)}, 2400, false, false},
		{"retired fixture", ns.Prefix, collection, ns.ID.String(), receipt, []string{docID, string(removed.DocumentID)}, 2400, false, false},
		{"owned", ns.Prefix, collection, ns.ID.String(), receipt, []string{docID}, 2400, true, true},
		{"truncated", ns.Prefix, collection, ns.ID.String(), receipt, []string{docID}, 60, true, false},
		{"missing receipt", ns.Prefix, collection, ns.ID.String(), "", []string{docID}, 2400, false, false},
		{"foreign receipt", ns.Prefix, collection, ns.ID.String(), uuid.NewString(), []string{docID}, 2400, false, false},
		{"off", "", collection, ns.ID.String(), receipt, []string{docID}, 2400, false, false},
		{"unrelated collection", ns.Prefix, "other", ns.ID.String(), receipt, []string{docID}, 2400, false, false},
		{"default collection", ns.Prefix, "", ns.ID.String(), receipt, []string{docID}, 2400, false, false},
		{"unowned allowlist", ns.Prefix, collection, ns.ID.String(), receipt, []string{docID, uuid.NewString()}, 2400, false, false},
		{"no allowlist", ns.Prefix, collection, ns.ID.String(), receipt, nil, 2400, false, false},
		{"empty allowlist", ns.Prefix, collection, ns.ID.String(), receipt, []string{}, 2400, true, false},
		{"invalid header", ns.Prefix, collection, "bad-value", receipt, []string{docID}, 2400, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := streamQueryConfig()
			cfg.EvaluationCollectionPrefix = tc.prefix
			cfg.SynthesisContextTokens = tc.budget
			amb := llm.NewAmbassador(cfg)
			amb.SetClient(llm.NewFakeConverser())
			ret := retriever.NewRetriever(vector, &deadlineEmbedder{release: closedQueryChannel()}, .7, 10)
			query := QueryHandler(ret, amb, cfg, db.Pool)
			body, err := json.Marshal(map[string]any{"query": "Which lamps open the observatory?", "mode": "global", "collection": tc.collection, "allowed_document_ids": tc.ids})
			require.NoError(t, err)
			req := streamQueryRequest(t.Context(), string(body))
			req.Header.Set("Accept", "application/json")
			if tc.receipt != "" {
				req.Header.Set(evaluation.ReceiptHeader, tc.receipt)
			}
			req.Header.Set(evaluation.FixtureHeader, tc.header)
			w := httptest.NewRecorder()
			query.ServeHTTP(w, req)
			require.Equal(t, http.StatusOK, w.Code, w.Body.String())
			header := w.Header().Get(evaluation.MatchesHeader)
			if !tc.measured {
				require.Empty(t, header)
				return
			}
			proof, err := evaluation.DecodeMatches(header)
			require.NoError(t, err)
			if !tc.covered {
				require.Empty(t, proof)
				return
			}
			require.Len(t, proof, 1)
			require.Equal(t, atlas.Spans[0].ID, proof[0].Span)
			require.Greater(t, proof[0].Runes, 600)
			var response QueryResponse
			require.NoError(t, json.Unmarshal(w.Body.Bytes(), &response))
			require.Len(t, response.ScoredChunks, 1)
			require.NotContains(t, response.ScoredChunks[0].Text, atlas.Spans[0].Text)
		})
	}
}

func TestEvaluationExecutionExcludesOfflineAndNoEvidence(t *testing.T) {
	cfg := streamQueryConfig()
	cfg.VectorStore, cfg.EmbeddingProvider, cfg.LLMProvider = "postgres", "openai", "bedrock"
	qc := &pipeline.QueryContext{FinalAnswer: "Four lamps.", QueryEmbedding: []float32{1}, Excerpts: []domain.ScoredChunk{{}}}
	require.Equal(t, "postgres,openai,bedrock", evaluationExecution(cfg, qc))
	cfg.FakeMode = true
	require.Equal(t, "offline", evaluationExecution(cfg, qc))
	cfg.FakeMode = false
	qc.EarlyExit = true
	require.Empty(t, evaluationExecution(cfg, qc))
}

type evaluationVisibility struct {
	domain.EvidenceVisibility
	check func(context.Context, []domain.ScoredChunk) ([]domain.ScoredChunk, error)
}

func (v evaluationVisibility) VerifyEvaluationEvidence(ctx context.Context, _ string, _ []string, chunks []domain.ScoredChunk) (bool, error) {
	visible, err := v.check(ctx, chunks)
	return len(visible) == len(chunks), err
}

func TestEvaluationProofOmitsStaleAndUnavailableEvidence(t *testing.T) {
	ns := evaluation.Namespace{Prefix: "staging", ID: uuid.New()}
	atlas := evaluation.Documents()[0]
	id := ns.DocumentID(atlas.Key)
	selected := domain.ScoredChunk{Chunk: domain.Chunk{DocumentID: domain.DocumentID(id), Text: atlas.Text}}
	retained := selected
	retained.Chunk.Text = atlas.Spans[0].Text
	r := streamQueryRequest(t.Context(), "")
	r.Header.Set(evaluation.FixtureHeader, ns.ID.String())
	r.Header.Set(evaluation.ReceiptHeader, uuid.NewString())
	qc := &pipeline.QueryContext{
		AnswerMode: domain.AnswerModeGenerated, Collection: ns.Collection(false), AllowedDocumentIDs: []string{id},
		RetrievedChunks: []domain.ScoredChunk{selected}, Excerpts: []domain.ScoredChunk{retained},
	}
	require.Empty(t, evaluationProof(t.Context(), r, ns.Prefix, qc))
	for _, storageErr := range []error{nil, errors.New("storage unavailable")} {
		qc.EvidenceVisibility = evaluationVisibility{check: func(_ context.Context, chunks []domain.ScoredChunk) ([]domain.ScoredChunk, error) {
			require.Equal(t, []domain.ScoredChunk{selected}, chunks)
			return nil, storageErr
		}}
		require.Empty(t, evaluationProof(t.Context(), r, ns.Prefix, qc))
	}
}
