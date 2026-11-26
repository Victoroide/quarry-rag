package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/citationsupport"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/embedder"
	"github.com/Victoroide/quarry-rag/internal/llm"
	"github.com/Victoroide/quarry-rag/internal/retriever"
	pgstore "github.com/Victoroide/quarry-rag/internal/store/postgres"
	"github.com/Victoroide/quarry-rag/internal/testutil/pgtest"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

type citationScopeGenerator struct {
	llm.Generator
	generate func(context.Context, llm.Request) (*llm.Response, error)
}

func (g citationScopeGenerator) Generate(ctx context.Context, req llm.Request) (*llm.Response, error) {
	return g.generate(ctx, req)
}

func TestCitationScopeRejectsStaleAndReusedEvidence(t *testing.T) {
	ctx := t.Context()
	db, err := pgstore.NewClient(ctx, pgtest.StartVector(t))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close(context.Background())) })
	require.NoError(t, db.RunMigrations(ctx))
	embedding := embedder.NewFakeEmbedder(8)
	vector, err := embedding.Embed(ctx, "archive")
	require.NoError(t, err)
	pgvector, err := pgstore.NewVectorStore(ctx, db, len(vector))
	require.NoError(t, err)

	makeChunk := func() domain.Chunk {
		return domain.Chunk{ID: domain.ChunkID(uuid.NewString()), DocumentID: domain.DocumentID(uuid.NewString()), Text: "Same archival statement.",
			Embedding: vector, Metadata: map[string]any{"doc_type": "record", "collection": "archive"}}
	}
	allowed, other := makeChunk(), makeChunk()
	other.Metadata["collection"] = ""
	persist := func(ch domain.Chunk) {
		t.Helper()
		_, dbErr := db.Pool.Exec(ctx, `INSERT INTO documents(id,content,content_preview,status,collection,doc_type)
			VALUES ($1,$2,'Same title','completed',$3,'record')`, ch.DocumentID, ch.Text, ch.Metadata["collection"])
		require.NoError(t, dbErr)
		require.NoError(t, db.InsertChunks(ctx, []domain.Chunk{ch}))
		require.NoError(t, pgvector.Upsert(ctx, "", []domain.Chunk{ch}))
	}
	persist(allowed)
	persist(other)
	for _, tc := range []struct {
		name       string
		generate   func()
		wantedCode int
	}{
		{"pgvector", func() {}, http.StatusOK},
		{"database outage after generation", func() {
			_, dbErr := db.Pool.Exec(ctx, `ALTER TABLE chunks RENAME TO unavailable_chunks`)
			require.NoError(t, dbErr)
		}, http.StatusInternalServerError},
		{"identical delete and reinsert after generation", func() {
			_, dbErr := db.Pool.Exec(ctx, `DELETE FROM documents WHERE id=$1`, allowed.DocumentID)
			require.NoError(t, dbErr)
			persist(allowed)
		}, http.StatusInternalServerError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.name == "database outage after generation" {
				t.Cleanup(func() {
					_, dbErr := db.Pool.Exec(ctx, `ALTER TABLE unavailable_chunks RENAME TO chunks`)
					require.NoError(t, dbErr)
				})
			}
			cfg := streamQueryConfig()
			amb := llm.NewAmbassador(cfg)
			amb.SetGenerator(citationScopeGenerator{generate: func(_ context.Context, req llm.Request) (*llm.Response, error) {
				prompt := req.Messages[0].Content[0].Text
				require.Equal(t, 1, strings.Count(prompt, allowed.Text))
				tc.generate()
				return &llm.Response{Text: "Archival statement [1][2].", StopReason: "end_turn"}, nil
			}})
			calls := 0
			verifier := handlerVerifier(func(_ context.Context, b citationsupport.Batch) ([]citationsupport.Status, error) {
				calls++
				require.Equal(t, []citationsupport.Pair{{Claim: "Archival statement", Body: allowed.Text}}, b.Pairs)
				return []citationsupport.Status{citationsupport.Supported}, nil
			})
			h := QueryHandler(retriever.NewRetriever(pgvector, embedding, .7, 10), amb, cfg, db.Pool, WithCitationSupport(verifier))
			body := fmt.Sprintf(`{"query":"archive","mode":"global","collection":"archive","allowed_document_ids":[%q],"include_citations":true}`, allowed.DocumentID)
			r := streamQueryRequest(ctx, body)
			r.Header.Del("Accept")
			rec := httptest.NewRecorder()
			h(rec, r)
			require.Equal(t, tc.wantedCode, rec.Code, rec.Body.String())
			if tc.wantedCode == http.StatusOK {
				require.Equal(t, 1, calls)
				var got QueryResponse
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
				require.Len(t, got.Citations, 1)
				require.Equal(t, string(allowed.DocumentID), got.Citations[0].DocID)
			} else {
				require.Zero(t, calls)
				require.NotContains(t, rec.Body.String(), allowed.Text)
			}
		})
	}
}
