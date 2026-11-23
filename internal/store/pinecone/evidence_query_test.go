package pinecone_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/api/handler"
	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/embedder"
	"github.com/Victoroide/quarry-rag/internal/retriever"
	pcstore "github.com/Victoroide/quarry-rag/internal/store/pinecone"
	pgstore "github.com/Victoroide/quarry-rag/internal/store/postgres"
	"github.com/Victoroide/quarry-rag/internal/testutil/pgtest"
	"github.com/google/uuid"
	"github.com/pinecone-io/go-pinecone/v5/pinecone"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"
)

type evidenceEmbedder struct{ domain.EmbeddingProvider }

func (evidenceEmbedder) Embed(_ context.Context, text string) ([]float32, error) {
	if text != "Archive?" {
		return nil, errUnexpectedQuestion
	}
	return []float32{1, 0, 0}, nil
}

var errUnexpectedQuestion = errors.New("unexpected embedding input")

type evidenceRecorder struct {
	*httptest.ResponseRecorder
	onFlush func()
}

func (r *evidenceRecorder) Flush() {
	r.ResponseRecorder.Flush()
	if r.onFlush != nil {
		onFlush := r.onFlush
		r.onFlush = nil
		onFlush()
	}
}

func newEvidenceDatabase(t *testing.T, databaseURL string) *pgstore.Client {
	t.Helper()
	db, err := pgstore.NewClient(t.Context(), databaseURL)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close(context.Background())) })
	require.NoError(t, db.RunMigrations(t.Context()))
	return db
}

func insertEvidenceDocument(t *testing.T, db *pgstore.Client, ch domain.Chunk, collection, docType string) {
	t.Helper()
	_, err := db.Pool.Exec(t.Context(), `INSERT INTO documents(id,content,status,collection,doc_type)
		VALUES ($1,$2,'completed',$3,$4)`, string(ch.DocumentID), ch.Text, collection, docType)
	require.NoError(t, err)
	require.NoError(t, db.InsertChunks(t.Context(), []domain.Chunk{ch}))
}

func evidenceMatch(t *testing.T, ch domain.Chunk) *pinecone.QueryVectorsResponse {
	t.Helper()
	md, err := structpb.NewStruct(map[string]any{"doc_id": string(ch.DocumentID), "chunk_index": float64(ch.Index), "text": ch.Text, "collection": ch.Metadata["collection"], "doc_type": ch.Metadata["doc_type"]})
	require.NoError(t, err)
	return &pinecone.QueryVectorsResponse{Matches: []*pinecone.ScoredVector{{Vector: &pinecone.Vector{Id: string(ch.ID), Metadata: md}, Score: 1}}}
}

func serveEvidenceQuery(t *testing.T, query http.Handler, w http.ResponseWriter, accept, body string) {
	t.Helper()
	r := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/query", strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Accept", accept)
	query.ServeHTTP(w, r)
}

func TestEvidenceQueryUsesCurrentCategoryAfterDocumentReuse(t *testing.T) {
	if testing.Short() {
		t.Skip("requires a Docker daemon")
	}
	db := newEvidenceDatabase(t, pgtest.Start(t))
	id := uuid.NewString()
	ch := domain.Chunk{ID: domain.ChunkID(uuid.NewString()), DocumentID: domain.DocumentID(id), Index: 2,
		Text: "Fold the paper.", Metadata: map[string]any{"collection": "archive", "doc_type": "old-category"}}
	insertEvidenceDocument(t, db, ch, "archive", "old-category")
	_, err := db.Pool.Exec(t.Context(), `DELETE FROM documents WHERE id=$1`, id)
	require.NoError(t, err)
	insertEvidenceDocument(t, db, ch, "archive", "new-category")
	store := pcstore.NewTestDriver(func(_ context.Context, req *pinecone.QueryByVectorValuesRequest) (*pinecone.QueryVectorsResponse, error) {
		require.NotContains(t, req.MetadataFilter.AsMap(), "doc_type")
		return evidenceMatch(t, ch), nil
	})
	cfg := &config.Config{MaxChunksPerQuery: 10}
	query := handler.QueryHandler(retriever.NewRetriever(store, evidenceEmbedder{}, .7, 10), nil, cfg, db.Pool)
	body := `{"query":"Archive?","answer_mode":"evidence","mode":"global","collection":"archive","allowed_document_ids":["` + id + `"]}`
	for _, accept := range []string{"application/json", "text/event-stream"} {
		t.Run(accept, func(t *testing.T) {
			rec := httptest.NewRecorder()
			serveEvidenceQuery(t, query, rec, accept, body)
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			require.Contains(t, rec.Body.String(), `"finish_reason":"evidence_only"`)
			require.NotContains(t, rec.Body.String(), "old-category")
			require.Contains(t, rec.Body.String(), `"doc_type":"new-category"`)
		})
	}
	t.Run("category changes before completion", func(t *testing.T) {
		rec := &evidenceRecorder{ResponseRecorder: httptest.NewRecorder(), onFlush: func() {
			_, err := db.Pool.Exec(t.Context(), `UPDATE documents SET doc_type='later-category' WHERE id=$1`, id)
			require.NoError(t, err)
		}}
		serveEvidenceQuery(t, query, rec, "text/event-stream", body)
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		require.Contains(t, rec.Body.String(), `"doc_type":"new-category"`)
		require.Contains(t, rec.Body.String(), "event: error\n")
		require.NotContains(t, rec.Body.String(), "event: done\n")
		require.NotContains(t, rec.Body.String(), "later-category")
	})
}

func TestEvidenceQueryRejectsRevokedRoutingEvidenceBeforeResponse(t *testing.T) {
	if testing.Short() {
		t.Skip("requires a Docker daemon")
	}
	db := newEvidenceDatabase(t, pgtest.Start(t))
	for _, accept := range []string{"application/json", "text/event-stream"} {
		t.Run(accept, func(t *testing.T) {
			id := uuid.NewString()
			ch := domain.Chunk{ID: domain.ChunkID(uuid.NewString()), DocumentID: domain.DocumentID(id), Index: 2,
				Text: "Fold the paper.", Metadata: map[string]any{"collection": "archive", "doc_type": "guide"}}
			insertEvidenceDocument(t, db, ch, "archive", "guide")
			searches := 0
			store := pcstore.NewTestDriver(func(ctx context.Context, req *pinecone.QueryByVectorValuesRequest) (*pinecone.QueryVectorsResponse, error) {
				searches++
				switch searches {
				case 1:
					require.EqualValues(t, 5, req.TopK)
					return evidenceMatch(t, ch), nil
				case 2:
					ids := req.MetadataFilter.AsMap()["doc_id"]
					require.Equal(t, map[string]any{"$in": []any{id}}, ids, "probe must have selected local retrieval")
					_, err := db.Pool.Exec(ctx, `DELETE FROM documents WHERE id=$1`, id)
					require.NoError(t, err)
				}
				return &pinecone.QueryVectorsResponse{}, nil
			})
			cfg := &config.Config{MaxChunksPerQuery: 10}
			query := handler.QueryHandler(retriever.NewRetriever(store, evidenceEmbedder{}, .7, 10), nil, cfg, db.Pool)
			rec := httptest.NewRecorder()
			serveEvidenceQuery(t, query, rec, accept, `{"query":"Archive?","answer_mode":"evidence","collection":"archive"}`)
			require.Equal(t, 3, searches)
			require.Equal(t, http.StatusInternalServerError, rec.Code, rec.Body.String())
			require.Equal(t, "application/json", rec.Header().Get("Content-Type"))
			require.JSONEq(t, `{"error":"pipeline","message":"query processing failed"}`, rec.Body.String())
		})
	}
}

func TestEvidenceQueryChecksNamedEntityRouteAfterDocumentReuse(t *testing.T) {
	if testing.Short() {
		t.Skip("requires a Docker daemon")
	}
	db := newEvidenceDatabase(t, pgtest.Start(t))
	for _, accept := range []string{"application/json", "text/event-stream"} {
		for _, tc := range []struct {
			name, mode, newPreview string
			wantError              bool
		}{
			{"unchanged preview", "auto", "Cedar Archive accepts paper records.", false},
			{"changed mode", "auto", "Maple Museum accepts paper records.", true},
			{"changed local document", "local", "Maple Museum accepts paper records.", true},
		} {
			t.Run(accept+"/"+tc.name, func(t *testing.T) {
				id := uuid.NewString()
				ch := domain.Chunk{ID: domain.ChunkID(uuid.NewString()), DocumentID: domain.DocumentID(id), Index: 2,
					Text: "Fold the paper.", Metadata: map[string]any{"collection": "archive", "doc_type": "guide"}}
				insertWithPreview := func(ctx context.Context, preview string) {
					t.Helper()
					_, err := db.Pool.Exec(ctx, `INSERT INTO documents(id,content,content_preview,status,collection,doc_type)
						VALUES ($1,$2,$3,'completed','archive','guide')`, id, preview+"\n\n"+ch.Text, preview)
					require.NoError(t, err)
					require.NoError(t, db.InsertChunks(ctx, []domain.Chunk{ch}))
				}
				insertWithPreview(t.Context(), "Cedar Archive accepts paper records.")
				probe := evidenceMatch(t, ch)
				probe.Matches[0].Score = 0
				if tc.mode == "local" {
					other := ch
					other.ID = domain.ChunkID(uuid.NewString())
					other.DocumentID = domain.DocumentID(uuid.NewString())
					insertEvidenceDocument(t, db, other, "archive", "guide")
					fallback := evidenceMatch(t, other).Matches[0]
					fallback.Score = .25
					probe.Matches = append(probe.Matches, fallback)
				}
				searches := 0
				store := pcstore.NewTestDriver(func(ctx context.Context, req *pinecone.QueryByVectorValuesRequest) (*pinecone.QueryVectorsResponse, error) {
					searches++
					switch searches {
					case 1:
						return probe, nil
					case 2:
						require.Equal(t, map[string]any{"$in": []any{id}}, req.MetadataFilter.AsMap()["doc_id"])
						_, err := db.Pool.Exec(ctx, `DELETE FROM documents WHERE id=$1`, id)
						require.NoError(t, err)
						insertWithPreview(ctx, tc.newPreview)
					}
					result := evidenceMatch(t, ch)
					result.Matches[0].Score = 0
					return result, nil
				})
				cfg := &config.Config{MaxChunksPerQuery: 10}
				query := handler.QueryHandler(retriever.NewRetriever(store, embedder.NewFakeEmbedder(3), .7, 10), nil, cfg, db.Pool)
				rec := httptest.NewRecorder()
				body := `{"query":"Cedar Archive","answer_mode":"evidence","mode":"` + tc.mode + `","collection":"archive"}`
				serveEvidenceQuery(t, query, rec, accept, body)
				require.Equal(t, 3, searches)
				if tc.wantError {
					require.Equal(t, http.StatusInternalServerError, rec.Code, rec.Body.String())
					require.Equal(t, "application/json", rec.Header().Get("Content-Type"))
					require.JSONEq(t, `{"error":"pipeline","message":"query processing failed"}`, rec.Body.String())
					return
				}
				require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
				require.Equal(t, accept, rec.Header().Get("Content-Type"))
				require.Contains(t, rec.Body.String(), `"mode":"LOCAL"`)
				require.Contains(t, rec.Body.String(), `"finish_reason":"evidence_only"`)
				require.Contains(t, rec.Body.String(), ch.Text)
			})
		}
	}
}

func TestEvidenceQueryChecksCurrentScopeAcrossVectorStores(t *testing.T) {
	if testing.Short() {
		t.Skip("requires a Docker daemon")
	}
	ctx := t.Context()
	db, err := pgstore.NewClient(ctx, pgtest.StartVector(t))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close(context.Background())) })
	require.NoError(t, db.RunMigrations(ctx))
	pgVectors, err := pgstore.NewVectorStore(ctx, db, 3)
	require.NoError(t, err)

	archiveID, defaultID := uuid.NewString(), uuid.NewString()
	archive := domain.Chunk{ID: domain.ChunkID(uuid.NewString()), DocumentID: domain.DocumentID(archiveID), Index: 2, Text: "Fold the paper.", Embedding: []float32{1, 0, 0}, Metadata: map[string]any{"collection": "archive", "doc_type": "guide"}}
	unlabelled := domain.Chunk{ID: domain.ChunkID(uuid.NewString()), DocumentID: domain.DocumentID(defaultID), Index: 2, Text: archive.Text, Embedding: []float32{1, 1, 0}, Metadata: map[string]any{"doc_type": "guide"}}
	insertDocument := func(ch domain.Chunk, collection string) {
		t.Helper()
		_, err := db.Pool.Exec(ctx, `INSERT INTO documents(id,content,status,collection,doc_type,metadata)
			VALUES ($1,$2,'completed',$3,'guide','{"title":"Shared title"}')`, string(ch.DocumentID), ch.Text, collection)
		require.NoError(t, err)
		require.NoError(t, db.InsertChunks(ctx, []domain.Chunk{ch}))
	}
	insertDocument(archive, "archive")
	insertDocument(unlabelled, "")
	require.NoError(t, pgVectors.Upsert(ctx, "", []domain.Chunk{archive, unlabelled}))

	stale := []domain.Chunk{archive, unlabelled}
	pineconeVectors := pcstore.NewTestDriver(func(_ context.Context, req *pinecone.QueryByVectorValuesRequest) (*pinecone.QueryVectorsResponse, error) {
		require.Equal(t, []float32{1, 0, 0}, req.Vector)
		fields := req.MetadataFilter.AsMap()
		require.Equal(t, map[string]any{"$eq": "guide"}, fields["doc_type"])
		var matches []*pinecone.ScoredVector
		// Return both hits so that service-side scope checks are exercised too.
		for i, ch := range stale {
			md, err := structpb.NewStruct(map[string]any{"doc_id": string(ch.DocumentID), "chunk_index": float64(ch.Index), "text": ch.Text, "collection": ch.Metadata["collection"], "doc_type": ch.Metadata["doc_type"]})
			require.NoError(t, err)
			matches = append(matches, &pinecone.ScoredVector{Vector: &pinecone.Vector{Id: string(ch.ID), Metadata: md}, Score: 1 - float32(i)/4})
		}
		return &pinecone.QueryVectorsResponse{Matches: matches}, nil
	})
	for _, tt := range []struct {
		name  string
		store domain.VectorStore
	}{{"pinecone", pineconeVectors}, {"pgvector", pgVectors}} {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &config.Config{MaxChunksPerQuery: 10}
			query := handler.QueryHandler(retriever.NewRetriever(tt.store, evidenceEmbedder{}, .7, 10), nil, cfg, db.Pool)
			request := func(scope string, stream bool, w http.ResponseWriter) {
				t.Helper()
				r := httptest.NewRequestWithContext(ctx, http.MethodPost, "/query", strings.NewReader(`{"query":"Archive?","answer_mode":"evidence","filter_doc_type":"guide","mode":"global"`+scope+`}`))
				r.Header.Set("Content-Type", "application/json")
				if stream {
					r.Header.Set("Accept", "text/event-stream")
				}
				query.ServeHTTP(w, r)
			}
			for _, scope := range []struct {
				body string
				ids  []string
			}{
				{`,"collection":"archive","allowed_document_ids":["` + archiveID + `"]`, []string{archiveID}},
				{`,"allowed_document_ids":["` + defaultID + `"]`, []string{defaultID}},
				{`,"collection":"archive","allowed_document_ids":["` + defaultID + `"]`, nil},
				{`,"collection":"archive","allowed_document_ids":null`, []string{archiveID}},
				{"", []string{archiveID, defaultID}},
				{`,"allowed_document_ids":[]`, nil},
			} {
				rec := httptest.NewRecorder()
				request(scope.body, false, rec)
				require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
				var result handler.QueryResponse
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &result))
				require.Len(t, result.ScoredChunks, len(scope.ids))
				for i, chunk := range result.ScoredChunks {
					require.Equal(t, scope.ids[i], chunk.DocID)
					require.Equal(t, 2, chunk.ChunkIndex)
					require.Equal(t, "Fold the paper.", chunk.Text)
				}
			}

			scope := `,"collection":"archive","allowed_document_ids":["` + archiveID + `"]`
			rec := &evidenceRecorder{ResponseRecorder: httptest.NewRecorder(), onFlush: func() {
				_, err := db.Pool.Exec(ctx, `UPDATE documents SET status='processing' WHERE id=$1`, archiveID)
				require.NoError(t, err)
			}}
			request(scope, true, rec)
			body := rec.Body.String()
			require.Contains(t, body, "event: context\n")
			require.Contains(t, body, `"doc_id":"`+archiveID+`"`)
			require.NotContains(t, body, "event: done\n")
			require.Contains(t, body, "event: error\n")
			require.NotContains(t, body, defaultID)

			_, err := db.Pool.Exec(ctx, `DELETE FROM documents WHERE id=$1`, archiveID)
			require.NoError(t, err)
			replacement := archive
			replacement.Text = "New paper instructions."
			insertDocument(replacement, "archive")
			require.NoError(t, pgVectors.Upsert(ctx, "", []domain.Chunk{archive}))
			rec = &evidenceRecorder{ResponseRecorder: httptest.NewRecorder()}
			request(scope, false, rec)
			require.Contains(t, rec.Body.String(), `"finish_reason":"no_evidence"`)
			require.NotContains(t, rec.Body.String(), "Fold the paper.")
			require.NotContains(t, rec.Body.String(), replacement.Text)

			_, err = db.Pool.Exec(ctx, `ALTER TABLE chunks RENAME TO unavailable_chunks`)
			require.NoError(t, err)
			rec = &evidenceRecorder{ResponseRecorder: httptest.NewRecorder()}
			request(scope, true, rec)
			require.Equal(t, http.StatusInternalServerError, rec.Code)
			require.Equal(t, "application/json", rec.Header().Get("Content-Type"))
			require.NotContains(t, rec.Body.String(), "Fold the paper.")
			_, err = db.Pool.Exec(ctx, `ALTER TABLE unavailable_chunks RENAME TO chunks`)
			require.NoError(t, err)

			require.NoError(t, db.InsertChunks(ctx, []domain.Chunk{archive}))
		})
	}
}
