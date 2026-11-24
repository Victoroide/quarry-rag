package pinecone_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/api/handler"
	"github.com/Victoroide/quarry-rag/internal/chunker"
	"github.com/Victoroide/quarry-rag/internal/citationsupport"
	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/embedder"
	"github.com/Victoroide/quarry-rag/internal/llm"
	"github.com/Victoroide/quarry-rag/internal/pipeline"
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
		Text: "Fold the paper.", Embedding: []float32{1, 0, 0}, Metadata: map[string]any{"collection": "archive", "doc_type": "old-category"}}
	insertEvidenceDocument(t, db, ch, "archive", "old-category")
	retired := evidenceMatch(t, ch)
	result := retired
	var upserted *pinecone.Vector
	store := pcstore.NewTestDriverWithUpsert(func(_ context.Context, req *pinecone.QueryByVectorValuesRequest) (*pinecone.QueryVectorsResponse, error) {
		require.NotContains(t, req.MetadataFilter.AsMap(), "doc_type")
		require.NotContains(t, req.MetadataFilter.AsMap(), domain.VectorIncarnationKey)
		return result, nil
	}, func(_ context.Context, vectors []*pinecone.Vector) (uint32, error) {
		require.Len(t, vectors, 1)
		upserted = vectors[0]
		return 1, nil
	})
	cfg := &config.Config{MaxChunksPerQuery: 10}
	query := handler.QueryHandler(retriever.NewRetriever(store, evidenceEmbedder{}, .7, 10), nil, cfg, db.Pool)
	body := `{"query":"Archive?","answer_mode":"evidence","mode":"global","collection":"archive","allowed_document_ids":["` + id + `"]}`
	t.Run("historical record", func(t *testing.T) {
		rec := httptest.NewRecorder()
		serveEvidenceQuery(t, query, rec, "application/json", body)
		require.Equal(t, http.StatusOK, rec.Code)
		require.Contains(t, rec.Body.String(), `"finish_reason":"evidence_only"`)
		require.Contains(t, rec.Body.String(), `"doc_type":"old-category"`)
	})
	ctx := t.Context()
	require.NoError(t, db.DeleteDocument(ctx, ch.DocumentID))
	admission, err := db.AcceptDocument(ctx, domain.AcceptRequest{
		Document: domain.Document{ID: ch.DocumentID, Content: ch.Text}, CallerSupplied: true,
		Collection: "archive", DocType: "old-category", Fingerprint: chunker.ScopedFingerprint("archive", ch.Text), MaxPending: 2,
	})
	require.NoError(t, err)
	require.Equal(t, domain.AcceptAccepted, admission.Outcome)
	job, err := db.ClaimJob(ctx, "worker", time.Minute, 3)
	require.NoError(t, err)
	require.NotNil(t, job)
	require.False(t, job.ScopedVectors)
	ic := pipeline.NewIngestContext(ch.DocumentID, ch.Text, nil)
	ic.Collection, ic.DocType = job.Collection, job.DocType
	ic.Chunks = []domain.Chunk{ch}
	ic.MutateVectors = func(ctx context.Context, call func(context.Context) error) error {
		return db.MutateJobVectors(ctx, *job, call)
	}
	require.NoError(t, pipeline.NewPersistStage(store).Run(ctx, ic))
	published, err := db.CompleteJob(ctx, *job, ic.Chunks, ic.IndexObservation)
	require.NoError(t, err)
	require.True(t, published)
	require.NotNil(t, upserted)
	require.Equal(t, string(ch.ID), upserted.Id)
	fields := upserted.Metadata.AsMap()
	require.NotContains(t, fields, domain.VectorRevisionKey)
	require.NotContains(t, fields, domain.VectorAttemptKey)
	_, err = db.Pool.Exec(ctx, `UPDATE documents SET doc_type='new-category' WHERE id=$1`, id)
	require.NoError(t, err)
	current := &pinecone.QueryVectorsResponse{Matches: []*pinecone.ScoredVector{{Vector: upserted, Score: 1}}}
	for _, accept := range []string{"application/json", "text/event-stream"} {
		t.Run(accept+"/retired record", func(t *testing.T) {
			result = retired
			rec := httptest.NewRecorder()
			serveEvidenceQuery(t, query, rec, accept, body)
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			require.Contains(t, rec.Body.String(), `"finish_reason":"no_evidence"`)
			require.NotContains(t, rec.Body.String(), ch.Text)
		})
		t.Run(accept+"/current record", func(t *testing.T) {
			result = current
			rec := httptest.NewRecorder()
			serveEvidenceQuery(t, query, rec, accept, body)
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			require.Contains(t, rec.Body.String(), `"finish_reason":"evidence_only"`)
			require.NotContains(t, rec.Body.String(), "old-category")
			require.Contains(t, rec.Body.String(), `"doc_type":"new-category"`)
			require.NotContains(t, rec.Body.String(), job.Incarnation)
			require.NotContains(t, rec.Body.String(), domain.VectorIncarnationKey)
		})
	}
	require.Equal(t, job.Incarnation, fields[domain.VectorIncarnationKey])
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
	const originalPreview = "Cedar Archive accepts paper records."
	db := newEvidenceDatabase(t, pgtest.Start(t))
	for _, accept := range []string{"application/json", "text/event-stream"} {
		for _, tc := range []struct {
			name, mode, newPreview string
			replace                bool
			wantError              bool
		}{
			{name: "unchanged document", mode: "auto"},
			{name: "replacement with unchanged preview", mode: "auto", newPreview: originalPreview, replace: true, wantError: true},
			{name: "changed mode", mode: "auto", newPreview: "Maple Museum accepts paper records.", wantError: true},
			{name: "changed local document", mode: "local", newPreview: "Maple Museum accepts paper records.", wantError: true},
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
				insertWithPreview(t.Context(), originalPreview)
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
						if tc.replace {
							_, err := db.Pool.Exec(ctx, `DELETE FROM documents WHERE id=$1`, id)
							require.NoError(t, err)
							insertWithPreview(ctx, tc.newPreview)
						} else if tc.newPreview != "" {
							_, err := db.Pool.Exec(ctx, `UPDATE documents SET content_preview=$2 WHERE id=$1`, id, tc.newPreview)
							require.NoError(t, err)
						}
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

func TestEvidencePolicyRoutingWithStaleHostedHitsAndPreviewErrors(t *testing.T) {
	if testing.Short() {
		t.Skip("requires a Docker daemon")
	}
	db := newEvidenceDatabase(t, pgtest.Start(t))
	staleID, visibleID := uuid.NewString(), uuid.NewString()
	staleChunk := domain.Chunk{
		ID: domain.ChunkID(uuid.NewString()), DocumentID: domain.DocumentID(staleID), Index: 0,
		Text: "Stale Cedar Archive copy.", Metadata: map[string]any{"collection": "archive", "doc_type": "guide"},
	}
	visibleChunk := domain.Chunk{
		ID: domain.ChunkID(uuid.NewString()), DocumentID: domain.DocumentID(visibleID), Index: 1,
		Text: "Fold the paper.", Metadata: map[string]any{"collection": "archive", "doc_type": "guide"},
	}
	insertEvidenceDocument(t, db, staleChunk, "archive", "guide")
	_, err := db.Pool.Exec(t.Context(), `DELETE FROM documents WHERE id=$1`, staleID)
	require.NoError(t, err)
	currentChunk := staleChunk
	currentChunk.Text = "Current Cedar Archive revision."
	insertEvidenceDocument(t, db, currentChunk, "archive", "guide")
	insertEvidenceDocument(t, db, visibleChunk, "archive", "guide")

	mdStale, err := structpb.NewStruct(map[string]any{
		"doc_id": staleID, "chunk_index": float64(0), "text": staleChunk.Text, "collection": "archive", "doc_type": "guide",
	})
	require.NoError(t, err)
	mdVisible, err := structpb.NewStruct(map[string]any{
		"doc_id": visibleID, "chunk_index": float64(1), "text": visibleChunk.Text, "collection": "archive", "doc_type": "guide",
	})
	require.NoError(t, err)
	mdMalformed, err := structpb.NewStruct(map[string]any{
		"doc_id": "not-a-uuid", "chunk_index": float64(0), "text": "Malformed hosted hit.", "collection": "archive", "doc_type": "guide",
	})
	require.NoError(t, err)
	malformedChunkID := uuid.NewString()

	store := pcstore.NewTestDriver(func(_ context.Context, _ *pinecone.QueryByVectorValuesRequest) (*pinecone.QueryVectorsResponse, error) {
		return &pinecone.QueryVectorsResponse{
			Matches: []*pinecone.ScoredVector{
				{Vector: &pinecone.Vector{Id: malformedChunkID, Metadata: mdMalformed}, Score: 1},
				{Vector: &pinecone.Vector{Id: string(staleChunk.ID), Metadata: mdStale}, Score: 0.99},
				{Vector: &pinecone.Vector{Id: string(visibleChunk.ID), Metadata: mdVisible}, Score: 0.85},
			},
		}, nil
	})

	margin := 0.10
	cfg := &config.Config{
		MaxChunksPerQuery:         10,
		RoutingPolicy:             config.RoutingPolicyEvidence,
		RoutingMargin:             &margin,
		RoutingCalibrationMatched: true,
		EffectiveRoutingMargin:    margin,
		BedrockFallbackModel:      "fallback",
	}
	amb := llm.NewAmbassador(cfg)
	amb.SetClient(llm.NewFakeConverser())
	query := handler.QueryHandler(retriever.NewRetriever(store, evidenceEmbedder{}, 0.70, 10), amb, cfg, db.Pool)

	// Stale top hosted hit is discarded before evidence aggregation; sole remaining visible doc leads => LOCAL.
	rec := httptest.NewRecorder()
	serveEvidenceQuery(t, query, rec, "application/json", `{"query":"Archive?","answer_mode":"evidence","collection":"archive"}`)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	var got handler.QueryResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
	require.Equal(t, "LOCAL", got.Mode)
	require.Len(t, got.ScoredChunks, 1)
	require.Equal(t, visibleID, got.ScoredChunks[0].DocID)

	for _, accept := range []string{"application/json", "text/event-stream"} {
		t.Run(accept, func(t *testing.T) {
			generated := httptest.NewRecorder()
			serveEvidenceQuery(t, query, generated, accept, `{"query":"Archive?","collection":"archive"}`)
			require.Equal(t, http.StatusOK, generated.Code, generated.Body.String())
			require.Contains(t, generated.Body.String(), `"mode":"LOCAL"`)
			require.Contains(t, generated.Body.String(), visibleID)
			require.NotContains(t, generated.Body.String(), staleID)
			require.NotContains(t, generated.Body.String(), staleChunk.Text)
			require.NotContains(t, generated.Body.String(), "Malformed hosted hit.")
		})
	}

	for _, staleCount := range []int{4, 5} {
		t.Run("stale hits in the probe "+strconv.Itoa(staleCount), func(t *testing.T) {
			var searchLimits []uint32
			displacing := pcstore.NewTestDriver(func(_ context.Context, req *pinecone.QueryByVectorValuesRequest) (*pinecone.QueryVectorsResponse, error) {
				searchLimits = append(searchLimits, req.TopK)
				require.Equal(t, map[string]any{"collection": map[string]any{"$eq": "archive"}}, req.MetadataFilter.AsMap())
				matches := make([]*pinecone.ScoredVector, 0, 6)
				for range staleCount {
					matches = append(matches, &pinecone.ScoredVector{Vector: &pinecone.Vector{Id: uuid.NewString(), Metadata: mdStale}, Score: 0.99})
				}
				matches = append(matches, &pinecone.ScoredVector{Vector: &pinecone.Vector{Id: string(visibleChunk.ID), Metadata: mdVisible}, Score: 0.85})
				return &pinecone.QueryVectorsResponse{Matches: matches[:min(int(req.TopK), len(matches))]}, nil
			})
			displacedQuery := handler.QueryHandler(retriever.NewRetriever(displacing, evidenceEmbedder{}, 0.70, 10), amb, cfg, db.Pool)
			displaced := httptest.NewRecorder()
			serveEvidenceQuery(t, displacedQuery, displaced, "application/json", `{"query":"Archive?","collection":"archive"}`)
			require.Equal(t, http.StatusOK, displaced.Code, displaced.Body.String())
			require.Contains(t, displaced.Body.String(), `"mode":"GLOBAL"`)
			require.Contains(t, displaced.Body.String(), visibleID)
			require.NotContains(t, displaced.Body.String(), staleID)
			require.Equal(t, []uint32{10}, searchLimits)
		})
	}

	t.Run("scope changes after the context frame", func(t *testing.T) {
		stream := &evidenceRecorder{
			ResponseRecorder: httptest.NewRecorder(),
			onFlush: func() {
				require.NoError(t, db.UpdateDocumentStatus(t.Context(), visibleChunk.DocumentID, "processing"))
			},
		}
		t.Cleanup(func() {
			require.NoError(t, db.UpdateDocumentStatus(context.WithoutCancel(t.Context()), visibleChunk.DocumentID, "completed"))
		})
		serveEvidenceQuery(t, query, stream, "text/event-stream", `{"query":"Archive?","collection":"archive"}`)
		require.Equal(t, http.StatusOK, stream.Code)
		require.Contains(t, stream.Body.String(), "event: context")
		require.Contains(t, stream.Body.String(), "event: error")
		require.NotContains(t, stream.Body.String(), "event: delta")
	})

	// Visibility/preview failure during evidence routing fails closed with 500 instead of silently degrading.
	_, err = db.Pool.Exec(t.Context(), `ALTER TABLE documents RENAME TO unavailable_documents`)
	require.NoError(t, err)
	rec = httptest.NewRecorder()
	serveEvidenceQuery(t, query, rec, "application/json", `{"query":"Archive?","answer_mode":"evidence","collection":"archive"}`)
	_, restoreErr := db.Pool.Exec(t.Context(), `ALTER TABLE unavailable_documents RENAME TO documents`)
	require.NoError(t, restoreErr)
	require.Equal(t, http.StatusInternalServerError, rec.Code, rec.Body.String())
	require.JSONEq(t, `{"error":"pipeline","message":"query processing failed"}`, rec.Body.String())
}

type citationGenerator struct {
	llm.Generator
	generate func(llm.Request) *llm.Response
}

func (g citationGenerator) Generate(_ context.Context, req llm.Request) (*llm.Response, error) {
	return g.generate(req), nil
}

type citationVerifier func(citationsupport.Batch) []citationsupport.Status

func (v citationVerifier) Version() string { return "fixture-pinned-revision" }

func (v citationVerifier) Verify(_ context.Context, batch citationsupport.Batch) ([]citationsupport.Status, error) {
	return v(batch), nil
}

func TestLocalQuerySkipsPineconeHitsWithoutDocumentIDs(t *testing.T) {
	if testing.Short() {
		t.Skip("requires a Docker daemon")
	}
	db := newEvidenceDatabase(t, pgtest.Start(t))
	hit := domain.Chunk{ID: domain.ChunkID(uuid.NewString()), DocumentID: domain.DocumentID(uuid.NewString()), Text: "The library opens at nine.",
		Metadata: map[string]any{"collection": "archive", "doc_type": "guide"}}
	insertEvidenceDocument(t, db, hit, "archive", "guide")
	for _, tc := range []struct {
		name        string
		withSupport bool
	}{{"disabled", false}, {"enabled", true}} {
		t.Run(tc.name, func(t *testing.T) {
			searches := 0
			vectors := pcstore.NewTestDriver(func(_ context.Context, _ *pinecone.QueryByVectorValuesRequest) (*pinecone.QueryVectorsResponse, error) {
				searches++
				result := evidenceMatch(t, hit)
				if searches == 1 {
					missing := evidenceMatch(t, hit).Matches[0]
					missing.Vector.Id = uuid.NewString()
					delete(missing.Vector.Metadata.Fields, "doc_id")
					missing.Score = .75
					result.Matches = append(result.Matches, missing)
				}
				return result, nil
			})
			cfg := &config.Config{MaxChunksPerQuery: 2, BedrockFallbackModel: "fixture"}
			amb := llm.NewAmbassador(cfg)
			amb.SetGenerator(citationGenerator{generate: func(req llm.Request) *llm.Response {
				require.Contains(t, req.Messages[0].Content[0].Text, hit.Text)
				return &llm.Response{Text: "The library opens at nine [1].", StopReason: "end_turn"}
			}})
			var opts []handler.QueryOption
			if tc.withSupport {
				opts = append(opts, handler.WithCitationSupport(citationVerifier(func(batch citationsupport.Batch) []citationsupport.Status {
					require.Equal(t, []citationsupport.Pair{{Claim: "The library opens at nine", Body: hit.Text}}, batch.Pairs)
					return []citationsupport.Status{citationsupport.Supported}
				})))
			}
			query := handler.QueryHandler(retriever.NewRetriever(vectors, evidenceEmbedder{}, .7, 2), amb, cfg, db.Pool, opts...)
			rec := httptest.NewRecorder()
			serveEvidenceQuery(t, query, rec, "application/json", `{"query":"Archive?","mode":"local","collection":"archive","include_citations":true}`)
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			require.Equal(t, 3, searches)
			var got handler.QueryResponse
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
			require.Contains(t, got.Answer, "The library opens at nine")
			require.Len(t, got.Citations, 1)
			require.Equal(t, string(hit.DocumentID), got.Citations[0].DocID)
			if tc.withSupport {
				require.NotNil(t, got.Citations[0].Support)
				require.Equal(t, citationsupport.Supported, got.Citations[0].Support.Status)
			} else {
				require.Nil(t, got.Citations[0].Support)
			}
		})
	}
}

func TestCitationSupportUsesOnlyScopedPineconeBodies(t *testing.T) {
	if testing.Short() {
		t.Skip("requires a Docker daemon")
	}
	db := newEvidenceDatabase(t, pgtest.Start(t))
	allowed := domain.Chunk{ID: domain.ChunkID(uuid.NewString()), DocumentID: domain.DocumentID(uuid.NewString()), Text: "Fold the paper.",
		Metadata: map[string]any{"collection": "archive", "doc_type": "guide"}}
	insertEvidenceDocument(t, db, allowed, "archive", "guide")
	outside := allowed
	outside.ID, outside.DocumentID = domain.ChunkID(uuid.NewString()), domain.DocumentID(uuid.NewString())
	insertEvidenceDocument(t, db, outside, "", "guide")
	stale := allowed
	stale.ID, stale.Text = domain.ChunkID(uuid.NewString()), "Stale paper instruction."
	pineconeVectors := pcstore.NewTestDriver(func(_ context.Context, req *pinecone.QueryByVectorValuesRequest) (*pinecone.QueryVectorsResponse, error) {
		require.Equal(t, map[string]any{"$eq": "archive"}, req.MetadataFilter.AsMap()["collection"])
		return &pinecone.QueryVectorsResponse{Matches: append(evidenceMatch(t, allowed).Matches,
			evidenceMatch(t, outside).Matches[0], evidenceMatch(t, stale).Matches[0])}, nil
	})
	cfg := &config.Config{MaxChunksPerQuery: 10, BedrockFallbackModel: "fallback"}
	amb := llm.NewAmbassador(cfg)
	amb.SetGenerator(citationGenerator{generate: func(req llm.Request) *llm.Response {
		prompt := req.Messages[0].Content[0].Text
		require.Equal(t, 1, strings.Count(prompt, allowed.Text))
		require.NotContains(t, prompt, stale.Text)
		return &llm.Response{Text: "Fold the paper [1][2][3].", StopReason: "end_turn"}
	}})
	calls := 0
	verifier := citationVerifier(func(batch citationsupport.Batch) []citationsupport.Status {
		calls++
		require.Equal(t, []citationsupport.Pair{{Claim: "Fold the paper", Body: allowed.Text}}, batch.Pairs)
		return []citationsupport.Status{citationsupport.Supported}
	})
	query := handler.QueryHandler(retriever.NewRetriever(pineconeVectors, evidenceEmbedder{}, .7, 10), amb, cfg, db.Pool, handler.WithCitationSupport(verifier))
	for _, scope := range []string{"", `,"allowed_document_ids":null`, `,"allowed_document_ids":[]`} {
		rec := httptest.NewRecorder()
		serveEvidenceQuery(t, query, rec, "application/json", `{"query":"Archive?","mode":"global","collection":"archive","include_citations":true`+scope+`}`)
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		var got handler.QueryResponse
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
		if scope == `,"allowed_document_ids":[]` {
			require.Equal(t, "no_evidence", got.FinishReason)
			require.Empty(t, got.Citations)
			continue
		}
		require.Len(t, got.Citations, 1)
		require.Equal(t, string(allowed.DocumentID), got.Citations[0].DocID)
		require.Equal(t, citationsupport.Supported, got.Citations[0].Support.Status)
	}
	require.Equal(t, 2, calls)
}
