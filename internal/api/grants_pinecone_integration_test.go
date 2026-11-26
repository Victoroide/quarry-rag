package api_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/client"
	"github.com/Victoroide/quarry-rag/internal/domain"
	pcstore "github.com/Victoroide/quarry-rag/internal/store/pinecone"
	pgstore "github.com/Victoroide/quarry-rag/internal/store/postgres"
	"github.com/Victoroide/quarry-rag/internal/testutil/grantse2e"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestGrantsPineconeFiltersBeforeRanking(t *testing.T) {
	f := grantse2e.New(t, "pinecone", (*pgstore.Client).ActivateDeletion)
	docs := f.Seed(t)
	index := f.Pinecone
	reader := f.Client(t, 1, f.Reader)
	req := grantse2e.Query("global", client.AnswerModeEvidence)
	req.Collection, req.FilterDocType, req.MaxChunks = grantse2e.Collection, grantse2e.FileType, 1
	current := index.Record(t, docs.First.ID)
	foreign := index.Record(t, docs.Foreign.ID)
	document, incarnation, attempt, chunk, ok := pcstore.ParseTaggedVectorID(current.Id)
	require.True(t, ok)
	require.Equal(t, docs.First.ID, document)
	require.Equal(t, docs.First.Incarnation, incarnation)
	var committedAttempt string
	require.NoError(t, f.Admin.Pool.QueryRow(t.Context(), "SELECT vector_attempt::text FROM documents WHERE id=$1", document).Scan(&committedAttempt))
	require.Equal(t, committedAttempt, attempt)
	require.Equal(t, attempt, current.Metadata.Fields[domain.VectorAttemptKey].GetStringValue())
	require.Equal(t, incarnation, current.Metadata.Fields[domain.VectorRevisionKey].GetStringValue())
	var committedChunk string
	require.NoError(t, f.Admin.Pool.QueryRow(t.Context(), "SELECT id::text FROM chunks WHERE document_id=$1", document).Scan(&committedChunk))
	committedChunkID, err := uuid.Parse(committedChunk)
	require.NoError(t, err, "invalid committed chunk UUID")
	vectorChunkID, err := uuid.Parse(chunk)
	require.NoError(t, err, "invalid tagged vector chunk UUID")
	require.Equal(t, committedChunkID, vectorChunkID)

	t.Run("empty scope stays local", func(t *testing.T) {
		before := len(index.Calls())
		empty := req
		empty.AllowedDocumentIDs = []string{}
		result, err := reader.Query(t.Context(), empty)
		require.NoError(t, err)
		require.Empty(t, result.ScoredChunks)
		require.Len(t, index.Calls(), before)
	})

	t.Run("wire predicates precede ranking", func(t *testing.T) {
		// The other tenant has a better score even with the same document type.
		index.SetField(t, foreign, "doc_type", docs.First.DocType)
		before := len(index.Calls())
		result, err := reader.Query(t.Context(), req)
		require.NoError(t, err)
		requireEvidence(t, result.ScoredChunks, docs.First)
		calls := index.Calls()[before:]
		require.NotEmpty(t, calls)
		for _, call := range calls {
			require.Equal(t, "/query", call.Path)
			require.Equal(t, "", call.Namespace)
			require.Positive(t, call.TopK)
			require.Equal(t, map[string]any{
				"$and": []any{
					map[string]any{domain.VectorTenantKey: map[string]any{"$eq": f.Owner.Tenant}},
					map[string]any{"doc_id": map[string]any{"$in": []any{docs.First.ID}}},
					map[string]any{"collection": map[string]any{"$eq": grantse2e.Collection}},
					map[string]any{"doc_type": map[string]any{"$eq": grantse2e.FileType}},
					map[string]any{"$or": []any{
						map[string]any{domain.VectorRevisionKey: map[string]any{"$in": []any{docs.First.Incarnation}}},
						map[string]any{
							"revision_id":            map[string]any{"$exists": true},
							domain.VectorRevisionKey: map[string]any{"$exists": false},
						},
					}},
				},
			}, call.Filter)
			require.Equal(t, []string{current.Id}, call.Keys)
		}
	})

	t.Run("SQL rejects stale and inconsistent hits", func(t *testing.T) {
		staleAttempt := index.CloneTagged(t, current, incarnation, uuid.NewString())
		staleIncarnation := index.CloneTagged(t, current, uuid.NewString(), attempt)
		index.Put(t, staleAttempt, staleIncarnation)
		t.Cleanup(func() { index.Remove(t, staleAttempt.Id, staleIncarnation.Id) })
		query := req
		query.MaxChunks = 8
		before := len(index.Calls())
		result, err := reader.Query(t.Context(), query)
		require.NoError(t, err)
		requireEvidence(t, result.ScoredChunks, docs.First)
		for _, call := range index.Calls()[before:] {
			require.Contains(t, call.Keys, staleAttempt.Id)
			require.NotContains(t, call.Keys, staleIncarnation.Id)
		}
		index.AppendQueryHits(foreign, staleIncarnation)
		defer index.AppendQueryHits()
		result, err = reader.Query(t.Context(), query)
		require.NoError(t, err)
		requireEvidence(t, result.ScoredChunks, docs.First)

		// Matching tenant and document are still insufficient when the only
		// returned record carries a retired attempt.
		index.Remove(t, current.Id)
		t.Cleanup(func() { index.Put(t, current) })
		result, err = reader.Query(t.Context(), query)
		require.NoError(t, err)
		require.Empty(t, result.ScoredChunks)
	})

	t.Run("canceled provider request", func(t *testing.T) {
		started, canceled := index.BlockQuery()
		ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
		defer cancel()
		returned := make(chan error, 1)
		go func() {
			_, err := reader.Query(ctx, req)
			returned <- err
		}()
		awaitPineconeSignal(ctx, t, started)
		cancel()
		select {
		case err := <-returned:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(5 * time.Second):
			t.Fatal("query did not return on cancellation")
		}
		drain, stop := context.WithTimeout(t.Context(), 5*time.Second)
		defer stop()
		awaitPineconeSignal(drain, t, canceled)
	})

	t.Run("qualified deletion checkpoints provider failure", func(t *testing.T) {
		stale := index.CloneTagged(t, current, uuid.NewString(), attempt)
		index.Put(t, stale)
		index.FailDeletes(true)
		defer index.FailDeletes(false)
		owner := f.Client(t, 0, f.Owner)
		_, err := owner.DeleteDocumentIncarnation(t.Context(), docs.First.ID, docs.First.Incarnation)
		requireAPIError(t, err, http.StatusInternalServerError, "internal")
		var finalized bool
		var step int
		require.NoError(t, f.Admin.Pool.QueryRow(t.Context(), "SELECT step, finalized FROM document_deletions WHERE document_id=$1", docs.First.ID).Scan(&step, &finalized))
		require.False(t, finalized)
		require.Less(t, step, 3)
		require.Contains(t, index.IDs(), current.Id)
		index.FailDeletes(false)
		result, err := owner.DeleteDocumentIncarnation(t.Context(), docs.First.ID, docs.First.Incarnation)
		require.NoError(t, err)
		require.True(t, result.Deleted)
		f.RequireFinalized(t, docs.First)
		require.NotContains(t, index.IDs(), current.Id)
		require.Contains(t, index.IDs(), stale.Id)
		require.Contains(t, index.IDs(), foreign.Id)
		f.RequireStoredVectors(t, docs.Second, true)
	})
}

func awaitPineconeSignal(ctx context.Context, t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}
