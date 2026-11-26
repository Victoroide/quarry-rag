package pipeline

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/chunker"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestChunkStageProducesFeasibleExactProfilePartition(t *testing.T) {
	profile := testutil.OpenAIProfile(t, "../embeddinginput/testdata/cl100k_base.tiktoken.gz", 1)
	stage := NewChunkStageWithEmbeddingProfile(chunker.NewChunker(3, 1), profile)
	ic := NewIngestContext("doc", "测试测试", nil)
	require.NoError(t, stage.Run(t.Context(), ic))
	require.Len(t, ic.Chunks, 2)
	for i, chunk := range ic.Chunks {
		require.Equal(t, "测试", chunk.Text)
		require.Equal(t, i, chunk.Index)
	}
}

type concurrentIngestEmbedder struct{ active, peak atomic.Int32 }

func (e *concurrentIngestEmbedder) Embed(ctx context.Context, _ string) ([]float32, error) {
	n := e.active.Add(1)
	defer e.active.Add(-1)
	for old := e.peak.Load(); n > old; old = e.peak.Load() {
		if e.peak.CompareAndSwap(old, n) {
			break
		}
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(20 * time.Millisecond):
		return []float32{1}, nil
	}
}
func TestEmbedStageRunsConcurrently(t *testing.T) {
	e := &concurrentIngestEmbedder{}
	ic := NewIngestContext("doc", "", nil)
	ic.Chunks = make([]domain.Chunk, 8)
	require.NoError(t, NewEmbedStage(e, 3).Run(t.Context(), ic))
	require.Equal(t, int32(3), e.peak.Load())
	for _, ch := range ic.Chunks {
		require.Equal(t, []float32{1}, ch.Embedding)
	}
}

type tenantRecordingStore struct {
	domain.VectorStore
	chunks []domain.Chunk
}

func (s *tenantRecordingStore) Upsert(_ context.Context, _ string, chunks []domain.Chunk) error {
	s.chunks = append(s.chunks, chunks...)
	return nil
}

func TestPersistLabelsOwnedChunksWithTheirTenant(t *testing.T) {
	for _, tenant := range []string{"tenant-a", ""} {
		ic := NewIngestContext("doc", "text", nil)
		ic.Tenant = tenant
		ic.Chunks = []domain.Chunk{{ID: "chunk", Embedding: []float32{1}}}
		store := &tenantRecordingStore{}
		require.NoError(t, NewPersistStage(store).Run(t.Context(), ic))
		require.Len(t, store.chunks, 1)
		label, labeled := store.chunks[0].Metadata[domain.VectorTenantKey]
		require.Equal(t, tenant != "", labeled)
		if labeled {
			require.Equal(t, tenant, label)
		}
	}
}

func TestChunkStageUsesLayoutSectionsWhenPresent(t *testing.T) {
	stage := NewChunkStage(chunker.NewChunker(64, 8))
	ic := NewIngestContext("doc-layout", "fallback", nil)
	ic.LayoutSections = []domain.LayoutSection{
		{Page: 1, TableID: "p1-t1", RowID: "p1-t1-r1", Text: "Key: Status | Value: Ready"},
	}
	require.NoError(t, stage.Run(t.Context(), ic))
	require.Len(t, ic.Chunks, 1)
	require.Equal(t, []int{1}, ic.Chunks[0].Metadata["pages"])
	require.Equal(t, []string{"p1-t1"}, ic.Chunks[0].Metadata["table_ids"])
	require.Equal(t, []string{"p1-t1-r1"}, ic.Chunks[0].Metadata["row_ids"])
}
