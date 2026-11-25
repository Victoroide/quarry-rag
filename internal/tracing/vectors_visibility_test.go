package tracing

import (
	"context"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/store/memory"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
)

type receiptWriter struct{ writes int }

func (w *receiptWriter) UpsertWithVisibility(_ context.Context, _ string, chunks []domain.Chunk) (domain.VisibilityProbe, error) {
	w.writes += len(chunks)
	return func(context.Context, []float32) (domain.IndexVisibility, error) {
		return domain.VisibilityWritesIndexed, nil
	}, nil
}

type refillTestStore struct{ *memory.FakeVectorStore }

func (*refillTestStore) VisibilityRefillLimit() int { return 7 }

func (s *refillTestStore) QueryCandidates(ctx context.Context, embedding []float32, topK int, filter domain.VectorFilter) ([]domain.ScoredChunk, error) {
	return s.QueryFiltered(ctx, embedding, topK, filter)
}

func (s *refillTestStore) QueryFilteredBudgeted(ctx context.Context, embedding []float32, topK int, filter domain.VectorFilter, _ int) ([]domain.ScoredChunk, int, error) {
	chunks, err := s.QueryFiltered(ctx, embedding, topK, filter)
	return chunks, 3, err
}

type writerTestStore struct {
	*memory.FakeVectorStore
	*receiptWriter
}

type hostedTestStore struct {
	*refillTestStore
	*receiptWriter
}

// The two optional hosted capabilities are enabled independently, so tracing
// must keep whichever combination the store provides.
func TestVectorDecoratorKeepsHostedCapabilities(t *testing.T) {
	for _, tt := range []struct {
		name           string
		store          func(*receiptWriter) domain.VectorStore
		writer, refill bool
	}{
		{"neither", func(*receiptWriter) domain.VectorStore { return memory.NewFakeVectorStore() }, false, false},
		{"receipts only", func(w *receiptWriter) domain.VectorStore {
			return &writerTestStore{memory.NewFakeVectorStore(), w}
		}, true, false},
		{"refill only", func(*receiptWriter) domain.VectorStore {
			return &refillTestStore{memory.NewFakeVectorStore()}
		}, false, true},
		{"receipts and refill", func(w *receiptWriter) domain.VectorStore {
			return &hostedTestStore{&refillTestStore{memory.NewFakeVectorStore()}, w}
		}, true, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			recorder := recordSpans(t)
			inner := &receiptWriter{}
			store := WrapVectorStore(tt.store(inner), "pinecone")
			chunks := []domain.Chunk{
				{ID: "a", DocumentID: "doc", Text: "fixture-excerpt", Embedding: []float32{1, 0}},
				{ID: "b", DocumentID: "doc", Text: "fixture-excerpt", Embedding: []float32{1, 0}},
			}

			writer, isWriter := store.(domain.VisibilityWriter)
			require.Equal(t, tt.writer, isWriter, "receipt capability")
			refill, isRefill := store.(domain.VisibilityRefillStore)
			require.Equal(t, tt.refill, isRefill, "refill capability")
			_, isTransactional := store.(domain.TransactionalVectorStore)
			require.False(t, isTransactional)
			require.NoError(t, store.Upsert(t.Context(), "", chunks))

			spans := 1
			if isWriter {
				probe, err := writer.UpsertWithVisibility(t.Context(), "", chunks)
				require.NoError(t, err)
				require.Equal(t, len(chunks), inner.writes, "the write reaches the store")
				got, err := probe(t.Context(), []float32{1, 0})
				require.NoError(t, err)
				require.Equal(t, domain.VisibilityWritesIndexed, got, "the attempt's probe is returned unchanged")
				spans++
			}
			filter := domain.VectorFilter{DocumentIDs: []string{"doc"}}
			if isRefill {
				require.Equal(t, 7, refill.VisibilityRefillLimit())
				found, err := refill.QueryCandidates(t.Context(), []float32{1, 0}, 5, filter)
				require.NoError(t, err)
				require.Len(t, found, 2)
				found, used, err := refill.QueryFilteredBudgeted(t.Context(), []float32{1, 0}, 5, filter, 4)
				require.NoError(t, err)
				require.Len(t, found, 2)
				require.Equal(t, 3, used, "the provider attempt count is returned unchanged")
				spans += 2
			}

			ended := recorder.Ended()
			require.Len(t, ended, spans)
			for _, span := range ended {
				require.Equal(t, trace.SpanKindClient, span.SpanKind())
				for _, attr := range span.Attributes() {
					require.NotContains(t, attr.Value.String(), "fixture-excerpt")
				}
			}
			if isWriter {
				require.Equal(t, "vector.upsert", ended[1].Name())
				require.EqualValues(t, len(chunks), spanAttrs(ended[1])["quarry.vector.records"].AsInt64())
			}
		})
	}
}
