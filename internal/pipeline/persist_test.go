package pipeline

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/metrics"
	"github.com/Victoroide/quarry-rag/internal/store/memory"
	"github.com/stretchr/testify/require"
)

type persistVectorWrite struct {
	namespace string
	chunks    []domain.Chunk
}

type persistVectorRecorder struct {
	domain.VectorStore
	writes []persistVectorWrite
	err    error
}

func (s *persistVectorRecorder) Upsert(_ context.Context, namespace string, chunks []domain.Chunk) error {
	s.writes = append(s.writes, persistVectorWrite{namespace: namespace, chunks: chunks})
	return s.err
}

type persistFilteredStore struct {
	persistVectorRecorder
	query func(context.Context, []float32, int, domain.VectorFilter) ([]domain.ScoredChunk, error)
}

func (s *persistFilteredStore) QueryFiltered(ctx context.Context, embedding []float32, topK int, filter domain.VectorFilter) ([]domain.ScoredChunk, error) {
	return s.query(ctx, embedding, topK, filter)
}

func visibilityIngest() *IngestContext {
	ic := NewIngestContext("document-a", "Synthetic document.", nil)
	ic.Chunks = []domain.Chunk{
		{ID: "first", DocumentID: "document-a", Embedding: []float32{1, 0}},
		{ID: "target", DocumentID: "document-a", Embedding: []float32{0, 1}},
		{ID: "unembedded", DocumentID: "document-a"},
	}
	return ic
}

func visibilityMetrics(t *testing.T, service *metrics.Service) string {
	t.Helper()
	var output strings.Builder
	require.NoError(t, service.Registry.WriteText(&output))
	return output.String()
}

func TestPersistWaitsForLastEmbeddedChunk(t *testing.T) {
	for _, queryErrors := range []bool{false, true} {
		t.Run(map[bool]string{false: "delayed", true: "query errors"}[queryErrors], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ic := visibilityIngest()
				service := metrics.NewService(nil)
				vs := &persistFilteredStore{}
				var probes []time.Duration
				start := time.Now()
				vs.query = func(ctx context.Context, embedding []float32, topK int, filter domain.VectorFilter) ([]domain.ScoredChunk, error) {
					require.Len(t, vs.writes, 1)
					require.Equal(t, []float32{0, 1}, embedding)
					require.Equal(t, 2, topK)
					require.Equal(t, domain.VectorFilter{DocumentIDs: []string{"document-a"}}, filter)
					deadline, ok := ctx.Deadline()
					require.True(t, ok)
					require.Equal(t, start.Add(20*time.Second), deadline)
					probes = append(probes, time.Since(start))
					if len(probes) == 6 {
						return []domain.ScoredChunk{{Chunk: ic.Chunks[1]}}, nil
					}
					if queryErrors {
						return nil, errors.New("temporarily unavailable")
					}
					return []domain.ScoredChunk{{Chunk: ic.Chunks[0]}}, nil
				}
				err := NewPersistStage(vs, WithVisibilityWait(20*time.Second)).Run(metrics.WithService(t.Context(), service), ic)
				require.NoError(t, err)
				require.Equal(t, []time.Duration{0, 200 * time.Millisecond, 600 * time.Millisecond, 1400 * time.Millisecond, 3 * time.Second, 5 * time.Second}, probes)
				require.Contains(t, visibilityMetrics(t, service), `quarry_index_visibility_waits_total{result="visible"} 1`)
				require.NotContains(t, visibilityMetrics(t, service), `result="timeout"`)
			})
		})
	}
}

func TestPersistVisibilityTimeoutCompletesWithWarning(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var logs bytes.Buffer
		previous := slog.Default()
		slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
		defer slog.SetDefault(previous)
		service := metrics.NewService(nil)
		queries := 0
		vs := &persistFilteredStore{query: func(context.Context, []float32, int, domain.VectorFilter) ([]domain.ScoredChunk, error) {
			queries++
			return nil, errors.New("query rejected")
		}}
		start := time.Now()
		require.NoError(t, NewPersistStage(vs, WithVisibilityWait(750*time.Millisecond)).Run(metrics.WithService(t.Context(), service), visibilityIngest()))
		require.Equal(t, 750*time.Millisecond, time.Since(start))
		require.Equal(t, 3, queries)
		require.Contains(t, visibilityMetrics(t, service), `quarry_index_visibility_waits_total{result="timeout"} 1`)
		require.NotContains(t, visibilityMetrics(t, service), `result="visible"`)
		require.Contains(t, logs.String(), `"level":"WARN"`)
		require.Contains(t, logs.String(), `"msg":"document vectors not yet searchable"`)
		require.Contains(t, logs.String(), `"document_id":"document-a"`)
		require.Contains(t, logs.String(), `"waited_ms":750`)
		require.Contains(t, logs.String(), `"error":"query rejected"`)
		require.NotContains(t, logs.String(), "Synthetic document.")
	})
}

func TestPersistVisibilityBoundsInFlightQuery(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		vs := &persistFilteredStore{query: func(ctx context.Context, _ []float32, _ int, _ domain.VectorFilter) ([]domain.ScoredChunk, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		}}
		service := metrics.NewService(nil)
		start := time.Now()
		require.NoError(t, NewPersistStage(vs, WithVisibilityWait(time.Second)).Run(metrics.WithService(t.Context(), service), visibilityIngest()))
		require.Equal(t, time.Second, time.Since(start))
		require.Contains(t, visibilityMetrics(t, service), `quarry_index_visibility_waits_total{result="timeout"} 1`)
	})
}

func TestPersistVisibilityCancellation(t *testing.T) {
	for _, mode := range []string{"before probe", "during backoff", "during query", "parent deadline"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				want := context.Canceled
				switch mode {
				case "before probe":
					cancel()
				case "parent deadline":
					var stop context.CancelFunc
					ctx, stop = context.WithTimeout(ctx, 100*time.Millisecond)
					defer stop()
					want = context.DeadlineExceeded
				default:
					time.AfterFunc(100*time.Millisecond, cancel)
				}
				queries := 0
				vs := &persistFilteredStore{query: func(probeCtx context.Context, _ []float32, _ int, _ domain.VectorFilter) ([]domain.ScoredChunk, error) {
					queries++
					if mode == "during query" {
						<-probeCtx.Done()
						return nil, probeCtx.Err()
					}
					return nil, nil
				}}
				service := metrics.NewService(nil)
				err := NewPersistStage(vs, WithVisibilityWait(time.Minute)).Run(metrics.WithService(ctx, service), visibilityIngest())
				require.ErrorIs(t, err, want)
				if mode == "before probe" {
					require.Zero(t, queries)
				} else {
					require.Equal(t, 1, queries)
				}
				require.NotContains(t, visibilityMetrics(t, service), `quarry_index_visibility_waits_total{`)
			})
		})
	}
}

func TestPersistVisibilityAcceptsResultAtDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ic := visibilityIngest()
		service := metrics.NewService(nil)
		vs := &persistFilteredStore{query: func(context.Context, []float32, int, domain.VectorFilter) ([]domain.ScoredChunk, error) {
			time.Sleep(time.Second + time.Nanosecond)
			return []domain.ScoredChunk{{Chunk: ic.Chunks[1]}}, nil
		}}
		require.NoError(t, NewPersistStage(vs, WithVisibilityWait(time.Second)).Run(metrics.WithService(t.Context(), service), ic))
		require.Contains(t, visibilityMetrics(t, service), `quarry_index_visibility_waits_total{result="visible"} 1`)
	})
}

func TestPersistVisibilityEndsWhenStopping(t *testing.T) {
	for _, blocking := range []bool{false, true} {
		t.Run(map[bool]string{false: "during backoff", true: "during query"}[blocking], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				stopping := make(chan struct{})
				ic := visibilityIngest()
				ic.Stopping = stopping
				time.AfterFunc(time.Second, func() { close(stopping) })
				vs := &persistFilteredStore{query: func(ctx context.Context, _ []float32, _ int, _ domain.VectorFilter) ([]domain.ScoredChunk, error) {
					if blocking {
						<-ctx.Done()
						return nil, ctx.Err()
					}
					return nil, nil
				}}
				service := metrics.NewService(nil)
				start := time.Now()
				require.NoError(t, NewPersistStage(vs, WithVisibilityWait(time.Minute)).Run(metrics.WithService(t.Context(), service), ic))
				require.Equal(t, time.Second, time.Since(start))
				require.Contains(t, visibilityMetrics(t, service), `quarry_index_visibility_waits_total{result="timeout"} 1`)
			})
		})
	}
}

func TestPersistSkipsVisibilityWait(t *testing.T) {
	for _, mode := range []string{"default disabled", "explicit disabled", "no embeddings", "no chunks", "unsupported store", "upsert error"} {
		t.Run(mode, func(t *testing.T) {
			vs := &persistFilteredStore{query: func(context.Context, []float32, int, domain.VectorFilter) ([]domain.ScoredChunk, error) {
				t.Fatal("unexpected visibility query")
				return nil, nil
			}}
			ic := visibilityIngest()
			opts := []PersistOption{WithVisibilityWait(time.Second)}
			var store domain.VectorStore = vs
			switch mode {
			case "default disabled":
				opts = nil
			case "explicit disabled":
				opts = []PersistOption{WithVisibilityWait(0)}
			case "no embeddings":
				for i := range ic.Chunks {
					ic.Chunks[i].Embedding = nil
				}
			case "no chunks":
				ic.Chunks = nil
			case "unsupported store":
				store = &vs.persistVectorRecorder
			case "upsert error":
				vs.err = errors.New("upsert failed")
			}
			service := metrics.NewService(nil)
			err := NewPersistStage(store, opts...).Run(metrics.WithService(t.Context(), service), ic)
			if mode == "upsert error" {
				require.ErrorIs(t, err, vs.err)
			} else {
				require.NoError(t, err)
			}
			require.NotContains(t, visibilityMetrics(t, service), `quarry_index_visibility_waits_total{`)
		})
	}
}

func TestPersistVisibilityCapsTopK(t *testing.T) {
	ic := visibilityIngest()
	for range 12 {
		ic.Chunks = append(ic.Chunks, domain.Chunk{ID: "more", Embedding: []float32{1, 0}})
	}
	queries := 0
	vs := &persistFilteredStore{query: func(_ context.Context, embedding []float32, topK int, _ domain.VectorFilter) ([]domain.ScoredChunk, error) {
		queries++
		require.Equal(t, 10, topK)
		require.Equal(t, []float32{1, 0}, embedding)
		return []domain.ScoredChunk{{Chunk: domain.Chunk{ID: "more"}}}, nil
	}}
	require.NoError(t, NewPersistStage(vs, WithVisibilityWait(time.Second)).Run(t.Context(), ic))
	require.Equal(t, 1, queries)
}

func TestPersistVisibilityInMemory(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ic := visibilityIngest()
		vs := memory.NewFakeVectorStore()
		service := metrics.NewService(nil)
		start := time.Now()
		require.NoError(t, NewPersistStage(vs, WithVisibilityWait(time.Second)).Run(metrics.WithService(t.Context(), service), ic))
		require.Zero(t, time.Since(start), "the first search must find the vector without backoff")
		require.Contains(t, visibilityMetrics(t, service), `quarry_index_visibility_waits_total{result="visible"} 1`)
	})
}

func TestPersistWritesOnlyDefaultNamespace(t *testing.T) {
	vs := &persistVectorRecorder{}
	ic := NewIngestContext("document-a", "First paragraph. Second paragraph.", nil)
	ic.DocType = "reference"
	ic.Chunks = []domain.Chunk{
		{ID: "chunk-a", DocumentID: "document-a", Text: "First paragraph.", Embedding: []float32{1, 0}},
		{ID: "chunk-b", DocumentID: "document-a", Index: 1, Text: "Second paragraph.", Embedding: []float32{0, 1}},
	}

	require.NoError(t, NewPersistStage(vs).Run(t.Context(), ic))

	require.Len(t, vs.writes, 1, "each chunk batch must be written once")
	require.Empty(t, vs.writes[0].namespace, "vectors must use the default namespace")
	want := []domain.Chunk{
		{ID: "chunk-a", DocumentID: "document-a", Text: "First paragraph.", Embedding: []float32{1, 0}, Metadata: map[string]any{"doc_type": "reference"}},
		{ID: "chunk-b", DocumentID: "document-a", Index: 1, Text: "Second paragraph.", Embedding: []float32{0, 1}, Metadata: map[string]any{"doc_type": "reference"}},
	}
	require.Equal(t, want, vs.writes[0].chunks)
}

func TestPersistCollectionMetadata(t *testing.T) {
	for _, collection := range []string{"archive", ""} {
		t.Run(collection, func(t *testing.T) {
			ic := visibilityIngest()
			ic.Collection = collection
			vs := &persistVectorRecorder{}
			require.NoError(t, NewPersistStage(vs).Run(t.Context(), ic))
			for _, chunk := range vs.writes[0].chunks {
				if collection == "" {
					require.NotContains(t, chunk.Metadata, "collection")
				} else {
					require.Equal(t, collection, chunk.Metadata["collection"])
				}
			}
		})
	}
}
