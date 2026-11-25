package pipeline

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/stretchr/testify/require"
)

type lsnStore struct {
	persistFilteredStore
	probe domain.VisibilityProbe
}

func (s *lsnStore) UpsertWithVisibility(ctx context.Context, namespace string, chunks []domain.Chunk) (domain.VisibilityProbe, error) {
	return s.probe, s.Upsert(ctx, namespace, chunks)
}

func TestPersistLSNObservation(t *testing.T) {
	for _, mode := range []string{"indexed", "missing receipt", "outage", "shutdown", "late response", "lease lost", "disabled"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ic := visibilityIngest()
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				stopping := make(chan struct{})
				ic.Stopping = stopping
				store := &lsnStore{}
				store.query = func(context.Context, []float32, int, domain.VectorFilter) ([]domain.ScoredChunk, error) {
					t.Fatal("unexpected sample query")
					return nil, nil
				}
				var probes []time.Duration
				start := time.Now()
				store.probe = func(probeCtx context.Context, vector []float32) (domain.IndexVisibility, error) {
					require.Equal(t, ic.Chunks[1].Embedding, vector)
					probes = append(probes, time.Since(start))
					switch mode {
					case "shutdown", "late response", "lease lost":
						<-probeCtx.Done()
						return domain.VisibilityWritesIndexed, nil
					case "outage":
						return "", errors.New("unavailable")
					default:
						if len(probes) == 2 {
							return domain.VisibilityWritesIndexed, nil
						}
						return "", nil
					}
				}
				timeout := 750 * time.Millisecond
				want := domain.VisibilityWritesIndexed
				wantProbes := []time.Duration{0, 200 * time.Millisecond}
				switch mode {
				case "missing receipt":
					store.probe, want, wantProbes = nil, domain.VisibilityUnknown, nil
				case "outage":
					want, wantProbes = domain.VisibilityTimeout, []time.Duration{0, 200 * time.Millisecond, 600 * time.Millisecond}
				case "shutdown":
					time.AfterFunc(50*time.Millisecond, func() { close(stopping) })
					want, wantProbes = domain.VisibilityUnknown, []time.Duration{0}
				case "late response":
					want, wantProbes = domain.VisibilityTimeout, []time.Duration{0}
				case "lease lost":
					time.AfterFunc(50*time.Millisecond, cancel)
					want, wantProbes = "", []time.Duration{0}
				case "disabled":
					timeout, want, wantProbes = 0, domain.VisibilityDisabled, nil
				}
				err := NewPersistStage(store, WithVisibilityWait(timeout), WithLSNVisibility()).Run(ctx, ic)
				require.Len(t, store.writes, 1)
				require.Equal(t, wantProbes, probes)
				require.Equal(t, want, ic.IndexObservation.Visibility)
				if mode == "lease lost" {
					require.ErrorIs(t, err, context.Canceled)
					require.Nil(t, ic.IndexObservation.ObservedAt)
					return
				}
				require.NoError(t, err)
				require.NotNil(t, ic.IndexObservation.ObservedAt)
				require.True(t, time.Now().Equal(*ic.IndexObservation.ObservedAt))
			})
		})
	}
}

func TestPersistSampleObservation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		for _, tied := range []bool{false, true} {
			ic := visibilityIngest()
			store := &persistFilteredStore{query: func(context.Context, []float32, int, domain.VectorFilter) ([]domain.ScoredChunk, error) {
				if tied {
					return []domain.ScoredChunk{{Chunk: ic.Chunks[0], Score: 1}}, nil
				}
				return []domain.ScoredChunk{{Chunk: ic.Chunks[1], Score: 1}}, nil
			}}
			require.NoError(t, NewPersistStage(store, WithVisibilityWait(time.Second)).Run(t.Context(), ic))
			want := domain.VisibilitySampleSeen
			if tied {
				want = domain.VisibilityTimeout
			}
			require.Equal(t, want, ic.IndexObservation.Visibility)
			require.NotNil(t, ic.IndexObservation.ObservedAt)
		}
	})
}
