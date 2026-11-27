package pipeline

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/stretchr/testify/require"
)

type lineBoundaryReader struct {
	domain.EvidenceVisibility
	lookup func(context.Context, []domain.ScoredChunk) ([]domain.ScoredChunk, error)
}

func (r lineBoundaryReader) VisibleEvidenceWithLineBoundaries(ctx context.Context, hits []domain.ScoredChunk) ([]domain.ScoredChunk, error) {
	return r.lookup(ctx, hits)
}

func TestSynthesisSourceLineBoundariesParity(t *testing.T) {
	marked := []domain.SourceLineBoundary{{Offset: 6, Kind: domain.SourceLineBreak}}
	body := "Page 3 " + strings.Repeat("pallet positions marked ", 200)
	hits := []domain.ScoredChunk{
		{Chunk: domain.Chunk{ID: "first", DocumentID: "doc", Text: body, Metadata: map[string]any{"collection": "archive"}}, Score: 0.9, ObservedIncarnation: "lifetime", ObservedSnapshot: "presearch"},
		{Chunk: domain.Chunk{ID: "wrong-collection", DocumentID: "other", Text: "Other text", VectorIncarnation: "write"}, Score: 0.8},
		{Chunk: domain.Chunk{ID: "post-only", DocumentID: "doc", Text: "Historical", Metadata: map[string]any{"collection": "archive"}}, Score: 0.7, ObservedIncarnation: "lifetime"},
	}
	for _, policy := range []string{config.SynthesisContextPolicyRanked, config.SynthesisContextPolicyDiverse} {
		t.Run(policy, func(t *testing.T) {
			cfg := &config.Config{SynthesisContextPolicy: policy, SynthesisContextTokens: 96}
			initial := QueryContext{AnswerMode: domain.AnswerModeEvidence, RetrievedChunks: hits, Collection: "archive"}
			disabled := initial
			calls := 0
			reader := lineBoundaryReader{lookup: func(_ context.Context, candidates []domain.ScoredChunk) ([]domain.ScoredChunk, error) {
				calls++
				require.Len(t, candidates, 1)
				require.Equal(t, "presearch", candidates[0].ObservedSnapshot)
				require.Empty(t, candidates[0].ObservedIncarnation)
				require.Equal(t, body, candidates[0].Chunk.Text)
				candidates[0].ObservedIncarnation = "lifetime"
				candidates[0].Chunk.SourceLineBoundaries = marked
				return candidates, nil
			}}
			disabled.EvidenceVisibility = reader
			require.NoError(t, NewSynthesisStage(nil, cfg).Run(t.Context(), &disabled))
			require.Zero(t, calls)
			require.True(t, disabled.Truncated[0])
			enabled := initial
			enabled.EvidenceVisibility = reader
			cfg.PreserveSourceLineBoundaries = true
			require.NoError(t, NewSynthesisStage(nil, cfg).Run(t.Context(), &enabled))
			require.Equal(t, 1, calls)
			require.Equal(t, disabled.Excerpts, enabled.Excerpts)
			require.Equal(t, disabled.Truncated, enabled.Truncated)
			require.Equal(t, disabled.ContextTokens, enabled.ContextTokens)
			require.Equal(t, marked, enabled.RetrievedChunks[0].Chunk.SourceLineBoundaries)
			require.Nil(t, hits[0].Chunk.SourceLineBoundaries)
			require.Nil(t, enabled.RetrievedChunks[1].Chunk.SourceLineBoundaries)
			require.Nil(t, enabled.RetrievedChunks[2].Chunk.SourceLineBoundaries)
			enabled.RetrievedChunks[0].Chunk.SourceLineBoundaries[0].Offset++
			require.Equal(t, 6, marked[0].Offset)
		})
	}
}

func TestSynthesisSourceLineBoundariesLookupTimeout(t *testing.T) {
	for _, mode := range []string{"pool acquisition", "SQL stall", "reader deadline", "caller cancellation"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			hit := domain.ScoredChunk{Chunk: domain.Chunk{ID: "chunk", DocumentID: "doc", Text: "a b", VectorIncarnation: "write"}, Score: .9}
			disabled := QueryContext{AnswerMode: domain.AnswerModeEvidence, RetrievedChunks: []domain.ScoredChunk{hit}}
			cfg := &config.Config{SynthesisTimeout: 25 * time.Millisecond}
			require.NoError(t, NewSynthesisStage(nil, cfg).Run(ctx, &disabled))
			qc := QueryContext{AnswerMode: domain.AnswerModeEvidence, RetrievedChunks: []domain.ScoredChunk{hit}}
			connection := make(chan struct{}, 1)
			if mode != "pool acquisition" {
				connection <- struct{}{}
			}
			calls := 0
			qc.EvidenceVisibility = lineBoundaryReader{lookup: func(lookupCtx context.Context, _ []domain.ScoredChunk) ([]domain.ScoredChunk, error) {
				calls++
				select {
				case <-lookupCtx.Done():
					return nil, lookupCtx.Err()
				case <-connection:
				}
				switch mode {
				case "reader deadline":
					readerCtx, stop := context.WithTimeout(lookupCtx, 0)
					defer stop()
					return nil, readerCtx.Err()
				case "caller cancellation":
					cancel()
				}
				<-lookupCtx.Done()
				return nil, lookupCtx.Err()
			}}
			cfg.PreserveSourceLineBoundaries = true
			err := NewSynthesisStage(nil, cfg).Run(ctx, &qc)
			require.Equal(t, 1, calls)
			if mode == "caller cancellation" {
				require.ErrorIs(t, err, context.Canceled)
				require.Empty(t, qc.Excerpts)
				return
			}
			require.NoError(t, ctx.Err())
			require.NoError(t, err)
			require.Equal(t, disabled.Excerpts, qc.Excerpts)
			require.Equal(t, disabled.FinishReason, qc.FinishReason)
			require.Nil(t, qc.RetrievedChunks[0].Chunk.SourceLineBoundaries)
		})
	}
}

func TestHydrateSourceLineBoundariesSkipsOnLowBudget(t *testing.T) {
	hit := domain.ScoredChunk{Chunk: domain.Chunk{ID: "chunk", DocumentID: "doc", Text: "a b", VectorIncarnation: "write"}}
	qc := &QueryContext{RetrievedChunks: []domain.ScoredChunk{hit}}
	qc.EvidenceVisibility = lineBoundaryReader{lookup: func(context.Context, []domain.ScoredChunk) ([]domain.ScoredChunk, error) {
		t.Fatal("optional lookup started with less than 10 ms left")
		return nil, nil
	}}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Millisecond)
	defer cancel()
	if err := hydrateSourceLineBoundaries(ctx, qc, qc.RetrievedChunks); err != nil {
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.ErrorIs(t, ctx.Err(), context.DeadlineExceeded)
	}
}

func TestHydrateSourceLineBoundariesLifetimeAndFailures(t *testing.T) {
	for _, mode := range []string{"no capability", "query error", "canceled", "expired", "missing", "body changed", "lifetime changed", "postgres observation", "write identity", "tagged attempt", "manifest"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			hit := domain.ScoredChunk{Chunk: domain.Chunk{ID: "chunk", DocumentID: "doc", Text: "a b"}, ObservedIncarnation: "observed", ObservedSnapshot: "presearch"}
			qc := &QueryContext{}
			switch mode {
			case "postgres observation":
				qc.vectorObservesIncarnation = true
				hit.ObservedSnapshot = ""
			case "write identity":
				hit.Chunk.VectorIncarnation = "write"
				hit.ObservedSnapshot = ""
			case "tagged attempt":
				hit.Chunk.Metadata = map[string]any{domain.VectorRevisionKey: "rev", domain.VectorAttemptKey: "attempt"}
				hit.ObservedSnapshot = ""
			case "manifest":
				hit.Chunk.RevisionID = "manifest"
				hit.ObservedSnapshot = ""
			}
			qc.RetrievedChunks = []domain.ScoredChunk{hit}
			if mode != "no capability" {
				qc.EvidenceVisibility = lineBoundaryReader{lookup: func(_ context.Context, hits []domain.ScoredChunk) ([]domain.ScoredChunk, error) {
					require.Len(t, hits, 1)
					hits[0].ObservedIncarnation = "observed"
					hits[0].Chunk.SourceLineBoundaries = []domain.SourceLineBoundary{{Offset: 1, Kind: domain.SourceLineBreak}}
					switch mode {
					case "query error":
						return nil, errors.New("db unavailable")
					case "canceled":
						cancel()
					case "expired":
						return nil, context.DeadlineExceeded
					case "missing":
						return nil, nil
					case "body changed":
						hits[0].Chunk.Text = "a c"
					case "lifetime changed":
						hits[0].ObservedIncarnation = "replacement"
					}
					return hits, nil
				}}
			}
			err := hydrateSourceLineBoundaries(ctx, qc, []domain.ScoredChunk{hit})
			if mode == "canceled" {
				require.ErrorIs(t, err, context.Canceled)
			} else {
				require.NoError(t, err)
			}
			switch mode {
			case "postgres observation", "write identity", "tagged attempt", "manifest":
				require.Len(t, qc.RetrievedChunks[0].Chunk.SourceLineBoundaries, 1)
			default:
				require.Nil(t, qc.RetrievedChunks[0].Chunk.SourceLineBoundaries)
			}
		})
	}
}

func TestHydrateSourceLineBoundariesCapsOneScopedBatch(t *testing.T) {
	now := time.Now()
	auth := &ReadAuthorization{ExpiresAt: now.Add(time.Second), Now: func() time.Time { return now }}
	qc := &QueryContext{Authorization: auth}
	for i := range maxSynthesisExcerpts + 2 {
		id := strconv.Itoa(i)
		qc.RetrievedChunks = append(qc.RetrievedChunks, domain.ScoredChunk{Chunk: domain.Chunk{
			ID: domain.ChunkID(id), DocumentID: domain.DocumentID(id), Text: "Page 3", VectorIncarnation: "written",
		}})
	}
	calls := 0
	qc.EvidenceVisibility = lineBoundaryReader{lookup: func(_ context.Context, hits []domain.ScoredChunk) ([]domain.ScoredChunk, error) {
		calls++
		require.Len(t, hits, maxSynthesisExcerpts)
		require.Equal(t, domain.ChunkID("0"), hits[0].Chunk.ID)
		require.Equal(t, domain.ChunkID(strconv.Itoa(maxSynthesisExcerpts-1)), hits[len(hits)-1].Chunk.ID)
		// The credential expires while the batch is in flight.
		now = now.Add(time.Second)
		for i := range hits {
			hits[i].Chunk.SourceLineBoundaries = []domain.SourceLineBoundary{}
		}
		return hits, nil
	}}
	qc.AllowedDocumentIDs = []string{}
	require.NoError(t, hydrateSourceLineBoundaries(t.Context(), qc, qc.RetrievedChunks))
	require.Zero(t, calls)
	qc.AllowedDocumentIDs = nil
	require.NoError(t, hydrateSourceLineBoundaries(t.Context(), qc, qc.RetrievedChunks))
	require.Equal(t, 1, calls)
	for _, hit := range qc.RetrievedChunks {
		require.Nil(t, hit.Chunk.SourceLineBoundaries)
	}
	require.NoError(t, hydrateSourceLineBoundaries(t.Context(), qc, qc.RetrievedChunks))
	require.Equal(t, 1, calls)
}
