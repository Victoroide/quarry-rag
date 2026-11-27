package postgres

import (
	"testing"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/store/memory"
	"github.com/stretchr/testify/require"
)

func TestVisibleEvidenceSourceLineBoundariesScope(t *testing.T) {
	t.Run("pgvector and service", func(t *testing.T) {
		f := newRetrievalFixture(t)
		ctx := t.Context()
		candidates, err := f.vectors.QueryFiltered(ctx, retrievalQuery, 10, domain.VectorFilter{})
		require.NoError(t, err)
		require.Len(t, candidates, 4)
		marked := []domain.SourceLineBoundary{{Offset: 5, Kind: domain.SourceLineBreak}}
		for _, hit := range candidates {
			require.Nil(t, hit.Chunk.SourceLineBoundaries)
			// The shared document represents a historical row.
			if string(hit.Chunk.DocumentID) != f.shared {
				_, updateErr := f.c.Pool.Exec(ctx, `UPDATE chunks SET source_line_boundaries=ARRAY[5] WHERE id=$1`, hit.Chunk.ID)
				require.NoError(t, updateErr)
			}
		}
		serving, _ := publishedServingClient(t, f.c)
		reader := GrantReader{Pool: serving.Pool}
		check := func(vis domain.SourceLineBoundaryVisibility, hits []domain.ScoredChunk, want ...string) []domain.ScoredChunk {
			t.Helper()
			got, visibilityErr := vis.VisibleEvidenceWithLineBoundaries(ctx, hits)
			require.NoError(t, visibilityErr)
			require.ElementsMatch(t, want, documentIDs(got))
			return got
		}

		owner := reader.Evidence(f.owner, "", "guide").Visibility.(domain.SourceLineBoundaryVisibility)
		rows := check(owner, candidates, f.own, f.shared)
		for _, row := range rows {
			if string(row.Chunk.DocumentID) == f.shared {
				require.Nil(t, row.Chunk.SourceLineBoundaries)
			} else {
				require.Equal(t, marked, row.Chunk.SourceLineBoundaries)
			}
		}
		check(reader.Evidence(f.owner, "archive", "").Visibility.(domain.SourceLineBoundaryVisibility), candidates)
		check(reader.Evidence(f.owner, "", "report").Visibility.(domain.SourceLineBoundaryVisibility), candidates)
		check(Visibility{Pool: serving.Pool}, candidates, f.own, f.shared, f.other, f.peerOnly)
		offline := memory.NewFakeVectorStore()
		for _, hit := range candidates {
			hit.Chunk.Embedding = retrievalQuery
			hit.Chunk.SourceLineBoundaries = marked
			require.NoError(t, offline.Upsert(ctx, "", []domain.Chunk{hit.Chunk}))
		}
		offlineHits, err := offline.QueryFiltered(ctx, retrievalQuery, 4, domain.VectorFilter{})
		require.NoError(t, err)
		for _, hit := range offlineHits {
			require.Nil(t, hit.Chunk.SourceLineBoundaries)
		}
		check(Visibility{Pool: serving.Pool}, offlineHits, f.own, f.shared, f.other, f.peerOnly)
		set := f.createSet(t, f.owner, "", f.shared)
		setReader := reader.EvidenceInSet(f.owner, *f.setRef(set), "", "").Visibility.(domain.SourceLineBoundaryVisibility)
		check(setReader, candidates, f.shared)

		// A caller-supplied sidecar cannot stand in for corrupt or absent SQL data.
		_, err = f.c.Pool.Exec(ctx, `UPDATE chunks SET source_line_boundaries=ARRAY[5,5] WHERE document_id=$1`, f.own)
		require.NoError(t, err)
		for i := range candidates {
			candidates[i].Chunk.SourceLineBoundaries = marked
		}
		for _, row := range check(owner, candidates, f.own, f.shared) {
			require.Nil(t, row.Chunk.SourceLineBoundaries)
		}
		f.grantRead(t, f.peer, f.shared, f.owner, false)
		check(setReader, candidates)
		check(owner, candidates, f.own)
	})

	t.Run("hosted revision", func(t *testing.T) {
		c, serving, _ := restrictedDatabase(t)
		f := grantsManifestFixture(t, c)
		ctx := t.Context()
		const body = "alpha policy text"
		req := ownedRequest(f.owner, "", body)
		req.SupportsScopedVectors = true
		require.Equal(t, domain.AcceptAccepted, f.accept(t, req).Outcome)
		first := startManifestAttempt(t, c, req.Document.ID, "first")
		chunks := first.chunks(req.Document.ID, body)
		marked := []domain.SourceLineBoundary{{Offset: 5, Kind: domain.SourceLineBreak}}
		chunks[0].SourceLineBoundaries = marked
		ok, err := c.CompleteJob(ctx, first.job, chunks, domain.IndexObservation{})
		require.NoError(t, err)
		require.True(t, ok)
		hit := domain.ScoredChunk{Chunk: chunks[0], Score: .9}
		hit.Chunk.SourceLineBoundaries = nil
		hit.Chunk.RevisionID, hit.Chunk.VectorID = first.revision, first.vectorID
		vis := Visibility{Pool: serving.Pool, Subject: &f.owner}
		check := func(hits []domain.ScoredChunk, want int) []domain.ScoredChunk {
			t.Helper()
			rows, err := vis.VisibleEvidenceWithLineBoundaries(ctx, hits)
			require.NoError(t, err)
			require.Len(t, rows, want)
			return rows
		}
		stale := hit
		stale.Chunk.Text = "alpha edited text"
		require.Equal(t, marked, check([]domain.ScoredChunk{stale, hit}, 1)[0].Chunk.SourceLineBoundaries)
		grantExec(t, c, `WITH reopened AS (UPDATE documents SET status='processing' WHERE id=$1 RETURNING id)
			INSERT INTO ingest_jobs (document_id) SELECT id FROM reopened`, req.Document.ID)
		second := persistManifestAttemptWithChunk(ctx, t, c, req.Document.ID, *f.claim(t), first.chunkID)
		active := completeManifestHit(t, c, second, .8)
		require.Equal(t, "retired", manifestState(t, c, first.revision))
		rows := check([]domain.ScoredChunk{hit, active}, 1)
		require.Equal(t, active.Chunk.RevisionID, rows[0].Chunk.RevisionID)
		require.Nil(t, rows[0].Chunk.SourceLineBoundaries)
	})
}
