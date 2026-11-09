package integration

import (
	"context"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/chunker"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/embedder"
	"github.com/Victoroide/quarry-rag/internal/ocr"
	"github.com/Victoroide/quarry-rag/internal/pipeline"
	"github.com/Victoroide/quarry-rag/internal/store/memory"
	"github.com/Victoroide/quarry-rag/internal/worker"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

type storedTextDetector struct {
	text string
	err  error
}

func (d storedTextDetector) Extract(context.Context, string) (string, error) { return d.text, d.err }

func TestOCRDurableProcessingAndFailure(t *testing.T) {
	for _, tc := range []struct {
		name, text, state string
		err               error
	}{
		{"detected", "The scanned archive retains records for seven years.", "completed", nil},
		{"empty", "", "failed", domain.Permanent(ocr.ErrNoText)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := durableDatabase(t)
			vectors := memory.NewFakeVectorStore()
			key := "synthetic-original"
			const original = "synthetic PDF bytes"
			const collection = "archive"
			fp := chunker.ScopedFingerprint(collection, original)
			req := domain.AcceptRequest{Document: domain.Document{ID: domain.DocumentID(uuid.NewString()), S3Key: &key, TextSource: "ocr_pending"}, Collection: collection, Fingerprint: fp, MaxPending: 100}
			accepted, err := c.AcceptDocument(t.Context(), req)
			require.NoError(t, err)
			require.Equal(t, domain.AcceptAccepted, accepted.Outcome)
			w := worker.New(c, []pipeline.Stage{pipeline.NewChunkStage(chunker.NewChunker(32, 4)), pipeline.NewEmbedStage(embedder.NewFakeEmbedder(256), 2), pipeline.NewPersistStage(vectors)}, vectors, worker.Options{Workers: 1, Lease: time.Second, JobTimeout: time.Minute, PollInterval: 10 * time.Millisecond, OCR: storedTextDetector{text: tc.text, err: tc.err}})
			w.Start(t.Context())
			defer w.Shutdown()
			durableStatus(t, c, string(accepted.DocumentID), tc.state)
			var content, preview, source, fingerprint string
			var lastError *string
			var count int
			require.NoError(t, c.Pool.QueryRow(t.Context(), `SELECT content,content_preview,text_source,fingerprint,last_error,chunk_count FROM documents WHERE id=$1`, string(accepted.DocumentID)).Scan(&content, &preview, &source, &fingerprint, &lastError, &count))
			require.Equal(t, fp, fingerprint)
			if tc.err != nil {
				require.Equal(t, "no_text", *lastError)
				require.Empty(t, content)
				require.Zero(t, count)
				return
			}
			require.Equal(t, tc.text, content)
			require.Equal(t, tc.text, preview)
			require.Equal(t, "ocr", source)
			require.Positive(t, count)
			var chunks int
			require.NoError(t, c.Pool.QueryRow(t.Context(), `SELECT count(*) FROM chunks WHERE document_id=$1`, string(accepted.DocumentID)).Scan(&chunks))
			require.Equal(t, count, chunks)
			matches, err := vectors.QueryFiltered(t.Context(), nil, 10, domain.VectorFilter{Collection: collection})
			require.NoError(t, err)
			require.Len(t, matches, count)
			for _, match := range matches {
				require.Equal(t, accepted.DocumentID, match.Chunk.DocumentID)
			}
			req.Document.ID = domain.DocumentID(uuid.NewString())
			duplicate, err := c.AcceptDocument(t.Context(), req)
			require.NoError(t, err)
			require.Equal(t, domain.AcceptDuplicate, duplicate.Outcome)
			require.Equal(t, accepted.DocumentID, duplicate.DocumentID)
		})
	}
}
