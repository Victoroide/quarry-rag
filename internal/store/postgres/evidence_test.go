package postgres

import (
	"context"
	"net/url"
	"strings"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestEvidenceVisibilityMatchesUTF8BytesOnLatin1Database(t *testing.T) {
	ctx := t.Context()
	baseURL := startTestDatabase(t)
	admin, err := NewClient(ctx, baseURL)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, admin.Close(context.Background())) })
	_, err = admin.Pool.Exec(ctx, `CREATE DATABASE latin_evidence TEMPLATE template0 ENCODING 'LATIN1' LC_COLLATE 'C' LC_CTYPE 'C'`)
	require.NoError(t, err)
	dsn, err := url.Parse(baseURL)
	require.NoError(t, err)
	dsn.Path = "/latin_evidence"
	params := dsn.Query()
	params.Set("client_encoding", "UTF8")
	dsn.RawQuery = params.Encode()
	c, err := NewClient(ctx, dsn.String())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, c.Close(context.Background())) })
	require.NoError(t, c.RunMigrations(ctx))
	var serverEncoding, clientEncoding string
	require.NoError(t, c.Pool.QueryRow(ctx, `SELECT current_setting('server_encoding'), current_setting('client_encoding')`).Scan(&serverEncoding, &clientEncoding))
	require.Equal(t, "LATIN1", serverEncoding)
	require.Equal(t, "UTF8", clientEncoding)
	docID := domain.DocumentID(uuid.NewString())
	require.NoError(t, c.InsertDocument(ctx, domain.Document{ID: docID}))
	require.NoError(t, c.UpdateDocumentStatus(ctx, docID, "completed"))
	visibility := Visibility{Pool: c.Pool}

	for i, word := range []string{"cafe", "café"} {
		t.Run(word, func(t *testing.T) {
			ch := domain.Chunk{ID: domain.ChunkID(uuid.NewString()), DocumentID: docID, Index: i, Text: word}
			require.NoError(t, c.InsertChunks(t.Context(), []domain.Chunk{ch}))
			var stored string
			require.NoError(t, c.Pool.QueryRow(t.Context(), `SELECT text FROM chunks WHERE id=$1`, string(ch.ID)).Scan(&stored))
			require.Equal(t, word, stored)
			visible, err := visibility.VisibleEvidence(t.Context(), []domain.ScoredChunk{{Chunk: ch}})
			require.NoError(t, err)
			require.Len(t, visible, 1)
			require.Equal(t, word, visible[0].Chunk.Text)
		})
	}
}

func TestEvidenceVisibilityMatchesCurrentChunkAndScope(t *testing.T) {
	c, _ := listingDatabase(t)
	ctx := t.Context()
	_, err := c.Pool.Exec(ctx, `UPDATE documents SET status='completed', doc_type='report',
		collection=CASE WHEN id=$1 THEN 'archive' ELSE '' END, metadata='{"title":"Shared title"}'`, listingFirstID)
	require.NoError(t, err)
	chunks := []domain.Chunk{
		{ID: domain.ChunkID(strings.ReplaceAll(uuid.NewString(), "-", "")), DocumentID: listingFirstID, Index: 4, Text: "Shared text.", Metadata: map[string]any{"doc_type": "report"}},
		{ID: domain.ChunkID(uuid.NewString()), DocumentID: listingSecondID, Index: 4, Text: "Shared text.", Metadata: map[string]any{"doc_type": "report"}},
	}
	require.NoError(t, c.InsertChunks(ctx, chunks))
	candidates := []domain.ScoredChunk{{Chunk: chunks[1], Score: .99}, {Chunk: chunks[0], Score: .9}}
	visibility := Visibility{Pool: c.Pool, Collection: "archive", DocType: "report"}
	got, err := visibility.VisibleEvidence(ctx, candidates)
	require.NoError(t, err)
	require.Equal(t, candidates[1:], got)

	// Aliases match the stored UUID and keep the vector store's spelling, which a
	// byte-for-byte metadata filter needs to find the same document again.
	for _, spelling := range []string{"urn:uuid:" + string(candidates[1].Chunk.DocumentID), strings.ToUpper(string(candidates[1].Chunk.DocumentID))} {
		alias := candidates[1]
		alias.Chunk.DocumentID = domain.DocumentID(spelling)
		got, err = visibility.VisibleEvidence(ctx, []domain.ScoredChunk{alias})
		require.NoError(t, err)
		require.Equal(t, []domain.ScoredChunk{alias}, got)
	}

	visibility.Collection = ""
	got, err = visibility.VisibleEvidence(ctx, candidates)
	require.NoError(t, err)
	require.Equal(t, candidates, got)
	visibility.Collection = "archive"

	_, err = c.Pool.Exec(ctx, `DELETE FROM documents WHERE id=$1`, listingFirstID)
	require.NoError(t, err)
	got, err = visibility.VisibleEvidence(ctx, candidates)
	require.NoError(t, err)
	require.Empty(t, got)

	_, err = c.Pool.Exec(ctx, `INSERT INTO documents(id,content,status,collection,doc_type)
		VALUES ($1,'Replacement','completed','archive','report')`, listingFirstID)
	require.NoError(t, err)
	replacement := chunks[0]
	replacement.Text = "Replacement."
	require.NoError(t, c.InsertChunks(ctx, []domain.Chunk{replacement}))
	got, err = visibility.VisibleEvidence(ctx, candidates)
	require.NoError(t, err)
	require.Empty(t, got)
	got, err = visibility.VisibleEvidence(ctx, []domain.ScoredChunk{{Chunk: replacement}})
	require.NoError(t, err)
	require.Len(t, got, 1)

	_, err = c.Pool.Exec(ctx, `ALTER TABLE chunks RENAME TO unavailable_chunks`)
	require.NoError(t, err)
	got, err = visibility.VisibleEvidence(ctx, candidates)
	require.ErrorContains(t, err, "visible evidence")
	require.Nil(t, got)
}
