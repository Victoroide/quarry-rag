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
	c := jobsDatabase(t)
	ctx := t.Context()
	archiveIncarnation := uuid.NewString()
	_, err := c.Pool.Exec(ctx, `INSERT INTO documents(id, content, status, doc_type, collection, incarnation, metadata) VALUES
		($1, 'private text', 'completed', 'report', 'archive', $3, '{"title":"Shared title"}'),
		($2, 'private text', 'completed', 'report', '', NULL, '{"title":"Shared title"}')`,
		listingFirstID, listingSecondID, archiveIncarnation)
	require.NoError(t, err)
	chunks := []domain.Chunk{
		{ID: domain.ChunkID(strings.ReplaceAll(uuid.NewString(), "-", "")), DocumentID: listingFirstID, Index: 4, Text: "Shared text.", Metadata: map[string]any{"doc_type": "report"}},
		{ID: domain.ChunkID(uuid.NewString()), DocumentID: listingSecondID, Index: 4, Text: "Shared text.", Metadata: map[string]any{"doc_type": "report"}},
	}
	require.NoError(t, c.InsertChunks(ctx, chunks))
	candidates := []domain.ScoredChunk{{Chunk: chunks[1], Score: .99}, {Chunk: chunks[0], Score: .9}}
	expected := []domain.ScoredChunk{candidates[0], candidates[1]}
	expected[0].ObservedIncarnation = listingSecondID
	expected[1].ObservedIncarnation = archiveIncarnation
	visibility := Visibility{Pool: c.Pool, Collection: "archive", DocType: "report"}
	got, err := visibility.VisibleEvidence(ctx, candidates)
	require.NoError(t, err)
	require.Equal(t, expected[1:], got)

	// Aliases match the stored UUID and keep the vector store's spelling, which a
	// byte-for-byte metadata filter needs to find the same document again.
	for _, spelling := range []string{"urn:uuid:" + string(candidates[1].Chunk.DocumentID), strings.ToUpper(string(candidates[1].Chunk.DocumentID))} {
		alias := candidates[1]
		alias.Chunk.DocumentID = domain.DocumentID(spelling)
		got, err = visibility.VisibleEvidence(ctx, []domain.ScoredChunk{alias})
		require.NoError(t, err)
		aliased := alias
		aliased.ObservedIncarnation = archiveIncarnation
		require.Equal(t, []domain.ScoredChunk{aliased}, got)
	}

	visibility.Collection = ""
	got, err = visibility.VisibleEvidence(ctx, candidates)
	require.NoError(t, err)
	require.Equal(t, expected, got)
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
	require.NotEmpty(t, got[0].ObservedIncarnation)
	require.NotEqual(t, archiveIncarnation, got[0].ObservedIncarnation)

	_, err = c.Pool.Exec(ctx, `ALTER TABLE chunks RENAME TO unavailable_chunks`)
	require.NoError(t, err)
	got, err = visibility.VisibleEvidence(ctx, candidates)
	require.ErrorContains(t, err, "visible evidence")
	require.Nil(t, got)
}
