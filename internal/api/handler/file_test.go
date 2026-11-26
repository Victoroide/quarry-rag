package handler

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/stretchr/testify/require"
)

type fileSignerFunc func(context.Context, string) (string, error)

func (f fileSignerFunc) PresignGet(ctx context.Context, key string) (string, error) {
	return f(ctx, key)
}

// guardedFiles serves a source only while the document is outside deletion.
type guardedFiles struct {
	domain.ChunkStore
	key     *string
	err     error
	guarded int
	plain   int
}

func (s *guardedFiles) WithDocumentFile(ctx context.Context, _ domain.DocumentID, sign func(context.Context, string) error) (bool, error) {
	s.guarded++
	if s.err != nil || s.key == nil {
		return false, s.err
	}
	return true, sign(ctx, *s.key)
}

func (s *guardedFiles) GetDocumentS3Key(context.Context, domain.DocumentID) (*string, error) {
	s.plain++
	return s.key, nil
}

type unguardedFiles struct {
	domain.ChunkStore
	key *string
}

func (s unguardedFiles) GetDocumentS3Key(context.Context, domain.DocumentID) (*string, error) {
	return s.key, nil
}

func serveFile(t *testing.T, store domain.ChunkStore, signer FileSigner) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/documents/"+deletionTestDocument+"/file", http.NoBody)
	req.SetPathValue("doc_id", deletionTestDocument)
	rec := httptest.NewRecorder()
	FileHandler(store, signer)(rec, req)
	return rec
}

func TestFileHandlerSignsInsideTheDeletionGuard(t *testing.T) {
	key := "documents/original.pdf"
	var signed []string
	store := &guardedFiles{key: &key}
	rec := serveFile(t, store, fileSignerFunc(func(_ context.Context, k string) (string, error) {
		signed = append(signed, k)
		return "signed-address", nil
	}))
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Contains(t, rec.Body.String(), "signed-address")
	require.Equal(t, []string{key}, signed)
	require.Equal(t, 1, store.guarded)
	require.Zero(t, store.plain, "a guarded store must not be read outside its guard")
}

func TestFileHandlerMakesNoSignatureOnceDeletionBegan(t *testing.T) {
	store := &guardedFiles{}
	rec := serveFile(t, store, fileSignerFunc(func(context.Context, string) (string, error) {
		t.Error("signed the source of a document in deletion")
		return "", nil
	}))
	require.Equal(t, http.StatusNotFound, rec.Code, rec.Body.String())
}

func TestFileHandlerReportsLookupAndSigningFailures(t *testing.T) {
	key := "documents/original.pdf"
	lookup := serveFile(t, &guardedFiles{err: errors.New("database unavailable")}, fileSignerFunc(func(context.Context, string) (string, error) {
		return "", nil
	}))
	require.Equal(t, http.StatusInternalServerError, lookup.Code)
	require.Contains(t, lookup.Body.String(), "failed to look up document")
	signing := serveFile(t, &guardedFiles{key: &key}, fileSignerFunc(func(context.Context, string) (string, error) {
		return "", errors.New("credentials expired")
	}))
	require.Equal(t, http.StatusInternalServerError, signing.Code)
	require.Contains(t, signing.Body.String(), "failed to generate download URL")
	require.NotContains(t, signing.Body.String(), "credentials expired")
}

func TestFileHandlerKeepsTheLookupPathForPlainStores(t *testing.T) {
	key := "documents/original.pdf"
	signer := fileSignerFunc(func(_ context.Context, k string) (string, error) { return "address-for-" + k, nil })
	rec := serveFile(t, unguardedFiles{key: &key}, signer)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, rec.Body.String(), "address-for-"+key)
	require.Equal(t, http.StatusNotFound, serveFile(t, unguardedFiles{}, signer).Code)
	require.Equal(t, http.StatusNotFound, serveFile(t, unguardedFiles{key: &key}, nil).Code, "unconfigured file storage")
}
