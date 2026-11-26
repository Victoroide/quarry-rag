package postgres_test

import (
	"bytes"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/api"
	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/store/memory"
	"github.com/Victoroide/quarry-rag/internal/store/postgres"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestUpgradedServingRoleServesIngestAndGuardedDelete(t *testing.T) {
	owner, serving := postgres.UpgradedServingRoleDatabase(t)
	vectors := memory.NewFakeVectorStore()
	const token = "upgraded-serving-role"
	router := api.NewRouter(&api.RouterDeps{
		ChunkStore:                 serving,
		VectorStore:                vectors,
		PineconeDriver:             vectors,
		LegacyDeletionReservations: true,
		Config: &config.Config{
			AuthzMode:      config.AuthzModeService,
			ServiceToken:   token,
			FakeMode:       true,
			MaxUploadBytes: 1 << 20,
		},
	})

	// A caller-supplied v5 UUID also exercises the evaluation-reservation
	// predicates that ordinary admission and removal use for these IDs.
	id := uuid.NewSHA1(uuid.New(), []byte("ordinary document")).String()
	ingest := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/ingest",
		strings.NewReader(`{"content":"ordinary JSON document","document_id":"`+id+`"}`))
	ingest.Header.Set("Content-Type", "application/json")
	ingest.Header.Set("X-Service-Token", token)
	response := httptest.NewRecorder()
	router.ServeHTTP(response, ingest)
	require.Equal(t, http.StatusAccepted, response.Code, response.Body.String())

	var body bytes.Buffer
	form := multipart.NewWriter(&body)
	part, err := form.CreateFormFile("file", "document.txt")
	require.NoError(t, err)
	_, err = part.Write([]byte("ordinary multipart document"))
	require.NoError(t, err)
	require.NoError(t, form.Close())
	upload := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/ingest", &body)
	upload.Header.Set("Content-Type", form.FormDataContentType())
	upload.Header.Set("X-Service-Token", token)
	response = httptest.NewRecorder()
	router.ServeHTTP(response, upload)
	require.Equal(t, http.StatusAccepted, response.Code, response.Body.String())

	remove := httptest.NewRequestWithContext(t.Context(), http.MethodDelete, "/documents/"+id, nil)
	remove.Header.Set("X-Service-Token", token)
	response = httptest.NewRecorder()
	router.ServeHTTP(response, remove)
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())

	var live, finalized, held bool
	require.NoError(t, owner.Pool.QueryRow(t.Context(), `
		SELECT EXISTS(SELECT 1 FROM documents WHERE id=$1), finalized, held
		FROM document_deletions WHERE document_id=$1 AND kind='legacy_reservation'
		  AND vector_mode='memory' AND memory_owner=$2
	`, id, vectors.MemoryOwner()).Scan(&live, &finalized, &held))
	require.False(t, live)
	require.True(t, finalized)
	require.False(t, held)
}
