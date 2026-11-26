package cleanup

import (
	"context"
	"net/http"
	"testing"

	"github.com/Victoroide/quarry-rag/client"
	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/stretchr/testify/require"
)

type offlineService struct {
	bare      []string
	qualified [][2]string
	err       error
}

func (s *offlineService) DeleteDocument(_ context.Context, id string) (*client.DeleteResponse, error) {
	s.bare = append(s.bare, id)
	return &client.DeleteResponse{Deleted: s.err == nil, DocID: id}, s.err
}

func (s *offlineService) DeleteDocumentIncarnation(_ context.Context, id, incarnation string) (*client.DeleteResponse, error) {
	s.qualified = append(s.qualified, [2]string{id, incarnation})
	return &client.DeleteResponse{Deleted: s.err == nil, DocID: id}, s.err
}

func TestOfflineCleanupSendsEachRequestOnceToTheOwningService(t *testing.T) {
	service := &offlineService{}
	cleaner, err := New(t.Context(), &config.Config{FakeMode: true}, WithOfflineService(service))
	require.NoError(t, err)

	require.NoError(t, cleaner.Delete(t.Context(), "00000000-0000-4000-8000-000000000001"))
	require.NoError(t, cleaner.DeleteIncarnation(t.Context(), "00000000-0000-4000-8000-000000000002", "00000000-0000-4000-8000-0000000000aa"))

	require.Equal(t, []string{"00000000-0000-4000-8000-000000000001"}, service.bare)
	require.Equal(t, [][2]string{{"00000000-0000-4000-8000-000000000002", "00000000-0000-4000-8000-0000000000aa"}}, service.qualified)
	require.NoError(t, cleaner.Close(t.Context()), "an offline cleaner holds no store connection")
}

func TestOfflineCleanupNeverDowngradesAnEmptyIncarnation(t *testing.T) {
	service := &offlineService{}
	cleaner, err := New(t.Context(), &config.Config{FakeMode: true}, WithOfflineService(service))
	require.NoError(t, err)

	err = cleaner.DeleteIncarnation(t.Context(), "00000000-0000-4000-8000-000000000001", "")

	require.ErrorIs(t, err, domain.ErrDeletionPreconditionRequired)
	require.Empty(t, service.bare, "an explicit empty qualifier is not an ID-only request")
	require.Empty(t, service.qualified)
}

func TestOfflineCleanupMapsAnAbsentDocument(t *testing.T) {
	service := &offlineService{err: &client.APIError{StatusCode: http.StatusNotFound}}
	cleaner, err := New(t.Context(), &config.Config{FakeMode: true}, WithOfflineService(service))
	require.NoError(t, err)
	require.ErrorIs(t, cleaner.Delete(t.Context(), "00000000-0000-4000-8000-000000000001"), ErrDocumentNotFound)
}

func TestOfflineCleanupSurfacesTheActivatedContract(t *testing.T) {
	service := &offlineService{err: &client.APIError{StatusCode: http.StatusPreconditionRequired}}
	cleaner, err := New(t.Context(), &config.Config{FakeMode: true}, WithOfflineService(service))
	require.NoError(t, err)
	var apiErr *client.APIError
	require.ErrorAs(t, cleaner.Delete(t.Context(), "00000000-0000-4000-8000-000000000001"), &apiErr)
	require.Equal(t, http.StatusPreconditionRequired, apiErr.StatusCode)
}

func TestOfflineCleanupDefaultsToTheLocalService(t *testing.T) {
	cleaner, err := New(t.Context(), &config.Config{FakeMode: true, ServerPort: "8081", ServiceTokens: []string{"", "second"}})
	require.NoError(t, err)
	require.NotNil(t, cleaner.offline)
}

func TestServiceTokenPrefersTheSingleToken(t *testing.T) {
	require.Equal(t, "single", serviceToken(&config.Config{ServiceToken: "single", ServiceTokens: []string{"other"}}))
	require.Equal(t, "other", serviceToken(&config.Config{ServiceTokens: []string{"", "other"}}))
	require.Empty(t, serviceToken(&config.Config{ServiceTokens: []string{""}}))
}
