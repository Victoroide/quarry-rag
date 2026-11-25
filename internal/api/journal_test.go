package api

import (
	"context"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/stretchr/testify/require"
)

type journalingStore struct {
	domain.ChunkStore
	staged []domain.SourceUpload
}

func (s *journalingStore) StageSourceUpload(_ context.Context, upload domain.SourceUpload) error {
	s.staged = append(s.staged, upload)
	return nil
}
func (*journalingStore) BeginSourceUpload(context.Context, string) error { return nil }
func (*journalingStore) RecordSourceUploadResult(context.Context, string, domain.SourceUploadResult) error {
	return nil
}

func TestSourceUploadJournalUsesTheDocumentStore(t *testing.T) {
	store := &journalingStore{}
	journal := sourceUploadJournal(store)
	require.NoError(t, journal.StageSourceUpload(t.Context(), domain.SourceUpload{ID: "upload"}))
	require.Len(t, store.staged, 1)
}

func TestSourceUploadJournalRefusesUploadsWithoutStoreSupport(t *testing.T) {
	journal := sourceUploadJournal(nil)
	require.Error(t, journal.StageSourceUpload(t.Context(), domain.SourceUpload{ID: "upload"}))
	require.Error(t, journal.BeginSourceUpload(t.Context(), "upload"))
	require.Error(t, journal.RecordSourceUploadResult(t.Context(), "upload", domain.SourceUploadResult{}))
}
