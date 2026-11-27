package worker

import (
	"context"
	"sync"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

type revisionVectorDelete struct {
	documentID string
	revisionID string
}

type revisionVectorRecorder struct {
	mu           sync.Mutex
	docDeletes   int
	revDeletes   []revisionVectorDelete
	revKnownIDs  [][]string
	activeChunks map[string]domain.Chunk
}

func (r *revisionVectorRecorder) DeleteAllVectorsInNamespace(context.Context, string) error {
	return nil
}

func (r *revisionVectorRecorder) DeleteByDocID(context.Context, string) error {
	r.mu.Lock()
	r.docDeletes++
	r.mu.Unlock()
	return nil
}

func (r *revisionVectorRecorder) DeleteByManifestRevision(_ context.Context, documentID, revisionID string, knownIDs []string) error {
	r.mu.Lock()
	r.revDeletes = append(r.revDeletes, revisionVectorDelete{documentID: documentID, revisionID: revisionID})
	r.revKnownIDs = append(r.revKnownIDs, append([]string(nil), knownIDs...))
	for id, ch := range r.activeChunks {
		if string(ch.DocumentID) == documentID && ch.RevisionID == revisionID {
			delete(r.activeChunks, id)
		}
	}
	r.mu.Unlock()
	return nil
}

func TestRevisionVectorRecorderScopesDeletes(t *testing.T) {
	documentID := domain.DocumentID(uuid.NewString())
	otherDocumentID := domain.DocumentID(uuid.NewString())
	revisionID := uuid.NewString()
	otherRevisionID := uuid.NewString()
	target := domain.FormatRevisionVectorID(documentID, revisionID, 0)
	otherDocument := domain.FormatRevisionVectorID(otherDocumentID, revisionID, 0)
	otherRevision := domain.FormatRevisionVectorID(documentID, otherRevisionID, 0)
	recorder := &revisionVectorRecorder{activeChunks: map[string]domain.Chunk{
		target:        {DocumentID: documentID, RevisionID: revisionID},
		otherDocument: {DocumentID: otherDocumentID, RevisionID: revisionID},
		otherRevision: {DocumentID: documentID, RevisionID: otherRevisionID},
	}}
	knownIDs := []string{target}

	require.NoError(t, recorder.DeleteByManifestRevision(t.Context(), string(documentID), revisionID, knownIDs))
	knownIDs[0] = otherDocument

	require.NotContains(t, recorder.activeChunks, target)
	require.Contains(t, recorder.activeChunks, otherDocument)
	require.Contains(t, recorder.activeChunks, otherRevision)
	require.Equal(t, []revisionVectorDelete{{documentID: string(documentID), revisionID: revisionID}}, recorder.revDeletes)
	require.Equal(t, [][]string{{target}}, recorder.revKnownIDs)
}
