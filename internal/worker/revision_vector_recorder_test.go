package worker

import (
	"context"
	"sync"

	"github.com/Victoroide/quarry-rag/internal/domain"
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
		if ch.RevisionID == revisionID {
			delete(r.activeChunks, id)
		}
	}
	r.mu.Unlock()
	return nil
}
