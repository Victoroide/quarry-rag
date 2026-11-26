package tracing

import (
	"testing"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/store/memory"
	"github.com/stretchr/testify/require"
)

func TestWrappedMemoryStoreKeepsItsOwnerAndDeletionSlots(t *testing.T) {
	inner := memory.NewFakeVectorStore()
	wrapped := WrapVectorStore(inner, "offline")
	index, ok := wrapped.(domain.InMemoryIndex)
	require.True(t, ok, "tracing must not hide which process owns the index")
	require.Equal(t, inner.MemoryOwner(), index.MemoryOwner())

	unlock, locked := index.LockDocument("doc")
	require.True(t, locked)
	_, again := inner.LockDocument("doc")
	require.False(t, again, "the wrapper and the store share one slot")
	unlock()

	_, revisions := wrapped.(domain.RevisionVectorDeleter)
	require.True(t, revisions)

	manifests, ok := wrapped.(domain.ManifestRevisionVectorDeleter)
	require.True(t, ok, "wrapped in-memory store must keep ManifestRevisionVectorDeleter")
	require.NoError(t, manifests.DeleteByManifestRevision(t.Context(), "doc", "rev", []string{"doc#rev#0"}))

	presence, ok := wrapped.(domain.VectorPresenceChecker)
	require.True(t, ok, "wrapped in-memory store must keep VectorPresenceChecker")
	present, err := presence.FetchPresentVectorIDs(t.Context(), "", []string{"doc#rev#0"})
	require.NoError(t, err)
	require.Empty(t, present)
}
