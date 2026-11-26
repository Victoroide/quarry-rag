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
}
