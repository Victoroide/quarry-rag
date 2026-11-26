package main

import (
	"github.com/Victoroide/quarry-rag/internal/domain"
	pcstore "github.com/Victoroide/quarry-rag/internal/store/pinecone"
	pgstore "github.com/Victoroide/quarry-rag/internal/store/postgres"
)

// isTagged reports whether the document keeps its vectors under revision- and
// attempt-qualified tagged keys. A document with an active hosted revision keeps
// them under its manifest keys instead, whether or not it is also marked tagged.
func isTagged(rev pgstore.DocumentRevision) bool {
	return rev.ScopedVectors && rev.ActiveRevisionID == ""
}

// taggedRefusal explains why a document cannot be staged or verified on a store,
// or returns "". tagged is true for a hosted store, which holds tagged and
// manifested documents under their own keys.
func taggedRefusal(tagged bool, rev pgstore.DocumentRevision) string {
	switch {
	case rev.ActiveRevisionID != "":
		if !tagged {
			return "a PostgreSQL target does not store hosted vector revisions"
		}
	case !rev.ScopedVectors:
	case rev.VectorAttempt == "" || rev.MutationPending:
		return "its published vector attempt cannot be determined"
	}
	return ""
}

// vectorKey is the key under which a store holds a chunk of the document. A
// manifested document's chunks sit under manifest keys on every store that can
// hold them. A tagged document's sit under revision- and attempt-qualified keys
// on a tagged (hosted) store, and under the chunk key elsewhere.
func vectorKey(tagged bool, rev pgstore.DocumentRevision, c domain.Chunk) (string, error) {
	switch {
	case rev.ActiveRevisionID != "":
		return domain.FormatRevisionVectorID(domain.DocumentID(rev.ID), rev.ActiveRevisionID, c.Index), nil
	case tagged && rev.ScopedVectors:
		return pcstore.TaggedVectorID(rev.ID, rev.Incarnation, rev.VectorAttempt, string(c.ID))
	}
	return string(c.ID), nil
}
