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

// sourceIdentityMatches proves identity for a record fetched under vectorKey
// with the source store policy before staging or verification uses its payload.
func sourceIdentityMatches(sourceTagged bool, snap *pgstore.DocumentSnapshot, want, src domain.Chunk, found bool) bool {
	if !found || src.Index < 0 || src.Index != want.Index || src.DocumentID != want.DocumentID || src.ID != want.ID || src.Text != want.Text ||
		(src.VectorIncarnation != "" && src.VectorIncarnation != snap.Incarnation) || src.RevisionID != snap.ActiveRevisionID {
		return false
	}
	// Hosted records carry their own incarnation stamp. PostgreSQL source
	// rows are associated with the document by the relational snapshot.
	if sourceTagged && snap.VectorIncarnationRequired && src.VectorIncarnation == "" {
		return false
	}
	tenant, present := src.Metadata[domain.VectorTenantKey]
	tenantID, valid := tenant.(string)
	if present && (!valid || tenantID != "" && tenantID != snap.TenantID) {
		return false
	}
	if !sourceTagged || !isTagged(snap.DocumentRevision) {
		return true
	}
	return src.Metadata[domain.VectorRevisionKey] == snap.Incarnation &&
		src.Metadata[domain.VectorAttemptKey] == snap.VectorAttempt &&
		tenantID == snap.TenantID
}
