package handler

import (
	"net/http"
	"strings"
	"testing"

	"github.com/google/uuid"
)

// FuzzOwnedIngestFields checks the order-independent field parser. Whatever the
// fields, a request is either refused before authorization or authorized once
// with no reserved metadata, and it never reads shared state.
func FuzzOwnedIngestFields(f *testing.F) {
	id := uuid.NewString()
	for _, seed := range []struct {
		fileFirst              bool
		documentID, collection string
		docType, metadata      string
	}{
		{false, "", "", "", ""},
		{true, id, "team", "note", `{"a":1}`},
		{false, strings.ToUpper(id), "", "", `{"Tenant_ID":"x"}`},
		{true, "not-a-uuid", "", "", ""},
		{false, "", "a\x00b", "", `{`},
		{true, "", "", "", `{"a":1,"a":2}`},
		{false, "", "", "", `{"nested":{"tenant_id":"x"}}`},
		{true, "", strings.Repeat("c", 129), strings.Repeat("d", 129), `[]`},
	} {
		f.Add(seed.fileFirst, seed.documentID, seed.collection, seed.docType, seed.metadata)
	}
	f.Fuzz(func(t *testing.T, fileFirst bool, documentID, collection, docType, metadata string) {
		if len(documentID)+len(collection)+len(docType)+len(metadata) > 1<<16 {
			t.Skip()
		}
		h := newOwnedHarness(t)
		fields := []uploadPart{{"document_id", documentID}, {"collection", collection}, {"doc_type", docType}, {"metadata", metadata}}
		rec := h.serve(ownedUpload(t, fileFirst, []byte(ownedBody), fields...))
		switch rec.Code {
		case http.StatusAccepted, http.StatusBadRequest, http.StatusNotFound:
		default:
			t.Fatalf("unexpected status %d: %s", rec.Code, rec.Body.String())
		}
		if h.store.sharedReads != 0 {
			t.Fatal("owned ingestion consulted shared state")
		}
		if rec.Code == http.StatusBadRequest && len(h.store.authorized) != 0 {
			t.Fatal("a malformed request reached authorization")
		}
		for _, req := range h.store.accepted {
			for key := range req.Document.Metadata {
				if _, reserved := reservedMetadataKeys[strings.ToLower(key)]; reserved {
					t.Fatalf("reserved metadata key %q was accepted", key)
				}
			}
			if req.Owner == nil || req.Owner.Principal != h.principal {
				t.Fatal("acceptance is not bound to the verified principal")
			}
		}
	})
}
