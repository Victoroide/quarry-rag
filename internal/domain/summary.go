package domain

import (
	"context"
	"encoding/json"
	"time"
)

// DocumentSummary describes a document without its content or metadata.
type DocumentSummary struct {
	ID                        string          `json:"document_id"`
	DocType                   string          `json:"doc_type"`
	Collection                string          `json:"collection"`
	S3Key                     *string         `json:"s3_key,omitempty"`
	Status                    string          `json:"status"`
	ChunkCount                int             `json:"chunk_count"`
	FileName                  *string         `json:"file_name"`
	FileType                  *string         `json:"file_type"`
	CreatedAt                 time.Time       `json:"created_at"`
	UpdatedAt                 time.Time       `json:"updated_at"`
	CompletedAt               *time.Time      `json:"completed_at"`
	LastError                 *string         `json:"last_error,omitempty"`
	Incarnation               string          `json:"-"`
	IndexVisibility           IndexVisibility `json:"index_visibility,omitempty"`
	IndexVisibilityObservedAt *time.Time      `json:"index_visibility_observed_at,omitempty"`
	PDFCoverage               PDFCoverage     `json:"pdf_coverage,omitempty"`
}

type documentSummaryJSON DocumentSummary

func (d DocumentSummary) jsonFields() documentSummaryJSON {
	if d.Status != "failed" {
		d.LastError = nil
	}
	return documentSummaryJSON(d)
}

// MarshalJSON exposes the last error only for failed documents.
func (d DocumentSummary) MarshalJSON() ([]byte, error) {
	return json.Marshal(d.jsonFields())
}

// DocumentIncarnationSummary encodes an explicitly requested incarnation with the summary.
type DocumentIncarnationSummary struct {
	DocumentSummary
	Incarnation string `json:"incarnation"`
}

// WithIncarnation adds the selected incarnation to the summary's JSON projection.
func (d DocumentSummary) WithIncarnation() DocumentIncarnationSummary {
	return DocumentIncarnationSummary{DocumentSummary: d, Incarnation: d.Incarnation}
}

// MarshalJSON encodes the incarnation alongside the standard summary fields.
func (d DocumentIncarnationSummary) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		documentSummaryJSON
		Incarnation string `json:"incarnation"`
	}{d.jsonFields(), d.Incarnation})
}

// DocumentLister supports ordered document inventory and status lookups.
type DocumentLister interface {
	StreamDocuments(context.Context, func(DocumentSummary) error) error
	ListDocumentsPage(ctx context.Context, after string, limit int) ([]DocumentSummary, error)
	GetDocumentSummary(ctx context.Context, id DocumentID) (*DocumentSummary, error)
}

// DocumentIncarnationLister provides a summary projection that reads document incarnations.
type DocumentIncarnationLister interface {
	WithDocumentIncarnations() DocumentLister
}

// CollectionDocumentLister applies a collection predicate before ordering and pagination.
type CollectionDocumentLister interface {
	StreamDocumentsInCollection(ctx context.Context, collection string, visit func(DocumentSummary) error) error
	ListDocumentsPageInCollection(ctx context.Context, after string, limit int, collection string) ([]DocumentSummary, error)
}

// ScopedPolicyState captures the principal's activity and inventory revision
// together with the catalog revision used for audit provenance.
type ScopedPolicyState struct {
	Active          bool
	Revision        int64
	CatalogRevision int64
}

// ScopedDocumentPage contains one page of summaries visible to a verified principal.
type ScopedDocumentPage struct {
	Active          bool
	Revision        int64
	CatalogRevision int64
	Documents       []DocumentSummary
}

// ScopedDocumentSummary contains one summary visible to a verified principal.
type ScopedDocumentSummary struct {
	Active          bool
	Revision        int64
	CatalogRevision int64
	Document        *DocumentSummary
}

// ScopedDocumentLister enforces tenant and read grants inside inventory and status lookups.
type ScopedDocumentLister interface {
	ScopedPolicyState(ctx context.Context, principal Principal) (ScopedPolicyState, error)
	ListScopedDocumentsPage(ctx context.Context, principal Principal, after string, limit int, collection string, includeIncarnation bool) (ScopedDocumentPage, error)
	GetScopedDocumentSummary(ctx context.Context, principal Principal, id DocumentID, includeIncarnation bool) (ScopedDocumentSummary, error)
}
