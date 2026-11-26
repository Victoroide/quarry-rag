package client

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// ListOptions selects a document page. A zero limit uses 100 documents.
type ListOptions struct {
	Limit int
	After string

	Collection             string
	IncludeIndexVisibility bool
	// IncludePDFCoverage requests recorded mixed PDF extraction coverage.
	IncludePDFCoverage bool
}

// DocumentSummary describes a document and its collection without content or metadata.
type DocumentSummary struct {
	ID                        string     `json:"document_id"`
	DocType                   string     `json:"doc_type"`
	Collection                string     `json:"collection"`
	S3Key                     *string    `json:"s3_key,omitempty"`
	Status                    string     `json:"status"`
	ChunkCount                int        `json:"chunk_count"`
	FileName                  *string    `json:"file_name"`
	FileType                  *string    `json:"file_type"`
	CreatedAt                 time.Time  `json:"created_at"`
	UpdatedAt                 time.Time  `json:"updated_at"`
	CompletedAt               *time.Time `json:"completed_at"`
	LastError                 *string    `json:"last_error,omitempty"`
	IndexVisibility           string     `json:"index_visibility,omitempty"`
	IndexVisibilityObservedAt *time.Time `json:"index_visibility_observed_at,omitempty"`
	PDFCoverage               string     `json:"pdf_coverage,omitempty"`
}

// DocumentPage contains ordered document summaries and an optional continuation cursor.
type DocumentPage struct {
	Documents []DocumentSummary `json:"documents"`
	Count     int               `json:"count"`
	NextAfter *string           `json:"next_after"`
}

// DocumentIncarnationSummary includes a document's stable incarnation.
type DocumentIncarnationSummary struct {
	DocumentSummary
	Incarnation string `json:"incarnation,omitempty"`
}

// DocumentIncarnationPage contains ordered summaries with their incarnations.
type DocumentIncarnationPage struct {
	Documents []DocumentIncarnationSummary `json:"documents"`
	Count     int                          `json:"count"`
	NextAfter *string                      `json:"next_after"`
}

// DeleteResponse reports deletion of a document and its related records.
type DeleteResponse struct {
	Deleted bool   `json:"deleted"`
	DocID   string `json:"doc_id"`
}

// FileURL is a temporary download address for an original file.
type FileURL struct {
	URL string `json:"url"`
	// ExpiresIn is the requested lifetime in seconds. The address can stop working sooner.
	ExpiresIn int `json:"expires_in"`
}

// ListDocuments retrieves one page of document summaries.
func (c *Client) ListDocuments(ctx context.Context, opts ListOptions) (*DocumentPage, error) {
	var response DocumentPage
	if err := c.listDocuments(ctx, opts, false, &response); err != nil {
		return nil, err
	}
	return &response, nil
}

// ListDocumentsWithIncarnation retrieves one page with each document's incarnation.
func (c *Client) ListDocumentsWithIncarnation(ctx context.Context, opts ListOptions) (*DocumentIncarnationPage, error) {
	var response DocumentIncarnationPage
	if err := c.listDocuments(ctx, opts, true, &response); err != nil {
		return nil, err
	}
	return &response, nil
}

func (c *Client) listDocuments(ctx context.Context, opts ListOptions, includeIncarnation bool, response any) error {
	if opts.Limit == 0 {
		opts.Limit = 100
	}
	query := url.Values{"limit": {strconv.Itoa(opts.Limit)}}
	if opts.After != "" {
		query.Set("after", opts.After)
	}
	if opts.Collection != "" {
		query.Set("collection", opts.Collection)
	}
	if includeIncarnation {
		query.Set("include_incarnation", "true")
	}
	if opts.IncludeIndexVisibility {
		query.Set("include_index_visibility", "true")
	}
	if opts.IncludePDFCoverage {
		query.Set("include_pdf_coverage", "true")
	}
	return c.jsonRequest(ctx, http.MethodGet, "/documents?"+query.Encode(), nil, response)
}

// GetDocument retrieves a document's ingestion status and summary.
func (c *Client) GetDocument(ctx context.Context, id string) (*DocumentSummary, error) {
	return c.getDocument(ctx, id)
}

// GetDocumentWithPDFCoverage retrieves the status and any recorded PDF coverage.
func (c *Client) GetDocumentWithPDFCoverage(ctx context.Context, id string) (*DocumentSummary, error) {
	return c.getDocument(ctx, id, "include_pdf_coverage")
}

// GetDocumentWithIndexVisibility retrieves a summary with its index visibility observation.
func (c *Client) GetDocumentWithIndexVisibility(ctx context.Context, id string) (*DocumentSummary, error) {
	return c.getDocument(ctx, id, "include_index_visibility")
}

// GetDocumentWithIndexVisibilityAndPDFCoverage requests both optional observations.
func (c *Client) GetDocumentWithIndexVisibilityAndPDFCoverage(ctx context.Context, id string) (*DocumentSummary, error) {
	return c.getDocument(ctx, id, "include_index_visibility", "include_pdf_coverage")
}

func (c *Client) getDocument(ctx context.Context, id string, projections ...string) (*DocumentSummary, error) {
	var response DocumentSummary
	if err := c.readDocument(ctx, id, &response, projections...); err != nil {
		return nil, err
	}
	return &response, nil
}

// GetDocumentWithIncarnation retrieves the status summary and its incarnation.
func (c *Client) GetDocumentWithIncarnation(ctx context.Context, id string) (*DocumentIncarnationSummary, error) {
	return c.getDocumentWithIncarnation(ctx, id, "include_incarnation")
}

// GetDocumentWithIncarnationAndPDFCoverage retrieves the status with both optional fields.
func (c *Client) GetDocumentWithIncarnationAndPDFCoverage(ctx context.Context, id string) (*DocumentIncarnationSummary, error) {
	return c.getDocumentWithIncarnation(ctx, id, "include_incarnation", "include_pdf_coverage")
}

func (c *Client) getDocumentWithIncarnation(ctx context.Context, id string, projections ...string) (*DocumentIncarnationSummary, error) {
	var response DocumentIncarnationSummary
	if err := c.readDocument(ctx, id, &response, projections...); err != nil {
		return nil, err
	}
	return &response, nil
}

func (c *Client) readDocument(ctx context.Context, id string, response any, projections ...string) error {
	path := "/documents/" + url.PathEscape(id)
	query := url.Values{}
	for _, projection := range projections {
		query.Set(projection, "true")
	}
	if len(query) != 0 {
		path += "?" + query.Encode()
	}
	return c.jsonRequest(ctx, http.MethodGet, path, nil, response)
}

// DeleteDocument removes a document and its stored chunks, vectors and original file.
func (c *Client) DeleteDocument(ctx context.Context, id string) (*DeleteResponse, error) {
	var response DeleteResponse
	if err := c.jsonRequest(ctx, http.MethodDelete, "/documents/"+url.PathEscape(id), nil, &response); err != nil {
		return nil, err
	}
	return &response, nil
}

// ErrInvalidIncarnation reports an incarnation that is not one canonical UUID.
// A request that carried it could be mistaken for an ID-only deletion, so the
// client never sends one.
var ErrInvalidIncarnation = errors.New("incarnation must be one canonical UUID")

// DeleteDocumentIncarnation removes only the document lifetime the caller
// observed. Save the value once from GetDocumentWithIncarnation or
// ListDocumentsWithIncarnation and send the same value on every attempt, including
// a retry after a lost response. The client never looks up the current value,
// never repeats the request and never falls back to an ID-only deletion, so
// send it only to a service that enforces the qualifier.
func (c *Client) DeleteDocumentIncarnation(ctx context.Context, id, incarnation string) (*DeleteResponse, error) {
	canonical, err := canonicalIncarnation(incarnation)
	if err != nil {
		return nil, err
	}
	var response DeleteResponse
	setIncarnation := func(req *http.Request) { req.Header.Set(incarnationHeader, canonical) }
	if err := c.jsonRequest(ctx, http.MethodDelete, "/documents/"+url.PathEscape(id), nil, &response, setIncarnation); err != nil {
		return nil, err
	}
	return &response, nil
}

// incarnationHeader names the document lifetime a deletion addresses.
const incarnationHeader = "X-Document-Incarnation"

// canonicalIncarnation accepts exactly one hyphenated UUID and returns its lowercase form.
func canonicalIncarnation(value string) (string, error) {
	if len(value) != 36 {
		return "", ErrInvalidIncarnation
	}
	for i := 0; i < len(value); i++ {
		switch c := value[i]; {
		case i == 8 || i == 13 || i == 18 || i == 23:
			if c != '-' {
				return "", ErrInvalidIncarnation
			}
		case c >= '0' && c <= '9', c >= 'a' && c <= 'f', c >= 'A' && c <= 'F':
		default:
			return "", ErrInvalidIncarnation
		}
	}
	return strings.ToLower(value), nil
}

// DocumentFileURL retrieves a temporary download address without downloading the file.
func (c *Client) DocumentFileURL(ctx context.Context, id string) (*FileURL, error) {
	var response FileURL
	if err := c.jsonRequest(ctx, http.MethodGet, "/documents/"+url.PathEscape(id)+"/file", nil, &response); err != nil {
		return nil, err
	}
	return &response, nil
}
