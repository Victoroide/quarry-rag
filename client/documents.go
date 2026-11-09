package client

import (
	"context"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

// ListOptions selects a document page. A zero limit uses 100 documents.
type ListOptions struct {
	Limit int
	After string

	Collection string
}

// DocumentSummary describes a document and its collection without content or metadata.
type DocumentSummary struct {
	ID          string     `json:"document_id"`
	DocType     string     `json:"doc_type"`
	Collection  string     `json:"collection"`
	S3Key       *string    `json:"s3_key,omitempty"`
	Status      string     `json:"status"`
	ChunkCount  int        `json:"chunk_count"`
	FileName    *string    `json:"file_name"`
	FileType    *string    `json:"file_type"`
	CreatedAt   time.Time  `json:"created_at"`
	UpdatedAt   time.Time  `json:"updated_at"`
	CompletedAt *time.Time `json:"completed_at"`
	LastError   *string    `json:"last_error,omitempty"`
}

// DocumentPage contains ordered document summaries and an optional continuation cursor.
type DocumentPage struct {
	Documents []DocumentSummary `json:"documents"`
	Count     int               `json:"count"`
	NextAfter *string           `json:"next_after"`
}

// DeleteResponse reports deletion of a document and its related records.
type DeleteResponse struct {
	Deleted bool   `json:"deleted"`
	DocID   string `json:"doc_id"`
}

// FileURL is a temporary download address for an original file.
type FileURL struct {
	URL       string `json:"url"`
	ExpiresIn int    `json:"expires_in"`
}

// ListDocuments retrieves one page of document summaries.
func (c *Client) ListDocuments(ctx context.Context, opts ListOptions) (*DocumentPage, error) {
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
	var response DocumentPage
	if err := c.jsonRequest(ctx, http.MethodGet, "/documents?"+query.Encode(), nil, &response); err != nil {
		return nil, err
	}
	return &response, nil
}

// GetDocument retrieves a document's ingestion status and summary.
func (c *Client) GetDocument(ctx context.Context, id string) (*DocumentSummary, error) {
	var response DocumentSummary
	if err := c.jsonRequest(ctx, http.MethodGet, "/documents/"+url.PathEscape(id), nil, &response); err != nil {
		return nil, err
	}
	return &response, nil
}

// DeleteDocument removes a document and its stored chunks, vectors and original file.
func (c *Client) DeleteDocument(ctx context.Context, id string) (*DeleteResponse, error) {
	var response DeleteResponse
	if err := c.jsonRequest(ctx, http.MethodDelete, "/documents/"+url.PathEscape(id), nil, &response); err != nil {
		return nil, err
	}
	return &response, nil
}

// DocumentFileURL retrieves a temporary download address without downloading the file.
func (c *Client) DocumentFileURL(ctx context.Context, id string) (*FileURL, error) {
	var response FileURL
	if err := c.jsonRequest(ctx, http.MethodGet, "/documents/"+url.PathEscape(id)+"/file", nil, &response); err != nil {
		return nil, err
	}
	return &response, nil
}
