package client

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime/multipart"
	"net/http"
)

// IngestRequest is the JSON ingestion request, including its collection label.
type IngestRequest struct {
	DocumentID string         `json:"document_id,omitempty"`
	Content    string         `json:"content"`
	Metadata   map[string]any `json:"metadata,omitzero"`
	DocType    string         `json:"doc_type,omitempty"`
	Collection string         `json:"collection,omitempty"`
}

// IngestResponse reports the accepted identity and asynchronous processing status.
type IngestResponse struct {
	DocumentID string `json:"document_id"`
	Status     string `json:"status"`
}

// FileUpload supplies an original file and its ingestion attributes, including collection.
// Reader is streamed and remains owned by the caller, who must close it when needed.
type FileUpload struct {
	Reader     io.Reader
	Filename   string
	DocType    string
	Collection string
	DocumentID string
	Metadata   map[string]any
}

// Ingest submits text for asynchronous ingestion.
func (c *Client) Ingest(ctx context.Context, req IngestRequest) (*IngestResponse, error) {
	var response IngestResponse
	if err := c.jsonRequest(ctx, http.MethodPost, "/ingest", req, &response); err != nil {
		return nil, err
	}
	return &response, nil
}

// IngestFile streams a multipart upload without buffering the file.
func (c *Client) IngestFile(ctx context.Context, upload FileUpload) (*IngestResponse, error) {
	if upload.Reader == nil || upload.Filename == "" {
		return nil, errors.New("file reader and filename are required")
	}
	var metadata []byte
	if upload.Metadata != nil {
		var err error
		metadata, err = json.Marshal(upload.Metadata)
		if err != nil {
			return nil, err
		}
	}
	reader, writer := io.Pipe()
	defer func() { _ = reader.Close() }()
	form := multipart.NewWriter(writer)
	go func() {
		err := writeUpload(form, upload, metadata)
		if err == nil {
			err = form.Close()
		}
		_ = writer.CloseWithError(err)
	}()
	var response IngestResponse
	if err := c.request(ctx, http.MethodPost, "/ingest", reader, form.FormDataContentType(), &response); err != nil {
		return nil, err
	}
	return &response, nil
}

func writeUpload(form *multipart.Writer, upload FileUpload, metadata []byte) error {
	part, err := form.CreateFormFile("file", upload.Filename)
	if err != nil {
		return err
	}
	if _, err := io.Copy(part, upload.Reader); err != nil {
		return err
	}
	for _, field := range []struct{ name, value string }{{"doc_type", upload.DocType}, {"document_id", upload.DocumentID}, {"collection", upload.Collection}, {"metadata", string(metadata)}} {
		if field.value != "" {
			if err := form.WriteField(field.name, field.value); err != nil {
				return err
			}
		}
	}
	return nil
}
