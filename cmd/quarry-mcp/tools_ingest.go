package main

import (
	"context"
	"path/filepath"

	"github.com/Victoroide/quarry-rag/client"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type textInput struct {
	DocumentID string         `json:"document_id,omitempty" jsonschema:"Optional document UUID; omit to generate one."`
	Content    string         `json:"content" jsonschema:"Text to ingest."`
	Metadata   map[string]any `json:"metadata,omitempty" jsonschema:"Optional JSON object stored with the document."`
	DocType    string         `json:"doc_type,omitempty" jsonschema:"Optional document type label."`
	Collection string         `json:"collection,omitempty" jsonschema:"Optional collection label that scopes content deduplication. Omit or leave empty for the default collection."`
}

type fileInput struct {
	Path       string         `json:"path" jsonschema:"Absolute path of a .pdf, .docx, .doc, .txt, .md or .markdown file inside a configured root."`
	DocType    string         `json:"doc_type,omitempty" jsonschema:"Optional document type label."`
	DocumentID string         `json:"document_id,omitempty" jsonschema:"Optional document UUID; omit to generate one."`
	Metadata   map[string]any `json:"metadata,omitempty" jsonschema:"Optional JSON object stored with the document."`
	Collection string         `json:"collection,omitempty" jsonschema:"Optional collection label that scopes content deduplication. Omit or leave empty for the default collection."`
}

func registerIngestTools(s *mcp.Server, api *client.Client, cfg config) {
	addAPITool(s, &mcp.Tool{Name: "quarry_ingest_text", Description: "Submit text for asynchronous ingestion", Annotations: writeAnnotations(false)}, cfg.token, func(ctx context.Context, input textInput) (*client.IngestResponse, error) {
		return api.Ingest(ctx, client.IngestRequest(input))
	})
	if len(cfg.fileRoots) == 0 {
		return
	}
	addAPITool(s, &mcp.Tool{Name: "quarry_ingest_file", Description: "Submit a document file from an allowed directory for asynchronous ingestion", Annotations: writeAnnotations(false)}, cfg.token, func(ctx context.Context, input fileInput) (*client.IngestResponse, error) {
		file, err := openUpload(input.Path, cfg.fileRoots)
		if err != nil {
			return nil, err
		}
		defer func() { _ = file.Close() }()
		return api.IngestFile(ctx, client.FileUpload{Reader: file, Filename: filepath.Base(input.Path), DocType: input.DocType, DocumentID: input.DocumentID, Metadata: input.Metadata, Collection: input.Collection})
	})
}
