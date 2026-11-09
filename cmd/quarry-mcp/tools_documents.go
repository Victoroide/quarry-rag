package main

import (
	"context"

	"github.com/Victoroide/quarry-rag/client"
	"github.com/google/jsonschema-go/jsonschema"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type listInput struct {
	Limit int    `json:"limit,omitempty" jsonschema:"Documents per page, 1 to 1000."`
	After string `json:"after,omitempty" jsonschema:"Continue after this document ID, taken from next_after."`

	Collection string `json:"collection,omitempty" jsonschema:"Only list documents with this collection label; empty means all collections."`
}

type documentInput struct {
	DocumentID string `json:"document_id" jsonschema:"Document UUID."`
}

type listOutput struct {
	Documents []client.DocumentSummary `json:"documents"`
	NextAfter *string                  `json:"next_after"`
}

func registerDocumentTools(s *mcp.Server, api *client.Client, cfg config) error {
	schema, err := jsonschema.For[listInput](nil)
	if err != nil {
		return err
	}
	schema.Properties["limit"].Minimum = new(float64(1))
	schema.Properties["limit"].Maximum = new(float64(1000))
	schema.Properties["limit"].Default = []byte("100")
	addAPITool(s, &mcp.Tool{Name: "quarry_list_documents", Description: "List a page of document summaries and ingestion statuses", Annotations: readAnnotations(), InputSchema: schema}, cfg.token, func(ctx context.Context, input listInput) (listOutput, error) {
		page, err := api.ListDocuments(ctx, client.ListOptions{Limit: input.Limit, After: input.After, Collection: input.Collection})
		if err != nil {
			return listOutput{}, err
		}
		if page.Documents == nil {
			page.Documents = []client.DocumentSummary{}
		}
		return listOutput{Documents: page.Documents, NextAfter: page.NextAfter}, nil
	})
	addAPITool(s, &mcp.Tool{Name: "quarry_get_document", Description: "Get a document summary and ingestion status", Annotations: readAnnotations()}, cfg.token, func(ctx context.Context, input documentInput) (*client.DocumentSummary, error) {
		return api.GetDocument(ctx, input.DocumentID)
	})
	if !cfg.readOnly {
		addAPITool(s, &mcp.Tool{Name: "quarry_delete_document", Description: "Delete a document and its stored chunks, vectors and original file", Annotations: writeAnnotations(true)}, cfg.token, func(ctx context.Context, input documentInput) (*client.DeleteResponse, error) {
			return api.DeleteDocument(ctx, input.DocumentID)
		})
	}
	return nil
}
