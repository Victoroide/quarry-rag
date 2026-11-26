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

	Collection             string `json:"collection,omitempty" jsonschema:"Only list documents with this collection label; empty means all collections."`
	IncludeIndexVisibility bool   `json:"include_index_visibility,omitempty" jsonschema:"Include the index visibility observation."`
	IncludePDFCoverage     bool   `json:"include_pdf_coverage,omitempty" jsonschema:"Include recorded mixed PDF extraction coverage."`
}

type documentInput struct {
	DocumentID string `json:"document_id" jsonschema:"Document UUID."`
}

type deleteInput struct {
	documentInput
	Incarnation *string `json:"incarnation,omitempty" jsonschema:"Incarnation of the document lifetime to delete: one canonical UUID saved from an earlier status read. Required once the service has activated checkpointed deletion and refused when it has not. An empty or invalid value is rejected and never deletes by document ID alone."`
}

type getDocumentInput struct {
	documentInput
	IncludeIndexVisibility bool `json:"include_index_visibility,omitempty" jsonschema:"Include the index visibility observation."`
	IncludePDFCoverage     bool `json:"include_pdf_coverage,omitempty" jsonschema:"Include recorded mixed PDF extraction coverage."`
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
		page, err := api.ListDocuments(ctx, client.ListOptions{
			Limit: input.Limit, After: input.After, Collection: input.Collection, IncludePDFCoverage: input.IncludePDFCoverage, IncludeIndexVisibility: input.IncludeIndexVisibility,
		})
		if err != nil {
			return listOutput{}, err
		}
		if page.Documents == nil {
			page.Documents = []client.DocumentSummary{}
		}
		return listOutput{Documents: page.Documents, NextAfter: page.NextAfter}, nil
	})
	addAPITool(s, &mcp.Tool{Name: "quarry_get_document", Description: "Get a document summary and ingestion status", Annotations: readAnnotations()}, cfg.token, func(ctx context.Context, input getDocumentInput) (*client.DocumentSummary, error) {
		if input.IncludeIndexVisibility && input.IncludePDFCoverage {
			return api.GetDocumentWithIndexVisibilityAndPDFCoverage(ctx, input.DocumentID)
		}
		if input.IncludePDFCoverage {
			return api.GetDocumentWithPDFCoverage(ctx, input.DocumentID)
		}
		if input.IncludeIndexVisibility {
			return api.GetDocumentWithIndexVisibility(ctx, input.DocumentID)
		}
		return api.GetDocument(ctx, input.DocumentID)
	})
	if !cfg.readOnly {
		deleteSchema, schemaErr := jsonschema.For[deleteInput](nil)
		if schemaErr != nil {
			return schemaErr
		}
		// Only one canonical UUID names a lifetime. A null, empty or malformed
		// value fails validation, so it can never become an ID-only request.
		incarnation := deleteSchema.Properties["incarnation"]
		incarnation.Types, incarnation.Type = nil, "string"
		incarnation.MinLength, incarnation.MaxLength = new(36), new(36)
		incarnation.Pattern = "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
		addAPITool(s, &mcp.Tool{Name: "quarry_delete_document", Description: "Delete a document and its stored chunks, vectors and original file", Annotations: writeAnnotations(true), InputSchema: deleteSchema}, cfg.token, func(ctx context.Context, input deleteInput) (*client.DeleteResponse, error) {
			if input.Incarnation != nil {
				return api.DeleteDocumentIncarnation(ctx, input.DocumentID, *input.Incarnation)
			}
			return api.DeleteDocument(ctx, input.DocumentID)
		})
	}
	return nil
}
