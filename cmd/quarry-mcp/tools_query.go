package main

import (
	"context"
	"errors"

	"github.com/Victoroide/quarry-rag/client"
	"github.com/google/jsonschema-go/jsonschema"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type queryInput struct {
	Query              string            `json:"query" jsonschema:"Question to answer from the indexed documents."`
	Mode               string            `json:"mode,omitempty" jsonschema:"Retrieval mode: auto, local or global. Defaults to auto."`
	AnswerMode         client.AnswerMode `json:"answer_mode,omitempty" jsonschema:"Choose a generated answer or selected excerpts. If omitted, the service sets the mode."`
	FilterDocType      string            `json:"filter_doc_type,omitempty" jsonschema:"Only retrieve chunks from documents of this type."`
	MaxChunks          int               `json:"max_chunks,omitempty" jsonschema:"Maximum evidence chunks, 0 to 100. 0 uses the service default; larger values are capped by the service."`
	IncludeCitations   bool              `json:"include_citations,omitempty" jsonschema:"Request inline citations, lexical overlap and semantic support when enabled by the service."`
	AllowedDocumentIDs []string          `json:"allowed_document_ids,omitempty" jsonschema:"Document UUIDs the caller may read. Omit for unrestricted retrieval; an empty list matches no documents."`

	Collection string `json:"collection,omitempty" jsonschema:"Only retrieve documents with this collection label; empty means all collections."`
}

type queryOutput struct {
	Answer           string            `json:"answer"`
	FinishReason     string            `json:"finish_reason"`
	Mode             string            `json:"mode"`
	ChunksUsed       int               `json:"chunks_used"`
	ContextChunks    int               `json:"context_chunks"`
	CountMethod      string            `json:"count_method,omitempty"`
	ModelInputTokens int               `json:"model_input_tokens,omitempty"`
	TokenUsage       client.TokenUsage `json:"token_usage"`
	Chunks           []queryChunk      `json:"chunks"`
	// Citations is present when the request asked for citations.
	Citations        []client.CitationResponse `json:"citations,omitzero"`
	CitationCoverage *client.CitationCoverage  `json:"citation_coverage,omitempty"`
}

type queryChunk struct {
	DocID      string  `json:"doc_id"`
	DocType    string  `json:"doc_type"`
	ChunkIndex int     `json:"chunk_index"`
	Score      float64 `json:"score"`
	Text       string  `json:"text"`
	Excerpt    int     `json:"excerpt,omitempty"`
	Truncated  bool    `json:"truncated,omitempty"`
}

func registerQueryTool(s *mcp.Server, api *client.Client, token string) error {
	schema, err := jsonschema.For[queryInput](nil)
	if err != nil {
		return err
	}
	schema.Properties["answer_mode"].Enum = []any{string(client.AnswerModeGenerated), string(client.AnswerModeEvidence)}
	mcp.AddTool(s, &mcp.Tool{Name: "quarry_query", Description: "Answer a question or retrieve evidence from indexed documents", Annotations: readAnnotations(), InputSchema: schema}, func(ctx context.Context, _ *mcp.CallToolRequest, input queryInput) (*mcp.CallToolResult, queryOutput, error) {
		response, err := api.Query(ctx, client.QueryRequest(input))
		if err != nil {
			return nil, queryOutput{}, errors.New(toolErrorMessage(err, token))
		}
		chunks := make([]queryChunk, 0, len(response.ScoredChunks))
		for _, c := range response.ScoredChunks {
			chunks = append(chunks, queryChunk{DocID: c.DocID, DocType: c.DocType, ChunkIndex: c.ChunkIndex, Score: c.Score, Text: c.Text, Excerpt: c.Excerpt, Truncated: c.Truncated})
		}
		var result *mcp.CallToolResult
		if response.FinishReason == client.FinishReasonEvidenceOnly {
			result = &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: "Retrieved excerpts are in the structured result."}}}
		}
		return result, queryOutput{Answer: response.Answer, FinishReason: response.FinishReason, Mode: response.Mode, ChunksUsed: response.ChunksUsed, ContextChunks: response.ContextChunks, CountMethod: response.CountMethod, ModelInputTokens: response.ModelInputTokens, TokenUsage: response.TokenUsage, Chunks: chunks, Citations: response.Citations, CitationCoverage: response.CitationCoverage}, nil
	})
	return nil
}
