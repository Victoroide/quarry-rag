package main

import (
	"context"

	"github.com/Victoroide/quarry-rag/client"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type queryInput struct {
	Query              string   `json:"query" jsonschema:"Question to answer from the indexed documents."`
	Mode               string   `json:"mode,omitempty" jsonschema:"Retrieval mode: auto, local or global. Defaults to auto."`
	FilterDocType      string   `json:"filter_doc_type,omitempty" jsonschema:"Only retrieve chunks from documents of this type."`
	MaxChunks          int      `json:"max_chunks,omitempty" jsonschema:"Maximum evidence chunks, 0 to 100. 0 uses the service default; larger values are capped by the service."`
	AllowedDocumentIDs []string `json:"allowed_document_ids,omitempty" jsonschema:"Document UUIDs the caller may read. Omit for unrestricted retrieval; an empty list matches no documents."`
}

type queryOutput struct {
	Answer        string            `json:"answer"`
	FinishReason  string            `json:"finish_reason"`
	Mode          string            `json:"mode"`
	ChunksUsed    int               `json:"chunks_used"`
	ContextChunks int               `json:"context_chunks"`
	TokenUsage    client.TokenUsage `json:"token_usage"`
	Chunks        []queryChunk      `json:"chunks"`
}

type queryChunk struct {
	DocID      string  `json:"doc_id"`
	DocType    string  `json:"doc_type"`
	ChunkIndex int     `json:"chunk_index"`
	Score      float64 `json:"score"`
	Text       string  `json:"text"`
}

func registerQueryTool(s *mcp.Server, api *client.Client, token string) {
	addAPITool(s, &mcp.Tool{Name: "quarry_query", Description: "Answer a question from the indexed documents", Annotations: readAnnotations()}, token, func(ctx context.Context, input queryInput) (queryOutput, error) {
		response, err := api.Query(ctx, client.QueryRequest(input))
		if err != nil {
			return queryOutput{}, err
		}
		chunks := make([]queryChunk, 0, len(response.ScoredChunks))
		for _, c := range response.ScoredChunks {
			chunks = append(chunks, queryChunk{DocID: c.DocID, DocType: c.DocType, ChunkIndex: c.ChunkIndex, Score: c.Score, Text: c.Text})
		}
		return queryOutput{Answer: response.Answer, FinishReason: response.FinishReason, Mode: response.Mode, ChunksUsed: response.ChunksUsed, ContextChunks: response.ContextChunks, TokenUsage: response.TokenUsage, Chunks: chunks}, nil
	})
}
