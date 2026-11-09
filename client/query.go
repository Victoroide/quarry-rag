package client

import (
	"context"
	"encoding/json"
	"net/http"
)

// QueryRequest selects the question, retrieval mode and authorized document scope.
// A nil allowlist is unrestricted; a non-nil empty allowlist denies all documents.
type QueryRequest struct {
	Query              string   `json:"query"`
	Mode               string   `json:"mode,omitempty"`
	FilterDocType      string   `json:"filter_doc_type,omitempty"`
	MaxChunks          int      `json:"max_chunks,omitempty"`
	IncludeCitations   bool     `json:"include_citations,omitempty"`
	AllowedDocumentIDs []string `json:"allowed_document_ids,omitzero"`

	Collection string `json:"collection,omitempty"`
}

// MarshalJSON preserves an explicitly empty authorization scope.
func (q QueryRequest) MarshalJSON() ([]byte, error) {
	type request QueryRequest
	var ids *[]string
	if q.AllowedDocumentIDs != nil {
		ids = &q.AllowedDocumentIDs
	}
	return json.Marshal(struct {
		request
		AllowedDocumentIDs *[]string `json:"allowed_document_ids,omitempty"`
	}{request(q), ids})
}

// ScoredChunkResponse contains retrieved evidence and its similarity score.
type ScoredChunkResponse struct {
	DocID       string   `json:"doc_id"`
	DocType     string   `json:"doc_type"`
	ChunkIndex  int      `json:"chunk_index"`
	Score       float64  `json:"score"`
	Text        string   `json:"text"`
	Kind        string   `json:"kind,omitempty"`
	Quality     string   `json:"quality,omitempty"`
	Excerpt     int      `json:"excerpt,omitempty"`
	Truncated   bool     `json:"truncated,omitempty"`
	RerankScore *float64 `json:"rerank_score,omitempty"`
}

// CitationResponse links an answer marker to an excerpt and its lexical check.
type CitationResponse struct {
	Excerpt    int     `json:"excerpt"`
	DocID      string  `json:"doc_id"`
	ChunkIndex int     `json:"chunk_index"`
	Offset     int     `json:"offset"`
	Verified   bool    `json:"verified"`
	Overlap    float64 `json:"overlap"`
}

// TokenUsage reports synthesis usage.
type TokenUsage struct {
	InputTokens  int32 `json:"input_tokens"`
	OutputTokens int32 `json:"output_tokens"`
	TotalTokens  int32 `json:"total_tokens"`
}

// QueryResponse contains the answer, retrieved evidence and synthesis diagnostics.
type QueryResponse struct {
	FinishReason  string                `json:"finish_reason,omitempty"`
	Answer        string                `json:"answer"`
	Mode          string                `json:"mode"`
	ChunksUsed    int                   `json:"chunks_used"`
	ContextChunks int                   `json:"context_chunks,omitempty"`
	ContextTokens int                   `json:"context_tokens,omitempty"`
	EarlyExit     bool                  `json:"early_exit,omitempty"`
	StageTimings  map[string]int64      `json:"stage_timings_ms,omitempty"`
	TokenUsage    TokenUsage            `json:"token_usage"`
	ScoredChunks  []ScoredChunkResponse `json:"scored_chunks"`
	Citations     []CitationResponse    `json:"citations,omitzero"`
}

// RequestOption configures an individual HTTP request.
type RequestOption func(*http.Request)

// WithAcceptLanguage sets the language preferences for a no-evidence answer.
func WithAcceptLanguage(tag string) RequestOption {
	return func(req *http.Request) {
		req.Header.Set("Accept-Language", tag)
	}
}

// Query answers a question using indexed evidence in the requested scope.
func (c *Client) Query(ctx context.Context, req QueryRequest, opts ...RequestOption) (*QueryResponse, error) {
	var response QueryResponse
	if err := c.jsonRequest(ctx, http.MethodPost, "/query", req, &response, opts...); err != nil {
		return nil, err
	}
	return &response, nil
}
