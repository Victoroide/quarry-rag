package handler

import "github.com/Victoroide/quarry-rag/internal/retriever"

type queryOptions struct {
	reranker retriever.Reranker
}

// QueryOption configures optional query dependencies.
type QueryOption func(*queryOptions)

// WithReranker selects the reranker used before synthesis context selection.
func WithReranker(r retriever.Reranker) QueryOption {
	return func(opts *queryOptions) { opts.reranker = r }
}
