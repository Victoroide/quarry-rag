package domain

import "errors"

// ErrDocumentExists indicates that a document identity is already stored.
var ErrDocumentExists = errors.New("document already exists")

// ErrUpstreamThrottled indicates temporary capacity exhaustion at a provider.
var ErrUpstreamThrottled = errors.New("upstream service is busy")

// ErrEmbeddingModelLimit indicates that the provider reported an input window violation.
var ErrEmbeddingModelLimit = errors.New("embedding model rejected the input length")

// ErrEmbeddingDenied indicates that the provider refused permission to embed.
var ErrEmbeddingDenied = errors.New("embedding provider denied the request")
