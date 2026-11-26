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

// ErrWritersFrozen indicates that document writes are paused for a vector migration.
var ErrWritersFrozen = errors.New("document writes are frozen for a vector migration")

// ErrGenerationMismatch indicates that this replica serves a vector generation
// other than the active one.
var ErrGenerationMismatch = errors.New("replica vector generation is not the active generation")

// ErrAuthzModeMismatch indicates that this replica's configured authorization
// mode does not match the mode recorded on the corpus.
var ErrAuthzModeMismatch = errors.New("authorization mode does not match the corpus")
