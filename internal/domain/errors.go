package domain

import "errors"

// ErrDocumentExists indicates that a document identity is already stored.
var ErrDocumentExists = errors.New("document already exists")

// ErrUpstreamThrottled indicates temporary capacity exhaustion at a provider.
var ErrUpstreamThrottled = errors.New("upstream service is busy")
