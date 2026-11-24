package pipeline

import "errors"

// ErrSynthesisEmpty indicates that answer synthesis returned no text.
var ErrSynthesisEmpty = errors.New("synthesis returned no text")

// ErrEvidenceScopeChanged indicates evidence became unavailable before release.
var ErrEvidenceScopeChanged = errors.New("evidence scope changed during query")

// ErrSynthesisInputTooLarge indicates the serialized synthesis input and output reserve exceed the model context window.
var ErrSynthesisInputTooLarge = errors.New("synthesis input exceeds model context budget")

// ErrSynthesisProfileUnsupported indicates the synthesis input profile or capability does not match the routed model or provider.
var ErrSynthesisProfileUnsupported = errors.New("synthesis input profile is unsupported")

// ErrSynthesisCountFailed indicates pre-synthesis input token counting failed or exceeded its attempt ceiling.
var ErrSynthesisCountFailed = errors.New("synthesis input counting failed")
