package pipeline

import "errors"

// ErrSynthesisEmpty indicates that answer synthesis returned no text.
var ErrSynthesisEmpty = errors.New("synthesis returned no text")

// ErrEvidenceScopeChanged indicates evidence became unavailable before release.
var ErrEvidenceScopeChanged = errors.New("evidence scope changed during query")
