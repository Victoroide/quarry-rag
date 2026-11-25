package domain

import (
	"context"
	"time"
)

// Bounds on one original-file upload attempt. The staging and PUT deadline
// starts before the lease, leaving a margin for acknowledgement and acceptance.
const (
	SourceUploadPutTimeout = 5 * time.Minute
	SourceUploadLease      = 15 * time.Minute
)

// SourceUpload identifies one attempt-specific write of an original file.
type SourceUpload struct {
	ID         string
	DocumentID DocumentID
	Collection string
	Bucket     string
	Key        string
}

// SourceUploadLink names a staged upload that acceptance must adopt.
// VersionID is the object version returned by the write, when the store reports one.
type SourceUploadLink struct {
	ID        string
	VersionID string
}

// SourceUploadResult accounts for an S3 PUT and all of its retries.
type SourceUploadResult struct {
	VersionID    string
	Acknowledged bool // S3 confirmed the final attempt.
	Settled      bool // No attempt can still land.
}

// SourceUploadJournal records the owner of an original file before it is written.
type SourceUploadJournal interface {
	// StageSourceUpload durably records the intent to write the object.
	StageSourceUpload(context.Context, SourceUpload) error
	// BeginSourceUpload marks the PUT as possibly delivered before dispatch.
	BeginSourceUpload(context.Context, string) error
	// RecordSourceUploadResult records the provider outcome and any uncertainty.
	RecordSourceUploadResult(context.Context, string, SourceUploadResult) error
}
