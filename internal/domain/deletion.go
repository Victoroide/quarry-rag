package domain

import (
	"context"
	"errors"
)

var (
	// ErrDocumentNotFound indicates that the requested document is absent.
	ErrDocumentNotFound = errors.New("document not found")
	// ErrFileStorageUnavailable indicates a stored source cannot be deleted without object storage.
	ErrFileStorageUnavailable = errors.New("file storage is not configured")
	// ErrVectorStorageUnavailable indicates the vectors of a document cannot be deleted without their store.
	ErrVectorStorageUnavailable = errors.New("vector storage is not configured")
	// ErrDeletionBusy indicates that another operation still owns the cleanup target.
	ErrDeletionBusy = errors.New("document cleanup is busy")
	// ErrDeletionPreconditionRequired protects a live document from an unqualified deletion.
	ErrDeletionPreconditionRequired = errors.New("document incarnation is required")
	// ErrDeletionPreconditionFailed indicates that the supplied incarnation is not the stored one.
	ErrDeletionPreconditionFailed = errors.New("document incarnation does not match")
	// ErrDeletionInactive indicates that the database has not selected checkpointed deletion.
	ErrDeletionInactive = errors.New("checkpointed deletion is not active")
	// ErrDeletionTargetMismatch indicates that the configured stores differ from the recorded cleanup target.
	ErrDeletionTargetMismatch = errors.New("configured stores differ from the recorded cleanup target")
)

// DeleteOperations supplies the configured external operations for one deletion.
// A nil operation means the corresponding store is not configured.
type DeleteOperations struct {
	// ExpectedIncarnation names the document lifetime the caller observed.
	ExpectedIncarnation string
	// InMemory marks vectors that live in the serving process.
	InMemory bool
	// SourceBucket and VectorIndex identify the nonsecret stores the operations act on.
	// They are recorded with the cleanup target and compared on every resumption.
	SourceBucket string
	VectorIndex  string
	// LegacyNamespace and LegacyVectors remove vectors written without an incarnation.
	LegacyNamespace func(ctx context.Context, documentID string) error
	LegacyVectors   func(ctx context.Context, documentID string) error
	// RevisionVectors removes the vectors of one incarnation.
	RevisionVectors func(ctx context.Context, documentID, incarnation string) error
	// Source removes the exact original object.
	Source func(ctx context.Context, key string) error
}

// DocumentDeleter durably checkpoints the cleanup of one document incarnation.
type DocumentDeleter interface {
	// DeletionActive reports whether the database selected checkpointed deletion.
	DeletionActive(ctx context.Context) (bool, error)
	// DeleteWithCleanup records the cleanup target, runs each external step under
	// a bounded claim and removes the document with its receipt in one commit.
	DeleteWithCleanup(ctx context.Context, id DocumentID, ops DeleteOperations) error
}

// DocumentFileReader keeps a file lookup valid while the caller signs its address.
type DocumentFileReader interface {
	WithDocumentFile(ctx context.Context, id DocumentID, sign func(ctx context.Context, key string) error) (bool, error)
}
