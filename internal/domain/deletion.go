package domain

import (
	"context"
	"errors"
	"time"
)

var (
	// ErrDocumentNotFound indicates that the requested document is absent.
	ErrDocumentNotFound = errors.New("document not found")
	// ErrUnauthorized indicates that the caller's subject credential is missing or expired.
	ErrUnauthorized = errors.New("invalid or missing credentials")
	// ErrForbidden indicates that the caller's principal is absent or disabled.
	ErrForbidden = errors.New("access denied")
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
	// MemoryOwner identifies the in-memory index. A qualified request converts a
	// reservation recorded for another owner only on that owner.
	MemoryOwner string
	// SourceBucket and VectorIndex identify the nonsecret stores the operations act on.
	// They are recorded with the cleanup target and compared on every resumption.
	SourceBucket string
	VectorIndex  string
	// LegacyNamespace and LegacyVectors remove vectors written without an incarnation.
	LegacyNamespace func(ctx context.Context, documentID string) error
	LegacyVectors   func(ctx context.Context, documentID string) error
	// RevisionVectors removes the vectors of one incarnation.
	RevisionVectors func(ctx context.Context, documentID, incarnation string) error
	// ManifestVectors removes the vectors one hosted manifest revision wrote, by
	// the exact IDs its manifest recorded. A document that owns manifested
	// revisions is deleted this way and never by document or namespace.
	ManifestVectors func(ctx context.Context, documentID, revisionID string, vectorIDs []string) error
	// Source removes the exact original object.
	Source func(ctx context.Context, key string) error
	// ConfirmedOfflineOwnerTerminated marks operator verification that the
	// recorded process-local index owner exited and its local vectors are gone.
	ConfirmedOfflineOwnerTerminated bool
}

// HeldDeletion describes an unfinished or held deletion receipt that keeps a
// public UUID from being admitted again.
type HeldDeletion struct {
	DocumentID      DocumentID `json:"document_id"`
	Incarnation     string     `json:"incarnation"`
	Kind            string     `json:"kind"`
	VectorMode      string     `json:"vector_mode"`
	VectorIndex     string     `json:"vector_index,omitempty"`
	VectorNamespace string     `json:"vector_namespace,omitempty"`
	MemoryOwner     string     `json:"memory_owner,omitempty"`
	SourceKey       string     `json:"source_key,omitempty"`
	SourceBucket    string     `json:"source_bucket,omitempty"`
	Step            int        `json:"step"`
	Phase           string     `json:"phase"`
	Held            bool       `json:"held"`
	Finalized       bool       `json:"finalized"`
	DocumentPresent bool       `json:"document_present"`
	Claimed         bool       `json:"claimed"`
	Claim           string     `json:"claim,omitempty"`
	ClaimUntil      *time.Time `json:"claim_until,omitempty"`
}

// QuarantineTarget names the default backend recorded when quarantining an
// absent document UUID that has no existing receipt.
type QuarantineTarget struct {
	VectorMode  string
	VectorIndex string
}

// HeldDeletionReleaser releases a held or unfinished deletion receipt after
// verified cleanup.
type HeldDeletionReleaser interface {
	ReleaseHeldDeletion(ctx context.Context, id DocumentID, incarnation string, ops DeleteOperations) error
}

// HeldDeletionStore inspects, quarantines and releases held identities.
type HeldDeletionStore interface {
	HeldDeletionReleaser
	ListHeldDeletions(ctx context.Context) ([]HeldDeletion, error)
	QuarantineDeletion(ctx context.Context, id DocumentID, target QuarantineTarget) (HeldDeletion, error)
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

// ErrDeletionActivated indicates that the database selected checkpointed
// deletion while an ID-only cleanup was waiting to start. The caller continues
// on the activated contract.
var ErrDeletionActivated = errors.New("checkpointed deletion has been activated")

// LegacyDeletionSpec describes the stores an ID-only cleanup acts on. The
// reservation records them before the first external mutation.
type LegacyDeletionSpec struct {
	// Hosted marks a remote vector index and Memory a process-local one. Neither
	// is set for transactional vectors, which the metadata removal cascades to.
	Hosted bool
	Memory bool
	// RevisionVectors reports that vectors of one incarnation can be deleted.
	RevisionVectors bool
	// SourceAvailable reports that the original object can be deleted.
	SourceAvailable bool
	// SourceBucket and VectorIndex name the nonsecret stores of the cleanup.
	SourceBucket string
	VectorIndex  string
	// MemoryOwner identifies the process-local index that holds the vectors.
	MemoryOwner string
}

// LegacyDeletionTarget is the document lifetime observed under its row lock.
type LegacyDeletionTarget struct {
	Incarnation string
	// SourceKey is the exact original object of that lifetime, if any.
	SourceKey *string
	// ScopedVectors reports that the lifetime wrote incarnation-tagged vectors.
	ScopedVectors bool
	// Claim names the committed reservation this cleanup owns, or is empty when
	// the vectors need none.
	Claim string
}

// LegacyDocumentStore runs one ID-only cleanup against the original row. Its
// methods act on the connection that holds the activation lock while that
// connection lives, and on a fresh one afterwards.
type LegacyDocumentStore interface {
	// PrepareLegacyDeletion locks the live row, synchronizes with worker
	// mutations and commits the reservation before any external mutation.
	PrepareLegacyDeletion(ctx context.Context, id DocumentID, spec LegacyDeletionSpec) (LegacyDeletionTarget, error)
	// NoteLegacyUncertain keeps the identity held although a later attempt may succeed.
	NoteLegacyUncertain(ctx context.Context, id DocumentID, target LegacyDeletionTarget) error
	// RetireLegacyDocument removes the observed row without completing its reservation.
	RetireLegacyDocument(ctx context.Context, id DocumentID, target LegacyDeletionTarget) error
	// CompleteLegacyDeletion records that the last external effect returned.
	CompleteLegacyDeletion(ctx context.Context, id DocumentID, target LegacyDeletionTarget, uncertain bool) error
	// AbandonLegacyDeletion lets another request resume the reservation.
	AbandonLegacyDeletion(ctx context.Context, id DocumentID, target LegacyDeletionTarget)
}

// LegacyDeletionGuard keeps activation from committing under an ID-only cleanup.
type LegacyDeletionGuard interface {
	// WithLegacyDeletion returns ErrDeletionActivated, without calling remove,
	// when the database already selected checkpointed deletion.
	WithLegacyDeletion(ctx context.Context, id DocumentID, remove func(LegacyDocumentStore) error) error
}

// InMemoryIndex identifies a process-local vector index and serializes its
// deletions. Only the process that owns the index can complete a mutation of it.
type InMemoryIndex interface {
	// MemoryOwner identifies this index instance for as long as the process lives.
	MemoryOwner() string
	// LockDocument takes the single local deletion slot of one document, or
	// reports ok as false while another caller holds it.
	LockDocument(documentID string) (unlock func(), ok bool)
}

// ScopedFileResult reports whether the principal was active and a downloadable
// original object was found and signed.
type ScopedFileResult struct {
	Active bool
	Found  bool
}

// ScopedDocumentFileReader checks owner or read-plus-download grants under a
// shared row lock while the caller signs the stored object key.
type ScopedDocumentFileReader interface {
	WithScopedDocumentFile(ctx context.Context, principal Principal, id DocumentID, sign func(ctx context.Context, key string) error) (ScopedFileResult, error)
}

// ScopedDocumentDeleter atomically authorizes owner-only deletion against the
// live incarnation and completes detached cleanup for that target.
type ScopedDocumentDeleter interface {
	BeginScopedDeletion(ctx context.Context, principal Principal, validUntil time.Time, id DocumentID, ops DeleteOperations) (incarnation string, complete bool, err error)
	ResumeScopedDeletion(ctx context.Context, id DocumentID, incarnation string, ops DeleteOperations) error
}
