package domain

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// AcceptOutcome describes the atomic acceptance decision.
type AcceptOutcome int

// Acceptance outcomes distinguish existing documents from newly queued work.
const (
	AcceptAccepted AcceptOutcome = iota
	AcceptDuplicate
	AcceptInProgress
	AcceptRequeued
	AcceptConflict
	AcceptQueueFull
	AcceptNeedsSource
	AcceptOCRRetryRequired
	AcceptNeedsPDFText
	AcceptOCRRetryAfterExpiry
	AcceptUploadExpired
	// AcceptDeletionConflict reports a document identity reserved by an unfinished or held deletion.
	AcceptDeletionConflict
)

// ContentDedup controls whether uploads participate in collection-wide content matching.
type ContentDedup string

// Content deduplication policies distinguish shared and document-bound identity.
const (
	ContentDedupCollection ContentDedup = "collection"
	ContentDedupOff        ContentDedup = "off"
)

// AcceptRequest supplies document identity, content, and queue admission limits.
type AcceptRequest struct {
	Document                 Document
	CallerSupplied           bool
	Fingerprint              string
	DocType                  string
	Collection               string
	ContentDedup             ContentDedup
	RequireSource            bool
	MaxPending               int
	EvaluationReceipt        string
	EvaluationVectorIdentity string
	PDF                      *PDFExtraction
	LegacyParserFingerprint  string
	LegacyOCRFingerprint     string
	TryLegacyPDFText         bool
	// SupportsScopedVectors permits tagged admission if the database is active.
	SupportsScopedVectors bool
	// SourceUpload, when set, must still be staged for the new document's original file.
	SourceUpload *SourceUploadLink
}

// AcceptResult identifies the document selected by acceptance.
type AcceptResult struct {
	DocumentID  DocumentID
	Outcome     AcceptOutcome
	Incarnation string
}

// DocumentAcceptor durably stores accepted documents and their pending work.
type DocumentAcceptor interface {
	AcceptDocument(context.Context, AcceptRequest) (AcceptResult, error)
}

// ExistingPDFAcceptor resolves an existing PDF without admitting a new document.
// Anonymous requests may match only collection-wide original-byte identities.
type ExistingPDFAcceptor interface {
	AcceptExistingPDF(ctx context.Context, request AcceptRequest, includeLegacy bool) (AcceptResult, error)
}

// Job carries the document and the current lease identity.
type Job struct {
	Document    Document
	DocType     string
	Attempts    int
	LeaseOwner  string
	LeaseToken  string
	Incarnation string

	Collection    string
	PDF           *PDFExtraction
	ScopedVectors bool
}

// JobStore coordinates durable ingestion through owned leases.
type JobStore interface {
	ClaimJob(context.Context, string, time.Duration, int) (*Job, error)
	HeartbeatJob(context.Context, Job, time.Duration) (bool, error)
	CompleteJob(context.Context, Job, []Chunk, IndexObservation) (bool, error)
	RetryJob(context.Context, Job, time.Duration, string) error
	FailJob(context.Context, Job, string) error
	ReleaseJob(context.Context, Job) error
}

// JobVectorGuard claims provider mutations for a live job attempt.
type JobVectorGuard interface {
	MutateJobVectors(context.Context, Job, func(context.Context) error) error
}

// ErrPermanent marks failures that cannot succeed on retry.
var ErrPermanent = errors.New("permanent ingestion failure")

// Permanent preserves the original error while marking it non-retryable.
func Permanent(err error) error { return fmt.Errorf("%w: %w", ErrPermanent, err) }

// ErrJobNotOwned indicates that a lease transition did not apply.
var ErrJobNotOwned = errors.New("ingestion job lease is not owned")
