package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	pgstore "github.com/Victoroide/quarry-rag/internal/store/postgres"
	s3store "github.com/Victoroide/quarry-rag/internal/store/s3"
	"github.com/aws/smithy-go"
	"github.com/google/uuid"
)

const (
	// maxInspectedVersions bounds the history examined for one key; a longer one stays pending.
	maxInspectedVersions = 100
	// uploadClaimLease must outlast a run; rows a stopped runner holds become due again afterwards.
	uploadClaimLease = 45 * time.Minute
	maxRetryDelay    = 24 * time.Hour
	maxBackoffSteps  = 6
	reportedUploads  = 100
)

// Reasons an upload stays pending. They are stored with the journal row and
// shown in the inventory, so each names the uncertainty that blocks removal.
const (
	reasonAwaitingSettlement   = "awaiting_settlement"
	reasonUnacknowledged       = "unacknowledged_write"
	reasonLegalHold            = "legal_hold"
	reasonRetention            = "retention_until="
	reasonVersionsUnlisted     = "versions_unlisted"
	reasonVersionsTruncated    = "versions_truncated"
	reasonVersionIDMissing     = "version_id_missing"
	reasonVersionSizeInvalid   = "version_size_invalid"
	reasonLockUnknown          = "lock_unknown"
	reasonDeleteFailed         = "delete_failed"
	reasonByteLimit            = "byte_limit"
	reasonReferenced           = "referenced_by_document"
	reasonUnrecognizedKey      = "unrecognized_key"
	actionDelete, actionSettle = "delete", "settle"
	actionPending              = "pending"
)

type uploadJournal interface {
	SourceUploadSummaries(context.Context, string) ([]pgstore.SourceUploadSummary, error)
	DueSourceUploads(context.Context, pgstore.SourceUploadSelection) ([]pgstore.SourceUploadRecord, error)
	ClaimSourceUploads(context.Context, pgstore.SourceUploadSelection, string, time.Duration) ([]pgstore.SourceUploadRecord, error)
	ReferencedSourceKeys(context.Context, []string) (map[string]bool, error)
	DeferSourceUpload(context.Context, string, string, string, time.Duration) (bool, error)
	SettleSourceUpload(context.Context, string, string) (bool, error)
}

type uploadObjects interface {
	ListVersions(ctx context.Context, key string, limit int) ([]s3store.ObjectVersion, bool, error)
	VersionLock(ctx context.Context, key, versionID string) (s3store.ObjectLock, error)
	DeleteVersion(ctx context.Context, key, versionID string) error
}

type uploadSummary struct {
	State        string    `json:"state"`
	Reason       string    `json:"reason,omitempty"`
	Acknowledged bool      `json:"acknowledged"`
	WritePending bool      `json:"write_pending"`
	Count        int       `json:"count"`
	Oldest       time.Time `json:"oldest"`
}

type uploadOutcome struct {
	UploadID     string `json:"upload_id"`
	Key          string `json:"key"`
	Acknowledged bool   `json:"acknowledged"`
	WritePending bool   `json:"write_pending"`
	Action       string `json:"action"`
	Reason       string `json:"reason,omitempty"`
	Versions     int    `json:"versions"`
	Removals     int    `json:"removals"`
	Bytes        int64  `json:"bytes"`
}

type uploadReport struct {
	Mode            string          `json:"mode"`
	Examined        int             `json:"examined"`
	Limited         bool            `json:"limited"`
	DeletedVersions int             `json:"deleted_versions"`
	DeletedBytes    int64           `json:"deleted_bytes"`
	Settled         int             `json:"settled"`
	Pending         int             `json:"pending"`
	Uploads         []uploadOutcome `json:"uploads"`
	Journal         []uploadSummary `json:"journal"`
}

// uploadPlan is what one exact-key inspection concludes for a journal row.
type uploadPlan struct {
	row      pgstore.SourceUploadRecord
	remove   []s3store.ObjectVersion
	reason   string
	versions int
}

// recognizedKey matches the journal row to a generated key, which includes
// a canonical incarnation UUID between the document and upload IDs for owned uploads.
func recognizedKey(row pgstore.SourceUploadRecord) bool {
	parts := strings.Split(row.Key, "/")
	if len(parts) != 5 && len(parts) != 6 {
		return false
	}
	kind, name := parts[1], parts[len(parts)-1]
	if kind == "" || kind == "." || kind == ".." || name == "" || name == "." || name == ".." ||
		parts[0] != "documents" || parts[2] != row.DocumentID || parts[len(parts)-2] != row.ID {
		return false
	}
	if len(parts) == 5 {
		return true
	}
	incarnation, err := uuid.Parse(parts[3])
	return err == nil && incarnation.String() == parts[3]
}

// settlementDue reports whether a tombstone may be dropped after a clean
// check. The window applies only once every possible PUT is accounted for.
func settlementDue(row pgstore.SourceUploadRecord, settle time.Duration) bool {
	return !row.WritePending && row.Checks >= 1 && !row.Now.Before(row.ClaimedAt.Add(settle))
}

// retryDelay schedules the next check. Settlement checks repeat at a fixed
// pace because they end once the window elapses. Every other pending reason,
// including a write that may never be acknowledged, backs off up to a day.
func retryDelay(reason string, checks int, base time.Duration) time.Duration {
	if reason == reasonAwaitingSettlement {
		return base
	}
	delay := base
	for range min(checks, maxBackoffSteps) {
		delay *= 2
	}
	return min(delay, maxRetryDelay)
}

func apiErrorCode(err error) string {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) && apiErr.ErrorCode() != "" {
		return apiErr.ErrorCode()
	}
	return ""
}

// inspect reads the exact recorded key and decides what may be removed. Any
// doubt about versions or protection leaves everything in place.
func inspect(ctx context.Context, objects uploadObjects, row pgstore.SourceUploadRecord, referenced bool) uploadPlan {
	plan := uploadPlan{row: row}
	switch {
	case !recognizedKey(row):
		plan.reason = reasonUnrecognizedKey
		return plan
	case referenced:
		plan.reason = reasonReferenced
		return plan
	}
	versions, complete, err := objects.ListVersions(ctx, row.Key, maxInspectedVersions)
	if errors.Is(err, s3store.ErrVersionIDMissing) {
		plan.reason = reasonVersionIDMissing
		return plan
	}
	if errors.Is(err, s3store.ErrVersionSizeInvalid) {
		plan.reason = reasonVersionSizeInvalid
		return plan
	}
	if err != nil {
		plan.reason = reasonVersionsUnlisted
		return plan
	}
	plan.versions = len(versions)
	if !complete {
		plan.reason = reasonVersionsTruncated
		return plan
	}
	for _, v := range versions {
		if v.DeleteMarker {
			plan.remove = append(plan.remove, v)
			continue
		}
		lock, lockErr := objects.VersionLock(ctx, row.Key, v.ID)
		switch {
		case errors.Is(lockErr, s3store.ErrVersionNotFound):
			plan.versions--
			continue
		case lockErr != nil:
			plan.reason, plan.remove = reasonLockUnknown, nil
			return plan
		case lock.LegalHold:
			plan.reason, plan.remove = reasonLegalHold, nil
			return plan
		case lock.Protected(row.Now):
			plan.reason, plan.remove = reasonRetention+lock.RetainUntil.UTC().Format(time.RFC3339), nil
			return plan
		}
		plan.remove = append(plan.remove, v)
	}
	return plan
}

type sourceUploadRun struct {
	journal uploadJournal
	objects uploadObjects
	opts    options
	claim   string
	report  uploadReport
	budget  int64
}

// reconcileSourceUploads inspects journaled uploads that no document adopted.
// Without Apply it only reads. With Apply it claims expired uploads, rechecks
// ownership, and removes the recorded object's versions, keeping a tombstone
// until later checks show that no outstanding write can still land.
func reconcileSourceUploads(ctx context.Context, journal uploadJournal, objects uploadObjects, bucket string, opts options) (uploadReport, error) {
	run := &sourceUploadRun{journal: journal, objects: objects, opts: opts, claim: uuid.NewString(), budget: opts.MaxBytes}
	run.report = uploadReport{Mode: "inventory", Uploads: []uploadOutcome{}, Journal: []uploadSummary{}}
	if opts.Apply {
		run.report.Mode = "apply"
	}
	sel := pgstore.SourceUploadSelection{Bucket: bucket, MinAge: opts.MinAge}
	var handled []string
	for run.report.Examined < opts.Limit {
		sel.Limit = min(opts.Batch, opts.Limit-run.report.Examined)
		sel.Skip = handled
		var rows []pgstore.SourceUploadRecord
		var err error
		switch {
		case opts.Apply:
			rows, err = journal.ClaimSourceUploads(ctx, sel, run.claim, uploadClaimLease)
		default:
			sel.Limit = opts.Limit
			rows, err = journal.DueSourceUploads(ctx, sel)
		}
		if err != nil {
			return run.report, err
		}
		if len(rows) == 0 {
			break
		}
		keys := make([]string, len(rows))
		for i, row := range rows {
			keys[i] = row.Key
		}
		referenced, err := journal.ReferencedSourceKeys(ctx, keys)
		if err != nil {
			return run.report, err
		}
		for _, row := range rows {
			handled = append(handled, row.ID)
			run.report.Examined++
			if err := run.handle(ctx, row, referenced[row.Key]); err != nil {
				return run.report, err
			}
		}
		if !opts.Apply {
			break
		}
	}
	if run.report.Examined >= opts.Limit {
		more, err := journal.DueSourceUploads(ctx, pgstore.SourceUploadSelection{Bucket: bucket, MinAge: opts.MinAge, Limit: 1, Skip: handled})
		if err != nil {
			return run.report, err
		}
		run.report.Limited = len(more) > 0
	}
	summaries, err := journal.SourceUploadSummaries(ctx, bucket)
	if err != nil {
		return run.report, err
	}
	for _, s := range summaries {
		run.report.Journal = append(run.report.Journal, uploadSummary(s))
	}
	return run.report, nil
}

func (r *sourceUploadRun) handle(ctx context.Context, row pgstore.SourceUploadRecord, referenced bool) error {
	plan := inspect(ctx, r.objects, row, referenced)
	outcome := uploadOutcome{UploadID: row.ID, Key: row.Key, Acknowledged: row.Acknowledged, WritePending: row.WritePending, Versions: plan.versions, Reason: plan.reason}
	switch {
	case plan.reason != "":
		outcome.Action = actionPending
	case len(plan.remove) > 0:
		// A removal is verified by a later clean check before the tombstone goes.
		outcome.Action, outcome.Reason = actionDelete, reasonAwaitingSettlement
		if stopped := r.remove(ctx, plan, &outcome); stopped != "" {
			outcome.Action, outcome.Reason = actionPending, stopped
		}
	case settlementDue(row, r.opts.Settle):
		outcome.Action = actionSettle
	default:
		outcome.Action, outcome.Reason = actionPending, reasonAwaitingSettlement
		if row.WritePending {
			outcome.Reason = reasonUnacknowledged
		}
	}
	if r.opts.Apply {
		if err := r.record(ctx, row, outcome); err != nil {
			return err
		}
	} else if outcome.Action != actionSettle {
		r.report.Pending++
	}
	if len(r.report.Uploads) < reportedUploads {
		r.report.Uploads = append(r.report.Uploads, outcome)
	}
	return nil
}

// remove plans or applies removals within the run's byte budget. It reports
// why removal stopped early, if applicable.
func (r *sourceUploadRun) remove(ctx context.Context, plan uploadPlan, outcome *uploadOutcome) string {
	for _, v := range plan.remove {
		if v.Size > r.budget {
			return reasonByteLimit
		}
		// A failed reply can follow a completed deletion. Reserve the bytes
		// before dispatch and count them as deleted only on success.
		r.budget -= v.Size
		if r.opts.Apply {
			if err := r.objects.DeleteVersion(ctx, plan.row.Key, v.ID); err != nil {
				reason := reasonDeleteFailed
				if code := apiErrorCode(err); code != "" {
					reason += ":" + code
				}
				return reason
			}
			r.report.DeletedVersions++
			r.report.DeletedBytes += v.Size
		}
		outcome.Removals++
		outcome.Bytes += v.Size
	}
	return ""
}

func (r *sourceUploadRun) record(ctx context.Context, row pgstore.SourceUploadRecord, outcome uploadOutcome) error {
	var owned bool
	var err error
	if outcome.Action == actionSettle {
		owned, err = r.journal.SettleSourceUpload(ctx, row.ID, r.claim)
		if err == nil && owned {
			r.report.Settled++
		}
	} else {
		retry := retryDelay(outcome.Reason, row.Checks, r.opts.Settle/3)
		owned, err = r.journal.DeferSourceUpload(ctx, row.ID, r.claim, outcome.Reason, retry)
		if err == nil && owned {
			r.report.Pending++
		}
	}
	if err != nil {
		return fmt.Errorf("record outcome of upload %s: %w", row.ID, err)
	}
	return nil
}
