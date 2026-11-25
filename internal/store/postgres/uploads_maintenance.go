package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// SourceUploadRecord is a journal row as maintenance sees it. Now is the
// database clock when the row was read, so decisions never depend on the
// maintenance host's clock.
type SourceUploadRecord struct {
	ID, DocumentID, Collection, Bucket, Key, State, VersionID, Reason string
	Acknowledged, WritePending                                        bool
	Checks                                                            int
	CreatedAt, ExpiresAt, ClaimedAt, LastCheckedAt, NextCheckAt, Now  time.Time
}

// SourceUploadSelection bounds the uploads one maintenance pass may touch.
type SourceUploadSelection struct {
	Bucket string
	// MinAge keeps uploads younger than this out of reach, even once expired.
	MinAge time.Duration
	Limit  int
	// Skip lists upload IDs a pass has already handled, so deferred rows
	// that are immediately due again are not selected twice.
	Skip []string
}

// SourceUploadSummary counts journal rows sharing a state and pending reason.
type SourceUploadSummary struct {
	State, Reason              string
	Acknowledged, WritePending bool
	Count                      int
	Oldest                     time.Time
}

const (
	sourceUploadFields = `id::text,document_id::text,collection,bucket,object_key,state,version_id,reason,
		acknowledged_at IS NOT NULL,write_state='uncertain',checks,created_at,expires_at,claimed_at,last_checked_at,next_check_at,NOW()`
	sourceUploadOrder = ` ORDER BY COALESCE(next_check_at,expires_at),id`
	// Both states must meet the minimum age. A staged upload must also have
	// expired, and a claimed one must be due for recheck with no live claim.
	dueSourceUploads = `bucket=$1 AND NOT (id::text = ANY($3::text[])) AND created_at<NOW()-make_interval(secs=>$2) AND (
		(state='staged' AND expires_at<NOW())
		OR (state='claimed' AND (next_check_at IS NULL OR next_check_at<=NOW()) AND (claim_until IS NULL OR claim_until<NOW())))`
)

func scanSourceUploads(rows pgx.Rows) ([]SourceUploadRecord, error) {
	defer rows.Close()
	var records []SourceUploadRecord
	for rows.Next() {
		var r SourceUploadRecord
		var version, reason *string
		var claimed, checked, next *time.Time
		if err := rows.Scan(&r.ID, &r.DocumentID, &r.Collection, &r.Bucket, &r.Key, &r.State, &version, &reason,
			&r.Acknowledged, &r.WritePending, &r.Checks, &r.CreatedAt, &r.ExpiresAt, &claimed, &checked, &next, &r.Now); err != nil {
			return nil, err
		}
		if version != nil {
			r.VersionID = *version
		}
		if reason != nil {
			r.Reason = *reason
		}
		if claimed != nil {
			r.ClaimedAt = *claimed
		}
		if checked != nil {
			r.LastCheckedAt = *checked
		}
		if next != nil {
			r.NextCheckAt = *next
		}
		records = append(records, r)
	}
	return records, rows.Err()
}

func skipList(sel SourceUploadSelection) []string {
	if sel.Skip == nil {
		return []string{}
	}
	return sel.Skip
}

// DueSourceUploads lists the uploads a claim would select, without writing.
func (c *Client) DueSourceUploads(ctx context.Context, sel SourceUploadSelection) ([]SourceUploadRecord, error) {
	rows, err := c.Pool.Query(ctx, `SELECT `+sourceUploadFields+` FROM source_uploads WHERE `+dueSourceUploads+
		sourceUploadOrder+` LIMIT $4`, sel.Bucket, sel.MinAge.Seconds(), skipList(sel), sel.Limit)
	if err != nil {
		return nil, fmt.Errorf("postgres: list due source uploads: %w", err)
	}
	records, err := scanSourceUploads(rows)
	if err != nil {
		return nil, fmt.Errorf("postgres: read due source uploads: %w", err)
	}
	return records, nil
}

// ClaimSourceUploads fences the due uploads for one runner. A staged upload
// can no longer be adopted by acceptance once claimed. Rows that another
// transaction holds are skipped and stay due for a later pass.
func (c *Client) ClaimSourceUploads(ctx context.Context, sel SourceUploadSelection, claim string, lease time.Duration) ([]SourceUploadRecord, error) {
	rows, err := c.Pool.Query(ctx, `WITH due AS (
			SELECT id FROM source_uploads WHERE `+dueSourceUploads+sourceUploadOrder+` LIMIT $4 FOR UPDATE SKIP LOCKED),
		claimed AS (
			UPDATE source_uploads u SET state='claimed',claim=$5,claim_until=NOW()+make_interval(secs=>$6),claimed_at=COALESCE(u.claimed_at,NOW())
			FROM due WHERE u.id=due.id RETURNING u.*)
		SELECT `+sourceUploadFields+` FROM claimed`+sourceUploadOrder,
		sel.Bucket, sel.MinAge.Seconds(), skipList(sel), sel.Limit, claim, lease.Seconds())
	if err != nil {
		return nil, fmt.Errorf("postgres: claim source uploads: %w", err)
	}
	records, err := scanSourceUploads(rows)
	if err != nil {
		return nil, fmt.Errorf("postgres: read claimed source uploads: %w", err)
	}
	return records, nil
}

// ReferencedSourceKeys reports which object keys a document or a pending
// deletion still names, so cleanup never removes an object that is in use.
func (c *Client) ReferencedSourceKeys(ctx context.Context, keys []string) (map[string]bool, error) {
	referenced := make(map[string]bool)
	if len(keys) == 0 {
		return referenced, nil
	}
	rows, err := c.Pool.Query(ctx, `SELECT k FROM unnest($1::text[]) k
		WHERE EXISTS (SELECT 1 FROM documents WHERE s3_key=k) OR EXISTS (SELECT 1 FROM document_deletions WHERE source_key=k)`, keys)
	if err != nil {
		return nil, fmt.Errorf("postgres: look up referenced source keys: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return nil, fmt.Errorf("postgres: read referenced source key: %w", err)
		}
		referenced[key] = true
	}
	return referenced, rows.Err()
}

// DeferSourceUpload records that a claimed upload stays pending, and why, then
// schedules its next check. It reports false when the caller no longer holds the claim.
func (c *Client) DeferSourceUpload(ctx context.Context, id, claim, reason string, retry time.Duration) (bool, error) {
	tag, err := c.Pool.Exec(ctx, `UPDATE source_uploads SET claim=NULL,claim_until=NULL,checks=checks+1,last_checked_at=NOW(),
		next_check_at=NOW()+make_interval(secs=>$3),reason=left($4,256) WHERE id=$1 AND claim=$2 AND state='claimed'`, id, claim, retry.Seconds(), reason)
	if err != nil {
		return false, fmt.Errorf("postgres: defer source upload: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// SettleSourceUpload drops the tombstone of a claimed upload whose object is
// gone and can no longer appear. It reports false if settlement is fenced.
func (c *Client) SettleSourceUpload(ctx context.Context, id, claim string) (bool, error) {
	tag, err := c.Pool.Exec(ctx, `DELETE FROM source_uploads WHERE id=$1 AND claim=$2 AND state='claimed' AND write_state<>'uncertain'`, id, claim)
	if err != nil {
		return false, fmt.Errorf("postgres: settle source upload: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// SourceUploadSummaries counts the bucket's journal rows by state, pending reason
// and acknowledgment, so uncertainty stays visible to operators.
func (c *Client) SourceUploadSummaries(ctx context.Context, bucket string) ([]SourceUploadSummary, error) {
	rows, err := c.Pool.Query(ctx, `SELECT state,COALESCE(reason,''),acknowledged_at IS NOT NULL,write_state='uncertain',count(*),min(created_at)
		FROM source_uploads WHERE bucket=$1 GROUP BY 1,2,3,4 ORDER BY 1,2,3,4`, bucket)
	if err != nil {
		return nil, fmt.Errorf("postgres: summarize source uploads: %w", err)
	}
	defer rows.Close()
	var summaries []SourceUploadSummary
	for rows.Next() {
		var s SourceUploadSummary
		if err := rows.Scan(&s.State, &s.Reason, &s.Acknowledged, &s.WritePending, &s.Count, &s.Oldest); err != nil {
			return nil, fmt.Errorf("postgres: read source upload summary: %w", err)
		}
		summaries = append(summaries, s)
	}
	return summaries, rows.Err()
}
