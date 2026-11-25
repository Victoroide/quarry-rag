package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/google/uuid"
)

// StageSourceUpload durably records the owner and object key of an original
// file before the object store receives it. The record expires after
// domain.SourceUploadLease unless acceptance links it to a document.
func (c *Client) StageSourceUpload(ctx context.Context, upload domain.SourceUpload) error {
	if _, err := uuid.Parse(upload.ID); err != nil {
		return fmt.Errorf("postgres: stage source upload: invalid upload id: %w", err)
	}
	if _, err := uuid.Parse(string(upload.DocumentID)); err != nil {
		return fmt.Errorf("postgres: stage source upload: invalid document id: %w", err)
	}
	if upload.Bucket == "" || upload.Key == "" {
		return errors.New("postgres: stage source upload: bucket and key are required")
	}
	_, err := c.Pool.Exec(ctx, `INSERT INTO source_uploads(id,document_id,collection,bucket,object_key,expires_at)
		VALUES($1,$2,$3,$4,$5,NOW()+make_interval(secs=>$6))`,
		upload.ID, string(upload.DocumentID), upload.Collection, upload.Bucket, upload.Key, domain.SourceUploadLease.Seconds())
	if err != nil {
		return fmt.Errorf("postgres: stage source upload: %w", err)
	}
	return nil
}

// BeginSourceUpload records possible delivery before I/O. Only the writer
// that makes this transition can dispatch the PUT and its SDK retries.
func (c *Client) BeginSourceUpload(ctx context.Context, id string) error {
	tag, err := c.Pool.Exec(ctx, `UPDATE source_uploads SET write_state='uncertain'
		WHERE id=$1 AND state='staged' AND write_state='not_started' AND expires_at>NOW()`, id)
	if err != nil {
		return fmt.Errorf("postgres: begin source upload: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return errors.New("postgres: source upload cannot begin")
	}
	return nil
}

// RecordSourceUploadResult can account for a PUT even if maintenance has
// already claimed the upload. It leaves the maintenance claim and fence intact.
func (c *Client) RecordSourceUploadResult(ctx context.Context, id string, result domain.SourceUploadResult) error {
	_, err := c.Pool.Exec(ctx, `UPDATE source_uploads SET
		acknowledged_at=CASE WHEN $2 THEN COALESCE(acknowledged_at,NOW()) ELSE acknowledged_at END,
		version_id=COALESCE(NULLIF($3,''),version_id),
		write_state=CASE WHEN $4 THEN 'settled' ELSE write_state END
		WHERE id=$1 AND write_state='uncertain' AND state IN ('staged','claimed')`,
		id, result.Acknowledged, result.VersionID, result.Settled)
	if err != nil {
		return fmt.Errorf("postgres: record source upload result: %w", err)
	}
	return nil
}
