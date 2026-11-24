package postgres

import (
	"context"
	"encoding/hex"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/Victoroide/quarry-rag/internal/textutil"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const (
	maxGrantBatchBytes   = 256 << 10
	maxGrantBatchEntries = 500
	// GrantImportTimeout bounds database work for a grant import.
	GrantImportTimeout = 30 * time.Second
)

var grantBatchFields = map[string][]string{
	"tenants":            {"tenant_id"},
	"principals":         {"tenant_id", "principal_id", "issuer", "subject", "enabled"},
	"collections":        {"tenant_id", "collection"},
	"create_permissions": {"tenant_id", "principal_id", "collection", "can_create"},
	"assignments":        {"tenant_id", "principal_id", "collection", "document_id", "source_version"},
	"grants":             {"tenant_id", "principal_id", "document_id", "incarnation", "can_read", "can_download"},
	"reservations":       {"tenant_id", "principal_id", "collection", "document_id"},
}

// GrantImportResult reports the revision observed by a preview or recorded by an import.
type GrantImportResult struct {
	Revision int64 `json:"revision"`
	DryRun   bool  `json:"dry_run"`
	Replayed bool  `json:"replayed"`
}

// GrantBatch holds a bounded, validated operator payload.
type GrantBatch struct {
	raw string
}

// PrepareGrantBatch reads and validates operator input before database work.
func PrepareGrantBatch(input io.Reader) (GrantBatch, error) {
	payload, err := decodeGrantBatch(input)
	return GrantBatch{raw: string(payload)}, err
}

// ImportGrantBatch validates and submits a bounded catalog import using the connected operator login.
func (c *Client) ImportGrantBatch(ctx context.Context, input io.Reader, dryRun bool) (GrantImportResult, error) {
	batch, err := PrepareGrantBatch(input)
	if err != nil {
		return GrantImportResult{}, err
	}
	return c.ImportPreparedGrantBatch(ctx, batch, dryRun)
}

// ImportPreparedGrantBatch submits a batch that was validated before connecting.
func (c *Client) ImportPreparedGrantBatch(ctx context.Context, batch GrantBatch, dryRun bool) (GrantImportResult, error) {
	ctx, cancel := context.WithTimeout(ctx, GrantImportTimeout)
	defer cancel()
	options := pgx.TxOptions{IsoLevel: pgx.ReadCommitted}
	if dryRun {
		options = pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly}
	}
	var result GrantImportResult
	err := pgx.BeginTxFunc(ctx, c.Pool, options, func(tx pgx.Tx) error {
		if _, setupErr := tx.Exec(ctx, `SET LOCAL lock_timeout = '5s'; SET LOCAL statement_timeout = '25s'`); setupErr != nil {
			return setupErr
		}
		if schemaErr := bindMetadataSchema(ctx, tx); schemaErr != nil {
			return schemaErr
		}
		return tx.QueryRow(ctx, `SELECT resulting_revision, replayed FROM grant_catalog_import_batch($1,$2)`, batch.raw, dryRun).
			Scan(&result.Revision, &result.Replayed)
	})
	if err != nil {
		return GrantImportResult{}, grantBatchError(err)
	}
	result.DryRun = dryRun
	return result, nil
}

func grantBatchError(err error) error {
	var postgresErr *pgconn.PgError
	if errors.As(err, &postgresErr) {
		// The catalog raises these fixed messages. Other diagnostics can contain input values.
		switch postgresErr.Code {
		case "QG001", "QG002", "QG003", "QG004", "QG005":
			return fmt.Errorf("grant import: %s", postgresErr.Message)
		default:
			return fmt.Errorf("grant import failed (SQLSTATE %s)", postgresErr.Code)
		}
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("grant import interrupted: %w", err)
	}
	return errors.New("grant import interrupted before confirmation")
}

func decodeGrantBatch(input io.Reader) ([]byte, error) {
	payload, err := io.ReadAll(io.LimitReader(input, maxGrantBatchBytes+1))
	if err != nil {
		return nil, errors.New("cannot read grant batch")
	}
	if len(payload) > maxGrantBatchBytes {
		return nil, fmt.Errorf("grant batch exceeds %d bytes", maxGrantBatchBytes)
	}
	var batch map[string]jsontext.Value
	if err := json.Unmarshal(payload, &batch); err != nil {
		return nil, errors.New("invalid grant batch JSON")
	}
	var expected *int64
	if !validGrantField("batch_id", batch["batch_id"]) ||
		json.Unmarshal(batch["expected_revision"], &expected) != nil || expected == nil || *expected < 0 {
		return nil, errors.New("grant batch requires a batch_id and nonnegative expected_revision")
	}
	count := 0
	for group, body := range batch {
		if group == "batch_id" || group == "expected_revision" {
			continue
		}
		fields, ok := grantBatchFields[group]
		if !ok {
			return nil, errors.New("unknown grant batch field")
		}
		n, err := validateGrantEntries(group, body, fields, maxGrantBatchEntries-count)
		if err != nil {
			return nil, err
		}
		count += n
	}
	if count == 0 {
		return nil, errors.New("grant batch requires at least one entry")
	}
	return payload, nil
}

func validateGrantEntries(group string, body jsontext.Value, fields []string, remaining int) (int, error) {
	var entries []map[string]jsontext.Value
	if body.Kind() != '[' || json.Unmarshal(body, &entries) != nil {
		return 0, fmt.Errorf("grant batch %s must be an array", group)
	}
	if len(entries) > remaining {
		return 0, fmt.Errorf("grant batch exceeds %d entries", maxGrantBatchEntries)
	}
	seen := make(map[string]bool, len(entries))
	for i, entry := range entries {
		if len(entry) != len(fields) {
			return 0, fmt.Errorf("grant batch %s entry %d has missing or unexpected fields", group, i+1)
		}
		for _, field := range fields {
			if !validGrantField(field, entry[field]) {
				return 0, fmt.Errorf("grant batch %s entry %d has invalid %s", group, i+1, field)
			}
		}
		if group == "create_permissions" || group == "reservations" {
			var label string
			_ = json.Unmarshal(entry["collection"], &label)
			canonical, err := textutil.NormalizeLabel(label)
			if err != nil || canonical != label {
				return 0, fmt.Errorf("grant batch %s entry %d requires a canonical collection", group, i+1)
			}
		}
		key := grantEntryKey(group, entry)
		if seen[key] {
			return 0, fmt.Errorf("grant batch %s entry %d duplicates an earlier target", group, i+1)
		}
		seen[key] = true
	}
	return len(entries), nil
}

func grantEntryKey(group string, entry map[string]jsontext.Value) string {
	value := func(field string) string {
		var s string
		_ = json.Unmarshal(entry[field], &s)
		return s
	}
	switch group {
	case "tenants":
		return value("tenant_id")
	case "principals":
		return value("principal_id")
	case "collections":
		return value("tenant_id") + value("collection")
	case "create_permissions":
		return value("principal_id") + value("collection")
	case "grants":
		return value("document_id") + value("principal_id")
	default:
		return value("document_id")
	}
}

func validGrantField(field string, raw jsontext.Value) bool {
	if field == "enabled" || strings.HasPrefix(field, "can_") {
		return raw.Kind() == 't' || raw.Kind() == 'f'
	}
	if raw.Kind() != '"' {
		return false
	}
	var value string
	if json.Unmarshal(raw, &value) != nil || strings.ContainsRune(value, 0) {
		return false
	}
	switch field {
	case "collection":
		return len(value) <= 128
	case "issuer", "subject":
		return value != "" && len(value) <= 1024
	case "source_version":
		_, err := hex.DecodeString(value)
		return err == nil && len(value) == 64 && value == strings.ToLower(value)
	default:
		id, err := uuid.Parse(value)
		return err == nil && id != uuid.Nil && value == id.String()
	}
}
