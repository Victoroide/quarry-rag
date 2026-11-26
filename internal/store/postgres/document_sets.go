package postgres

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const (
	// MaxDocumentSetManifestBytes bounds the manifest an operator may stream.
	MaxDocumentSetManifestBytes = 8 << 20
	// MaxDocumentSetMembers is the largest membership ceiling an operator may configure.
	MaxDocumentSetMembers = 100000
	// DefaultDocumentSetTTL is the lifetime of a set created without one.
	DefaultDocumentSetTTL = time.Hour
	// MaxDocumentSetTTL is the longest lifetime a set can have.
	MaxDocumentSetTTL = 24 * time.Hour
	// MaxDocumentSetImportKeyBytes bounds an owner-scoped idempotency key.
	MaxDocumentSetImportKeyBytes = 128
	// DocumentSetImportTimeout bounds database work for one creation.
	DocumentSetImportTimeout = 60 * time.Second
	// PurgeDocumentSetsStep is the most expired sets one purge call removes.
	PurgeDocumentSetsStep = 100

	// manifestLineBytes is a canonical UUID with its line ending.
	manifestLineBytes = 38
)

// ErrDocumentSetManifest marks a manifest the operator command refuses before
// any database work.
var ErrDocumentSetManifest = errors.New("invalid document set manifest")

// ReadDocumentSetManifest streams a manifest of one document UUID per line. It
// refuses more than maxMembers raw entries and more than
// MaxDocumentSetManifestBytes before it canonicalizes anything, rejects blank
// lines and any spelling other than the hyphenated 36-character form, and
// returns the distinct identifiers in ascending order. An empty manifest names
// an empty set.
func ReadDocumentSetManifest(input io.Reader, maxMembers int) ([]string, error) {
	if maxMembers < 1 || maxMembers > MaxDocumentSetMembers {
		return nil, fmt.Errorf("%w: membership ceiling must be between 1 and %d", ErrDocumentSetManifest, MaxDocumentSetMembers)
	}
	limited := &io.LimitedReader{R: input, N: MaxDocumentSetManifestBytes + 1}
	scanner := bufio.NewScanner(limited)
	scanner.Buffer(make([]byte, 0, manifestLineBytes*2), manifestLineBytes*2)
	seen := make(map[uuid.UUID]struct{})
	raw := 0
	for scanner.Scan() {
		raw++
		if raw > maxMembers {
			return nil, fmt.Errorf("%w: more than %d entries", ErrDocumentSetManifest, maxMembers)
		}
		line := scanner.Text()
		id, err := uuid.Parse(line)
		if len(line) != 36 || err != nil || id == uuid.Nil {
			return nil, fmt.Errorf("%w: entry %d is not a document UUID", ErrDocumentSetManifest, raw)
		}
		seen[id] = struct{}{}
	}
	if limited.N <= 0 {
		return nil, fmt.Errorf("%w: exceeds %d bytes", ErrDocumentSetManifest, MaxDocumentSetManifestBytes)
	}
	if err := scanner.Err(); err != nil {
		if errors.Is(err, bufio.ErrTooLong) {
			return nil, fmt.Errorf("%w: entry %d is not a document UUID", ErrDocumentSetManifest, raw+1)
		}
		return nil, errors.New("cannot read document set manifest")
	}
	ids := make([]string, 0, len(seen))
	for id := range seen {
		ids = append(ids, id.String())
	}
	slices.Sort(ids)
	return ids, nil
}

// DocumentSetRequest describes one set an operator creates for one principal.
type DocumentSetRequest struct {
	TenantID  string
	OwnerID   string
	TTL       time.Duration
	ImportKey string
	// DocumentIDs are the distinct members, as ReadDocumentSetManifest returns them.
	DocumentIDs []string
	// MaxMembers is the configured membership ceiling.
	MaxMembers int
}

// DocumentSet is a created or replayed set. The reference grants no access.
type DocumentSet struct {
	ID        string    `json:"document_set_id"`
	Revision  string    `json:"revision"`
	ExpiresAt time.Time `json:"expires_at"`
	Members   int       `json:"members"`
	// Replayed reports that an identical earlier import with the same key
	// already produced this set.
	Replayed bool `json:"replayed"`
}

// Validate checks a request without touching the database, so an operator
// command can refuse it before connecting.
func (r DocumentSetRequest) Validate() error {
	_, _, _, err := r.normalized()
	return err
}

func (r DocumentSetRequest) normalized() (tenant, owner uuid.UUID, ttl time.Duration, err error) {
	ttl = r.TTL
	if ttl == 0 {
		ttl = DefaultDocumentSetTTL
	}
	tenant, tenantErr := uuid.Parse(r.TenantID)
	owner, ownerErr := uuid.Parse(r.OwnerID)
	switch {
	case tenantErr != nil || ownerErr != nil || tenant == uuid.Nil || owner == uuid.Nil:
		err = errors.New("document set requires a tenant and an owning principal UUID")
	case ttl < time.Second || ttl > MaxDocumentSetTTL || ttl%time.Second != 0:
		err = fmt.Errorf("document set lifetime must be a whole number of seconds from 1s to %s", MaxDocumentSetTTL)
	case len(r.ImportKey) > MaxDocumentSetImportKeyBytes || !utf8.ValidString(r.ImportKey) || strings.ContainsRune(r.ImportKey, 0):
		err = fmt.Errorf("document set import key must be valid text of at most %d bytes", MaxDocumentSetImportKeyBytes)
	case r.MaxMembers < 1 || r.MaxMembers > MaxDocumentSetMembers || len(r.DocumentIDs) > r.MaxMembers:
		err = fmt.Errorf("document set exceeds the membership ceiling of %d", r.MaxMembers)
	}
	return tenant, owner, ttl, err
}

// CreateDocumentSet creates a set in one transaction. Nothing is published when
// any member is unknown, unfinished or not readable by the owner.
func (c *Client) CreateDocumentSet(ctx context.Context, req DocumentSetRequest) (DocumentSet, error) {
	tenant, owner, ttl, err := req.normalized()
	if err != nil {
		return DocumentSet{}, err
	}
	var key *string
	if req.ImportKey != "" {
		key = &req.ImportKey
	}
	documents := req.DocumentIDs
	if documents == nil {
		documents = []string{}
	}
	ctx, cancel := context.WithTimeout(ctx, DocumentSetImportTimeout)
	defer cancel()
	var created DocumentSet
	err = pgx.BeginTxFunc(ctx, c.Pool, pgx.TxOptions{IsoLevel: pgx.ReadCommitted}, func(tx pgx.Tx) error {
		if _, setupErr := tx.Exec(ctx, `SET LOCAL lock_timeout = '5s'; SET LOCAL statement_timeout = '55s'`); setupErr != nil {
			return setupErr
		}
		if schemaErr := bindMetadataSchema(ctx, tx); schemaErr != nil {
			return schemaErr
		}
		return tx.QueryRow(ctx, `
SELECT result_set::text, result_revision::text, result_expires, result_members, result_replayed
FROM grant_catalog_create_document_set($1::uuid,$2::uuid,$3::integer,$4::text,$5::uuid[],$6::integer)`,
			tenant.String(), owner.String(), int32(ttl/time.Second), key, documents, int32(req.MaxMembers)).
			Scan(&created.ID, &created.Revision, &created.ExpiresAt, &created.Members, &created.Replayed)
	})
	if err != nil {
		return DocumentSet{}, documentSetError(err)
	}
	return created, nil
}

// PurgeExpiredDocumentSets removes one bounded step of expired sets and reports
// how many it removed. Repeat it until it returns zero.
func (c *Client) PurgeExpiredDocumentSets(ctx context.Context) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, DocumentSetImportTimeout)
	defer cancel()
	var removed int
	err := pgx.BeginTxFunc(ctx, c.Pool, pgx.TxOptions{IsoLevel: pgx.ReadCommitted}, func(tx pgx.Tx) error {
		if _, setupErr := tx.Exec(ctx, `SET LOCAL lock_timeout = '5s'; SET LOCAL statement_timeout = '55s'`); setupErr != nil {
			return setupErr
		}
		if schemaErr := bindMetadataSchema(ctx, tx); schemaErr != nil {
			return schemaErr
		}
		return tx.QueryRow(ctx, `SELECT grant_catalog_purge_document_sets($1::integer)`, int32(PurgeDocumentSetsStep)).Scan(&removed)
	})
	if err != nil {
		return 0, documentSetError(err)
	}
	return removed, nil
}

func documentSetError(err error) error {
	var postgresErr *pgconn.PgError
	if errors.As(err, &postgresErr) {
		// The catalog raises these fixed messages. Other diagnostics can contain input values.
		switch postgresErr.Code {
		case "QD001", "QD002", "QD003", "QD004", "QD005":
			return fmt.Errorf("document set: %s", postgresErr.Message)
		default:
			return fmt.Errorf("document set failed (SQLSTATE %s)", postgresErr.Code)
		}
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("document set interrupted: %w", err)
	}
	return errors.New("document set interrupted before confirmation")
}
