package postgres

import (
	"context"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type removalKey struct{}

type removalConnection struct {
	client *Client
	conn   *pgxpool.Conn
}

const fixtureDocumentLockKey = "hashtextextended($1::uuid::text, 487011)"

func lockFixtureDocument(ctx context.Context, tx pgx.Tx, id string) error {
	// Use the same UUID lock as the reservation trigger.
	_, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock("+fixtureDocumentLockKey+")", id)
	return err
}

// WithOrdinaryRemoval guards fixture-shaped document IDs through ordinary cleanup.
func (c *Client) WithOrdinaryRemoval(ctx context.Context, id domain.DocumentID, remove func(context.Context) error) error {
	parsed, err := uuid.Parse(string(id))
	if err != nil || parsed.Version() != 5 {
		return remove(ctx)
	}
	conn, err := c.Pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	// Hold the fence over the existing autocommitted removals, including the
	// last vector sweep, without changing their partial failure behavior.
	if _, err = conn.Exec(ctx, "SELECT pg_advisory_lock("+fixtureDocumentLockKey+")", parsed.String()); err != nil {
		discardRemovalConnection(ctx, conn)
		return err
	}
	defer func() {
		unlockCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		var released bool
		if unlockErr := conn.QueryRow(unlockCtx, "SELECT pg_advisory_unlock("+fixtureDocumentLockKey+")", parsed.String()).Scan(&released); unlockErr != nil || !released {
			discardRemovalConnection(ctx, conn)
		}
	}()
	var reserved bool
	if readErr := conn.QueryRow(ctx, `SELECT evaluation_fixture_reserved($1)`, parsed.String()).Scan(&reserved); readErr != nil {
		return readErr
	}
	if reserved {
		return domain.ErrFixtureDeletionRequired
	}
	return remove(context.WithValue(ctx, removalKey{}, removalConnection{client: c, conn: conn}))
}

func discardRemovalConnection(ctx context.Context, conn *pgxpool.Conn) {
	closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	_ = conn.Hijack().Close(closeCtx)
}

func (c *Client) removalSession(ctx context.Context) *pgxpool.Conn {
	if removal, ok := ctx.Value(removalKey{}).(removalConnection); ok && removal.client == c {
		return removal.conn
	}
	return nil
}

func (c *Client) beginRemoval(ctx context.Context) (pgx.Tx, error) {
	if conn := c.removalSession(ctx); conn != nil {
		return conn.Begin(ctx)
	}
	return c.Pool.Begin(ctx)
}

func (c *Client) execRemoval(ctx context.Context, query string, args ...any) (pgconn.CommandTag, error) {
	if conn := c.removalSession(ctx); conn != nil {
		return execWriterSQL(ctx, conn, query, args...)
	}
	return execWriterSQL(ctx, c.Pool, query, args...)
}
