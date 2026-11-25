package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Victoroide/quarry-rag/internal/config"
	pgstore "github.com/Victoroide/quarry-rag/internal/store/postgres"
)

func main() {
	if err := run(); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		slog.Error("reindex", "error", err)
		os.Exit(1)
	}
}

func run() error {
	opts, err := parseOptions(os.Args[1:], os.Stderr)
	if err != nil {
		return err
	}
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	if cfg.FakeMode {
		return errors.New("reindex needs a configured vector store; offline fake mode keeps none")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, opts.Timeout)
	defer cancel()
	pg, err := pgstore.NewClient(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := pg.Close(ctx); closeErr != nil {
			slog.Error("close document store", "error", closeErr)
		}
	}()
	if migrationErr := requireMigration(ctx, pg); migrationErr != nil {
		return migrationErr
	}
	return execute(ctx, newDeps(cfg, pg), opts, os.Stdout, 2*time.Second)
}

// requireMigration fails when the schema predates vector generations; the tool never migrates.
func requireMigration(ctx context.Context, pg *pgstore.Client) error {
	var present bool
	if err := pg.Pool.QueryRow(ctx, `SELECT to_regclass('vector_state') IS NOT NULL`).Scan(&present); err != nil {
		return err
	}
	if !present {
		return errors.New("migration 016 is not applied; start a release that contains it before reindexing")
	}
	return nil
}

// execute runs one invocation. Without -apply it only reads.
func execute(ctx context.Context, d deps, opts options, out io.Writer, poll time.Duration) error {
	switch opts.action() {
	case actionThaw:
		return d.thaw(ctx, opts, out)
	case actionAttest:
		return d.attest(ctx, opts, out)
	case actionFreeze:
		return d.freeze(ctx, opts, out, poll)
	case actionActivate:
		return d.activate(ctx, opts, out)
	case actionVerify:
		return d.verify(ctx, opts, d.resolve(opts), out)
	}
	profile, err := d.loadTargetProfile(opts)
	if err != nil {
		return err
	}
	plan := d.resolve(opts)
	if opts.action() == actionStage {
		return d.stage(ctx, opts, plan, profile, out)
	}
	rep, err := d.inventory(ctx, opts, plan, profile)
	if err != nil {
		return err
	}
	return json.NewEncoder(out).Encode(rep)
}
