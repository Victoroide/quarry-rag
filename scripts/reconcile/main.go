// Package main reconciles stored vectors and original files with document ownership.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Victoroide/quarry-rag/internal/config"
	pgstore "github.com/Victoroide/quarry-rag/internal/store/postgres"
	s3store "github.com/Victoroide/quarry-rag/internal/store/s3"
	"github.com/google/uuid"
	"github.com/pinecone-io/go-pinecone/v5/pinecone"
)

type options struct {
	Apply bool
	Limit int
	Page  int

	BackfillCollections bool
	After               string

	// SourceUploads selects the object-store mode for journaled originals.
	SourceUploads bool
	MinAge        time.Duration
	Settle        time.Duration
	MaxBytes      int64
	Batch         int
}

const (
	defaultSourceUploadLimit = 1000
	minSettle                = time.Minute
)

func parseOptions(args []string, output io.Writer) (options, error) {
	var opts options
	flags := flag.NewFlagSet("reconcile", flag.ContinueOnError)
	flags.SetOutput(output)
	flags.BoolVar(&opts.Apply, "apply", false, "apply the selected deletions or collection updates")
	flags.BoolVar(&opts.BackfillCollections, "backfill-collections", false, "label existing vectors with their document collection instead of deleting orphans")
	flags.IntVar(&opts.Limit, "limit", 100000, "maximum vectors and namespace vectors selected, documents for collection backfill, or uploads examined (default 1000 with -source-uploads)")
	flags.IntVar(&opts.Page, "page", 100, "vector listing page size (1-1000)")
	flags.StringVar(&opts.After, "after", "", "continue a collection backfill after this document UUID")
	flags.BoolVar(&opts.SourceUploads, "source-uploads", false, "reclaim journaled original files that no document adopted, independent of the vector store")
	flags.DurationVar(&opts.MinAge, "min-age", time.Hour, "with -source-uploads, leave uploads younger than this untouched")
	flags.DurationVar(&opts.Settle, "settle", 30*time.Minute, "with -source-uploads, minimum time between a claim and the settlement of a fully accounted-for write (at least 1m)")
	flags.Int64Var(&opts.MaxBytes, "max-bytes", 1<<30, "with -source-uploads, maximum object bytes removed in one run")
	flags.IntVar(&opts.Batch, "batch", 100, "with -source-uploads, uploads claimed and removed per round (1-1000)")
	if err := flags.Parse(args); err != nil {
		return opts, err
	}
	set := map[string]bool{}
	flags.Visit(func(f *flag.Flag) { set[f.Name] = true })
	if opts.SourceUploads {
		if opts.BackfillCollections || set["after"] || set["page"] {
			return opts, errors.New("-source-uploads cannot be combined with -backfill-collections, -after or -page")
		}
		if !set["limit"] {
			opts.Limit = defaultSourceUploadLimit
		}
		if opts.MinAge < 0 || opts.Settle < minSettle || opts.MaxBytes < 1 || opts.Batch < 1 || opts.Batch > 1000 {
			return opts, errors.New("provide -min-age of zero or more, -settle of at least 1m, -max-bytes greater than zero and -batch between 1 and 1000")
		}
	} else {
		for _, name := range []string{"min-age", "settle", "max-bytes", "batch"} {
			if set[name] {
				return opts, fmt.Errorf("-%s requires -source-uploads", name)
			}
		}
	}
	if flags.NArg() != 0 || opts.Limit < 1 || opts.Page < 1 || opts.Page > 1000 {
		return opts, errors.New("provide -limit greater than zero and -page between 1 and 1000, with no positional arguments")
	}
	if opts.After != "" {
		id, err := uuid.Parse(opts.After)
		if err != nil || !opts.BackfillCollections {
			return opts, errors.New("-after requires -backfill-collections and a document UUID")
		}
		opts.After = id.String()
	}
	return opts, nil
}

func main() {
	if err := run(); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		slog.Error("reconcile", "error", err)
		os.Exit(1)
	}
}

func run() error {
	opts, err := parseOptions(os.Args[1:], os.Stderr)
	if err != nil {
		return err
	}
	if opts.SourceUploads {
		return runSourceUploads(opts)
	}
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	if (!opts.BackfillCollections || opts.Apply) && (cfg.FakeMode || cfg.PineconeAPIKey == "" || cfg.PineconeIndexName == "") {
		return errors.New("reconciliation requires a configured hosted vector index")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, 30*time.Minute)
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
	var collectionDocuments []collectionDocument
	if opts.BackfillCollections {
		collectionDocuments, err = listCollectionDocuments(ctx, pg, opts.After, opts.Limit)
		if err != nil {
			return err
		}
		if !opts.Apply {
			result, backfillErr := backfillCollections(ctx, nil, collectionDocuments, opts)
			if backfillErr != nil {
				return backfillErr
			}
			return json.NewEncoder(os.Stdout).Encode(result)
		}
	}
	pc, err := pinecone.NewClient(pinecone.NewClientParams{ApiKey: cfg.PineconeAPIKey})
	if err != nil {
		return fmt.Errorf("create index client: %w", err)
	}
	startupCtx, startupCancel := context.WithTimeout(ctx, cfg.PineconeTimeout)
	defer startupCancel()
	idx, err := pc.DescribeIndex(startupCtx, cfg.PineconeIndexName)
	if err != nil {
		return fmt.Errorf("describe index: %w", err)
	}
	conn, err := pc.Index(pinecone.NewIndexConnParams{Host: idx.Host})
	if err != nil {
		return fmt.Errorf("connect to index: %w", err)
	}
	defer func() {
		if closeErr := conn.Close(); closeErr != nil {
			slog.Error("close index connection", "error", closeErr)
		}
	}()
	if opts.BackfillCollections {
		result, backfillErr := backfillCollections(ctx, conn, collectionDocuments, opts)
		if backfillErr != nil {
			return backfillErr
		}
		return json.NewEncoder(os.Stdout).Encode(result)
	}
	lookup := func(lookupCtx context.Context, ids []string) (map[string]documentState, error) {
		return lookupDocuments(lookupCtx, pg, ids)
	}
	deleteNamespace := func(deleteCtx context.Context, name string) error {
		return conn.WithNamespace(name).DeleteAllVectorsInNamespace(deleteCtx)
	}
	result, err := reconcile(ctx, conn, deleteNamespace, lookup, opts, os.Stderr)
	if err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(result)
}

func runSourceUploads(opts options) error {
	cfg, err := config.LoadSourceUploadMaintenance()
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, 30*time.Minute)
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
	objects, err := s3store.New(ctx, cfg)
	if err != nil {
		return err
	}
	result, err := reconcileSourceUploads(ctx, pg, objects, cfg.S3Bucket, opts)
	if err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(result)
}

func lookupDocuments(ctx context.Context, pg *pgstore.Client, ids []string) (map[string]documentState, error) {
	canonical := make([]string, 0, len(ids))
	for _, id := range ids {
		if parsed, err := uuid.Parse(id); err == nil {
			canonical = append(canonical, parsed.String())
		}
	}
	states := make(map[string]documentState)
	if len(canonical) == 0 {
		return states, nil
	}
	rows, err := pg.Pool.Query(ctx, `SELECT d.id::text, d.status,
        EXISTS (SELECT 1 FROM ingest_jobs j WHERE j.document_id = d.id)
        FROM documents d WHERE d.id = ANY($1::uuid[])`, canonical)
	if err != nil {
		return nil, fmt.Errorf("look up documents: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		var state documentState
		if err := rows.Scan(&id, &state.Status, &state.HasJob); err != nil {
			return nil, fmt.Errorf("read document state: %w", err)
		}
		states[id] = state
	}
	return states, rows.Err()
}
