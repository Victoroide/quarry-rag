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
	"github.com/Victoroide/quarry-rag/internal/domain"
	pcstore "github.com/Victoroide/quarry-rag/internal/store/pinecone"
	pgstore "github.com/Victoroide/quarry-rag/internal/store/postgres"
	s3store "github.com/Victoroide/quarry-rag/internal/store/s3"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/pinecone-io/go-pinecone/v5/pinecone"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type options struct {
	Apply bool
	Limit int
	Page  int

	BackfillCollections  bool
	BackfillScoped       bool
	CanonicalDocumentIDs bool
	After                string

	SupersededUnrevisioned bool
	ScanLimit              int
	PageToken              string
	ConfirmTargetDrained   uuid.UUID

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
	flags.BoolVar(&opts.BackfillScoped, "backfill-scoped", false, "verify assigned historical documents and label their current vectors with tenant and incarnation")
	flags.BoolVar(&opts.CanonicalDocumentIDs, "canonical-document-ids", false, "rewrite hosted vector document IDs spelled other than the canonical UUID")
	flags.IntVar(&opts.Limit, "limit", 100000, "maximum vectors and namespace vectors selected, documents examined, or uploads examined (default 1000 with -source-uploads)")
	flags.IntVar(&opts.Page, "page", 100, "vector listing page size (1-1000)")
	flags.StringVar(&opts.After, "after", "", "continue a backfill or canonicalization after this document UUID")
	flags.BoolVar(&opts.SupersededUnrevisioned, "superseded-unrevisioned", false, "scan the active Pinecone index for bare vectors superseded by a manifested revision")
	scanLimit := flags.Int("scan-limit", 100000, "with -superseded-unrevisioned, maximum listed vectors to examine")
	flags.StringVar(&opts.PageToken, "page-token", "", "with -superseded-unrevisioned, resume listing with the reported next_page_token")
	confirmation := flags.String("confirm-target-drained", "", "with -superseded-unrevisioned -apply, reservation UUID of the delete confirmed settled at Pinecone")
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
	if opts.SupersededUnrevisioned {
		opts.ScanLimit = *scanLimit
		if opts.SourceUploads || opts.BackfillCollections || opts.BackfillScoped || opts.CanonicalDocumentIDs || set["after"] {
			return opts, errors.New("-superseded-unrevisioned cannot be combined with other reconciliation modes or -after")
		}
		if opts.ScanLimit < 1 {
			return opts, errors.New("provide -scan-limit greater than zero")
		}
	} else if set["scan-limit"] || set["page-token"] || set["confirm-target-drained"] {
		return opts, errors.New("-scan-limit, -page-token and -confirm-target-drained require -superseded-unrevisioned")
	}
	if set["confirm-target-drained"] {
		if !opts.Apply || set["page-token"] {
			return opts, errors.New("-confirm-target-drained requires -apply and a fresh scan without -page-token")
		}
		id, err := uuid.Parse(*confirmation)
		if err != nil || id == uuid.Nil || id.String() != *confirmation {
			return opts, errors.New("-confirm-target-drained requires the reservation UUID printed for the pending delete")
		}
		opts.ConfirmTargetDrained = id
	}
	if opts.SourceUploads {
		if opts.BackfillCollections || opts.BackfillScoped || opts.CanonicalDocumentIDs || set["after"] || set["page"] {
			return opts, errors.New("-source-uploads cannot be combined with -backfill-collections, -backfill-scoped, -canonical-document-ids, -after or -page")
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
	modes := 0
	if opts.BackfillCollections {
		modes++
	}
	if opts.BackfillScoped {
		modes++
	}
	if opts.CanonicalDocumentIDs {
		modes++
	}
	if modes > 1 {
		return opts, errors.New("choose one reconciliation mode")
	}
	if opts.CanonicalDocumentIDs && set["page"] {
		return opts, errors.New("-page cannot be used with -canonical-document-ids")
	}
	if opts.After != "" {
		id, err := uuid.Parse(opts.After)
		if err != nil || (!opts.BackfillCollections && !opts.BackfillScoped && !opts.CanonicalDocumentIDs) {
			return opts, errors.New("-after requires a backfill or canonicalization mode and a document UUID")
		}
		opts.After = id.String()
	}
	return opts, nil
}

// acquireMaintenanceWriter holds the freeze barrier through an applied pass.
func acquireMaintenanceWriter(ctx context.Context, gate domain.WriterLocker, opts options) (func(), error) {
	if !opts.Apply {
		return func() {}, nil
	}
	release, err := gate.AcquireWriter(ctx)
	if err != nil {
		if errors.Is(err, domain.ErrWritersFrozen) || errors.Is(err, domain.ErrGenerationMismatch) {
			return nil, fmt.Errorf("maintenance writes are paused: %w", err)
		}
		return nil, fmt.Errorf("check document writers: %w", err)
	}
	return release, nil
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

func needsHostedIndex(opts options) bool {
	if opts.SourceUploads {
		return false
	}
	return opts.CanonicalDocumentIDs || (!opts.BackfillCollections && !opts.BackfillScoped) || opts.Apply
}

func validateReconcileConfig(opts options, cfg *config.Config) error {
	if opts.BackfillScoped && cfg.VectorStore == "postgres" {
		return nil
	}
	if needsHostedIndex(opts) {
		if cfg.VectorStore != "pinecone" {
			return errors.New("hosted reconciliation requires Pinecone as the active vector store")
		}
		if cfg.FakeMode || cfg.PineconeAPIKey == "" || cfg.PineconeIndexName == "" {
			return errors.New("reconciliation requires a configured hosted vector index")
		}
	}
	return nil
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
	if cfgErr := validateReconcileConfig(opts, cfg); cfgErr != nil {
		return cfgErr
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
	return reconcileConfigured(ctx, cfg, pg, opts)
}

func runMaintenance(ctx context.Context, gate domain.WriterLocker, opts options, work func() error) error {
	release, err := acquireMaintenanceWriter(ctx, gate, opts)
	if err != nil {
		return err
	}
	defer release()
	return work()
}

func pinAndRunMaintenance(ctx, pinCtx context.Context, cfg *config.Config, pg *pgstore.Client, host string, opts options, work func() error) error {
	if opts.Apply {
		store, target := cfg.VectorTarget()
		if err := pg.PinExistingVectorGeneration(pinCtx, cfg.ActiveVectorGeneration(), store, target, host, cfg.EmbeddingSettings()); err != nil {
			return fmt.Errorf("pin the maintenance target: %w", err)
		}
	}
	return runMaintenance(ctx, pg, opts, work)
}

func runScopedLocal(ctx context.Context, cfg *config.Config, pg *pgstore.Client, vectors scopedVectors, opts options, out io.Writer) error {
	return pinAndRunMaintenance(ctx, ctx, cfg, pg, "", opts, func() error {
		result, backfillErr := backfillScoped(ctx, pg, vectors, opts)
		if backfillErr != nil {
			return backfillErr
		}
		return json.NewEncoder(out).Encode(result)
	})
}

func reconcileConfigured(ctx context.Context, cfg *config.Config, pg *pgstore.Client, opts options) error {
	var err error
	if opts.BackfillScoped && !opts.Apply {
		result, backfillErr := backfillScoped(ctx, pg, nil, opts)
		if backfillErr != nil {
			return backfillErr
		}
		return json.NewEncoder(os.Stdout).Encode(result)
	}
	if opts.BackfillScoped && cfg.VectorStore == "postgres" {
		return runScopedLocal(ctx, cfg, pg, pgvectorVectors{pg}, opts, os.Stdout)
	}
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
	var documentIDs []string
	if opts.CanonicalDocumentIDs {
		documentIDs, err = listDocumentIDs(ctx, pg, opts.After, opts.Limit)
		if err != nil {
			return err
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
	conn, err := pc.Index(pinecone.NewIndexConnParams{Host: idx.Host}, reconcileIndexDialOptions(opts)...)
	if err != nil {
		return fmt.Errorf("connect to index: %w", err)
	}
	defer func() {
		if closeErr := conn.Close(); closeErr != nil {
			slog.Error("close index connection", "error", closeErr)
		}
	}()
	if opts.SupersededUnrevisioned {
		result, runErr := runSuperseded(ctx, cfg, conn.WithNamespace(defaultVectorNamespace), pg, idx.Host, opts, os.Stderr)
		return errors.Join(runErr, json.NewEncoder(os.Stdout).Encode(result))
	}
	return pinAndRunMaintenance(ctx, startupCtx, cfg, pg, idx.Host, opts, func() error {
		return reconcileIndex(ctx, cfg, conn, pg, collectionDocuments, documentIDs, opts)
	})
}

func reconcileIndexDialOptions(opts options) []grpc.DialOption {
	if opts.SupersededUnrevisioned {
		return []grpc.DialOption{grpc.WithChainUnaryInterceptor(validateSupersededReadNamespace)}
	}
	return nil
}

func reconcileIndex(ctx context.Context, cfg *config.Config, conn *pinecone.IndexConnection, pg *pgstore.Client, collectionDocuments []collectionDocument, documentIDs []string, opts options) error {
	if opts.CanonicalDocumentIDs {
		result, canonicalErr := canonicalizeDocumentKeys(ctx, conn, documentIDs, opts)
		if canonicalErr != nil {
			return canonicalErr
		}
		return json.NewEncoder(os.Stdout).Encode(result)
	}
	if opts.BackfillScoped {
		result, backfillErr := backfillScoped(ctx, pg, hostedVectors{conn}, opts)
		if backfillErr != nil {
			return backfillErr
		}
		return json.NewEncoder(os.Stdout).Encode(result)
	}
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
	result, err := reconcileRevisions(ctx, pg, &indexChecker{conn: conn}, conn, deleteNamespace, lookup, opts,
		sweepSettings{DrainDelay: cfg.HostedVectorDrainDelay, WriteFence: cfg.IndexVisibilityMode == "lsn"}, os.Stderr)
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

func isUndefinedTable(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "42P01"
}

type indexChecker struct {
	conn *pinecone.IndexConnection
}

var _ domain.VectorPresenceChecker = (*indexChecker)(nil)

func (c *indexChecker) nsConn(namespace string) *pinecone.IndexConnection {
	if namespace == "" {
		return c.conn
	}
	return c.conn.WithNamespace(namespace)
}

func (c *indexChecker) DeleteVectorIDs(ctx context.Context, namespace string, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	err := c.nsConn(namespace).DeleteVectorsById(ctx, ids)
	if status.Code(err) == codes.NotFound {
		return nil
	}
	return err
}

func (c *indexChecker) FetchPresentVectorIDs(ctx context.Context, namespace string, ids []string) ([]string, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	res, err := c.nsConn(namespace).FetchVectors(ctx, ids)
	if err != nil {
		return nil, err
	}
	if res == nil {
		return nil, nil
	}
	var present []string
	for _, id := range ids {
		if res.Vectors[id] != nil {
			present = append(present, id)
		}
	}
	return present, nil
}

func (c *indexChecker) ListPrefixVectorIDs(ctx context.Context, namespace, prefix string, limit int, token string) (ids []string, nextToken string, supported bool, err error) {
	page := uint32(limit) // #nosec G115 -- limit is bounded by options validation (1-1000).
	req := &pinecone.ListVectorsRequest{Prefix: &prefix, Limit: &page}
	if token != "" {
		req.PaginationToken = &token
	}
	res, listErr := c.nsConn(namespace).ListVectors(ctx, req)
	if listErr != nil {
		if pcstore.IsListUnsupported(listErr) {
			return nil, "", false, nil
		}
		return nil, "", true, listErr
	}
	if res == nil {
		return nil, "", true, nil
	}
	out := make([]string, 0, len(res.VectorIds))
	for _, id := range res.VectorIds {
		if id != nil && *id != "" {
			out = append(out, *id)
		}
	}
	next := ""
	if res.NextPaginationToken != nil {
		next = *res.NextPaginationToken
	}
	return out, next, true, nil
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
        EXISTS (SELECT 1 FROM ingest_jobs j WHERE j.document_id = d.id),
        COALESCE(r.active_revision_id::text, '')
        FROM documents d
        LEFT JOIN hosted_vector_revisions r ON r.document_id = d.id
        WHERE d.id = ANY($1::uuid[])`, canonical)
	if err != nil {
		if !isUndefinedTable(err) {
			return nil, fmt.Errorf("look up documents: %w", err)
		}
		return lookupLegacyDocuments(ctx, pg, canonical)
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		var state documentState
		if scanErr := rows.Scan(&id, &state.Status, &state.HasJob, &state.ActiveRevision); scanErr != nil {
			return nil, fmt.Errorf("read document state: %w", scanErr)
		}
		states[id] = state
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := addManifestRevisions(ctx, pg, canonical, states); err != nil && !isUndefinedTable(err) {
		return nil, err
	}
	return states, nil
}

func lookupLegacyDocuments(ctx context.Context, pg *pgstore.Client, canonical []string) (map[string]documentState, error) {
	states := make(map[string]documentState)
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

func addManifestRevisions(ctx context.Context, pg *pgstore.Client, canonical []string, states map[string]documentState) error {
	pendingRows, err := pg.Pool.Query(ctx, `
		SELECT document_id::text, revision_id::text
		FROM hosted_vector_manifests
		WHERE document_id = ANY($1::uuid[]) AND state = 'writing'`, canonical)
	if err != nil {
		return fmt.Errorf("look up pending vector manifests: %w", err)
	}
	defer pendingRows.Close()
	for pendingRows.Next() {
		var docID, revID string
		if scanErr := pendingRows.Scan(&docID, &revID); scanErr != nil {
			return fmt.Errorf("read pending vector manifest: %w", scanErr)
		}
		st := states[docID]
		if st.PendingRevisions == nil {
			st.PendingRevisions = make(map[string]bool)
		}
		st.PendingRevisions[revID] = true
		states[docID] = st
	}
	if rowsErr := pendingRows.Err(); rowsErr != nil {
		return rowsErr
	}
	cleanupRows, err := pg.Pool.Query(ctx, `
		SELECT document_id::text, revision_id::text
		FROM hosted_vector_cleanups
		WHERE document_id = ANY($1::uuid[])`, canonical)
	if err != nil {
		return fmt.Errorf("look up vector cleanups: %w", err)
	}
	defer cleanupRows.Close()
	for cleanupRows.Next() {
		var docID, revID string
		if scanErr := cleanupRows.Scan(&docID, &revID); scanErr != nil {
			return fmt.Errorf("read vector cleanup: %w", scanErr)
		}
		st := states[docID]
		if st.TrackedRevisions == nil {
			st.TrackedRevisions = make(map[string]bool)
		}
		st.TrackedRevisions[revID] = true
		states[docID] = st
	}
	return cleanupRows.Err()
}
