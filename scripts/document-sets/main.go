// Command document-sets creates subject-bound document sets and removes expired
// ones. A set is an immutable, expiring selector of documents that one
// principal may name in a query. It grants no access.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/store/postgres"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := execute(ctx, os.Args[1:], os.Stdin, os.Stdout)
	stop()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func execute(ctx context.Context, args []string, input io.Reader, output io.Writer) error {
	maxMembers, err := config.LoadDocumentSetMaxMembers()
	if err != nil {
		return err
	}
	return run(ctx, args, os.Getenv("DATABASE_URL"), maxMembers, input, output)
}

func run(ctx context.Context, args []string, databaseURL string, maxMembers int, input io.Reader, output io.Writer) error {
	flags := flag.NewFlagSet("document-sets", flag.ContinueOnError)
	tenant := flags.String("tenant", "", "tenant UUID of the owning principal")
	owner := flags.String("owner", "", "UUID of the principal that owns and may use the set")
	ttl := flags.Duration("ttl", postgres.DefaultDocumentSetTTL, "lifetime of the set, from 1s to 24h")
	importKey := flags.String("import-key", "", "optional key that makes a retry return the original reference")
	purge := flags.Bool("purge-expired", false, "remove one bounded step of expired sets and print how many")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return errors.New("invalid document-sets flags")
	}
	given := map[string]bool{}
	flags.Visit(func(option *flag.Flag) { given[option.Name] = true })
	switch {
	case flags.NArg() != 0:
		return errors.New("use -tenant and -owner with a manifest on standard input, or -purge-expired alone")
	case *purge && len(given) != 1:
		return errors.New("use -purge-expired without other flags")
	case !*purge && (*tenant == "" || *owner == ""):
		return errors.New("-tenant and -owner are required")
	}
	if databaseURL == "" {
		return errors.New("DATABASE_URL is required")
	}
	var request postgres.DocumentSetRequest
	if !*purge {
		documents, err := readManifest(ctx, input, maxMembers)
		if err != nil {
			return err
		}
		request = postgres.DocumentSetRequest{
			TenantID: *tenant, OwnerID: *owner, TTL: *ttl, ImportKey: *importKey,
			DocumentIDs: documents, MaxMembers: maxMembers,
		}
		if err := request.Validate(); err != nil {
			return err
		}
	}
	databaseCtx, cancel := context.WithTimeout(ctx, postgres.DocumentSetImportTimeout)
	defer cancel()
	client, err := postgres.NewClient(databaseCtx, databaseURL)
	if err != nil {
		return errors.New("cannot connect to the metadata database")
	}
	defer client.Pool.Close()
	if *purge {
		removed, purgeErr := purgeExpired(databaseCtx, client)
		if purgeErr != nil {
			return purgeErr
		}
		return json.NewEncoder(output).Encode(map[string]int{"purged": removed})
	}
	created, err := client.CreateDocumentSet(databaseCtx, request)
	if err != nil {
		return err
	}
	return json.NewEncoder(output).Encode(created)
}

// purgeExpired repeats the bounded step until nothing expired remains or the
// command's own deadline ends the run.
func purgeExpired(ctx context.Context, client *postgres.Client) (int, error) {
	total := 0
	for {
		removed, err := client.PurgeExpiredDocumentSets(ctx)
		total += removed
		if err != nil || removed == 0 {
			return total, err
		}
		select {
		case <-ctx.Done():
			return total, ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func readManifest(ctx context.Context, input io.Reader, maxMembers int) ([]string, error) {
	type result struct {
		documents []string
		err       error
	}
	// A pipe can remain open after cancellation. The command can exit while its reader is parked.
	read := make(chan result, 1)
	go func() {
		documents, err := postgres.ReadDocumentSetManifest(input, maxMembers)
		read <- result{documents, err}
	}()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case received := <-read:
		return received.documents, received.err
	}
}
