// Command grant-catalog prepares the metadata schema or imports operator entitlements.
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

	"github.com/Victoroide/quarry-rag/internal/store/postgres"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := run(ctx, os.Args[1:], os.Getenv("DATABASE_URL"), os.Stdin, os.Stdout)
	stop()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, databaseURL string, input io.Reader, output io.Writer) error {
	flags := flag.NewFlagSet("grant-catalog", flag.ContinueOnError)
	migrate := flags.Bool("migrate", false, "apply the metadata schema as its owner")
	dryRun := flags.Bool("dry-run", false, "validate the JSON batch from stdin without writing")
	dimensions := flags.Int("vector-dimensions", 0, "prepare pgvector with this dimension when applying migrations")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return errors.New("invalid grant-catalog flags")
	}
	var dimensionSet bool
	flags.Visit(func(option *flag.Flag) {
		if option.Name == "vector-dimensions" {
			dimensionSet = true
		}
	})
	if flags.NArg() != 0 || (*migrate && *dryRun) || (!*migrate && dimensionSet) {
		return errors.New("use -migrate with optional -vector-dimensions, -dry-run, or no flags to apply a batch")
	}
	if dimensionSet {
		if _, err := postgres.ValidateVectorDimensions(*dimensions); err != nil {
			return err
		}
	}
	if databaseURL == "" {
		return errors.New("DATABASE_URL is required")
	}
	var batch postgres.GrantBatch
	if !*migrate {
		prepared, err := readBatch(ctx, input)
		if err != nil {
			return err
		}
		batch = prepared
	}
	databaseCtx, cancel := context.WithTimeout(ctx, postgres.GrantImportTimeout)
	defer cancel()
	client, err := postgres.NewClient(databaseCtx, databaseURL)
	if err != nil {
		return errors.New("cannot connect to the metadata database")
	}
	defer client.Pool.Close()
	if *migrate {
		if migrationErr := client.RunMigrations(ctx); migrationErr != nil {
			return migrationErr
		}
		if dimensionSet {
			_, vectorErr := postgres.NewVectorStore(ctx, client, *dimensions)
			return vectorErr
		}
		return nil
	}
	result, err := client.ImportPreparedGrantBatch(databaseCtx, batch, *dryRun)
	if err != nil {
		return err
	}
	return json.NewEncoder(output).Encode(result)
}

func readBatch(ctx context.Context, input io.Reader) (postgres.GrantBatch, error) {
	type result struct {
		batch postgres.GrantBatch
		err   error
	}
	// A pipe can remain open after cancellation. The command can exit while its reader is parked.
	read := make(chan result, 1)
	go func() {
		batch, err := postgres.PrepareGrantBatch(input)
		read <- result{batch, err}
	}()
	select {
	case <-ctx.Done():
		return postgres.GrantBatch{}, ctx.Err()
	case received := <-read:
		return received.batch, received.err
	}
}
