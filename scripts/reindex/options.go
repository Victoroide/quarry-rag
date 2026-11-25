// Package main stages vector generations without changing document identities.
package main

import (
	"errors"
	"flag"
	"io"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/google/uuid"
)

type action string

const (
	actionInventory action = "inventory"
	actionStage     action = "stage"
	actionAttest    action = "attest-source"
)

type options struct {
	Generation  string
	Profile     string
	TargetStore string
	TargetIndex string
	Apply       bool
	Attest      bool

	After         string
	MaxDocuments  int
	MaxChunks     int
	MaxBytes      int64
	MaxEmbedCalls int
	Batch         int
	Page          int
	Timeout       time.Duration
}

// action selects the operation; without a mode flag, -apply stages and its absence inventories.
func (o options) action() action {
	switch {
	case o.Attest:
		return actionAttest
	case o.Apply:
		return actionStage
	default:
		return actionInventory
	}
}

func parseOptions(args []string, output io.Writer) (options, error) {
	var opts options
	flags := flag.NewFlagSet("reindex", flag.ContinueOnError)
	flags.SetOutput(output)
	flags.StringVar(&opts.Generation, "generation", "", "name of the target vector generation")
	flags.StringVar(&opts.Profile, "profile", "", "path of the JSON embedding profile of the target (or of the source with -attest-source)")
	flags.StringVar(&opts.TargetStore, "target-store", "", "store of the target: postgres or pinecone (default: the configured VECTOR_STORE)")
	flags.StringVar(&opts.TargetIndex, "target-index", "", "name of the target Pinecone index")
	flags.BoolVar(&opts.Apply, "apply", false, "permit writes; without it every mode only reads and reports")
	flags.BoolVar(&opts.Attest, "attest-source", false, "record the operator-attested profile of the active target")
	flags.StringVar(&opts.After, "after", "", "continue a staging run after this document UUID")
	flags.IntVar(&opts.MaxDocuments, "max-documents", 1000, "maximum documents staged or scanned in one run")
	flags.IntVar(&opts.MaxChunks, "max-chunks", 100000, "maximum chunks staged in one run")
	flags.Int64Var(&opts.MaxBytes, "max-bytes", 128<<20, "maximum chunk text bytes staged in one run")
	flags.IntVar(&opts.MaxEmbedCalls, "max-embed-calls", 0, "maximum embedding provider calls; zero forbids them")
	flags.IntVar(&opts.Batch, "batch", 100, "vectors per target write (1-1000)")
	flags.IntVar(&opts.Page, "page", 100, "documents per source page (1-1000)")
	flags.DurationVar(&opts.Timeout, "timeout", 30*time.Minute, "maximum duration of the run")
	if err := flags.Parse(args); err != nil {
		return opts, err
	}
	return opts, opts.validate(flags.NArg())
}

func (o options) validate(positional int) error {
	if positional != 0 {
		return errors.New("no positional arguments are accepted")
	}
	if err := o.validateBounds(); err != nil {
		return err
	}
	if o.Generation != "" && !domain.ValidGenerationName(o.Generation) {
		return errors.New("-generation must be 1 to 24 lowercase letters, digits or underscores starting with a letter")
	}
	if o.After != "" {
		id, err := uuid.Parse(o.After)
		if err != nil || id.String() != o.After {
			return errors.New("-after must be a canonical document UUID")
		}
	}
	if o.action() == actionAttest {
		if o.Profile == "" || o.Generation != "" {
			return errors.New("-attest-source takes -profile and no -generation")
		}
		return nil
	}
	if o.Generation == "" || o.Profile == "" {
		return errors.New("-generation and -profile are required")
	}
	return o.validateTarget()
}

func (o options) validateBounds() error {
	switch {
	case o.MaxDocuments < 1 || o.MaxChunks < 1 || o.MaxBytes < 1 || o.MaxEmbedCalls < 0:
		return errors.New("-max-documents, -max-chunks and -max-bytes must be positive and -max-embed-calls must not be negative")
	case o.Batch < 1 || o.Batch > 1000 || o.Page < 1 || o.Page > 1000:
		return errors.New("-batch and -page must be between 1 and 1000")
	case o.Timeout <= 0:
		return errors.New("-timeout must be positive")
	}
	return nil
}

func (o options) validateTarget() error {
	switch o.TargetStore {
	case "", "postgres":
		if o.TargetIndex != "" {
			return errors.New("-target-index applies only to a Pinecone target")
		}
	case "pinecone":
		if o.TargetIndex == "" {
			return errors.New("-target-index is required for a Pinecone target")
		}
	default:
		return errors.New("-target-store must be postgres or pinecone")
	}
	return nil
}
