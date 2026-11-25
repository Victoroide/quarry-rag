// Package main stages, verifies and activates vector generations without
// changing document identities.
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/google/uuid"
)

type action string

const (
	actionInventory action = "inventory"
	actionStage     action = "stage"
	actionVerify    action = "verify"
	actionFreeze    action = "freeze"
	actionThaw      action = "thaw"
	actionActivate  action = "activate"
	actionAttest    action = "attest-source"
)

type options struct {
	Generation           string
	Profile              string
	TargetStore          string
	TargetIndex          string
	Apply                bool
	ConfirmTargetDrained bool

	Verify, Freeze, Thaw, Activate, Attest bool

	After         string
	PruneAfter    string
	MaxDocuments  int
	MaxChunks     int
	MaxBytes      int64
	MaxEmbedCalls int
	Batch         int
	Page          int
	Timeout       time.Duration
	Quiesce       time.Duration

	SampleSize     int
	TopK           int
	ScopeDocuments int
	MinRecall      float64
	MaxP95         time.Duration
	QueryFixtures  string
}

// action selects the operation; without a mode flag, -apply stages and its absence inventories.
func (o options) action() action {
	switch {
	case o.Verify:
		return actionVerify
	case o.Freeze:
		return actionFreeze
	case o.Thaw:
		return actionThaw
	case o.Activate:
		return actionActivate
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
	flags.BoolVar(&opts.ConfirmTargetDrained, "confirm-target-drained", false, "resume staging after independently confirming that an unacknowledged target write can no longer land")
	flags.BoolVar(&opts.Verify, "verify", false, "compare the target with PostgreSQL and run the matched-query sample")
	flags.BoolVar(&opts.Freeze, "freeze", false, "pause admission, job claims, deletions and maintenance")
	flags.BoolVar(&opts.Thaw, "thaw", false, "resume writers")
	flags.BoolVar(&opts.Activate, "activate", false, "make the verified generation the active one")
	flags.BoolVar(&opts.Attest, "attest-source", false, "record the operator-attested profile of the active target")
	flags.StringVar(&opts.After, "after", "", "continue a staging run after this document UUID")
	flags.StringVar(&opts.PruneAfter, "prune-after", "", "continue the frozen rollback target-key scan from this token")
	flags.IntVar(&opts.MaxDocuments, "max-documents", 1000, "maximum documents staged or scanned in one run")
	flags.IntVar(&opts.MaxChunks, "max-chunks", 100000, "maximum chunks staged or verified in one run")
	flags.Int64Var(&opts.MaxBytes, "max-bytes", 128<<20, "maximum chunk text bytes staged or verified in one run")
	flags.IntVar(&opts.MaxEmbedCalls, "max-embed-calls", 0, "maximum embedding provider calls; zero forbids them")
	flags.IntVar(&opts.Batch, "batch", 100, "vectors per target write (1-1000)")
	flags.IntVar(&opts.Page, "page", 100, "documents per source page (1-1000)")
	flags.DurationVar(&opts.Timeout, "timeout", 30*time.Minute, "maximum duration of the run")
	flags.DurationVar(&opts.Quiesce, "quiesce", domain.WriteRequestTimeout, "settling time after the freeze before activation")
	flags.IntVar(&opts.SampleSize, "sample-size", 0, "matched queries to run for acceptance")
	flags.StringVar(&opts.QueryFixtures, "query-fixtures", "", "path to independently computed vectors for different spaces or an unattested legacy source")
	flags.IntVar(&opts.TopK, "top-k", 10, "results compared per matched query (1-100)")
	flags.IntVar(&opts.ScopeDocuments, "scope-documents", 5, "documents in each query's allowed scope")
	flags.Float64Var(&opts.MinRecall, "min-recall", 0, "minimum mean recall at top-k against the current target")
	flags.DurationVar(&opts.MaxP95, "max-p95", 0, "maximum 95th percentile latency of a target query")
	if err := flags.Parse(args); err != nil {
		return opts, err
	}
	declared := map[string]bool{}
	flags.Visit(func(f *flag.Flag) { declared[f.Name] = true })
	return opts, opts.validate(flags.NArg(), declared)
}

func (o options) validate(positional int, declared map[string]bool) error {
	if positional != 0 {
		return errors.New("no positional arguments are accepted")
	}
	modes := 0
	for _, set := range []bool{o.Verify, o.Freeze, o.Thaw, o.Activate, o.Attest} {
		if set {
			modes++
		}
	}
	if modes > 1 {
		return errors.New("choose at most one of -verify, -freeze, -thaw, -activate and -attest-source")
	}
	if err := o.validateBounds(); err != nil {
		return err
	}
	act := o.action()
	if o.QueryFixtures != "" && act != actionVerify {
		return errors.New("-query-fixtures applies only to -verify")
	}
	if (o.ConfirmTargetDrained || o.PruneAfter != "") && act != actionStage {
		return errors.New("-confirm-target-drained and -prune-after apply only to staging with -apply")
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
	switch act {
	case actionInventory, actionStage:
		if o.Generation == "" || o.Profile == "" {
			return errors.New("-generation and -profile are required")
		}
		return o.validateTarget()
	case actionVerify:
		if o.Generation == "" {
			return errors.New("-generation is required")
		}
		return o.validateThresholds(declared)
	case actionFreeze, actionActivate:
		if o.Generation == "" {
			return errors.New("-generation is required")
		}
		if o.Quiesce < domain.WriteRequestTimeout {
			return fmt.Errorf("-quiesce must be at least %s, the longest a deletion request can run", domain.WriteRequestTimeout)
		}
	case actionAttest:
		if o.Profile == "" || o.Generation != "" {
			return errors.New("-attest-source takes -profile and no -generation")
		}
	case actionThaw:
	}
	return nil
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

func (o options) validateThresholds(declared map[string]bool) error {
	for _, name := range []string{"sample-size", "min-recall", "max-p95"} {
		if !declared[name] {
			return fmt.Errorf("-%s must be declared before verification", name)
		}
	}
	switch {
	case o.SampleSize < 1 || o.SampleSize > 100000:
		return errors.New("-sample-size must be between 1 and 100000")
	case !(o.MinRecall > 0 && o.MinRecall <= 1):
		return errors.New("-min-recall must be above 0 and at most 1")
	case o.MaxP95 <= 0:
		return errors.New("-max-p95 must be positive")
	case o.TopK < 1 || o.TopK > 100:
		return errors.New("-top-k must be between 1 and 100")
	case o.ScopeDocuments < 1:
		return errors.New("-scope-documents must be positive")
	}
	return nil
}
