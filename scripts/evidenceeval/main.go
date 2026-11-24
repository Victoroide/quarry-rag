// Command evidenceeval measures scoped retrieval against an opted-in deployment.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"syscall"

	"github.com/Victoroide/quarry-rag/client"
	"github.com/Victoroide/quarry-rag/internal/evaluation"
	"github.com/google/uuid"
)

type settings struct {
	K              int     `json:"k"`
	Mode           string  `json:"retrieval_mode"`
	PollDeadline   string  `json:"poll_deadline,omitempty"`
	PollInterval   string  `json:"poll_interval,omitempty"`
	RequestTimeout string  `json:"request_timeout,omitempty"`
	HTTP           string  `json:"transport,omitempty"`
	Instrument     bool    `json:"instrumented_requested"`
	Raw            bool    `json:"raw_requested"`
	MaxTokens      uint64  `json:"max_provider_tokens,omitempty"`
	MaxSpend       float64 `json:"max_provider_spend_usd,omitempty"`
}

type output struct {
	Version     string       `json:"fixture_version"`
	Settings    settings     `json:"client_settings"`
	HTTP        usage        `json:"http_operations"`
	Baseline    string       `json:"baseline,omitempty"`
	Targets     []report     `json:"targets"`
	Comparisons []comparison `json:"matched_query_comparisons,omitempty"`
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := run(ctx, os.Args[1:])
	stop()
	os.Exit(code)
}

func run(ctx context.Context, args []string) int {
	o, err := parseOptions(args, os.Getenv("RUN_SMOKE"))
	if errors.Is(err, flag.ErrHelp) {
		return 0
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	out, err := evaluate(ctx, o)
	if out.Version != "" {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if encodeErr := enc.Encode(out); encodeErr != nil {
			fmt.Fprintln(os.Stderr, encodeErr)
			return 1
		}
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	return 0
}

func evaluate(ctx context.Context, o options) (output, error) {
	out := output{Version: evaluation.Version, Settings: settings{K: o.K, Raw: o.IncludeRaw, Mode: "global"}}
	switch {
	case o.Annotate != "":
		return annotate(o)
	case !o.Live && o.Cleanup == "":
		out.Targets = []report{replay(o.IncludeRaw)}
		return out, nil
	}
	targets, err := readTargets(o.Targets)
	if err != nil {
		return output{}, err
	}
	return evaluateTargets(ctx, o, out, targets)
}

func evaluateTargets(ctx context.Context, o options, out output, targets []targetConfig) (output, error) {
	out.Settings = settings{K: o.K, Mode: "global", PollDeadline: o.PollDeadline.String(),
		PollInterval: o.PollInterval.String(), RequestTimeout: o.RequestTimeout.String(),
		HTTP: "http1-fresh-connection", Instrument: o.Instrument, Raw: o.IncludeRaw,
		MaxTokens: o.MaxTokens, MaxSpend: o.MaxSpend}
	budget := requestBudget{limit: o.MaxRequests, hardBudget: o.hardBudget()}
	out.HTTP.Cap = budget.limit
	if o.Cleanup != "" {
		if len(targets) != 1 {
			return output{}, errors.New("cleanup requires exactly one target")
		}
		r, err := cleanupTarget(ctx, o, targets[0], &budget)
		out.Targets, out.HTTP.Used = []report{r}, budget.used
		return out, err
	}
	if o.Baseline != "" && !slices.ContainsFunc(targets, func(t targetConfig) bool { return t.Name == o.Baseline }) ||
		o.Baseline == "" && len(targets) > 1 {
		return output{}, errors.New("select one configured --baseline for a multi-target comparison")
	}
	out.Baseline = o.Baseline
	if !budget.hardBudget {
		if err := os.MkdirAll(o.LedgerDir, 0o700); err != nil {
			return output{}, fmt.Errorf("create ledger directory: %w", err)
		}
	}
	var issues []error
	for _, target := range targets {
		r, err := runTarget(ctx, o, target, &budget)
		out.Targets = append(out.Targets, r)
		if err != nil {
			issues = append(issues, fmt.Errorf("target %s: %w", target.Name, err))
		}
	}
	out.HTTP.Used = budget.used
	out.compare()
	return out, errors.Join(issues...)
}

func (out *output) compare() {
	out.Comparisons = nil
	if out.Baseline == "" {
		return
	}
	for _, baseline := range out.Targets {
		if baseline.Target == out.Baseline {
			for _, candidate := range out.Targets {
				if candidate.Target != out.Baseline {
					out.Comparisons = append(out.Comparisons, compare(baseline, candidate))
				}
			}
			return
		}
	}
}

func runTarget(ctx context.Context, o options, target targetConfig, budget *requestBudget) (report, error) {
	r := report{Target: target.Name, Backend: target.Backend, Deployment: &target.deployment}
	startUsed := budget.used
	ns := evaluation.Namespace{Prefix: o.Collection, ID: uuid.New()}
	stopReason := ""
	var runErr error
	if budget.hardBudget {
		stopReason, runErr = "unverifiable_provider_budget", errHardBudget
	}
	if ctx.Err() != nil {
		stopReason, _ = failure(ctx.Err())
		runErr = ctx.Err()
	}
	token, err := target.token()
	if err != nil && stopReason == "" {
		stopReason, runErr = "missing_token", err
	}
	if stopReason == "" {
		c, err := newLiveClient(target, token, budget, o.RequestTimeout)
		if err != nil {
			stopReason, runErr = "client_setup", err
		} else {
			path := filepath.Join(o.LedgerDir, target.Name+"-"+ns.ID.String()+".json")
			l, err := createLedger(path, target, ns)
			if err != nil {
				stopReason, runErr = "ledger_error", err
			} else {
				runErr = runFixture(ctx, c, o, l, &r)
				runErr = errors.Join(runErr, l.Close())
			}
		}
	}
	if stopReason != "" {
		for _, doc := range evaluation.Documents() {
			r.Lifecycle = append(r.Lifecycle, stepResult{Document: doc.Key, Step: "create", Outcome: stopReason})
			r.Visibility = append(r.Visibility, visibility{Document: doc.Key, Outcome: "failed", Reason: stopReason})
		}
		for _, q := range evaluation.Questions() {
			s := newSample(q)
			s.Error = stopReason
			r.Queries = append(r.Queries, s)
		}
	}
	r.HTTP = usage{Cap: budget.limit, Used: budget.used - startUsed}
	r.summarize(true)
	return r, runErr
}

func (t targetConfig) token() (string, error) {
	if t.TokenEnv == "" {
		return "", nil
	}
	token := os.Getenv(t.TokenEnv)
	if token == "" {
		return "", errors.New("configured service token environment variable is empty")
	}
	return token, nil
}

func runFixture(ctx context.Context, c *client.Client, o options, l *ledger, r *report) error {
	r.Ledger = l.path
	ready := make(map[string]bool)
	var failed bool
	for _, doc := range evaluation.Documents() {
		created := submit(ctx, c, l, doc)
		r.Lifecycle = append(r.Lifecycle, created)
		v := visibility{Document: doc.Key, Outcome: "failed", Reason: created.Outcome, Status: created.Status}
		if _, tracked := l.Entries[doc.Key]; tracked {
			v = waitVisible(ctx, c, l, doc, o.PollDeadline, o.PollInterval)
		}
		r.Visibility = append(r.Visibility, v)
		ready[doc.Key] = v.Completed
		failed = failed || v.Outcome != "observed"
	}
	for _, q := range evaluation.Questions() {
		var s sample
		if slices.ContainsFunc(q.Allowed, func(key string) bool { return !ready[key] }) {
			s = newSample(q)
			s.Error = "fixture_unavailable"
		} else {
			s = checkQuery(ctx, c, l.namespace, l.Receipt, q, o)
		}
		r.Queries = append(r.Queries, s)
		failed = failed || s.Error != ""
	}
	if o.Delete {
		for _, step := range cleanup(ctx, c, l) {
			r.Lifecycle = append(r.Lifecycle, step)
			failed = failed || step.Outcome != "deleted" && step.Outcome != "preexisting" && step.Outcome != "retired"
		}
	}
	r.RemainingIDs = l.remainingIDs()
	if failed {
		return errors.New("one or more fixture, query or cleanup checks did not finish successfully")
	}
	return nil
}

func cleanupTarget(ctx context.Context, o options, target targetConfig, budget *requestBudget) (report, error) {
	r := report{Target: target.Name, Backend: target.Backend, Deployment: &target.deployment, Ledger: o.Cleanup}
	r.summarize(true)
	l, err := openLedger(o.Cleanup, target)
	if err != nil {
		return r, err
	}
	defer func() { _ = l.Close() }()
	r.RemainingIDs = l.remainingIDs()
	token, err := target.token()
	if err != nil {
		return r, err
	}
	c, err := newLiveClient(target, token, budget, o.RequestTimeout)
	if err != nil {
		return r, err
	}
	r.Lifecycle = cleanup(ctx, c, l)
	r.HTTP = usage{Cap: budget.limit, Used: budget.used}
	r.RemainingIDs = l.remainingIDs()
	if len(r.RemainingIDs) != 0 {
		return r, errors.New("cleanup incomplete")
	}
	for _, step := range r.Lifecycle {
		if step.Outcome != "deleted" && step.Outcome != "preexisting" && step.Outcome != "retired" {
			return r, errors.New("cleanup encountered an ownership or ledger error")
		}
	}
	return r, nil
}
