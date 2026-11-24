package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/Victoroide/quarry-rag/client"
	"github.com/Victoroide/quarry-rag/internal/evaluation"
)

type options struct {
	Live, Create, Delete, IncludeRaw, Instrument bool
	Collection, Targets, Baseline, LedgerDir     string
	Cleanup, Annotate, Reviews                   string
	MaxRequests, K                               int
	MaxTokens                                    uint64
	MaxSpend                                     float64
	PollDeadline, PollInterval, RequestTimeout   time.Duration
}

func parseOptions(args []string, optIn string) (options, error) {
	var o options
	f := flag.NewFlagSet("evidenceeval", flag.ContinueOnError)
	f.BoolVar(&o.Live, "live", false, "compare selected staging deployments")
	f.BoolVar(&o.Create, "create", false, "authorize creating namespaced fixtures")
	f.BoolVar(&o.Delete, "delete", false, "authorize deleting ledger-owned fixtures")
	f.BoolVar(&o.IncludeRaw, "include-snippets", false, "include raw answers, previews and citations for human annotation")
	f.BoolVar(&o.Instrument, "instrument", false, "request staging excerpt observations")
	f.StringVar(&o.Collection, "collection", "", "dedicated collection prefix for new fixtures")
	f.StringVar(&o.Targets, "targets", "", "JSON file describing selected staging deployments")
	f.StringVar(&o.Baseline, "baseline", "", "target name for matched-query comparisons")
	f.StringVar(&o.LedgerDir, "ledger-dir", "", "directory for durable cleanup ledgers")
	f.StringVar(&o.Cleanup, "cleanup", "", "cleanup only, using the selected ledger file")
	f.StringVar(&o.Annotate, "annotate-report", "", "rescore a prior report offline")
	f.StringVar(&o.Reviews, "reviews", "", "JSON file of human labels for the prior report")
	f.IntVar(&o.MaxRequests, "max-requests", 0, "absolute HTTP operation cap across all selected targets")
	f.IntVar(&o.K, "k", defaultK, "number of ranked results considered for recall")
	f.Uint64Var(&o.MaxTokens, "max-tokens", 0, "hard provider token ceiling (0 disables)")
	f.Float64Var(&o.MaxSpend, "max-spend", 0, "hard provider spend ceiling in USD (0 disables)")
	f.DurationVar(&o.PollDeadline, "poll-deadline", 30*time.Second, "maximum indexing and visibility wait per accepted document")
	f.DurationVar(&o.PollInterval, "poll-interval", 500*time.Millisecond, "interval between visibility checks")
	f.DurationVar(&o.RequestTimeout, "request-timeout", 60*time.Second, "timeout per HTTP operation")
	if err := f.Parse(args); err != nil {
		return o, err
	}
	if f.NArg() != 0 || o.K < 1 || o.K > 100 || o.MaxRequests < 0 ||
		math.IsNaN(o.MaxSpend) || math.IsInf(o.MaxSpend, 0) || o.MaxSpend < 0 {
		return o, errors.New("invalid arguments, rank or budget")
	}
	if o.PollDeadline <= 0 || o.PollDeadline > 5*time.Minute || o.PollInterval <= 0 ||
		o.PollInterval > o.PollDeadline || o.RequestTimeout <= 0 || o.RequestTimeout > 2*time.Minute {
		return o, errors.New("invalid polling deadline, interval or request timeout")
	}
	switch {
	case o.Live:
		if o.Cleanup != "" || o.Annotate != "" || o.Reviews != "" || !o.Create || o.Targets == "" ||
			o.LedgerDir == "" || !evaluation.ValidPrefix(o.Collection) {
			return o, errors.New("live evaluation requires --create, --targets, --ledger-dir and a dedicated --collection prefix")
		}
	case o.Cleanup != "":
		if o.Create || !o.Delete || o.Targets == "" || o.Annotate != "" || o.Reviews != "" ||
			o.LedgerDir != "" || o.Collection != "" || o.Instrument || o.Baseline != "" || o.IncludeRaw {
			return o, errors.New("cleanup requires --delete and --targets, and accepts only cleanup and budget settings")
		}
	default:
		if o.Create || o.Delete || o.Targets != "" || o.Collection != "" || o.LedgerDir != "" ||
			o.Instrument || o.Baseline != "" || o.MaxRequests != 0 || o.hardBudget() ||
			(o.Annotate == "") != (o.Reviews == "") || o.K != defaultK {
			return o, errors.New("offline mode accepts --include-snippets or --annotate-report with --reviews")
		}
	}
	if (o.Live || o.Cleanup != "") && (optIn != "1" || o.MaxRequests == 0) {
		return o, errors.New("live requests require RUN_SMOKE=1 and a positive --max-requests")
	}
	return o, nil
}

func (o options) hardBudget() bool { return o.MaxTokens > 0 || o.MaxSpend > 0 }

type deployment struct {
	Backend           string `json:"backend"`
	EmbeddingProvider string `json:"embedding_provider"`
	EmbeddingModel    string `json:"embedding_model"`
	Dimensions        int    `json:"dimensions"`
	AnswerProvider    string `json:"answer_provider"`
	AnswerModels      string `json:"answer_models"`
	Reranker          string `json:"reranker"`
	SettingsRevision  string `json:"settings_revision"`
}

type targetConfig struct {
	Name     string `json:"name"`
	BaseURL  string `json:"base_url"`
	TokenEnv string `json:"token_env"`
	deployment
}

func (t targetConfig) validate() error {
	if !evaluation.ValidPrefix(t.Name) || t.Backend != "postgres" && t.Backend != "pinecone" ||
		t.Dimensions <= 0 || t.TokenEnv != "" && !envName.MatchString(t.TokenEnv) {
		return errors.New("invalid target name, backend, dimensions or token environment variable")
	}
	for _, field := range []string{t.EmbeddingProvider, t.EmbeddingModel, t.AnswerProvider, t.AnswerModels, t.Reranker, t.SettingsRevision} {
		if strings.TrimSpace(field) == "" || len(field) > 200 {
			return errors.New("deployment provider, model, reranker and settings identifiers are required")
		}
	}
	_, err := client.New(t.BaseURL)
	return err
}

var envName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func readTargets(path string) ([]targetConfig, error) {
	var targets []targetConfig
	if err := readJSON(path, &targets); err != nil {
		return nil, fmt.Errorf("read targets: %w", err)
	}
	if len(targets) == 0 || len(targets) > 12 {
		return nil, errors.New("select between 1 and 12 targets")
	}
	seen := make(map[string]bool)
	for i, target := range targets {
		if err := target.validate(); err != nil {
			return nil, fmt.Errorf("target %d: %w", i+1, err)
		}
		if seen[target.Name] {
			return nil, errors.New("target names must be unique")
		}
		seen[target.Name] = true
	}
	return targets, nil
}

func readJSON(path string, dst any) error {
	root, err := os.OpenRoot(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer func() { _ = root.Close() }()
	f, err := root.Open(filepath.Base(path))
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	info, err := f.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return errors.New("JSON input must be a regular file")
	}
	return decodeLimited(f, dst)
}

const maxInputBytes = 8 << 20

func decodeLimited(reader io.Reader, dst any) error {
	body, err := io.ReadAll(io.LimitReader(reader, maxInputBytes+1))
	if err != nil {
		return err
	}
	if len(body) > maxInputBytes {
		return errors.New("JSON input exceeds 8 MiB")
	}
	d := json.NewDecoder(bytes.NewReader(body))
	d.DisallowUnknownFields()
	if err := d.Decode(dst); err != nil {
		return err
	}
	var extra any
	if err := d.Decode(&extra); !errors.Is(err, io.EOF) {
		return errors.New("JSON input contains trailing data")
	}
	return nil
}
