// Package main deletes, inspects, quarantines and releases document
// identities, and performs the explicit activation of checkpointed deletion.
// Positional arguments accept bare UUIDs (addressing the live document) or
// UUID@incarnation (addressing only the observed lifetime).
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/Victoroide/quarry-rag/internal/cleanup"
	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/google/uuid"
)

func main() {
	if err := run(); err != nil {
		slog.Error("delete documents", "error", err)
		os.Exit(1)
	}
}

type action int

const (
	actionDelete action = iota
	actionListHeld
	actionQuarantine
	actionRelease
	actionActivate
)

// activateFlag selects the one-way activation of checkpointed deletion. It is
// exclusive: it takes no document, no other flag and no value.
const activateFlag = "--activate-deletion"

// target is one validated command-line argument.
type target struct {
	id          string
	incarnation string
	// qualified is set when the argument carried an incarnation.
	qualified bool
}

type command struct {
	action          action
	targets         []target
	terminatedSet   bool
	terminatedOwner string
}

func parseCanonicalUUID(value string) (string, error) {
	if len(value) != len(uuid.UUID{}.String()) {
		return "", errors.New("must be one canonical hyphenated UUID")
	}
	parsed, err := uuid.Parse(value)
	if err != nil {
		return "", errors.New("must be one canonical hyphenated UUID")
	}
	return parsed.String(), nil
}

// parseTarget accepts UUID or UUID@incarnation. A separator with an empty,
// repeated or non-canonical incarnation is an error and never an ID-only request.
func parseTarget(arg string) (target, error) {
	id, incarnation, qualified := strings.Cut(arg, "@")
	parsedID, err := uuid.Parse(id)
	if err != nil {
		return target{}, fmt.Errorf("invalid document id: %w", err)
	}
	if !qualified {
		return target{id: parsedID.String()}, nil
	}
	canonicalInc, err := parseCanonicalUUID(incarnation)
	if err != nil {
		return target{}, fmt.Errorf("invalid incarnation: %w", err)
	}
	return target{id: parsedID.String(), incarnation: canonicalInc, qualified: true}, nil
}

func parseTargets(args []string) ([]target, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("provide at least one document UUID or UUID@incarnation")
	}
	targets := make([]target, 0, len(args))
	for _, arg := range args {
		parsed, err := parseTarget(arg)
		if err != nil {
			return nil, err
		}
		targets = append(targets, parsed)
	}
	return targets, nil
}

func parseCommand(args []string) (command, error) {
	if len(args) == 0 {
		return command{}, fmt.Errorf("provide document UUIDs, --list-held, --quarantine, --release-held, or %s", activateFlag)
	}
	if slices.Contains(args, activateFlag) {
		if len(args) != 1 {
			return command{}, fmt.Errorf("%s is an exclusive action and takes no document, flag or value", activateFlag)
		}
		return command{action: actionActivate}, nil
	}
	switch args[0] {
	case "--list-held":
		if len(args) != 1 {
			return command{}, fmt.Errorf("--list-held takes no arguments")
		}
		return command{action: actionListHeld}, nil
	case "--quarantine":
		if len(args) < 2 {
			return command{}, fmt.Errorf("--quarantine requires at least one document UUID")
		}
		targets := make([]target, 0, len(args)-1)
		for _, raw := range args[1:] {
			if strings.HasPrefix(raw, "-") || strings.Contains(raw, "@") {
				return command{}, fmt.Errorf("invalid quarantine document UUID %q", raw)
			}
			canonical, err := parseCanonicalUUID(raw)
			if err != nil {
				return command{}, fmt.Errorf("invalid quarantine document UUID: %w", err)
			}
			targets = append(targets, target{id: canonical})
		}
		return command{action: actionQuarantine, targets: targets}, nil
	case "--release-held":
		return parseReleaseCommand(args[1:])
	default:
		for _, arg := range args {
			if strings.HasPrefix(arg, "-") {
				return command{}, fmt.Errorf("unknown flag %q", arg)
			}
		}
		targets, err := parseTargets(args)
		if err != nil {
			return command{}, err
		}
		return command{action: actionDelete, targets: targets}, nil
	}
}

func parseReleaseCommand(args []string) (command, error) {
	cmd := command{action: actionRelease}
	var positional []string
	for _, arg := range args {
		if value, ok := strings.CutPrefix(arg, "--offline-owner-terminated="); ok {
			if cmd.terminatedSet {
				return command{}, fmt.Errorf("duplicate --offline-owner-terminated flag")
			}
			cmd.terminatedSet = true
			if value != "unknown" {
				canonical, err := parseCanonicalUUID(value)
				if err != nil {
					return command{}, fmt.Errorf("invalid --offline-owner-terminated value: %w", err)
				}
				cmd.terminatedOwner = canonical
			}
			continue
		}
		if strings.HasPrefix(arg, "-") {
			return command{}, fmt.Errorf("invalid flag %q for --release-held", arg)
		}
		positional = append(positional, arg)
	}
	if len(positional) != 2 || strings.Contains(positional[0], "@") || strings.Contains(positional[1], "@") {
		return command{}, fmt.Errorf("--release-held requires <document-uuid> <incarnation-uuid>")
	}
	docID, err := parseCanonicalUUID(positional[0])
	if err != nil {
		return command{}, fmt.Errorf("invalid document UUID: %w", err)
	}
	incarnation, err := parseCanonicalUUID(positional[1])
	if err != nil {
		return command{}, fmt.Errorf("invalid incarnation UUID: %w", err)
	}
	cmd.targets = []target{{id: docID, incarnation: incarnation, qualified: true}}
	return cmd, nil
}

func run() error {
	return runArgs(os.Args[1:], os.Stdout)
}

func runArgs(args []string, stdout io.Writer) error {
	cmd, err := parseCommand(args)
	if err != nil {
		return err
	}
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	var opts []cleanup.Option
	switch cmd.action {
	case actionListHeld, actionQuarantine, actionActivate:
		opts = append(opts, cleanup.WithDirectRecovery(false))
	case actionRelease:
		if cmd.terminatedSet {
			opts = append(opts, cleanup.WithDirectRecovery(true))
		} else if !cfg.FakeMode && cfg.VectorStore == "pinecone" {
			t := cmd.targets[0]
			opts = append(opts, cleanup.WithHostedReleaseTarget(t.id, t.incarnation))
		}
	default:
		// An ID-only request reserves its targets under the activation lock.
		opts = append(opts, cleanup.WithLegacyReservations())
	}
	cleaner, err := cleanup.New(ctx, cfg, opts...)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := cleaner.Close(ctx); closeErr != nil {
			slog.Error("close document stores", "error", closeErr)
		}
	}()

	switch cmd.action {
	case actionActivate:
		activation, activateErr := cleaner.ActivateDeletion(ctx)
		if activateErr != nil {
			return activateErr
		}
		return json.NewEncoder(stdout).Encode(activation)
	case actionListHeld:
		held, listErr := cleaner.ListHeld(ctx)
		if listErr != nil {
			return listErr
		}
		return json.NewEncoder(stdout).Encode(struct {
			Held []domain.HeldDeletion `json:"held"`
		}{Held: held})
	case actionQuarantine:
		quarantined := make([]domain.HeldDeletion, 0, len(cmd.targets))
		for _, t := range cmd.targets {
			entry, qErr := cleaner.Quarantine(ctx, t.id)
			if qErr != nil {
				return fmt.Errorf("quarantine %s: %w", t.id, qErr)
			}
			quarantined = append(quarantined, entry)
		}
		return json.NewEncoder(stdout).Encode(struct {
			Quarantined []domain.HeldDeletion `json:"quarantined"`
		}{Quarantined: quarantined})
	case actionRelease:
		t := cmd.targets[0]
		if cmd.terminatedSet {
			return cleaner.ReleaseTerminatedOfflineHeld(ctx, t.id, t.incarnation, cmd.terminatedOwner)
		}
		return cleaner.ReleaseHeld(ctx, t.id, t.incarnation)
	default:
		for _, t := range cmd.targets {
			var delErr error
			if t.qualified {
				delErr = cleaner.DeleteIncarnation(ctx, t.id, t.incarnation)
			} else {
				delErr = cleaner.Delete(ctx, t.id)
			}
			if delErr != nil {
				return fmt.Errorf("delete %s: %w", t.id, delErr)
			}
		}
		return nil
	}
}
