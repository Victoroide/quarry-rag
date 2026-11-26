// Package main deletes the explicitly supplied document identifiers. Each
// argument is a bare UUID, which addresses the live document, or UUID@incarnation,
// which addresses only the document lifetime that was observed.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/Victoroide/quarry-rag/internal/cleanup"
	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/google/uuid"
)

func main() {
	if err := run(); err != nil {
		slog.Error("delete documents", "error", err)
		os.Exit(1)
	}
}

// target is one validated command-line argument.
type target struct {
	id          string
	incarnation string
	// qualified is set when the argument carried an incarnation.
	qualified bool
}

// parseTarget accepts UUID or UUID@incarnation. A separator with an empty,
// repeated or non-canonical incarnation is an error and never an ID-only request.
func parseTarget(arg string) (target, error) {
	id, incarnation, qualified := strings.Cut(arg, "@")
	if _, err := uuid.Parse(id); err != nil {
		return target{}, fmt.Errorf("invalid document id: %w", err)
	}
	if !qualified {
		return target{id: id}, nil
	}
	if len(incarnation) != len(uuid.UUID{}.String()) {
		return target{}, errors.New("invalid incarnation: use exactly one canonical UUID after @")
	}
	parsed, err := uuid.Parse(incarnation)
	if err != nil {
		return target{}, errors.New("invalid incarnation: use exactly one canonical UUID after @")
	}
	return target{id: id, incarnation: parsed.String(), qualified: true}, nil
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

func run() error {
	targets, err := parseTargets(os.Args[1:])
	if err != nil {
		return err
	}
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cleaner, err := cleanup.New(ctx, cfg)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := cleaner.Close(ctx); closeErr != nil {
			slog.Error("close document stores", "error", closeErr)
		}
	}()
	for _, t := range targets {
		if t.qualified {
			err = cleaner.DeleteIncarnation(ctx, t.id, t.incarnation)
		} else {
			err = cleaner.Delete(ctx, t.id)
		}
		if err != nil {
			return fmt.Errorf("delete %s: %w", t.id, err)
		}
	}
	return nil
}
