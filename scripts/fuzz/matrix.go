package main

import (
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
	"time"
)

// GitHub-hosted runners end jobs after six hours.
const runnerLimitMinutes = 360

func writeMatrix(w io.Writer, packages []fuzzPackage, fuzztime string) error {
	minutes, err := targetTimeoutMinutes(fuzztime)
	if err != nil {
		return err
	}
	type batch struct {
		Package   string `json:"package"`
		Directory string `json:"directory"`
		Inventory string `json:"inventory"`
		Timeout   int    `json:"timeout"`
	}
	var batches []batch
	for _, pkg := range packages {
		for targets := range slices.Chunk(pkg.Targets, runnerLimitMinutes/minutes) {
			batches = append(batches, batch{
				Package:   pkg.Path,
				Directory: pkg.Directory,
				Inventory: strings.TrimSuffix(formatTargets(pkg.Path, targets), "\n"),
				Timeout:   len(targets) * minutes,
			})
		}
	}
	return json.NewEncoder(w).Encode(batches)
}

func targetTimeoutMinutes(fuzztime string) (int, error) {
	// Preserve the original per-target allowance and reserve time for setup,
	// compilation, seed processing and artifact upload on longer campaigns.
	const originalTimeoutMinutes = 20
	const overhead = 4 * time.Minute
	if count, ok := strings.CutSuffix(fuzztime, "x"); ok {
		if n, err := strconv.Atoi(count); err == nil && n > 0 {
			return originalTimeoutMinutes, nil
		}
	}
	duration, err := time.ParseDuration(fuzztime)
	if err != nil || duration <= 0 {
		return 0, fmt.Errorf("invalid fuzz duration or iteration count %q", fuzztime)
	}
	maximum := runnerLimitMinutes*time.Minute - overhead
	if duration > maximum {
		return 0, fmt.Errorf("fuzz duration %s exceeds the %s per-target limit", duration, maximum)
	}
	minutes := int((duration + overhead + time.Minute - 1) / time.Minute)
	return max(originalTimeoutMinutes, minutes), nil
}
