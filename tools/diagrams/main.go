// Package main maintains the documentation images.
package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

var diagramSizeBudgets = map[string]int{
	"system-architecture": 86000,
	"database-schema":     88000,
	"database-operations": 75000,
	"repository-layout":   101000,
}

const combinedSizeBudget = 638000

func checkSizeBudgets(outputs map[string][]byte, perScene map[string]int, combinedLimit int) error {
	total := 0
	paths := make([]string, 0, len(outputs))
	for path := range outputs {
		paths = append(paths, path)
	}
	slices.Sort(paths)
	for _, path := range paths {
		b := outputs[path]
		base := filepath.Base(path)
		scene := strings.TrimSuffix(strings.TrimSuffix(base, "-light.svg"), "-dark.svg")
		limit, ok := perScene[scene]
		if !ok || (base != scene+"-light.svg" && base != scene+"-dark.svg") {
			return fmt.Errorf("unexpected diagram output %s", path)
		}
		if len(b) > limit {
			return fmt.Errorf("%s size %d exceeds %d byte budget", path, len(b), limit)
		}
		total += len(b)
	}
	scenes := make([]string, 0, len(perScene))
	for scene := range perScene {
		scenes = append(scenes, scene)
	}
	slices.Sort(scenes)
	for _, scene := range scenes {
		for _, theme := range []string{"light", "dark"} {
			want := filepath.Join("docs", "diagrams", scene+"-"+theme+".svg")
			if _, ok := outputs[want]; !ok {
				return fmt.Errorf("missing diagram output %s", want)
			}
		}
	}
	if total > combinedLimit {
		return fmt.Errorf("combined SVGs size %d exceeds %d byte budget", total, combinedLimit)
	}
	return nil
}

func generate(root string, check bool) (err error) {
	rootFS, err := os.OpenRoot(root)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, rootFS.Close()) }()
	f, err := loadFonts()
	if err != nil {
		return err
	}
	ds, err := buildDiagrams(root)
	if err != nil {
		return err
	}
	if err := checkSources(root, ds); err != nil {
		return err
	}
	outputs := map[string][]byte{}
	for _, d := range ds {
		if err := validate(d, f); err != nil {
			return err
		}
		for _, theme := range []string{"light", "dark"} {
			b, err := render(d, theme, f)
			if err != nil {
				return err
			}
			if len(b) > 250000 {
				return fmt.Errorf("%s %s exceeds 250 KB", d.Name, theme)
			}
			outputs[filepath.Join("docs", "diagrams", d.Name+"-"+theme+".svg")] = b
		}
	}
	if err := checkSizeBudgets(outputs, diagramSizeBudgets, combinedSizeBudget); err != nil {
		return err
	}
	for path, b := range outputs {
		if check {
			old, err := rootFS.ReadFile(path)
			if err != nil {
				return err
			}
			if !bytes.Equal(old, b) {
				return fmt.Errorf("stale diagram %s; use make diagrams", path)
			}
		} else {
			if err := rootFS.WriteFile(path, b, 0o600); err != nil {
				return err
			}
		}
	}
	return nil
}
func main() {
	check := flag.Bool("check", false, "Check reproducible output without writing files")
	root := flag.String("root", "../..", "Repository root")
	flag.Parse()
	if err := generate(*root, *check); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
