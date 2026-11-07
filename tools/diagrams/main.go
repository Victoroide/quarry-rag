// Package main maintains the documentation images.
package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
)

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
	total := 0
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
			total += len(b)
		}
	}
	if total >= 600000 {
		return fmt.Errorf("combined SVGs exceed 600 KB: %d", total)
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
