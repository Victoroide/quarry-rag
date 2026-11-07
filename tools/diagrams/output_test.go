package main

import (
	"bytes"
	"encoding/xml"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestStandaloneOutputs(t *testing.T) {
	for _, name := range []string{"system-architecture", "database-schema", "repository-layout"} {
		for _, theme := range []string{"light", "dark"} {
			p := filepath.Join("..", "..", "docs", "diagrams", name+"-"+theme+".svg")
			data, err := readWithin("../..", strings.TrimPrefix(p, "../../"))
			if err != nil {
				t.Error(err)
				continue
			}
			for _, s := range []string{`viewBox=`, `role="img"`, `<title>`, `<desc>`, `<defs>`, `<use `, `@keyframes`, `prefers-reduced-motion`} {
				if !bytes.Contains(data, []byte(s)) {
					t.Errorf("%s missing %s", p, s)
				}
			}
			for _, s := range []string{`<text`, `<script`, `foreignObject`, `<image`, `@font-face`, `<animate`, `filter=`, `<linearGradient`} {
				if bytes.Contains(data, []byte(s)) {
					t.Errorf("%s contains %s", p, s)
				}
			}
			if len(data) > 250000 {
				t.Errorf("%s: %d bytes", p, len(data))
			}
		}
	}
}

func TestCheckRejectsStaleOutput(t *testing.T) {
	root := t.TempDir()
	for p := range directoryDescriptions {
		if err := os.MkdirAll(filepath.Join(root, p), 0o750); err != nil {
			t.Fatal(err)
		}
	}
	paths := []string{"internal/api/router.go"}
	for _, pattern := range []string{"internal/store/postgres/migrations/*.sql", "docs/diagrams/src/*.json", "docs/diagrams/*.svg"} {
		matches, err := filepath.Glob(filepath.Join("..", "..", pattern))
		if err != nil {
			t.Fatal(err)
		}
		for _, p := range matches {
			rel, err := filepath.Rel("../..", p)
			if err != nil {
				t.Fatal(err)
			}
			paths = append(paths, rel)
		}
	}
	for _, rel := range paths {
		b, err := readWithin("../..", rel)
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(root, rel)
		if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
			t.Fatal(err)
		}
		if err := writeWithin(root, rel, b); err != nil {
			t.Fatal(err)
		}
	}
	trackDirectories(t, root, directoryDescriptions)
	if err := generate(root, true); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "docs", "diagrams", "system-architecture-light.svg")
	if err := os.WriteFile(path, []byte("stale"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := generate(root, true); err == nil {
		t.Fatal("stale output accepted")
	}
	if err := generate(root, false); err != nil {
		t.Fatal(err)
	}
	if err := generate(root, true); err != nil {
		t.Fatal(err)
	}
}

func TestAlternativeTextMatchesDescription(t *testing.T) {
	ds, err := buildDiagrams("../..")
	if err != nil {
		t.Fatal(err)
	}
	docs := map[string][]string{"system-architecture": {"README.md", "docs/architecture.md"}, "database-schema": {"docs/architecture.md"}, "repository-layout": {"docs/development.md"}}
	for _, d := range ds {
		for _, path := range docs[d.Name] {
			b, err := readWithin("../..", path)
			if err != nil {
				t.Fatal(err)
			}
			want := `alt="` + d.Description + `" src="`
			if !bytes.Contains(b, []byte(want)) {
				t.Errorf("%s alternative text differs from %s description", path, d.Name)
			}
		}
	}
}

func TestPreferredSizeBudget(t *testing.T) {
	matches, err := filepath.Glob("../../docs/diagrams/*.svg")
	if err != nil {
		t.Fatal(err)
	}
	total := 0
	for _, path := range matches {
		b, err := readWithin("../..", strings.TrimPrefix(path, "../../"))
		if err != nil {
			t.Fatal(err)
		}
		total += len(b)
		if len(b) >= 100000 {
			t.Errorf("%s exceeds 100 KB target: %d", path, len(b))
		}
	}
	if total >= 600000 {
		t.Errorf("combined size %d exceeds 600 KB", total)
	}
}

func TestSVGReferencesAreInternalAndUnique(t *testing.T) {
	files, err := filepath.Glob("../../docs/diagrams/*.svg")
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		b, err := readWithin("../..", strings.TrimPrefix(file, "../../"))
		if err != nil {
			t.Fatal(err)
		}
		decoder := xml.NewDecoder(strings.NewReader(string(b)))
		ids := map[string]bool{}
		var refs []string
		for {
			token, err := decoder.Token()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			start, ok := token.(xml.StartElement)
			if !ok {
				continue
			}
			for _, attr := range start.Attr {
				if attr.Name.Local == "id" {
					if ids[attr.Value] {
						t.Fatalf("duplicate SVG id %s", attr.Value)
					}
					ids[attr.Value] = true
				}
				if attr.Name.Local == "href" {
					if !strings.HasPrefix(attr.Value, "#") {
						t.Fatalf("external reference %s", attr.Value)
					}
					refs = append(refs, strings.TrimPrefix(attr.Value, "#"))
				}
			}
		}
		for _, ref := range refs {
			if !ids[ref] {
				t.Fatalf("unresolved glyph %s in %s", ref, file)
			}
		}
	}
}
