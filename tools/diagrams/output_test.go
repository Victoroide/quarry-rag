package main

import (
	"bytes"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestStandaloneOutputs(t *testing.T) {
	scheduled := map[string]bool{
		"system-architecture": true,
		"database-schema":     true,
		"repository-layout":   true,
	}
	for _, name := range []string{"system-architecture", "database-schema", "database-operations", "repository-layout"} {
		for _, theme := range []string{"light", "dark"} {
			p := filepath.Join("..", "..", "docs", "diagrams", name+"-"+theme+".svg")
			data, err := readWithin("../..", strings.TrimPrefix(p, "../../"))
			if err != nil {
				t.Error(err)
				continue
			}
			required := []string{`viewBox=`, `role="img"`, `<title>`, `<desc>`, `<defs>`, `<use `}
			if scheduled[name] {
				required = append(required, `@keyframes`, `prefers-reduced-motion`)
			}
			for _, s := range required {
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
	docs := map[string][]string{
		"system-architecture": {"README.md", "docs/architecture.md"},
		"database-schema":     {"docs/architecture.md"},
		"database-operations": {"docs/architecture.md"},
		"repository-layout":   {"docs/development.md"},
	}
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
	outputs := make(map[string][]byte, len(matches))
	for _, path := range matches {
		b, err := readWithin("../..", strings.TrimPrefix(path, "../../"))
		if err != nil {
			t.Fatal(err)
		}
		outputs[filepath.Join("docs", "diagrams", filepath.Base(path))] = b
	}
	if err := checkSizeBudgets(outputs, diagramSizeBudgets, combinedSizeBudget); err != nil {
		t.Fatal(err)
	}
	for scene, limit := range diagramSizeBudgets {
		capLimit := 119999
		if scene == "database-schema" || scene == "database-operations" {
			capLimit = 100000
		}
		if limit > capLimit {
			t.Errorf("%s budget %d exceeds %d cap", scene, limit, capLimit)
		}
	}
	if combinedSizeBudget >= 640000 {
		t.Errorf("combined budget %d must stay below 640000", combinedSizeBudget)
	}
}

func TestSchemaSizeBudgetBoundaries(t *testing.T) {
	const scene = "test-scene"
	const imageLimit = 8
	perScene := map[string]int{scene: imageLimit}
	light := filepath.Join("docs", "diagrams", scene+"-light.svg")
	dark := filepath.Join("docs", "diagrams", scene+"-dark.svg")

	for _, path := range []string{light, dark} {
		t.Run(filepath.Base(path), func(t *testing.T) {
			const combinedLimit = 2*imageLimit + 1
			outputs := map[string][]byte{light: make([]byte, imageLimit), dark: make([]byte, imageLimit)}
			if err := checkSizeBudgets(outputs, perScene, combinedLimit); err != nil {
				t.Fatalf("exact image limit rejected: %v", err)
			}
			outputs[path] = append(outputs[path], 0)
			want := fmt.Sprintf("%s size %d exceeds %d byte budget", path, imageLimit+1, imageLimit)
			if err := checkSizeBudgets(outputs, perScene, combinedLimit); err == nil || err.Error() != want {
				t.Fatalf("one byte over image limit: got %v, want %q", err, want)
			}
		})
	}

	t.Run("combined", func(t *testing.T) {
		const combinedLimit = imageLimit + 2
		outputs := map[string][]byte{
			light: make([]byte, combinedLimit/2),
			dark:  make([]byte, combinedLimit-combinedLimit/2),
		}
		if total := len(outputs[light]) + len(outputs[dark]); total != combinedLimit {
			t.Fatalf("fixture totals %d bytes, want %d", total, combinedLimit)
		}
		if len(outputs[light])+1 > imageLimit || len(outputs[dark]) > imageLimit {
			t.Fatal("fixture would exceed the per-image limit")
		}
		if err := checkSizeBudgets(outputs, perScene, combinedLimit); err != nil {
			t.Fatalf("exact combined limit rejected: %v", err)
		}
		outputs[light] = append(outputs[light], 0)
		want := fmt.Sprintf("combined SVGs size %d exceeds %d byte budget", combinedLimit+1, combinedLimit)
		if err := checkSizeBudgets(outputs, perScene, combinedLimit); err == nil || err.Error() != want {
			t.Fatalf("one byte over combined limit: got %v, want %q", err, want)
		}
	})

	// Stale SVGs in both themes, missing placement, and dangling reference/placement.
	root := t.TempDir()
	for p := range directoryDescriptions {
		if err := os.MkdirAll(filepath.Join(root, p), 0o750); err != nil {
			t.Fatal(err)
		}
	}
	copyPattern := func(pattern string) {
		matches, globErr := filepath.Glob(filepath.Join("..", "..", pattern))
		if globErr != nil {
			t.Fatal(globErr)
		}
		for _, p := range matches {
			rel, relErr := filepath.Rel("../..", p)
			if relErr != nil {
				t.Fatal(relErr)
			}
			b, readErr := readWithin("../..", rel)
			if readErr != nil {
				t.Fatal(readErr)
			}
			dst := filepath.Join(root, rel)
			if mkErr := os.MkdirAll(filepath.Dir(dst), 0o750); mkErr != nil {
				t.Fatal(mkErr)
			}
			if wrErr := writeWithin(root, rel, b); wrErr != nil {
				t.Fatal(wrErr)
			}
		}
	}
	copyPattern("internal/api/router.go")
	copyPattern("internal/store/postgres/migrations/*.sql")
	copyPattern("docs/diagrams/src/*.json")
	copyPattern("docs/diagrams/*.svg")
	trackDirectories(t, root, directoryDescriptions)

	for _, name := range []string{"database-schema-light.svg", "database-schema-dark.svg", "database-operations-light.svg", "database-operations-dark.svg"} {
		rel := filepath.Join("docs", "diagrams", name)
		orig, err := readWithin(root, rel)
		if err != nil {
			t.Fatal(err)
		}
		if err := writeWithin(root, rel, []byte("stale")); err != nil {
			t.Fatal(err)
		}
		if err := generate(root, true); err == nil || !strings.Contains(err.Error(), "stale diagram "+rel) {
			t.Fatalf("checking stale %s: %v", name, err)
		}
		if err := writeWithin(root, rel, orig); err != nil {
			t.Fatal(err)
		}
	}

	opsPath := filepath.Join("docs", "diagrams", "src", "database-operations.json")
	origOps, err := readWithin(root, opsPath)
	if err != nil {
		t.Fatal(err)
	}
	missingPlacement := strings.Replace(string(origOps), `"hosted_vector_revisions":`, `"removed_node":`, 1)
	if err = writeWithin(root, opsPath, []byte(missingPlacement)); err != nil {
		t.Fatal(err)
	}
	if _, err = buildDiagrams(root); err == nil || !strings.Contains(err.Error(), "no placement for hosted_vector_revisions") {
		t.Fatalf("checking missing placement: %v", err)
	}
	danglingPlacement := strings.Replace(string(origOps), `"vector_state": {`, `"dangling_table": {"X": 16, "Y": 16, "W": 100, "H": 56}, "vector_state": {`, 1)
	if err = writeWithin(root, opsPath, []byte(danglingPlacement)); err != nil {
		t.Fatal(err)
	}
	if _, err = buildDiagrams(root); err == nil || !strings.Contains(err.Error(), "obsolete placements") {
		t.Fatalf("checking dangling placement: %v", err)
	}

	f, err := loadFonts()
	if err != nil {
		t.Fatal(err)
	}
	danglingEdge := diagram{
		W: 840, H: 400,
		Nodes: []node{{ID: "documents-ref", Title: "documents.id", Lines: []string{"Document lifecycle"}, Box: box{484, 78, 340, 56}}},
		Edges: []edge{{ID: "dangling", From: "documents-ref", To: "missing_target", Points: []point{{484, 106}, {428, 106}, {428, 200}}}},
	}
	if err := validate(danglingEdge, f); err == nil || !strings.Contains(err.Error(), "missing_target") {
		t.Fatalf("dangling edge target accepted: %v", err)
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
