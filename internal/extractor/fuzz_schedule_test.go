package extractor

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// TestFuzzTargetsAreScheduled keeps the local and scheduled fuzzing entry
// points in step with the fuzz functions declared by this package.
func TestFuzzTargetsAreScheduled(t *testing.T) {
	const root = "../.."
	workingDirectory, err := filepath.Abs(".")
	if err != nil {
		t.Fatal(err)
	}
	moduleRoot, err := filepath.Abs(root)
	if err != nil {
		t.Fatal(err)
	}
	relative, err := filepath.Rel(moduleRoot, workingDirectory)
	if err != nil {
		t.Fatal(err)
	}
	pkg := "./" + filepath.ToSlash(relative)
	quotedPkg := regexp.QuoteMeta(pkg)

	files, err := filepath.Glob("*_test.go")
	if err != nil {
		t.Fatal(err)
	}
	var targets []string
	for _, file := range files {
		parsed, parseErr := parser.ParseFile(token.NewFileSet(), file, nil, 0)
		if parseErr != nil {
			t.Fatal(parseErr)
		}
		for _, decl := range parsed.Decls {
			if fn, ok := decl.(*ast.FuncDecl); ok && fn.Recv == nil && strings.HasPrefix(fn.Name.Name, "Fuzz") {
				targets = append(targets, fn.Name.Name)
			}
		}
	}
	if len(targets) == 0 {
		t.Fatal("no fuzz targets found")
	}

	makefile, err := os.ReadFile(filepath.Join(root, "Makefile"))
	if err != nil {
		t.Fatal(err)
	}
	workflow, err := os.ReadFile(filepath.Join(root, ".github", "workflows", "fuzz.yml"))
	if err != nil {
		t.Fatal(err)
	}
	for _, target := range targets {
		name := regexp.QuoteMeta(target)
		command := regexp.MustCompile(`(?m)^\tgo test -run='\^\$\$' -fuzz='\^` + name + `\$\$' -fuzztime="\$\(FUZZTIME\)" ` + quotedPkg + `$`)
		if !command.Match(makefile) {
			t.Errorf("Makefile fuzz target does not run %s in %s", target, pkg)
		}
		entry := regexp.MustCompile(`(?m)^\s*- package: ` + quotedPkg + `\n\s*target: ` + name + `$`)
		if !entry.Match(workflow) {
			t.Errorf("fuzz workflow matrix does not run %s in %s", target, pkg)
		}
	}
}
