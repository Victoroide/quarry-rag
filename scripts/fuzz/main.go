// Command fuzz lists the Go fuzz targets in the root module.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"
)

type fuzzPackage struct {
	Path      string
	Directory string
	Targets   []string
}

func main() {
	matrix := flag.Bool("matrix", false, "print JSON matrix entries for scheduled fuzzing")
	fuzztime := flag.String("fuzztime", "2m", "per-target fuzz budget for matrix scheduling")
	packagePath := flag.String("package", "", "list only this Go package's targets")
	flag.Parse()
	if err := run(context.Background(), *matrix, *fuzztime, *packagePath); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, matrix bool, fuzztime, packagePath string) error {
	root, err := os.Getwd()
	if err != nil {
		return err
	}
	packages, err := discover(ctx, root, packagePath)
	if err != nil {
		return err
	}
	if matrix {
		return writeMatrix(os.Stdout, packages, fuzztime)
	}
	for _, pkg := range packages {
		if _, err := os.Stdout.WriteString(formatTargets(pkg.Path, pkg.Targets)); err != nil {
			return err
		}
	}
	return nil
}

func formatTargets(path string, targets []string) string {
	var inventory strings.Builder
	for _, target := range targets {
		inventory.WriteString(path)
		inventory.WriteByte('\t')
		inventory.WriteString(target)
		inventory.WriteByte('\n')
	}
	return inventory.String()
}

func discover(ctx context.Context, root, packagePath string) ([]fuzzPackage, error) {
	cmd := exec.CommandContext(ctx, "go", "list", "-json=Dir,ImportPath,TestGoFiles,XTestGoFiles", "./...")
	cmd.Dir = root
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("go list ./...: %w\n%s", err, stderr.String())
	}

	var packages []fuzzPackage
	decoder := json.NewDecoder(bytes.NewReader(output))
	for {
		var pkg struct {
			Dir, ImportPath           string
			TestGoFiles, XTestGoFiles []string
		}
		if err := decoder.Decode(&pkg); err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("read go list output: %w", err)
		}
		if packagePath != "" && pkg.ImportPath != packagePath {
			continue
		}
		targets, err := discoverTargets(pkg.Dir, append(pkg.TestGoFiles, pkg.XTestGoFiles...))
		if err != nil {
			return nil, err
		}
		if len(targets) == 0 {
			continue
		}
		directory, err := filepath.Rel(root, pkg.Dir)
		if err != nil || !filepath.IsLocal(directory) {
			return nil, fmt.Errorf("package %s is outside %s", pkg.ImportPath, root)
		}
		packages = append(packages, fuzzPackage{Path: pkg.ImportPath, Directory: filepath.ToSlash(directory), Targets: targets})
	}
	if len(packages) == 0 {
		if packagePath != "" {
			return nil, fmt.Errorf("no fuzz targets in %s", packagePath)
		}
		return nil, errors.New("no fuzz targets in the root module")
	}
	slices.SortFunc(packages, func(a, b fuzzPackage) int { return strings.Compare(a.Path, b.Path) })
	return packages, nil
}

func discoverTargets(directory string, files []string) ([]string, error) {
	fset := token.NewFileSet()
	seen := make(map[string]bool)
	var targets []string
	for _, file := range files {
		src, err := parser.ParseFile(fset, filepath.Join(directory, file), nil, parser.SkipObjectResolution)
		if err != nil {
			return nil, err
		}
		for _, decl := range src.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Recv != nil || !isFuzzName(fn.Name.Name) {
				continue
			}
			name := fn.Name.Name
			if !isFuzzSignature(fn.Type) {
				return nil, fmt.Errorf("%s: invalid signature for %s", fset.Position(fn.Pos()), name)
			}
			if seen[name] {
				return nil, fmt.Errorf("%s: fuzz target %s is ambiguous within the package", fset.Position(fn.Pos()), name)
			}
			seen[name] = true
			targets = append(targets, name)
		}
	}
	slices.Sort(targets)
	return targets, nil
}

// Match the naming and syntactic signature rules used by go test's loader.
func isFuzzName(name string) bool {
	suffix, ok := strings.CutPrefix(name, "Fuzz")
	first, _ := utf8.DecodeRuneInString(suffix)
	return ok && !unicode.IsLower(first)
}

func isFuzzSignature(fn *ast.FuncType) bool {
	if fn.Results.NumFields() != 0 || fn.TypeParams.NumFields() != 0 || fn.Params.NumFields() != 1 {
		return false
	}
	ptr, ok := fn.Params.List[0].Type.(*ast.StarExpr)
	if !ok {
		return false
	}
	switch typ := ptr.X.(type) {
	case *ast.Ident:
		return typ.Name == "F"
	case *ast.SelectorExpr:
		return typ.Sel.Name == "F"
	default:
		return false
	}
}
