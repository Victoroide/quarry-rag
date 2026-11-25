package unit

import (
	"bytes"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/moby/patternmatcher"
	"github.com/moby/patternmatcher/ignorefile"
	"github.com/stretchr/testify/require"
)

// The Dockerfile test stage discovers packages in the build context. Check
// that it contains the root module's Go test files and the repository files
// they name with paths written as string literals.
func TestDockerContextIncludesFilesReadByTests(t *testing.T) {
	root, err := filepath.Abs("../..")
	require.NoError(t, err)
	data, err := os.ReadFile("../../.dockerignore")
	require.NoError(t, err)
	patterns, err := ignorefile.ReadAll(bytes.NewReader(data))
	require.NoError(t, err)

	gaps, err := dockerContextGaps(root, patterns)
	require.NoError(t, err)
	require.Empty(t, gaps, "allow the missing test sources and inputs in .dockerignore")
}

func TestDockerContextGaps(t *testing.T) {
	root := t.TempDir()
	read := "package pkg\n\nimport \"os\"\n\nfunc read() { _, _ = os.ReadFile(%s) }\n"
	for path, body := range map[string]string{
		"docs/guide.md":                "guide",
		"docs/other.md":                "other",
		"internal/pkg/read_test.go":    strings.Replace(read, "%s", `"../../docs/guide.md"`, 1),
		"internal/pkg/missing_test.go": strings.Replace(read, "%s", `"../../docs/absent.md"`, 1),
		"internal/pkg/dir_test.go":     strings.Replace(read, "%s", `"../../docs"`, 1),
		"scripts/tool/read_test.go":    strings.Replace(read, "%s", `"../../docs/guide.md"`, 1),
		"tools/diagrams/go.mod":        "module diagrams\n",
		"tools/diagrams/read_test.go":  strings.Replace(read, "%s", `"../../docs/guide.md"`, 1),
	} {
		full := filepath.Join(root, filepath.FromSlash(path))
		require.NoError(t, os.MkdirAll(filepath.Dir(full), 0o750))
		require.NoError(t, os.WriteFile(full, []byte(body), 0o600))
	}

	const (
		gap        = "internal/pkg/read_test.go reads docs/guide.md"
		scriptsGap = "scripts/tool/read_test.go reads docs/guide.md"
	)
	for _, tt := range []struct {
		name     string
		patterns []string
		want     []string
	}{
		{"nothing excluded", nil, nil},
		{"directory excluded", []string{"docs/"}, []string{gap, scriptsGap}},
		{"file allowed again", []string{"docs/", "!docs/guide.md"}, nil},
		{"another file allowed", []string{"docs/", "!docs/other.md"}, []string{gap, scriptsGap}},
		{"one test file excluded", []string{"internal/pkg/read_test.go"}, []string{"internal/pkg/read_test.go is excluded"}},
		{"tests under scripts excluded", []string{"scripts/", "docs/"}, []string{gap, "scripts/tool/read_test.go is excluded"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := dockerContextGaps(root, tt.patterns)
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

// dockerContextGaps lists test files that the Docker patterns exclude and
// excluded files named by the remaining tests. It skips nested modules, which
// go test ./... does not visit.
func dockerContextGaps(root string, patterns []string) ([]string, error) {
	matcher, err := patternmatcher.New(patterns)
	if err != nil {
		return nil, err
	}
	var gaps []string
	err = filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			if entry.Name() == ".git" {
				return filepath.SkipDir
			}
			if _, statErr := os.Stat(filepath.Join(path, "go.mod")); statErr == nil && path != root {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, "_test.go") {
			return nil
		}
		test, relErr := filepath.Rel(root, path)
		if relErr != nil {
			return relErr
		}
		testExcluded, matchErr := matcher.MatchesOrParentMatches(test)
		if matchErr != nil {
			return matchErr
		}
		if testExcluded {
			gaps = append(gaps, filepath.ToSlash(test)+" is excluded")
			return nil
		}
		files, parseErr := filesNamedBy(root, path)
		if parseErr != nil {
			return parseErr
		}
		for _, name := range files {
			excluded, matchErr := matcher.MatchesOrParentMatches(name)
			if matchErr != nil {
				return matchErr
			}
			if excluded {
				gaps = append(gaps, filepath.ToSlash(test)+" reads "+filepath.ToSlash(name))
			}
		}
		return nil
	})
	slices.Sort(gaps)
	return slices.Compact(gaps), err
}

// filesNamedBy returns the root-relative paths of the existing regular files
// that the string literals of one Go source file name relative to its directory.
func filesNamedBy(root, source string) ([]string, error) {
	parsed, err := parser.ParseFile(token.NewFileSet(), source, nil, 0)
	if err != nil {
		return nil, err
	}
	var files []string
	ast.Inspect(parsed, func(node ast.Node) bool {
		literal, ok := node.(*ast.BasicLit)
		if !ok || literal.Kind != token.STRING {
			return true
		}
		value, err := strconv.Unquote(literal.Value)
		if err != nil || value == "" || filepath.IsAbs(value) || strings.ContainsAny(value, "\n\x00") {
			return true
		}
		candidate := filepath.Join(filepath.Dir(source), filepath.FromSlash(value))
		name, err := filepath.Rel(root, candidate)
		if err != nil || name == ".." || strings.HasPrefix(name, ".."+string(filepath.Separator)) {
			return true
		}
		if info, err := os.Stat(candidate); err == nil && info.Mode().IsRegular() {
			files = append(files, name)
		}
		return true
	})
	return files, nil
}
