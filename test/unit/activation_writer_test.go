package unit

import (
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
	"testing/fstest"
)

// goSources lists the non-test Go files of the repository, relative to its root.
func goSources(t *testing.T, repository fs.FS) map[string]string {
	t.Helper()
	sources := map[string]string{}
	err := fs.WalkDir(repository, ".", func(name string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			if base := entry.Name(); base == ".git" || base == "vendor" || base == "node_modules" {
				return fs.SkipDir
			}
			if name != "." {
				// A nested checkout has its own repository, even when its
				// .git entry is a worktree pointer file.
				_, statErr := fs.Stat(repository, path.Join(name, ".git"))
				switch {
				case statErr == nil:
					return fs.SkipDir
				case !os.IsNotExist(statErr):
					return statErr
				}
			}
			return nil
		}
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			return nil
		}
		data, readErr := fs.ReadFile(repository, name)
		if readErr != nil {
			return readErr
		}
		sources[name] = string(data)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return sources
}

func TestGoSourcesSkipsNestedCheckouts(t *testing.T) {
	sources := goSources(t, fstest.MapFS{
		".git":                  {},
		"main.go":               {Data: []byte("package main")},
		"copy/.git/config":      {},
		"copy/main.go":          {},
		"worktree/.git":         {},
		"worktree/main.go":      {},
		"tools/diagrams/go.mod": {},
		"tools/diagrams/run.go": {Data: []byte("package diagrams")},
	})
	if len(sources) != 2 || sources["main.go"] != "package main" || sources["tools/diagrams/run.go"] != "package diagrams" {
		t.Fatalf("unexpected source files: %v", sources)
	}
}

// TestOnlyTheAdministrativeCommandActivatesDeletion keeps the activation marker
// read-only for every process that serves traffic, deletes documents or runs
// tests. A new writer, or a new caller of the activation operation, fails here
// until it is reviewed as the explicit transition it would be.
func TestOnlyTheAdministrativeCommandActivatesDeletion(t *testing.T) {
	root, err := os.OpenRoot(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if closeErr := root.Close(); closeErr != nil {
			t.Error(closeErr)
		}
	}()
	sources := goSources(t, root.FS())
	if len(sources) < 50 {
		t.Fatalf("found only %d Go sources; the walk does not cover the repository", len(sources))
	}
	write := regexp.MustCompile(`(?i)(UPDATE|INSERT\s+INTO|DELETE\s+FROM|TRUNCATE(\s+TABLE)?)\s+document_deletion_activation`)
	call := regexp.MustCompile(`\.ActivateDeletion\(`)
	var writers, callers []string
	for path, source := range sources {
		if write.MatchString(source) {
			writers = append(writers, path)
		}
		if call.MatchString(source) {
			callers = append(callers, path)
		}
	}
	slices.Sort(writers)
	slices.Sort(callers)
	if want := []string{"internal/store/postgres/deletion_activation.go"}; !slices.Equal(writers, want) {
		t.Errorf("code that writes the activation marker: %v, want %v", writers, want)
	}
	if want := []string{"internal/cleanup/helper.go", "scripts/cleanup/main.go"}; !slices.Equal(callers, want) {
		t.Errorf("callers of the activation operation: %v, want %v", callers, want)
	}
}
