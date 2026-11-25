package main

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDiscoverMatchesGoSeedEvents(t *testing.T) {
	t.Setenv("GOFLAGS", "")
	t.Setenv("GOWORK", "off")
	root := t.TempDir()
	for name, contents := range map[string]string{
		"go.mod":         "module fixture.test\n\ngo 1.24\n",
		"alpha/alpha.go": "package alpha\n",
		"alpha/internal_test.go": `package alpha
import check "testing"
func Fuzz(f *check.F) { f.Add(1); f.Fuzz(func(t *check.T, i int) {}) }
func FuzzÉlan(f *check.F) { f.Add(1); f.Fuzz(func(t *check.T, i int) {}) }
func Fuzzlower(string) {}
func Fuzzélan(string) {}
type helper struct{}
func (helper) FuzzMethod(*check.F) {}
`,
		"alpha/external_test.go": `package alpha_test
import . "testing"
func FuzzExternal(f *F) { f.Add(1); f.Fuzz(func(t *T, i int) {}) }
`,
		"alpha/testdata/fuzz/FuzzExternal/corpus": "go test fuzz v1\nint(2)\n",
		"alpha/ignored_test.go": `//go:build fixture_disabled

package alpha
import "testing"
func FuzzIgnored(*testing.F) {}
`,
		"beta/beta.go": `package beta
import "testing"
func FuzzNonTestFile(*testing.F) {}
`,
		"beta/beta_test.go": `package beta
import "testing"
func FuzzExternal(f *testing.F) { f.Add(1); f.Fuzz(func(t *testing.T, i int) {}) }
`,
		"gamma/gamma.go": "package gamma\n",
	} {
		path := filepath.Join(root, name)
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o700))
		require.NoError(t, os.WriteFile(path, []byte(contents), 0o600))
	}

	packages, err := discover(t.Context(), root, "")
	require.NoError(t, err)
	require.Equal(t, []fuzzPackage{
		{Path: "fixture.test/alpha", Directory: "alpha", Targets: []string{"Fuzz", "FuzzExternal", "FuzzÉlan"}},
		{Path: "fixture.test/beta", Directory: "beta", Targets: []string{"FuzzExternal"}},
	}, packages)
	selected, err := discover(t.Context(), root, packages[1].Path)
	require.NoError(t, err)
	require.Equal(t, packages[1:], selected)
	_, err = discover(t.Context(), root, "fixture.test/gamma")
	require.ErrorContains(t, err, "no fuzz targets")

	cmd := exec.CommandContext(t.Context(), "go", "test", "-count=1", "-p", "2", "-json", "./...")
	cmd.Dir = root
	output, err := cmd.Output()
	require.NoError(t, err, "%s", output)
	seeds := make(map[string]bool)
	var ranCorpusFile bool
	decoder := json.NewDecoder(bytes.NewReader(output))
	for {
		var event struct{ Action, Package, Test string }
		err := decoder.Decode(&event)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		if event.Action == "pass" && strings.HasPrefix(event.Test, "Fuzz") && strings.Contains(event.Test, "/") {
			target, seed, _ := strings.Cut(event.Test, "/")
			seeds[event.Package+"/"+target] = true
			ranCorpusFile = ranCorpusFile || event.Package == "fixture.test/alpha" && target == "FuzzExternal" && seed == "corpus"
		}
	}
	wantSeeds := make(map[string]bool)
	for _, pkg := range packages {
		for _, target := range pkg.Targets {
			wantSeeds[pkg.Path+"/"+target] = true
		}
	}
	require.Equal(t, wantSeeds, seeds)
	require.True(t, ranCorpusFile, "the regression corpus file did not execute")
}

func TestDiscoverRejectsInvalidOrAmbiguousTargets(t *testing.T) {
	for _, test := range []struct {
		name, declaration, want string
	}{
		{"two parameters", "func FuzzExtra(a, b *testing.F) {}", "invalid signature for FuzzExtra"},
		{"type parameter", "func FuzzGeneric[T any](f *testing.F) {}", "invalid signature for FuzzGeneric"},
		{"duplicate name", "func FuzzValid(f *testing.F) {}", "fuzz target FuzzValid is ambiguous"},
	} {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			require.NoError(t, os.WriteFile(filepath.Join(dir, "internal_test.go"), []byte("package fixture\nimport \"testing\"\nfunc FuzzValid(f *testing.F) {}\n"), 0o600))
			require.NoError(t, os.WriteFile(filepath.Join(dir, "external_test.go"), []byte("package fixture_test\nimport \"testing\"\n"+test.declaration+"\n"), 0o600))
			_, err := discoverTargets(dir, []string{"internal_test.go", "external_test.go"})
			require.ErrorContains(t, err, test.want)
		})
	}
}

func TestFuzzAttemptsEveryTargetAndReturnsEarlierFailures(t *testing.T) {
	if _, err := exec.LookPath("sh"); err != nil {
		t.Skip("the fuzz driver requires a POSIX shell")
	}
	fixture := t.TempDir()
	stubBin, err := filepath.Abs("testdata/bin")
	require.NoError(t, err)
	cmd := exec.CommandContext(t.Context(), "sh", ".github/scripts/fuzz.sh")
	cmd.Dir = "../.."
	cmd.Env = append(cmd.Environ(), "PATH="+stubBin+string(os.PathListSeparator)+os.Getenv("PATH"),
		"FUZZTIME=5s", "FUZZ_PACKAGE=", "FUZZ_TEST_FIXTURE="+fixture)
	output, err := cmd.CombinedOutput()
	var exitErr *exec.ExitError
	require.ErrorAs(t, err, &exitErr)
	require.Equal(t, 1, exitErr.ExitCode(), "%s", output)
	require.Contains(t, string(output), "Fuzz target failed: fixture.test/alpha FuzzFirst")
	var calls []string
	for line := range strings.SplitSeq(string(output), "\n") {
		if call, ok := strings.CutPrefix(line, "call: "); ok {
			calls = append(calls, call)
		}
	}
	require.Equal(t, []string{
		"test -run=^$ -fuzz=^FuzzFirst$ -fuzztime=5s fixture.test/alpha",
		"test -run=^$ -fuzz=^FuzzSecond$ -fuzztime=5s fixture.test/alpha",
		"test -run=^$ -fuzz=^FuzzSecond$ -fuzztime=5s fixture.test/beta",
	}, calls)
	require.FileExists(t, filepath.Join(fixture, "testdata", "fuzz", "FuzzFirst", "failing-input"))
}

func TestFuzzRunsAssignedTargetsWithManualBudget(t *testing.T) {
	if _, err := exec.LookPath("sh"); err != nil {
		t.Skip("the fuzz driver requires a POSIX shell")
	}
	stubBin, err := filepath.Abs("testdata/bin")
	require.NoError(t, err)
	cmd := exec.CommandContext(t.Context(), "sh", ".github/scripts/fuzz.sh")
	cmd.Dir = repositoryRoot
	cmd.Env = append(cmd.Environ(), "PATH="+stubBin+string(os.PathListSeparator)+os.Getenv("PATH"),
		"FUZZTIME=16m", "FUZZ_INVENTORY=fixture.test/alpha\tFuzzA\nfixture.test/alpha\tFuzzB\nfixture.test/alpha\tFuzzC\nfixture.test/alpha\tFuzzD")
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "%s", output)
	require.Equal(t, "call: test -run=^$ -fuzz=^FuzzA$ -fuzztime=16m fixture.test/alpha\n"+
		"call: test -run=^$ -fuzz=^FuzzB$ -fuzztime=16m fixture.test/alpha\n"+
		"call: test -run=^$ -fuzz=^FuzzC$ -fuzztime=16m fixture.test/alpha\n"+
		"call: test -run=^$ -fuzz=^FuzzD$ -fuzztime=16m fixture.test/alpha\n", string(output))
}
