package main

import (
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v4"
)

const repositoryRoot = "../.."
const fuzzWorkflowPath = repositoryRoot + "/.github/workflows/fuzz.yml"

func TestFuzzWorkflowUsesSharedDriver(t *testing.T) {
	makefile, err := os.ReadFile(filepath.Join(repositoryRoot, "Makefile"))
	require.NoError(t, err)
	require.Regexp(t, `(?m)^fuzz:\n\t[^\n]*sh \.github/scripts/fuzz\.sh$`, string(makefile))

	workflow, err := os.ReadFile(fuzzWorkflowPath)
	require.NoError(t, err)
	require.Contains(t, string(workflow), "run: make fuzz FUZZTIME=5s")
	require.Contains(t, string(workflow), "run: sh .github/scripts/fuzz.sh")
	require.Contains(t, string(workflow), "include: ${{ fromJSON(needs.inventory.outputs.packages) }}")
}

func TestFuzzWorkflowSchedulesManualBudget(t *testing.T) {
	const inventoryCommand = `packages=$(go run ./scripts/fuzz -matrix -fuzztime="$FUZZTIME")
printf 'packages=%s\n' "$packages" >> "$GITHUB_OUTPUT"
`
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("the inventory step requires Bash")
	}
	workflow, err := os.ReadFile(fuzzWorkflowPath)
	require.NoError(t, err)
	var config struct {
		Jobs map[string]struct {
			Timeout string `yaml:"timeout-minutes"`
			Steps   []struct {
				ID  string
				Run string
				Env map[string]string
			}
		}
	}
	require.NoError(t, yaml.Load(workflow, &config))
	require.Equal(t, "${{ matrix.timeout }}", config.Jobs["fuzz"].Timeout, "a fixed job timeout can truncate the per-target budgets")
	var inventoryStep string
	for _, step := range config.Jobs["inventory"].Steps {
		if step.ID == "inventory" {
			inventoryStep = step.Run
			require.Equal(t, "${{ inputs.fuzztime || '2m' }}", step.Env["FUZZTIME"])
		}
	}
	require.Equal(t, inventoryCommand, inventoryStep)
	var driverConfigured bool
	for _, step := range config.Jobs["fuzz"].Steps {
		if step.Run == "sh .github/scripts/fuzz.sh" {
			require.Equal(t, "${{ inputs.fuzztime || '2m' }}", step.Env["FUZZTIME"])
			require.Equal(t, "${{ matrix.inventory }}", step.Env["FUZZ_INVENTORY"])
			driverConfigured = true
		}
	}
	require.True(t, driverConfigured)

	root, err := filepath.Abs(repositoryRoot)
	require.NoError(t, err)
	packages, err := discover(t.Context(), root, "")
	require.NoError(t, err)
	want := make(map[string][]string)
	for _, pkg := range packages {
		want[pkg.Path] = pkg.Targets
	}

	for _, test := range []struct {
		budget    string
		allowance int
	}{
		{budget: "16m", allowance: 20},
		{budget: "2h", allowance: 124},
	} {
		t.Run(test.budget, func(t *testing.T) {
			output, err := os.CreateTemp(t.TempDir(), "github-output-*")
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, output.Close()) })
			cmd := exec.CommandContext(t.Context(), "bash", "-e", "-o", "pipefail", "-c", inventoryCommand)
			cmd.Dir = root
			cmd.Env = append(cmd.Environ(), "FUZZTIME="+test.budget, "GITHUB_OUTPUT="+output.Name())
			log, err := cmd.CombinedOutput()
			require.NoError(t, err, "%s", log)
			data, err := io.ReadAll(output)
			require.NoError(t, err)
			payload, ok := strings.CutPrefix(string(data), "packages=")
			require.True(t, ok, "missing packages output")
			var entries []struct {
				Package   string
				Inventory string
				Timeout   int
			}
			require.NoError(t, json.Unmarshal([]byte(payload), &entries))

			got := make(map[string][]string)
			for _, entry := range entries {
				require.NotEmpty(t, entry.Inventory)
				rows := strings.Split(strings.TrimSuffix(entry.Inventory, "\n"), "\n")
				require.Equal(t, len(rows)*test.allowance, entry.Timeout, "%s", entry.Package)
				require.LessOrEqual(t, entry.Timeout, 360, "%s", entry.Package)
				for _, row := range rows {
					pkg, target, ok := strings.Cut(row, "\t")
					require.True(t, ok, "%q", row)
					require.Equal(t, entry.Package, pkg)
					require.NotEmpty(t, target)
					got[pkg] = append(got[pkg], target)
				}
			}
			require.Equal(t, want, got)
		})
	}
}
