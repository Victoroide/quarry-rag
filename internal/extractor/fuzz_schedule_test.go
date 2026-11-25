package extractor

import (
	"os/exec"
	"slices"
	"strings"
	"testing"
)

func TestFuzzTargetsAreScheduled(t *testing.T) {
	pkg := strings.TrimSpace(fuzzCommandOutput(t, exec.CommandContext(t.Context(), "go", "list", ".")))
	listed := fuzzCommandOutput(t, exec.CommandContext(t.Context(), "go", "test", "-p", "2", "-list", "^Fuzz", "."))
	var want []string
	for line := range strings.SplitSeq(listed, "\n") {
		if strings.HasPrefix(line, "Fuzz") {
			want = append(want, line)
		}
	}
	if len(want) == 0 {
		t.Fatal("go test listed no active fuzz targets")
	}
	slices.Sort(want)

	cmd := exec.CommandContext(t.Context(), "go", "run", "./scripts/fuzz")
	cmd.Dir = "../.."
	inventory := fuzzCommandOutput(t, cmd)
	var got []string
	for line := range strings.SplitSeq(strings.TrimSuffix(inventory, "\n"), "\n") {
		path, target, ok := strings.Cut(line, "\t")
		if !ok || path == "" || target == "" {
			t.Fatalf("invalid inventory entry %q", line)
		}
		if path == pkg {
			got = append(got, target)
		}
	}
	if !slices.Equal(got, want) {
		t.Errorf("scheduled fuzz targets = %q, go test lists %q", got, want)
	}
}

func fuzzCommandOutput(t *testing.T, cmd *exec.Cmd) string {
	t.Helper()
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%s: %v\n%s", cmd, err, output)
	}
	return string(output)
}
