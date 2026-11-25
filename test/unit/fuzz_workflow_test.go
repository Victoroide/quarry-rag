package unit

import (
	"os/exec"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTokenizerFuzzTargetIsRegistered(t *testing.T) {
	const target = "FuzzCountOpenAIChatTokens"
	const root = "../.."

	list := exec.CommandContext(t.Context(), "go", "list", "./internal/llm")
	list.Dir = root
	output, err := list.CombinedOutput()
	require.NoError(t, err, "%s", output)
	pkg := strings.TrimSpace(string(output))

	inventory := exec.CommandContext(t.Context(), "go", "run", "./scripts/fuzz")
	inventory.Dir = root
	output, err = inventory.CombinedOutput()
	require.NoError(t, err, "%s", output)
	require.Contains(t, strings.Split(string(output), "\n"), pkg+"\t"+target)
}
