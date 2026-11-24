package unit

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v4"
)

func readCodeQLWorkflow(t *testing.T) map[string]any {
	t.Helper()
	data, err := os.ReadFile("../../.github/workflows/codeql.yml")
	require.NoError(t, err)
	var workflow map[string]any
	require.NoError(t, yaml.Load(data, &workflow))
	return workflow
}

func TestCodeQLConcurrency(t *testing.T) {
	workflow := readCodeQLWorkflow(t)
	require.Equal(t, map[string]any{
		"group":              "${{ github.workflow }}-${{ github.event_name }}-${{ github.event.pull_request.number || github.ref }}",
		"cancel-in-progress": "${{ github.event_name == 'pull_request' }}",
	}, workflow["concurrency"])
}

func TestCodeQLTriggers(t *testing.T) {
	workflow := readCodeQLWorkflow(t)
	triggers, ok := workflow["on"].(map[string]any)
	require.True(t, ok, "expected named workflow triggers")
	for _, event := range []string{"push", "pull_request"} {
		require.Equal(t, map[string]any{"branches": []any{"main"}}, triggers[event], event)
	}
	require.Equal(t, []any{map[string]any{"cron": "31 4 * * *"}}, triggers["schedule"])
}
