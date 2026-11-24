package unit

import (
	"os"
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v4"
)

func TestTokenizerFuzzTargetIsRegistered(t *testing.T) {
	const target = "FuzzCountOpenAIChatTokens"
	const pkg = "./internal/llm"
	makefile, err := os.ReadFile("../../Makefile")
	require.NoError(t, err)
	assert.Regexp(t, `(?m)^\s+go test .* -fuzz='\^`+target+`\$\$' .* `+regexp.QuoteMeta(pkg)+`$`, string(makefile))

	data, err := os.ReadFile("../../.github/workflows/fuzz.yml")
	require.NoError(t, err)
	var workflow struct {
		Jobs map[string]struct {
			Strategy struct {
				Matrix struct {
					Include []map[string]string `yaml:"include"`
				} `yaml:"matrix"`
			} `yaml:"strategy"`
		} `yaml:"jobs"`
	}
	require.NoError(t, yaml.Load(data, &workflow))
	assert.Contains(t, workflow.Jobs["fuzz"].Strategy.Matrix.Include, map[string]string{"package": pkg, "target": target})
}
