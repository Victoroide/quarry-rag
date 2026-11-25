package unit

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v4"
)

func workflowText(t *testing.T, name string) string {
	t.Helper()
	root, err := os.OpenRoot("../../.github/workflows")
	require.NoError(t, err)
	defer func() { require.NoError(t, root.Close()) }()
	data, err := root.ReadFile(name)
	require.NoError(t, err)
	return string(data)
}

func readWorkflowFile(t *testing.T, name string) map[string]any {
	t.Helper()
	var workflow map[string]any
	require.NoError(t, yaml.Load([]byte(workflowText(t, name)), &workflow))
	return workflow
}

func workflowJob(t *testing.T, workflow map[string]any, name string) map[string]any {
	t.Helper()
	jobs, ok := workflow["jobs"].(map[string]any)
	require.True(t, ok, "expected jobs")
	job, ok := jobs[name].(map[string]any)
	require.True(t, ok, "expected job %s", name)
	return job
}

func workflowSteps(t *testing.T, job map[string]any) []map[string]any {
	t.Helper()
	raw, ok := job["steps"].([]any)
	require.True(t, ok, "expected steps")
	steps := make([]map[string]any, 0, len(raw))
	for _, item := range raw {
		step, ok := item.(map[string]any)
		require.True(t, ok, "expected a step mapping")
		steps = append(steps, step)
	}
	return steps
}

func stepUsing(t *testing.T, steps []map[string]any, action string) map[string]any {
	t.Helper()
	for _, step := range steps {
		if uses, _ := step["uses"].(string); strings.HasPrefix(uses, action+"@") {
			return step
		}
	}
	require.Failf(t, "step missing", "no step uses %s", action)
	return nil
}

func stepIndex(t *testing.T, steps []map[string]any, match func(map[string]any) bool) int {
	t.Helper()
	for index, step := range steps {
		if match(step) {
			return index
		}
	}
	return -1
}

func runs(command string) func(map[string]any) bool {
	return func(step map[string]any) bool {
		run, _ := step["run"].(string)
		return strings.Contains(run, command)
	}
}

func uses(action string) func(map[string]any) bool {
	return func(step map[string]any) bool {
		value, _ := step["uses"].(string)
		return strings.HasPrefix(value, action+"@")
	}
}

func stepInputs(t *testing.T, step map[string]any) map[string]any {
	t.Helper()
	inputs, ok := step["with"].(map[string]any)
	require.True(t, ok, "expected step inputs")
	return inputs
}

const packagesAnalyzed = "env.PACKAGES_ANALYZED != 'true'"

func TestImageJobCompilesPackagesOnlyOutsideSourceAnalysis(t *testing.T) {
	codeql := readCodeQLWorkflow(t)
	triggers, ok := codeql["on"].(map[string]any)
	require.True(t, ok, "expected named workflow triggers")
	for _, event := range []string{"push", "pull_request"} {
		require.Equal(t, map[string]any{"branches": []any{"main"}}, triggers[event], event)
	}

	analysis := workflowSteps(t, workflowJob(t, codeql, "analyze"))
	initialize := stepInputs(t, stepUsing(t, analysis, "github/codeql-action/init"))
	require.Equal(t, "manual", initialize["build-mode"])
	build := stepIndex(t, analysis, runs("go build ./..."))
	require.GreaterOrEqual(t, build, 0, "source analysis must compile every package")
	require.Greater(t, build, stepIndex(t, analysis, uses("github/codeql-action/init")))
	require.Less(t, build, stepIndex(t, analysis, uses("github/codeql-action/analyze")))

	image := workflowJob(t, readWorkflowFile(t, "ci.yml"), "build")
	env, ok := image["env"].(map[string]any)
	require.True(t, ok, "expected job environment")
	require.Equal(t,
		"${{ ((github.event_name == 'push' && github.ref == 'refs/heads/main') || (github.event_name == 'pull_request' && github.base_ref == 'main')) && 'true' || 'false' }}",
		env["PACKAGES_ANALYZED"])

	steps := workflowSteps(t, image)
	hostBuild := stepIndex(t, steps, runs("go build ./..."))
	require.GreaterOrEqual(t, hostBuild, 0, "other events keep the host package build")
	require.Equal(t, packagesAnalyzed, steps[hostBuild]["if"])
	setup := stepIndex(t, steps, uses("actions/setup-go"))
	require.GreaterOrEqual(t, setup, 0)
	require.Less(t, setup, hostBuild)
	require.Equal(t, packagesAnalyzed, steps[setup]["if"])
}

func TestImageJobChecksBothPublishedPlatforms(t *testing.T) {
	steps := workflowSteps(t, workflowJob(t, readWorkflowFile(t, "ci.yml"), "build"))
	build := stepUsing(t, steps, "docker/build-push-action")
	inputs := stepInputs(t, build)
	require.Equal(t, "runtime", inputs["target"])
	require.Equal(t, "linux/amd64,linux/arm64", inputs["platforms"])
	require.NotContains(t, inputs, "push", "pull request images are never published")
	require.Equal(t, "type=oci,dest=${{ runner.temp }}/runtime-image/runtime-image.tar", inputs["outputs"])
	require.Equal(t, "type=gha,scope=ci-runtime", inputs["cache-from"])
	require.Equal(t, "${{ github.event_name == 'push' && 'type=gha,mode=max,scope=ci-runtime' || '' }}", inputs["cache-to"])

	emulation := stepInputs(t, stepUsing(t, steps, "docker/setup-qemu-action"))
	require.Equal(t, "arm64", emulation["platforms"])
	require.Less(t, stepIndex(t, steps, uses("docker/setup-buildx-action")), stepIndex(t, steps, uses("docker/build-push-action")))
	checkArchive := stepIndex(t, steps, runs("--target verify-runtime"))
	require.Greater(t, checkArchive, stepIndex(t, steps, uses("docker/build-push-action")), "the archive is checked after it is built")
	require.Contains(t, steps[checkArchive]["run"], `--build-context "runtime-output=$RUNNER_TEMP/runtime-image"`)

	release := workflowSteps(t, workflowJob(t, readWorkflowFile(t, "release.yml"), "image"))
	published := stepInputs(t, stepUsing(t, release, "docker/build-push-action"))
	require.Equal(t, published["platforms"], inputs["platforms"], "CI checks the platforms the release publishes")
	require.Equal(t, published["target"], inputs["target"])
}

func TestReleaseImageKeepsTrustedBuildChain(t *testing.T) {
	release := workflowText(t, "release.yml")
	require.NotContains(t, release, "ci-runtime", "release builds never read the pull request layer cache")
	require.NotContains(t, release, "cache-from")
	require.NotContains(t, release, "cache-to")

	workflow := readWorkflowFile(t, "release.yml")
	image := workflowJob(t, workflow, "image")
	require.Equal(t, "archives", image["needs"])
	steps := workflowSteps(t, image)
	build := stepUsing(t, steps, "docker/build-push-action")
	inputs := stepInputs(t, build)
	require.Equal(t, true, inputs["push"])
	require.Equal(t, "mode=max", inputs["provenance"])
	require.Equal(t, true, inputs["sbom"])
	require.Equal(t, "${{ steps.metadata.outputs.tags }}", inputs["tags"])
	require.Greater(t, stepIndex(t, steps, uses("actions/attest-build-provenance")), stepIndex(t, steps, uses("docker/build-push-action")))
	require.Equal(t, []any{"archives", "image"}, workflowJob(t, workflow, "publish")["needs"])
}
