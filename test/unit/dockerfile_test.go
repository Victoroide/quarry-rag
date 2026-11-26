package unit

import (
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

var dockerStage = regexp.MustCompile(`(?m)^FROM\s+(--platform=\S+\s+)?\S+\s+AS\s+(\S+)`)

func dockerStages(t *testing.T) map[string]string {
	t.Helper()
	data, err := os.ReadFile("../../Dockerfile")
	require.NoError(t, err)
	text := string(data)
	matches := dockerStage.FindAllStringSubmatchIndex(text, -1)
	require.NotEmpty(t, matches)
	stages := map[string]string{}
	for index, match := range matches {
		end := len(text)
		if index+1 < len(matches) {
			end = matches[index+1][0]
		}
		stages[text[match[4]:match[5]]] = text[match[0]:end]
	}
	return stages
}

func TestDockerfileCompilesOnBuildHostForTarget(t *testing.T) {
	stages := dockerStages(t)
	buildBase := stages["build-base"]
	require.Contains(t, buildBase, "FROM --platform=$BUILDPLATFORM ")
	require.Contains(t, buildBase, "COPY go.mod go.sum ./\nRUN go mod download\n")

	builder := stages["builder"]
	require.True(t, strings.HasPrefix(builder, "FROM build-base AS builder\n"))
	require.Contains(t, builder, "ARG TARGETOS\n")
	require.Contains(t, builder, "ARG TARGETARCH\n")
	compiled := 0
	for _, line := range strings.Split(builder, "\n") {
		if !strings.HasPrefix(line, "RUN ") {
			continue
		}
		compiled++
		require.Contains(t, line, "CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH ", line)
	}
	require.Equal(t, 2, compiled, "the binary and the license walk are the builder's only commands")
	require.Contains(t, builder, "go build -o /bin/server ./cmd/server")
	require.Contains(t, builder, "third-party-licenses.sh ./cmd/server /licenses")

	runtime := stages["runtime"]
	require.NotContains(t, strings.SplitN(runtime, "\n", 2)[0], "--platform", "the final filesystem is built for the target")
	require.Contains(t, runtime, "apk add --no-cache ca-certificates tzdata")
	require.Contains(t, runtime, "USER 10001:10001")
	require.Contains(t, runtime, `ENTRYPOINT ["/app/server"]`)
	require.Contains(t, runtime, "COPY --from=builder /licenses /usr/share/licenses/quarry-rag")
}

func TestDockerfileManualChecksUseRequestedPlatform(t *testing.T) {
	stages := dockerStages(t)
	base, _, _ := strings.Cut(stages["base"], "\n")
	require.NotEmpty(t, base)
	require.NotContains(t, base, "--platform", "manual checks inherit the requested platform")
	for _, name := range []string{"test", "lint"} {
		from, _, _ := strings.Cut(stages[name], "\n")
		require.Equal(t, "FROM base AS "+name, from)
	}
}

func TestDockerfileChecksImageAgainstPinnedBuildInputs(t *testing.T) {
	stage := dockerStages(t)["verify-runtime"]
	require.True(t, strings.HasPrefix(stage, "FROM build-base AS verify-runtime\n"))
	require.Contains(t, stage, "COPY . .\n")
	require.Contains(t, stage, "--mount=type=bind,from=runtime-output,target=/image")
	require.Contains(t, stage, ".github/scripts/image-check.sh /image/runtime-image.tar")
}

var (
	dockerFrom     = regexp.MustCompile(`(?m)^FROM\s+(?:--platform=\S+\s+)?(\S+)(?:\s+AS\s+(\S+))?\s*$`)
	dockerFromLine = regexp.MustCompile(`(?m)^FROM\b`)
	dockerPinned   = regexp.MustCompile(`^[a-z0-9][a-z0-9./_-]*:[A-Za-z0-9._-]+@sha256:[0-9a-f]{64}$`)
	dockerGoImages = regexp.MustCompile(`(?m)^FROM\s+(?:--platform=\S+\s+)?(golang:\S+)\s+AS\s+(\S+)`)
)

func TestDockerfileEveryBaseIsPinnedWhereUsed(t *testing.T) {
	data, err := os.ReadFile("../../Dockerfile")
	require.NoError(t, err)
	text := string(data)
	require.NotRegexp(t, `(?m)^ARG\s+\w*IMAGE`, text, "a build argument hides the image from static analysis")

	earlier := map[string]bool{}
	froms := dockerFrom.FindAllStringSubmatch(text, -1)
	require.NotEmpty(t, froms)
	require.Len(t, dockerFromLine.FindAllString(text, -1), len(froms),
		"every FROM line must be a plain image reference with an optional stage name")
	for _, from := range froms {
		ref, stage := from[1], from[2]
		require.False(t, strings.Contains(ref, "$"), "image %q is resolved at build time", ref)
		if !earlier[ref] {
			require.Regexp(t, dockerPinned, ref, "image must be a literal tag@sha256 reference or an earlier stage")
		}
		if stage != "" {
			earlier[stage] = true
		}
	}
}

func TestDockerfileGoStagesShareOnePinnedImage(t *testing.T) {
	data, err := os.ReadFile("../../Dockerfile")
	require.NoError(t, err)
	goImages := dockerGoImages.FindAllStringSubmatch(string(data), -1)
	require.Len(t, goImages, 2, "base and build-base declare the Go image")
	require.Equal(t, "base", goImages[0][2])
	require.Equal(t, "build-base", goImages[1][2])
	require.Equal(t, goImages[0][1], goImages[1][1], "both Go stages use the same image")
	require.Regexp(t, dockerPinned, goImages[0][1])
}

func TestDependabotUpdatesDockerfileImages(t *testing.T) {
	data, err := os.ReadFile("../../.github/dependabot.yml")
	require.NoError(t, err)
	require.Regexp(t, `(?m)^  - package-ecosystem: docker\n    directory: /\n`, string(data))
}
