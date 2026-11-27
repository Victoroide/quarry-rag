package pgtest

import (
	"os"
	"slices"
	"strings"
	"testing"

	"go.yaml.in/yaml/v4"
)

type raceJob struct {
	TimeoutMinutes int `yaml:"timeout-minutes"`
	Services       map[string]struct {
		Image string `yaml:"image"`
	} `yaml:"services"`
	Steps []struct {
		Env map[string]string `yaml:"env"`
		Run string            `yaml:"run"`
	} `yaml:"steps"`
}

func TestWorkflowPinsRaceSuiteBudget(t *testing.T) {
	job := loadRaceJob(t)
	if job.TimeoutMinutes != 20 {
		t.Errorf("race job timeout = %d minutes, want 20", job.TimeoutMinutes)
	}
	for _, step := range job.Steps {
		for line := range strings.Lines(step.Run) {
			command, _, _ := strings.Cut(line, "|")
			fields := strings.Fields(command)
			if len(fields) < 2 || fields[0] != "go" || fields[1] != "test" || !slices.Contains(fields, "./...") {
				continue
			}
			for _, arg := range []string{"-race", "-count=1", "-parallel=4", "-timeout=10m", "-json"} {
				if !slices.Contains(fields, arg) {
					t.Errorf("race suite command is missing %s: %s", arg, command)
				}
			}
			if got := step.Env["QUARRY_TEST_SHARED_DB_PARALLEL"]; got != "1" {
				t.Errorf("race suite parallel opt-in = %q, want 1", got)
			}
			return
		}
	}
	t.Error("race job does not run the race suite")
}

func loadRaceJob(t *testing.T) raceJob {
	t.Helper()
	data, err := os.ReadFile("../../../.github/workflows/ci.yml")
	if err != nil {
		t.Fatal(err)
	}
	var workflow struct {
		Jobs struct {
			Test raceJob `yaml:"test"`
		} `yaml:"jobs"`
	}
	if err := yaml.Unmarshal(data, &workflow); err != nil {
		t.Fatalf("parse workflow: %v", err)
	}
	return workflow.Jobs.Test
}

// TestWorkflowServicesUseTheLocalImages keeps the services of the race job on
// the images the helper starts when no service is configured, so that both
// paths test the same servers.
func TestWorkflowServicesUseTheLocalImages(t *testing.T) {
	services := loadRaceJob(t).Services
	if got := services["postgres"].Image; !strings.HasPrefix(got, image+"@sha256:") || len(got) != len(image)+len("@sha256:")+64 {
		t.Errorf("the postgres service uses %q, want %s pinned by a digest", got, image)
	}
	if got := services["postgres"].Image; got != replicationImage {
		t.Errorf("the postgres service uses %q, want %s for replication", got, replicationImage)
	}
	if got := services["pgvector"].Image; got != vectorImage {
		t.Errorf("the pgvector service uses %q, want %q", got, vectorImage)
	}
}

func TestWorkflowRunsReportToolWithRaceDetection(t *testing.T) {
	// The Go package pattern ./... excludes directories starting with a dot.
	for _, step := range loadRaceJob(t).Steps {
		fields := strings.Fields(step.Run)
		if len(fields) >= 2 && fields[0] == "go" && fields[1] == "test" &&
			slices.Contains(fields, "./.github/scripts/testreport") &&
			slices.Contains(fields, "-race") && slices.Contains(fields, "-count=1") {
			return
		}
	}
	t.Error("the race job must explicitly test ./.github/scripts/testreport with -race and -count=1")
}
