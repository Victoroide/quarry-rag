package main

import (
	"errors"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
)

type fileScenarioTransport struct {
	status int
	body   string
}

func (transport fileScenarioTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Method != http.MethodGet || req.URL.Path != "/documents/"+guideID+"/file" {
		return nil, errors.New("unexpected simulated file request")
	}
	return &http.Response{StatusCode: transport.status, Body: io.NopCloser(strings.NewReader(transport.body))}, nil
}

func TestFileScenarioDiagnostics(t *testing.T) {
	const childCase = "QUARRY_SIMULATE_TEST_FILE_CASE"
	const marker = "synthetic-download-address"
	cases := []struct {
		name       string
		status     int
		body       string
		diagnostic string
	}{
		{"unexpected_success", http.StatusOK, `{"url":"` + marker + `","expires_in":900}`, "status=200, expected 404"},
		{"malformed_not_found", http.StatusNotFound, `{"url":"` + marker + `"`, "expected 'no file stored'"},
	}

	if selected := os.Getenv(childCase); selected != "" {
		for _, tc := range cases {
			if selected == tc.name {
				guideID = "00000000-0000-4000-8000-000000000001"
				http.DefaultTransport = fileScenarioTransport{status: tc.status, body: tc.body}
				scenario10()
				t.Fatal("expected the file scenario to fail")
			}
		}
		t.Fatal("unknown file scenario fixture")
	}

	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// The simulator exits on a failed scenario. Capture its actual diagnostics.
			cmd := exec.CommandContext(t.Context(), executable, "-test.run=^TestFileScenarioDiagnostics$")
			cmd.Env = append(cmd.Environ(), childCase+"="+tc.name)
			output, err := cmd.CombinedOutput()
			if strings.Contains(string(output), marker) {
				t.Fatal("file scenario printed the download address")
			}
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != 1 {
				t.Fatalf("expected simulator exit 1, got %v", err)
			}
			if !strings.Contains(string(output), tc.diagnostic) {
				t.Fatalf("missing expected file scenario diagnostic: %s", output)
			}
		})
	}
}
