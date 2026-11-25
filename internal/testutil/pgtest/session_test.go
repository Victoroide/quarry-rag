package pgtest

import (
	"errors"
	"regexp"
	"testing"
	"time"
)

// reaperName matches what the container runtime accepts for the container
// the library names after the session.
var reaperName = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]*$`)

func TestProcessSessionIsAValidReaperName(t *testing.T) {
	id := processSession(4242, time.Unix(1_700_000_000, 123456789))
	name := "reaper_" + id
	if !reaperName.MatchString(name) || len(name) > 128 {
		t.Fatalf("reaper name %q is not accepted by the library", name)
	}
}

func TestProcessSessionDiffersBetweenProcesses(t *testing.T) {
	at := time.Unix(1_700_000_000, 0)
	seen := map[string]bool{}
	for _, id := range []string{
		processSession(4242, at),
		processSession(4243, at),
		processSession(4242, at.Add(time.Nanosecond)),
	} {
		if seen[id] {
			t.Fatalf("session %q repeats", id)
		}
		seen[id] = true
	}
}

func TestIsolateSessionKeepsAConfiguredSession(t *testing.T) {
	var set int
	err := isolateSession(
		func(string) string { return "chosen-by-the-caller" },
		func(string, string) error { set++; return nil },
		4242, time.Unix(1_700_000_000, 0),
	)
	if err != nil || set != 0 {
		t.Fatalf("isolateSession() = %v with %d writes, want nil without writes", err, set)
	}
}

func TestIsolateSessionNamesTheProcessWhenNoneIsConfigured(t *testing.T) {
	at := time.Unix(1_700_000_000, 0)
	var name, value string
	err := isolateSession(
		func(string) string { return "" },
		func(k, v string) error { name, value = k, v; return nil },
		4242, at,
	)
	if err != nil {
		t.Fatalf("isolateSession() = %v", err)
	}
	if name != sessionEnv || value != processSession(4242, at) {
		t.Fatalf("isolateSession() wrote %s=%q, want %s=%q", name, value, sessionEnv, processSession(4242, at))
	}
}

func TestIsolateSessionReportsAFailedWrite(t *testing.T) {
	failure := errors.New("environment is read-only")
	err := isolateSession(
		func(string) string { return "" },
		func(string, string) error { return failure },
		4242, time.Unix(1_700_000_000, 0),
	)
	if !errors.Is(err, failure) {
		t.Fatalf("isolateSession() = %v, want the write error", err)
	}
}
