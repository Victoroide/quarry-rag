package main

import (
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
)

func TestSpillJSONLResourceLimits(t *testing.T) {
	const recordLimit = 4 * 1024 * 1024
	const nameLimit = 8 * 1024
	valid := `{"Action":"pass","Package":"a"}`
	exact := `{"ignored":"` + strings.Repeat("x", recordLimit-len(`{"ignored":""}`)-2) + "\"}\r\n"
	longName := strings.Repeat("x", nameLimit)
	for _, tc := range []struct {
		name, input, reason string
	}{
		{name: "CRLF, blank and final record", input: " \r\n\t\n" + valid},
		{name: "escaped newline and unknown field", input: `{"ignored":"a\nb"}`},
		{name: "exact record", input: exact},
		{name: "oversized record", input: " " + exact, reason: "record limit"},
		{name: "oversized record without newline", input: strings.Repeat(" ", recordLimit+1), reason: "record limit"},
		{name: "exact name", input: `{"Action":"pass","Package":"` + longName + `"}`},
		{name: "oversized package", input: `{"Action":"pass","Package":"` + longName + `x"}`, reason: "name limit"},
		{name: "oversized build", input: `{"Action":"build-output","ImportPath":"` + longName + `x"}`, reason: "name limit"},
		{name: "oversized test", input: `{"Action":"output","Package":"a","Test":"` + longName + `x"}`, reason: "name limit"},
		{name: "concatenated", input: valid + valid, reason: "invalid JSON object"},
		{name: "malformed", input: valid + "\n{", reason: "invalid JSON object"},
		{name: "non-object", input: "null", reason: "invalid JSON object"},
		{name: "wrong type", input: `{"Package":12}`, reason: "invalid JSON object"},
		{name: "invalid Unicode is replaced", input: `{"Package":"\ud800","Action":"pass"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code, stdout, stderr := report(t, tc.input, "-diagnostic-spill")
			if tc.reason == "" {
				if code != 0 || stdout == "" {
					t.Fatalf("exit %d, stderr %.200s", code, stderr)
				}
				return
			}
			if code != 1 || stdout != "" || !strings.Contains(stderr, "read go test events: "+tc.reason) {
				t.Fatalf("exit %d, summary bytes %d, stderr %.200s", code, len(stdout), stderr)
			}
		})
	}
}

func TestSpillMaximumDiagnosticEvent(t *testing.T) {
	const prefix = `{"Action":"output","Package":"p","Test":"TestLarge","Output":"`
	const suffix = "\"}\n"
	size := maxEventBytes - len(prefix) - len(suffix)
	payload := strings.Repeat("x", size)
	s, probe, creates := newProbedDiagnosticStore(t, 2*maxEventBytes)
	r := newRun()
	r.store = s
	r.names.names = make(map[string]string)
	if err := r.decodeSpill(io.MultiReader(strings.NewReader(prefix), strings.NewReader(payload), strings.NewReader(suffix)),
		func(*packageResult) { t.Error("unexpected terminal event") }); err != nil {
		t.Fatal(err)
	}
	ref := &r.packages["p"].currentTest["TestLarge"].output.ref
	verifyPageOutput(t, s, ref, payload)
	if *creates != 1 || probe.highWater != int64(spoolHeaderSize+size) {
		t.Fatalf("factory calls %d, extent %d", *creates, probe.highWater)
	}
}

func FuzzSpillJSONL(f *testing.F) {
	f.Add(fixture(f, "shared-build.json"))
	f.Add(fixture(f, "benchmark-interrupted.json") + "{")
	f.Add("\r\n{}\r\nnull")
	f.Add(`{"Action":"pass","Package":"\ud800"}`)
	f.Fuzz(func(t *testing.T, input string) {
		if len(input) > 64*1024 {
			t.Skip("input exceeds 64 KiB")
		}
		code := execute([]string{"-diagnostic-spill"}, strings.NewReader(input), io.Discard, io.Discard)
		if code != 0 && code != 1 {
			t.Fatalf("unexpected exit %d", code)
		}
	})
}

func TestSpillRetainedIdentityLimits(t *testing.T) {
	t.Run("output-only identities and atomic build failure", func(t *testing.T) {
		r := newRun()
		r.names.names = make(map[string]string)
		for i := range maxPackageBuildIDs - 1 {
			action, name := "output", fmt.Sprintf("p%d", i)
			if i%2 == 0 {
				action = "build-output"
			}
			addBoundedEvent(t, r, event{Action: action, Package: name, ImportPath: name})
		}
		ev := event{Action: "build-fail", ImportPath: "unknown [unknown.test]"}
		if err := r.retainNames(&ev); !errors.Is(err, errIDLimit) || len(r.names.names) != maxPackageBuildIDs-1 {
			t.Fatalf("partially admitted a build failure, err = %v, names = %d", err, len(r.names.names))
		}
		addBoundedEvent(t, r, event{Action: "build-fail", ImportPath: "p1 [p1.test]"})
		addBoundedEvent(t, r, event{Action: "build-output", ImportPath: "p1 [p1.test]"})
		if err := r.retainNames(&event{Action: "build-fail", ImportPath: "p1"}); !errors.Is(err, errIDLimit) {
			t.Fatalf("identity overflow = %v", err)
		}
	})
	t.Run("empty build display package", func(t *testing.T) {
		r := newRun()
		r.names.names = make(map[string]string)
		for i := range maxPackageBuildIDs - 1 {
			addBoundedEvent(t, r, event{Action: "build-output", ImportPath: fmt.Sprintf("id%d", i)})
		}
		ev := event{Action: "build-fail", ImportPath: " [p.test]"}
		if err := r.retainNames(&ev); !errors.Is(err, errIDLimit) {
			t.Fatalf("empty display name bypassed the limit: %v", err)
		}
	})
	t.Run("occurrences count repeats but not reused names", func(t *testing.T) {
		r := newRun()
		r.names.names = make(map[string]string)
		s, probe, creates := newProbedDiagnosticStore(t, spoolPageSize)
		r.store = s
		ev := event{Action: "run", Package: "p", Test: "TestSame"}
		output := event{Action: "output", Package: ev.Package, Test: ev.Test, Output: "reused\n"}
		pass := event{Action: "pass", Package: ev.Package, Test: ev.Test}
		for range maxTestOccurrences {
			addBoundedEvent(t, r, ev)
			addBoundedEvent(t, r, output)
			addBoundedEvent(t, r, pass)
		}
		addBoundedEvent(t, r, event{Action: "output", Package: "p", Test: ev.Test})
		if err := r.retainNames(&ev); !errors.Is(err, errTestLimit) || r.names.bytes != len("pTestSame") {
			t.Fatalf("occurrence overflow = %v, distinct bytes = %d", err, r.names.bytes)
		}
		first, last := r.packages["p"].testOrder[0], r.packages["p"].testOrder[maxTestOccurrences-1]
		if first == last || first.name != last.name {
			t.Fatal("repeated test conflated its occurrences")
		}
		if s.failure != "" || *creates != 1 || probe.highWater != int64(spoolHeaderSize+len(output.Output)) {
			t.Fatalf("release at occurrence limit: failure %q, factory calls %d, extent %d", s.failure, *creates, probe.highWater)
		}
	})
	t.Run("exact distinct byte limit", func(t *testing.T) {
		r := newRun()
		r.names.names = make(map[string]string)
		common := strings.Repeat("x", maxNameBytes-4)
		for i := range maxIdentityBytes / maxNameBytes {
			name := common + fmt.Sprintf("%04d", i)
			addBoundedEvent(t, r, event{Action: "output", Package: name, Test: name})
		}
		addBoundedEvent(t, r, event{Action: "build-output", ImportPath: common + "0000"})
		if err := r.retainNames(&event{Action: "output", Package: common + "0000", Test: "a"}); !errors.Is(err, errIdentityBytes) {
			t.Fatalf("byte overflow = %v", err)
		}
		if r.names.bytes != maxIdentityBytes {
			t.Fatalf("distinct bytes = %d", r.names.bytes)
		}
	})
}

func addBoundedEvent(t *testing.T, r *run, ev event) {
	t.Helper()
	if err := r.retainNames(&ev); err != nil {
		t.Fatal(err)
	}
	r.add(ev)
}

func TestLegacyAcceptsInputBeyondSpillLimits(t *testing.T) {
	name := strings.Repeat("a", maxNameBytes+1)
	record := `{"ignored":"` + strings.Repeat("x", maxEventBytes) + `"}`
	input := record + `{"Action":"pass","Package":"` + name + `"}null`
	code, stdout, stderr := report(t, input)
	if code != 0 || !strings.Contains(stdout, "| "+name+" | pass |") {
		t.Fatalf("legacy exit %d, stdout bytes %d, stderr bytes %d", code, len(stdout), len(stderr))
	}
}
