package main

import (
	"math"
	"strconv"
	"testing"
	"unicode/utf8"
)

func TestAmbiguousBenchmarkDiagnostics(t *testing.T) {
	const pkg = "example.com/benchmark-diagnostics"
	// Go 1.27.3 emits the same run names when BenchmarkTree calls
	// b.Run("group/diverged", ...) directly and when group calls
	// b.Run("diverged", ...). The leaf measurement precedes either panic.
	for _, name := range []string{"sibling", "descendant"} {
		t.Run(name, func(t *testing.T) {
			input := fixture(t, "benchmark-slash-"+name+".json")
			code, stdout, stderr := report(t, input, pkg)
			if code != 1 {
				t.Fatalf("exit = %d, want 1", code)
			}
			requireContains(t, "summary", stdout, "| "+pkg+" | fail |")
			requireOnce(t, stderr,
				"--- INCOMPLETE: BenchmarkTree\n",
				"--- UNRESOLVED: BenchmarkTree/group\n=== RUN   BenchmarkTree/group\nBenchmarkTree/group\n",
				"--- INCOMPLETE: BenchmarkTree/group/diverged\n",
				"panic: "+name+" interrupted\n",
			)
			requireAbsent(t, stderr, "--- INCOMPLETE: BenchmarkTree/group\n", "BenchmarkTree/group/leaf", "ns/op")
		})
	}
}

func TestOverlappingBenchmarkPaths(t *testing.T) {
	const pkg = module + "/benchmarks"
	const root = "BenchmarkTree"
	const outer = root + "/group"
	const inner = outer + "/branch"
	const leaf = inner + "/leaf"
	const sibling = outer + "/other"
	events := []event{
		{Action: "run", Package: pkg, Test: root},
		{Action: "run", Package: pkg, Test: outer},
		{Action: "output", Package: pkg, Test: outer, Output: "outer diagnostic\n"},
		{Action: "run", Package: pkg, Test: inner},
		{Action: "output", Package: pkg, Test: inner, Output: "inner diagnostic\n"},
		{Action: "run", Package: pkg, Test: leaf},
		{Action: "output", Package: pkg, Test: leaf, Output: "leaf diagnostic\n"},
	}

	t.Run("EOF during overlapping runs", func(t *testing.T) {
		code, stdout, stderr := report(t, stream(t, events...), pkg)
		if code != 1 {
			t.Fatalf("exit = %d, want 1", code)
		}
		requireContains(t, "summary", stdout, "| "+pkg+" | no result |")
		requireOnce(t, stderr, "--- INCOMPLETE: "+root+"\n"+
			"--- UNRESOLVED: "+outer+"\nouter diagnostic\n"+
			"--- UNRESOLVED: "+inner+"\ninner diagnostic\n"+
			"--- INCOMPLETE: "+leaf+"\nleaf diagnostic\n")
	})

	events = append(events, event{
		Action: "output", Package: pkg, Test: leaf, Output: leaf + "-4\t1\t125.0 ns/op\n",
	})
	t.Run("EOF after leaving only the inner subtree", func(t *testing.T) {
		input := stream(t, append(events, event{Action: "run", Package: pkg, Test: sibling})...)
		code, _, stderr := report(t, input, pkg)
		if code != 1 {
			t.Fatalf("exit = %d, want 1", code)
		}
		requireOnce(t, stderr, "--- INCOMPLETE: "+root+"\n"+
			"--- UNRESOLVED: "+outer+"\nouter diagnostic\n"+
			"--- INCOMPLETE: "+sibling+"\n")
		requireAbsent(t, stderr, inner, "inner diagnostic", "leaf diagnostic", "ns/op")
	})

	// TestMain can fail the package after either of the runner's results.
	for _, result := range []string{"PASS", "FAIL"} {
		t.Run("framed "+result, func(t *testing.T) {
			input := stream(t, append(events,
				event{Action: "output", Package: pkg, OutputType: "frame", Output: result + "\n"},
				event{Action: "fail", Package: pkg},
			)...)
			code, stdout, stderr := report(t, input, pkg)
			if code != 1 {
				t.Fatalf("exit = %d, want 1", code)
			}
			requireContains(t, "summary", stdout, "| "+pkg+" | fail |")
			requireOnce(t, stderr, result+"\n")
			requireAbsent(t, stderr, "--- INCOMPLETE:", "--- UNRESOLVED:", "diagnostic", "ns/op")
		})
	}
}

func TestDiagnosticSpillClassifiesSplitBenchmarkMeasurements(t *testing.T) {
	const name = "BenchmarkÉtage"
	tests := []string{
		name + "-12  \t 120  \t" + strconv.FormatFloat(math.MaxFloat64, 'f', 0, 64) + " unités\t-0.5 ns/op\r\n",
		name + "\t1\n",
		name + "\t1\tinvalid ns/op\n",
		name + "-1\t1 2\n",
		name + "\t1\t1 unit extra\n",
		name + "\t1\t1\u2003unit\rend\n",
	}
	for _, line := range tests {
		for split := 1; split < len(line); split++ {
			if !utf8.RuneStart(line[split]) {
				continue
			}
			state := benchmarkMeasurement{}
			state.consume(name, line[:split])
			state.consume(name, line[split:len(line)-1])
			if got, want := state.complete(), isBenchmarkMeasurement(name, line); got != want {
				t.Fatalf("split %d in %q: completed %t, want %t", split, line, got, want)
			}
		}
	}
}
