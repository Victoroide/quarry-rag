package main

import (
	"fmt"
	"strings"
	"testing"
	"unicode/utf8"
)

func TestFailureDiagnosticsBoundOutput(t *testing.T) {
	const pkg = module + "/bounded"
	const limit = 512 * 1024
	const heading = "--- INCOMPLETE: TestEdge\n"
	const marker = "--- INCOMPLETE OUTPUT OMITTED: "

	t.Run("aggregate and latest fragments", func(t *testing.T) {
		events := []event{
			{Action: "run", Package: pkg, Test: "TestFirst"},
			{Action: "run", Package: pkg, Test: "TestSecond"},
			{Action: "output", Package: pkg, Test: "TestSecond", Output: "old stack line\npgtest: setup 2s\n"},
		}
		for range 3000 {
			events = append(events, event{Action: "output", Package: pkg, Test: "TestFirst", Output: strings.Repeat("ø", 100) + "\n"})
		}
		events = append(events,
			event{Action: "output", Package: pkg, Test: "TestSecond", Output: "new stack head\n"},
			event{Action: "output", Package: pkg, Test: "TestFirst", Output: "recent first test\npgtest: cleanup 500ms\n"},
			event{Action: "output", Package: pkg, Test: "TestSecond", Output: "new stack tail\n"},
		)
		input := stream(t, events...)
		code, stdout, stderr := report(t, input)
		if code != 1 {
			t.Fatalf("exit status = %d, want 1", code)
		}
		details := strings.TrimPrefix(stderr, "INCOMPLETE\t"+pkg+"\n")
		details = strings.TrimSuffix(details, "testreport: "+pkg+" has no result\n")
		if len(details) > limit || !utf8.ValidString(details) {
			t.Fatalf("diagnostics have %d bytes, valid UTF-8 = %t", len(details), utf8.ValidString(details))
		}
		requireOnce(t, details, marker, "recent first test\n", "new stack head\nnew stack tail\n")
		requireAbsent(t, details, "old stack line", "pgtest: setup")
		requireContains(t, "summary", stdout, "| Setup | 1 | 2.00 | 2.00 |", "| Cleanup | 1 | 0.50 | 0.50 |")
		_, _, again := report(t, input)
		if again != stderr {
			t.Error("identical input produced different diagnostics")
		}
	})

	t.Run("heading count and unbounded ordinary failure details", func(t *testing.T) {
		var events []event
		for index := range 130 {
			name := fmt.Sprintf("Test%03d", index)
			events = append(events, event{Action: "run", Package: pkg, Test: name})
		}
		events = append(events,
			event{Action: "output", Package: pkg, Test: "Test128", Output: "omitted"},
			event{Action: "output", Package: pkg, Test: "TestFailure", Output: strings.Repeat("x", limit) + "explicit tail\n"},
			event{Action: "fail", Package: pkg, Test: "TestFailure"},
			event{Action: "output", Package: pkg, Output: strings.Repeat("y", limit) + "package tail\n"},
			event{Action: "fail", Package: pkg},
		)
		code, _, stderr := report(t, stream(t, events...), pkg)
		if code != 1 {
			t.Fatalf("exit status = %d, want 1", code)
		}
		if count := strings.Count(stderr, "--- INCOMPLETE: "); count != 128 {
			t.Errorf("incomplete headings = %d, want 128", count)
		}
		requireOnce(t, stderr, "--- INCOMPLETE: Test000\n", "--- INCOMPLETE: Test127\n", marker+"7 bytes, 2 tests\n")
		if !strings.Contains(stderr, strings.Repeat("x", limit)+"explicit tail\n") ||
			!strings.Contains(stderr, strings.Repeat("y", limit)+"package tail\n") {
			t.Error("explicit test or package output was truncated")
		}
		requireAbsent(t, stderr, "--- INCOMPLETE: Test128", "--- INCOMPLETE: Test129")
	})

	t.Run("huge event and rune boundary", func(t *testing.T) {
		raw := strings.Repeat("界", limit/2) + "stack tail!\n"
		code, _, stderr := report(t, stream(t, event{Action: "output", Package: pkg, Test: "TestEdge", Output: raw}))
		details := strings.TrimPrefix(stderr, "INCOMPLETE\t"+pkg+"\n")
		details = strings.TrimSuffix(details, "testreport: "+pkg+" has no result\n")
		if code != 1 || len(details) > limit || !utf8.ValidString(details) {
			t.Fatalf("exit %d, %d diagnostic bytes, valid UTF-8 = %t", code, len(details), utf8.ValidString(details))
		}
		retained, _, _ := strings.Cut(strings.TrimPrefix(details, heading), marker)
		if !strings.HasSuffix(raw, retained) || !strings.HasSuffix(retained, "stack tail!\n") {
			t.Error("diagnostic did not retain a suffix of the huge event")
		}
		requireOnce(t, details, fmt.Sprintf("%s%d bytes, 0 tests\n", marker, len(raw)-len(retained)))
	})

	t.Run("exact boundary with buffered partial line", func(t *testing.T) {
		raw := strings.Repeat("z", limit-len(heading)-1)
		code, _, stderr := report(t, stream(t, event{Action: "output", Package: pkg, Test: "TestEdge", Output: raw}))
		want := "INCOMPLETE\t" + pkg + "\n" + heading + raw + "\ntestreport: " + pkg + " has no result\n"
		if code != 1 || stderr != want {
			t.Errorf("exit = %d, stderr bytes = %d, want exit 1 and %d exact bytes", code, len(stderr), len(want))
		}
	})

	t.Run("unresolved benchmark shares the byte budget", func(t *testing.T) {
		const root = "BenchmarkTree"
		const parent = root + "/group"
		const leaf = parent + "/pending"
		const headings = "--- INCOMPLETE: " + root + "\n" +
			"--- UNRESOLVED: " + parent + "\n" +
			"--- INCOMPLETE: " + leaf + "\n"
		raw := strings.Repeat("z", limit-len(headings)-1)
		code, _, stderr := report(t, stream(t,
			event{Action: "run", Package: pkg, Test: root},
			event{Action: "run", Package: pkg, Test: parent},
			event{Action: "output", Package: pkg, Test: parent, Output: raw},
			event{Action: "run", Package: pkg, Test: leaf},
		))
		want := "INCOMPLETE\t" + pkg + "\n--- INCOMPLETE: " + root + "\n" +
			"--- UNRESOLVED: " + parent + "\n" + raw + "\n--- INCOMPLETE: " + leaf + "\n" +
			"testreport: " + pkg + " has no result\n"
		if code != 1 || stderr != want {
			t.Errorf("exit = %d, stderr bytes = %d, want exit 1 and %d exact bytes", code, len(stderr), len(want))
		}
	})

	t.Run("heading too large for the budget", func(t *testing.T) {
		code, _, stderr := report(t, stream(t,
			event{Action: "run", Package: pkg, Test: strings.Repeat("x", limit)},
			event{Action: "output", Package: pkg, Test: "TestAfter", Output: "after"},
		))
		want := "INCOMPLETE\t" + pkg + "\n" + marker + "5 bytes, 2 tests\ntestreport: " + pkg + " has no result\n"
		if code != 1 || stderr != want {
			t.Errorf("exit = %d, stderr bytes = %d, want exit 1 and %d exact bytes", code, len(stderr), len(want))
		}
	})
}
