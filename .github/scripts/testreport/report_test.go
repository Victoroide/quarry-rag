package main

import (
	"bytes"
	"encoding/json"
	"io/fs"
	"os"
	"strings"
	"testing"
)

const module = "example.test/jsonmod"

func fixture(t testing.TB, name string) string {
	t.Helper()
	data, err := fs.ReadFile(os.DirFS("testdata"), name)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

// stream renders events as go test would print them.
func stream(t *testing.T, events ...event) string {
	t.Helper()
	var b strings.Builder
	enc := json.NewEncoder(&b)
	for _, ev := range events {
		if err := enc.Encode(ev); err != nil {
			t.Fatal(err)
		}
	}
	return b.String()
}

func report(t *testing.T, input string, args ...string) (code int, stdout, stderr string) {
	t.Helper()
	var out, errOut bytes.Buffer
	code = execute(args, strings.NewReader(input), &out, &errOut)
	return code, out.String(), errOut.String()
}

// section returns the part of a summary from the heading title up to the
// next heading.
func section(t *testing.T, summary, title string) string {
	t.Helper()
	_, rest, found := strings.Cut(summary, "### "+title)
	if !found {
		t.Fatalf("summary has no %q section:\n%s", title, summary)
	}
	body, _, _ := strings.Cut(rest, "\n### ")
	return body
}

func requireContains(t *testing.T, label, text string, wants ...string) {
	t.Helper()
	for _, want := range wants {
		if !strings.Contains(text, want) {
			t.Errorf("%s does not contain %q:\n%s", label, want, text)
		}
	}
}

func TestHealthyRunSummarizesEveryPackage(t *testing.T) {
	code, stdout, stderr := report(t, fixture(t, "healthy.json"),
		"-module-cache", "hit", "-build-cache", "restored from the previous week",
		module+"/ok", module+"/notests")
	if code != 0 {
		t.Fatalf("exit status = %d, stderr:\n%s", code, stderr)
	}
	requireContains(t, "summary", stdout,
		"| 1 | 0 | 0 | 1 | 0 |",
		"| "+module+"/ok | pass |",
		"| "+module+"/notests | no test files |",
		"### Packages without test files (1)",
		"- "+module+"/notests",
		"- "+module+"/ok TestParent/skipped",
		"| Setup | 1 | 1.50 | 1.50 |",
		"| Cleanup | 1 | 0.25 | 0.25 |",
		"| Go modules | hit |",
		"| Race build objects | restored from the previous week |",
	)
	if strings.Contains(stdout, "### Problems") {
		t.Errorf("a healthy run reports problems:\n%s", stdout)
	}
	requireContains(t, "progress", stderr, "ok   \t"+module+"/ok\t")
}

func TestTestsAreListedSeparatelyFromSubtests(t *testing.T) {
	_, stdout, _ := report(t, fixture(t, "healthy.json"))
	top := section(t, stdout, "Slowest top-level tests")
	if !strings.Contains(top, "| TestParent |") || strings.Contains(top, "TestParent/child") {
		t.Errorf("top-level table is wrong:\n%s", top)
	}
	subtests := section(t, stdout, "Slowest subtests")
	if !strings.Contains(subtests, "| TestParent/child |") || strings.Contains(subtests, "| TestParent |") {
		t.Errorf("subtest table is wrong:\n%s", subtests)
	}
}

func TestFailuresAndBuildErrorsFailTheReportWithTheirOutput(t *testing.T) {
	code, stdout, stderr := report(t, fixture(t, "mixed.json"),
		module+"/ok", module+"/broken", module+"/notests")
	if code != 1 {
		t.Fatalf("exit status = %d, want 1", code)
	}
	requireContains(t, "diagnostics", stderr,
		"FAIL \t"+module+"/ok\t",
		"--- FAIL: TestFails",
		"boom",
		"FAIL \t"+module+"/broken\t",
		"undefined: undefinedSymbol",
		"testreport: "+module+"/ok failed",
		"testreport: "+module+"/broken did not build",
	)
	requireContains(t, "summary", stdout, "| "+module+"/broken | did not build |", "### Problems")
	if strings.Contains(stderr, module+"/broken has no result") {
		t.Errorf("a package that did not build is also reported as missing:\n%s", stderr)
	}
}

func TestDependencyBuildErrorReportsTheCompilerDiagnostic(t *testing.T) {
	code, stdout, stderr := report(t, fixture(t, "dependency-build.json"), module+"/app")
	if code != 1 {
		t.Fatalf("exit status = %d, want 1", code)
	}
	requireContains(t, "summary", stdout, "| "+module+"/app | did not build |")
	requireContains(t, "diagnostics", stderr,
		"dep/dep.go:3:14: undefined: missingFixtureNumber",
		"testreport: "+module+"/app did not build",
	)
}

func TestAPackageWithoutAResultFails(t *testing.T) {
	code, stdout, stderr := report(t, fixture(t, "healthy.json"), module+"/ok", module+"/missing")
	if code != 1 {
		t.Fatalf("exit status = %d, want 1", code)
	}
	requireContains(t, "diagnostics", stderr, "testreport: "+module+"/missing has no result")
	requireContains(t, "summary", stdout, "- "+module+"/missing has no result")
}

func TestATruncatedStreamLeavesPackagesWithoutAResult(t *testing.T) {
	input := stream(t,
		event{Action: "start", Package: module + "/ok"},
		event{Action: "run", Package: module + "/ok", Test: "TestParent"},
	)
	code, stdout, _ := report(t, input, module+"/ok")
	if code != 1 {
		t.Fatalf("exit status = %d, want 1", code)
	}
	requireContains(t, "summary", stdout, "| "+module+"/ok | no result |", module+"/ok has no result")
}

func TestACachedResultFails(t *testing.T) {
	input := stream(t,
		event{Action: "output", Package: module + "/ok", Output: "ok  \t" + module + "/ok\t(cached)\n"},
		event{Action: "pass", Package: module + "/ok", Elapsed: 0},
	)
	code, _, stderr := report(t, input, module+"/ok")
	if code != 1 {
		t.Fatalf("exit status = %d, want 1", code)
	}
	requireContains(t, "diagnostics", stderr, "testreport: "+module+"/ok reused a cached result")
}

func TestSlowestTestsAreLongestFirstAndLimited(t *testing.T) {
	var events []event
	for name, elapsed := range map[string]float64{"TestA": 1, "TestB": 9, "TestC": 5, "TestD": 7, "TestA/leaf": 30} {
		events = append(events, event{Action: "pass", Package: module + "/ok", Test: name, Elapsed: elapsed})
	}
	events = append(events, event{Action: "pass", Package: module + "/ok", Elapsed: 12})
	_, stdout, _ := report(t, stream(t, events...), "-slowest", "2")
	top := section(t, stdout, "Slowest top-level tests")
	first, second := strings.Index(top, "| TestB |"), strings.Index(top, "| TestD |")
	if first < 0 || second < first || strings.Contains(top, "| TestC |") || strings.Contains(top, "| TestA |") {
		t.Errorf("top-level table should hold only TestB then TestD:\n%s", top)
	}
	requireContains(t, "summary", stdout, "| TestA/leaf |")
}

func TestInputThatIsNotAnEventStreamFails(t *testing.T) {
	code, _, stderr := report(t, "PASS\nok  \texample\t0.1s\n")
	if code != 1 {
		t.Fatalf("exit status = %d, want 1", code)
	}
	requireContains(t, "diagnostics", stderr, "read go test events")
}

func TestUnknownFlagIsAUsageError(t *testing.T) {
	if code, _, _ := report(t, "", "-nope"); code != 2 {
		t.Fatalf("exit status = %d, want 2", code)
	}
}

func TestPackageTimeoutPrintsUnfinishedStack(t *testing.T) {
	pkg := module + "/timeout"
	code, stdout, stderr := report(t, fixture(t, "timeout.json"), pkg)
	if code != 1 {
		t.Fatalf("exit status = %d, want 1", code)
	}
	requireContains(t, "summary", stdout, "| "+pkg+" | fail |", "- "+pkg+" failed")
	requireOnce(t, stderr,
		"--- INCOMPLETE: TestDeliberateTimeout\n",
		"panic: test timed out after 50ms\n",
		"\t\tTestDeliberateTimeout (0s)\n",
		"testing.(*M).startAlarm.func1()\n",
		"timeoutcase/timeout_test.go:6",
		"exit status 2\n",
	)
	requireAbsent(t, stderr, "--- FAIL: TestDeliberateTimeout", "has no result")
}

func requireOnce(t *testing.T, text string, wants ...string) {
	t.Helper()
	for _, want := range wants {
		if count := strings.Count(text, want); count != 1 {
			t.Errorf("count of %q = %d, want 1", want, count)
		}
	}
}

func requireAbsent(t *testing.T, text string, unwanted ...string) {
	t.Helper()
	for _, value := range unwanted {
		if strings.Contains(text, value) {
			t.Errorf("unexpected %q in output", value)
		}
	}
}

func TestBenchmarkDiagnostics(t *testing.T) {
	pkg := module + "/benchmarks"
	t.Run("completed benchmarks in a failed package", func(t *testing.T) {
		code, stdout, stderr := report(t, stream(t,
			event{Action: "start", Package: pkg},
			event{Action: "output", Package: pkg, Test: "BenchmarkLogged-2", Output: "successful log\n"},
			event{Action: "bench", Package: pkg, Test: "BenchmarkLogged-2", Elapsed: 9},
			event{Action: "run", Package: pkg, Test: "BenchmarkSilent"},
			event{Action: "output", Package: pkg, Test: "BenchmarkSilent", Output: "BenchmarkSilent-2 \t1\t209.0 ns/op\n"},
			event{Action: "fail", Package: pkg},
		))
		if code != 1 {
			t.Fatalf("exit = %d, want 1", code)
		}
		requireAbsent(t, stderr, "--- INCOMPLETE:", "successful log", "209.0 ns/op")
		requireAbsent(t, stdout, "BenchmarkLogged", "BenchmarkSilent", "### Skipped tests")
		requireOnce(t, stderr, "testreport: "+pkg+" failed")
	})

	t.Run("repeated CPU and count variants", func(t *testing.T) {
		const name = "BenchmarkRepeated/input-7"
		code, stdout, stderr := report(t, stream(t,
			event{Action: "start", Package: pkg},
			event{Action: "run", Package: pkg, Test: name},
			event{Action: "output", Package: pkg, Test: name, Output: name + " \t       1\t2.0 ns/op\n"},
			event{Action: "output", Package: pkg, Test: name, Output: "successful repeat log\n"},
			event{Action: "output", Package: pkg, Test: name, Output: name + " \t       1\n"},
			event{Action: "run", Package: pkg, Test: name},
			event{Action: "output", Package: pkg, Test: name, Output: name + "-16  \t"},
			event{Action: "output", Package: pkg, Test: name, Output: "       2\t-3.50 widgets/op\t0 B/op\t0 allocs/op\n"},
			event{Action: "fail", Package: pkg},
		))
		if code != 1 {
			t.Fatalf("exit = %d, want 1", code)
		}
		requireAbsent(t, stderr, "--- INCOMPLETE:", "successful repeat log", "widgets/op")
		requireAbsent(t, stdout, name, "### Skipped tests")
	})

	t.Run("later benchmark work remains visible", func(t *testing.T) {
		code, stdout, stderr := report(t, stream(t,
			event{Action: "start", Package: pkg},
			event{Action: "run", Package: pkg, Test: "BenchmarkInterrupted"},
			event{Action: "output", Package: pkg, Test: "BenchmarkInterrupted", Output: "first benchmark log\n"},
			event{Action: "output", Package: pkg, Test: "BenchmarkInterrupted", Output: "BenchmarkInterrupted-2 \t1\t209.0 ns/op\n"},
			event{Action: "output", Package: pkg, Test: "BenchmarkInterrupted", Output: "BenchmarkOther-2 \t1\t209.0 ns/op\n"},
			event{Action: "output", Package: pkg, Test: "BenchmarkInterrupted", Output: "BenchmarkInterrupted-extra \t1\t209.0 ns/op\n"},
			event{Action: "output", Package: pkg, Test: "BenchmarkInterrupted", Output: "BenchmarkInterrupted-2 \tunknown\t209.0 ns/op\n"},
			event{Action: "output", Package: pkg, Test: "BenchmarkInterrupted", Output: "BenchmarkInterrupted-2 \t1\tdiagnostic text\n"},
			event{Action: "output", Package: pkg, Test: "BenchmarkInterrupted", Output: "panic: test timed out\n"},
			event{Action: "output", Package: pkg, Test: "BenchmarkRestarted-2", Output: "completed explicit benchmark log\n"},
			event{Action: "bench", Package: pkg, Test: "BenchmarkRestarted-2"},
			event{Action: "run", Package: pkg, Test: "BenchmarkRestarted-2"},
			event{Action: "run", Package: pkg, Test: "BenchmarkFailed"},
			event{Action: "output", Package: pkg, Test: "BenchmarkFailed", Output: "BenchmarkFailed\t1\t7.0 ns/op\n"},
			event{Action: "output", Package: pkg, Test: "BenchmarkFailed", Output: "benchmark error\n"},
			event{Action: "fail", Package: pkg, Test: "BenchmarkFailed"},
			event{Action: "run", Package: pkg, Test: "TestActive"},
			event{Action: "output", Package: pkg, Test: "TestActive", Output: "TestActive-2 \t1\t209.0 ns/op\n"},
			event{Action: "fail", Package: pkg},
		))
		if code != 1 {
			t.Fatalf("exit = %d, want 1", code)
		}
		requireOnce(t, stderr,
			"--- FAIL: BenchmarkFailed\nbenchmark error\n",
			"--- INCOMPLETE: BenchmarkInterrupted\nBenchmarkOther-2 \t1\t209.0 ns/op\n"+
				"BenchmarkInterrupted-extra \t1\t209.0 ns/op\nBenchmarkInterrupted-2 \tunknown\t209.0 ns/op\n"+
				"BenchmarkInterrupted-2 \t1\tdiagnostic text\npanic: test timed out\n",
			"--- INCOMPLETE: BenchmarkRestarted-2\n--- INCOMPLETE: TestActive\n",
			"--- INCOMPLETE: TestActive\nTestActive-2 \t1\t209.0 ns/op\n",
		)
		requireAbsent(t, stderr, "first benchmark log", "completed explicit benchmark log", "BenchmarkFailed\t1", "--- INCOMPLETE: BenchmarkFailed")
		requireAbsent(t, stdout, "BenchmarkInterrupted", "TestActive", "### Skipped tests")
	})
}

func TestNestedBenchmarkDiagnostics(t *testing.T) {
	const capturedPackage = "example.com/benchmark-diagnostics"
	// These streams were captured with -benchtime=1x -cpu=1,2 and a TestMain
	// that exits 3 after m.Run, unless a benchmark panics first.
	for _, tc := range []struct {
		name   string
		want   []string
		absent []string
	}{
		{
			name:   "nested",
			want:   []string{"PASS\n", "exit status 3\n"},
			absent: []string{"--- INCOMPLETE:"},
		},
		{
			name:   "failed",
			want:   []string{"--- FAIL: BenchmarkError\n=== RUN   BenchmarkError\n", "explicit benchmark error\n", "\nFAIL\n", "exit status 3\n"},
			absent: []string{"--- INCOMPLETE:"},
		},
		{
			name:   "interrupted",
			want:   []string{"--- INCOMPLETE: BenchmarkInterrupted\n", "panic: interrupt parent\n", "benchmark_test.go:22"},
			absent: []string{"--- INCOMPLETE: BenchmarkInterrupted/child"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code, stdout, stderr := report(t, fixture(t, "benchmark-"+tc.name+".json"), capturedPackage)
			if code != 1 {
				t.Fatalf("exit = %d, want 1", code)
			}
			requireContains(t, "summary", stdout, "| "+capturedPackage+" | fail |")
			requireOnce(t, stderr, tc.want...)
			requireAbsent(t, stderr, tc.absent...)
		})
	}

	t.Run("leaving one subtree before EOF", func(t *testing.T) {
		const pkg = module + "/benchmarks"
		const other = module + "/other"
		code, stdout, stderr := report(t, stream(t,
			event{Action: "run", Package: pkg, Test: "BenchmarkTree"},
			event{Action: "run", Package: pkg, Test: "BenchmarkTree/group"},
			event{Action: "run", Package: pkg, Test: "BenchmarkTree/group/leaf"},
			event{Action: "output", Package: pkg, Test: "BenchmarkTree/group/leaf", Output: "BenchmarkTree/group/leaf-2\t1\t125.0 ns/op\n"},
			event{Action: "run", Package: other, Test: "BenchmarkOther"},
			event{Action: "output", Package: other, OutputType: "frame", Output: "PASS\n"},
			event{Action: "pass", Package: other},
			event{Action: "run", Package: pkg, Test: "BenchmarkTree/group-other"},
			event{Action: "output", Package: pkg, Output: "PASS\n"},
			event{Action: "output", Package: pkg, Output: "FAIL\n"},
		))
		if code != 1 {
			t.Fatalf("exit = %d, want 1", code)
		}
		requireContains(t, "summary", stdout, "| "+pkg+" | no result |", "| "+other+" | pass |")
		requireOnce(t, stderr, "--- INCOMPLETE: BenchmarkTree\n", "--- INCOMPLETE: BenchmarkTree/group-other\n")
		requireAbsent(t, stderr, "--- INCOMPLETE: BenchmarkTree/group\n", "--- INCOMPLETE: BenchmarkTree/group/leaf", "ns/op")
	})
}

func TestInterruptedPackageDiagnostics(t *testing.T) {
	a, b := module+"/a", module+"/b"
	active := []event{
		{Action: "start", Package: a},
		{Action: "run", Package: a, Test: "TestParent"},
		{Action: "start", Package: b},
		{Action: "run", Package: b, Test: "TestOtherPackage"},
		{Action: "output", Package: b, Test: "TestOtherPackage", Output: "other package output\n"},
		{Action: "pause", Package: a, Test: "TestParent/parallel"},
		{Action: "output", Package: a, Test: "TestParent/parallel", Output: "parallel output\n"},
		{Action: "cont", Package: a, Test: "TestParent/parallel"},
		{Action: "cont", Package: a, Test: "TestParent/silent"},
		{Action: "output", Package: a, Test: "TestOutputOnly", Output: "output without run\n"},
		{Action: "run", Package: a, Test: "TestRepeated"},
		{Action: "output", Package: a, Test: "TestRepeated", Output: "passed occurrence output\n"},
		{Action: "pass", Package: a, Test: "TestRepeated", Elapsed: 1},
		{Action: "output", Package: a, Test: "TestRepeated", Output: "late passed output\n"},
		{Action: "run", Package: a, Test: "TestRepeated"},
		{Action: "output", Package: a, Test: "TestRepeated", Output: "first active occurrence\n"},
		{Action: "run", Package: a, Test: "TestRepeated"},
		{Action: "output", Package: a, Test: "TestRepeated", Output: "second active occurrence\n"},
		{Action: "output", Package: a, Test: "TestSkipped", Output: "skipped output\n"},
		{Action: "skip", Package: a, Test: "TestSkipped"},
		{Action: "output", Package: a, Test: "TestFailed", Output: "explicit failure output\n"},
		{Action: "fail", Package: a, Test: "TestFailed", Elapsed: 2},
		{Action: "pass", Package: b, Elapsed: 3},
	}
	for _, tc := range []struct {
		name       string
		output     string
		pkgOutput  string
		pkgFail    bool
		tail       string
		expected   []string
		wantResult string
	}{
		{name: "panic with package output", output: "panic: bad test\n", pkgOutput: "package tail\n", pkgFail: true, expected: []string{a, b}, wantResult: "fail"},
		{name: "panic without package output", output: "panic: bad test\n", pkgFail: true, wantResult: "fail"},
		{name: "abrupt exit", output: "exit status 3\n", pkgFail: true, wantResult: "fail"},
		{name: "clean EOF with expected package", output: "buffered at EOF\n", expected: []string{a, b, module + "/missing", a}, wantResult: "no result"},
		{name: "clean EOF without expected package", output: "buffered at EOF\n", expected: []string{b}, wantResult: "no result"},
		{name: "malformed tail", output: "buffered before decode error\n", tail: "{", expected: []string{a, b}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			events := append([]event(nil), active...)
			events = append(events, event{Action: "output", Package: a, Test: "TestParent/parallel", Output: tc.output})
			if tc.pkgOutput != "" {
				events = append(events, event{Action: "output", Package: a, Output: tc.pkgOutput})
			}
			if tc.pkgFail {
				events = append(events, event{Action: "fail", Package: a})
			}
			code, stdout, stderr := report(t, stream(t, events...)+tc.tail, tc.expected...)
			if code != 1 {
				t.Fatalf("exit status = %d, want 1", code)
			}
			heading := "INCOMPLETE\t" + a + "\n"
			if tc.pkgFail {
				heading = "FAIL \t" + a + "\t"
			}
			requireOnce(t, stderr, heading, "ok   \t"+b+"\t", tc.output,
				"--- FAIL: TestFailed\nexplicit failure output\n",
				"--- INCOMPLETE: TestParent\n",
				"--- INCOMPLETE: TestParent/parallel\n",
				"--- INCOMPLETE: TestParent/silent\n",
				"--- INCOMPLETE: TestOutputOnly\noutput without run\n",
				"--- INCOMPLETE: TestRepeated\nfirst active occurrence\n--- INCOMPLETE: TestRepeated\nsecond active occurrence\n",
			)
			requireAbsent(t, stderr, "other package output", "TestOtherPackage", "TestSkipped",
				"skipped output", "passed occurrence output", "late passed output", "--- FAIL: TestParent")
			if tc.pkgOutput != "" {
				requireOnce(t, stderr, tc.pkgOutput)
			}
			previous := -1
			for _, want := range []string{heading, "--- FAIL: TestFailed", "--- INCOMPLETE: TestParent\n",
				"--- INCOMPLETE: TestParent/parallel", "--- INCOMPLETE: TestParent/silent",
				"--- INCOMPLETE: TestOutputOnly", "--- INCOMPLETE: TestRepeated"} {
				next := strings.Index(stderr, want)
				if next <= previous {
					t.Errorf("%q out of order in stderr:\n%s", want, stderr)
				}
				previous = next
			}
			if tc.tail != "" {
				requireOnce(t, stderr, "read go test events")
				if stdout != "" || strings.Index(stderr, "read go test events") <= previous {
					t.Errorf("malformed stream summary = %q, diagnostics:\n%s", stdout, stderr)
				}
				return
			}
			requireContains(t, "summary", stdout, "| "+a+" | "+tc.wantResult+" |", "| "+b+" | pass |")
			if tc.pkgFail {
				requireOnce(t, stderr, "testreport: "+a+" failed")
			} else {
				requireOnce(t, stderr, "testreport: "+a+" has no result")
				requireOnce(t, stdout, "- "+a+" has no result")
			}
			if len(tc.expected) > 2 {
				requireOnce(t, stderr, "testreport: "+module+"/missing has no result")
				requireAbsent(t, stderr, "INCOMPLETE\t"+module+"/missing")
			}
		})
	}

	for _, dependentFail := range []bool{false, true} {
		name := "standalone build failure"
		if dependentFail {
			name = "dependent package failure"
		}
		t.Run(name, func(t *testing.T) {
			events := []event{
				{Action: "start", Package: a},
				{Action: "output", Package: a, Output: "package build noise\n"},
				{Action: "output", Package: a, Test: "TestIrrelevant", Output: "test build noise\n"},
				{Action: "build-output", ImportPath: a, Output: "compiler diagnostic\n"},
				{Action: "build-fail", ImportPath: a},
			}
			if dependentFail {
				events = append(events, event{Action: "fail", Package: b, FailedBuild: a})
			}
			code, stdout, stderr := report(t, stream(t, events...), a)
			if code != 1 {
				t.Fatalf("exit status = %d, want 1", code)
			}
			requireOnce(t, stderr, "compiler diagnostic\n", "testreport: "+a+" did not build")
			requireContains(t, "summary", stdout, "| "+a+" | did not build |")
			requireAbsent(t, stderr, "build noise", "has no result", "--- INCOMPLETE:")
			if dependentFail {
				requireOnce(t, stderr, "FAIL \t"+b+"\t", "testreport: "+b+" did not build")
			}
		})
	}

	t.Run("multiple packages interrupted in first-seen order", func(t *testing.T) {
		input := stream(t,
			event{Action: "start", Package: b},
			event{Action: "output", Package: a, Test: "TestA", Output: "first fragment"},
			event{Action: "output", Package: b, Test: "TestB", Output: "second fragment"},
			event{Action: "output", Package: a, Output: "package A output\n"},
		)
		code, _, stderr := report(t, input, a, b)
		want := "INCOMPLETE\t" + b + "\n--- INCOMPLETE: TestB\nsecond fragment\n" +
			"INCOMPLETE\t" + a + "\n--- INCOMPLETE: TestA\nfirst fragment\npackage A output\n" +
			"testreport: " + b + " has no result\ntestreport: " + a + " has no result\n"
		if code != 1 || stderr != want {
			t.Errorf("exit = %d, stderr = %q, want exit 1 and %q", code, stderr, want)
		}
	})
}
