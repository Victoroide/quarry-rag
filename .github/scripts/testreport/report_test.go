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

func fixture(t *testing.T, name string) string {
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
