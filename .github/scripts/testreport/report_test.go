package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"testing/iotest"
	"time"
	"unicode/utf8"
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
func stream(t testing.TB, events ...event) string {
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

func reportInBothModes(t *testing.T, input string, args ...string) (code int, stdout, stderr string) {
	t.Helper()
	code, stdout, stderr = report(t, input, args...)
	spillCode, spillOut, spillErr := report(t, input, append([]string{"-diagnostic-spill"}, args...)...)
	if spillCode != code || spillOut != stdout || spillErr != stderr {
		t.Errorf("spilled exit %d, want %d\nstdout:\n%s\nwant:\n%s\nstderr:\n%s\nwant:\n%s",
			spillCode, code, spillOut, stdout, spillErr, stderr)
	}
	return code, stdout, stderr
}

func execute(args []string, in io.Reader, stdout, stderr io.Writer) int {
	return executeWithSpool(args, in, stdout, stderr, createDiagnosticSpool)
}

func executeWithSpool(args []string, in io.Reader, stdout, stderr io.Writer, factory spoolFactory) int {
	o, ok := parseOptions(args, stderr)
	if !ok {
		return 2
	}
	return runReport(o, in, stdout, stderr, factory, spoolLimit)
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

func TestCacheResultUsesFinalPackageLine(t *testing.T) {
	const pkg = "example.test/cache"
	const marker = "ok  \t" + pkg + "\t(cached)"
	const elapsed = "ok  \t" + pkg + "\t0.000s\n"
	for _, tc := range []struct {
		name, output, terminal string
		cached                 bool
	}{
		{"diagnostic", "normal diagnostic (cached)\n" + elapsed, "pass", false},
		{"exact imitation", marker + "\n" + elapsed, "pass", false},
		{"prefixed", "diagnostic: " + marker + "\n", "pass", false},
		{"suffixed", marker + " extra\n", "pass", false},
		{"other package", "ok  \tother\t(cached)\n", "pass", false},
		{"spacing", "ok   \t" + pkg + "\t(cached)\n", "pass", false},
		{"case", "OK  \t" + pkg + "\t(cached)\n", "pass", false},
		{"terminal", marker + "\n", "pass", true},
		{"CRLF", marker + "\r\n", "pass", true},
		{"bare CR", marker + "\r", "pass", false},
		{"extra CR", marker + "\r\r\n", "pass", false},
		{"missing LF", marker, "pass", false},
		{"blank after", marker + "\n\n", "pass", false},
		{"partial after", marker + "\npartial", "pass", false},
		{"coverage", marker + "\tcoverage: 0.0% of statements\n", "pass", true},
		{"coverage no statements", marker + "\tcoverage: [no statements]\n", "pass", true},
		{"coverage patterns", marker + "\tcoverage: 100.0% of statements in ./...,example.test/cache\n", "pass", true},
		{"copied coverage", marker + "\tcoverage: instrument initialized\n", "pass", true},
		{"empty coverage", marker + "\tcoverage: \n", "pass", true},
		{"no tests", marker + " [no tests to run]\n", "pass", true},
		{"no fuzz tests", marker + " [no fuzz tests to fuzz]\n", "pass", true},
		{"ambiguous fuzz", marker + "[-fuzz matches more than one fuzz test, won't fuzz]\n", "pass", true},
		{"coverage and no tests", marker + "\tcoverage: 100.0% of statements [no tests to run]\r\n", "pass", true},
		{"coverage and ambiguous fuzz", marker + "\tcoverage: [no statements][-fuzz matches more than one fuzz test, won't fuzz]\n", "pass", true},
		{"coverage missing tab", marker + " coverage: instrument initialized\n", "pass", false},
		{"coverage missing space", marker + "\tcoverage:\n", "pass", false},
		{"coverage missing LF", marker + "\tcoverage: instrument initialized", "pass", false},
		{"coverage with later elapsed line", marker + "\tcoverage: instrument initialized\n" + elapsed, "pass", false},
		{"bad warning", marker + "[no tests to run]\n", "pass", false},
		{"repeated warning", marker + " [no tests to run] [no fuzz tests to fuzz]\n", "pass", false},
		{"trailing whitespace", marker + " \n", "pass", false},
		{"skip", marker + "\n", "skip", false},
		{"failure", marker + "\n", "fail", false},
		{"missing result", marker + "\n", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			events := []event{{Action: "output", Package: pkg, Output: tc.output}}
			if tc.terminal != "" {
				events = append(events, event{Action: tc.terminal, Package: pkg})
			}
			code, stdout, stderr := reportInBothModes(t, stream(t, events...))
			wantCode, row := 0, tc.terminal
			switch tc.terminal {
			case "":
				wantCode, row = 1, "no result"
				requireOnce(t, stderr, "testreport: "+pkg+" has no result\n")
			case "fail":
				wantCode = 1
				requireOnce(t, stderr, "testreport: "+pkg+" failed\n")
			}
			if tc.cached {
				wantCode = 1
			}
			if code != wantCode {
				t.Errorf("exit = %d, want %d\n%s", code, wantCode, stderr)
			}
			requireOnce(t, stdout, "| "+pkg+" | "+row+" | 0.00 |")
			requireCacheProblem(t, pkg, stdout, stderr, tc.cached)
		})
	}
}

func requireCacheProblem(t *testing.T, pkg, stdout, stderr string, cached bool) {
	t.Helper()
	problem := pkg + " reused a cached result\n"
	if cached {
		requireOnce(t, stdout, "- "+problem)
		requireOnce(t, stderr, "testreport: "+problem)
	} else {
		requireAbsent(t, stdout, "reused a cached result")
		requireAbsent(t, stderr, "reused a cached result")
	}
}

func TestCacheResultRespectsOutputBoundaries(t *testing.T) {
	const pkg = "p"
	const marker = "ok  \tp\t(cached)\r\n"
	out := func(output string) event { return event{Action: "output", Package: pkg, Output: output} }
	pass := event{Action: "pass", Package: pkg}

	t.Run("every byte boundary", func(t *testing.T) {
		for _, line := range []string{marker, "ok  \tp\t(cached)\tcoverage: copied field\r\n"} {
			for i := range len(line) + 1 {
				input := stream(t, out("earlier line\n"+line[:i]), out(""), out(line[i:]), pass)
				code, stdout, stderr := reportInBothModes(t, input)
				if code != 1 {
					t.Errorf("%q split at byte %d gives exit %d, want 1", line, i, code)
				}
				requireCacheProblem(t, pkg, stdout, stderr, true)
			}
		}
	})
	t.Run("fragmented long coverage with warning", func(t *testing.T) {
		const prefix = "ok  \tp\t(cached)\tcov"
		patterns := strings.Repeat("example.test/long/path/...,", 128) + "./..."
		input := stream(t, out(prefix), out("erage: 100.0% of statements in "+patterns),
			out(" [no tests to run]\r"), out("\n"), pass)
		code, stdout, stderr := reportInBothModes(t, input)
		if code != 1 {
			t.Errorf("%d bytes of coverage patterns gives exit %d, want 1", len(patterns), code)
		}
		requireCacheProblem(t, pkg, stdout, stderr, true)
	})
	for _, tc := range []struct {
		name   string
		events []event
		cached bool
		code   int
	}{
		{"coalesced logical lines", []event{out("earlier\n\n" + marker), pass}, true, 1},
		{"interleaved", []event{out(marker[:6]),
			{Action: "start", Package: "other"},
			{Action: "build-output", Package: pkg, ImportPath: pkg, Output: "compiler\n"},
			{Action: "build-fail", Package: pkg, ImportPath: pkg},
			{Action: "output", Package: "other", Output: marker},
			{Action: "fail", Package: "other", FailedBuild: pkg},
			out(marker[6:]), pass}, true, 1},
		{"named output", []event{out(marker), {Action: "output", Package: pkg, Test: "TestP", Output: marker}, pass}, false, 0},
		{"named terminal", []event{out(marker), {Action: "pass", Package: pkg, Test: "TestP"}, pass}, false, 0},
		{"typed output", []event{out(marker), {Action: "output", Package: pkg, OutputType: "frame", Output: marker}, pass}, false, 0},
		{"start after line", []event{out(marker), {Action: "start", Package: pkg}, pass}, false, 0},
		{"start within line", []event{out(marker[:6]), {Action: "start", Package: pkg}, out(marker[6:]), pass}, false, 0},
		{"unknown action", []event{out(marker), {Action: "unknown", Package: pkg}, pass}, false, 0},
		{"repeated pass consumes candidate", []event{out(marker), pass, pass}, false, 0},
		{"later cached pass", []event{pass, out(marker), pass}, true, 1},
		{"two cached passes report once", []event{out(marker), pass, out(marker), pass}, true, 1},
		{"later skip", []event{out(marker), pass, {Action: "skip", Package: pkg}}, false, 0},
		{"later fail", []event{out(marker), pass, {Action: "fail", Package: pkg}}, false, 1},
		{"skip consumes candidate", []event{out(marker), {Action: "skip", Package: pkg}, pass}, false, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code, stdout, stderr := reportInBothModes(t, stream(t, tc.events...))
			if code != tc.code {
				t.Errorf("exit = %d, want %d\n%s", code, tc.code, stderr)
			}
			requireCacheProblem(t, pkg, stdout, stderr, tc.cached)
		})
	}
	t.Run("long multibyte name", func(t *testing.T) {
		pkg := strings.Repeat("界", maxNameBytes/3) + "ab"
		line := "ok  \t" + pkg + "\t(cached)\n"
		events := []event{}
		for len(line) > 1024 {
			at := 1024
			for !utf8.RuneStart(line[at]) {
				at--
			}
			events = append(events, event{Action: "output", Package: pkg, Output: line[:at]})
			line = line[at:]
		}
		events = append(events, event{Action: "output", Package: pkg, Output: line}, event{Action: "pass", Package: pkg})
		code, stdout, stderr := reportInBothModes(t, stream(t, events...))
		if code != 1 {
			t.Errorf("exit = %d, want 1", code)
		}
		requireCacheProblem(t, pkg, stdout, stderr, true)
	})
	t.Run("large allowed spill event", func(t *testing.T) {
		input := stream(t, out(marker[:len(marker)-2]+strings.Repeat("x", maxEventBytes-256)+"\n"+marker), pass)
		code, stdout, stderr := reportInBothModes(t, input)
		if code != 1 {
			t.Errorf("exit = %d, want 1", code)
		}
		requireCacheProblem(t, pkg, stdout, stderr, true)
	})
	t.Run("interrupted input", func(t *testing.T) {
		input := stream(t, out(marker))
		for _, spill := range []bool{false, true} {
			for _, suffix := range []io.Reader{strings.NewReader("{"), iotest.ErrReader(io.ErrClosedPipe)} {
				var stdout, stderr bytes.Buffer
				code := runReport(options{spill: spill}, io.MultiReader(strings.NewReader(input), suffix), &stdout, &stderr, createDiagnosticSpool)
				if code != 1 || stdout.Len() != 0 {
					t.Errorf("spill %t gives exit %d and summary %q", spill, code, stdout.String())
				}
				requireContains(t, "diagnostics", stderr.String(), "read go test events:")
				requireCacheProblem(t, pkg, stdout.String(), stderr.String(), false)
			}
		}
	})
}

func TestGoTestCacheResultFixture(t *testing.T) {
	const pkg = "example.test/cachefixture"
	const marker = "ok  \t" + pkg + "\t(cached)"
	dir, cache := t.TempDir(), t.TempDir()
	mod, err := os.ReadFile("../../../go.mod")
	if err != nil {
		t.Fatal(err)
	}
	goDirective := regexp.MustCompile(`(?m)^go [^\r\n]+$`).Find(mod)
	if goDirective == nil {
		t.Fatal("repository go.mod has no Go directive")
	}
	source := `package cachefixture
import (
	"fmt"
	"os"
	"testing"
)
func TestMain(m *testing.M) {
	fmt.Println("normal diagnostic (cached)")
	fmt.Println(` + fmt.Sprintf("%q", marker) + `)
	os.Exit(m.Run())
}
func TestPass(t *testing.T) {
	if answer() != 42 {
		t.Fatal("unexpected answer")
	}
}
`
	for name, data := range map[string]string{
		"go.mod":        "module " + pkg + "\n\n" + string(goDirective) + "\n",
		"cache.go":      "package cachefixture\n\nfunc answer() int { return 42 }\n",
		"cache_test.go": source,
	} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(data), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	for _, tc := range []struct {
		name, coverage string
		cached         bool
	}{
		{"forced", "", false}, {"cold", "", false}, {"warm", "", true},
		{"coverage cold", "\tcoverage: 100.0% of statements in ./...", false},
		{"coverage warm", "\tcoverage: 100.0% of statements in ./...", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var cmd *exec.Cmd
			switch {
			case tc.name == "forced":
				cmd = exec.CommandContext(ctx, "go", "test", "-p", "2", "-short", "-json", "-count=1", ".")
			case tc.coverage != "":
				cmd = exec.CommandContext(ctx, "go", "test", "-p", "2", "-short", "-json", "-coverpkg=./...", ".")
			default:
				cmd = exec.CommandContext(ctx, "go", "test", "-p", "2", "-short", "-json", ".")
			}
			cmd.Dir = dir
			cmd.Env = append(cmd.Environ(), "GOWORK=off", "GOENV=off", "GOFLAGS=", "GODEBUG=", "GOCACHEPROG=", "GOCACHE="+cache)
			var childErr bytes.Buffer
			cmd.Stderr = &childErr
			raw, err := cmd.Output()
			if err != nil {
				t.Fatalf("go test: %v\n%s", err, &childErr)
			}
			verifyGoCacheFixture(t, string(raw), pkg, marker, tc.coverage, tc.cached)
			code, stdout, stderr := reportInBothModes(t, string(raw), pkg)
			wantCode := 0
			if tc.cached {
				wantCode = 1
			}
			if code != wantCode {
				t.Errorf("exit = %d, want %d\n%s", code, wantCode, stderr)
			}
			requireCacheProblem(t, pkg, stdout, stderr, tc.cached)
		})
	}
}

func TestCacheResultMatcherDoesNotAllocatePerLine(t *testing.T) {
	name := strings.Repeat("a", maxNameBytes)
	marker := "ok  \t" + name + "\t(cached)"
	line := marker + "\tcoverage: 100.0% of statements in " + strings.Repeat("example.test/p,...,", 256) + "./... [no tests to run]\r\n"
	overlong := marker + strings.Repeat("x", 4096) + "\n"
	allocs := testing.AllocsPerRun(100, func() {
		var match cacheResultMatch
		match.add(name, overlong)
		if match.lastMatched {
			t.Fatal("overlong suffix matched without a coverage delimiter")
		}
		match.add(name, line[:1024])
		match.add(name, line[1024:])
		if !match.lastMatched || match.lineOpen {
			t.Fatal("valid result did not match after an overlong line")
		}
	})
	if allocs != 0 {
		t.Errorf("allocations per match = %v, want zero", allocs)
	}
}

func verifyGoCacheFixture(t *testing.T, input, pkg, marker, coverage string, cached bool) {
	t.Helper()
	dec := json.NewDecoder(strings.NewReader(input))
	var output strings.Builder
	for {
		var ev event
		if err := dec.Decode(&ev); err != nil {
			t.Fatalf("read fixture package pass: %v\n%s", err, input)
		}
		if ev.Package != pkg || ev.Test != "" {
			continue
		}
		if ev.Action == "pass" {
			break
		}
		if ev.Action == "output" && ev.OutputType == "" {
			output.WriteString(ev.Output)
		}
	}
	plain := output.String()
	if !cached {
		requireContains(t, "fixture output", plain, "normal diagnostic (cached)\n", marker+"\n")
	}
	beforeLF, complete := strings.CutSuffix(plain, "\n")
	summary := beforeLF[strings.LastIndexByte(beforeLF, '\n')+1:]
	elapsed := regexp.MustCompile(`^ok  \t` + regexp.QuoteMeta(pkg) + `\t[0-9]+\.[0-9]+s` + regexp.QuoteMeta(coverage) + `$`)
	if !complete || cached && summary != marker+coverage || !cached && !elapsed.MatchString(summary) {
		t.Fatalf("unexpected final Go summary (want cached = %t and coverage %q): %q\n%s", cached, coverage, summary, input)
	}
}

func TestNoTestResultUsesPackageTerminalOutcome(t *testing.T) {
	const (
		pkg          = "probe"
		marker       = "?   \tprobe\t[no test files]\n"
		diagnostic   = "diagnostic: [no test files]\n"
		okProgress   = "ok   \tprobe\t0.25s\n"
		failProgress = "FAIL \tprobe\t0.25s\n"
		failed       = "testreport: probe failed\n"
		incomplete   = "INCOMPLETE\tprobe\n"
		missing      = "testreport: probe has no result\n"
		okCount      = "| 1 | 0 | 0 | 0 | 0 |"
		failedCount  = "| 0 | 1 | 0 | 0 | 0 |"
		noTestsCount = "| 0 | 0 | 0 | 1 | 0 |"
		missingCount = "| 0 | 0 | 0 | 0 | 1 |"
	)
	markerOutput := event{Action: "output", Package: pkg, Output: marker}
	output := func(fragment string) event {
		return event{Action: "output", Package: pkg, Output: fragment}
	}
	diagnosticOutput := event{Action: "output", Package: pkg, Output: diagnostic}
	pass := event{Action: "pass", Package: pkg, Elapsed: 0.25}
	fail := event{Action: "fail", Package: pkg, Elapsed: 0.25}
	skip := event{Action: "skip", Package: pkg, Elapsed: 0.25}
	for _, tc := range []struct {
		name   string
		events []event
		code   int
		stderr string
		row    string
		count  string
	}{
		{"diagnostic before fail", []event{diagnosticOutput, fail}, 1, failProgress + diagnostic + failed, "fail", failedCount},
		{"diagnostic before pass", []event{diagnosticOutput, pass}, 0, okProgress, "pass", okCount},
		{"exact LF before skip", []event{markerOutput, skip}, 0, "", "no test files", noTestsCount},
		{"exact CRLF before skip", []event{{Action: "output", Package: pkg, Output: "?   \tprobe\t[no test files]\r\n"}, skip}, 0, "", "no test files", noTestsCount},
		{"split package and marker", []event{output(marker[:7]), output(marker[7:17]), output(marker[17:]), skip}, 0, "", "no test files", noTestsCount},
		{"split CRLF", []event{output(strings.TrimSuffix(marker, "\n")), output("\r"), output("\n"), skip}, 0, "", "no test files", noTestsCount},
		{"start after a line ending", []event{output("diagnostic"), output("\n" + marker[:7]), output(marker[7:]), skip}, 0, "", "no test files", noTestsCount},
		{"long diagnostic prefix", []event{output(strings.Repeat("x", 1024)), markerOutput, skip}, 0, okProgress, "skip", okCount},
		{"wrong package across fragments", []event{output(marker[:7]), output("xy" + marker[10:]), skip}, 0, okProgress, "skip", okCount},
		{"trailing data across fragments", []event{output(strings.TrimSuffix(marker, "\n")), output(" extra\n"), skip}, 0, okProgress, "skip", okCount},
		{"trailing data after CR", []event{output(strings.TrimSuffix(marker, "\n") + "\r"), output("x\n"), skip}, 0, okProgress, "skip", okCount},
		{"unfinished fragment before skip", []event{output(strings.TrimSuffix(marker, "\n")), skip}, 0, okProgress, "skip", okCount},
		{"unfinished fragment before EOF", []event{output(marker[:7])}, 1, incomplete + marker[:7] + "\n" + missing, "no result", missingCount},
		{"partitioned diagnostic before failure", []event{output(marker[:7]), output(marker[7:]), fail}, 1, failProgress + marker + failed, "fail", failedCount},
		{"typed output interrupts fragments", []event{output(marker[:7]), {Action: "output", Package: pkg, OutputType: "frame", Output: "PASS\n"}, output(marker[7:]), skip}, 0, okProgress, "skip", okCount},
		{"typed fragment cannot finish line", []event{output(marker[:7]), {Action: "output", Package: pkg, OutputType: "frame", Output: marker[7:]}, skip}, 0, okProgress, "skip", okCount},
		{"named fragment cannot finish line", []event{output(marker[:7]), {Action: "output", Package: pkg, Test: "TestProbe", Output: marker[7:]}, skip}, 0, okProgress, "skip", okCount},
		{"terminal resets partial match", []event{output(marker[:7]), skip, output(marker[7:]), skip}, 0, okProgress + okProgress, "skip", okCount},
		{"terminal resets discarded line", []event{output("diagnostic"), pass, markerOutput, skip}, 0, okProgress, "no test files", noTestsCount},
		{"exact before pass", []event{markerOutput, pass}, 0, okProgress, "pass", okCount},
		{"exact before fail", []event{markerOutput, fail}, 1, failProgress + marker + failed, "fail", failedCount},
		{"exact before EOF", []event{markerOutput}, 1, incomplete + marker + missing, "no result", missingCount},
		{"bare skip", []event{skip}, 0, okProgress, "skip", okCount},
		{"diagnostic before skip", []event{diagnosticOutput, skip}, 0, okProgress, "skip", okCount},
		{"wrong package", []event{{Action: "output", Package: pkg, Output: "?   \tother\t[no test files]\n"}, skip}, 0, okProgress, "skip", okCount},
		{"typed frame", []event{{Action: "output", Package: pkg, Output: marker, OutputType: "frame"}, skip}, 0, okProgress, "skip", okCount},
		{"named marker", []event{{Action: "output", Package: pkg, Output: marker, Test: "TestProbe"}, skip}, 0, okProgress, "skip", okCount},
		{"named output", []event{markerOutput, {Action: "output", Package: pkg, Test: "TestProbe", Output: "named log\n"}, skip}, 0, okProgress, "skip", okCount},
		{"named run", []event{markerOutput, {Action: "run", Package: pkg, Test: "TestProbe"}, skip}, 0, okProgress, "skip", okCount},
		{"named terminal before package skip", []event{markerOutput, {Action: "pass", Package: pkg, Test: "TestProbe"}, skip}, 0, okProgress, "skip", okCount},
		{"named skip before EOF", []event{markerOutput, {Action: "skip", Package: pkg, Test: "TestProbe"}}, 1, incomplete + marker + missing, "no result", missingCount},
		{"duplicate skip", []event{markerOutput, skip, skip}, 0, okProgress, "skip", okCount},
		{"duplicate terminal after pass", []event{markerOutput, pass, skip}, 0, okProgress + okProgress, "skip", okCount},
		{"late marker", []event{skip, markerOutput}, 0, okProgress, "skip", okCount},
		{"fresh marker before duplicate skip", []event{skip, markerOutput, skip}, 0, okProgress, "no test files", noTestsCount},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code, stdout, stderr := reportInBothModes(t, stream(t, tc.events...))
			if code != tc.code || stderr != tc.stderr {
				t.Errorf("got exit %d and stderr %q, want exit %d and stderr %q", code, stderr, tc.code, tc.stderr)
			}
			elapsed := "0.25"
			if tc.row == "no result" {
				elapsed = "0.00"
			}
			requireOnce(t, stdout, tc.count, "| "+pkg+" | "+tc.row+" | "+elapsed+" |")
			if tc.row == "no test files" {
				requireOnce(t, stdout, "### Packages without test files (1)\n\n- "+pkg+"\n")
			} else {
				requireAbsent(t, stdout, "### Packages without test files")
			}
		})
	}
}

func TestNoTestResultAtGoOutputBoundary(t *testing.T) {
	const prefix, suffix = "?   \t", "\t[no test files]\n"
	const outputLimit = 1024
	for _, tc := range []struct {
		name   string
		length int
	}{
		{"fits one output event", outputLimit},
		{"LF in its own event", outputLimit + 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pkg := strings.Repeat("a", tc.length-len(prefix)-len(suffix))
			line := prefix + pkg + suffix
			events := []event{{Action: "output", Package: pkg, Output: line[:outputLimit]}}
			if tail := line[outputLimit:]; tail != "" {
				events = append(events, event{Action: "output", Package: pkg, Output: tail})
			}
			events = append(events, event{Action: "skip", Package: pkg})
			requireNoTestResult(t, stream(t, events...), pkg)
		})
	}
}

func TestNoTestResultFromGoWithLongPackageName(t *testing.T) {
	// Go 1.27.4 emits this result in two plain output fragments of 1024 and
	// 23 bytes for a module containing only a package declaration.
	pkg := "example.test/" + strings.Repeat(strings.Repeat("a", 207)+"/", 4) + strings.Repeat("b", 180)
	requireNoTestResult(t, fixture(t, "long-no-test-result.json"), pkg)
}

func requireNoTestResult(t *testing.T, input, pkg string) {
	t.Helper()
	code, stdout, stderr := reportInBothModes(t, input, pkg)
	if code != 0 || stderr != "" {
		t.Errorf("got exit %d and stderr %q, want exit 0 and silent progress", code, stderr)
	}
	requireOnce(t, stdout, "| 0 | 0 | 0 | 1 | 0 |", "| "+pkg+" | no test files | 0.00 |",
		"### Packages without test files (1)\n\n- "+pkg+"\n")
}

func TestNoTestResultRequiresNoNamedEvents(t *testing.T) {
	const pkg = "probe"
	marker := event{Action: "output", Package: pkg, Output: "?   \tprobe\t[no test files]\n"}
	skip := event{Action: "skip", Package: pkg}
	for _, tc := range []struct{ action, test string }{
		{"bench", "BenchmarkProbe"},
		{"attr", "TestProbe"},
		{"artifacts", "ExampleProbe"},
	} {
		named := event{Action: tc.action, Package: pkg, Test: tc.test}
		for _, order := range []struct {
			name   string
			events []event
			stops  int
		}{
			{"before marker", []event{named, marker, skip}, 1},
			{"after marker", []event{marker, named, skip}, 1},
			{"before earlier terminal", []event{named, {Action: "pass", Package: pkg}, marker, skip}, 2},
		} {
			t.Run(tc.action+"/"+order.name, func(t *testing.T) {
				code, stdout, stderr := reportInBothModes(t, stream(t, order.events...))
				if want := strings.Repeat("ok   \tprobe\t0.00s\n", order.stops); code != 0 || stderr != want {
					t.Errorf("got exit %d and stderr %q, want exit 0 and stderr %q", code, stderr, want)
				}
				requireOnce(t, stdout, "| 1 | 0 | 0 | 0 | 0 |", "| probe | skip | 0.00 |")
				requireAbsent(t, stdout, "### Packages without test files", tc.test)
			})
		}
		t.Run(tc.action+"/empty test name", func(t *testing.T) {
			requireNoTestResult(t, stream(t, marker, event{Action: tc.action, Package: pkg}, skip), pkg)
		})
	}
}

func TestNoTestResultIsPackageScoped(t *testing.T) {
	const marker = "?   \twithout-tests\t[no test files]\n"
	code, stdout, stderr := reportInBothModes(t, stream(t,
		event{Action: "start", Package: "without-tests"},
		event{Action: "start", Package: "with-tests"},
		event{Action: "output", Package: "with-tests", Output: marker},
		event{Action: "output", Package: "without-tests", Output: marker[:10]},
		event{Action: "bench", Package: "with-tests", Test: "BenchmarkProbe"},
		event{Action: "attr", Package: "with-tests", Test: "TestProbe"},
		event{Action: "artifacts", Package: "with-tests", Test: "ExampleProbe"},
		event{Action: "output", Package: "with-tests", Test: "TestProbe", Output: "?   \twith-tests\t[no test files]\n"},
		event{Action: "output", Package: "without-tests", Output: marker[10:]},
		event{Action: "output", Package: "without-tests", Output: marker},
		event{Action: "skip", Package: "with-tests"},
		event{Action: "skip", Package: "without-tests"},
	))
	if code != 0 || stderr != "ok   \twith-tests\t0.00s\n" {
		t.Errorf("got exit %d and stderr %q, want only with-tests progress and exit 0", code, stderr)
	}
	requireOnce(t, stdout, "| 1 | 0 | 0 | 1 | 0 |",
		"| without-tests | no test files | 0.00 |\n| with-tests | skip | 0.00 |\n")
	if list := section(t, stdout, "Packages without test files"); list != " (1)\n\n- without-tests\n" {
		t.Errorf("unexpected no-test list %q", list)
	}
}

func TestNoTestResultFragmentsInterleave(t *testing.T) {
	code, stdout, stderr := reportInBothModes(t, stream(t,
		event{Action: "output", Package: "a", Output: "?   \ta\t[no"},
		event{Action: "output", Package: "b", Output: "?   \tb\t[no"},
		event{Action: "attr", Package: "other", Test: "TestProbe"},
		event{Action: "output", Package: "a", Output: " test files]\n"},
		event{Action: "output", Package: "b", Output: " test files]\n"},
		event{Action: "pass", Package: "other"},
		event{Action: "skip", Package: "b"},
		event{Action: "skip", Package: "a"},
	))
	if code != 0 || stderr != "ok   \tother\t0.00s\n" {
		t.Errorf("got exit %d and stderr %q, want exit 0 and only other progress", code, stderr)
	}
	requireOnce(t, stdout, "| 1 | 0 | 0 | 2 | 0 |", "| a | no test files | 0.00 |\n| b | no test files | 0.00 |",
		"### Packages without test files (2)\n\n- a\n- b\n")
}

func TestNoTestTextPreservesBuildAndCacheFailures(t *testing.T) {
	const shared = "dep [dep.test]"
	const sharedDiagnostic = "shared compiler diagnostic: [no test files]\n"
	const standaloneDiagnostic = "?   \tstandalone\t[no test files]\n"
	code, stdout, stderr := reportInBothModes(t, stream(t,
		event{Action: "build-output", ImportPath: shared, Output: sharedDiagnostic},
		event{Action: "build-fail", ImportPath: shared},
		event{Action: "output", Package: "a", Output: "?   \ta\t[no test files]\n"},
		event{Action: "fail", Package: "a", FailedBuild: shared},
		event{Action: "fail", Package: "b", FailedBuild: shared},
		event{Action: "build-output", ImportPath: "standalone", Output: standaloneDiagnostic},
		event{Action: "output", Package: "standalone", Output: standaloneDiagnostic},
		event{Action: "build-fail", ImportPath: "standalone"},
		event{Action: "output", Package: "cached", Output: "?   \tcached\t[no test files]\n"},
		event{Action: "output", Package: "cached", Output: "ok  \tcached\t(cached)\n"},
		event{Action: "pass", Package: "cached"},
	))
	if code != 1 {
		t.Errorf("exit status = %d, want 1", code)
	}
	requireOnce(t, stderr, "FAIL \ta\t0.00s\n"+sharedDiagnostic, sharedDiagnostic, "FAIL \tb\t0.00s\n",
		"INCOMPLETE\tstandalone\n"+standaloneDiagnostic+"BUILD FAIL\tstandalone\n"+standaloneDiagnostic,
		"testreport: a did not build\n", "testreport: b did not build\n", "testreport: standalone did not build\n",
		"testreport: standalone has no result\n", "ok   \tcached\t0.00s\n", "testreport: cached reused a cached result\n")
	requireOnce(t, stdout, "| 1 | 0 | 3 | 0 | 1 |", "| a | did not build |", "| b | did not build |",
		"| standalone | no result |", "| standalone | did not build |", "| cached | pass |")
	requireAbsent(t, stdout, "### Packages without test files")
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
				{Action: "output", Package: a, Output: "package output\n"},
				{Action: "output", Package: a, Test: "TestUnfinished", Output: "test output\n"},
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
			requireOnce(t, stderr, "compiler diagnostic\n", "testreport: "+a+" has no result\n",
				"INCOMPLETE\t"+a+"\n--- INCOMPLETE: TestUnfinished\ntest output\npackage output\n")
			requireOnce(t, stdout, "| 0 | 0 | 1 | 0 | 1 |", "| "+a+" | no result |", "- "+a+" has no result\n")
			if dependentFail {
				requireOnce(t, stderr, "FAIL \t"+b+"\t", "testreport: "+b+" did not build\n")
				requireOnce(t, stdout, "| "+b+" | did not build |")
				requireAbsent(t, stdout, "| "+a+" | did not build |")
			} else {
				requireOnce(t, stderr, "BUILD FAIL\t"+a+"\n", "testreport: "+a+" did not build\n")
				requireOnce(t, stdout, "| "+a+" | did not build |")
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

func TestDiagnosticSpillMatchesLegacyReport(t *testing.T) {
	fixtures, err := fs.Glob(os.DirFS("testdata"), "*.json")
	if err != nil || len(fixtures) == 0 {
		t.Fatalf("fixtures = %v, %v", fixtures, err)
	}
	for _, name := range fixtures {
		t.Run(name, func(t *testing.T) {
			reportInBothModes(t, fixture(t, name))
		})
	}
}

func TestDiagnosticSpillKeepsNewestUnfinishedTail(t *testing.T) {
	const pkg = "example.test/bounded"
	const root = "BenchmarkTree"
	const parent = root + "/group"
	const leaf = parent + "/pending"
	events := []event{
		{Action: "run", Package: pkg, Test: root},
		{Action: "run", Package: pkg, Test: parent},
		{Action: "run", Package: pkg, Test: leaf},
	}
	for range 126 {
		events = append(events, event{Action: "run", Package: pkg, Test: "TestRepeat"})
	}
	raw := strings.Repeat("界", maxIncompleteBytes/2) + "stack tail!\n"
	events = append(events,
		event{Action: "output", Package: pkg, Test: leaf, Output: "preceding output\n"},
		event{Action: "output", Package: pkg, Test: leaf, Output: raw},
	)
	input := stream(t, events...)
	wantCode, wantOut, wantErr := report(t, input)
	code, stdout, stderr := report(t, input, "-diagnostic-spill")
	if code != wantCode || stdout != wantOut || stderr != wantErr {
		t.Fatalf("spill changed unfinished report: exit %d, stdout bytes %d, stderr bytes %d", code, len(stdout), len(stderr))
	}
	details := strings.TrimPrefix(stderr, "INCOMPLETE\t"+pkg+"\n")
	details = strings.TrimSuffix(details, "testreport: "+pkg+" has no result\n")
	if len(details) > maxIncompleteBytes || !utf8.ValidString(details) {
		t.Fatalf("unfinished details have %d bytes, valid UTF-8 = %t", len(details), utf8.ValidString(details))
	}
	requireContains(t, "details", details, "--- UNRESOLVED: "+parent+"\n", "stack tail!\n", "bytes, 1 tests\n")
	if count := strings.Count(details, "--- INCOMPLETE: ") + strings.Count(details, "--- UNRESOLVED: "); count != 128 {
		t.Fatalf("unfinished headings = %d", count)
	}
}

func TestDiagnosticSpillInterleavesBuildsAndRepeatedTests(t *testing.T) {
	input := stream(t,
		event{Action: "output", Package: "first", Test: "TestRepeat", Output: "ré"},
		event{Action: "build-output", ImportPath: "shared [shared.test]", Output: "comp"},
		event{Action: "output", Package: "second", Test: "TestA", Output: "other package\n"},
		event{Action: "output", Package: "first", Test: "TestRepeat"},
		event{Action: "output", Package: "first", Test: "TestRepeat", Output: "sultat\n"},
		event{Action: "fail", Package: "first", Test: "TestRepeat"},
		event{Action: "run", Package: "first", Test: "TestRepeat"},
		event{Action: "output", Package: "first", Test: "TestRepeat", Output: "second occurrence\n"},
		event{Action: "build-fail", ImportPath: "shared [shared.test]"},
		event{Action: "output", Package: "first", Output: "package tail\n"},
		event{Action: "fail", Package: "first"},
		event{Action: "fail", Package: "consumer", FailedBuild: "shared [shared.test]"},
		event{Action: "build-output", ImportPath: "shared [shared.test]", Output: "iler late\n"},
		event{Action: "fail", Package: "consumer2", FailedBuild: "shared [shared.test]"},
	)
	wantCode, wantOut, wantErr := report(t, input)
	code, stdout, stderr := report(t, input, "-diagnostic-spill")
	if code != wantCode || stdout != wantOut || stderr != wantErr {
		t.Fatalf("spill changed interleaved report: exit %d\n%s\nwant:\n%s", code, stderr, wantErr)
	}
	requireOnce(t, stderr, "résultat\n", "second occurrence\n", "other package\n", "package tail\n", "comp\n", "iler late\n")
}

func TestHostedDiagnosticSpillInvocation(t *testing.T) {
	input := fixture(t, "healthy.json")
	packages := []string{module + "/ok", module + "/notests"}
	cacheFlags := []string{"-module-cache", "exact hit", "-build-cache", "restored from an earlier key"}
	legacyArgs := append(append([]string(nil), cacheFlags...), packages...)
	hostedArgs := append([]string{"-diagnostic-spill"}, legacyArgs...)
	wantCode, wantOut, wantErr := report(t, input, legacyArgs...)
	code, stdout, stderr := report(t, input, hostedArgs...)
	if code != 0 || code != wantCode || stdout != wantOut || stderr != wantErr {
		t.Fatalf("hosted report differed from baseline: exit %d vs %d\n%s\n%s", code, wantCode, stdout, stderr)
	}
	requireContains(t, "packages", section(t, stdout, "Packages"),
		"| "+packages[0]+" | pass |", "| "+packages[1]+" | no test files |")
	requireContains(t, "fixture timings", section(t, stdout, "Database fixtures"),
		"| Setup | 1 | 1.50 | 1.50 |", "| Cleanup | 1 | 0.25 | 0.25 |")
	requireContains(t, "cache states", section(t, stdout, "Caches"),
		"| Go modules | exact hit |", "| Race build objects | restored from an earlier key |")
}
