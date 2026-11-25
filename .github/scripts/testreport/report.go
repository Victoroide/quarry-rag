package main

import (
	"cmp"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"slices"
	"strings"
	"time"
)

// event is one line of `go test -json`. Build events name the package in
// ImportPath instead of Package.
type event struct {
	Action      string
	Package     string
	ImportPath  string
	Test        string
	Elapsed     float64
	Output      string
	FailedBuild string
}

type testResult struct {
	name    string
	outcome string
	elapsed float64
	output  []string
}

type packageResult struct {
	name        string
	outcome     string
	elapsed     float64
	noTests     bool
	cached      bool
	failedBuild string
	output      []string
	tests       map[string]*testResult
	order       []string
}

// stat accumulates the durations a fixture step reported.
type stat struct {
	count int
	total time.Duration
	max   time.Duration
}

func (s *stat) add(d time.Duration) {
	s.count++
	s.total += d
	s.max = max(s.max, d)
}

// run collects the events of one `go test` invocation.
type run struct {
	packages    map[string]*packageResult
	order       []string
	buildOutput map[string][]string
	setup       stat
	cleanup     stat
}

func newRun() *run {
	return &run{packages: map[string]*packageResult{}, buildOutput: map[string][]string{}}
}

var fixtureStep = regexp.MustCompile(`pgtest: (setup|cleanup) (\S+)`)

// buildPackage names the package a build event is about: the import path
// without the test binary suffix.
func buildPackage(importPath string) string {
	name, _, _ := strings.Cut(importPath, " ")
	return name
}

func (r *run) pkg(name string) *packageResult {
	p, ok := r.packages[name]
	if !ok {
		p = &packageResult{name: name, tests: map[string]*testResult{}}
		r.packages[name] = p
		r.order = append(r.order, name)
	}
	return p
}

func (p *packageResult) test(name string) *testResult {
	t, ok := p.tests[name]
	if !ok {
		t = &testResult{name: name}
		p.tests[name] = t
		p.order = append(p.order, name)
	}
	return t
}

// add records ev. It returns the package when ev ended it.
func (r *run) add(ev event) *packageResult {
	switch ev.Action {
	case "build-output":
		name := buildPackage(ev.ImportPath)
		r.buildOutput[name] = append(r.buildOutput[name], ev.Output)
		return nil
	case "build-fail":
		name := buildPackage(ev.ImportPath)
		r.pkg(name).failedBuild = name
		return nil
	}
	if ev.Package == "" {
		return nil
	}
	p := r.pkg(ev.Package)
	switch ev.Action {
	case "output":
		r.addOutput(p, ev)
	case "pass", "fail", "skip":
		if ev.Test != "" {
			t := p.test(ev.Test)
			t.outcome, t.elapsed = ev.Action, ev.Elapsed
			return nil
		}
		p.outcome, p.elapsed = ev.Action, ev.Elapsed
		if ev.FailedBuild != "" {
			p.failedBuild = buildPackage(ev.FailedBuild)
		}
		return p
	}
	return nil
}

func (r *run) addOutput(p *packageResult, ev event) {
	if match := fixtureStep.FindStringSubmatch(ev.Output); match != nil {
		if d, err := time.ParseDuration(match[2]); err == nil {
			if match[1] == "setup" {
				r.setup.add(d)
			} else {
				r.cleanup.add(d)
			}
		}
	}
	if ev.Test != "" {
		t := p.test(ev.Test)
		t.output = append(t.output, ev.Output)
		return
	}
	p.output = append(p.output, ev.Output)
	p.noTests = p.noTests || strings.Contains(ev.Output, "[no test files]")
	p.cached = p.cached || strings.Contains(ev.Output, "(cached)")
}

// decode feeds every event of in to the run and calls onEnd for each package
// that has ended. Input that is not an event stream is an error.
func (r *run) decode(in io.Reader, onEnd func(*packageResult)) error {
	dec := json.NewDecoder(in)
	for {
		var ev event
		err := dec.Decode(&ev)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("read go test events: %w", err)
		}
		if p := r.add(ev); p != nil && onEnd != nil {
			onEnd(p)
		}
	}
}

// problems lists why the run must fail, in package order.
func (r *run) problems(expected []string) []string {
	var out []string
	for _, name := range r.order {
		p := r.packages[name]
		switch {
		case p.failedBuild != "":
			out = append(out, name+" did not build")
		case p.outcome == "fail":
			out = append(out, name+" failed")
		}
		if p.cached {
			out = append(out, name+" reused a cached result")
		}
	}
	for _, name := range expected {
		if p, ok := r.packages[name]; !ok || p.outcome == "" {
			if p != nil && p.failedBuild != "" {
				continue
			}
			out = append(out, name+" has no result")
		}
	}
	return out
}

// failureDetails returns what to show for a package that did not pass: the
// compiler output, or the output of its failing tests and of the package
// itself.
func (r *run) failureDetails(p *packageResult) string {
	var b strings.Builder
	if p.failedBuild != "" {
		for _, line := range r.buildOutput[p.failedBuild] {
			b.WriteString(line)
		}
		return b.String()
	}
	for _, name := range p.order {
		t := p.tests[name]
		if t.outcome != "fail" {
			continue
		}
		fmt.Fprintf(&b, "--- FAIL: %s\n", t.name)
		for _, line := range t.output {
			b.WriteString(line)
		}
	}
	for _, line := range p.output {
		b.WriteString(line)
	}
	return b.String()
}

type options struct {
	slowest      int
	moduleCache  string
	buildCache   string
	expectedList []string
}

// printf writes to w. A failed write has nowhere to be reported, so it is
// dropped: the summary and the diagnostics are best effort.
func printf(w io.Writer, format string, args ...any) {
	_, _ = fmt.Fprintf(w, format, args...)
}

func seconds(v float64) string {
	return fmt.Sprintf("%.2f", v)
}

func (r *run) summary(w io.Writer, o options) {
	counts := map[string]int{}
	var noTests []string
	for _, name := range r.order {
		p := r.packages[name]
		switch {
		case p.noTests:
			noTests = append(noTests, name)
			counts["no test files"]++
		case p.failedBuild != "":
			counts["did not build"]++
		case p.outcome == "":
			counts["no result"]++
		default:
			counts[p.outcome]++
		}
	}
	printf(w, "## Race tests\n\n")
	printf(w, "| Passed | Failed | Did not build | No test files | No result |\n|---:|---:|---:|---:|---:|\n")
	printf(w, "| %d | %d | %d | %d | %d |\n\n", counts["pass"]+counts["skip"], counts["fail"], counts["did not build"], counts["no test files"], counts["no result"])

	printf(w, "### Packages\n\n| Package | Result | Seconds |\n|---|---|---:|\n")
	for _, name := range r.order {
		p := r.packages[name]
		result := p.outcome
		switch {
		case p.noTests:
			result = "no test files"
		case p.failedBuild != "":
			result = "did not build"
		case result == "":
			result = "no result"
		}
		printf(w, "| %s | %s | %s |\n", name, result, seconds(p.elapsed))
	}

	top, children := r.slowTests(o.slowest)
	writeTests(w, "Slowest top-level tests", top)
	writeTests(w, "Slowest subtests", children)

	if skipped := r.skippedTests(); len(skipped) > 0 {
		printf(w, "\n### Skipped tests\n\n")
		for _, name := range skipped {
			printf(w, "- %s\n", name)
		}
	}
	if len(noTests) > 0 {
		printf(w, "\n### Packages without test files (%d)\n\n", len(noTests))
		for _, name := range noTests {
			printf(w, "- %s\n", name)
		}
	}

	printf(w, "\n### Database fixtures\n\n| Step | Count | Total seconds | Longest seconds |\n|---|---:|---:|---:|\n")
	for _, row := range []struct {
		name string
		stat stat
	}{{"Setup", r.setup}, {"Cleanup", r.cleanup}} {
		printf(w, "| %s | %d | %s | %s |\n", row.name, row.stat.count, seconds(row.stat.total.Seconds()), seconds(row.stat.max.Seconds()))
	}

	printf(w, "\n### Caches\n\n| Cache | State |\n|---|---|\n| Go modules | %s |\n| Race build objects | %s |\n", cmp.Or(o.moduleCache, "not reported"), cmp.Or(o.buildCache, "not reported"))
	if problems := r.problems(o.expectedList); len(problems) > 0 {
		printf(w, "\n### Problems\n\n")
		for _, problem := range problems {
			printf(w, "- %s\n", problem)
		}
	}
}

type timedTest struct {
	name    string
	pkg     string
	elapsed float64
}

// slowTests returns the slowest tests that ran, split into top-level tests
// and subtests, longest first.
func (r *run) slowTests(limit int) (top, children []timedTest) {
	for _, name := range r.order {
		p := r.packages[name]
		for _, testName := range p.order {
			t := p.tests[testName]
			if t.outcome == "" || t.outcome == "skip" {
				continue
			}
			entry := timedTest{name: t.name, pkg: p.name, elapsed: t.elapsed}
			if strings.Contains(t.name, "/") {
				children = append(children, entry)
			} else {
				top = append(top, entry)
			}
		}
	}
	byTime := func(a, b timedTest) int { return cmp.Compare(b.elapsed, a.elapsed) }
	slices.SortStableFunc(top, byTime)
	slices.SortStableFunc(children, byTime)
	return top[:min(limit, len(top))], children[:min(limit, len(children))]
}

func (r *run) skippedTests() []string {
	var out []string
	for _, name := range r.order {
		p := r.packages[name]
		for _, testName := range p.order {
			if p.tests[testName].outcome == "skip" {
				out = append(out, p.name+" "+testName)
			}
		}
	}
	return out
}

func writeTests(w io.Writer, title string, tests []timedTest) {
	if len(tests) == 0 {
		return
	}
	printf(w, "\n### %s\n\n| Test | Package | Seconds |\n|---|---|---:|\n", title)
	for _, t := range tests {
		printf(w, "| %s | %s | %s |\n", t.name, t.pkg, seconds(t.elapsed))
	}
}
