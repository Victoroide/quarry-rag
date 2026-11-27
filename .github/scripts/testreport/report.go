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
	OutputType  string
	FailedBuild string
}

type testResult struct {
	name           string
	outcome        string
	elapsed        float64
	output         diagnosticOutput
	measurement    *benchmarkMeasurement
	hasPrefixedRun bool
}

type packageResult struct {
	name            string
	outcome         string
	elapsed         float64
	noTests         bool
	cached          bool
	failedBuild     string
	hasBuildFailure bool
	observed        bool
	standaloneBuild string
	output          diagnosticOutput
	currentTest     map[string]*testResult
	testOrder       []*testResult
	benchmarkPath   []*testResult
}

func (p *packageResult) didNotBuild() bool {
	return p.failedBuild != "" || (p.outcome == "" && p.hasBuildFailure)
}

func (p *packageResult) reportable() bool {
	return p.observed || p.standaloneBuild != ""
}

type buildResult struct {
	output     diagnosticOutput
	cursor     diagnosticCursor
	failed     bool
	referenced bool
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
	packages   map[string]*packageResult
	order      []string
	builds     map[string]*buildResult
	buildOrder []string
	// Independent builds have no terminal package result or synthetic slot
	// that accounts for their exact failure.
	independentBuilds map[string]bool
	outputOrder       uint64
	store             *diagnosticStore
	names             retainedNames
	setup             stat
	cleanup           stat
}

func newRun() *run {
	return &run{packages: map[string]*packageResult{}, builds: map[string]*buildResult{}}
}

var fixtureStep = regexp.MustCompile(`pgtest: (setup|cleanup) (\S+)`)

// buildPackage names the package a build event is about: the import path
// without the test binary suffix.
func buildPackage(importPath string) string {
	name, _, _ := strings.Cut(importPath, " ")
	return name
}

func (r *run) build(id string) *buildResult {
	b := r.builds[id]
	if b == nil {
		b = &buildResult{}
		r.builds[id] = b
		r.buildOrder = append(r.buildOrder, id)
	}
	return b
}

func (r *run) pkg(name string) *packageResult {
	p, ok := r.packages[name]
	if !ok {
		if canonical := r.names.names[name]; canonical != "" {
			name = canonical
		}
		p = &packageResult{name: name, currentTest: map[string]*testResult{}}
		r.packages[name] = p
		r.order = append(r.order, name)
	}
	return p
}

func (p *packageResult) test(name string) *testResult {
	if t := p.currentTest[name]; t != nil {
		return t
	}
	return p.startTest(name)
}

func (p *packageResult) startTest(name string) *testResult {
	t := &testResult{name: name}
	p.currentTest[name] = t
	p.testOrder = append(p.testOrder, t)
	return t
}

func (p *packageResult) completeBenchmark(t *testResult) {
	t.outcome = "bench"
	t.output, t.measurement = diagnosticOutput{}, nil
	// Repeated benchmark measurements can produce more output without a run event.
	delete(p.currentTest, t.name)
}

// add records ev. It returns the package when ev ended it.
func (r *run) add(ev event) *packageResult {
	switch ev.Action {
	case "build-output":
		if ev.ImportPath != "" {
			b := r.build(ev.ImportPath)
			r.appendOutput(&b.output, ev.Output)
		}
		return nil
	case "build-fail":
		if ev.ImportPath != "" {
			r.build(ev.ImportPath).failed = true
			r.pkg(buildPackage(ev.ImportPath)).hasBuildFailure = true
		}
		return nil
	}
	if ev.Package == "" {
		return nil
	}
	p := r.pkg(ev.Package)
	p.observed = true
	switch ev.Action {
	case "run":
		if isBenchmarkName(ev.Test) {
			p.advanceBenchmarks(ev.Test)
			p.benchmarkPath = append(p.benchmarkPath, p.startTest(ev.Test))
		} else if ev.Test != "" {
			p.startTest(ev.Test)
		}
	case "pause", "cont":
		if ev.Test != "" {
			p.test(ev.Test)
		}
	case "output":
		r.addOutput(p, ev)
	case "bench":
		if t := p.currentTest[ev.Test]; t != nil && t.outcome == "" {
			p.completeBenchmark(t)
		}
	case "pass", "fail", "skip":
		if ev.Test != "" {
			t := p.test(ev.Test)
			t.outcome, t.elapsed = ev.Action, ev.Elapsed
			if ev.Action != "fail" {
				t.output, t.measurement = diagnosticOutput{}, nil
			}
			return nil
		}
		p.outcome, p.elapsed = ev.Action, ev.Elapsed
		p.failedBuild = ""
		if ev.Action == "fail" && ev.FailedBuild != "" {
			p.failedBuild = ev.FailedBuild
			r.build(ev.FailedBuild).referenced = true
		}
		return p
	}
	return nil
}

// resolveBuildFailures matches sealed builds to the final package results.
// A package without a terminal result can represent one standalone build
// under its display name. Any other standalone failure gets its own row.
func (r *run) resolveBuildFailures() {
	referenced := make(map[string]bool)
	for _, name := range r.order {
		if id := r.packages[name].failedBuild; id != "" {
			referenced[id] = true
		}
	}
	for _, id := range r.buildOrder {
		if !r.builds[id].failed || referenced[id] {
			continue
		}
		p := r.packages[buildPackage(id)]
		// Prefer the plain ID for this slot if an independent variant would
		// otherwise share its name in the summary.
		if p.outcome == "" && (p.standaloneBuild == "" || id == p.name) {
			p.standaloneBuild = id
		}
	}
	r.independentBuilds = make(map[string]bool)
	for _, id := range r.buildOrder {
		if r.builds[id].failed && !referenced[id] && r.packages[buildPackage(id)].standaloneBuild != id {
			r.independentBuilds[id] = true
		}
	}
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
		if t.outcome != "pass" && t.outcome != "skip" && ev.Output != "" {
			r.appendOutput(&t.output, ev.Output)
			if t.outcome == "" && r.endsBenchmark(t, ev.Output) {
				p.completeBenchmark(t)
			}
		}
		return
	}
	if ev.OutputType == "frame" {
		// The testing runner emits a bare result. The go command can also
		// frame a package summary (FAIL followed by a tab and package name)
		// after an abrupt exit, so that summary cannot close benchmarks.
		switch ev.Output {
		case "PASS\n", "FAIL\n", "PASS\r\n", "FAIL\r\n":
			p.advanceBenchmarks("")
		}
	}
	r.appendOutput(&p.output, ev.Output)
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
		if !p.reportable() {
			continue
		}
		switch {
		case p.didNotBuild():
			out = append(out, name+" did not build")
		case p.outcome == "fail":
			out = append(out, name+" failed")
		case p.outcome == "":
			out = append(out, name+" has no result")
		}
		if p.cached {
			out = append(out, name+" reused a cached result")
		}
	}
	for _, id := range r.buildOrder {
		if r.independentBuilds[id] {
			out = append(out, id+" did not build")
		}
	}
	for _, name := range expected {
		if p := r.packages[name]; p == nil || !p.reportable() {
			out = append(out, name+" has no result")
		}
	}
	return out
}

// writeFailureDetails streams diagnostics in the order of the report sections.
func (r *run) writeFailureDetails(w *diagnosticWriter, p *packageResult) {
	for _, t := range p.testOrder {
		if t.outcome != "fail" {
			continue
		}
		printf(w, "--- FAIL: %s\n", t.name)
		r.writeOutput(w, &t.output, &diagnosticCursor{})
	}
	r.writeIncompleteDetails(w, p.testOrder)
	r.writeOutput(w, &p.output, &diagnosticCursor{})
}

type options struct {
	slowest      int
	spill        bool
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

func (r *run) summary(w io.Writer, o options, problems []string) {
	counts := map[string]int{}
	var noTests []string
	for _, name := range r.order {
		p := r.packages[name]
		if !p.reportable() {
			continue
		}
		switch {
		case p.didNotBuild():
			counts["did not build"]++
		case p.noTests:
			noTests = append(noTests, name)
			counts["no test files"]++
		case p.outcome == "":
			counts["no result"]++
		default:
			counts[p.outcome]++
		}
	}
	counts["did not build"] += len(r.independentBuilds)
	printf(w, "## Race tests\n\n")
	printf(w, "| Passed | Failed | Did not build | No test files | No result |\n|---:|---:|---:|---:|---:|\n")
	printf(w, "| %d | %d | %d | %d | %d |\n\n", counts["pass"]+counts["skip"], counts["fail"], counts["did not build"], counts["no test files"], counts["no result"])

	printf(w, "### Packages\n\n| Package | Result | Seconds |\n|---|---|---:|\n")
	for _, name := range r.order {
		p := r.packages[name]
		if !p.reportable() {
			continue
		}
		result := p.outcome
		switch {
		case p.didNotBuild():
			result = "did not build"
		case p.noTests:
			result = "no test files"
		case result == "":
			result = "no result"
		}
		printf(w, "| %s | %s | %s |\n", name, result, seconds(p.elapsed))
	}
	for _, id := range r.buildOrder {
		if r.independentBuilds[id] {
			printf(w, "| %s | did not build | %s |\n", id, seconds(0))
		}
	}

	top, children := r.slowTests(o.slowest)
	writeTests(w, "Slowest top-level tests", top)
	writeTests(w, "Slowest subtests", children)

	r.writeSkippedTests(w)
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
	if len(problems) > 0 {
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
		for _, t := range p.testOrder {
			if t.outcome != "pass" && t.outcome != "fail" {
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

func (r *run) writeSkippedTests(w io.Writer) {
	heading := "\n### Skipped tests\n\n"
	for _, name := range r.order {
		p := r.packages[name]
		for _, t := range p.testOrder {
			if t.outcome == "skip" {
				if heading != "" {
					printf(w, "%s", heading)
					heading = ""
				}
				printf(w, "- %s %s\n", p.name, t.name)
			}
		}
	}
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
