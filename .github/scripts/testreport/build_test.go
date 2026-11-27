package main

import (
	"strings"
	"testing"
)

func TestSharedBuildDiagnosticAppearsOnce(t *testing.T) {
	first, second := module+"/first", module+"/second"
	code, stdout, stderr := report(t, fixture(t, "shared-build.json"), first, second)
	if code != 1 {
		t.Fatalf("exit status = %d, want 1", code)
	}
	header := "# " + module + "/dep\n"
	detail := "dep/dep.go:3:14: undefined: missingFixtureNumber\n"
	requireOnce(t, stderr, header, detail, "FAIL \t"+first+"\t", "FAIL \t"+second+"\t")
	requireContains(t, "diagnostics", stderr,
		"FAIL \t"+first+"\t0.00s\n"+header+detail+"FAIL \t"+second+"\t0.00s\n")
	requireAbsent(t, stderr, "FAIL \t"+module+"/dep\t")
	requireOnce(t, stdout, "| "+first+" | did not build |", "| "+second+" | did not build |")
}

func TestExpectedPackageWithOnlyAttributedBuildHasNoResult(t *testing.T) {
	const firstFragment = "compiler: undefined "
	const secondFragment = "identifier\n"
	for _, tc := range []struct {
		name          string
		id            string
		terminalFirst bool
		shared        bool
	}{
		{name: "plain seal first", id: "p"},
		{name: "plain terminal first", id: "p", terminalFirst: true},
		{name: "variant seal first", id: "p [p.test]"},
		{name: "variant terminal first", id: "p [p.test]", terminalFirst: true},
		{name: "shared plain seal first", id: "p", shared: true},
		{name: "shared variant terminal first", id: "p [p.test]", terminalFirst: true, shared: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			seal := event{Action: "build-fail", ImportPath: tc.id}
			fail := event{Action: "fail", Package: "q", FailedBuild: tc.id}
			events := []event{seal, fail}
			if tc.terminalFirst {
				events = []event{fail, seal}
			}
			if tc.shared {
				events = append([]event{
					{Action: "build-output", ImportPath: tc.id, Output: firstFragment},
					{Action: "build-output", ImportPath: tc.id, Output: secondFragment},
				}, events...)
				events = append(events, event{Action: "fail", Package: "r", FailedBuild: tc.id})
			}
			input := stream(t, events...)
			code, stdout, stderr := report(t, input, "p", "q")
			if code != 1 {
				t.Fatalf("exit status = %d, want 1", code)
			}
			problems := section(t, stdout, "Problems")
			requireOnce(t, stderr, "testreport: p has no result\n", "FAIL \tq\t")
			requireOnce(t, problems, "- p has no result\n", "- q did not build\n")
			requireOnce(t, stdout, "| q | did not build |")
			requireAbsent(t, stdout, "| p |", "- p did not build")
			if tc.shared {
				requireOnce(t, stderr, "FAIL \tr\t", firstFragment, secondFragment, firstFragment+secondFragment)
				requireOnce(t, problems, "- r did not build\n")
				requireOnce(t, stdout, "| r | did not build |")
			}

			code, stdout, stderr = report(t, input, "q")
			if code != 1 {
				t.Fatalf("exit status with unlisted p = %d, want 1", code)
			}
			requireAbsent(t, stdout, "| p |", "- p has no result", "- p did not build")
			requireAbsent(t, stderr, "testreport: p ", "FAIL \tp\t", "INCOMPLETE\tp\n")
		})
	}
}

func TestVariantBuildIDsRemainDistinct(t *testing.T) {
	pkg, first, second, tests := module+"/dep", module+"/first", module+"/second", module+"/tests"
	variant := pkg + " [" + pkg + ".test]"
	const common = "same compiler text\n"
	code, stdout, stderr := report(t, stream(t,
		event{Action: "build-output", ImportPath: pkg, Output: "ordinary build\n"},
		event{Action: "build-output", ImportPath: variant, Output: "test variant\n"},
		event{Action: "build-output", ImportPath: pkg, Output: common},
		event{Action: "build-output", ImportPath: variant, Output: common},
		event{Action: "build-fail", ImportPath: pkg},
		event{Action: "build-fail", ImportPath: variant},
		event{Action: "fail", Package: first, FailedBuild: pkg},
		event{Action: "output", Package: tests, Test: "TestFailure", Output: "assertion failed\n"},
		event{Action: "fail", Package: tests, Test: "TestFailure"},
		event{Action: "fail", Package: tests},
		event{Action: "pass", Package: pkg, FailedBuild: variant},
		event{Action: "fail", Package: second, FailedBuild: variant},
	))
	if code != 1 {
		t.Fatalf("exit status = %d, want 1", code)
	}
	requireOnce(t, stderr,
		"FAIL \t"+first+"\t0.00s\nordinary build\n"+common,
		"FAIL \t"+second+"\t0.00s\ntest variant\n"+common,
		"--- FAIL: TestFailure\nassertion failed\n", "ok   \t"+pkg+"\t")
	if count := strings.Count(stderr, common); count != 2 {
		t.Errorf("count of shared text = %d, want 2", count)
	}
	requireContains(t, "summary", stdout, "| 1 | 1 | 2 | 0 | 0 |",
		"| "+pkg+" | pass |", "| "+tests+" | fail |",
		"| "+first+" | did not build |", "| "+second+" | did not build |")
	requireAbsent(t, stderr, "FAIL \t"+pkg+"\t", "testreport: "+pkg+" did not build")
}

func TestBuildDiagnosticFallback(t *testing.T) {
	const (
		pkg     = module + "/dep"
		variant = pkg + " [" + pkg + ".test]"
		app     = module + "/app"
		healthy = module + "/ok"
	)
	output := func(id, fragment string) event {
		return event{Action: "build-output", ImportPath: id, Output: fragment}
	}
	seal := func(id string) event { return event{Action: "build-fail", ImportPath: id} }
	fail := event{Action: "fail", Package: app, FailedBuild: pkg}
	pass := event{Action: "pass", Package: healthy}
	appHeader := "FAIL \t" + app + "\t0.00s\n"
	pkgHeader := "FAIL \t" + pkg + "\t0.00s\n"
	passHeader := "ok   \t" + healthy + "\t0.00s\n"
	buildHeader := "BUILD FAIL\t" + pkg + "\n"
	for _, tc := range []struct {
		name       string
		events     []event
		expected   []string
		tail       string
		wantCode   int
		wantStderr string
		wantRows   []string
	}{
		{
			name: "build-only with text", events: []event{output(pkg, "compiler diagnostic\n"), seal(pkg)},
			expected: []string{pkg},
			wantCode: 1, wantStderr: pkgHeader + buildHeader + "compiler diagnostic\ntestreport: " + pkg + " did not build\n",
			wantRows: []string{"| " + pkg + " | did not build | 0.00 |", "- " + pkg + " did not build\n"},
		},
		{
			name: "build-only without text", events: []event{seal(pkg), output(pkg, "")},
			wantCode: 1, wantStderr: pkgHeader + "testreport: " + pkg + " did not build\n",
			wantRows: []string{"| " + pkg + " | did not build | 0.00 |"},
		},
		{
			name:     "output before terminal with missing build-fail",
			events:   []event{output(pkg, "compiler diagnostic\n"), fail, pass},
			wantCode: 1, wantStderr: appHeader + passHeader + buildHeader + "compiler diagnostic\ntestreport: " + app + " did not build\n",
			wantRows: []string{"| " + app + " | did not build | 0.00 |", "| " + healthy + " | pass | 0.00 |"},
		},
		{
			name:     "terminal before output and seal",
			events:   []event{fail, output(pkg, "compiler diagnostic\n"), seal(pkg), pass},
			wantCode: 1, wantStderr: appHeader + passHeader + buildHeader + "compiler diagnostic\n" +
				"testreport: " + app + " did not build\n",
			wantRows: []string{"| " + app + " | did not build | 0.00 |"},
		},
		{
			name:     "late suffix and repeated seal",
			events:   []event{output(pkg, "early\n"), seal(pkg), fail, output(pkg, "late\n"), seal(pkg), pass},
			wantCode: 1, wantStderr: appHeader + "early\n" + passHeader + buildHeader + "late\n" +
				"testreport: " + app + " did not build\n",
			wantRows: []string{"| " + app + " | did not build | 0.00 |"},
		},
		{
			name: "malformed tail with first-seen build order",
			events: []event{output(pkg, "early\n"), seal(pkg), fail,
				output(variant, "first pending build\n"), output(healthy, "second pending build\n"),
				seal(healthy), seal(variant), output(pkg, "late\n")},
			tail: "{", wantCode: 1,
			wantStderr: appHeader + "early\n" + pkgHeader + "FAIL \t" + healthy + "\t0.00s\n" +
				"BUILD FAIL\t" + pkg + "\nlate\nBUILD FAIL\t" + variant + "\nfirst pending build\n" +
				"BUILD FAIL\t" + healthy + "\nsecond pending build\n" +
				"read go test events: unexpected EOF\n",
		},
		{
			name:     "sealed build has no text at terminal failure",
			events:   []event{seal(pkg), fail, pass, output(pkg, "first late fragment\r\n")},
			wantCode: 1, wantStderr: appHeader + passHeader + buildHeader + "first late fragment\r\n" +
				"testreport: " + app + " did not build\n",
			wantRows: []string{"| " + app + " | did not build | 0.00 |"},
		},
		{
			name:       "output-only success and empty IDs",
			events:     []event{output(pkg, "unconfirmed build output\n"), output("", "empty ID output\n"), seal(""), pass},
			wantStderr: passHeader,
			wantRows:   []string{"| 1 | 0 | 0 | 0 | 0 |", "| " + healthy + " | pass | 0.00 |"},
		},
		{
			name: "interleaved unicode fragments stay with their exact ID",
			events: []event{output(variant, "資料"), output("", "empty ID output\n"), seal(""),
				output(pkg, "unconfirmed build output\n"), output(variant, "⟶ café\n"), seal(variant), pass},
			wantCode: 1, wantStderr: passHeader + pkgHeader + "BUILD FAIL\t" + variant + "\n資料⟶ café\n" +
				"testreport: " + pkg + " did not build\n",
			wantRows: []string{"| " + pkg + " | did not build | 0.00 |"},
		},
		{
			name:     "two build-only variants sharing a display name",
			events:   []event{output(variant, "variant output\n"), output(pkg, "ordinary output\n"), seal(pkg), seal(variant)},
			wantCode: 1, wantStderr: pkgHeader + "BUILD FAIL\t" + variant + "\nvariant output\n" +
				buildHeader + "ordinary output\ntestreport: " + pkg + " did not build\n" +
				"testreport: " + variant + " did not build\n",
			wantRows: []string{"| " + pkg + " | did not build | 0.00 |", "| " + variant + " | did not build | 0.00 |"},
		},
		{
			name: "terminal test outcome wins over a later synthetic entry",
			events: []event{{Action: "output", Package: pkg, Output: "ordinary test failure\n"},
				{Action: "fail", Package: pkg}, output(variant, "variant compiler failure\n"), seal(variant)},
			wantCode: 1, wantStderr: pkgHeader + "ordinary test failure\nBUILD FAIL\t" + variant +
				"\nvariant compiler failure\ntestreport: " + pkg + " failed\n" +
				"testreport: " + variant + " did not build\n",
			wantRows: []string{"| " + pkg + " | fail | 0.00 |", "| " + variant + " | did not build | 0.00 |"},
		},
		{
			name: "compiler detail without an eligible package header",
			events: []event{{Action: "output", Package: app, Output: "[no test files]\n"}, fail,
				output(pkg, "compiler diagnostic\n")},
			wantCode: 1, wantStderr: "BUILD FAIL\t" + pkg + "\ncompiler diagnostic\ntestreport: " + app + " did not build\n",
			wantRows: []string{"| " + app + " | did not build | 0.00 |"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code, stdout, stderr := report(t, stream(t, tc.events...)+tc.tail, tc.expected...)
			if code != tc.wantCode || stderr != tc.wantStderr {
				t.Errorf("got exit %d and stderr %q, want exit %d and stderr %q", code, stderr, tc.wantCode, tc.wantStderr)
			}
			if tc.tail != "" && stdout != "" {
				t.Errorf("malformed stream wrote a summary: %q", stdout)
			}
			requireOnce(t, stdout, tc.wantRows...)
		})
	}
}

func TestDeferredBuildDiagnosticKeepsItsIdentity(t *testing.T) {
	const (
		variant = "p [p.test]"
		label   = "BUILD FAIL\t" + variant + "\n"
	)
	output := event{Action: "build-output", ImportPath: variant, Output: "VARIANT DIAGNOSTIC\n"}
	seal := event{Action: "build-fail", ImportPath: variant}
	fail := event{Action: "fail", Package: "p", FailedBuild: variant}
	for _, tc := range []struct {
		name   string
		events []event
		before string
	}{
		{"pass after unsealed build", []event{output, fail, {Action: "pass", Package: "z"}},
			"ok   \tz\t0.00s\n" + label},
		{"ordinary failure after multiple dependants", []event{output, fail,
			{Action: "fail", Package: "q", FailedBuild: variant},
			{Action: "output", Package: "z", Output: "ordinary test failure"},
			{Action: "fail", Package: "z"}},
			"FAIL \tz\t0.00s\nordinary test failure\n" + label},
		{"late unsealed output", []event{fail, output, {Action: "pass", Package: "z"}},
			"ok   \tz\t0.00s\n" + label},
		{"late output after seal", []event{seal, fail, output, {Action: "pass", Package: "z"}},
			"ok   \tz\t0.00s\n" + label},
		{"incomplete package after standalone failure", []event{
			{Action: "build-output", ImportPath: "p", Output: output.Output},
			{Action: "build-fail", ImportPath: "p"},
			{Action: "start", Package: "z"}},
			"INCOMPLETE\tz\nBUILD FAIL\tp\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, tail := range []string{"", "{"} {
				code, stdout, stderr := report(t, stream(t, tc.events...)+tail)
				if code != 1 {
					t.Errorf("exit status = %d, want 1", code)
				}
				requireOnce(t, stderr, tc.before+output.Output)
				if tail != "" {
					requireOnce(t, stderr, output.Output+"read go test events: unexpected EOF\n")
					if stdout != "" {
						t.Errorf("malformed stream wrote a summary: %q", stdout)
					}
				}
			}
		})
	}
}

func TestCompilerFragmentsStayTogetherAcrossLineBoundaries(t *testing.T) {
	const variant = "p [p.test]"
	for _, terminal := range []bool{false, true} {
		t.Run(map[bool]string{false: "deferred", true: "terminal"}[terminal], func(t *testing.T) {
			events := []event{
				{Action: "build-output", ImportPath: "p", Output: "no"},
				{Action: "build-output", ImportPath: variant, Output: "variant text"},
				{Action: "build-output", ImportPath: "p", Output: " newline"},
				{Action: "build-fail", ImportPath: "p"},
				{Action: "build-fail", ImportPath: variant},
			}
			boundary := "BUILD FAIL\t" + variant + "\n"
			if terminal {
				events = append(events,
					event{Action: "fail", Package: "p", FailedBuild: "p"},
					event{Action: "fail", Package: "q", FailedBuild: variant},
				)
				boundary = "FAIL \tq\t0.00s\n"
			}
			for _, tail := range []string{"", "{"} {
				code, _, stderr := report(t, stream(t, events...)+tail)
				if code != 1 {
					t.Errorf("exit status = %d, want 1", code)
				}
				requireOnce(t, stderr, "no newline\n"+boundary+"variant text\n")
				if tail != "" {
					requireOnce(t, stderr, "variant text\nread go test events: unexpected EOF\n")
				}
			}
		})
	}
}

func TestStandaloneBuildFailureSurvivesTerminalCollision(t *testing.T) {
	for _, id := range []string{"p", "p [p.test]"} {
		for _, outcome := range []string{"pass", "skip"} {
			t.Run(id+"/"+outcome, func(t *testing.T) {
				for _, terminalFirst := range []bool{false, true} {
					for _, detail := range []string{"", "compiler failure\n"} {
						terminal := event{Action: outcome, Package: "p"}
						events := []event{
							{Action: "build-output", ImportPath: id, Output: detail},
							{Action: "build-fail", ImportPath: id},
						}
						if terminalFirst {
							events = append([]event{terminal}, events...)
						} else {
							events = append(events, terminal)
						}
						code, stdout, stderr := report(t, stream(t, events...))
						if code != 1 {
							t.Errorf("terminal first = %t, detail = %q: exit status = %d, want 1", terminalFirst, detail, code)
						}
						requireOnce(t, stderr, "ok   \tp\t0.00s\n", "BUILD FAIL\t"+id+"\n"+detail,
							"testreport: "+id+" did not build\n")
						requireOnce(t, stdout, "| 1 | 0 | 1 | 0 | 0 |", "| p | "+outcome+" | 0.00 |",
							"| "+id+" | did not build | 0.00 |")
					}
				}
			})
		}
	}
}

func TestMultipleStandaloneBuildFailuresDoNotRepeatReferencedBuild(t *testing.T) {
	const standalone = "p [q.test]"
	const referenced = "p [r.test]"
	code, stdout, stderr := report(t, stream(t,
		event{Action: "build-fail", ImportPath: "p"},
		event{Action: "build-fail", ImportPath: standalone},
		event{Action: "build-fail", ImportPath: standalone},
		event{Action: "build-fail", ImportPath: referenced},
		event{Action: "fail", Package: "q", FailedBuild: referenced},
		event{Action: "pass", Package: "p"},
	))
	if code != 1 {
		t.Fatalf("exit status = %d, want 1", code)
	}
	requireOnce(t, stderr, "BUILD FAIL\tp\n", "BUILD FAIL\t"+standalone+"\n", "FAIL \tq\t")
	requireOnce(t, stdout, "| 1 | 0 | 3 | 0 | 0 |", "| p | pass |",
		"| p | did not build |", "| "+standalone+" | did not build |", "| q | did not build |")
	requireAbsent(t, stdout, referenced)
	requireAbsent(t, stderr, referenced)
}
