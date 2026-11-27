package main

import (
	"encoding/json"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"testing"
	"unicode/utf8"
)

func FuzzDecodeTestReportEvents(f *testing.F) {
	f.Add(fixture(f, "timeout.json"))
	f.Add(`{"Action":"start","Package":"a"}{"Action":"output","Package":"a","Test":"TestFault","Output":"partial"}{`)
	f.Add(`{"Action":"run","Package":"a","Test":"TestA"}{"Action":"run","Package":"b","Test":"TestB"}{"Action":"pass","Package":"a"}`)
	f.Add(strings.Repeat(`{"Action":"run","Package":"a","Test":"TestRepeated"}`, 129))
	sharedBuild := fixture(f, "shared-build.json")
	f.Add(sharedBuild)
	f.Add(sharedBuild + `{"Action":"build-output"`)
	collision := []event{
		{Action: "start", Package: "p"},
		{Action: "run", Package: "p", Test: "TestP"},
		{Action: "output", Package: "p", Test: "TestP", Output: "unfinished test output\n"},
		{Action: "build-output", ImportPath: "p [p.test]", Output: "compiler diagnostic\n"},
		{Action: "build-fail", ImportPath: "p [p.test]"},
	}
	f.Add(stream(f, collision...))
	f.Add(stream(f, append(collision, event{Action: "fail", Package: "q", FailedBuild: "p [p.test]"})...))
	f.Add(`{"Action":"fail","Package":"a","FailedBuild":"dep [dep.test]"}` +
		`{"Action":"build-output","ImportPath":"dep [dep.test]","Output":"compiler diagnostic"}`)
	marker := event{Action: "output", Package: "a", Output: "?   \ta\t[no test files]\n"}
	f.Add(stream(f, marker, event{Action: "fail", Package: "a"}))
	f.Add(stream(f, marker))
	f.Add(stream(f, marker,
		event{Action: "output", Package: "b", Output: marker.Output},
		event{Action: "skip", Package: "b"},
		event{Action: "skip", Package: "a"}))
	f.Add(stream(f, event{Action: "output", Package: "a", Output: marker.Output[:9]},
		event{Action: "output", Package: "a", Output: marker.Output[9:]},
		event{Action: "bench", Package: "a", Test: "BenchmarkProbe"},
		event{Action: "skip", Package: "a"}))
	f.Add("")

	omission := regexp.MustCompile(`^--- INCOMPLETE OUTPUT OMITTED: \d+ bytes, \d+ tests$`)
	fragment := regexp.MustCompile(`<build-fragment:\d+>`)
	f.Fuzz(func(t *testing.T, input string) {
		if len(input) > 64*1024 {
			t.Skip("input exceeds 64 KiB")
		}
		code, stdout, stderr := report(t, input)
		if code != 0 && code != 1 {
			t.Fatalf("exit status = %d, want 0 or 1", code)
		}
		if !utf8.ValidString(stdout) || !utf8.ValidString(stderr) {
			t.Fatal("report is invalid UTF-8")
		}
		if stdout == "" {
			if code != 1 || !strings.Contains(stderr, "read go test events:") {
				t.Fatalf("empty summary without a decode error, exit %d", code)
			}
		} else if !strings.HasPrefix(stdout, "## Race tests\n") {
			t.Fatal("invalid summary heading")
		}

		// Clear user-controlled strings before checking generated markers.
		var events []event
		var tail string
		eventStream := json.NewDecoder(strings.NewReader(input))
		for {
			var ev event
			if err := eventStream.Decode(&ev); err != nil {
				if stdout == "" {
					tail = "{"
				}
				break
			}
			switch {
			case ev.Action == "build-output":
				ev.Output = fmt.Sprintf("<build-fragment:%d>\n", len(events))
			case ev.Output != "":
				ev.Output = "sanitized output\n"
			}
			if ev.Test != "" {
				ev.Test = fmt.Sprintf("Test%x", ev.Test)
			}
			ev.Package = fmt.Sprintf("%x", ev.Package)
			ev.ImportPath = fmt.Sprintf("%x", ev.ImportPath)
			ev.FailedBuild = fmt.Sprintf("%x", ev.FailedBuild)
			events = append(events, ev)
		}
		_, _, sanitized := report(t, stream(t, events...)+tail)
		seen := map[string]bool{}
		for _, match := range fragment.FindAllString(sanitized, -1) {
			if seen[match] {
				t.Fatalf("build output fragment %q was emitted twice", match)
			}
			seen[match] = true
		}
		for line := range strings.SplitSeq(sanitized, "\n") {
			if strings.HasPrefix(line, "--- INCOMPLETE OUTPUT OMITTED:") && !omission.MatchString(line) {
				t.Fatalf("invalid omission marker %q", line)
			}
		}
	})
}

func FuzzIncompletePackageBuildCollision(f *testing.F) {
	f.Add(uint8(1), uint8(7), true, true, false)
	f.Add(uint8(2), uint8(2), false, false, false)
	f.Add(uint8(0), uint8(1), false, true, true)
	f.Fuzz(func(t *testing.T, variant, fragmentSize uint8, buildFirst, referenced, malformed bool) {
		const detail = "<compiler:first><compiler:last>\r\n"
		id := "p"
		if variant != 0 {
			id = fmt.Sprintf("p [probe%d.test]", variant)
		}
		activity := []event{
			{Action: "start", Package: "p"},
			{Action: "output", Package: "p", Test: "TestP", Output: "<test:unfinished>\n"},
		}
		var build []event
		width := max(1, int(fragmentSize))
		for i := 0; i < len(detail); i += width {
			build = append(build, event{Action: "build-output", ImportPath: id, Output: detail[i:min(i+width, len(detail))]})
		}
		build = append(build, event{Action: "build-fail", ImportPath: id})
		var terminal []event
		buildRow := id
		buildHeader := "BUILD FAIL\t" + id + "\n"
		if referenced {
			terminal = append(terminal, event{Action: "fail", Package: "q", FailedBuild: id})
			buildRow = "q"
			if buildFirst {
				buildHeader = "FAIL \tq\t0.00s\n"
			}
		}
		events := slices.Concat(activity, terminal, build)
		if buildFirst {
			events = slices.Concat(build, terminal, activity)
		}
		input := stream(t, events...)
		if malformed {
			input += "{"
		}
		code, stdout, stderr := report(t, input, "p")
		if code != 1 {
			t.Fatalf("exit status = %d, want 1", code)
		}
		requireOnce(t, stderr, "INCOMPLETE\tp\n--- INCOMPLETE: TestP\n<test:unfinished>\n",
			buildHeader+detail, "<compiler:first>", "<compiler:last>")
		requireAbsent(t, stderr, "FAIL \tp\t")
		if malformed {
			if stdout != "" || !strings.HasSuffix(stderr, "read go test events: unexpected EOF\n") {
				t.Fatalf("malformed stream summary = %q, diagnostics = %q", stdout, stderr)
			}
			return
		}
		requireOnce(t, stdout, "| 0 | 0 | 1 | 0 | 1 |", "| p | no result | 0.00 |", "- p has no result\n",
			"| "+buildRow+" | did not build | 0.00 |", "- "+buildRow+" did not build\n")
		requireOnce(t, stderr, "testreport: p has no result\n", "testreport: "+buildRow+" did not build\n")
		if referenced {
			requireAbsent(t, stdout, "| "+id+" | did not build |", "- "+id+" did not build\n")
		}
	})
}

func FuzzNoTestResultLine(f *testing.F) {
	const line = "?   \tprobe\t[no test files]"
	for _, output := range []string{
		line + "\n", line + "\r\n", "?   \tother\t[no test files]\n",
		"diagnostic: " + line + "\n", line + " extra\n", line, line + "\r",
		"?  \tprobe\t[no test files]\n", line + " \n", line + "\n\n", line + "\r\r\n",
		"other line\n" + line + "\n", line + "\nother line\n", "[no test files]\n", "?   \tprobe\t",
		line + "\xff\n",
	} {
		f.Add("probe", output, uint16(64))
		f.Add("probe", output, uint16(1))
	}
	for _, name := range []string{"", "pro\tbe", "pro\rbe", "pro\nbe", "pro\xffbe"} {
		f.Add(name, "?   \t"+name+"\t[no test files]\n", uint16(7))
	}
	f.Fuzz(func(t *testing.T, name, output string, fragmentSize uint16) {
		if len(name) > 64*1024 || len(output) > 64*1024 {
			t.Skip("input exceeds 64 KiB")
		}
		want := false
		for lineWithEnding := range strings.SplitAfterSeq(output, "\n") {
			line, ended := strings.CutSuffix(lineWithEnding, "\n")
			fields := strings.Split(strings.TrimSuffix(line, "\r"), "\t")
			if ended && !strings.ContainsAny(name, "\r\n") && len(fields) == 3 &&
				fields[0] == "?   " && fields[1] != "" && fields[1] == name && fields[2] == "[no test files]" {
				want = true
				break
			}
		}
		p := packageResult{name: name}
		width := max(1, int(fragmentSize))
		for i := 0; i < len(output); i += width {
			p.matchNoTestOutput(output[i:min(i+width, len(output))])
		}
		if p.noTestResultLine != want {
			t.Errorf("package %q with output %q in fragments of %d bytes matched = %t, want %t",
				name, output, width, p.noTestResultLine, want)
		}
	})
}
