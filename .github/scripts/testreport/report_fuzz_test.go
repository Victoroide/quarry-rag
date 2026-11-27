package main

import (
	"encoding/json"
	"fmt"
	"regexp"
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
	f.Add(`{"Action":"fail","Package":"a","FailedBuild":"dep [dep.test]"}` +
		`{"Action":"build-output","ImportPath":"dep [dep.test]","Output":"compiler diagnostic"}`)
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
