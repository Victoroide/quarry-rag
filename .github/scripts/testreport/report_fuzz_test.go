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
	f.Add("")

	omission := regexp.MustCompile(`^--- INCOMPLETE OUTPUT OMITTED: \d+ bytes, \d+ tests$`)
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

		if !strings.Contains(stderr, "--- INCOMPLETE OUTPUT OMITTED:") {
			return
		}
		// Clear user-controlled strings before checking reporter-generated markers.
		r := newRun()
		eventStream := json.NewDecoder(strings.NewReader(input))
		for {
			var ev event
			if err := eventStream.Decode(&ev); err != nil {
				break
			}
			if ev.Output != "" {
				ev.Output = "sanitized output\n"
			}
			if ev.Test != "" {
				ev.Test = fmt.Sprintf("Test%x", ev.Test)
			}
			r.add(ev)
		}
		for _, p := range r.packages {
			for line := range strings.SplitSeq(r.failureDetails(p), "\n") {
				if strings.HasPrefix(line, "--- INCOMPLETE OUTPUT OMITTED:") && !omission.MatchString(line) {
					t.Fatalf("invalid omission marker %q", line)
				}
			}
		}
	})
}
