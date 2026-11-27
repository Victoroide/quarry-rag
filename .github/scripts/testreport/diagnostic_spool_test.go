package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

var errTestSpool = errors.New("denied: /private/path/secret")

type spoolProbe struct {
	spoolFile
	write     func(*spoolProbe, []byte, int64) (int, error)
	read      func(*spoolProbe, []byte, int64) (int, error)
	closeErr  error
	highWater int64
	closes    int
	writes    int
	reads     int
}

func (p *spoolProbe) WriteAt(data []byte, at int64) (int, error) {
	p.writes++
	var n int
	var err error
	if p.write != nil {
		n, err = p.write(p, data, at)
	} else {
		n, err = p.spoolFile.WriteAt(data, at)
	}
	p.highWater = max(p.highWater, at+int64(n))
	return n, err
}

func (p *spoolProbe) ReadAt(data []byte, at int64) (int, error) {
	p.reads++
	if p.read != nil {
		return p.read(p, data, at)
	}
	return p.spoolFile.ReadAt(data, at)
}

func (p *spoolProbe) Close() error {
	p.closes++
	return errors.Join(p.spoolFile.Close(), p.closeErr)
}

func TestDiagnosticSpillReusesPassedAndSkippedSpace(t *testing.T) {
	const limit = 8 * spoolPageSize
	const pkg = "example.test/reuse"
	const failure = "retained failure:\n"
	noise := strings.Repeat("discarded diagnostic ", 53) + "\n"
	for _, failing := range []bool{false, true} {
		t.Run(fmt.Sprint(failing), func(t *testing.T) {
			cycles := []io.Reader{strings.NewReader(stream(t, event{Action: "start", Package: pkg}))}
			for i := range 8 {
				if failing && i == 1 {
					cycles = append(cycles, strings.NewReader(stream(t,
						event{Action: "output", Package: pkg, Test: "TestFailure", Output: failure},
						event{Action: "fail", Package: pkg, Test: "TestFailure"})))
				}
				output := func(test string) event { return event{Action: "output", Package: pkg, Test: test, Output: noise} }
				terminal := func(action, test string) event { return event{Action: action, Package: pkg, Test: test} }
				cycles = append(cycles, strings.NewReader(stream(t,
					terminal("run", "TestRepeat"), output("TestRepeat"), terminal("pass", "TestRepeat"),
					terminal("run", "TestRepeat"), output("TestRepeat"), terminal("skip", "TestRepeat"),
					terminal("run", "BenchmarkTerminal"), output("BenchmarkTerminal"), terminal("bench", "BenchmarkTerminal"),
					terminal("run", "BenchmarkMeasure"), output("BenchmarkMeasure"),
					event{Action: "output", Package: pkg, Test: "BenchmarkMeasure", Output: "BenchmarkMeasure-8\t1\t2 ns/op\n"},
					terminal("run", "BenchmarkParent"), output("BenchmarkParent"),
					terminal("run", "BenchmarkParent/child"), terminal("bench", "BenchmarkParent/child"),
					terminal("run", "BenchmarkNext"), terminal("bench", "BenchmarkNext"),
					output(""), terminal("pass", ""), output(""), terminal("skip", ""))))
			}
			if failing {
				cycles = append(cycles, strings.NewReader(stream(t,
					event{Action: "output", Package: pkg, Test: "TestFailure", Output: "retained suffix\n"},
					event{Action: "fail", Package: pkg})))
			}
			var creates int
			var path string
			var probe *spoolProbe
			dir := t.TempDir()
			factory := func() (spoolFile, func() error, error) {
				creates++
				f, err := os.CreateTemp(dir, "spool-*")
				if err != nil {
					return nil, nil, err
				}
				path = f.Name()
				probe = &spoolProbe{spoolFile: f}
				return probe, func() error { return os.Remove(path) }, nil
			}
			var stderr, summary bytes.Buffer
			code := runReport(options{spill: true}, io.MultiReader(cycles...), &summary, &stderr, factory, limit)
			wantCode := 0
			want := strings.Repeat("ok   \t"+pkg+"\t0.00s\n", 16)
			if failing {
				wantCode = 1
				want += "FAIL \t" + pkg + "\t0.00s\n--- FAIL: TestFailure\n" + failure + "retained suffix\ntestreport: " + pkg + " failed\n"
			}
			if code != wantCode || stderr.String() != want || creates != 1 || probe.closes != 1 || probe.highWater > limit {
				t.Fatalf("exit %d, creates %d, diagnostics %q (want %q)", code, creates, stderr.String(), want)
			}
			if _, err := os.Stat(path); !os.IsNotExist(err) {
				t.Fatalf("spool remains: %v", err)
			}
		})
	}
}

func TestDiagnosticReusePreservesReplay(t *testing.T) {
	const pkg = "running"
	const old = "old low-priority diagnostic\n"
	const newest = "newest diagnostic on a lower physical page\n"
	large := strings.Repeat("界", volumeChunk/3+20) + "\n"
	output := func(name, text string) event { return event{Action: "output", Package: pkg, Test: name, Output: text} }
	events := []event{
		output("TestRelease", strings.Repeat("r", len(large))),
		output("TestFirst", old),
		output("TestSecond", "second pending\n"),
		{Action: "build-output", ImportPath: "compiler", Output: "compiler prefix"},
		{Action: "build-fail", ImportPath: "compiler"},
		{Action: "fail", Package: "dependent", FailedBuild: "compiler"},
	}
	for range 15 {
		events = append(events, output("TestFirst", large))
	}
	events = append(events,
		event{Action: "pass", Package: pkg, Test: "TestRelease"},
		output("TestSecond", newest),
		output("TestSecond", strings.Repeat("x", volumeChunk-1)+"界\n"),
		event{Action: "build-output", ImportPath: "compiler", Output: " and suffix\n"},
		event{Action: "build-fail", ImportPath: "compiler"},
		event{Action: "fail", Package: "second-dependent", FailedBuild: "compiler"},
		event{Action: "fail", Package: "third-dependent", FailedBuild: "compiler"},
	)
	for i := range maxIncompleteTests {
		events = append(events, event{Action: "run", Package: pkg, Test: fmt.Sprintf("TestPending%03d", i)})
	}
	input := stream(t, events...)
	var legacy, spilled, legacySummary, spilledSummary bytes.Buffer
	if code := execute(nil, strings.NewReader(input), &legacySummary, &legacy); code != 1 {
		t.Fatalf("legacy exit %d", code)
	}
	if code := runReport(options{spill: true, slowest: 10}, strings.NewReader(input), &spilledSummary, &spilled,
		createDiagnosticSpool, 2*maxIncompleteBytes); code != 1 {
		t.Fatalf("spill exit %d", code)
	}
	if !bytes.Equal(spilled.Bytes(), legacy.Bytes()) || !bytes.Equal(spilledSummary.Bytes(), legacySummary.Bytes()) ||
		!utf8.Valid(spilled.Bytes()) {
		t.Fatalf("replay mismatch: stderr %d/%d bytes, summary %d/%d bytes, valid UTF-8 %t",
			spilled.Len(), legacy.Len(), spilledSummary.Len(), legacySummary.Len(), utf8.Valid(spilled.Bytes()))
	}
	requireOnce(t, spilled.String(), "compiler prefix", " and suffix\n", newest, "--- INCOMPLETE OUTPUT OMITTED:")
	requireAbsent(t, spilled.String(), old, "TestPending127", strings.Repeat("r", 50), "diagnostic storage")
}

func TestDiagnosticReusePackageRelease(t *testing.T) {
	for _, tc := range []struct{ name, output, terminal string }{
		{"pass", "package stdout\n", "pass"},
		{"cached", "ok   \tpackage\t(cached)\n", "pass"},
		{"skip", "package stdout\n", "skip"},
		{"no-test skip", "?   \tpackage\t[no test files]\n", "skip"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const pkg = "package"
			input := stream(t,
				event{Action: "output", Package: pkg, Output: tc.output},
				event{Action: tc.terminal, Package: pkg},
				event{Action: "output", Package: pkg, Output: "late package output\n"},
				event{Action: "fail", Package: pkg})
			var stderr bytes.Buffer
			if code := runReport(options{spill: true}, strings.NewReader(input), io.Discard, &stderr,
				createDiagnosticSpool, spoolPageSize); code != 1 {
				t.Fatalf("exit %d", code)
			}
			requireOnce(t, stderr.String(), "late package output\n")
			requireAbsent(t, stderr.String(), tc.output, "diagnostic storage", "OMITTED")
		})
	}
}

func TestDiagnosticReuseFailurePreservesCommittedOutput(t *testing.T) {
	// A reused page at offset zero holds the continuation of the second
	// header and its payload. The committed prefix remains on page one.
	const headerRemainder = spoolHeaderSize - 10
	prefix := strings.Repeat("p", spoolPageSize-spoolHeaderSize-11) + "\n"
	const tail = "界 recycled fragment\n"
	for _, tc := range []struct {
		name, failure string
		write, read   func(*spoolProbe, []byte, int64) (int, error)
		wantTail      string
	}{
		{"short split header write", "write failed", failWriteAt(0, 3), nil, tail},
		{"failed payload write", "write failed", failWriteAt(headerRemainder, 0), nil, tail},
		{"torn previous link", "write failed", failWriteAt(spoolPageSize+4, 1), nil, tail},
		{"short split header read", "read failed", nil, shortReadAt(0, 3), fmt.Sprintf("--- DIAGNOSTIC OUTPUT OMITTED: %d bytes\n", len(tail))},
		{"short reused payload read", "read failed", nil, shortReadAt(headerRemainder, 2), fmt.Sprintf("--- DIAGNOSTIC OUTPUT OMITTED: %d bytes\n", len(tail))},
		{"foreign id on recycled page", "read invalid record", nil, modifyRead(0, headerRemainder, func(raw []byte) {
			binary.LittleEndian.PutUint32(raw[2:6], 1)
		}), fmt.Sprintf("--- DIAGNOSTIC OUTPUT OMITTED: %d bytes\n", len(tail))},
		{"old ordinal on recycled page", "read invalid link", nil, modifyRead(0, headerRemainder, func(raw []byte) {
			binary.LittleEndian.PutUint64(raw[6:], 1)
		}), fmt.Sprintf("--- DIAGNOSTIC OUTPUT OMITTED: %d bytes\n", len(tail))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, probe, _ := newProbedDiagnosticStore(t, 2*spoolPageSize)
			var released, kept diagnosticRef
			s.append(&released, "unrelated discarded output\n")
			s.append(&kept, prefix)
			s.discard(&released)
			probe.write, probe.read = tc.write, tc.read
			s.append(&kept, tail)
			verifyPageOutput(t, s, &kept, prefix+tc.wantTail)
			if s.failure != tc.failure {
				t.Fatalf("failure = %q, want %q", s.failure, tc.failure)
			}
			assertPageOwners(t, s, []diagnosticRef{kept, released})
			writes := probe.writes
			s.discard(&kept)
			s.append(&released, "fresh fallback only\n")
			verifyPageOutput(t, s, &released, "fresh fallback only\n")
			if s.failure != tc.failure || probe.writes != writes {
				t.Fatal("a later release changed the first error or restarted disk writes")
			}
		})
	}
}

func TestDiagnosticBuildCursorRejectsChangedOrdinal(t *testing.T) {
	s, probe, _ := newProbedDiagnosticStore(t, 2*spoolPageSize)
	var earlier, build diagnosticRef
	s.append(&earlier, "another stream")
	s.append(&build, "compiler prefix\n")
	var out bytes.Buffer
	w := &diagnosticWriter{Writer: &out}
	var cursor diagnosticCursor
	s.replay(w, &build, &cursor)
	s.discard(&earlier)
	const suffix = "compiler suffix\n"
	s.append(&build, suffix)
	probe.read = modifyHeader(spoolPageSize, func(raw []byte) { binary.LittleEndian.PutUint64(raw[16:], 1) })
	s.replay(w, &build, &cursor)
	want := fmt.Sprintf("compiler prefix\n--- DIAGNOSTIC OUTPUT OMITTED: %d bytes\n", len(suffix))
	if got := out.String(); got != want || s.failure != "read invalid link" {
		t.Fatalf("replay %q, failure %q", got, s.failure)
	}
}

func TestDiagnosticTornLinkIsScopedToItsChain(t *testing.T) {
	s, probe, _ := newProbedDiagnosticStore(t, 2*spoolPageSize)
	var first, peer diagnosticRef
	s.append(&first, "committed first\n")
	s.append(&peer, "committed peer\n")
	probe.write = failWriteAt(4, 1)
	s.append(&first, "fallback first\n")
	probe.read = modifyHeader(spoolPageSize, func(raw []byte) { binary.LittleEndian.PutUint32(raw[4:8], 2) })
	verifyPageOutput(t, s, &first, "committed first\nfallback first\n")
	verifyPageOutput(t, s, &peer, fmt.Sprintf("--- DIAGNOSTIC OUTPUT OMITTED: %d bytes\n", len("committed peer\n")))
	if s.failure != "write failed" {
		t.Fatalf("failure = %q", s.failure)
	}
}

func TestDiagnosticRingRecencyFollowsDiskOrdinal(t *testing.T) {
	s, _, _ := newProbedDiagnosticStore(t, maxIncompleteBytes)
	s.ordinal = spoolLimit
	r := newRun()
	r.store = s
	old := &testResult{name: "TestOld"}
	recent := &testResult{name: "TestRecent"}
	s.append(&old.output.ref, strings.Repeat("x", maxIncompleteBytes-spoolHeaderSize))
	s.append(&recent.output.ref, "new fallback diagnostic\n")
	var out bytes.Buffer
	r.writeIncompleteDetails(&diagnosticWriter{Writer: &out}, []*testResult{old, recent})
	requireOnce(t, out.String(), "--- INCOMPLETE: TestRecent\nnew fallback diagnostic\n")
	if s.failure != "write quota exceeded" || out.Len() > maxIncompleteBytes {
		t.Fatalf("failure %q, %d output bytes", s.failure, out.Len())
	}
}

func TestDiagnosticSpillDegradesWithBoundedTail(t *testing.T) {
	const pkg = "example.test/failing"
	const test = "TestFail"
	input := stream(t,
		event{Action: "output", Package: pkg, Test: test, Output: "first-sensitive\n"},
		event{Action: "output", Package: pkg, Test: "TestOther", Output: "other-stream\n"},
		event{Action: "output", Package: pkg, Test: test, Output: "tail\n"},
		event{Action: "fail", Package: pkg, Test: test},
		event{Action: "pass", Package: pkg, Test: "TestOther"},
		event{Action: "fail", Package: pkg},
	)
	const thirdRecord = int64(spoolHeaderSize + len("first-sensitive\n"))
	for _, tc := range []struct {
		name, reason                   string
		createErr, closeErr, removeErr error
		read, write                    func(*spoolProbe, []byte, int64) (int, error)
		want                           []string
	}{
		{name: "create denied", reason: "create failed", createErr: errTestSpool, want: []string{"first-sensitive\ntail\n"}},
		{name: "disk full", reason: "write failed", write: failWriteAt(thirdRecord, 0), want: []string{"first-sensitive\ntail\n"}},
		{name: "short payload", reason: "write failed", write: failWriteAt(thirdRecord+spoolHeaderSize, 2), want: []string{"first-sensitive\ntail\n"}},
		{name: "short link patch", reason: "write failed", write: failWriteAt(4, 1), want: []string{"first-sensitive\ntail\n"}},
		{name: "short header", reason: "read failed", read: shortReadAt(thirdRecord, 4), want: []string{"first-sensitive\n--- DIAGNOSTIC OUTPUT OMITTED: 5 bytes\n"}},
		{name: "wrong stream", reason: "read invalid record", read: modifyHeader(thirdRecord, func(raw []byte) { binary.LittleEndian.PutUint32(raw[12:16], 2) }), want: []string{"first-sensitive\n--- DIAGNOSTIC OUTPUT OMITTED: 5 bytes\n"}},
		{name: "zero ordinal", reason: "read invalid record", read: modifyHeader(thirdRecord, func(raw []byte) { binary.LittleEndian.PutUint64(raw[16:], 0) }), want: []string{"first-sensitive\n--- DIAGNOSTIC OUTPUT OMITTED: 5 bytes\n"}},
		{name: "duplicate ordinal", reason: "read invalid link", read: modifyHeader(thirdRecord, func(raw []byte) { binary.LittleEndian.PutUint64(raw[16:], 1) }), want: []string{"first-sensitive\n--- DIAGNOSTIC OUTPUT OMITTED: 5 bytes\n"}},
		{name: "nonreciprocal link", reason: "read invalid link", read: modifyHeader(thirdRecord, func(raw []byte) { binary.LittleEndian.PutUint32(raw[:4], 2) }), want: []string{"first-sensitive\n--- DIAGNOSTIC OUTPUT OMITTED: 5 bytes\n"}},
		{name: "self link", reason: "read invalid record", read: modifyHeader(0, func(raw []byte) { binary.LittleEndian.PutUint32(raw[4:8], 1) }), want: []string{"--- DIAGNOSTIC OUTPUT OMITTED: 21 bytes\n"}},
		{name: "uncommitted link on healthy tail", reason: "read invalid record", read: modifyHeader(thirdRecord, func(raw []byte) { binary.LittleEndian.PutUint32(raw[4:8], 1) }), want: []string{"first-sensitive\n--- DIAGNOSTIC OUTPUT OMITTED: 5 bytes\n"}},
		{name: "payload out of extent", reason: "read invalid record", read: modifyHeader(thirdRecord, func(raw []byte) { binary.LittleEndian.PutUint32(raw[8:12], spoolLimit) }), want: []string{"first-sensitive\n--- DIAGNOSTIC OUTPUT OMITTED: 5 bytes\n"}},
		{name: "payload read failure", reason: "read failed", read: shortReadAt(thirdRecord+spoolHeaderSize, 1), want: []string{"first-sensitive\n--- DIAGNOSTIC OUTPUT OMITTED: 5 bytes\n"}},
		{name: "close failure", reason: "close failed", closeErr: errTestSpool, want: []string{"first-sensitive\ntail\n"}},
		{name: "remove failure", reason: "remove failed", removeErr: errTestSpool, want: []string{"first-sensitive\ntail\n"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			var path string
			var probe *spoolProbe
			var creates, removes int
			factory := func() (spoolFile, func() error, error) {
				creates++
				if tc.createErr != nil {
					return nil, nil, tc.createErr
				}
				f, err := os.CreateTemp(dir, "spool-*")
				if err != nil {
					return nil, nil, err
				}
				path = f.Name()
				probe = &spoolProbe{spoolFile: f, write: tc.write, read: tc.read, closeErr: tc.closeErr}
				return probe, func() error {
					removes++
					if probe.closes != 1 {
						t.Error("remove called before close")
					}
					return errors.Join(os.Remove(path), tc.removeErr)
				}, nil
			}

			var out, stderr bytes.Buffer
			code := executeWithSpool([]string{"-diagnostic-spill"}, strings.NewReader(input), &out, &stderr, factory)
			if code != 1 || creates != 1 || !strings.Contains(out.String(), "## Race tests") {
				t.Fatalf("exit %d, creates %d, summary %q", code, creates, out.String())
			}
			requireOnce(t, stderr.String(), "testreport: diagnostic storage: "+tc.reason+"\n")
			requireContains(t, "diagnostics", stderr.String(), tc.want...)
			requireAbsent(t, stderr.String(), "other-stream", errTestSpool.Error())
			if tc.createErr == nil && (probe.closes != 1 || removes != 1) {
				t.Fatalf("closes %d, removes %d", probe.closes, removes)
			}
			if _, err := os.Stat(path); !os.IsNotExist(err) {
				t.Fatalf("spool remains: %v", err)
			}
		})
	}
}

func failWriteAt(offset int64, short int) func(*spoolProbe, []byte, int64) (int, error) {
	return func(p *spoolProbe, data []byte, at int64) (int, error) {
		if at == offset {
			if short == 0 {
				return 0, errTestSpool
			}
			return p.spoolFile.WriteAt(data[:short], at)
		}
		return p.spoolFile.WriteAt(data, at)
	}
}

func shortReadAt(offset int64, short int) func(*spoolProbe, []byte, int64) (int, error) {
	return func(p *spoolProbe, data []byte, at int64) (int, error) {
		if at == offset {
			return p.spoolFile.ReadAt(data[:short], at)
		}
		return p.spoolFile.ReadAt(data, at)
	}
}

func modifyHeader(offset int64, modify func([]byte)) func(*spoolProbe, []byte, int64) (int, error) {
	return modifyRead(offset, spoolHeaderSize, modify)
}

func modifyRead(offset int64, count int, modify func([]byte)) func(*spoolProbe, []byte, int64) (int, error) {
	return func(p *spoolProbe, data []byte, at int64) (int, error) {
		n, err := p.spoolFile.ReadAt(data, at)
		if at == offset && n == count {
			modify(data)
		}
		return n, err
	}
}

func TestDiagnosticSpillQuotaAndFallbackEviction(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "spool-*")
	if err != nil {
		t.Fatal(err)
	}
	probe := &spoolProbe{spoolFile: f}
	s := newDiagnosticStore(func() (spoolFile, func() error, error) {
		return probe, func() error { return os.Remove(f.Name()) }, nil
	})
	defer s.close()
	var first, second diagnosticRef
	head := strings.Repeat("x", spoolPageSize-spoolHeaderSize-1) + "\n"
	s.limit = spoolPageSize
	s.append(&first, head)
	if first.end-1 != s.limit || probe.highWater != spoolPageSize || first.committedFragments != 1 || s.failure != "" {
		t.Fatalf("exact cap failed: extent %d, high water %d, fragments %d, failure %q", first.end-1, probe.highWater, first.committedFragments, s.failure)
	}
	s.append(&first, "x")
	if first.committedFragments != 1 || probe.highWater != spoolPageSize || s.failure != "write quota exceeded" {
		t.Fatal("quota overflow wrote spool data or advanced the committed chain")
	}
	s.append(&first, strings.Repeat("界", 200_000)+"\n")
	s.append(&second, "other chain\n")
	s.append(&first, "newest sentinel\n")
	var out bytes.Buffer
	s.replay(&diagnosticWriter{Writer: &out}, &first, &diagnosticCursor{})
	if out.Len() > fallbackSlots*fallbackSlotBytes+len(head)+100 || !utf8.Valid(out.Bytes()) {
		t.Fatalf("fallback produced %d bytes, valid UTF-8 = %t", out.Len(), utf8.Valid(out.Bytes()))
	}
	requireContains(t, "fallback", out.String(), head, "newest sentinel\n")
	requireAbsent(t, out.String(), "other chain")
	rest := strings.TrimPrefix(out.String(), head)
	line, tail, ok := strings.Cut(rest, "\n")
	if !ok || line != fmt.Sprintf("--- DIAGNOSTIC OUTPUT OMITTED: %d bytes", first.bytes-uint64(len(head)+len(tail))) {
		t.Fatalf("omissions did not account for the payload: %q", line)
	}
}

func TestDiagnosticSpillReadFailureWithFallback(t *testing.T) {
	input := stream(t,
		event{Action: "output", Package: "sealed", Test: "TestFail", Output: "readable head\n"},
		event{Action: "output", Package: "sealed", Test: "TestFail", Output: "unreadable fragment\n"},
		event{Action: "output", Package: "sealed", Test: "TestFail", Output: "fallback tail\n"},
		event{Action: "fail", Package: "sealed", Test: "TestFail"},
		event{Action: "fail", Package: "sealed"},
		event{Action: "output", Package: "interrupted", Test: "TestPending", Output: "unfinished tail\n"},
	)
	f, err := os.CreateTemp(t.TempDir(), "spool-*")
	if err != nil {
		t.Fatal(err)
	}
	second := int64(spoolHeaderSize + len("readable head\n"))
	third := second + spoolHeaderSize + int64(len("unreadable fragment\n"))
	probe := &spoolProbe{
		spoolFile: f,
		write:     failWriteAt(third, 0),
		read:      shortReadAt(second+spoolHeaderSize, 1),
	}
	factory := func() (spoolFile, func() error, error) {
		return probe, func() error { return os.Remove(f.Name()) }, nil
	}
	var stderr bytes.Buffer
	if code := executeWithSpool([]string{"-diagnostic-spill"}, strings.NewReader(input), io.Discard, &stderr, factory); code != 1 {
		t.Fatalf("exit = %d", code)
	}
	requireOnce(t, stderr.String(),
		"readable head\n--- DIAGNOSTIC OUTPUT OMITTED: 20 bytes\nfallback tail\n",
		"--- INCOMPLETE: TestPending\nunfinished tail\n",
		"testreport: diagnostic storage: write failed\n")
	requireAbsent(t, stderr.String(), "unreadable fragment")
}

type rejectedWriter struct {
	calls int
}

func (w *rejectedWriter) Write(data []byte) (int, error) {
	w.calls++
	return len(data) / 2, nil
}

func TestDiagnosticSpillDeliveryFailureDrainsInput(t *testing.T) {
	input := stream(t,
		event{Action: "output", Package: "first", Test: "TestFail", Output: "failing text\n"},
		event{Action: "fail", Package: "first", Test: "TestFail"},
		event{Action: "fail", Package: "first"},
		event{Action: "start", Package: "last"},
		event{Action: "pass", Package: "last"},
	)
	var summary bytes.Buffer
	stderr := &rejectedWriter{}
	code := execute([]string{"-diagnostic-spill"}, strings.NewReader(input), &summary, stderr)
	if code != 1 || stderr.calls != 1 {
		t.Fatalf("exit %d, sink calls %d", code, stderr.calls)
	}
	requireContains(t, "summary", summary.String(), "| last | pass |")

	out := &rejectedWriter{}
	var progress bytes.Buffer
	healthy := stream(t, event{Action: "pass", Package: "last"})
	code = execute([]string{"-diagnostic-spill"}, strings.NewReader(healthy), out, &progress)
	if code != 1 || out.calls != 1 {
		t.Fatalf("exit %d, summary sink calls %d", code, out.calls)
	}
	requireContains(t, "progress", progress.String(), "ok   \tlast\t")
}

type interruptedReader struct {
	io.Reader
}

func (r interruptedReader) Read(data []byte) (int, error) {
	n, err := r.Reader.Read(data)
	if err == io.EOF {
		return n, context.Canceled
	}
	return n, err
}

func TestDiagnosticSpillTempLifecycle(t *testing.T) {
	t.Run("parallel invocations", testDiagnosticSpillTempInvocation)
	t.Run("parallel subprocesses", testDiagnosticSpillTempProcesses)
}

func testDiagnosticSpillTempInvocation(t *testing.T) {
	dir := t.TempDir()
	input := stream(t,
		event{Action: "output", Package: "pkg", Test: "TestActive", Output: "partial stack\n"},
	)
	clean := input + stream(t, event{Action: "pass", Package: "pkg", Test: "TestActive"}, event{Action: "pass", Package: "pkg"})
	created := make(chan string, 2)
	release := make(chan struct{})
	type result struct {
		code        int
		diagnostics string
	}
	results := make(chan result, 2)
	factory := func() (spoolFile, func() error, error) {
		f, err := os.CreateTemp(dir, "testreport-*")
		if err != nil {
			created <- ""
			return nil, nil, err
		}
		created <- f.Name()
		<-release
		return f, func() error { return os.Remove(f.Name()) }, nil
	}
	for _, reader := range []io.Reader{strings.NewReader(clean), interruptedReader{strings.NewReader(input)}} {
		go func() {
			var stderr bytes.Buffer
			code := executeWithSpool([]string{"-diagnostic-spill"}, reader, io.Discard, &stderr, factory)
			results <- result{code: code, diagnostics: stderr.String()}
		}()
	}
	first, second := <-created, <-created
	if first == second || first == "" || second == "" {
		t.Errorf("concurrent spool paths = %q, %q", first, second)
	}
	if runtime.GOOS != "windows" {
		for _, path := range []string{first, second} {
			if info, err := os.Stat(path); err != nil || info.Mode().Perm()&0o077 != 0 {
				t.Errorf("spool mode accessible beyond its owner: %v, %v", info, err)
			}
		}
	}
	close(release)
	var interrupted int
	for range 2 {
		run := <-results
		if run.code == 1 {
			interrupted++
			requireContains(t, "diagnostics", run.diagnostics, "partial stack\n", "read go test events: input read failed\n")
		}
	}
	if interrupted != 1 {
		t.Fatalf("interrupted invocations = %d", interrupted)
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 0 {
		t.Fatalf("temporary files after return: %v, %v", entries, err)
	}
	code := executeWithSpool([]string{"-diagnostic-spill"}, strings.NewReader(""), io.Discard, io.Discard,
		func() (spoolFile, func() error, error) {
			t.Fatal("empty invocation opened a spool")
			return nil, nil, nil
		})
	if code != 0 {
		t.Fatalf("empty run exit %d", code)
	}
}

const reportProcess = "QUARRY_TESTREPORT_PROCESS"

func TestDiagnosticSpillProcess(*testing.T) {
	if os.Getenv(reportProcess) != "1" {
		return
	}
	os.Args = []string{"testreport", "-diagnostic-spill"}
	main()
}

type testReportProcess struct {
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	stderr *bufio.Reader
}

func startDiagnosticSpillProcess(ctx context.Context, t *testing.T, executable, dir string) testReportProcess {
	t.Helper()
	cmd := exec.CommandContext(ctx, executable, "-test.run=^TestDiagnosticSpillProcess$")
	cmd.Env = append(os.Environ(), reportProcess+"=1", "TMPDIR="+dir, "TMP="+dir, "TEMP="+dir)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		_ = stdin.Close()
		t.Fatal(err)
	}
	if startErr := cmd.Start(); startErr != nil {
		t.Fatal(startErr)
	}
	t.Cleanup(func() {
		_ = stdin.Close()
		if cmd.ProcessState == nil {
			_ = cmd.Wait()
		}
	})
	return testReportProcess{cmd, stdin, bufio.NewReader(stderr)}
}

func testDiagnosticSpillTempProcesses(t *testing.T) {
	dir := t.TempDir()
	executable, executableErr := os.Executable()
	if executableErr != nil {
		t.Fatal(executableErr)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	var children [2]testReportProcess
	for i := range children {
		child := startDiagnosticSpillProcess(ctx, t, executable, dir)
		children[i] = child
		input := stream(t,
			event{Action: "output", Package: "test", Output: "discarded package log\n"},
			event{Action: "pass", Package: "test"},
		)
		if _, err := io.WriteString(child.stdin, input); err != nil {
			t.Fatal(err)
		}
		if got, err := child.stderr.ReadString('\n'); err != nil || got != "ok   \ttest\t0.00s\n" {
			t.Fatalf("waiting for child %d: %q, %v", i, got, err)
		}
	}
	files, listErr := os.ReadDir(dir)
	if listErr != nil || len(files) != len(children) {
		t.Fatalf("temporary files while children are running: %v, %v", files, listErr)
	}
	for _, file := range files {
		info, err := file.Info()
		if err != nil || !strings.HasPrefix(file.Name(), "quarry-testreport-") ||
			(runtime.GOOS != "windows" && info.Mode().Perm()&0o077 != 0) {
			t.Errorf("unexpected spool file: %v, %v", info, err)
		}
	}
	if _, err := io.WriteString(children[1].stdin, "malformed input\n"); err != nil {
		t.Fatal(err)
	}
	for i, child := range children {
		if err := child.stdin.Close(); err != nil {
			t.Fatal(err)
		}
		stderr, err := io.ReadAll(child.stderr)
		if err != nil {
			t.Fatal(err)
		}
		if err := child.cmd.Wait(); child.cmd.ProcessState == nil || child.cmd.ProcessState.ExitCode() != i {
			t.Fatalf("child %d: exit %v, stderr %q", i, err, stderr)
		}
		if i == 1 {
			requireOnce(t, string(stderr), "read go test events: invalid JSON object\n")
		}
	}
	files, listErr = os.ReadDir(dir)
	if listErr != nil || len(files) != 0 {
		t.Fatalf("temporary files after child exit: %v, %v", files, listErr)
	}
}

const (
	volumeChunk   = 32 * 1024
	volumePackage = "example.test/volume"
	volumeTest    = "TestFailure"
	volumePeers   = 64
)

type volumeReader struct {
	line                                      strings.Reader
	chunks, emitted                           int
	pendingPeer                               bool
	mainLine, firstLine, middleLine, lastLine string
	peers                                     [volumePeers]string
	reuseTerminal                             string
	trailer                                   string
}

func volumeChunkText(marker string) string {
	return marker + strings.Repeat("x", volumeChunk-len(marker)-1) + "\n"
}

func newVolumeReader(mebibytes int, reuse bool) *volumeReader {
	r := &volumeReader{chunks: mebibytes * 1024 * 1024 / volumeChunk}
	line := func(ev event) string {
		data, err := json.Marshal(ev)
		if err != nil {
			panic(err)
		}
		return string(data) + "\n"
	}
	main := event{Action: "output", Package: volumePackage, Test: volumeTest}
	main.Output = volumeChunkText("ordinary")
	r.mainLine = line(main)
	main.Output = volumeChunkText("first sentinel")
	r.firstLine = line(main)
	main.Output = volumeChunkText("middle sentinel")
	r.middleLine = line(main)
	main.Output = volumeChunkText("last sentinel")
	r.lastLine = line(main)
	var trailer strings.Builder
	if reuse {
		const test = "BenchmarkReuse"
		r.mainLine = line(event{Action: "output", Package: volumePackage, Test: test, Output: volumeChunkText("discarded")})
		r.reuseTerminal = line(event{Action: "bench", Package: volumePackage, Test: test})
	} else {
		for i := range r.peers {
			test := fmt.Sprintf("TestPeer%02d", i)
			r.peers[i] = line(event{Action: "output", Package: volumePackage, Test: test, Output: "peer text\n"})
			trailer.WriteString(line(event{Action: "pass", Package: volumePackage, Test: test}))
		}
	}
	trailer.WriteString(line(event{Action: "fail", Package: volumePackage, Test: volumeTest}))
	trailer.WriteString(line(event{Action: "fail", Package: volumePackage}))
	r.trailer = trailer.String()
	return r
}

func (r *volumeReader) Read(data []byte) (int, error) {
	if r.line.Len() == 0 {
		switch {
		case r.pendingPeer:
			if r.reuseTerminal != "" {
				r.line.Reset(r.reuseTerminal)
			} else {
				r.line.Reset(r.peers[(r.emitted-1)%len(r.peers)])
			}
			r.pendingPeer = false
		case r.emitted == r.chunks:
			if r.trailer == "" {
				return 0, io.EOF
			}
			r.line.Reset(r.trailer)
			r.trailer = ""
		default:
			r.pendingPeer = r.reuseTerminal == ""
			switch r.emitted {
			case 0:
				r.line.Reset(r.firstLine)
			case r.chunks / 2:
				r.line.Reset(r.middleLine)
			case r.chunks - 1:
				r.line.Reset(r.lastLine)
			default:
				r.line.Reset(r.mainLine)
				r.pendingPeer = true
			}
			r.emitted++
		}
	}
	return r.line.Read(data)
}

type diagnosticDigest struct {
	hash.Hash
	bytes     int
	sentinels [3]bool
}

func (d *diagnosticDigest) Write(data []byte) (int, error) {
	d.bytes += len(data)
	for i, marker := range []string{"first sentinel", "middle sentinel", "last sentinel"} {
		d.sentinels[i] = d.sentinels[i] || bytes.HasPrefix(data, []byte(marker))
	}
	return d.Hash.Write(data)
}

func TestDiagnosticSpillRetainsExplicitFailure(t *testing.T) {
	const mebibytes = 128
	in := newVolumeReader(mebibytes, false)
	probe := verifyDiagnosticVolume(t, in, mebibytes*1024*1024)
	liveMain := in.chunks * (spoolHeaderSize + volumeChunk)
	peerBytes := (in.chunks / volumePeers) * (spoolHeaderSize + len("peer text\n"))
	peerPages := volumePeers * ((peerBytes + spoolPageSize - 1) / spoolPageSize)
	wantHighWater := int64((peerPages + (liveMain+spoolPageSize-1)/spoolPageSize) * spoolPageSize)
	if probe.highWater > wantHighWater || probe.highWater <= wantHighWater-spoolPageSize {
		t.Fatalf("spool high water %d (want final page ending at %d)", probe.highWater, wantHighWater)
	}
}

func verifyDiagnosticVolume(t testing.TB, in *volumeReader, retainedBytes int) *spoolProbe {
	t.Helper()
	sink := &diagnosticDigest{Hash: sha256.New()}
	var probe *spoolProbe
	var creates int
	factory := func() (spoolFile, func() error, error) {
		creates++
		f, cleanup, err := createDiagnosticSpool()
		if err != nil {
			return nil, nil, err
		}
		probe = &spoolProbe{spoolFile: f}
		return probe, cleanup, nil
	}
	code := executeWithSpool([]string{"-diagnostic-spill"}, in, io.Discard, sink, factory)
	want := sha256.New()
	prefix := "FAIL \t" + volumePackage + "\t0.00s\n--- FAIL: " + volumeTest + "\n"
	suffix := "testreport: " + volumePackage + " failed\n"
	_, _ = io.WriteString(want, prefix)
	ordinary := volumeChunkText("ordinary")
	for i := range in.chunks {
		switch i {
		case 0:
			_, _ = io.WriteString(want, volumeChunkText("first sentinel"))
		case in.chunks / 2:
			_, _ = io.WriteString(want, volumeChunkText("middle sentinel"))
		case in.chunks - 1:
			_, _ = io.WriteString(want, volumeChunkText("last sentinel"))
		default:
			if in.reuseTerminal == "" {
				_, _ = io.WriteString(want, ordinary)
			}
		}
	}
	_, _ = io.WriteString(want, suffix)
	if code != 1 || sink.bytes != retainedBytes+len(prefix)+len(suffix) ||
		sink.sentinels != [3]bool{true, true, true} || !bytes.Equal(sink.Sum(nil), want.Sum(nil)) {
		t.Fatalf("replay exit %d, bytes %d, sentinels %v, digest %x (want %x)",
			code, sink.bytes, sink.sentinels, sink.Sum(nil), want.Sum(nil))
	}
	if creates != 1 || probe.closes != 1 || probe.highWater > spoolLimit || in.emitted != in.chunks {
		t.Fatalf("spool creates %d, closes %d, high water %d, input chunks %d/%d",
			creates, probe.closes, probe.highWater, in.emitted, in.chunks)
	}
	return probe
}

func verifyReusedDiagnosticVolume(t testing.TB, mebibytes int) {
	t.Helper()
	input := newVolumeReader(mebibytes, true)
	probe := verifyDiagnosticVolume(t, input, 3*volumeChunk)
	// The three sentinels stay live. Every other record has its own short
	// benchmark occurrence and can occupy the same reusable pages.
	maxExtent := int64(4 * (volumeChunk + spoolPageSize))
	if probe.highWater > maxExtent || probe.writes > 6*input.chunks {
		t.Fatalf("reuse volume: high water %d (max %d), %d writes for %d fragments",
			probe.highWater, maxExtent, probe.writes, input.chunks)
	}
}

func TestDiagnosticSpillReuseVolumeFixture(t *testing.T) {
	verifyReusedDiagnosticVolume(t, 4)
}

func TestDiagnosticSpillUnfinishedReadFailureKeepsAvailableEvidence(t *testing.T) {
	const head = "first stack line\n"
	const missing = "missing stack line\n"
	input := stream(t,
		event{Action: "output", Package: "p", Test: "TestA", Output: head},
		event{Action: "output", Package: "p", Test: "TestA", Output: missing},
		event{Action: "output", Package: "p", Test: "TestB", Output: "unaffected\n"},
	)
	f, err := os.CreateTemp(t.TempDir(), "spool-*")
	if err != nil {
		t.Fatal(err)
	}
	secondPayload := int64(2*spoolHeaderSize + len(head))
	probe := &spoolProbe{spoolFile: f, read: shortReadAt(secondPayload, 1)}
	factory := func() (spoolFile, func() error, error) {
		return probe, func() error { return os.Remove(f.Name()) }, nil
	}
	var stderr bytes.Buffer
	code := executeWithSpool([]string{"-diagnostic-spill"}, strings.NewReader(input), io.Discard, &stderr, factory)
	if code != 1 {
		t.Fatalf("exit = %d", code)
	}
	requireOnce(t, stderr.String(),
		"--- INCOMPLETE: TestA\n"+head,
		"--- INCOMPLETE: TestB\nunaffected\n",
		fmt.Sprintf(omissionFormat, len(missing), 0),
		"testreport: diagnostic storage: read failed\n",
	)
	requireAbsent(t, stderr.String(), missing)
}

func TestDiagnosticSpillChunkBoundariesOnReadFailure(t *testing.T) {
	for _, corrupt := range []bool{false, true} {
		t.Run(fmt.Sprintf("corrupt=%t", corrupt), func(t *testing.T) {
			f, err := os.CreateTemp(t.TempDir(), "spool-*")
			if err != nil {
				t.Fatal(err)
			}
			const head = spoolHeaderSize
			probe := &spoolProbe{spoolFile: f}
			s := newDiagnosticStore(func() (spoolFile, func() error, error) {
				return probe, func() error { return os.Remove(f.Name()) }, nil
			})
			defer s.close()
			prefix := strings.Repeat("x", len(s.buffer)-1)
			var ref diagnosticRef
			s.append(&ref, prefix+"界unfinished\n")
			if corrupt {
				probe.read = func(p *spoolProbe, data []byte, at int64) (int, error) {
					n, readErr := p.spoolFile.ReadAt(data, at)
					if at == head && n == len(s.buffer) {
						for i := n - utf8.UTFMax; i < n; i++ {
							data[i] = 0x80
						}
					}
					return n, readErr
				}
			} else {
				probe.read = shortReadAt(int64(head+len(prefix)), 1)
			}
			var out bytes.Buffer
			s.replay(&diagnosticWriter{Writer: &out}, &ref, &diagnosticCursor{})
			wantText, wantFailure, missing := prefix+"\n", "read failed", len("界unfinished\n")
			if corrupt {
				wantText, wantFailure, missing = "", "read invalid payload", len(prefix)+missing
			}
			want := wantText + fmt.Sprintf("--- DIAGNOSTIC OUTPUT OMITTED: %d bytes\n", missing)
			if s.failure != wantFailure || !bytes.Equal(out.Bytes(), []byte(want)) || !utf8.Valid(out.Bytes()) {
				t.Fatalf("failure %q, output size %d, valid UTF-8 = %t", s.failure, out.Len(), utf8.Valid(out.Bytes()))
			}
		})
	}
}
