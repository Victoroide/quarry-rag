package main

import (
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
	"runtime"
	"strings"
	"testing"
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
}

func (p *spoolProbe) WriteAt(data []byte, at int64) (int, error) {
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
	if p.read != nil {
		return p.read(p, data, at)
	}
	return p.spoolFile.ReadAt(data, at)
}

func (p *spoolProbe) Close() error {
	p.closes++
	return errors.Join(p.spoolFile.Close(), p.closeErr)
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
	const secondRecord = int64(1 + spoolHeaderSize + len("first-sensitive\n"))
	const thirdRecord = secondRecord + int64(spoolHeaderSize+len("other-stream\n"))
	for _, tc := range []struct {
		name, reason                   string
		createErr, closeErr, removeErr error
		read, write                    func(*spoolProbe, []byte, int64) (int, error)
		want                           []string
	}{
		{name: "create denied", reason: "create failed", createErr: errTestSpool, want: []string{"first-sensitive\ntail\n"}},
		{name: "disk full", reason: "write failed", write: failWriteAt(thirdRecord, 0), want: []string{"first-sensitive\ntail\n"}},
		{name: "short payload", reason: "write failed", write: failWriteAt(thirdRecord+spoolHeaderSize, 2), want: []string{"first-sensitive\ntail\n"}},
		{name: "short link patch", reason: "write failed", write: failWriteAt(9, 1), want: []string{"first-sensitive\ntail\n"}},
		{name: "short header", reason: "read failed", read: shortReadAt(thirdRecord, 4), want: []string{"first-sensitive\n--- DIAGNOSTIC OUTPUT OMITTED: 5 bytes\n"}},
		{name: "wrong stream", reason: "read invalid record", read: modifyHeader(thirdRecord, func(raw []byte) { binary.LittleEndian.PutUint32(raw[20:], 2) }), want: []string{"first-sensitive\n--- DIAGNOSTIC OUTPUT OMITTED: 5 bytes\n"}},
		{name: "nonreciprocal link", reason: "read invalid link", read: modifyHeader(thirdRecord, func(raw []byte) { binary.LittleEndian.PutUint64(raw[:8], uint64(secondRecord)) }), want: []string{"first-sensitive\n--- DIAGNOSTIC OUTPUT OMITTED: 5 bytes\n"}},
		{name: "self link", reason: "read invalid record", read: modifyHeader(1, func(raw []byte) { binary.LittleEndian.PutUint64(raw[8:16], 1) }), want: []string{"--- DIAGNOSTIC OUTPUT OMITTED: 21 bytes\n"}},
		{name: "uncommitted link on healthy tail", reason: "read invalid record", read: modifyHeader(thirdRecord, func(raw []byte) { binary.LittleEndian.PutUint64(raw[8:16], 1) }), want: []string{"first-sensitive\n--- DIAGNOSTIC OUTPUT OMITTED: 5 bytes\n"}},
		{name: "payload out of extent", reason: "read invalid record", read: modifyHeader(thirdRecord, func(raw []byte) { binary.LittleEndian.PutUint32(raw[16:20], spoolLimit) }), want: []string{"first-sensitive\n--- DIAGNOSTIC OUTPUT OMITTED: 5 bytes\n"}},
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
	return func(p *spoolProbe, data []byte, at int64) (int, error) {
		n, err := p.spoolFile.ReadAt(data, at)
		if at == offset && n == spoolHeaderSize {
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
	const head = "committed head\n"
	s.limit = uint64(1 + spoolHeaderSize + len(head))
	s.append(&first, head)
	if s.end != s.limit || probe.highWater != int64(1+spoolHeaderSize+len(head)) || first.committedFragments != 1 || s.failure != "" {
		t.Fatalf("exact cap failed: extent %d, high water %d, fragments %d, failure %q", s.end, probe.highWater, first.committedFragments, s.failure)
	}
	s.append(&first, "x")
	if first.committedFragments != 1 || probe.highWater != int64(1+spoolHeaderSize+len(head)) || s.failure != "write quota exceeded" {
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
	second := int64(1 + spoolHeaderSize + len("readable head\n"))
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
	trailer                                   string
}

func volumeChunkText(marker string) string {
	return marker + strings.Repeat("x", volumeChunk-len(marker)-1) + "\n"
}

func newVolumeReader(mebibytes int) *volumeReader {
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
	for i := range r.peers {
		test := fmt.Sprintf("TestPeer%02d", i)
		r.peers[i] = line(event{Action: "output", Package: volumePackage, Test: test, Output: "peer text\n"})
		trailer.WriteString(line(event{Action: "pass", Package: volumePackage, Test: test}))
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
			r.line.Reset(r.peers[(r.emitted-1)%len(r.peers)])
			r.pendingPeer = false
		case r.emitted == r.chunks:
			if r.trailer == "" {
				return 0, io.EOF
			}
			r.line.Reset(r.trailer)
			r.trailer = ""
		default:
			switch r.emitted {
			case 0:
				r.line.Reset(r.firstLine)
			case r.chunks / 2:
				r.line.Reset(r.middleLine)
			case r.chunks - 1:
				r.line.Reset(r.lastLine)
			default:
				r.line.Reset(r.mainLine)
			}
			r.emitted++
			r.pendingPeer = true
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
	in := newVolumeReader(mebibytes)
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
			_, _ = io.WriteString(want, ordinary)
		}
	}
	_, _ = io.WriteString(want, suffix)
	if code != 1 || sink.bytes != mebibytes*1024*1024+len(prefix)+len(suffix) ||
		sink.sentinels != [3]bool{true, true, true} || !bytes.Equal(sink.Sum(nil), want.Sum(nil)) {
		t.Fatalf("replay exit %d, bytes %d, sentinels %v, digest %x (want %x)",
			code, sink.bytes, sink.sentinels, sink.Sum(nil), want.Sum(nil))
	}
	wantHighWater := int64(1 + in.chunks*(2*spoolHeaderSize+volumeChunk+len("peer text\n")))
	if creates != 1 || probe.closes != 1 || probe.highWater != wantHighWater || probe.highWater > spoolLimit {
		t.Fatalf("spool creates %d, closes %d, high water %d (want %d)", creates, probe.closes, probe.highWater, wantHighWater)
	}
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
	secondPayload := int64(1 + 2*spoolHeaderSize + len(head))
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
			const head = spoolHeaderSize + 1
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
