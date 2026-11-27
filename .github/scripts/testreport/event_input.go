package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
)

const (
	maxEventBytes      = 4 * 1024 * 1024
	maxNameBytes       = 8 * 1024
	maxPackageBuildIDs = 8192
	maxTestOccurrences = 65536
	maxIdentityBytes   = 16 * 1024 * 1024
)

var (
	errEventLimit    = errors.New("record limit exceeded")
	errEventJSON     = errors.New("invalid JSON object")
	errEventRead     = errors.New("input read failed")
	errNameLimit     = errors.New("name limit exceeded")
	errIDLimit       = errors.New("package and build identity limit exceeded")
	errTestLimit     = errors.New("test occurrence limit exceeded")
	errIdentityBytes = errors.New("identity byte limit exceeded")
)

type jsonLines struct {
	input  *bufio.Reader
	record []byte
}

func (l *jsonLines) next() (event, error) {
	l.record = l.record[:0]
	for {
		line, err := l.input.ReadSlice('\n')
		if len(line) > maxEventBytes-len(l.record) {
			return event{}, errEventLimit
		}
		switch {
		case errors.Is(err, bufio.ErrBufferFull):
			if cap(l.record) == 0 {
				l.record = make([]byte, 0, maxEventBytes)
			}
			l.record = append(l.record, line...)
			continue
		case err == nil, errors.Is(err, io.EOF):
		default:
			return event{}, errEventRead
		}
		if len(l.record) > 0 {
			line = append(l.record, line...)
		}
		line = bytes.Trim(line, " \t\r\n")
		if len(line) > 0 {
			var ev event
			if line[0] != '{' || json.Unmarshal(line, &ev) != nil {
				return event{}, errEventJSON
			}
			return ev, nil
		}
		if errors.Is(err, io.EOF) {
			return event{}, io.EOF
		}
		l.record = l.record[:0]
	}
}

type retainedNames struct {
	names       map[string]string
	bytes       int
	occurrences int
}

func (r *run) decodeSpill(in io.Reader, onEnd func(*packageResult)) error {
	lines := jsonLines{input: bufio.NewReaderSize(in, 32*1024)}
	for {
		ev, err := lines.next()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err == nil {
			err = r.retainNames(&ev)
		}
		if err != nil {
			return fmt.Errorf("read go test events: %w", err)
		}
		if p := r.add(ev); p != nil {
			onEnd(p)
		}
	}
}

// Check a whole event before retaining any of its identities, including the
// display package of a standalone build.
func (r *run) retainNames(ev *event) error {
	var pkg, build, test string
	var hasPackage, startTest bool
	switch ev.Action {
	case "build-fail":
		build = ev.ImportPath
		pkg, hasPackage = buildPackage(build), build != ""
	case "build-output":
		build = ev.ImportPath
	default:
		pkg = ev.Package
		if pkg == "" {
			return nil
		}
		hasPackage = true
		switch ev.Action {
		case "run":
			test, startTest = ev.Test, true
		case "pause", "cont", "output", "skip", "pass", "fail":
			test = ev.Test
			if ev.Action == "fail" && test == "" {
				build = ev.FailedBuild
			}
		}
	}
	newIDs := len(r.packages) + len(r.builds)
	p := r.packages[pkg]
	if p == nil && hasPackage {
		newIDs++
	}
	if build != "" && r.builds[build] == nil {
		newIDs++
	}
	if newIDs > maxPackageBuildIDs {
		return errIDLimit
	}
	newTest := test != "" && (startTest || p == nil || p.currentTest[test] == nil)
	if newTest && r.names.occurrences >= maxTestOccurrences {
		return errTestLimit
	}
	extra := 0
	candidates := [...]string{pkg, build, test}
	for i, name := range candidates {
		if len(name) > maxNameBytes {
			return errNameLimit
		}
		if name == "" || r.names.names[name] != "" {
			continue
		}
		duplicate := false
		for _, previous := range candidates[:i] {
			duplicate = duplicate || previous == name
		}
		if !duplicate {
			extra += len(name)
		}
	}
	if extra > maxIdentityBytes-r.names.bytes {
		return errIdentityBytes
	}
	for _, name := range candidates {
		if name != "" && r.names.names[name] == "" {
			canonical := strings.Clone(name)
			r.names.names[canonical] = canonical
		}
	}
	r.names.bytes += extra
	if newTest {
		r.names.occurrences++
	}
	switch ev.Action {
	case "build-fail", "build-output":
		ev.ImportPath = r.names.names[build]
	default:
		ev.Package = r.names.names[pkg]
		if test != "" {
			ev.Test = r.names.names[test]
		}
		if build != "" {
			ev.FailedBuild = r.names.names[build]
		}
	}
	return nil
}
