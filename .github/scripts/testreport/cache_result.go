package main

import (
	"bytes"
	"strings"
)

const (
	cacheResultPrefix      = "ok  \t"
	cacheResultToken       = "\t(cached)"
	cacheResultCoverage    = "\tcoverage: "
	cacheResultFuzzWarning = "[-fuzz matches more than one fuzz test, won't fuzz]"
)

// cacheResultMatch keeps the current logical line and the last complete
// line's classification without retaining a copy of the package name.
type cacheResultMatch struct {
	prefixBytes int
	suffixBytes int
	suffix      [len(cacheResultFuzzWarning) + 1]byte // Longest warning and optional CR.
	coverage    bool
	lineOpen    bool
	rejected    bool
	lastMatched bool
}

func (m *cacheResultMatch) add(name, output string) {
	for output != "" {
		fragment, rest, ended := strings.Cut(output, "\n")
		if fragment != "" && !m.rejected {
			m.addFragment(name, fragment)
		}
		m.lineOpen = !ended
		if ended {
			const fixedPrefixBytes = len(cacheResultPrefix) + len(cacheResultToken)
			m.lastMatched = !m.rejected && m.prefixBytes == len(name)+fixedPrefixBytes && m.matchesSuffix()
			m.prefixBytes, m.suffixBytes = 0, 0
			m.rejected, m.coverage = false, false
		}
		output = rest
	}
}

func (m *cacheResultMatch) addFragment(name, fragment string) {
	if name == "" {
		m.rejected = true
		return
	}
	skip := m.prefixBytes
	for i, part := range [...]string{cacheResultPrefix, name, cacheResultToken} {
		if skip >= len(part) {
			skip -= len(part)
			continue
		}
		n := min(len(part)-skip, len(fragment))
		piece := fragment[:n]
		if piece != part[skip:skip+n] || i == 1 && strings.ContainsAny(piece, "\t\r\n") {
			m.rejected = true
			return
		}
		m.prefixBytes += n
		fragment = fragment[n:]
		if fragment == "" {
			return
		}
		skip = 0
	}
	if m.suffixBytes == 0 {
		m.coverage = fragment[0] == '\t'
	}
	if m.coverage {
		// cmd/go can copy the coverage text verbatim. Only match its delimiter.
		remaining := cacheResultCoverage[m.suffixBytes:]
		n := min(len(remaining), len(fragment))
		m.rejected = fragment[:n] != remaining[:n]
		m.suffixBytes += n
		return
	}
	if len(fragment) > len(m.suffix)-m.suffixBytes {
		m.rejected = true
		return
	}
	m.suffixBytes += copy(m.suffix[m.suffixBytes:], fragment)
}

func (m *cacheResultMatch) matchesSuffix() bool {
	if m.coverage {
		return m.suffixBytes == len(cacheResultCoverage)
	}
	suffix := bytes.TrimSuffix(m.suffix[:m.suffixBytes], []byte("\r"))
	switch string(suffix) {
	case "", " [no tests to run]", " [no fuzz tests to fuzz]", cacheResultFuzzWarning:
		return true
	default:
		return false
	}
}
