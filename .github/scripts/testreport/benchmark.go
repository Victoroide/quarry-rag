package main

import (
	"regexp"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

var benchmarkDecimal = regexp.MustCompile(`^\d+$`)

func isBenchmarkName(name string) bool {
	suffix, benchmark := strings.CutPrefix(name, "Benchmark")
	first, _ := utf8.DecodeRuneInString(suffix)
	return benchmark && !unicode.IsLower(first)
}

// advanceBenchmarks closes possible structural parents when the next run
// leaves their name prefix. An empty name means the runner has finished.
func (r *run) advanceBenchmarks(p *packageResult, next string) {
	for len(p.benchmarkPath) > 0 {
		last := len(p.benchmarkPath) - 1
		t := p.benchmarkPath[last]
		if len(next) > len(t.name) && next[len(t.name)] == '/' && strings.HasPrefix(next, t.name) {
			// A nested name prefix may belong to an ancestor or to a
			// sibling whose B.Run name contains slashes.
			t.hasPrefixedRun = true
			return
		}
		if t.hasPrefixedRun && t.outcome == "" {
			r.completeBenchmark(p, t)
		}
		p.benchmarkPath[last] = nil
		p.benchmarkPath = p.benchmarkPath[:last]
	}
}

// endsBenchmarkMeasurement checks the latest output line of a benchmark.
// test2json can deliver the name and result fields in separate fragments.
func endsBenchmarkMeasurement(t *testResult) bool {
	last := len(t.output.legacy) - 1
	if !isBenchmarkName(t.name) || !strings.HasSuffix(t.output.legacy[last].text, "\n") {
		return false
	}
	start := last
	for start > 0 && !strings.Contains(t.output.legacy[start-1].text, "\n") {
		start--
	}
	line := t.output.legacy[last].text
	if start < last {
		var joined strings.Builder
		for _, fragment := range t.output.legacy[start:] {
			joined.WriteString(fragment.text)
		}
		line = joined.String()
	}
	return isBenchmarkMeasurement(t.name, line)
}

func isBenchmarkMeasurement(benchmark, line string) bool {
	line = strings.TrimSuffix(line, "\n")
	line = strings.TrimSuffix(line, "\r")
	if strings.ContainsAny(line, "\r\n") {
		return false
	}
	name, result, ok := strings.Cut(line, "\t")
	if !ok {
		return false
	}
	name = strings.TrimRight(name, " ")
	if name != benchmark {
		cpu, ok := strings.CutPrefix(name, benchmark+"-")
		if !ok || !benchmarkDecimal.MatchString(cpu) {
			return false
		}
	}
	iterations, metrics, hasMetrics := strings.Cut(result, "\t")
	if !benchmarkDecimal.MatchString(strings.Trim(iterations, " ")) {
		return false
	}
	// Go can omit all metrics, for example when ns/op is explicitly set to zero.
	if !hasMetrics {
		return true
	}
	for metric := range strings.SplitSeq(metrics, "\t") {
		var count int
		for field := range strings.FieldsSeq(metric) {
			count++
			if count == 1 {
				if _, err := strconv.ParseFloat(field, 64); err != nil {
					return false
				}
			} else if count > 2 {
				return false
			}
		}
		if count != 2 {
			return false
		}
	}
	return true
}
