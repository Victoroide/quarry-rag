package main

import (
	"strconv"
	"strings"
	"unicode"
)

func (r *run) endsBenchmark(t *testResult, output string) bool {
	if !isBenchmarkName(t.name) {
		return false
	}
	if r.store == nil {
		return endsBenchmarkMeasurement(t)
	}
	line, ended := strings.CutSuffix(output, "\n")
	if strings.ContainsRune(line, '\n') {
		t.measurement = nil
		return false
	}
	if ended && t.measurement == nil {
		return isBenchmarkMeasurement(t.name, output)
	}
	if t.measurement == nil {
		t.measurement = &benchmarkMeasurement{}
	}
	state := t.measurement
	state.consume(t.name, line)
	if !ended {
		return false
	}
	t.measurement = nil
	return state.complete()
}

type measurementPhase uint8

const (
	measurementName measurementPhase = iota
	measurementCPU
	measurementNamePadding
	measurementIterations
	measurementNumber
	measurementUnit
)

// test2json can split a measurement into arbitrarily many output fragments.
// Keep its grammar state. Go formats a metric from a float64, whose widest
// fixed-point value occupies 309 decimal digits plus a sign.
type benchmarkMeasurement struct {
	phase                           measurementPhase
	nameBytes                       int
	digits, padding, unit, unitDone bool
	cr, invalid                     bool
	number                          [320]byte
	numberLen                       int
}

func (m *benchmarkMeasurement) consume(name, text string) {
	if m.invalid {
		return
	}
	for _, char := range text {
		switch {
		case m.cr:
			m.invalid = true
		case char == '\r':
			m.cr = true
		default:
			m.consumeRune(name, char)
		}
		if m.invalid {
			return
		}
	}
}

func (m *benchmarkMeasurement) consumeRune(name string, char rune) {
	switch m.phase {
	case measurementName:
		if m.nameBytes == len(name) {
			switch char {
			case '-':
				m.phase = measurementCPU
			case ' ':
				m.phase = measurementNamePadding
			case '\t':
				m.phase = measurementIterations
			default:
				m.invalid = true
			}
			return
		}
		part := string(char)
		if char == '\t' || !strings.HasPrefix(name[m.nameBytes:], part) {
			m.invalid = true
			return
		}
		m.nameBytes += len(part)
	case measurementCPU:
		if char >= '0' && char <= '9' {
			m.digits = true
			return
		}
		if !m.digits {
			m.invalid = true
			return
		}
		m.phase = measurementNamePadding
		m.consumeRune(name, char)
	case measurementNamePadding:
		if char == '\t' {
			m.phase, m.digits = measurementIterations, false
		} else if char != ' ' {
			m.invalid = true
		}
	case measurementIterations:
		switch {
		case char == ' ':
			m.padding = m.digits
		case char == '\t' && m.digits:
			m.phase = measurementNumber
		case char >= '0' && char <= '9' && !m.padding:
			m.digits = true
		default:
			m.invalid = true
		}
	case measurementNumber:
		switch {
		case char == '\t':
			m.invalid = true
		case unicode.IsSpace(char):
			if m.numberLen > 0 {
				_, err := strconv.ParseFloat(string(m.number[:m.numberLen]), 64)
				m.invalid = err != nil
				m.phase = measurementUnit
			}
		case char >= 0 && char <= unicode.MaxASCII && m.numberLen < len(m.number):
			m.number[m.numberLen] = byte(char)
			m.numberLen++
		default:
			m.invalid = true
		}
	case measurementUnit:
		switch {
		case char == '\t' && m.unit:
			m.phase, m.numberLen, m.unit, m.unitDone = measurementNumber, 0, false, false
		case char == '\t':
			m.invalid = true
		case unicode.IsSpace(char):
			m.unitDone = m.unit
		case m.unitDone:
			m.invalid = true
		default:
			m.unit = true
		}
	}
}

func (m *benchmarkMeasurement) complete() bool {
	return !m.invalid && ((m.phase == measurementIterations && m.digits) || (m.phase == measurementUnit && m.unit))
}
