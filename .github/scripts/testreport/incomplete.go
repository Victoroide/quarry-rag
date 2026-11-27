package main

import (
	"bytes"
	"container/heap"
	"fmt"
	"strings"
	"unicode/utf8"
)

const (
	maxIncompleteTests = 128
	maxIncompleteBytes = 512 * 1024
	incompleteHeading  = "--- INCOMPLETE: "
	unresolvedHeading  = "--- UNRESOLVED: "
	omissionFormat     = "--- INCOMPLETE OUTPUT OMITTED: %d bytes, %d tests\n"
)

type incompleteSection struct {
	test         *testResult
	heading      string
	headingBytes int
	needsNewline bool
	tail         recentOutput
	retained     int
	payload      []byte
}

func (r *run) writeIncompleteDetails(w *diagnosticWriter, tests []*testResult) {
	if w.failed() {
		return
	}
	var sections []incompleteSection
	var totalTests int
	var totalBytes, fullSize uint64
	for _, test := range tests {
		if test.outcome != "" {
			continue
		}
		totalTests++
		totalBytes = saturatingAdd(totalBytes, test.output.bytes())
		if len(sections) == maxIncompleteTests {
			continue
		}
		heading := incompleteHeading
		if test.hasPrefixedRun && strings.Contains(test.name, "/") {
			heading = unresolvedHeading
		}
		section := incompleteSection{test: test, heading: heading, headingBytes: len(heading) + len(test.name) + 1}
		section.needsNewline = test.output.bytes() != 0 && !test.output.endsLine()
		fullSize = saturatingAdd(fullSize, uint64(len(heading))+uint64(len(test.name))+1)
		fullSize = saturatingAdd(fullSize, test.output.bytes())
		if section.needsNewline {
			fullSize = saturatingAdd(fullSize, 1)
		}
		sections = append(sections, section)
	}
	if totalTests == 0 {
		return
	}

	budget := maxIncompleteBytes
	if w.partialLine {
		w.startLine()
		budget--
	}
	markerSize := len(fmt.Sprintf(omissionFormat, totalBytes, totalTests))
	payloadBudget := budget
	if totalTests > len(sections) || fullSize > uint64(budget) || (r.store != nil && r.store.failure != "") {
		payloadBudget -= markerSize
	}
	for i := range sections {
		if sections[i].headingBytes > payloadBudget {
			sections = sections[:i]
			break
		}
		payloadBudget -= sections[i].headingBytes
	}
	retained := r.retainRecentOutput(sections, payloadBudget)

	// Stage only the bounded unfinished payload. A read failure during forward
	// replay can require an omission marker even when the healthy report fits.
	stage := bytes.NewBuffer(make([]byte, 0, retained))
	var written uint64
	size, headings := 0, 0
	for i := range sections {
		section := &sections[i]
		start := stage.Len()
		section.tail.copyTo(stage, r.store, &section.test.output)
		section.payload = stage.Bytes()[start:]
		written += uint64(len(section.payload))
		headings += section.headingBytes
		size += section.headingBytes + framedSize(section.payload)
	}
	omittedTests := totalTests - len(sections)
	if written < totalBytes || omittedTests > 0 {
		for headings+markerSize > budget {
			last := sections[len(sections)-1]
			headings -= last.headingBytes
			size -= last.headingBytes + framedSize(last.payload)
			written -= uint64(len(last.payload))
			sections = sections[:len(sections)-1]
			omittedTests++
		}
		trim := size + markerSize - budget
		for i := range sections {
			if trim <= 0 {
				break
			}
			section := &sections[i]
			payload := section.payload
			skip := runeBoundary(payload, min(trim, len(payload)))
			section.payload = payload[skip:]
			written -= uint64(len(payload[:skip]))
			trim -= framedSize(payload) - framedSize(section.payload)
		}
	}

	for _, section := range sections {
		printf(w, "%s%s\n", section.heading, section.test.name)
		_, _ = w.Write(section.payload)
		if len(section.payload) != 0 {
			w.startLine()
		}
	}
	if written < totalBytes || omittedTests > 0 {
		printf(w, omissionFormat, totalBytes-written, omittedTests)
	}
}

func framedSize(payload []byte) int {
	if len(payload) != 0 && payload[len(payload)-1] != '\n' {
		return len(payload) + 1
	}
	return len(payload)
}

func runeBoundary(data []byte, start int) int {
	for start < len(data) && !utf8.RuneStart(data[start]) {
		start++
	}
	return start
}

// retainRecentOutput merges the selected tests' output in reverse event order.
// Each heap entry holds only one current fragment and the start of its suffix.
func (r *run) retainRecentOutput(sections []incompleteSection, budget int) int {
	queue := make(fragmentHeap, 0, len(sections))
	for i := range sections {
		section := &sections[i]
		tail := &section.tail
		tail.init(r.store, &section.test.output)
		if tail.ordinal != 0 {
			if r.store != nil && r.store.failure != "" {
				section.needsNewline = !tail.endsLine(r.store, &section.test.output.ref)
			}
			queue = append(queue, section)
		}
	}
	heap.Init(&queue)
	retained := 0
	for len(queue) > 0 && budget > 0 {
		section := queue[0]
		tail := &section.tail
		available := budget
		if section.retained == 0 && section.needsNewline {
			available--
		}
		start, ok := tail.start(r.store, &section.test.output.ref, available)
		switch {
		case !ok:
			tail.dropDisk()
		case start == tail.length:
			return retained
		default:
			tail.keep(start)
			kept := tail.length - start
			section.retained += kept
			retained += kept
			budget = available - kept
			if start > 0 || budget == 0 {
				return retained
			}
		}
		tail.older(r.store, &section.test.output)
		if tail.ordinal == 0 {
			heap.Pop(&queue)
		} else {
			heap.Fix(&queue, 0)
		}
	}
	return retained
}

type fragmentHeap []*incompleteSection

func (h fragmentHeap) Len() int           { return len(h) }
func (h fragmentHeap) Less(i, j int) bool { return h[i].tail.ordinal > h[j].tail.ordinal }
func (h fragmentHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *fragmentHeap) Push(x any)        { *h = append(*h, x.(*incompleteSection)) }

func (h *fragmentHeap) Pop() any {
	last := len(*h) - 1
	section := (*h)[last]
	(*h)[last] = nil
	*h = (*h)[:last]
	return section
}
