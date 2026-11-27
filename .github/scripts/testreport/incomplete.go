package main

import (
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
	test          *testResult
	heading       string
	headingBytes  int
	needsNewline  bool
	firstFragment int
	firstOffset   int
}

func writeIncompleteDetails(b *strings.Builder, tests []*testResult) {
	var sections []incompleteSection
	var totalTests, totalBytes, fullSize int
	for _, test := range tests {
		if test.outcome != "" {
			continue
		}
		totalTests++
		totalBytes += test.outputBytes
		if len(sections) == maxIncompleteTests {
			continue
		}
		heading := incompleteHeading
		if test.hasPrefixedRun && strings.Contains(test.name, "/") {
			heading = unresolvedHeading
		}
		section := incompleteSection{test: test, heading: heading, headingBytes: len(heading) + len(test.name) + 1}
		if len(test.output) > 0 {
			section.needsNewline = !strings.HasSuffix(test.output[len(test.output)-1].text, "\n")
		}
		fullSize += section.headingBytes + test.outputBytes
		if section.needsNewline {
			fullSize++
		}
		sections = append(sections, section)
	}
	if totalTests == 0 {
		return
	}

	budget := maxIncompleteBytes
	if b.Len() > 0 && !strings.HasSuffix(b.String(), "\n") {
		b.WriteByte('\n')
		budget--
	}
	var omittedBytes, omittedTests int
	if totalTests > len(sections) || fullSize > budget {
		// The totals give an upper bound on the marker's byte length.
		budget -= len(fmt.Sprintf(omissionFormat, totalBytes, totalTests))
		for i, section := range sections {
			if section.headingBytes > budget {
				sections = sections[:i]
				break
			}
			budget -= section.headingBytes
		}
		omittedTests = totalTests - len(sections)
		omittedBytes = totalBytes - retainRecentOutput(sections, budget)
	}

	for _, section := range sections {
		b.WriteString(section.heading)
		b.WriteString(section.test.name)
		b.WriteByte('\n')
		fragments := section.test.output[section.firstFragment:]
		for i, fragment := range fragments {
			if i == 0 {
				b.WriteString(fragment.text[section.firstOffset:])
			} else {
				b.WriteString(fragment.text)
			}
		}
		if len(fragments) > 0 && section.needsNewline {
			b.WriteByte('\n')
		}
	}
	if omittedBytes > 0 || omittedTests > 0 {
		fmt.Fprintf(b, omissionFormat, omittedBytes, omittedTests)
	}
}

// retainRecentOutput merges the selected tests' output in reverse event order.
// Each heap entry points to one test's newest fragment still under consideration.
func retainRecentOutput(sections []incompleteSection, budget int) int {
	queue := make(fragmentHeap, 0, len(sections))
	for i := range sections {
		section := &sections[i]
		section.firstFragment = len(section.test.output)
		if section.firstFragment > 0 {
			queue = append(queue, section)
		}
	}
	heap.Init(&queue)
	retained := 0
	for len(queue) > 0 && budget > 0 {
		section := queue[0]
		index := section.firstFragment - 1
		text := section.test.output[index].text
		available := budget
		if section.firstFragment == len(section.test.output) && section.needsNewline {
			available--
		}

		offset := max(0, len(text)-available)
		for offset < len(text) && !utf8.RuneStart(text[offset]) {
			offset++
		}
		if offset == len(text) {
			break
		}
		section.firstFragment, section.firstOffset = index, offset
		kept := len(text) - offset
		retained += kept
		budget = available - kept
		if offset > 0 {
			break
		}
		if index == 0 {
			heap.Pop(&queue)
		} else {
			heap.Fix(&queue, 0)
		}
	}
	return retained
}

type fragmentHeap []*incompleteSection

func (h fragmentHeap) Len() int { return len(h) }

func (h fragmentHeap) Less(i, j int) bool {
	a, b := h[i], h[j]
	return a.test.output[a.firstFragment-1].ordinal > b.test.output[b.firstFragment-1].ordinal
}

func (h fragmentHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *fragmentHeap) Push(x any) { *h = append(*h, x.(*incompleteSection)) }

func (h *fragmentHeap) Pop() any {
	last := len(*h) - 1
	section := (*h)[last]
	(*h)[last] = nil
	*h = (*h)[:last]
	return section
}
