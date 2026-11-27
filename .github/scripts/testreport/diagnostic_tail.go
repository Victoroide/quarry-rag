package main

import (
	"bytes"
	"unicode/utf8"
)

// recentOutput walks one output chain backwards and records at most one
// selected suffix per backend. Ring indexes are one-based here.
type recentOutput struct {
	ordinal uint64
	length  int
	text    string

	legacyNext, legacyStart, legacySkip int
	ringNext, ringStart                 uint64
	ringSkip                            int

	diskOffset                uint64
	diskNext                  uint64
	diskIndex                 uint64
	diskHeader                spoolHeader
	diskRemaining             uint64
	diskStart                 uint64
	diskFirstHeader           spoolHeader
	diskFirstIndex, diskCount uint64
	diskBytes                 int
	diskSkip                  int
}

func (t *recentOutput) init(s *diagnosticStore, out *diagnosticOutput) {
	if s == nil {
		t.legacyNext, t.legacyStart = len(out.legacy), len(out.legacy)
	} else {
		t.ringNext = s.slots
		t.diskOffset = out.ref.last
		t.diskIndex = out.ref.committedFragments
		t.diskRemaining = out.ref.committedBytes
	}
	t.older(s, out)
}

func (t *recentOutput) dropDisk() {
	t.diskOffset = 0
}

func (t *recentOutput) older(s *diagnosticStore, out *diagnosticOutput) {
	t.ordinal = 0
	if s == nil {
		if t.legacyNext != 0 {
			t.legacyNext--
			fragment := out.legacy[t.legacyNext]
			t.text, t.length, t.ordinal = fragment.text, len(fragment.text), fragment.ordinal
		}
		return
	}
	for t.ringNext > s.firstSlot() {
		index := t.ringNext
		t.ringNext--
		slot := s.slot(t.ringNext)
		if slot.id == out.ref.id {
			t.ordinal, t.length = saturatingAdd(spoolLimit, index), len(slot.payload)
			return
		}
	}
	if t.diskOffset == 0 {
		return
	}
	t.diskIndex--
	h, err := s.header(&out.ref, t.diskOffset, t.diskIndex)
	if err != nil || s.reciprocal(h.next == t.diskNext) != nil {
		t.dropDisk()
		return
	}
	if uint64(h.length) > t.diskRemaining || (uint64(h.length) == t.diskRemaining) != (t.diskIndex == 0) {
		s.fail("read invalid length")
		t.dropDisk()
		return
	}
	t.diskRemaining -= uint64(h.length)
	t.diskHeader, t.diskNext = h, t.diskOffset
	t.diskOffset = h.prev
	t.ordinal, t.length = t.diskNext, int(h.length)
}

func (t *recentOutput) endsLine(s *diagnosticStore) bool {
	if t.ordinal > spoolLimit {
		return s.slot(t.ringNext).data[t.length-1] == '\n'
	}
	var suffix [1]byte
	err := s.readAt(suffix[:], t.diskNext+spoolHeaderSize+uint64(t.diskHeader.length)-1)
	return err == nil && suffix[0] == '\n'
}

func (t *recentOutput) start(s *diagnosticStore, available int) (int, bool) {
	start := max(0, t.length-available)
	if start == 0 || start == t.length {
		return start, true
	}
	if s == nil {
		for start < t.length && !utf8.RuneStart(t.text[start]) {
			start++
		}
		return start, true
	}
	if t.ordinal > spoolLimit {
		return runeBoundary(s.slot(t.ringNext).data[:t.length], start), true
	}
	var prefix [utf8.UTFMax]byte
	data := prefix[:min(len(prefix), t.length-start)]
	if s.readAt(data, t.diskNext+spoolHeaderSize+uint64(start)) != nil {
		return 0, false
	}
	return start + runeBoundary(data, 0), true
}

func (t *recentOutput) keep(skip int) {
	switch {
	case t.text != "":
		t.legacyStart, t.legacySkip = t.legacyNext, skip
	case t.ordinal > spoolLimit:
		t.ringStart, t.ringSkip = t.ringNext+1, skip
	default:
		t.diskStart, t.diskFirstHeader = t.diskNext, t.diskHeader
		t.diskFirstIndex, t.diskSkip = t.diskIndex, skip
		t.diskCount++
		t.diskBytes += t.length - skip
	}
}

func (t *recentOutput) copyTo(w *bytes.Buffer, s *diagnosticStore, out *diagnosticOutput) {
	if s == nil {
		for _, fragment := range out.legacy[t.legacyStart:] {
			_, _ = w.WriteString(fragment.text[t.legacySkip:])
			t.legacySkip = 0
		}
		return
	}

	offset, header, skip := t.diskStart, t.diskFirstHeader, t.diskSkip
	remaining := t.diskBytes
	for i := uint64(0); i < t.diskCount; i++ {
		length := int(header.length) - skip
		if length > remaining || (length == remaining) != (i+1 == t.diskCount) {
			s.fail("read invalid length")
			break
		}
		remaining -= length
		if _, err := s.copyPayload(w, offset, skip, int(header.length)); err != nil {
			break
		}
		if i+1 < t.diskCount {
			next, err := s.header(&out.ref, header.next, t.diskFirstIndex+i+1)
			if err != nil || s.reciprocal(next.prev == offset) != nil {
				break
			}
			offset, header, skip = header.next, next, 0
		}
	}
	for index := t.ringStart; index != 0 && index <= s.slots; index++ {
		slot := s.slot(index - 1)
		if slot.id == out.ref.id {
			_, _ = w.Write(slot.payload[t.ringSkip:])
			t.ringSkip = 0
		}
	}
}
