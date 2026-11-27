package main

import (
	"io"
	"strings"
)

type diagnosticOutput struct {
	legacy      []outputFragment
	ref         diagnosticRef
	legacyBytes uint64
}

type outputFragment struct {
	text    string
	ordinal uint64
}

func (r *run) appendOutput(out *diagnosticOutput, text string) {
	if text == "" {
		return
	}
	if r.store != nil {
		r.store.append(&out.ref, text)
		return
	}
	r.outputOrder++
	out.legacy = append(out.legacy, outputFragment{text: text, ordinal: r.outputOrder})
	out.legacyBytes += uint64(len(text))
}

func (r *run) discardOutput(out *diagnosticOutput) {
	if r.store != nil {
		r.store.discard(&out.ref)
	}
	*out = diagnosticOutput{}
}

func (out *diagnosticOutput) bytes() uint64 {
	return out.legacyBytes + out.ref.bytes
}

type diagnosticCursor struct {
	last, ordinal    uint64
	fragments, bytes uint64
	broken           bool
}

func (r *run) writeOutput(w *diagnosticWriter, out *diagnosticOutput, cursor *diagnosticCursor) {
	if r.store != nil {
		r.store.replay(w, &out.ref, cursor)
		return
	}
	for _, fragment := range out.legacy[cursor.fragments:] {
		_, _ = w.WriteString(fragment.text)
	}
	cursor.fragments = uint64(len(out.legacy))
	cursor.bytes = out.legacyBytes
}

func (s *diagnosticStore) replay(w *diagnosticWriter, ref *diagnosticRef, cursor *diagnosticCursor) {
	if w.failed() || cursor.bytes == ref.bytes {
		return
	}
	if !cursor.broken && cursor.fragments < ref.committedFragments {
		s.replayCommitted(w, ref, cursor)
	}
	if w.failed() {
		return
	}
	for slotIndex := s.firstSlot(); slotIndex < s.slots; slotIndex++ {
		slot := s.slot(slotIndex)
		if slot.id != ref.id || saturatingAdd(slot.position, uint64(len(slot.payload))) <= cursor.bytes {
			continue
		}
		if cursor.bytes < slot.position {
			writeGap(w, slot.position-cursor.bytes)
			cursor.bytes = slot.position
		}
		start := cursor.bytes - slot.position
		n, err := w.Write(slot.payload[start:])
		if n > 0 {
			cursor.bytes = saturatingAdd(cursor.bytes, uint64(n))
		}
		if err != nil {
			return
		}
	}
	if cursor.bytes < ref.bytes {
		writeGap(w, ref.bytes-cursor.bytes)
		cursor.bytes = ref.bytes
	}
}

func writeGap(w *diagnosticWriter, bytes uint64) {
	w.startLine()
	printf(w, "--- DIAGNOSTIC OUTPUT OMITTED: %d bytes\n", bytes)
}

func (s *diagnosticStore) replayCommitted(w *diagnosticWriter, ref *diagnosticRef, cursor *diagnosticCursor) {
	offset := ref.first
	if cursor.last != 0 {
		last, err := s.header(ref, cursor.last, cursor.fragments-1)
		if err != nil || s.reciprocal(last.ordinal == cursor.ordinal) != nil {
			cursor.broken = true
			return
		}
		offset = last.next
	}
	for cursor.fragments < ref.committedFragments {
		header, err := s.header(ref, offset, cursor.fragments)
		if err != nil || s.reciprocal(header.prev == cursor.last && header.ordinal > cursor.ordinal) != nil {
			cursor.broken = true
			return
		}
		remaining := ref.committedBytes - cursor.bytes
		if uint64(header.length) > remaining ||
			(uint64(header.length) == remaining) != (cursor.fragments == ref.committedFragments-1) {
			s.fail("read invalid length")
			cursor.broken = true
			return
		}
		n, err := s.copyPayload(w, ref, offset, 0, int(header.length))
		if n > 0 {
			cursor.bytes += uint64(n)
		}
		if err != nil {
			cursor.broken = !w.failed()
			return
		}
		cursor.last, cursor.ordinal, offset = offset, header.ordinal, header.next
		cursor.fragments++
	}
}

func (out *diagnosticOutput) endsLine() bool {
	if len(out.legacy) != 0 {
		return strings.HasSuffix(out.legacy[len(out.legacy)-1].text, "\n")
	}
	return out.ref.endsLine
}

// stickyWriter makes replay stop on the first delivery error. Subsequent
// writes to this stream return that error without touching the sink.
type stickyWriter struct {
	io.Writer
	err error
}

func (w *stickyWriter) Write(data []byte) (int, error) {
	if w.err != nil {
		return 0, w.err
	}
	n, err := w.Writer.Write(data)
	return w.result(n, err, len(data))
}

func (w *stickyWriter) WriteString(data string) (int, error) {
	if w.err != nil {
		return 0, w.err
	}
	n, err := io.WriteString(w.Writer, data)
	return w.result(n, err, len(data))
}

func (w *stickyWriter) result(n int, err error, expected int) (int, error) {
	if err == nil && n != expected {
		err = io.ErrShortWrite
	}
	w.err = err
	return n, err
}
