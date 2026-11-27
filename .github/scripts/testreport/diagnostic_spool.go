package main

import (
	"encoding/binary"
	"errors"
	"io"
	"math"
	"os"
	"strings"
	"unicode/utf8"
)

const (
	spoolLimit        = 512 * 1024 * 1024
	spoolHeaderSize   = 24
	fallbackSlots     = 128
	fallbackSlotBytes = 4096
)

var errSpoolRecord = errors.New("invalid spool record")

type spoolFile interface {
	io.ReaderAt
	io.WriterAt
	io.Closer
}

type spoolFactory func() (spoolFile, func() error, error)

func createDiagnosticSpool() (spoolFile, func() error, error) {
	f, err := os.CreateTemp("", "quarry-testreport-*")
	if err != nil {
		return nil, nil, err
	}
	return f, func() error { return os.Remove(f.Name()) }, nil
}

type diagnosticRef struct {
	spoolPages
	id                 uint64
	first, last        uint64
	committedFragments uint64
	committedBytes     uint64
	fragments, bytes   uint64
	endsLine           bool
}

type spoolHeader struct {
	prev, next uint64
	length     uint32
	ordinal    uint64
}

type spoolLink struct {
	id, offset uint64
}

type fallbackSlot struct {
	data     [fallbackSlotBytes]byte
	id       uint64
	position uint64
	payload  []byte
}

type diagnosticStore struct {
	pageAllocator
	factory         spoolFactory
	file            spoolFile
	remove          func() error
	limit           uint64
	uncommittedTail spoolLink
	nextID, ordinal uint64
	failure         string
	buffer          [32 * 1024]byte
	record          [spoolHeaderSize]byte
	fallback        *[fallbackSlots]fallbackSlot
	slots           uint64
}

func newDiagnosticStore(factory spoolFactory) *diagnosticStore {
	return &diagnosticStore{factory: factory, limit: spoolLimit}
}

func (s *diagnosticStore) fail(reason string) {
	if s.failure == "" {
		s.failure = reason
	}
}

func (s *diagnosticStore) close() {
	if s.file == nil {
		return
	}
	if s.file.Close() != nil {
		s.fail("close failed")
	}
	if s.remove() != nil {
		s.fail("remove failed")
	}
}

func saturatingAdd(a, b uint64) uint64 {
	if b > math.MaxUint64-a {
		return math.MaxUint64
	}
	return a + b
}

func (s *diagnosticStore) append(ref *diagnosticRef, output string) {
	if output == "" {
		return
	}
	position := ref.bytes
	ref.bytes = saturatingAdd(position, uint64(len(output)))
	ref.fragments = saturatingAdd(ref.fragments, 1)
	ref.endsLine = strings.HasSuffix(output, "\n")
	if ref.id == 0 {
		if s.nextID == math.MaxUint64 {
			s.fail("write chain limit exceeded")
			return
		}
		s.nextID++
		ref.id = s.nextID
	}
	if s.failure == "" {
		s.appendSpool(ref, output)
		if s.failure == "" {
			return
		}
	}
	s.appendFallback(ref.id, position, output)
}

func (s *diagnosticStore) appendSpool(ref *diagnosticRef, output string) {
	if s.ordinal == math.MaxUint64 || ref.id > math.MaxUint32 {
		s.fail("write record limit exceeded")
		return
	}
	pages, ok := s.reservePages(ref.spoolPages, spoolHeaderSize+uint64(len(output)))
	if !ok {
		s.fail("write quota exceeded")
		return
	}
	offset := max(1, ref.end)
	ordinal := s.ordinal + 1
	if !s.open() || !s.writeFragment(ref, pages, output, offset, ordinal) {
		s.releasePages(pages.pages[len(ref.pages):])
		return
	}
	if ref.first == 0 {
		ref.first = offset
	}
	ref.spoolPages = pages
	ref.last = offset
	ref.committedBytes = ref.bytes
	ref.committedFragments++
	s.ordinal = ordinal
}

func (s *diagnosticStore) open() bool {
	if s.file != nil {
		return true
	}
	file, remove, err := s.factory()
	if err != nil {
		s.fail("create failed")
		return false
	}
	s.file, s.remove = file, remove
	return true
}

func (s *diagnosticStore) writeFragment(ref *diagnosticRef, pages spoolPages, output string, offset, ordinal uint64) bool {
	previous, id, length := ref.last, ref.id, uint64(len(output))
	if previous > math.MaxUint32 || id > math.MaxUint32 || length > math.MaxUint32 || offset > math.MaxUint32 {
		s.fail("write invalid record")
		return false
	}
	header := s.record[:]
	binary.LittleEndian.PutUint32(header[:4], uint32(previous))
	binary.LittleEndian.PutUint32(header[4:8], 0)
	binary.LittleEndian.PutUint32(header[8:12], uint32(length))
	binary.LittleEndian.PutUint32(header[12:16], uint32(id))
	binary.LittleEndian.PutUint64(header[16:], ordinal)
	if !s.writeAt(pages, header, offset) {
		return false
	}
	position := offset + spoolHeaderSize
	for output != "" {
		n := copy(s.buffer[:], output)
		chunk := s.buffer[:n]
		if !s.writeAt(pages, chunk, position) {
			return false
		}
		position += uint64(len(chunk))
		output = output[n:]
	}
	if ref.last != 0 {
		binary.LittleEndian.PutUint32(header[4:8], uint32(offset))
		if !s.writeAt(pages, header[4:8], ref.last+4) {
			s.uncommittedTail = spoolLink{id: ref.id, offset: ref.last}
			return false
		}
	}
	return true
}

func (s *diagnosticStore) header(ref *diagnosticRef, offset, index uint64) (spoolHeader, error) {
	raw := s.record[:]
	if !ref.contains(offset, spoolHeaderSize) || index >= ref.committedFragments {
		s.fail("read invalid record")
		return spoolHeader{}, errSpoolRecord
	}
	if err := s.readAt(ref, raw, offset); err != nil {
		return spoolHeader{}, err
	}
	h := spoolHeader{
		prev:    uint64(binary.LittleEndian.Uint32(raw[:4])),
		next:    uint64(binary.LittleEndian.Uint32(raw[4:8])),
		length:  binary.LittleEndian.Uint32(raw[8:12]),
		ordinal: binary.LittleEndian.Uint64(raw[16:]),
	}
	last := index == ref.committedFragments-1
	// A failed patch may have changed the committed tail's next field.
	if last && s.uncommittedTail == (spoolLink{id: ref.id, offset: offset}) {
		h.next = 0
	}
	if uint64(binary.LittleEndian.Uint32(raw[12:16])) != ref.id ||
		h.ordinal == 0 || h.ordinal > s.ordinal || !ref.validHeader(h, offset, index == 0, last) {
		s.fail("read invalid record")
		return spoolHeader{}, errSpoolRecord
	}
	return h, nil
}

func (ref *diagnosticRef) validHeader(h spoolHeader, offset uint64, first, last bool) bool {
	if h.length == 0 || !ref.contains(offset+spoolHeaderSize, uint64(h.length)) ||
		first != (offset == ref.first) || first != (h.prev == 0) ||
		(h.prev != 0 && (offset <= spoolHeaderSize || h.prev >= offset-spoolHeaderSize)) ||
		last != (offset == ref.last) {
		return false
	}
	end := offset + spoolHeaderSize + uint64(h.length)
	if last {
		return h.next == 0 && end == ref.end
	}
	return h.next == end && h.next <= ref.last
}

func (s *diagnosticStore) reciprocal(valid bool) error {
	if !valid {
		s.fail("read invalid link")
		return errSpoolRecord
	}
	return nil
}

func (s *diagnosticStore) copyPayload(w io.Writer, ref *diagnosticRef, offset uint64, start, limit int) (int, error) {
	written := 0
	for start >= 0 && start < limit {
		data := s.buffer[:min(len(s.buffer), limit-start)]
		if err := s.readAt(ref, data, offset+spoolHeaderSize+uint64(start)); err != nil {
			return written, err
		}
		if start+len(data) < limit {
			// A later read can fail. Leave any partial rune for the next chunk.
			if last := len(data) - 1; data[last] >= utf8.RuneSelf {
				if _, size := utf8.DecodeLastRune(data); size == 1 {
					first := max(0, len(data)-utf8.UTFMax+1)
					for last > first && !utf8.RuneStart(data[last]) {
						last--
					}
					if last == 0 || !utf8.RuneStart(data[last]) {
						s.fail("read invalid payload")
						return written, errSpoolRecord
					}
					data = data[:last]
				}
			}
		}
		n, err := w.Write(data)
		written += n
		if err != nil {
			return written, err
		}
		if n != len(data) {
			return written, io.ErrShortWrite
		}
		start += n
	}
	return written, nil
}

func (s *diagnosticStore) appendFallback(id, position uint64, output string) {
	if s.fallback == nil {
		s.fallback = new([fallbackSlots]fallbackSlot)
	}
	for output != "" {
		n := min(len(output), fallbackSlotBytes)
		// Keep each evictable slot's endpoints on rune boundaries.
		for n < len(output) && n > 0 && !utf8.RuneStart(output[n]) {
			n--
		}
		if n == 0 {
			n = min(len(output), fallbackSlotBytes)
		}
		slot := &s.fallback[s.slots%fallbackSlots]
		copy(slot.data[:], output[:n])
		slot.id, slot.position, slot.payload = id, position, slot.data[:n]
		s.slots = saturatingAdd(s.slots, 1)
		position = saturatingAdd(position, uint64(len(slot.payload)))
		output = output[n:]
	}
}

func (s *diagnosticStore) firstSlot() uint64 {
	return s.slots - min(s.slots, fallbackSlots)
}

func (s *diagnosticStore) slot(index uint64) *fallbackSlot {
	return &s.fallback[index%fallbackSlots]
}
