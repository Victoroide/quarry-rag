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
	id                 uint32
	first, last        uint64
	committedFragments uint64
	committedBytes     uint64
	fragments, bytes   uint64
	endsLine           bool
}

type spoolHeader struct {
	prev, next uint64
	length     uint32
}

type fallbackSlot struct {
	data     [fallbackSlotBytes]byte
	id       uint32
	position uint64
	payload  []byte
}

type diagnosticStore struct {
	factory         spoolFactory
	file            spoolFile
	remove          func() error
	limit, end      uint64
	uncommittedTail uint64
	nextID          uint32
	failure         string
	buffer          [32 * 1024]byte
	fallback        *[fallbackSlots]fallbackSlot
	slots           uint64
}

func newDiagnosticStore(factory spoolFactory) *diagnosticStore {
	return &diagnosticStore{factory: factory, limit: spoolLimit, end: 1}
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
	if ref.id == 0 {
		s.nextID++
		ref.id = s.nextID
	}
	position := ref.bytes
	ref.bytes = saturatingAdd(position, uint64(len(output)))
	ref.fragments = saturatingAdd(ref.fragments, 1)
	ref.endsLine = strings.HasSuffix(output, "\n")
	if s.failure == "" {
		s.appendSpool(ref, output)
		if s.failure == "" {
			return
		}
	}
	s.appendFallback(ref.id, position, output)
}

func (s *diagnosticStore) appendSpool(ref *diagnosticRef, output string) {
	length := uint64(len(output))
	if length > math.MaxUint32 || s.end > s.limit || s.limit-s.end < spoolHeaderSize || length > s.limit-s.end-spoolHeaderSize {
		s.fail("write quota exceeded")
		return
	}
	if s.file == nil {
		file, remove, err := s.factory()
		if err != nil {
			s.fail("create failed")
			return
		}
		s.file, s.remove = file, remove
	}
	var header [spoolHeaderSize]byte
	binary.LittleEndian.PutUint64(header[0:8], ref.last)
	binary.LittleEndian.PutUint32(header[16:20], uint32(length))
	binary.LittleEndian.PutUint32(header[20:24], ref.id)
	if !s.writeAt(header[:], s.end) {
		return
	}
	offset := s.end + spoolHeaderSize
	for output != "" {
		n := copy(s.buffer[:], output)
		if !s.writeAt(s.buffer[:n], offset) {
			return
		}
		offset += uint64(len(s.buffer[:n]))
		output = output[n:]
	}
	if ref.last != 0 {
		binary.LittleEndian.PutUint64(header[8:16], s.end)
		if !s.writeAt(header[8:16], ref.last+8) {
			s.uncommittedTail = ref.last
			return
		}
	} else {
		ref.first = s.end
	}
	ref.last = s.end
	ref.committedBytes = ref.bytes
	ref.committedFragments++
	s.end = offset
}

func (s *diagnosticStore) writeAt(data []byte, offset uint64) bool {
	if offset > math.MaxInt64 {
		s.fail("write invalid offset")
		return false
	}
	if n, err := s.file.WriteAt(data, int64(offset)); n != len(data) || err != nil {
		s.fail("write failed")
		return false
	}
	return true
}

func (s *diagnosticStore) readAt(data []byte, offset uint64) error {
	if offset > math.MaxInt64 {
		s.fail("read invalid offset")
		return errSpoolRecord
	}
	if n, err := s.file.ReadAt(data, int64(offset)); n != len(data) || (err != nil && !errors.Is(err, io.EOF)) {
		s.fail("read failed")
		return errSpoolRecord
	}
	return nil
}

func (s *diagnosticStore) header(ref *diagnosticRef, offset, index uint64) (spoolHeader, error) {
	var raw [spoolHeaderSize]byte
	if offset == 0 || s.end < spoolHeaderSize || offset > s.end-spoolHeaderSize || index >= ref.committedFragments {
		s.fail("read invalid record")
		return spoolHeader{}, errSpoolRecord
	}
	if err := s.readAt(raw[:], offset); err != nil {
		return spoolHeader{}, err
	}
	prev := binary.LittleEndian.Uint64(raw[:8])
	next := binary.LittleEndian.Uint64(raw[8:16])
	length := binary.LittleEndian.Uint32(raw[16:20])
	last := index == ref.committedFragments-1
	// The committed tail's next field can contain an uncommitted patch.
	if last && offset == s.uncommittedTail {
		next = 0
	}
	if binary.LittleEndian.Uint32(raw[20:]) != ref.id ||
		length == 0 || uint64(length) > s.end-offset-spoolHeaderSize ||
		(index == 0) != (offset == ref.first) || (prev == 0) != (index == 0) ||
		(prev != 0 && (offset <= spoolHeaderSize || prev >= offset-spoolHeaderSize)) ||
		last != (offset == ref.last) || (last && next != 0) ||
		(!last && (next < offset+spoolHeaderSize+uint64(length) || next > ref.last)) {
		s.fail("read invalid record")
		return spoolHeader{}, errSpoolRecord
	}
	return spoolHeader{prev: prev, next: next, length: length}, nil
}

func (s *diagnosticStore) reciprocal(valid bool) error {
	if !valid {
		s.fail("read invalid link")
		return errSpoolRecord
	}
	return nil
}

func (s *diagnosticStore) copyPayload(w io.Writer, offset uint64, start, limit int) (int, error) {
	written := 0
	for start >= 0 && start < limit {
		data := s.buffer[:min(len(s.buffer), limit-start)]
		if err := s.readAt(data, offset+spoolHeaderSize+uint64(start)); err != nil {
			return written, err
		}
		if start+len(data) < limit {
			// A later read can fail. Leave any partial rune for the next chunk.
			if last := len(data) - 1; data[last] >= utf8.RuneSelf {
				if _, size := utf8.DecodeLastRune(data); size == 1 {
					for last > 0 && !utf8.RuneStart(data[last]) {
						last--
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

func (s *diagnosticStore) appendFallback(id uint32, position uint64, output string) {
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
