package main

import (
	"errors"
	"io"
	"math"
)

const spoolPageSize = 512

type spoolPages struct {
	pages []uint32
	end   uint64
}

type pageAllocator struct {
	nextPage  uint32
	freePages []uint32
}

func (s *diagnosticStore) pageLimit() uint64 {
	return min(s.limit, spoolLimit) / spoolPageSize
}

// reservePages stages a mapping without publishing it to the ref.
func (s *diagnosticStore) reservePages(current spoolPages, size uint64) (spoolPages, bool) {
	end := max(1, current.end)
	limit := s.pageLimit()
	extent := limit * spoolPageSize
	used := end - 1
	reserved := uint64(s.nextPage)
	if reserved > limit || used > extent || size > extent-used {
		return spoolPages{}, false
	}
	end += size
	pageCount := (end - 1 + spoolPageSize - 1) / spoolPageSize
	if pageCount > math.MaxInt {
		return spoolPages{}, false
	}
	count := int(pageCount)
	added := count - len(current.pages)
	if added < 0 || uint64(added) > uint64(len(s.freePages))+limit-reserved {
		return spoolPages{}, false
	}
	pages := resizePages(current.pages, count)
	for i := len(current.pages); i < count; i++ {
		if last := len(s.freePages) - 1; last >= 0 {
			pages[i] = s.freePages[last]
			s.freePages = s.freePages[:last]
		} else {
			pages[i] = s.nextPage
			s.nextPage++
		}
	}
	return spoolPages{pages: pages, end: end}, true
}

func resizePages(pages []uint32, length int) []uint32 {
	if length <= cap(pages) {
		return pages[:length]
	}
	grown := make([]uint32, length, max(length, 2*cap(pages)))
	copy(grown, pages)
	return grown
}

func (s *diagnosticStore) releasePages(pages []uint32) {
	start := len(s.freePages)
	s.freePages = resizePages(s.freePages, start+len(pages))
	// Keep each released chain in reverse so a subsequent pop can recover
	// its forward spans in the same order.
	for i, page := range pages {
		s.freePages[start+len(pages)-1-i] = page
	}
}

func (s *diagnosticStore) discard(ref *diagnosticRef) {
	s.releasePages(ref.pages)
	*ref = diagnosticRef{}
}

func (p spoolPages) contains(offset, length uint64) bool {
	return offset != 0 && offset <= p.end && length <= p.end-offset
}

// span maps the longest physically contiguous prefix of a valid logical range.
func (p spoolPages) span(offset uint64, length int) (physical uint64, size int) {
	index, within := (offset-1)/spoolPageSize, (offset-1)%spoolPageSize
	page := p.pages[index]
	n := min(length, spoolPageSize-int(within))
	for n < length && p.pages[index+1] == p.pages[index]+1 {
		index++
		n += min(length-n, spoolPageSize)
	}
	return uint64(page)*spoolPageSize + within, n
}

func (s *diagnosticStore) validSpan(offset, length uint64) bool {
	extent := min(s.pageLimit(), uint64(s.nextPage)) * spoolPageSize
	return offset <= extent && length <= extent-offset
}

func (s *diagnosticStore) writeAt(pages spoolPages, data []byte, offset uint64) bool {
	if !pages.contains(offset, uint64(len(data))) {
		s.fail("write invalid offset")
		return false
	}
	for len(data) != 0 {
		at, span := pages.span(offset, len(data))
		chunk := data[:span]
		size := uint64(len(chunk))
		if at > math.MaxInt64 || !s.validSpan(at, size) {
			s.fail("write invalid offset")
			return false
		}
		if n, err := s.file.WriteAt(chunk, int64(at)); n != span || err != nil {
			s.fail("write failed")
			return false
		}
		data, offset = data[span:], offset+size
	}
	return true
}

func (s *diagnosticStore) readAt(ref *diagnosticRef, data []byte, offset uint64) error {
	if !ref.contains(offset, uint64(len(data))) {
		s.fail("read invalid offset")
		return errSpoolRecord
	}
	for len(data) != 0 {
		at, span := ref.span(offset, len(data))
		chunk := data[:span]
		size := uint64(len(chunk))
		if at > math.MaxInt64 || !s.validSpan(at, size) {
			s.fail("read invalid offset")
			return errSpoolRecord
		}
		if n, err := s.file.ReadAt(chunk, int64(at)); n != span || (err != nil && !errors.Is(err, io.EOF)) {
			s.fail("read failed")
			return errSpoolRecord
		}
		data, offset = data[span:], offset+size
	}
	return nil
}
