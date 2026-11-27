package main

import (
	"bytes"
	"fmt"
	"math"
	"os"
	"reflect"
	"strings"
	"testing"
)

func newProbedDiagnosticStore(t *testing.T, limit uint64) (*diagnosticStore, *spoolProbe, *int) {
	t.Helper()
	dir := t.TempDir()
	var creates int
	probe := &spoolProbe{}
	s := newDiagnosticStore(func() (spoolFile, func() error, error) {
		creates++
		f, err := os.CreateTemp(dir, "spool-*")
		if err != nil {
			return nil, nil, err
		}
		probe.spoolFile = f
		return probe, func() error { return os.Remove(f.Name()) }, nil
	})
	s.limit = limit
	t.Cleanup(s.close)
	return s, probe, &creates
}

func TestDiagnosticPageAccounting(t *testing.T) {
	s, probe, creates := newProbedDiagnosticStore(t, 3*spoolPageSize)
	var first, next diagnosticRef
	s.append(&first, "")
	if *creates != 0 || first.id != 0 {
		t.Fatal("empty output allocated storage")
	}
	prefix := strings.Repeat("a", spoolPageSize-spoolHeaderSize-10)
	s.append(&first, prefix)
	s.append(&next, "other")
	s.append(&first, strings.Repeat("b", spoolPageSize-spoolHeaderSize+10))
	if !reflect.DeepEqual(first.pages, []uint32{0, 2}) || probe.highWater != 3*spoolPageSize {
		t.Fatalf("exact cap: pages %v, high water %d, failure %q", first.pages, probe.highWater, s.failure)
	}
	verifyPageOutput(t, s, &first, prefix+strings.Repeat("b", spoolPageSize-spoolHeaderSize+10))
	nextID := next.id
	s.discard(&next)
	s.discard(&next)
	s.append(&next, "recycled")
	if next.id <= nextID || !reflect.DeepEqual(next.pages, []uint32{1}) {
		t.Fatalf("recycled ref: id %d, pages %v", next.id, next.pages)
	}
	before := probe.writes
	s.append(&first, "over budget")
	if s.failure != "write quota exceeded" || probe.writes != before || len(first.pages) != 2 {
		t.Fatalf("quota overflow: writes %d (was %d), pages %v, failure %q", probe.writes, before, first.pages, s.failure)
	}
	s.discard(&next)
	s.append(&next, "ring only")
	verifyPageOutput(t, s, &next, "ring only")
	if probe.writes != before || *creates != 1 {
		t.Fatal("discard restarted the disk writer")
	}
}

func TestDiagnosticPageRejectsInvalidRanges(t *testing.T) {
	for _, offset := range []uint64{0, math.MaxUint64, spoolPageSize} {
		t.Run(fmt.Sprint(offset), func(t *testing.T) {
			s, probe, _ := newProbedDiagnosticStore(t, spoolPageSize)
			var ref diagnosticRef
			s.append(&ref, "x")
			writes, reads := probe.writes, probe.reads
			if s.readAt(&ref, make([]byte, 2), offset) == nil || s.writeAt(ref.spoolPages, []byte("zz"), offset) {
				t.Fatal("invalid range accepted")
			}
			if probe.writes != writes || probe.reads != reads {
				t.Fatal("invalid range reached the spool file")
			}
		})
	}
	s, _, creates := newProbedDiagnosticStore(t, spoolPageSize-1)
	var ref diagnosticRef
	s.append(&ref, "x")
	if *creates != 0 || s.failure != "write quota exceeded" {
		t.Fatal("zero-page quota opened the spool")
	}
}

func TestDiagnosticPagePhysicalCap(t *testing.T) {
	s, probe, _ := newProbedDiagnosticStore(t, spoolLimit)
	pages, ok := s.reservePages(spoolPages{}, spoolLimit)
	if !ok || pages.end != spoolLimit+1 || s.nextPage != spoolLimit/spoolPageSize {
		t.Fatalf("exact extent reservation: accepted = %t, end = %d, pages = %d", ok, pages.end, s.nextPage)
	}
	if !s.open() || !s.writeAt(pages, []byte("x"), spoolLimit) || probe.highWater != spoolLimit {
		t.Fatalf("write at exact cap: failure %q, high water %d", s.failure, probe.highWater)
	}
	if _, ok := s.reservePages(pages, 1); ok || s.writeAt(pages, []byte("x"), spoolLimit+1) || probe.writes != 1 {
		t.Fatal("accepted writes beyond the real cap")
	}
}

func TestDiagnosticPageCoalescesContiguousIO(t *testing.T) {
	s, probe, _ := newProbedDiagnosticStore(t, spoolLimit)
	var ref diagnosticRef
	payload := volumeChunkText("contiguous")
	for range 3 {
		probe.writes, probe.reads = 0, 0
		s.append(&ref, payload)
		verifyPageOutput(t, s, &ref, payload)
		if probe.writes != 2 || probe.reads != 2 {
			t.Fatalf("contiguous fragment: %d writes, %d reads", probe.writes, probe.reads)
		}
		s.discard(&ref)
	}
}

func FuzzDiagnosticPageAllocator(f *testing.F) {
	f.Add([]byte{0, 252, 1, 16, 0, 21, 5, 0, 2, 220, 3, 255})
	f.Add([]byte{4, 0, 4, 0, 0, 0, 3, 250, 7, 0, 3, 250})
	f.Fuzz(func(t *testing.T, ops []byte) {
		const limit = 8 * spoolPageSize
		s, probe, creates := newProbedDiagnosticStore(t, limit)
		var refs [4]diagnosticRef
		var model [4]string
		var logical [4]int
		for i := 0; i+1 < min(len(ops), 256); i += 2 {
			chain := int(ops[i] % byte(len(refs)))
			ref := &refs[chain]
			if ops[i]&4 != 0 {
				s.discard(ref)
				model[chain] = ""
				logical[chain] = 0
			} else {
				length := int(ops[i+1]) * 3
				part := strings.Repeat(string(rune('a'+chain)), length)
				s.append(ref, part)
				model[chain] += part
				if part != "" {
					logical[chain] += spoolHeaderSize + length
				}
			}
			var allocated int
			for _, size := range logical {
				allocated += (size + spoolPageSize - 1) / spoolPageSize
			}
			if (allocated > limit/spoolPageSize) != (s.failure != "") {
				t.Fatalf("model needs %d pages, allocator reported %q", allocated, s.failure)
			}
			if s.failure != "" {
				if s.failure != "write quota exceeded" || probe.highWater > limit {
					t.Fatalf("unexpected allocator failure %q or extent %d", s.failure, probe.highWater)
				}
				verifyPageOutput(t, s, ref, model[chain])
				break
			}
			assertPageOwners(t, s, refs[:])
			for j := range refs {
				verifyPageOutput(t, s, &refs[j], model[j])
			}
		}
		if *creates > 1 || probe.highWater > limit {
			t.Fatalf("creates %d, extent %d", *creates, probe.highWater)
		}
	})
}

func TestDiagnosticPageOrdinalOverflow(t *testing.T) {
	s, probe, creates := newProbedDiagnosticStore(t, spoolPageSize)
	s.ordinal = math.MaxUint64 - 1
	var ref diagnosticRef
	s.append(&ref, "last disk ordinal\n")
	writes := probe.writes
	s.append(&ref, "first fallback fragment\n")
	verifyPageOutput(t, s, &ref, "last disk ordinal\nfirst fallback fragment\n")
	if s.ordinal != math.MaxUint64 || s.failure != "write record limit exceeded" || probe.writes != writes || *creates != 1 {
		t.Fatalf("ordinal overflow: ordinal %d, writes %d, failure %q", s.ordinal, probe.writes, s.failure)
	}
}

func assertPageOwners(t *testing.T, s *diagnosticStore, refs []diagnosticRef) {
	t.Helper()
	owned := make(map[uint32]bool)
	for _, ref := range refs {
		for _, page := range ref.pages {
			if owned[page] || page >= s.nextPage {
				t.Fatalf("duplicate or invalid live page %d", page)
			}
			owned[page] = true
		}
	}
	for _, page := range s.freePages {
		if owned[page] || page >= s.nextPage {
			t.Fatalf("duplicate or invalid free page %d", page)
		}
		owned[page] = true
	}
	if len(owned) != int(s.nextPage) {
		t.Fatalf("%d accounted pages, %d reserved", len(owned), s.nextPage)
	}
}

func verifyPageOutput(t *testing.T, s *diagnosticStore, ref *diagnosticRef, want string) {
	t.Helper()
	var got bytes.Buffer
	s.replay(&diagnosticWriter{Writer: &got}, ref, &diagnosticCursor{})
	if got.String() != want {
		t.Fatalf("replay: got %q, want %q", got.String(), want)
	}
}
