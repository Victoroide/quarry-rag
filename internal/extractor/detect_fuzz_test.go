package extractor

import (
	"context"
	"testing"
	"time"
)

func FuzzResolveContext(f *testing.F) {
	for _, seed := range extractorFuzzSeeds(f) {
		mismatch := "document.pdf"
		if seed.name == "document.pdf" {
			mismatch = "document.docx"
		}
		for _, name := range []string{seed.name, mismatch, ""} {
			f.Add(name, seed.data)
		}
	}
	f.Fuzz(func(t *testing.T, name string, data []byte) {
		if len(data) > 1<<20 || len(name) > 64<<10 {
			t.Skip()
		}
		resolve := func() (FileType, error) {
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel()
			return ResolveContext(ctx, name, data)
		}
		got, err := resolve()
		if err == nil {
			switch got {
			case FileTypeText, FileTypePDF, FileTypeDOCX, FileTypeDOC:
			default:
				t.Fatalf("unsupported successful type %q", got)
			}
		}
		again, againErr := resolve()
		if got != again || (err == nil) != (againErr == nil) {
			t.Fatalf("resolve is not deterministic: %q/%t then %q/%t", got, err == nil, again, againErr == nil)
		}
		sniffed := Sniff(data)
		if sniffed != Sniff(data) {
			t.Fatal("sniff is not deterministic")
		}
		if err == nil && got != sniffed {
			t.Fatalf("resolved %q but the content sniffs as %q", got, sniffed)
		}
	})
}
