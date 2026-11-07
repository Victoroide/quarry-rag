package textutil

import (
	"strings"
	"testing"
	"unicode"
	"unicode/utf8"
)

var normalizationFuzzSeeds = []string{
	"", "Readable plain text.", "docu-\n ments\r\n\r\nNext paragraph.",
	"世界 🌍 e\u0301 ﬁ Ａ", "a\u200b\u202e\u2066\ufeffb", "a\x00\x01\x1f\x7f\u0085b\n\t", "a\xff\xfeb",
}

func FuzzSanitizeForPrompt(f *testing.F) {
	for _, seed := range normalizationFuzzSeeds {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, s string) {
		if len(s) > 64<<10 {
			t.Skip()
		}
		out := SanitizeForPrompt(s)
		assertNormalizedRunes(t, s, out)
		if SanitizeForPrompt(out) != out {
			t.Fatal("prompt sanitization is not idempotent")
		}
	})
}

func FuzzNormalizeForIngest(f *testing.F) {
	for _, seed := range normalizationFuzzSeeds {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, s string) {
		if len(s) > 64<<10 {
			t.Skip()
		}
		out := NormalizeForIngest(s)
		assertNormalizedRunes(t, s, out)
		if strings.ContainsRune(out, '\r') {
			t.Fatal("ingest normalization retained a carriage return")
		}
	})
}

func assertNormalizedRunes(t *testing.T, input, out string) {
	t.Helper()
	if utf8.ValidString(input) && !utf8.ValidString(out) {
		t.Fatal("valid UTF-8 became invalid")
	}
	for _, r := range out {
		if unicode.Is(unicode.Cf, r) || (unicode.IsControl(r) && r != '\n' && r != '\t') {
			t.Fatalf("output contains prohibited rune %U", r)
		}
	}
}
