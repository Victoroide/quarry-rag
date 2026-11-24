//go:build ignore

// Generate the normalization data embedded in the unpublished catalog migration.
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"unicode"
	"unicode/utf8"

	"golang.org/x/text/unicode/norm"
)

func main() {
	const start = "$unicode_data$\n"
	const end = "\n$unicode_data$"
	if len(os.Args) != 2 {
		panic("expected the catalog migration path")
	}
	path := os.Args[1]
	source, err := os.ReadFile(path)
	if err != nil {
		panic(err)
	}
	before, rest, found := strings.Cut(string(source), start)
	_, after, closed := strings.Cut(rest, end)
	if !found || !closed {
		panic("missing normalization data block")
	}
	if err := os.WriteFile(path, []byte(before+start+buildData()+end+after), 0o644); err != nil {
		panic(err)
	}
}

func buildData() string {
	var properties, compositions []string
	for cp := rune(1); cp <= unicode.MaxRune; cp++ {
		// Hangul syllables use their algorithmic decomposition and composition.
		if !utf8.ValidRune(cp) || cp >= 0xAC00 && cp < 0xD7A4 {
			continue
		}
		input := string(cp)
		decomposed := norm.NFD.String(input)
		leading, trailing := nonstarters(input)
		ccc := int(norm.NFC.PropertiesString(input).CCC())
		if leading != 0 || trailing != 0 || ccc != 0 || decomposed != input {
			props := []int{ccc, leading, trailing}
			if decomposed != input {
				for _, part := range decomposed {
					props = append(props, int(part))
				}
			}
			encoded, err := json.Marshal(props)
			if err != nil {
				panic(err)
			}
			properties = append(properties, fmt.Sprintf(`"%d":%s`, cp, encoded))
		}
		if decomposed == input || norm.NFC.String(decomposed) != input {
			continue
		}
		last, width := utf8.DecodeLastRuneInString(decomposed)
		prefix := norm.NFC.String(decomposed[:len(decomposed)-width])
		starter, starterWidth := utf8.DecodeRuneInString(prefix)
		if starterWidth == len(prefix) && prefix != "" {
			compositions = append(compositions, fmt.Sprintf(`"%d,%d":%d`, starter, last, cp))
		}
	}
	return fmt.Sprintf("{\n  \"version\": %q,\n  \"runes\": {\n%s\n  },\n  \"pairs\": {\n%s\n  }\n}",
		norm.Version, groupEntries(properties), groupEntries(compositions))
}

func nonstarters(input string) (leading, trailing int) {
	// Stream-safe counting uses the compatibility decomposition. Runes that
	// combine backward also contribute, even when their combining class is zero.
	decomposed := []rune(norm.NFKD.String(input))
	for _, cp := range decomposed {
		if norm.NFKC.PropertiesString(string(cp)).BoundaryBefore() {
			break
		}
		leading++
	}
	for i := len(decomposed) - 1; i >= 0; i-- {
		if norm.NFKC.PropertiesString(string(decomposed[i])).BoundaryBefore() {
			break
		}
		trailing++
	}
	return leading, trailing
}

func groupEntries(entries []string) string {
	const perLine = 5
	var lines []string
	for len(entries) > 0 {
		count := min(perLine, len(entries))
		lines = append(lines, "    "+strings.Join(entries[:count], ", "))
		entries = entries[count:]
	}
	return strings.Join(lines, ",\n")
}
