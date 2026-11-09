package pipeline

import (
	"math"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"golang.org/x/text/unicode/norm"
)

// Citation reports the lexical overlap of a sentence with a numbered excerpt.
type Citation struct {
	Excerpt    int
	DocumentID domain.DocumentID
	ChunkIndex int
	Offset     int
	Verified   bool
	Overlap    float64
}

// VerifyCitations cleans excerpt markers and checks their lexical overlap.
func VerifyCitations(answer string, excerpts []domain.ScoredChunk) (string, []Citation) {
	cleaned := cleanCitationMarkers(answer, len(excerpts))
	citations := make([]Citation, 0)
	excerptTokens := make(map[int]map[string]struct{})
	var sentence strings.Builder
	previous, byteOffset, runeOffset := 0, 0, 0
	for _, match := range citationMarker.FindAllStringIndex(cleaned, -1) {
		text := cleaned[previous:match[0]]
		if boundary := strings.LastIndexFunc(text, citationSentenceBoundary); boundary >= 0 {
			sentence.Reset()
			_, size := utf8.DecodeRuneInString(text[boundary:])
			text = text[boundary+size:]
		}
		sentence.WriteString(text)
		tokens := citationTokens(sentence.String())
		runeOffset += utf8.RuneCountInString(cleaned[byteOffset:match[0]])
		byteOffset = match[0]
		for _, number := range citationNumbers(cleaned[match[0]:match[1]]) {
			if _, ok := excerptTokens[number]; !ok {
				excerptTokens[number] = citationTokens(excerpts[number-1].Chunk.Text)
			}
			shared := 0
			for token := range tokens {
				if _, ok := excerptTokens[number][token]; ok {
					shared++
				}
			}
			overlap := 0.0
			if len(tokens) > 0 {
				overlap = math.Round(100*float64(shared)/float64(len(tokens))) / 100
			}
			excerpt := excerpts[number-1].Chunk
			citations = append(citations, Citation{Excerpt: number, DocumentID: excerpt.DocumentID, ChunkIndex: excerpt.Index, Offset: runeOffset, Verified: overlap >= .3, Overlap: overlap})
		}
		previous = match[1]
	}
	return cleaned, citations
}

var citationMarker = regexp.MustCompile(`\[\d{1,3}(?:\s*,\s*\d{1,3})*\]`)

func citationNumbers(marker string) []int {
	parts := strings.Split(marker[1:len(marker)-1], ",")
	numbers := make([]int, 0, len(parts))
	for _, part := range parts {
		number, _ := strconv.Atoi(strings.TrimSpace(part))
		numbers = append(numbers, number)
	}
	return numbers
}

func cleanCitationMarkers(answer string, count int) string {
	type opening struct {
		start   int
		blocked bool
	}
	var stack []opening
	out := make([]byte, 0, len(answer))
	for i := range len(answer) {
		switch answer[i] {
		case '[':
			stack = append(stack, opening{start: len(out)})
			out = append(out, '[')
		case ']':
			out = append(out, ']')
			if len(stack) == 0 {
				continue
			}
			open := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			candidate := out[open.start:]
			var match []int
			if !open.blocked {
				match = citationMarker.FindIndex(candidate)
			}
			if len(match) == 2 && match[0] == 0 && match[1] == len(candidate) {
				numbers := citationNumbers(string(candidate))
				out = out[:open.start]
				kept := 0
				for j, number := range numbers {
					if number < 1 || number > count || slices.Contains(numbers[:j], number) {
						continue
					}
					if kept == 0 {
						out = append(out, '[')
					} else {
						out = append(out, ',')
					}
					out = strconv.AppendInt(out, int64(number), 10)
					kept++
				}
				if kept == 0 {
					if len(out) > 0 && out[len(out)-1] == ' ' {
						out = out[:len(out)-1]
					}
					continue
				}
				out = append(out, ']')
			}
			// A parent that keeps a nested bracket cannot be a marker; skip rescanning it.
			if len(stack) > 0 {
				stack[len(stack)-1].blocked = true
			}
		default:
			out = append(out, answer[i])
		}
	}
	return string(out)
}

func citationSentenceBoundary(r rune) bool {
	return strings.ContainsRune(".!?。！？\n", r)
}

func citationDenseRune(r rune) bool {
	return unicode.In(r, unicode.Han, unicode.Hiragana, unicode.Katakana, unicode.Hangul,
		unicode.Thai, unicode.Lao, unicode.Khmer, unicode.Myanmar, unicode.Tibetan)
}

func citationTokens(text string) map[string]struct{} {
	tokens := make(map[string]struct{})
	var run []rune
	dense, digit := false, false
	flush := func() {
		if dense {
			for i := 1; i < len(run); i++ {
				tokens[string(run[i-1:i+1])] = struct{}{}
			}
		} else if len(run) >= 3 || digit {
			tokens[string(run)] = struct{}{}
		}
		run, digit = run[:0], false
	}
	for _, r := range strings.ToLower(norm.NFC.String(text)) {
		// Excerpts lose format characters before synthesis, so they never split words.
		if unicode.Is(unicode.Cf, r) {
			continue
		}
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) && !unicode.IsMark(r) {
			flush()
			continue
		}
		isDense := citationDenseRune(r)
		// Combining marks and script-neutral letters such as ー stay in the run they follow.
		if len(run) > 0 && !unicode.IsDigit(r) && unicode.In(r, unicode.Common, unicode.Inherited) {
			isDense = dense
		}
		if len(run) > 0 && dense != isDense {
			flush()
		}
		dense = isDense
		digit = digit || unicode.IsDigit(r)
		run = append(run, r)
	}
	flush()
	return tokens
}
