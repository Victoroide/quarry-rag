package pipeline

import (
	"strconv"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/require"
)

const fuzzMaxAnswerBytes = 64 << 10

func FuzzCitationCodeRanges(f *testing.F) {
	for _, seed := range []string{
		"a `b[0]` c", "``a`b``", "`foo\\`bar`", "\\`x`", "```go\nx\n\ny\n```\n[1]", "~~~\na\n~~~~", "```\r\nx\r\n```\r\n",
		"```\ncode\n``` [1]", "   ```\nx", "    ```\nx\n    ```", "```x`y```", "> ```\n> a\n> ```", "档案 `x[0]` 记录", "\xff`\xfe`", "~~~`",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, text string) {
		if len(text) > fuzzMaxAnswerBytes {
			t.Skip()
		}
		ranges := citationCodeRanges(text)
		require.Equal(t, ranges, citationCodeRanges(text))
		valid := utf8.ValidString(text)
		previous := 0
		for _, r := range ranges {
			require.LessOrEqual(t, previous, r[0])
			require.Less(t, r[0], r[1])
			require.LessOrEqual(t, r[1], len(text))
			previous = r[1]
			code := text[r[0]:r[1]]
			require.Contains(t, "`~", code[:1])
			if valid {
				require.True(t, r[1] == len(text) || utf8.RuneStart(text[r[1]]))
			}
			if strings.Contains(code, "\n") {
				continue
			}
			// A single-line range is an inline span or a fence on its own line.
			leading := len(code) - len(strings.TrimLeft(code, code[:1]))
			trailing := len(code) - len(strings.TrimRight(code, "`"))
			if trailing > 0 && code[0] == '`' {
				require.Equal(t, leading, trailing)
				continue
			}
			require.Equal(t, len(text), r[1], "an unterminated single-line range is an unclosed fence")
			require.GreaterOrEqual(t, leading, 3)
		}
		// The membership scan is quadratic, so only short inputs get it.
		for pos := range min(len(text), 2048) {
			inside := false
			for _, r := range ranges {
				inside = inside || (r[0] <= pos && pos < r[1])
			}
			require.Equal(t, inside, inCitationCode(pos, ranges), pos)
		}
	})
}

func FuzzVerifyCitationsPreservingCode(f *testing.F) {
	for _, seed := range []string{
		"The first station is `stations[0]` [1].", "Use `a[01]` here [1].", "Use `items[1]` for the second item [1].",
		"```go\nx := a[0]\n\ny := b[1]\n```\nDone [1].", "Press the ` key [0].\n\nThen run `ls` [1].", "`a[9]` b [9] [1]",
		"Archive [see `x` 1] [1]", "`a`[0]`b`", "~~~\na[0]\n~~~~\n[1]", "```\ncode\n``` [1]", "[1[99]]", "plain [1, 2][999]", "\xff`[1]`",
	} {
		f.Add(seed, uint8(1))
	}
	f.Fuzz(func(t *testing.T, answer string, count uint8) {
		if len(answer) > fuzzMaxAnswerBytes {
			t.Skip()
		}
		excerpts := int(count) % 9
		hasDelimiter := strings.ContainsAny(answer, "`~")
		for _, streaming := range []bool{false, true} {
			legacyAnswer, legacy := preservingFinalize(t, answer, excerpts, streaming, false)
			gotAnswer, got := preservingFinalize(t, answer, excerpts, streaming, true)
			if !hasDelimiter {
				require.Equal(t, legacyAnswer, gotAnswer)
				require.Equal(t, legacy, got)
			}
			if streaming {
				require.Equal(t, answer, gotAnswer)
			}
			literal := citationCodeRanges(answer)
			if !streaming {
				_, cleanedLiteral := cleanCitationMarkersInto(answer, excerpts, nil, true)
				require.Len(t, cleanedLiteral, len(literal))
				for i, r := range literal {
					require.Equal(t, answer[r[0]:r[1]], gotAnswer[cleanedLiteral[i][0]:cleanedLiteral[i][1]])
				}
				literal = cleanedLiteral
			}
			runes := []rune(gotAnswer)
			for _, c := range got {
				require.Less(t, c.Offset, len(runes))
				require.Equal(t, '[', runes[c.Offset])
				require.False(t, inCitationCode(c.markerByte, literal))
				require.GreaterOrEqual(t, c.Excerpt, 1)
				require.LessOrEqual(t, c.Excerpt, excerpts)
			}
			if streaming {
				continue
			}
			for _, match := range citationMarker.FindAllStringIndex(gotAnswer, -1) {
				if inCitationCode(match[0], literal) {
					continue
				}
				for _, number := range citationNumbers(gotAnswer[match[0]:match[1]]) {
					require.GreaterOrEqual(t, number, 1, strconv.Quote(gotAnswer))
					require.LessOrEqual(t, number, excerpts)
				}
			}
		}
	})
}
