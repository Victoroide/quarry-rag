package pipeline

import (
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/stretchr/testify/require"
)

func TestCitationCodeRanges(t *testing.T) {
	for _, tc := range []struct {
		name, text string
		want       []string
	}{
		{"empty", "", nil},
		{"inline span", "a `b[0]` c", []string{"`b[0]`"}},
		{"span holding a shorter run", "x ``a`b`` y", []string{"``a`b``"}},
		{"closer must match the opener width", "`a`` b``` c`", []string{"`a`` b``` c`"}},
		{"two spans", "`x[0]` and `y[2]`", []string{"`x[0]`", "`y[2]`"}},
		{"backslash is literal inside a span", "`foo\\`bar`", []string{"`foo\\`"}},
		{"escaped opener", "\\`not code`", nil},
		{"escaped backslash before an opener", "\\\\`code`", []string{"`code`"}},
		{"escape leaves a shorter opener", "\\``a`", []string{"`a`"}},
		{"backslash before a letter does not escape", "\\a `b`", []string{"`b`"}},
		{"unmatched backtick", "a ` b", nil},
		{"span does not cross a line", "a `b\nc` d", nil},
		{"stray backtick does not hide the next line", "Press ` key.\nThen `ls`.", []string{"`ls`"}},
		{"backtick fence with an info string and a blank line", "```go\nx := a[0]\n\ny := b[1]\n```\nDone [1].", []string{"```go\nx := a[0]\n\ny := b[1]\n```"}},
		{"tilde fence", "~~~\nx[0]\n~~~\n[1]", []string{"~~~\nx[0]\n~~~"}},
		{"closing fence longer than the opener", "~~~\na[0]\n~~~~\n[1]", []string{"~~~\na[0]\n~~~~"}},
		{"closing fence shorter than the opener", "````\na\n```\nb\n````\n[1]", []string{"````\na\n```\nb\n````"}},
		{"closing fence followed by text", "```\ncode\n``` [1]", []string{"```\ncode\n``` [1]"}},
		{"closing fence with other character", "~~~\na\n```\nb", []string{"~~~\na\n```\nb"}},
		{"closing fence with trailing blanks", "```\na\n```  \t\nb", []string{"```\na\n```"}},
		{"unclosed fence runs to the end", "text\n```\ncode [1]", []string{"```\ncode [1]"}},
		{"lone fence", "```", []string{"```"}},
		{"three-space indent is a fence", "   ```\nx\n   ```\n[1]", []string{"```\nx\n   ```"}},
		{"four-space indent is not a fence", "    ```\nx\n    ```", nil},
		{"tab indent is not a fence", "\t```\nx[0]\n\t```", nil},
		{"fence inside a block quote is not recognized", "> ```\n> x[0]\n> ```", nil},
		{"backtick in the info string is not a fence", "```a`b\nx[0]", nil},
		{"single-line triple backtick span", "```x`y``` [1]", []string{"```x`y```"}},
		{"tilde info string may hold a backtick", "~~~a`b\nx\n~~~", []string{"~~~a`b\nx\n~~~"}},
		{"no inline scan inside a fence", "```\na `b\n```\n`c`", []string{"```\na `b\n```", "`c`"}},
		{"CRLF line endings", "```\r\nx[0]\r\n```\r\nDone [1].", []string{"```\r\nx[0]\r\n```"}},
		{"CRLF closes an inline line", "`a`\r\n`b`", []string{"`a`", "`b`"}},
		{"multibyte text around a span", "档案 `x[0]` 记录", []string{"`x[0]`"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var got []string
			for _, r := range citationCodeRanges(tc.text) {
				got = append(got, tc.text[r[0]:r[1]])
			}
			require.Equal(t, tc.want, got)
		})
	}
	require.Equal(t, [][2]int{{2, 8}}, citationCodeRanges("a `b[0]` c"))
}

func TestInCitationCode(t *testing.T) {
	ranges := [][2]int{{2, 5}, {8, 9}}
	for pos, want := range []bool{false, false, true, true, true, false, false, false, true, false} {
		require.Equal(t, want, inCitationCode(pos, ranges), pos)
	}
	require.False(t, inCitationCode(0, nil))
}

func BenchmarkCitationCodeRanges(b *testing.B) {
	var line strings.Builder
	for n := 1; line.Len() < 64<<10; n++ {
		line.WriteString(strings.Repeat("`", n))
		line.WriteString(" a ")
	}
	text := line.String()
	b.SetBytes(int64(len(text)))
	for b.Loop() {
		citationCodeRanges(text)
	}
}

// preservingFinalize runs the production finalization for a response mode.
func preservingFinalize(t *testing.T, answer string, excerpts int, streaming, preserve bool) (string, []Citation) {
	t.Helper()
	bodies := make([]string, excerpts)
	for i := range bodies {
		bodies[i] = "first station open"
	}
	qc := &QueryContext{Excerpts: citationEvidence(bodies...), FinalAnswer: answer, IncludeCitations: true, PreserveCode: preserve}
	if streaming {
		qc.Stream = &QueryStream{}
	}
	require.NoError(t, NewSynthesisStage(nil, &config.Config{}).finalizeCitations(t.Context(), qc))
	return qc.FinalAnswer, qc.Citations
}

func citationOffsets(citations []Citation) []int {
	var offsets []int
	for _, c := range citations {
		offsets = append(offsets, c.Offset)
	}
	return offsets
}

func TestVerifyCitationsPreservingCode(t *testing.T) {
	// An empty preserved or legacy answer means the answer is returned unchanged.
	for _, tc := range []struct {
		name, answer, preserved, legacy string
		offsets, legacyOffsets          []int
	}{
		{"zero index", "The first station is `stations[0]` [1].", "", "The first station is `stations` [1].", []int{35}, []int{32}},
		{"leading zero", "Use `a[01]` here [1].", "", "Use `a[1]` here [1].", []int{17}, []int{6, 16}},
		{"valid excerpt number in code", "Use `items[1]` for the second item [1].", "", "", []int{35}, []int{10, 35}},
		{"two spans", "Read `x[0]` and `y[2]`; see [1].", "", "Read `x` and `y`; see [1].", []int{28}, []int{22}},
		{"fenced block with a blank line", "```go\nx := a[0]\n\ny := b[1]\n```\nDone [1].", "", "```go\nx := a\n\ny := b[1]\n```\nDone [1].", []int{36}, []int{20, 33}},
		{"stray backtick", "Press the ` key [0].\n\nThen run `ls` [1].", "Press the ` key.\n\nThen run `ls` [1].", "Press the ` key.\n\nThen run `ls` [1].", []int{32}, []int{32}},
		{"invalid marker next to code", "`a[9]` b [9] [1]", "`a[9]` b [1]", "`a` b [1]", []int{9}, []int{6}},
		{"bracket spanning code", "Archive [see `x` 1] [1]", "", "", []int{20}, []int{20}},
		{"dense script around a span", "档案 `x[0]` 记录 [1]", "", "档案 `x` 记录 [1]", []int{13}, []int{10}},
		{"fence closed by a longer run", "~~~\na[0]\n~~~~\n[1]", "", "~~~\na\n~~~~\n[1]", []int{14}, []int{11}},
		{"unclosed fence hides the marker", "```\ncode\n``` [1]", "", "", nil, []int{13}},
		{"no code at all", "Archive [99] open [1, 1, 99] now.", "Archive open [1] now.", "Archive open [1] now.", []int{13}, []int{13}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			orDefault := func(answer string) string {
				if answer == "" {
					return tc.answer
				}
				return answer
			}
			answer, citations := preservingFinalize(t, tc.answer, 1, false, false)
			require.Equal(t, orDefault(tc.legacy), answer)
			require.Equal(t, tc.legacyOffsets, citationOffsets(citations))

			answer, citations = preservingFinalize(t, tc.answer, 1, false, true)
			require.Equal(t, orDefault(tc.preserved), answer)
			require.Equal(t, tc.offsets, citationOffsets(citations))
			runes := []rune(answer)
			for _, c := range citations {
				require.Equal(t, '[', runes[c.Offset])
			}
		})
	}
}

func TestVerifyCitationsPreservingCodeWithoutOptionIsLegacy(t *testing.T) {
	for _, answer := range []string{"The first station is `stations[0]` [1].", "```\na[0]\n```\n[1] [2] [99]", "plain [1][2]"} {
		wantAnswer, want := VerifyCitations(answer, citationEvidence("first station open", "first station open"))
		gotAnswer, got := preservingFinalize(t, answer, 2, false, false)
		require.Equal(t, wantAnswer, gotAnswer)
		require.Len(t, got, len(want))
		for i := range want {
			require.Equal(t, want[i], got[i])
		}
	}
}

func TestVerifyCitationsPreservingCodeKeepsJoinedBytes(t *testing.T) {
	// Removing the invalid marker between two spans joins their backtick runs.
	// The code bytes stay intact; the joined text need not clean to itself.
	answer := "`a`[0]`b`"
	cleaned, literal := cleanCitationMarkersInto(answer, 1, nil, true)
	require.Equal(t, "`a``b`", cleaned)
	require.Equal(t, [][2]int{{0, 3}, {3, 6}}, literal)
	require.Equal(t, "`a`", cleaned[literal[0][0]:literal[0][1]])
	require.Equal(t, "`b`", cleaned[literal[1][0]:literal[1][1]])
}

func TestCleanCitationMarkersLiteralRangesAreCleanedCoordinates(t *testing.T) {
	answer := "Intro [9] and `a[0]`, then [8]\n```\nx[0]\n```\n[1]"
	cleaned, literal := cleanCitationMarkersInto(answer, 1, nil, true)
	require.Equal(t, "Intro and `a[0]`, then\n```\nx[0]\n```\n[1]", cleaned)
	var spans []string
	for _, r := range literal {
		spans = append(spans, cleaned[r[0]:r[1]])
	}
	require.Equal(t, []string{"`a[0]`", "```\nx[0]\n```"}, spans)
}

func TestStreamCitationsPreservingCode(t *testing.T) {
	answer := "Use `items[1]` and `a[01]`; 档案 [1].\n```\nb[1]\n```\nDone [1]."
	got, citations := preservingFinalize(t, answer, 1, true, true)
	require.Equal(t, answer, got)
	require.Len(t, citations, 2)
	for _, c := range citations {
		require.Equal(t, "[", string([]rune(answer)[c.Offset:c.Offset+1]))
	}
	require.Equal(t, utf8.RuneCountInString("Use `items[1]` and `a[01]`; 档案 "), citations[0].Offset)
	require.Equal(t, utf8.RuneCountInString(answer)-len("[1]."), citations[1].Offset)

	_, legacy := preservingFinalize(t, answer, 1, true, false)
	require.Len(t, legacy, 5)
}
