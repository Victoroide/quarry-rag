package pipeline

import (
	"strconv"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/stretchr/testify/require"
)

var citationFenceRegressions = []struct {
	name, answer string
	blocks       []string
	offset       int
}{
	{"dedent to sibling", "1. Install:\n    ```bash\n    npm i x\n2. Run:\n```\nrun\n```\nThen restart [1].", []string{"```\nrun\n```"}, 69},
	{"dedent after blank", "1. Install:\n    ```bash\n    npm i x\n\n```\nrun\n```\nThen restart [1].", []string{"```\nrun\n```"}, 62},
	{"dedent from indented block", "Example:\n\n    ```\n    raw\n\n```\nx\n```\nThen [1].", []string{"```\nx\n```"}, 42},
	{"over-indented top-level closer", "  ```\n  a\n     ```\n  b\n  ```\nThen [1].", []string{"```\n  a\n     ```\n  b\n  ```"}, 34},
	{"thematic break before top-level fence", "- - -\n  ```\nliteral\n```\nThen [1].", []string{"```\nliteral\n```"}, 29},
	{"ordered paragraph continuation before top-level fence", "Paragraph text\n2. continuation\n   ```\nliteral\n```\nThen [1].", []string{"```\nliteral\n```"}, 55},
}

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
		{"four-space indent is not a fence", "    ```\n    x\n    ```", nil},
		{"four-space opener closed at three spaces", "1. Install:\n    ```bash\n    npm i x[0]\n   ``` \nThen restart [1].", nil},
		{"two-space opener closed at four spaces", "- Install:\n  ```bash\n  npm i x[0]\n    ``` \nThen restart [1].", []string{"```bash\n  npm i x[0]\n    ```"}},
		{"three-space opener closed at six spaces", "1. Install:\n   ```bash\n   npm i x[0]\n      ```\nThen restart [1].", []string{"```bash\n   npm i x[0]\n      ```"}},
		{"top-level fence ignores four-space literal fence", "```markdown\n    ```\n```\nThen [1].", []string{"```markdown\n    ```\n```"}},
		{"one-space fence ignores four-space literal fence", " ```markdown\n    ```\n ```\nThen [1].", []string{"```markdown\n    ```\n ```"}},
		{"tilde straddle and length mismatch", "1. First\n    ~~~~\n    a[0]\n   ~~~\n   ~~~~\n`b` [1]\n- Next\n  ~~~\n  x[0]\n     ~~\n     ~~~~\nDone [2].", []string{"`b`", "~~~\n  x[0]\n     ~~\n     ~~~~"}},
		{"top-level tilde closer must reach the left margin", "  ~~~\n  x[0]\n     ~~~~\n   ~~~~\nDone [1].", []string{"~~~\n  x[0]\n     ~~~~\n   ~~~~"}},
		{"indented backtick in info string is not an opener", "    ```a`b\n   ```\nx\n   ```\n`c`", []string{"```\nx\n   ```", "`c`"}},
		{"tab indent is not a fence", "\t```\nx[0]\n\t```", nil},
		{"inline span inside unrecognized list fence", "1. Install\n    ```bash\n    `x[0]`\n   ``` \n`y[0]` [1]", []string{"`y[0]`"}},
		{"unclosed four-space fence", "    ```bash\n`x[0]`\nThen [1].", []string{"`x[0]`"}},
		{"CRLF straddle fences", "1. Install:\r\n    ```bash\r\n    npm i x[0]\r\n   ```\r\n- Next:\r\n  ```bash\r\n  npm i y[0]\r\n    ```\r\nThen [1].", []string{"```bash\r\n  npm i y[0]\r\n    ```"}},
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
	for _, tc := range citationFenceRegressions {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.offset, strings.LastIndex(tc.answer, "[1]"))
			assertCitationCodeRanges(t, tc.answer, tc.blocks)
		})
	}
	require.Equal(t, [][2]int{{2, 8}}, citationCodeRanges("a `b[0]` c"))
}

func assertCitationCodeRanges(t *testing.T, text string, blocks []string) {
	t.Helper()
	var want [][2]int
	end := 0
	for _, block := range blocks {
		i := strings.Index(text[end:], block)
		require.NotEqual(t, -1, i, "%q is missing from the fixture", block)
		start := end + i
		end = start + len(block)
		want = append(want, [2]int{start, end})
	}
	require.Equal(t, want, citationCodeRanges(text))
}

func TestCitationFenceContainerBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name, text string
		blocks     []string
	}{
		{"ordered parenthesis and multiple digits", "12) Setup\n    ```\n    `x`\n    ```\n`y`", []string{"`y`"}},
		{"nine digits are a marker", "123456789. Setup\n           ```\n           `x`\n          ```\n          `y`\n`z`", []string{"`z`"}},
		{"zero-padded one interrupts a paragraph", "Paragraph\n000000001) Item\n           ```\n               ```\n           `hidden`\n           ```\n`visible`", []string{"`visible`"}},
		{"ten digits are prose", "1234567890. Setup\n  ```\n  a\n    ```\n  b\n  ```\n[1]", []string{"```\n  a\n    ```\n  b\n  ```"}},
		{"sibling resets margin", "12) First\n    ```\n- Second\n  ```\n  `x`\n    ```\n`y`", []string{"```\n  `x`\n    ```", "`y`"}},
		{"unrelated list expires on prose", "- First\nParagraph\n  ```\n  a\n    ```\n  b\n  ```\n[1]", []string{"```\n  a\n    ```\n  b\n  ```"}},
		{"marker-like prose", "See - list\n  ```\n  a\n    ```\n  b\n  ```\n[1]", []string{"```\n  a\n    ```\n  b\n  ```"}},
		{"marker without separating whitespace", "1.No\n  ```\n  a\n    ```\n  b\n  ```\n[1]", []string{"```\n  a\n    ```\n  b\n  ```"}},
		{"marker-only line", "-  \t\r\n  ```\n  a\n    ```\n  b\n  ```\n[1]", []string{"```\n  a\n    ```\n  b\n  ```"}},
		{"wide marker padding falls back to one column", "+     Setup\n  ```\n  a\n     ```\n`y`", []string{"```\n  a\n     ```", "`y`"}},
		{"four padding columns contribute to the margin", "+    Setup\n     ```\n     `x`\n    ```\n     `y`\n`z`", []string{"`z`"}},
		{"blank lines keep list and open fence", "* Setup\n \t\r\n  ```\n\r\n \t\r\n  a\n     ```\n`y`", []string{"```\n\r\n \t\r\n  a\n     ```", "`y`"}},
		{"tab padding after marker uses the current column", "  -\tSetup\r\n \t```\r\n\t`x`\r\n\t    ```\r\n\t`y`\r\n\t  ```\t\r\n\t`z`", []string{"`z`"}},
		{"mixed indentation counts columns but uses byte offsets", "档案\n- Setup\n  ```\r\n  x\r\n \t```\t\r\n`y`", []string{"```\r\n  x\r\n \t```", "`y`"}},
		{"nonzero opener indentation keeps list margin", " - Setup\n   ```\n   a\n      ```\n`y`", []string{"```\n   a\n      ```", "`y`"}},
		{"closer bounds follow margin even when opener is deeper", "- Setup\n   ```\n   a\n      ```\n     ```\n`y`", []string{"```\n   a\n      ```\n     ```", "`y`"}},
		{"list closer must match delimiter length and tail", "- Setup\n  ~~~~\n  ~~~\n  ````\n  ~~~~ text\n    ~~~~~\t\n`y`", []string{"~~~~\n  ~~~\n  ````\n  ~~~~ text\n    ~~~~~", "`y`"}},
		{"list suppression uses bounded closer indentation", "- Setup\n    ~~~~\n      ~~~~\n    `x`\n     ~~~~~\n  `y`", []string{"`y`"}},
		{"opener beyond list fence indentation uses indented block margin", "- Setup\n      ```\n    `x`\n  `y`", []string{"`y`"}},
		{"standalone tab block ends before dedented inline span", "\t```\n \t`x`\n\t \r\n   `y`", []string{"`y`"}},
		{"standalone closer remains inside the indented block", "    ~~~\n        ~~~\n    `y`", []string{"`y`"}},
		{"recognized list fence dedents to prose", "- Setup\r\n  ```\r\n  a\r\n `y` [1]", []string{"```\r\n  a\r\n", "`y`"}},
		{"recognized list fence dedents to another fence", "- Setup\n  ```\n  a\n```\nb\n```\n[1]", []string{"```\n  a\n", "```\nb\n```"}},
		{"unclosed list fence at eof without lf", "1. Setup\n   ```\n   a", []string{"```\n   a"}},
		{"unclosed list fence at eof with lf", "1. Setup\n   ```\n   a\n", []string{"```\n   a\n"}},
		{"lone carriage return is not a line separator", "- Setup\n  ```\r[1]", []string{"```\r[1]"}},
		{"block quote is outside the fence subset", "> ```\n> `x`\n> ```\n`y`", []string{"`x`", "`y`"}},
		{"direct marker fence is outside the fence subset", "- ```\n  `x`\n  ```\n  open", []string{"`x`", "```\n  open"}},
		{"nested marker does not establish a second margin", "- Outer\n  - Inner\n  ```\n  a\n     ```\n`y`", []string{"```\n  a\n     ```", "`y`"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assertCitationCodeRanges(t, tc.text, tc.blocks)
		})
	}
}

// The indented closers in these fixtures distinguish a real list fence from
// a top-level one. The same fixtures also seed both citation fuzz targets.
type citationListStartCase struct {
	name, prefix, indent string
	list                 bool
}

var citationListStartCases = []citationListStartCase{
	{"dash thematic break", "- - -\n", "  ", false},
	{"asterisk thematic break", " * * *\n", "   ", false},
	{"compact thematic break", "---\n", "  ", false},
	{"interleaved thematic break with CRLF", "- -\t-- \r\n", "  ", false},
	{"asterisk thematic break with compact run and tab", "* **\t*\n", "  ", false},
	{"mixed thematic characters are a bullet item", "- * -\n", "  ", true},
	{"one dash run short of a thematic break", "- -\n", "  ", true},
	{"dash interrupts a paragraph", "Paragraph\n- Item\n", "  ", true},
	{"plus interrupts a paragraph", "Paragraph\n+ Item\n", "  ", true},
	{"asterisk interrupts a paragraph", "Paragraph\n* Item\n", "  ", true},
	{"bullet without following whitespace", "-Item\n", "  ", false},
	{"non-ASCII whitespace is not marker padding", "-\u00a0Item\n", "  ", false},
	{"ordered marker without following whitespace", "1.Item\n", "   ", false},
	{"empty bullet cannot interrupt", "Paragraph\n* \t\n", "  ", false},
	{"empty ordered item cannot interrupt", "Paragraph\n1)\n", "   ", false},
	{"ordered marker requires an ASCII delimiter", "1: Item\n", "   ", false},
	{"ordered marker requires ASCII digits", "１. Item\n", "   ", false},
	{"ordered marker stops at nine digits", "Paragraph\n0000000001. Item\n", "   ", false},
	{"four-column marker indentation", "    - Item\n", "  ", false},
	{"tab expands to four columns before the marker", " \t* Item\n", "  ", false},
	{"non-one period cannot interrupt", "Paragraph\n2. Item\n", "   ", false},
	{"zero with parenthesis cannot interrupt", "Paragraph\n0) Item\n", "   ", false},
	{"zero-padded non-one cannot interrupt", "Paragraph\n02. Item\n", "   ", false},
	{"one with period interrupts", "Paragraph\n1. Item\n", "   ", true},
	{"one with parenthesis interrupts", "Paragraph\n1) Item\n", "   ", true},
	{"quote before ordered list", "> Quote\n2. Second\n", "   ", true},
	{"quote in column one before marker one", " > Quote\n1) Item\n", "   ", true},
	{"quoted heading in column two", "  > # Heading\n2) Item\n", "   ", true},
	{"quote in column three interrupts a paragraph", "Paragraph\n   > Quote\n2. Item\n", "   ", true},
	{"lazy continuation of quoted prose", "> Quote\nLazy continuation\n2) Item\n", "   ", true},
	{"indented lazy continuation of quoted prose", "> Quote\n    Lazy continuation\n2. Item\n", "   ", true},
	{"equals signs lazily continue quoted prose", "> Quote\n===\n2. Item\n", "   ", true},
	{"prose after a quoted heading", "> # Heading\nParagraph\n2) Item\n", "   ", false},
	{"prose after a quoted blank line", "> Quote\n>\nParagraph\n2. Item\n", "   ", false},
	{"prose after an unclosed quoted fence", "> ~~~\n> code\nParagraph\n2. Item\n", "   ", false},
	{"prose after quoted HTML", "> <div>\n> raw text\nParagraph\n2. Item\n", "   ", false},
	{"escaped quote continues top-level prose", "Paragraph\n\\> text\n2. Item\n", "   ", false},
	{"quote marker at column four continues prose", "Paragraph\n    > text\n2. Item\n", "   ", false},
	{"single-line HTML comment", "<!-- comment -->\n2. Item\n", "   ", true},
	{"multiline HTML comment interrupts prose", "Paragraph\n <!-- comment\n\n2. Raw content\n -->\n2) Item\n", "   ", true},
	{"HTML processing instruction", "  <?instruction\nprocessing ?> trailing text\n2. Item\n", "   ", true},
	{"HTML declaration", "   <!DOCTYPE html>\n2) Item\n", "   ", true},
	{"CDATA section with CRLF", "<![CDATA[\r\ncontent\r\n]]>\r\n2. Item\n", "   ", true},
	{"raw script with a case-insensitive terminator", "<ScRiPt>\ncontent\n</SCRIPT>\n2) Item\n", "   ", true},
	{"raw pre block ends at another raw end tag", "<pre\ncontent\n</style>\n2. Item\n", "   ", true},
	{"single-line raw style block", "<style media='screen'>a{color:red}</style>\n2. Item\n", "   ", true},
	{"raw textarea block", "<textarea>\n</textarea>\n2. Item\n", "   ", true},
	{"blank-terminated known HTML block stays open after end tag", "Paragraph\n<div>\n</div>\n1. Raw content\n", "   ", false},
	{"blank-terminated custom HTML block stays open after end tag", "<custom-tag data-key = 'x > y' />\n1) Raw content\n", "   ", false},
	{"custom tag can start with a raw tag name", "<script-widget>\n1. Raw content\n", "   ", false},
	{"blank-terminated HTML with closing tag as opener", "</custom-tag>\n1. Raw content\n", "   ", false},
	{"blank line ends known HTML block", "<section>\n</section>\n\n2. Item\n", "   ", true},
	{"blank line ends custom HTML block", "<custom-tag>\n\n2) Item\n", "   ", true},
	{"blank line does not end a raw comment", "<!-- start\n\n1. Raw content\n", "   ", false},
	{"custom HTML tag cannot interrupt prose", "Paragraph\n<span data-key=x>\n1) Item\n", "   ", true},
	{"invalid HTML tag is paragraph text", "<span bogus=>\n1. Item\n", "   ", true},
	{"inline tag with trailing text is paragraph text", "<span> prose\n1) Item\n", "   ", true},
	{"inline angle-bracket text continues prose", "Paragraph <div>\n2. Item\n", "   ", false},
	{"escaped angle bracket continues prose", "Paragraph\n\\<!-- literal -->\n2) Item\n", "   ", false},
	{"raw tag prefix alone cannot open a block", "Paragraph\n<scripture>\n2. Item\n", "   ", false},
	{"indented HTML opener continues prose", "Paragraph\n    <!-- literal -->\n2. Item\n", "   ", false},
	{"prose after a terminated raw block", "<!-- comment -->\nParagraph\n2) Item\n", "   ", false},
	{"non-one period starts after blank line", "Paragraph\n\n2. Item\n", "   ", true},
	{"non-one parenthesis starts after a closed fence", "~~~\ncode\n~~~\n2) Item\n", "   ", true},
	{"non-one starts after a thematic break", "Paragraph\n* * *\n2. Item\n", "   ", true},
	{"non-one starts after an ATX heading", "# Heading\n2. Item\n", "   ", true},
	{"non-one starts after a setext heading", "Heading\n=====\n2. Item\n", "   ", true},
	{"hashes without heading whitespace do not end prose", "Paragraph\n#Heading\n2. Item\n", "   ", false},
	{"setext underline requires an open paragraph", "=====\n2. Item\n", "   ", false},
	{"non-one period sibling", "1. First\n2. Item\n", "   ", true},
	{"non-one parenthesis sibling", "1) First\n2) Item\n", "   ", true},
	{"numbered sibling after lazy prose", "1. First\nLazy continuation\n2. Item\n", "   ", true},
	{"different delimiter starts a separate list", "1. First\n2) Item\n", "   ", true},
	{"non-one after bullet starts a separate list", "- First\n2. Item\n", "   ", true},
	{"lazy marker-like prose leaves no fence margin", "- First\n2.continuation\n", "  ", false},
	{"non-one after an indented paragraph continuation", "Paragraph\n    continuation\n2. Item\n", "   ", false},
	{"indented fence-like text cannot interrupt a paragraph", "Paragraph\n    ~~~\n2. Item\n", "   ", false},
}

func (tc citationListStartCase) fenceAnswer(fence, bodyIndent string) (answer, block string) {
	closer := fence
	if tc.list {
		closer = "    " + closer
	}
	block = fence + "\n" + bodyIndent + "literal [01]\n" + closer
	return tc.prefix + tc.indent + block + "\nThen [1].", block
}

func TestCitationListFenceStarts(t *testing.T) {
	for _, tc := range citationListStartCases {
		t.Run(tc.name, func(t *testing.T) {
			for _, fence := range []string{"```", "~~~"} {
				for _, bodyIndent := range []string{"", tc.indent} {
					if tc.list && bodyIndent == "" {
						continue
					}
					answer, block := tc.fenceAnswer(fence, bodyIndent)
					start := len(tc.prefix) + len(tc.indent)
					ranges := citationCodeRanges(answer)
					require.NotEmpty(t, ranges, answer)
					require.Equal(t, [2]int{start, start + len(block)}, ranges[len(ranges)-1], answer)
					for _, streaming := range []bool{false, true} {
						got, citations := preservingFinalize(t, answer, 1, streaming, true)
						require.Equal(t, answer, got)
						offset := utf8.RuneCountInString(answer[:strings.LastIndex(answer, "[1]")])
						require.Equal(t, []int{offset}, citationOffsets(citations))
					}
				}
			}
		})
	}
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

func BenchmarkCitationCodeRangeDedents(b *testing.B) {
	const item = "- Setup\n  ```\n  code\n~~~\n[1]\n~~~\nThen [1].\n"
	for _, kiB := range []int{10, 100} {
		b.Run(strconv.Itoa(kiB)+"KiB", func(b *testing.B) {
			answer := strings.Repeat(item, kiB*1024/len(item)+1)[:kiB*1024]
			b.SetBytes(int64(len(answer)))
			b.ReportAllocs()
			for b.Loop() {
				citationCodeRanges(answer)
			}
		})
	}
}

func TestVerifyCitationsAfterFenceDedent(t *testing.T) {
	for _, tc := range citationFenceRegressions {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.offset, strings.LastIndex(tc.answer, "[1]"))
			for _, streaming := range []bool{false, true} {
				for _, preserve := range []bool{false, true} {
					answer, citations := preservingFinalize(t, tc.answer, 1, streaming, preserve)
					require.Equal(t, tc.answer, answer)
					require.Equal(t, []int{tc.offset}, citationOffsets(citations))
				}
			}
		})
	}
	t.Run("marker inside recovered top-level fence is literal", func(t *testing.T) {
		answer := strings.Replace(citationFenceRegressions[0].answer, "\nrun\n", "\nrun [01]\n", 1)
		for _, streaming := range []bool{false, true} {
			got, citations := preservingFinalize(t, answer, 1, streaming, true)
			require.Equal(t, answer, got)
			require.Equal(t, []int{strings.LastIndex(answer, "[1]")}, citationOffsets(citations))
		}
	})
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
		{"nested fence straddle four-to-three", "1. Install:\n    ```bash\n    npm i x[0]\n   ```\nThen restart [1].", "1. Install:\n    ```bash\n    npm i x\n   ```\nThen restart [1].", "1. Install:\n    ```bash\n    npm i x\n   ```\nThen restart [1].", []int{56}, []int{56}},
		{"nested fence straddle four-to-three without numbers in code", "1. Install:\n    ```bash\n    npm i x\n   ```\nThen restart [1].", "", "", []int{56}, []int{56}},
		{"nested fence straddle cleans outside markers", "1. Install:\n    ```bash\n    npm i x[0]\n   ```\nThen [99] restart [1].", "1. Install:\n    ```bash\n    npm i x\n   ```\nThen restart [1].", "1. Install:\n    ```bash\n    npm i x\n   ```\nThen restart [1].", []int{56}, []int{56}},
		{"nested fence straddle two-to-four", "- Install:\n  ```bash\n  npm i x[0]\n    ```\nThen restart [1].", "", "- Install:\n  ```bash\n  npm i x\n    ```\nThen restart [1].", []int{55}, []int{52}},
		{"nested fence straddle two-to-four without numbers in code", "- Install:\n  ```bash\n  npm i x\n    ```\nThen restart [1].", "", "", []int{52}, []int{52}},
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

	for _, tc := range []struct {
		name, answer  string
		offsets       []int
		legacyOffsets []int
	}{
		{
			name:          "four-to-three straddle",
			answer:        "1. Install:\n    ```bash\n    npm i x[0]\n   ```\nThen restart [1].",
			offsets:       []int{59},
			legacyOffsets: []int{59},
		},
		{
			name:          "two-to-four straddle",
			answer:        "- Install:\n  ```bash\n  npm i x[1]\n    ```\nThen restart [1].",
			offsets:       []int{55},
			legacyOffsets: []int{30, 55},
		},
		{
			name:          "two-to-four straddle without numbers in code",
			answer:        "- Install:\n  ```bash\n  npm i x\n    ```\nThen restart [1].",
			offsets:       []int{52},
			legacyOffsets: []int{52},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, citations := preservingFinalize(t, tc.answer, 1, true, true)
			require.Equal(t, tc.answer, got)
			require.Equal(t, tc.offsets, citationOffsets(citations))

			gotLegacy, legacyCitations := preservingFinalize(t, tc.answer, 1, true, false)
			require.Equal(t, tc.answer, gotLegacy)
			require.Equal(t, tc.legacyOffsets, citationOffsets(legacyCitations))
		})
	}
}
