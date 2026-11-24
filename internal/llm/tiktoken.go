package llm

import (
	"container/heap"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"unicode"
	"unicode/utf8"

	"github.com/dlclark/regexp2/v2"
	"github.com/tiktoken-go/tokenizer"
	"golang.org/x/text/unicode/rangetable"
)

type chatTokenizer struct {
	split *regexp2.Regexp
	ranks map[string]int
}

// The template uses ordinary text tokens. Control tokens are supplied only by
// the chat frame, so text such as "<|endoftext|>" is encoded as ordinary text.
var chatTokenizers = map[string]func() (*chatTokenizer, error){
	"cl100k_base": sync.OnceValues(func() (*chatTokenizer, error) {
		return loadChatTokenizer(tokenizer.Cl100kBase, 100256, "b7a748b22844495035738492d9a5da45a52231128591a3a0aa5e36dd9c8f23a6")
	}),
	"o200k_base": sync.OnceValues(func() (*chatTokenizer, error) {
		return loadChatTokenizer(tokenizer.O200kBase, 199998, "577114f5d6e052bc5ea53b4d516abb04b14de93b54b1a1120a82d61eed3270a0")
	}),
}

func cachedTiktoken(encoding string) (*chatTokenizer, error) {
	load, ok := chatTokenizers[encoding]
	if !ok {
		return nil, fmt.Errorf("%w: unsupported encoding %q", ErrUnsupportedProfile, encoding)
	}
	return load()
}

func loadChatTokenizer(encoding tokenizer.Encoding, size int, digest string) (*chatTokenizer, error) {
	// Verify the embedded merge ranks in order against tiktoken 0.14.0.
	codec, err := tokenizer.Get(encoding)
	if err != nil {
		return nil, err
	}
	ranks := make(map[string]int, size)
	hash := sha256.New()
	for rank := range size {
		piece, decodeErr := codec.Decode([]uint{uint(rank)})
		if decodeErr != nil || len(piece) > 128 {
			return nil, fmt.Errorf("%w: invalid %s merge rank %d", ErrUnsupportedProfile, encoding, rank)
		}
		ranks[piece] = rank
		sized := append(strconv.AppendInt(nil, int64(len(piece)), 10), ':')
		_, _ = hash.Write(sized)
		_, _ = hash.Write([]byte(piece))
	}
	if hex.EncodeToString(hash.Sum(nil)) != digest {
		return nil, fmt.Errorf("%w: %s merge ranks differ from the reference", ErrUnsupportedProfile, encoding)
	}
	split, err := compiledChatPatterns()
	if err != nil {
		return nil, err
	}
	return &chatTokenizer{split: split[encoding], ranks: ranks}, nil
}

var compiledChatPatterns = sync.OnceValues(func() (map[tokenizer.Encoding]*regexp2.Regexp, error) {
	// tiktoken 0.14.0 uses Unicode 16. Go's Unicode 17 data adds new
	// assignments and reclassifies U+0295 from Ll to Lo.
	assigned := rangetable.Assigned("16.0.0")
	if assigned == nil || unicode.Version != "17.0.0" {
		return nil, fmt.Errorf("%w: unverified Unicode tables", ErrUnsupportedProfile)
	}
	letterOrNumber := chatClass(func(r rune) bool {
		return unicode.Is(assigned, r) && unicode.In(r, unicode.L, unicode.N)
	})
	letters := chatClass(func(r rune) bool { return unicode.Is(assigned, r) && unicode.IsLetter(r) })
	numbers := chatClass(func(r rune) bool { return unicode.Is(assigned, r) && unicode.IsNumber(r) })
	upper := chatClass(func(r rune) bool {
		return r != '\u0295' && unicode.Is(assigned, r) && unicode.In(r, unicode.Lu, unicode.Lt, unicode.Lm, unicode.Lo, unicode.M)
	})
	lower := chatClass(func(r rune) bool {
		return unicode.Is(assigned, r) && unicode.In(r, unicode.Ll, unicode.Lm, unicode.Lo, unicode.M)
	})
	space := chatClass(unicode.IsSpace)
	// The tiktoken patterns use Unicode simple folding. Only s has a
	// non-ASCII fold among the letters used in the contraction alternatives.
	contraction := `'(?:[sSſdDmMtT]|[lL][lL]|[vV][eE]|[rR][eE])`
	substitute := strings.NewReplacer(
		"{letters}", letters, "{numbers}", numbers, "{alnum}", letterOrNumber,
		"{upper}", upper, "{lower}", lower, "{space}", space, "{contraction}", contraction,
	)
	patterns := map[tokenizer.Encoding]string{
		tokenizer.Cl100kBase: `{contraction}|(?>[^\r\n{alnum}]?)(?>[{letters}]+)|(?>[{numbers}]{1,3})| ?(?>[^{space}{alnum}]+)(?>[\r\n]*)|(?>[{space}]+)\z|[{space}]*[\r\n]|[{space}]+(?![^{space}])|[{space}]`,
		tokenizer.O200kBase:  `[^\r\n{alnum}]?[{upper}]*[{lower}]+(?:{contraction})?|[^\r\n{alnum}]?[{upper}]+[{lower}]*(?:{contraction})?|[{numbers}]{1,3}| ?[^{space}{alnum}]+[\r\n/]*|[{space}]*[\r\n]+|[{space}]+(?![^{space}])|[{space}]+`,
	}
	compiled := make(map[tokenizer.Encoding]*regexp2.Regexp, len(patterns))
	for encoding, pattern := range patterns {
		re, err := regexp2.Compile(substitute.Replace(pattern))
		if err != nil {
			return nil, fmt.Errorf("%w: %s pre-tokenization: %w", ErrUnsupportedProfile, encoding, err)
		}
		compiled[encoding] = re
	}
	return compiled, nil
})

func chatClass(contains func(rune) bool) string {
	var out strings.Builder
	for first := rune(0); first <= unicode.MaxRune; first++ {
		if !contains(first) {
			continue
		}
		last := first
		for last < unicode.MaxRune && contains(last+1) {
			last++
		}
		out.WriteRune(first)
		if last != first {
			out.WriteByte('-')
			out.WriteRune(last)
		}
		first = last
	}
	return out.String()
}

func (c *chatTokenizer) count(ctx context.Context, input string, memo map[string]int) (int, error) {
	// Match encoding/json's replacement of each invalid input byte.
	if !utf8.ValidString(input) {
		input = string([]rune(input))
	}
	total, consumed := 0, 0
	match, err := c.split.FindStringMatch(input)
	for match != nil && err == nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return 0, ctxErr
		}
		piece := match.String()
		n, ok := memo[piece]
		if !ok {
			n = c.countPiece(piece)
			if len(memo) < 1024 {
				memo[piece] = n
			}
		}
		total += n
		consumed += len(piece)
		match, err = c.split.FindNextMatch(match)
	}
	if err != nil {
		return 0, err
	}
	if consumed != len(input) {
		return 0, fmt.Errorf("tiktoken: pre-tokenization did not cover the full input")
	}
	return total, ctx.Err()
}

type bytePair struct {
	left, right, end, rank int
}

type rankedPairs []bytePair

func (p rankedPairs) Len() int      { return len(p) }
func (p rankedPairs) Swap(i, j int) { p[i], p[j] = p[j], p[i] }
func (p rankedPairs) Less(i, j int) bool {
	return p[i].rank < p[j].rank || p[i].rank == p[j].rank && p[i].left < p[j].left
}
func (p *rankedPairs) Push(pair any) { *p = append(*p, pair.(bytePair)) }
func (p *rankedPairs) Pop() any {
	last := len(*p) - 1
	pair := (*p)[last]
	*p = (*p)[:last]
	return pair
}

func (c *chatTokenizer) countPiece(piece string) int {
	if _, ok := c.ranks[piece]; ok {
		return 1
	}
	// Offsets form a linked list of the remaining token boundaries.
	// A queued merge is valid only while both of its boundaries are adjacent.
	next := make([]int, len(piece)+1)
	prev := make([]int, len(piece)+1)
	pairs := make(rankedPairs, 0, len(piece))
	for i := range next {
		next[i], prev[i] = i+1, i-1
	}
	for i := 0; i+2 <= len(piece); i++ {
		if rank, ok := c.ranks[piece[i:i+2]]; ok {
			pairs = append(pairs, bytePair{i, i + 1, i + 2, rank})
		}
	}
	heap.Init(&pairs)
	queue := func(left, right, end int) {
		if left >= 0 && end <= len(piece) {
			if rank, ok := c.ranks[piece[left:end]]; ok {
				heap.Push(&pairs, bytePair{left, right, end, rank})
			}
		}
	}
	total := len(piece)
	for pairs.Len() != 0 {
		pair := heap.Pop(&pairs).(bytePair)
		if next[pair.left] != pair.right || next[pair.right] != pair.end {
			continue
		}
		next[pair.left], prev[pair.end] = pair.end, pair.left
		next[pair.right] = -1
		total--
		queue(prev[pair.left], pair.left, pair.end)
		queue(pair.left, pair.end, next[pair.end])
	}
	return total
}
