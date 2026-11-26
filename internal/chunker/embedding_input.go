package chunker

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/embeddinginput"
	"github.com/Victoroide/quarry-rag/internal/textutil"
	"github.com/clipperhouse/uax29/v2/graphemes"
)

const maxProfileChunks = 10_000

var (
	errEmbeddingChunkCount    = domain.Permanent(fmt.Errorf("embedding profile would produce more than %d chunks", maxProfileChunks))
	errEmbeddingChunkBoundary = domain.Permanent(errors.New("a text grapheme exceeds the chunk byte or token budget"))
	errEmbeddingPartition     = domain.Permanent(fmt.Errorf("cannot partition text on grapheme boundaries: %w", embeddinginput.ErrInputLimit))
)

// Each boundary stores prefix counts and properties of the cluster that follows it.
type profileBoundary struct {
	offset, chars, dense, words, other int
	insideWord, blank, sentence        bool
}

type profileText struct {
	text   string
	points []profileBoundary
}

type profileSpan struct {
	start, end int
	limited    bool
}

func indexProfileText(ctx context.Context, text string) (profileText, error) {
	index := profileText{text: text, points: make([]profileBoundary, 0, min(len(text)+1, MaxChunkBytes))}
	iter := graphemes.FromString(text)
	var prefix tokenEstimate
	chars := 0
	for iter.Next() {
		if len(index.points)%256 == 0 {
			if err := ctx.Err(); err != nil {
				return profileText{}, err
			}
		}
		cluster := iter.Value()
		r, _ := utf8.DecodeRuneInString(cluster)
		var first tokenEstimate
		first.addRune(r)
		index.points = append(index.points, profileBoundary{
			offset: iter.Start(), chars: chars, dense: prefix.dense,
			words: prefix.words, other: prefix.other,
			insideWord: prefix.endsWord && first.endsWord, blank: strings.TrimSpace(cluster) == "",
		})
		for _, r := range cluster {
			prefix.addRune(r)
			chars++
		}
	}
	index.points = append(index.points, profileBoundary{
		offset: len(text), chars: chars, dense: prefix.dense,
		words: prefix.words, other: prefix.other,
	})
	if err := index.markSentences(ctx); err != nil {
		return profileText{}, err
	}
	return index, ctx.Err()
}

func (s profileText) markSentences(ctx context.Context) error {
	nextParagraph := 0
	for _, paragraph := range textutil.SplitParagraphs(s.text) {
		paragraphStart := nextParagraph + strings.Index(s.text[nextParagraph:], paragraph)
		nextSentence := 0
		for _, sentence := range textutil.SplitSentences(paragraph) {
			if err := ctx.Err(); err != nil {
				return err
			}
			nextSentence += strings.Index(paragraph[nextSentence:], sentence) + len(sentence)
			// Punctuation may be followed by a combining mark. Move the
			// preference to the enclosing cluster's end.
			end := paragraphStart + nextSentence
			i := sort.Search(len(s.points), func(i int) bool { return s.points[i].offset >= end })
			s.points[i].sentence = true
		}
		nextParagraph = paragraphStart + len(paragraph)
	}
	return ctx.Err()
}

func (s profileText) slice(start, end int) string {
	return s.text[s.points[start].offset:s.points[end].offset]
}

func (s profileText) estimate(start, end int) int {
	a, b := s.points[start], s.points[end]
	words := b.words - a.words
	if a.insideWord {
		words++
	}
	return (tokenEstimate{dense: b.dense - a.dense, words: words, other: b.other - a.other}).count()
}

func (s profileText) skipBlanks(start int) int {
	for s.points[start].blank {
		start++
	}
	return start
}

func (s profileText) trimEnd(start, end int) int {
	for end > start && s.points[end-1].blank {
		end--
	}
	return end
}

func (c *Chunker) withinChunk(s profileText, start, end, bytes, chars int) bool {
	a, b := s.points[start], s.points[end]
	return b.offset-a.offset <= bytes && (chars == 0 || b.chars-a.chars <= chars) &&
		c.chunkSize >= s.estimate(start, end)
}

func (c *Chunker) ceiling(s profileText, start, bytes, chars int) int {
	n := sort.Search(len(s.points)-start-1, func(i int) bool {
		return !c.withinChunk(s, start, start+i+1, bytes, chars)
	})
	return start + n
}

func (s profileText) preferredCut(start, end int, sentence bool) int {
	if end == len(s.points)-1 {
		return end
	}
	if sentence {
		for i := end; i > start; i-- {
			if s.points[i].sentence {
				return i
			}
		}
	}
	for i := end; i > start+(end-start)/2; i-- {
		if s.points[i].blank {
			return s.trimEnd(start, i)
		}
	}
	return end
}

func (c *Chunker) chunkProfiled(ctx context.Context, docID domain.DocumentID, text string, profile *embeddinginput.Profile, baseVersion string) ([]domain.Chunk, []chunkSpan, error) {
	source, err := indexProfileText(ctx, text)
	if err != nil {
		return nil, nil, err
	}
	bytes, chars := profile.SplitLimits()
	if bytes == 0 {
		bytes = MaxChunkBytes
	}
	bytes = min(bytes, MaxChunkBytes)
	spans, err := c.greedyProfileSpans(ctx, source, profile, bytes, chars)
	if errors.Is(err, embeddinginput.ErrInputLimit) || errors.Is(err, errEmbeddingChunkCount) || errors.Is(err, errEmbeddingChunkBoundary) {
		spans, err = c.profilePartition(ctx, source, profile, bytes, chars)
	}
	if err != nil {
		return nil, nil, err
	}
	version := fmt.Sprintf("%squarry.chunk.profile.v2.uax29-unicode17\x00%s\x00%d\x00%d\x00", baseVersion, profile.Fingerprint(), c.chunkSize, c.overlap)
	var chunks []domain.Chunk
	var byteSpans []chunkSpan
	for i, span := range spans {
		start := span.start
		if c.overlap > 0 && i > 0 && !span.limited && !spans[i-1].limited {
			start, err = c.profileOverlap(ctx, source, profile, spans[i-1], span, bytes, chars)
			if err != nil {
				return nil, nil, err
			}
		}
		chunks = append(chunks, makeChunk(docID, i, source.slice(start, span.end), version))
		byteSpans = append(byteSpans, chunkSpan{start: source.points[start].offset, end: source.points[span.end].offset})
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	return chunks, byteSpans, nil
}

func (c *Chunker) greedyProfileSpans(ctx context.Context, s profileText, profile *embeddinginput.Profile, bytes, chars int) ([]profileSpan, error) {
	var spans []profileSpan
	for start := s.skipBlanks(0); start < len(s.points)-1; {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if len(spans) == maxProfileChunks {
			return nil, errEmbeddingChunkCount
		}
		estimateEnd := s.trimEnd(start, c.ceiling(s, start, MaxChunkBytes, 0))
		if estimateEnd == start {
			return nil, errEmbeddingChunkBoundary
		}
		ideal := s.preferredCut(start, estimateEnd, true)
		bound := s.trimEnd(start, min(ideal, c.ceiling(s, start, bytes, chars)))
		end, err := fittingProfileEnd(ctx, s, profile, start, bound)
		if err != nil {
			return nil, err
		}
		if end != ideal {
			near := s.preferredCut(start, end, false)
			if near < end {
				err := profile.Allows(ctx, s.slice(start, near))
				if err == nil {
					end = near
				} else if !errors.Is(err, embeddinginput.ErrInputLimit) {
					return nil, err
				}
			}
		}
		spans = append(spans, profileSpan{start: start, end: end, limited: end != ideal})
		start = s.skipBlanks(end)
	}
	return spans, nil
}

// The greedy search only returns a checked prefix. A failed continuation
// invokes the exact partition search because token counts can decrease.
func fittingProfileEnd(ctx context.Context, s profileText, profile *embeddinginput.Profile, start, bound int) (int, error) {
	bad := bound + 1
	good := start
	for bad-good > 1 {
		end := good + (bad-good)/2
		if bad == bound+1 {
			end = bound
		}
		err := profile.Allows(ctx, s.slice(start, s.trimEnd(start, end)))
		switch {
		case err == nil:
			good = end
		case errors.Is(err, embeddinginput.ErrInputLimit):
			bad = end
		default:
			return 0, err
		}
	}
	if good == start {
		return 0, embeddinginput.ErrInputLimit
	}
	return s.trimEnd(start, good), nil
}

// Search backwards over the original grapheme boundaries. Every candidate
// is bounded by the byte, character and estimated-token ceilings. A suffix
// with no continuation is never counted, and each suffix is solved once.
func (c *Chunker) profilePartition(ctx context.Context, s profileText, profile *embeddinginput.Profile, bytes, chars int) ([]profileSpan, error) {
	last := len(s.points) - 1
	cost, next := make([]int, len(s.points)), make([]int, len(s.points))
	reachable := []int{last}
	nonblank := last
	for start := last - 1; start >= 0; start-- {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		cost[start] = math.MaxInt
		if s.points[start].blank {
			cost[start], next[start] = cost[start+1], start+1
		} else {
			nonblank = start
		}
		if cost[start] > 1 {
			ceiling := c.ceiling(s, start, bytes, chars)
			at := sort.Search(len(reachable), func(i int) bool { return reachable[i] <= ceiling })
			if err := s.improveProfilePartition(ctx, profile, start, nonblank, reachable[at:], cost, next); err != nil {
				return nil, err
			}
		}
		if cost[start] != math.MaxInt {
			reachable = append(reachable, start)
		}
	}
	switch {
	case cost[0] == math.MaxInt:
		return nil, errEmbeddingPartition
	case cost[0] > maxProfileChunks:
		return nil, errEmbeddingChunkCount
	}
	spans := make([]profileSpan, 0, cost[0])
	for start := 0; start < last; start = next[start] {
		end := next[start]
		if cost[start] != cost[end] {
			spans = append(spans, profileSpan{start: start, end: end, limited: true})
		}
	}
	return spans, nil
}

func (s profileText) improveProfilePartition(ctx context.Context, profile *embeddinginput.Profile, start, nonblank int, ends, cost, next []int) error {
	for i, end := range ends {
		if end <= nonblank || cost[start] == 1 {
			break
		}
		if i%256 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		if cost[end] >= cost[start]-1 {
			continue
		}
		err := profile.Allows(ctx, s.slice(start, end))
		if errors.Is(err, embeddinginput.ErrInputLimit) {
			continue
		}
		if err != nil {
			return err
		}
		cost[start], next[start] = cost[end]+1, end
	}
	return nil
}

func (c *Chunker) profileOverlap(ctx context.Context, s profileText, profile *embeddinginput.Profile, prev, current profileSpan, bytes, chars int) (int, error) {
	maxBytes := (s.points[prev.end].offset - s.points[prev.start].offset) / 2
	maxTokens := min(c.overlap, s.estimate(prev.start, prev.end)/2)
	wordStart, sentenceStart := current.start, current.start
	for start := prev.end - 1; start > prev.start; start-- {
		if s.points[prev.end].offset-s.points[start].offset > maxBytes || s.estimate(start, prev.end) > maxTokens ||
			!c.withinChunk(s, start, current.end, bytes, chars) {
			break
		}
		if s.points[start].blank || (!s.points[start-1].blank && !s.points[start].sentence) {
			continue
		}
		err := profile.Allows(ctx, s.slice(start, current.end))
		if errors.Is(err, embeddinginput.ErrInputLimit) {
			continue
		}
		if err != nil {
			return 0, err
		}
		wordStart = start
		if s.points[start].sentence || s.points[start-1].sentence {
			sentenceStart = start
		}
	}
	if sentenceStart != current.start {
		return sentenceStart, nil
	}
	return wordStart, nil
}
