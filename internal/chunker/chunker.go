// Package chunker splits raw documents into token-bounded chunks
// with semantic-boundary awareness. The chunker first normalizes the
// raw extract (NFC, width folding, hyphenation, decoration noise), then walks
// paragraphs and sentences building chunks that try to honor structure
// instead of slicing mid-word. Each chunk is tagged with a (kind,
// quality) classification that retrieval can use to de-prioritize
// header / contact noise without losing it.
package chunker

import (
	"context"
	"crypto/sha256"
	"fmt"
	"strconv"
	"strings"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/embeddinginput"
	"github.com/Victoroide/quarry-rag/internal/textutil"
)

// Chunker splits raw text into Chunks using semantic-aware boundaries.
type Chunker struct {
	chunkSize int
	overlap   int
}

// NewChunker creates a Chunker with estimated token budgets. Sizes below two
// are treated as two, the minimum estimate for a non-dense character.
// Overlap is clamped between zero and half the effective size, rounded down.
func NewChunker(chunkSize, overlap int) *Chunker {
	chunkSize = max(2, chunkSize)
	return &Chunker{
		chunkSize: chunkSize,
		overlap:   min(max(0, overlap), chunkSize/2),
	}
}

type textUnit struct {
	text      string
	separator string
	estimate  tokenEstimate
}

// Chunk splits normalized text at paragraph, sentence, word and rune boundaries.
// Every chunk stays within the estimated token and byte limits, and overlapping
// windows always include previously unemitted text.
func (c *Chunker) Chunk(docID domain.DocumentID, text string) []domain.Chunk {
	chunks, _ := c.ChunkForEmbedding(context.Background(), docID, text, nil)
	return chunks
}

// ChunkForEmbedding validates chunk text against the embedding input profile.
// On failure it returns no chunks, even if earlier windows fit.
func (c *Chunker) ChunkForEmbedding(ctx context.Context, docID domain.DocumentID, text string, profile *embeddinginput.Profile) ([]domain.Chunk, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	cleaned := textutil.NormalizeForIngest(stripHTML(text))
	if profile != nil {
		return c.chunkProfiled(ctx, docID, cleaned, profile)
	}
	units, err := c.textUnits(ctx, cleaned)
	if err != nil {
		return nil, err
	}

	var chunks []domain.Chunk
	start, nextNew := 0, 0
	for nextNew < len(units) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		end, size, estimate := c.window(units, start)
		if end <= nextNew {
			if start < nextNew {
				start = nextNew
				continue
			}
			return nil, errEmbeddingChunkBoundary
		}
		window := joinUnits(units[start:end], size)
		chunks = append(chunks, makeChunk(docID, len(chunks), window, ""))
		nextNew = end
		if end == len(units) {
			break
		}
		start = c.overlapStart(units, start, end, size, estimate)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return chunks, nil
}

func (c *Chunker) textUnits(ctx context.Context, cleaned string) ([]textUnit, error) {
	var units []textUnit
	for i, paragraph := range textutil.SplitParagraphs(cleaned) {
		offset := 0
		for j, sentence := range textutil.SplitSentences(paragraph) {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			sentenceStart := offset + strings.Index(paragraph[offset:], sentence)
			separator := paragraph[offset:sentenceStart]
			if i > 0 && j == 0 {
				separator = "\n\n"
			}
			units = c.appendSentence(units, sentence, separator)
			offset = sentenceStart + len(sentence)
		}
	}
	return units, nil
}

func (c *Chunker) window(units []textUnit, start int) (end, size int, estimate tokenEstimate) {
	for end = start; end < len(units); end++ {
		unit := units[end]
		separator := ""
		if end > start {
			separator = unit.separator
		}
		nextEstimate := estimate.append(unit.estimate, separator != "")
		nextSize := size + len(separator) + len(unit.text)
		if nextEstimate.count() > c.chunkSize || nextSize > MaxChunkBytes {
			break
		}
		estimate, size = nextEstimate, nextSize
	}
	return end, size, estimate
}

func joinUnits(units []textUnit, size int) string {
	var b strings.Builder
	b.Grow(size)
	for i, unit := range units {
		if i > 0 {
			b.WriteString(unit.separator)
		}
		b.WriteString(unit.text)
	}
	return b.String()
}

func (c *Chunker) overlapStart(units []textUnit, start, end, size int, estimate tokenEstimate) int {
	overlapStart := end
	suffixSize := 0
	var suffix tokenEstimate
	for i := end - 1; i > start; i-- {
		nextSuffix := units[i].estimate
		nextSize := len(units[i].text)
		if i < end-1 {
			nextSuffix = nextSuffix.append(suffix, units[i+1].separator != "")
			nextSize += len(units[i+1].separator) + suffixSize
		}
		// Discard at least half of this window in tokens and bytes,
		// while leaving room for the first new unit.
		withNew := nextSuffix.append(units[end].estimate, units[end].separator != "")
		if nextSuffix.count() > min(c.overlap, estimate.count()/2) || nextSize > size/2 ||
			withNew.count() > c.chunkSize ||
			nextSize+len(units[end].separator)+len(units[end].text) > MaxChunkBytes {
			break
		}
		suffix = nextSuffix
		suffixSize = nextSize
		overlapStart = i
	}
	return overlapStart
}

func (c *Chunker) appendSentence(units []textUnit, sentence, separator string) []textUnit {
	estimate := measureTokens(sentence)
	if estimate.count() <= c.chunkSize && len(sentence) <= MaxChunkBytes {
		return append(units, textUnit{sentence, separator, estimate})
	}
	for i, word := range strings.Fields(sentence) {
		if i > 0 {
			separator = " "
		}
		estimate = measureTokens(word)
		if estimate.count() <= c.chunkSize && len(word) <= MaxChunkBytes {
			units = append(units, textUnit{word, separator, estimate})
			continue
		}
		units = c.appendWord(units, word, separator)
	}
	return units
}

func (c *Chunker) appendWord(units []textUnit, word, separator string) []textUnit {
	// Smaller fragments let successive windows share part of a long word.
	fragmentSize := c.chunkSize
	if c.overlap > 0 {
		fragmentSize = c.overlap
	}
	start := 0
	var estimate tokenEstimate
	for offset, r := range word {
		next := estimate
		next.addRune(r)
		if offset > start && (next.count() > fragmentSize || offset-start+len(string(r)) > MaxChunkBytes) {
			units = append(units, textUnit{word[start:offset], separator, estimate})
			start, separator, estimate = offset, "", tokenEstimate{}
			estimate.addRune(r)
		} else {
			estimate = next
		}
	}
	return append(units, textUnit{word[start:], separator, estimate})
}

func makeChunk(docID domain.DocumentID, index int, text, version string) domain.Chunk {
	kind, quality := Classify(text)
	return domain.Chunk{
		ID:         domain.ChunkID(chunkID(version+string(docID), index)),
		DocumentID: docID,
		Index:      index,
		Text:       text,
		TokenCount: EstimateTokens(text),
		Metadata: map[string]any{
			"kind":    string(kind),
			"quality": string(quality),
		},
	}
}

// Fingerprint computes a SHA-256 hex fingerprint for the given content.
func Fingerprint(content string) string {
	h := sha256.Sum256([]byte(content))
	return fmt.Sprintf("%x", h)
}

// ScopedFingerprint preserves default fingerprints and isolates named collections.
func ScopedFingerprint(collection, content string) string {
	if collection == "" {
		return Fingerprint(content)
	}
	return Fingerprint("quarry.collection.v1\x00" + collection + "\x00" + content)
}

func chunkID(docID string, index int) string {
	h := sha256.Sum256([]byte(docID + strconv.Itoa(index)))
	return fmt.Sprintf("%x", h[:16])
}
