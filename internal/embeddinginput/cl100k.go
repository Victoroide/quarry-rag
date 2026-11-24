package embeddinginput

import (
	"bufio"
	"bytes"
	"container/heap"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

const (
	// OpenAIEndpoint is the verified public endpoint for exact profiles.
	OpenAIEndpoint = "https://api.openai.com/v1"
	cl100kDigest   = "223921b76ee99bde995b7ff738513eef100fb51d18c93597a113bcffe865b2a7"
	cl100kWindow   = 8192
)

type cl100kTokenizer struct {
	ranks         map[string]int
	maxTokenBytes int
}

func loadExactTokenizer(path string, spec profileSpec, target Target) (*cl100kTokenizer, error) {
	declared := spec.Tokenizer
	if declared == nil || spec.MaxTokens == 0 {
		return nil, errors.New("an exact profile requires a tokenizer and a positive max_tokens")
	}
	switch target.Model {
	case "text-embedding-3-small", "text-embedding-3-large", "text-embedding-ada-002":
	default:
		return nil, errors.New("exact tokenizer is unverified for this embedding model")
	}
	if target.Provider != "openai" || strings.TrimRight(target.BaseURL, "/") != OpenAIEndpoint ||
		spec.Revision != spec.Model || declared.Type != "tiktoken" || declared.Version != "cl100k_base" {
		return nil, errors.New("exact tokenizer is unverified for this endpoint, model revision or tokenizer version")
	}
	if spec.MaxTokens > cl100kWindow {
		return nil, fmt.Errorf("max_tokens must be at most %d for this model", cl100kWindow)
	}
	if declared.Digest != cl100kDigest {
		return nil, errors.New("tokenizer digest does not match the published cl100k_base digest")
	}
	if declared.Artifact == "" {
		return nil, errors.New("exact profile requires a local tokenizer artifact")
	}
	data, err := readLocalFile(artifactPath(path, declared.Artifact), 2<<20)
	if err != nil {
		return nil, fmt.Errorf("read tokenizer artifact: %w", err)
	}
	if fmt.Sprintf("%x", sha256.Sum256(data)) != declared.Digest {
		return nil, errors.New("tokenizer artifact digest mismatch")
	}

	ranks := make(map[string]int, 100_256)
	maxTokenBytes := 0
	lines := bufio.NewScanner(bytes.NewReader(data))
	for lines.Scan() {
		encoded, rawRank, found := strings.Cut(lines.Text(), " ")
		value, decodeErr := base64.StdEncoding.DecodeString(encoded)
		rank, rankErr := strconv.Atoi(rawRank)
		if !found || decodeErr != nil || rankErr != nil || rank != len(ranks) {
			return nil, errors.New("invalid tokenizer ranks")
		}
		ranks[string(value)] = rank
		maxTokenBytes = max(maxTokenBytes, len(value))
	}
	if lines.Err() != nil || len(ranks) != 100_256 {
		return nil, errors.New("incomplete tokenizer ranks")
	}
	return &cl100kTokenizer{ranks: ranks, maxTokenBytes: maxTokenBytes}, nil
}

func (t *cl100kTokenizer) count(ctx context.Context, text string, stopAbove int) (int, error) {
	count := 0
	for text != "" {
		if cancelErr := ctx.Err(); cancelErr != nil {
			return 0, cancelErr
		}
		end, err := cl100kPiece(ctx, text)
		if err != nil {
			return 0, err
		}
		n, err := t.countPiece(ctx, text[:end])
		if err != nil {
			return 0, err
		}
		count += n
		if stopAbove > 0 && count > stopAbove {
			break
		}
		text = text[end:]
	}
	return count, nil
}

type boundary struct {
	previous, next int
}

type merge struct {
	left, right, end, rank int
}

type mergeQueue []merge

func (q mergeQueue) Len() int { return len(q) }
func (q mergeQueue) Less(i, j int) bool {
	if q[i].rank == q[j].rank {
		return q[i].left < q[j].left
	}
	return q[i].rank < q[j].rank
}
func (q mergeQueue) Swap(i, j int)   { q[i], q[j] = q[j], q[i] }
func (q *mergeQueue) Push(value any) { *q = append(*q, value.(merge)) }
func (q *mergeQueue) Pop() any {
	last := len(*q) - 1
	value := (*q)[last]
	*q = (*q)[:last]
	return value
}

func (t *cl100kTokenizer) countPiece(ctx context.Context, piece string) (int, error) {
	if _, ok := t.ranks[piece]; ok || len(piece) == 1 {
		return 1, nil
	}
	// Merges change only the two adjacent pairs. Queue entries carry the old
	// edges so an invalidated candidate can be discarded without a heap search.
	bounds := make([]boundary, len(piece)+1)
	for i := range bounds {
		bounds[i] = boundary{previous: i - 1, next: i + 1}
	}
	bounds[len(piece)].next = -1
	candidate := func(left int) (merge, bool) {
		right := bounds[left].next
		if right == len(piece) || right == -1 {
			return merge{}, false
		}
		end := bounds[right].next
		rank, ok := t.ranks[piece[left:end]]
		return merge{left: left, right: right, end: end, rank: rank}, ok
	}
	queue := make(mergeQueue, 0, len(piece))
	for i := 0; i+1 < len(piece); i++ {
		if m, ok := candidate(i); ok {
			queue = append(queue, m)
		}
	}
	heap.Init(&queue)
	count := len(piece)
	for i := 0; len(queue) > 0; i++ {
		if i%256 == 0 && ctx.Err() != nil {
			return 0, ctx.Err()
		}
		m := heap.Pop(&queue).(merge)
		if bounds[m.left].next != m.right || bounds[m.right].next != m.end {
			continue
		}
		bounds[m.right].next = -1
		bounds[m.left].next = m.end
		bounds[m.end].previous = m.left
		count--
		if next, ok := candidate(m.left); ok {
			heap.Push(&queue, next)
		}
		if prev := bounds[m.left].previous; prev >= 0 {
			if next, ok := candidate(prev); ok {
				heap.Push(&queue, next)
			}
		}
	}
	return count, nil
}
