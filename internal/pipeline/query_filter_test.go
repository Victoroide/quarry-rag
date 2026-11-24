package pipeline

import (
	"testing"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/stretchr/testify/require"
)

func sc(docID string, score float64) domain.ScoredChunk {
	return domain.ScoredChunk{
		Chunk: domain.Chunk{DocumentID: domain.DocumentID(docID), Text: "t-" + docID},
		Score: score,
	}
}

func docIDs(chunks []domain.ScoredChunk) []string {
	out := make([]string, 0, len(chunks))
	for _, c := range chunks {
		out = append(out, string(c.Chunk.DocumentID))
	}
	return out
}

func TestFilterChunksByAllowedDocIDs_NilAllowlistIsNoOp(t *testing.T) {
	in := []domain.ScoredChunk{sc("a", 0.9), sc("b", 0.8)}
	got := filterChunksByAllowedDocIDs(in, nil)
	if len(got) != 2 {
		t.Fatalf("nil allowlist must not filter; got %v", docIDs(got))
	}
}

func TestFilterChunksByAllowedDocIDs_RestrictsToAllowed(t *testing.T) {
	// Simulates a scoped caller: only document "a" belongs to its scope,
	// so "b" (another scope's document) must never reach synthesis.
	in := []domain.ScoredChunk{sc("a", 0.9), sc("b", 0.8), sc("c", 0.7)}
	got := filterChunksByAllowedDocIDs(in, []string{"a"})
	if len(got) != 1 || string(got[0].Chunk.DocumentID) != "a" {
		t.Fatalf("expected only [a], got %v", docIDs(got))
	}
}

func TestFilterChunksByAllowedDocIDs_EmptyAllowlistMatchesNothing(t *testing.T) {
	// A caller with zero authorized documents must retrieve nothing — never the full corpus.
	in := []domain.ScoredChunk{sc("a", 0.9), sc("b", 0.8)}
	got := filterChunksByAllowedDocIDs(in, []string{})
	if len(got) != 0 {
		t.Fatalf("empty (non-nil) allowlist must match nothing; got %v", docIDs(got))
	}
}

func TestFilterChunksByAllowedDocIDs_PreservesOrder(t *testing.T) {
	in := []domain.ScoredChunk{sc("a", 0.9), sc("b", 0.8), sc("c", 0.7)}
	got := filterChunksByAllowedDocIDs(in, []string{"c", "a"})
	if len(got) != 2 || string(got[0].Chunk.DocumentID) != "a" || string(got[1].Chunk.DocumentID) != "c" {
		t.Fatalf("expected score order [a,c], got %v", docIDs(got))
	}
}

// An admitted chunk carries its collection and document type back from the
// vector store; a hit without them cannot be placed in the requested scope.
func TestEvidenceScopeRequiresAdmittedCollectionMetadata(t *testing.T) {
	filter := domain.VectorFilter{Collection: "archive", DocType: domain.DefaultDocType}
	hit := func(metadata map[string]any) domain.ScoredChunk {
		return domain.ScoredChunk{Chunk: domain.Chunk{ID: "c", DocumentID: "d", Text: "t", Metadata: metadata}, Score: .9}
	}
	for _, tc := range []struct {
		name     string
		metadata map[string]any
		kept     int
	}{
		{"no metadata", nil, 0},
		{"admitted", map[string]any{"collection": "archive", "doc_type": domain.DefaultDocType}, 1},
		{"foreign collection", map[string]any{"collection": "other", "doc_type": domain.DefaultDocType}, 0},
		{"foreign type", map[string]any{"collection": "archive", "doc_type": "report"}, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Len(t, filterEvidenceScope(filter, []domain.ScoredChunk{hit(tc.metadata)}), tc.kept)
		})
	}
}
