package handler

import (
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/pipeline"
)

func queryScoredChunks(qc *pipeline.QueryContext) []ScoredChunkResponse {
	if qc.AnswerMode == domain.AnswerModeEvidence {
		scoredChunks := make([]ScoredChunkResponse, 0, len(qc.Excerpts))
		for i, sc := range qc.Excerpts {
			scoredChunks = append(scoredChunks, scoredChunkResponse(sc, sc.Chunk.Text, i+1, qc.Truncated[i]))
		}
		return scoredChunks
	}
	scoredChunks := make([]ScoredChunkResponse, 0, len(qc.RetrievedChunks))
	for _, sc := range qc.RetrievedChunks {
		// Bound response snippets without splitting UTF-8 sequences.
		text := sc.Chunk.Text
		if runes := []rune(text); len(runes) > 600 {
			text = string(runes[:600])
		}

		excerpt, truncated := 0, false
		for i, sent := range qc.Excerpts {
			if sent.Chunk.DocumentID == sc.Chunk.DocumentID && sent.Chunk.Index == sc.Chunk.Index && sent.Chunk.ID == sc.Chunk.ID {
				excerpt, truncated = i+1, qc.Truncated[i]
				break
			}
		}
		scoredChunks = append(scoredChunks, scoredChunkResponse(sc, text, excerpt, truncated))
	}
	return scoredChunks
}

func scoredChunkResponse(sc domain.ScoredChunk, text string, excerpt int, truncated bool) ScoredChunkResponse {
	docType, _ := sc.Chunk.Metadata["doc_type"].(string)
	kind, _ := sc.Chunk.Metadata["kind"].(string)
	quality, _ := sc.Chunk.Metadata["quality"].(string)
	return ScoredChunkResponse{
		DocID:       string(sc.Chunk.DocumentID),
		DocType:     docType,
		ChunkIndex:  sc.Chunk.Index,
		Score:       sc.Score,
		RerankScore: sc.RerankScore,
		Text:        text,
		Kind:        kind,
		Quality:     quality,
		Excerpt:     excerpt,
		Truncated:   truncated,
	}
}
