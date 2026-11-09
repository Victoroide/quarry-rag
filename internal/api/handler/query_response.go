package handler

import "github.com/Victoroide/quarry-rag/internal/pipeline"

func queryScoredChunks(qc *pipeline.QueryContext) []ScoredChunkResponse {
	scoredChunks := make([]ScoredChunkResponse, 0, len(qc.RetrievedChunks))
	for _, sc := range qc.RetrievedChunks {
		docType, kind, quality := "", "", ""
		if sc.Chunk.Metadata != nil {
			if dt, ok := sc.Chunk.Metadata["doc_type"]; ok {
				docType, _ = dt.(string)
			}
			if k, ok := sc.Chunk.Metadata["kind"]; ok {
				kind, _ = k.(string)
			}
			if q, ok := sc.Chunk.Metadata["quality"]; ok {
				quality, _ = q.(string)
			}
		}
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
		scoredChunks = append(scoredChunks, ScoredChunkResponse{
			DocID:      string(sc.Chunk.DocumentID),
			DocType:    docType,
			ChunkIndex: sc.Chunk.Index,
			Score:      sc.Score,
			Text:       text,
			Kind:       kind,
			Quality:    quality,
			Excerpt:    excerpt,
			Truncated:  truncated,
		})
	}
	return scoredChunks
}
