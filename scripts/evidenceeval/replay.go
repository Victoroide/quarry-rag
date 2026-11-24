package main

import (
	"github.com/Victoroide/quarry-rag/client"
	"github.com/Victoroide/quarry-rag/internal/evaluation"
	"github.com/google/uuid"
)

const defaultK = 3

func replay(includeRaw bool) report {
	ns := evaluation.Namespace{Prefix: "replay", ID: uuid.Nil}
	r := report{Target: "public-fixture", Backend: "recorded"}
	questions := evaluation.Questions()
	atlas := evaluation.Documents()[0]
	garden := evaluation.Documents()[1]
	atlasChunk := client.ScoredChunkResponse{DocID: ns.DocumentID(atlas.Key), Text: string([]rune(atlas.Text)[:600]), Score: .9, Excerpt: 1}
	gardenChunk := client.ScoredChunkResponse{DocID: ns.DocumentID(garden.Key), Text: garden.Text, Score: .9, Excerpt: 1}
	archiveChunk := client.ScoredChunkResponse{DocID: ns.DocumentID("archive"), Text: evaluation.Documents()[2].Text, Score: .9}
	for i, response := range []struct {
		answer string
		finish string
		chunks []client.ScoredChunkResponse
		label  review
		ms     float64
	}{
		{questions[0].Accepted[0], "stop", []client.ScoredChunkResponse{atlasChunk, archiveChunk}, review{Claims: 1, Accepted: true}, 10},
		{questions[1].Accepted[0], "stop", []client.ScoredChunkResponse{archiveChunk, atlasChunk}, review{Claims: 1, Accepted: true}, 20},
		{questions[2].Accepted[0], "stop", []client.ScoredChunkResponse{gardenChunk}, review{Claims: 1, Accepted: true}, 30},
		{"Exactamente ocho minutos.", "stop", []client.ScoredChunkResponse{archiveChunk}, review{Claims: 1, Unsupported: 1}, 40},
		{"The archive lists one cover. It does not identify its owner.", "stop", []client.ScoredChunkResponse{archiveChunk}, review{Claims: 1, Accepted: true}, 50},
		{"", "no_evidence", []client.ScoredChunkResponse{}, review{Accepted: true}, 5},
	} {
		q := questions[i]
		s := observe(q, ns, &client.QueryResponse{Answer: response.answer, FinishReason: response.finish, ScoredChunks: response.chunks}, nil, defaultK, includeRaw)
		s.Milliseconds = response.ms
		s.Review = &response.label
		r.Queries = append(r.Queries, s)
	}
	ten, twenty, thirty := 10.0, 20.0, 30.0
	r.Visibility = []visibility{
		{Document: "atlas", Outcome: "observed", DelayMS: &ten},
		{Document: "garden", Outcome: "observed", DelayMS: &twenty},
		{Document: "archive", Outcome: "censored", Reason: "deadline", CensoredMS: &thirty},
		{Document: "distractor", Outcome: "failed", Reason: "throttled", Status: 429},
	}
	r.summarize(false)
	return r
}
