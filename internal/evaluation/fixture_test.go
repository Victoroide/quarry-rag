package evaluation

import (
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestFixtureLabelsAndScope(t *testing.T) {
	ns := Namespace{Prefix: "eval", ID: uuid.New()}
	other := Namespace{Prefix: ns.Prefix, ID: uuid.New()}
	require.NotEqual(t, ns.Collection(false), other.Collection(false))
	require.NotEqual(t, ns.DocumentID("atlas"), other.DocumentID("atlas"))
	require.NotEqual(t, ns.Collection(false), ns.Collection(true))
	require.Equal(t, Documents()[0].Text, Documents()[3].Text)

	bySpan := map[string]Document{}
	for _, doc := range Documents() {
		for _, span := range doc.Spans {
			require.Contains(t, doc.Text, span.Text)
			require.NotContains(t, bySpan, span.ID)
			bySpan[span.ID] = doc
		}
	}
	atlas := Documents()[0]
	offset := strings.Index(atlas.Text, atlas.Spans[0].Text)
	require.Greater(t, utf8.RuneCountInString(atlas.Text[:offset]), 600)
	questions := map[string][]Question{}
	for _, question := range Questions() {
		require.NotNil(t, question.Allowed)
		require.NotEmpty(t, question.Accepted)
		questions[question.Pair] = append(questions[question.Pair], question)
		for _, id := range question.Gold {
			require.Contains(t, question.Allowed, bySpan[id].Key)
		}
	}
	for pair, set := range questions {
		if pair != "" {
			require.Len(t, set, 2)
			require.NotEqual(t, set[0].Language, set[1].Language)
			require.Equal(t, set[0].Gold, set[1].Gold)
		}
	}
}

func TestProofMatchesOnlyOwnedAllowedRetainedSpans(t *testing.T) {
	ns := Namespace{Prefix: "qa", ID: uuid.New()}
	atlas, garden := Documents()[0], Documents()[1]
	chunks := []domain.ScoredChunk{
		{Chunk: domain.Chunk{DocumentID: domain.DocumentID(ns.DocumentID("distractor")), Text: atlas.Text}},
		{Chunk: domain.Chunk{DocumentID: domain.DocumentID(ns.DocumentID(garden.Key)), Text: garden.Text}},
		{Chunk: domain.Chunk{DocumentID: domain.DocumentID(ns.DocumentID(atlas.Key)), Text: string([]rune(atlas.Text)[:600])}},
		{Chunk: domain.Chunk{DocumentID: domain.DocumentID(ns.DocumentID(atlas.Key)), Text: atlas.Text}},
	}
	hits := MatchExcerpts(ns, []string{ns.DocumentID(atlas.Key)}, chunks)
	require.Equal(t, []Match{{Span: atlas.Spans[0].ID, Excerpt: 4, Runes: utf8.RuneCountInString(atlas.Text)}}, hits)
	require.Empty(t, MatchExcerpts(ns, []string{}, chunks))
	value, err := EncodeMatches(hits)
	require.NoError(t, err)
	got, err := DecodeMatches(value)
	require.NoError(t, err)
	require.Equal(t, hits, got)
}

func FuzzDecodeMatches(f *testing.F) {
	valid, err := EncodeMatches([]Match{{Span: "atlas-signal", Excerpt: 1, Runes: 800}})
	if err != nil {
		f.Fatal(err)
	}
	f.Add(valid)
	f.Add("")
	f.Add("====")
	f.Fuzz(func(t *testing.T, value string) {
		matches, err := DecodeMatches(value)
		if err == nil {
			encoded, encErr := EncodeMatches(matches)
			require.NoError(t, encErr)
			decoded, decErr := DecodeMatches(encoded)
			require.NoError(t, decErr)
			require.Equal(t, matches, decoded)
		}
	})
}
