package pipeline

import (
	"fmt"
	"slices"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/retriever"
	"github.com/Victoroide/quarry-rag/internal/store/memory"
	"github.com/stretchr/testify/require"
)

func routingFixtureChunks(doc string, n int, v []float32) []domain.Chunk {
	var chunks []domain.Chunk
	for i := range n {
		chunks = append(chunks, domain.Chunk{ID: domain.ChunkID(fmt.Sprintf("%s%d", doc, i)), DocumentID: domain.DocumentID(doc), Index: i, Embedding: v})
	}
	return chunks
}

func runRoutingFixture(t *testing.T, cfg RoutingConfig, names map[string]string, chunks []domain.Chunk, q, requested string, k int, allowed ...string) (qc *QueryContext, embeds, searches int) {
	t.Helper()
	store := &localRecordingStore{FakeVectorStore: memory.NewFakeVectorStore()}
	require.NoError(t, store.Upsert(t.Context(), "", chunks))
	emb := &scopeCountingEmbedder{}
	ret := retriever.NewRetriever(store, emb, .70, k)
	qc = &QueryContext{OriginalQuery: q, RequestedMode: requested, MaxChunks: k, AllowedDocumentIDs: allowed}
	_, err := RunRetrievalPipeline(t.Context(), qc, []QueryStage{NewDecomposeStage(), NewRoutingStageWithConfig(ret, nil, &scopePreviews{values: names}, cfg), NewRetrievalStage(ret)})
	require.NoError(t, err)
	return qc, emb.calls, len(store.filters)
}

func TestRoutingProbeCutoffTie(t *testing.T) {
	for _, n := range []int{4, 5} {
		chunks := append(routingFixtureChunks("a", n, []float32{1, 0}), routingFixtureChunks("z", 1, []float32{1, 0})...)
		for _, req := range []string{"", "global"} {
			qc, embeds, searches := runRoutingFixture(t, evidenceRoutingConfig(.08), nil, chunks, "What are the regional retention rules?", req, 6)
			t.Logf("A chunks=%d request=%q mode=%s reason=%s candidates=%v retrieved=%v embeds=%d searches=%d", n, req, qc.RetrievalMode, qc.RouteReason, candidateDocumentIDs(qc.Candidates), candidateDocumentIDs(qc.RetrievedChunks), embeds, searches)
			if qc.RetrievalMode != "GLOBAL" || !slices.Contains(candidateDocumentIDs(qc.RetrievedChunks), "z") {
				t.Errorf("an additional equal-scoring A chunk eclipsed tied document Z")
			}
		}
	}
	chunks := routingFixtureChunks("a", 6, []float32{1, 0})
	qc, _, _ := runRoutingFixture(t, evidenceRoutingConfig(.08), nil, chunks, "Retention rule?", "", 6, "a")
	require.Equal(t, "LOCAL", qc.RetrievalMode)
	require.Equal(t, retriever.RouteReasonStrongLead, qc.RouteReason)

	chunks = append(routingFixtureChunks("head", 1, []float32{1, 0}),
		routingFixtureChunks("tail", 4, []float32{.8, .6})...)
	for i := range chunks {
		chunks[i].DocumentID = "a"
	}
	chunks = append(chunks, routingFixtureChunks("z", 1, []float32{.6, .8})...)
	qc, _, _ = runRoutingFixture(t, evidenceRoutingConfig(.08), nil, chunks, "Retention rule?", "", 6)
	require.Equal(t, "LOCAL", qc.RetrievalMode)
	require.Equal(t, retriever.RouteReasonStrongLead, qc.RouteReason)
	require.Equal(t, []string{"a"}, candidateDocumentIDs(qc.RetrievedChunks))
}

func TestEvidenceLocalRetainsNamedTarget(t *testing.T) {
	chunks := append(routingFixtureChunks("a", 3, []float32{1, 0}), routingFixtureChunks("b", 3, []float32{.99, .1})...)
	names := map[string]string{"a": "Maple Station.", "b": "Cedar Archive."}
	for _, limit := range []int{1, 2, 3} {
		qc, embeds, searches := runRoutingFixture(t, evidenceRoutingConfig(.08), names, chunks, "Cedar Archive retention", "", limit)
		require.Equal(t, "LOCAL", qc.RetrievalMode)
		require.Equal(t, "b", qc.LocalDocumentID)
		require.Equal(t, []string{"b"}, candidateDocumentIDs(qc.RetrievedChunks))
		require.Len(t, qc.RetrievedChunks, limit)
		require.Equal(t, 1, embeds)
		require.Equal(t, 2, searches)
	}
}

func TestEvidenceRoutingIndependentRequests(t *testing.T) {
	for _, tt := range []struct{ name, query, first, second, want string }{
		{"independent names", "Cedar Archive North and Red Oak retention", "Cedar Archive North.", "Cedar Archive. Red Oak.", "GLOBAL"},
		{"positive and excluded in one preview", "Maple Station policies, excluding Cedar Archive", "Maple Station. Cedar Archive.", "Red Oak.", "GLOBAL"},
		{"negative verb before entity", "What is not covered by Cedar Archive?", "Cedar Archive.", "Red Oak.", "LOCAL"},
		{"single named summary", "Summarize Cedar Archive.", "Cedar Archive.", "Red Oak.", "LOCAL"},
		{"single noun modifier", "What does Cedar Archive require for all employees?", "Cedar Archive.", "Red Oak.", "LOCAL"},
		{"two unmarked requests", "1. State retention.\n   Include the regional exception.\n2. State shipping times.", "Cedar Archive.", "Red Oak.", "GLOBAL"},
		{"direct question then unpunctuated yes-no question", "When are records deleted? Is an invoice retained", "Cedar Archive.", "Red Oak.", "GLOBAL"},
		{"explicit second request with terminal punctuation only", "Who maintains the schedule and where are the forms kept?", "Cedar Archive.", "Red Oak.", "GLOBAL"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			chunks := append(routingFixtureChunks("a", 3, []float32{1, 0}), routingFixtureChunks("b", 1, []float32{.2, .98})...)
			names := map[string]string{"a": tt.first, "b": tt.second}
			require.Equal(t, MatchNamedEntityEvidence(tt.query, names, []string{"a", "b"}),
				MatchNamedEntityEvidence(tt.query, names, []string{"b", "a"}))
			qc, _, _ := runRoutingFixture(t, evidenceRoutingConfig(.08), names, chunks, tt.query, "", 3)
			t.Logf("%q => %s reason=%s selected=%s", tt.query, qc.RetrievalMode, qc.RouteReason, qc.LocalDocumentID)
			if qc.RetrievalMode != tt.want {
				t.Errorf("want %s", tt.want)
			}
		})
	}
}

func TestMixedScriptHomonymFollowsAllowedScope(t *testing.T) {
	chunks := append(routingFixtureChunks("a", 3, []float32{1, 0}), routingFixtureChunks("b", 1, []float32{.2, .98})...)
	names := map[string]string{"a": "東京 Metro. Weekday service.", "b": "東京 Metro. Holiday service."}
	qc, _, _ := runRoutingFixture(t, evidenceRoutingConfig(.08), names, chunks, "東京 Metro hours?", "", 4)
	require.Equal(t, retriever.RouteReasonAmbiguousEntities, qc.RouteReason)
	require.False(t, qc.previewNegativeMatch)
	qc, _, _ = runRoutingFixture(t, evidenceRoutingConfig(.08), names, chunks, "東京 Metro hours?", "", 4, "b")
	require.Equal(t, retriever.RouteReasonNamedEntity, qc.RouteReason)
	require.Equal(t, []string{"b"}, candidateDocumentIDs(qc.RetrievedChunks))
}

func TestEvidenceRoutingExclusionGovernsItsOwnName(t *testing.T) {
	names := map[string]string{"a": "Maple Station.", "b": "Cedar Archive."}
	for _, tt := range []struct {
		name, query, mode, target string
	}{
		{"modifier after the name", "Can I enter Cedar Archive without a badge?", "LOCAL", "b"},
		{"complement before the name", "Is entry without a badge permitted at Cedar Archive?", "LOCAL", "b"},
		{"spanish complement before the name", "¿Se permite la entrada sin credencial en Cedar Archive?", "LOCAL", "b"},
		{"source of an excluded item", "Which material is excluded from the Cedar Archive collection?", "LOCAL", "b"},
		{"exclusion ends before a new constituent", "Do any rules except the retired ones apply to Cedar Archive?", "LOCAL", "b"},
		{"direct object of an exclusion", "What are the opening hours, excluding Cedar Archive?", "GLOBAL", ""},
		{"direct object without a comma", "What are the opening hours excluding Cedar Archive?", "GLOBAL", ""},
		{"object with a modifier", "What are the opening hours excluding the old Cedar Archive?", "GLOBAL", ""},
		{"object reached through a location", "What are the hours except those in Cedar Archive?", "GLOBAL", ""},
		{"object reached through a noun linker", "Which rules apply, excluding rules for Cedar Archive?", "GLOBAL", ""},
		{"second object of an exclusion", "What are the hours excluding Maple Station and Cedar Archive?", "GLOBAL", ""},
		{"spanish direct object", "Políticas sin incluir Cedar Archive", "GLOBAL", ""},
		{"complement then a later exclusion", "Which hours apply excluding badge rules for Cedar Archive?", "GLOBAL", ""},
	} {
		t.Run(tt.name, func(t *testing.T) {
			chunks := append(routingFixtureChunks("a", 3, []float32{1, 0}), routingFixtureChunks("b", 1, []float32{.99, .1})...)
			qc, _, _ := runRoutingFixture(t, evidenceRoutingConfig(.08), names, chunks, tt.query, "", 1)
			require.Equal(t, tt.mode, qc.RetrievalMode, "reason %s", qc.RouteReason)
			require.Equal(t, tt.target, qc.LocalDocumentID)
			if tt.target != "" {
				require.Equal(t, []string{tt.target}, candidateDocumentIDs(qc.RetrievedChunks))
			}
		})
	}
}

func TestEvidenceRoutingNumberedTitleIsAHomonym(t *testing.T) {
	first, second := "Apollo 11. Weekend hours.", "Apollo 11. Weekday hours."
	chunks := append(routingFixtureChunks("a", 3, []float32{1, 0}), routingFixtureChunks("b", 1, []float32{.2, .98})...)
	names := map[string]string{"a": first, "b": second}

	qc, _, _ := runRoutingFixture(t, evidenceRoutingConfig(.08), names, chunks, "Apollo 11 hours?", "", 4)
	require.Equal(t, "GLOBAL", qc.RetrievalMode)
	require.Equal(t, retriever.RouteReasonAmbiguousEntities, qc.RouteReason)
	require.ElementsMatch(t, []string{"a", "b"}, candidateDocumentIDs(qc.RetrievedChunks))
	require.False(t, qc.previewNegativeMatch)

	// The same scores and structure with a spelled number are the control.
	spelled := map[string]string{"a": "Apollo Eleven. Weekend hours.", "b": "Apollo Eleven. Weekday hours."}
	control, _, _ := runRoutingFixture(t, evidenceRoutingConfig(.08), spelled, chunks, "Apollo Eleven hours?", "", 4)
	require.Equal(t, qc.RouteReason, control.RouteReason)
	require.ElementsMatch(t, candidateDocumentIDs(qc.RetrievedChunks), candidateDocumentIDs(control.RetrievedChunks))

	// With one visible document the number completes a single named target.
	scoped, _, _ := runRoutingFixture(t, evidenceRoutingConfig(.08), names, chunks, "Apollo 11 hours?", "", 4, "b")
	require.Equal(t, "LOCAL", scoped.RetrievalMode)
	require.Equal(t, retriever.RouteReasonNamedEntity, scoped.RouteReason)
	require.Equal(t, []string{"b"}, candidateDocumentIDs(scoped.RetrievedChunks))

	// A different number or a bare number does not name the title.
	for _, query := range []string{"Apollo 12 hours?", "11 hours?", "Apollo hours?"} {
		other, _, _ := runRoutingFixture(t, evidenceRoutingConfig(.08), names, chunks, query, "", 4)
		require.Empty(t, other.NamedEntity, query)
		require.NotEqual(t, retriever.RouteReasonAmbiguousEntities, other.RouteReason, query)
	}
}

func TestEvidenceRoutingExclusionPolarity(t *testing.T) {
	names := map[string]string{"a": "Maple Station.", "b": "Cedar Archive."}
	chunks := append(routingFixtureChunks("a", 3, []float32{1, 0}), routingFixtureChunks("b", 1, []float32{.99, .1})...)
	for _, tc := range []struct {
		query, document string
		reason          retriever.RouteReason
	}{
		{"For opening hours, include Cedar Archive.", "b", retriever.RouteReasonNamedEntity},
		{"For opening hours, omit Cedar Archive.", "a", retriever.RouteReasonNegatedEntity},
		{"For opening hours, do not omit Cedar Archive.", "b", retriever.RouteReasonNamedEntity},
		{"For opening hours, Cedar Archive is excluded.", "a", retriever.RouteReasonNegatedEntity},
		{"For opening hours, Cedar Archive is not excluded.", "b", retriever.RouteReasonNamedEntity},
		{"For opening hours, Cedar Archive is not open on Mondays.", "b", retriever.RouteReasonNamedEntity},
		{"Para el horario, no excluir Cedar Archive.", "b", retriever.RouteReasonNamedEntity},
		// A negated inclusion verb excludes the name it governs.
		{"For opening hours, do not include Cedar Archive.", "a", retriever.RouteReasonNegatedEntity},
		{"For opening hours, don't include Cedar Archive.", "a", retriever.RouteReasonNegatedEntity},
		{"For opening hours, don\u2019t include the Cedar Archive.", "a", retriever.RouteReasonNegatedEntity},
		{"For opening hours, DON'T include Cedar Archive.", "a", retriever.RouteReasonNegatedEntity},
		{"For opening hours, not including Cedar Archive.", "a", retriever.RouteReasonNegatedEntity},
		{"Para el horario, no incluyas Cedar Archive.", "a", retriever.RouteReasonNegatedEntity},
		{"For opening hours, Cedar Archive isn't included.", "a", retriever.RouteReasonNegatedEntity},
		// The contraction means the same as its expansion.
		{"For opening hours, don't omit Cedar Archive.", "b", retriever.RouteReasonNamedEntity},
		{"For opening hours, don\u2019t omit Cedar Archive.", "b", retriever.RouteReasonNamedEntity},
		{"For opening hours, Cedar Archive isn't excluded.", "b", retriever.RouteReasonNamedEntity},
		{"For opening hours, Cedar Archive isn\u2019t excluded.", "b", retriever.RouteReasonNamedEntity},
		{"For opening hours, you shouldn't exclude Cedar Archive.", "b", retriever.RouteReasonNamedEntity},
		{"For opening hours, you can't exclude Cedar Archive.", "b", retriever.RouteReasonNamedEntity},
		// Negating the predicate, the verb's other object or an earlier verb
		// leaves the name included.
		{"For opening hours, Cedar Archive isn't open on Mondays.", "b", retriever.RouteReasonNamedEntity},
		{"For opening hours, Cedar Archive doesn't include a badge desk.", "b", retriever.RouteReasonNamedEntity},
		{"For opening hours, do not forget to include Cedar Archive.", "b", retriever.RouteReasonNamedEntity},
		{"For opening hours, omit staff rules but include Cedar Archive.", "b", retriever.RouteReasonNamedEntity},
		{"Is Cedar Archive open without a badge?", "b", retriever.RouteReasonNamedEntity},
	} {
		t.Run(tc.query, func(t *testing.T) {
			qc, _, _ := runRoutingFixture(t, evidenceRoutingConfig(.08), names, chunks, tc.query, "", 1)
			require.Equal(t, []string{tc.document}, candidateDocumentIDs(qc.RetrievedChunks))
			require.Equal(t, tc.reason, qc.RouteReason)
		})
	}
}

func TestEvidenceRoutingPartialNumberedTitle(t *testing.T) {
	names := map[string]string{"a": "Harbor Guide.", "b": "Apollo Mission 11."}
	chunks := append(routingFixtureChunks("a", 3, []float32{1, 0}), routingFixtureChunks("b", 1, []float32{.99, .1})...)
	for _, tc := range []struct {
		query, mode, document string
		reason                retriever.RouteReason
	}{
		{"Apollo Mission 11 hours?", "LOCAL", "b", retriever.RouteReasonNamedEntity},
		{"Apollo Mission hours?", "LOCAL", "b", retriever.RouteReasonNamedEntity},
		{"Apollo Mission 12 hours?", "GLOBAL", "a", retriever.RouteReasonAmbiguousMargin},
	} {
		t.Run(tc.query, func(t *testing.T) {
			qc, _, _ := runRoutingFixture(t, evidenceRoutingConfig(.08), names, chunks, tc.query, "", 1)
			require.Equal(t, tc.mode, qc.RetrievalMode)
			require.Equal(t, tc.reason, qc.RouteReason)
			require.Equal(t, []string{tc.document}, candidateDocumentIDs(qc.RetrievedChunks))
		})
	}
	for _, tc := range []struct{ title, query string }{
		{"Apollo Mission Control 11.", "Apollo Mission Control 12 hours?"},
		{"Apollo 11 Mission Control.", "Apollo 12 Mission Control hours?"},
	} {
		names["b"] = tc.title
		require.Empty(t, MatchNamedEntityEvidence(tc.query, names, []string{"a", "b"}).DocumentID, tc.query)
	}
}

func TestEvidenceEntityMatchesNumbersExactly(t *testing.T) {
	order := []string{"a"}
	previews := map[string]string{"a": "Terminal 20241. Gate hours."}
	require.Equal(t, "a", MatchNamedEntityEvidence("Terminal 20241 hours?", previews, order).DocumentID)
	require.Empty(t, MatchNamedEntityEvidence("Terminal 20242 hours?", previews, order).DocumentID)
	require.Empty(t, MatchNamedEntityEvidence("2024 gate hours?", previews, order).DocumentID)
}
