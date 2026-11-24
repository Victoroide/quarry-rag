package pipeline

import (
	"context"
	"errors"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/retriever"
	"github.com/Victoroide/quarry-rag/internal/store/memory"
	"github.com/stretchr/testify/require"
)

type scopeCountingEmbedder struct{ calls int }

func (e *scopeCountingEmbedder) Embed(context.Context, string) ([]float32, error) {
	e.calls++
	return []float32{1, 0}, nil
}

type scopeCountingStore struct {
	domain.VectorStore
	calls int
}

func (s *scopeCountingStore) QueryAllNamespaces(context.Context, []float32, int, string) ([]domain.ScoredChunk, error) {
	s.calls++
	return nil, nil
}

func TestRoutingIgnoresDocumentsOutsideAllowlist(t *testing.T) {
	vs := memory.NewFakeVectorStore()
	require.NoError(t, vs.Upsert(t.Context(), "", []domain.Chunk{
		{ID: "private", DocumentID: "private", Embedding: []float32{1, 0}},
		{ID: "allowed", DocumentID: "allowed", Embedding: []float32{0, 1}},
	}))
	qc := &QueryContext{OriginalQuery: "archive", SubQueries: []string{"archive"}, AllowedDocumentIDs: []string{"allowed"}}
	ret := retriever.NewRetriever(vs, &scopeCountingEmbedder{}, 0.7, 10)
	require.NoError(t, NewRoutingStage(ret).Run(t.Context(), qc))
	require.Equal(t, "GLOBAL", qc.RetrievalMode)
	require.Zero(t, qc.TopScore)
}

func TestEmptyAllowlistSkipsEmbeddingAndSearch(t *testing.T) {
	for _, mode := range []string{"", "local", "global"} {
		t.Run(mode, func(t *testing.T) {
			vs, emb := &scopeCountingStore{}, &scopeCountingEmbedder{}
			ret := retriever.NewRetriever(vs, emb, 0.7, 10)
			qc := &QueryContext{OriginalQuery: "archive", RequestedMode: mode, AllowedDocumentIDs: []string{}}
			_, err := RunRetrievalPipeline(t.Context(), qc, []QueryStage{NewRoutingStage(ret), NewRetrievalStage(ret), NewSynthesisStage(nil, nil)})
			require.NoError(t, err)
			require.Zero(t, emb.calls)
			require.Zero(t, vs.calls)
			require.Equal(t, domain.TokenUsage{}, qc.TokenUsage)
			require.True(t, qc.EarlyExit)
		})
	}
}

func TestGlobalRetrievalIssuesOneQuery(t *testing.T) {
	vs := &scopeCountingStore{}
	ret := retriever.NewRetriever(vs, &scopeCountingEmbedder{}, 0.7, 10)
	qc := &QueryContext{OriginalQuery: "archive", RetrievalMode: "GLOBAL", QueryEmbedding: []float32{1, 0}}
	require.NoError(t, NewRetrievalStage(ret).Run(t.Context(), qc))
	require.Equal(t, 1, vs.calls)
}

type scopePreviews struct {
	requested []string
	values    map[string]string
}

func (p *scopePreviews) DocumentPreviews(_ context.Context, ids []string) (map[string]string, error) {
	p.requested = ids
	return p.values, nil
}

type scopeVisibility struct {
	visible map[string]struct{}
	err     error
}

func (v scopeVisibility) VisibleDocumentIDs(context.Context, []string) (map[string]struct{}, error) {
	return v.visible, v.err
}

func TestEntityRoutingUsesOnlyAuthorizedCandidates(t *testing.T) {
	vs := memory.NewFakeVectorStore()
	require.NoError(t, vs.Upsert(t.Context(), "", []domain.Chunk{
		{ID: "private", DocumentID: "private", Embedding: []float32{1, 0}},
		{ID: "invisible", DocumentID: "invisible", Embedding: []float32{1, 0}},
		{ID: "allowed", DocumentID: "allowed", Embedding: []float32{0, 1}},
	}))
	ret := retriever.NewRetriever(vs, &scopeCountingEmbedder{}, 0.7, 10)
	previews := &scopePreviews{values: map[string]string{"allowed": "Cedar Archive stores records."}}
	stage := NewRoutingStageWithScope(ret, scopeVisibility{visible: map[string]struct{}{"allowed": {}}}, previews)
	qc := &QueryContext{OriginalQuery: "Falcon Station", SubQueries: []string{"Falcon Station"}, AllowedDocumentIDs: []string{"allowed", "invisible"}, NamedEntity: "falcon station"}
	require.NoError(t, stage.Run(t.Context(), qc))
	require.Equal(t, "GLOBAL", qc.RetrievalMode)
	require.Empty(t, qc.NamedEntity)
	require.Empty(t, qc.EntityDocumentID)
	require.Equal(t, []string{"allowed"}, previews.requested)
	require.Len(t, qc.Candidates, 1)
	require.Zero(t, qc.TopScore)
	qc.OriginalQuery = "Cedar Archive"
	require.NoError(t, stage.Run(t.Context(), qc))
	require.Equal(t, "LOCAL", qc.RetrievalMode)
	require.Equal(t, "cedar archive", qc.NamedEntity)
	require.Equal(t, "allowed", qc.EntityDocumentID)
}

func TestEntityMatchingUsesWholeWords(t *testing.T) {
	entity, id := MatchNamedEntity("start here", map[string]string{"doc": "Art Center stores records."}, []string{"doc"})
	require.Empty(t, entity)
	require.Empty(t, id)
	entity, id = MatchNamedEntity("ART, center?", map[string]string{"doc": "Art Center stores records."}, []string{"doc"})
	require.Equal(t, "art center", entity)
	require.Equal(t, "doc", id)
	entity, id = MatchNamedEntity("ÉRABLE central", map[string]string{"doc": "Érable Central stores records."}, []string{"doc"})
	require.Equal(t, "érable central", entity)
	require.Equal(t, "doc", id)
}

func TestEntitySelectionIsDeterministic(t *testing.T) {
	previews := map[string]string{"first": "Cedar Archive stores records.", "second": "Cedar Archive stores records.", "long": "Cedar Archive North stores records.", "word": "Archive Station stores records."}
	for range 100 {
		entity, id := MatchNamedEntity("Cedar Archive North", previews, []string{"word", "second", "first", "long"})
		require.Equal(t, "cedar archive north", entity)
		require.Equal(t, "long", id)
		entity, id = MatchNamedEntity("Cedar Archive", previews, []string{"word", "second", "first", "long"})
		require.Equal(t, "cedar archive", entity)
		require.Equal(t, "second", id)
	}
}

func TestExplicitGlobalSkipsProbe(t *testing.T) {
	for _, cfg := range []RoutingConfig{{Policy: retriever.RoutingPolicyLegacy}, evidenceRoutingConfig(.08)} {
		vs, emb := &scopeCountingStore{}, &scopeCountingEmbedder{}
		ret := retriever.NewRetriever(vs, emb, 0.7, 10)
		qc := &QueryContext{OriginalQuery: "archive", RequestedMode: "global"}
		require.NoError(t, NewRoutingStageWithConfig(ret, nil, nil, cfg).Run(t.Context(), qc))
		require.Equal(t, 1, emb.calls)
		require.Zero(t, vs.calls)
		require.Equal(t, retriever.RouteReasonExplicitGlobal, qc.RouteReason)
		require.NoError(t, NewRetrievalStage(ret).Run(t.Context(), qc))
		require.Equal(t, 1, vs.calls)
	}
}

func TestRoutingAppliesDocumentType(t *testing.T) {
	vs := memory.NewFakeVectorStore()
	require.NoError(t, vs.Upsert(t.Context(), "", []domain.Chunk{
		{ID: "other-type", DocumentID: "other", Embedding: []float32{1, 0}, Metadata: map[string]any{"doc_type": "note"}},
		{ID: "allowed", DocumentID: "allowed", Embedding: []float32{0, 1}, Metadata: map[string]any{"doc_type": "report"}},
	}))
	qc := &QueryContext{OriginalQuery: "archive", SubQueries: []string{"archive"}, FilterDocType: "report"}
	ret := retriever.NewRetriever(vs, &scopeCountingEmbedder{}, 0.7, 10)
	require.NoError(t, NewRoutingStage(ret).Run(t.Context(), qc))
	require.Equal(t, "GLOBAL", qc.RetrievalMode)
	require.Len(t, qc.Candidates, 1)
	require.Equal(t, domain.DocumentID("allowed"), qc.Candidates[0].Chunk.DocumentID)
}

func evidenceRoutingConfig(margin float64) RoutingConfig {
	return RoutingConfig{Policy: retriever.RoutingPolicyEvidence, Margin: margin, Calibrated: true}
}

func TestEvidenceLocalSparseUsesOneScopedRetrieval(t *testing.T) {
	store := &localRecordingStore{FakeVectorStore: memory.NewFakeVectorStore()}
	require.NoError(t, store.Upsert(t.Context(), "", []domain.Chunk{
		{ID: "local", DocumentID: "a", Embedding: []float32{1, 0}, Metadata: map[string]any{"collection": "archive", "doc_type": "guide"}},
		{ID: "allowed", DocumentID: "b", Embedding: []float32{0.6, 0.8}, Metadata: map[string]any{"collection": "archive", "doc_type": "guide"}},
		{ID: "private", DocumentID: "c", Embedding: []float32{1, 0}, Metadata: map[string]any{"collection": "private", "doc_type": "guide"}},
	}))
	emb := &scopeCountingEmbedder{}
	ret := retriever.NewRetriever(store, emb, 0.7, 10)
	qc := &QueryContext{OriginalQuery: "Backup and recovery", Collection: "archive", FilterDocType: "guide", AllowedDocumentIDs: []string{"a", "b"}}
	_, err := RunRetrievalPipeline(t.Context(), qc, []QueryStage{NewDecomposeStage(), NewRoutingStageWithConfig(ret, nil, nil, evidenceRoutingConfig(0.08)), NewRetrievalStage(ret)})
	require.NoError(t, err)
	require.Equal(t, "LOCAL", qc.RetrievalMode)
	require.Equal(t, 1, emb.calls)
	require.Len(t, store.filters, 2)
	require.Equal(t, domain.VectorFilter{Collection: "archive", DocType: "guide", DocumentIDs: []string{"a"}}, store.filters[1])
	require.Equal(t, []string{"a", "b"}, candidateDocumentIDs(qc.RetrievedChunks))
}

func TestEvidenceRoutingEdgeCases(t *testing.T) {
	t.Run("probe failure reuses the original query embedding for global retrieval", func(t *testing.T) {
		store := &localRecordingStore{FakeVectorStore: memory.NewFakeVectorStore(), probeErr: errors.New("search unavailable")}
		emb := &scopeCountingEmbedder{}
		ret := retriever.NewRetriever(store, emb, 0.7, 10)
		qc := &QueryContext{OriginalQuery: "What regulations apply?"}
		_, err := RunRetrievalPipeline(t.Context(), qc, []QueryStage{NewRoutingStageWithConfig(ret, nil, nil, evidenceRoutingConfig(.08)), NewRetrievalStage(ret)})
		require.NoError(t, err)
		require.Equal(t, "GLOBAL", qc.RetrievalMode)
		require.Equal(t, retriever.RouteReasonProbeFailed, qc.RouteReason)
		require.Equal(t, 1, emb.calls)
		require.Len(t, store.filters, 2)
		require.Equal(t, store.filters[0], store.filters[1])
	})

	t.Run("conjunction in single noun phrase with strong lead routes local", func(t *testing.T) {
		vs := memory.NewFakeVectorStore()
		require.NoError(t, vs.Upsert(t.Context(), "", []domain.Chunk{
			{ID: "d1-0", DocumentID: "d1", Text: "Backup and Recovery manual.", Embedding: []float32{1, 0}},
			{ID: "d2-0", DocumentID: "d2", Text: "Unrelated onboarding note.", Embedding: []float32{0.5, 0.866}},
		}))
		ret := retriever.NewRetriever(vs, &scopeCountingEmbedder{}, 0.7, 10)
		previews := &scopePreviews{values: map[string]string{
			"d1": "Backup and Recovery Manual covers retention.",
			"d2": "Onboarding Guide covers orientation.",
		}}
		stage := NewRoutingStageWithConfig(ret, nil, previews, evidenceRoutingConfig(0.08))
		qc := &QueryContext{OriginalQuery: "What is the Backup and Recovery retention rule?"}
		require.NoError(t, NewDecomposeStage().Run(t.Context(), qc))
		require.Len(t, qc.SubQueries, 2) // legacy cue matched "and"
		require.NoError(t, stage.Run(t.Context(), qc))
		require.Equal(t, "LOCAL", qc.RetrievalMode)
		require.Equal(t, "d1", qc.LocalDocumentID)
		require.Equal(t, retriever.RouteReasonStrongLead, qc.RouteReason)
	})

	t.Run("duplicate hits from one document aggregate before margin check", func(t *testing.T) {
		vs := memory.NewFakeVectorStore()
		require.NoError(t, vs.Upsert(t.Context(), "", []domain.Chunk{
			{ID: "d1-0", DocumentID: "d1", Embedding: []float32{0.96, 0.28}},
			{ID: "d1-1", DocumentID: "d1", Embedding: []float32{0.95, 0.31}},
			{ID: "d1-2", DocumentID: "d1", Embedding: []float32{0.94, 0.34}},
			{ID: "d1-3", DocumentID: "d1", Embedding: []float32{0.93, 0.37}},
			{ID: "d2-0", DocumentID: "d2", Embedding: []float32{0.60, 0.80}},
		}))
		ret := retriever.NewRetriever(vs, &scopeCountingEmbedder{}, 0.7, 10)
		stage := NewRoutingStageWithConfig(ret, nil, nil, evidenceRoutingConfig(0.08))
		qc := &QueryContext{OriginalQuery: "What is the retention rule?"}
		require.NoError(t, stage.Run(t.Context(), qc))
		require.Equal(t, "LOCAL", qc.RetrievalMode)
		require.Equal(t, "d1", qc.LocalDocumentID)
		require.Equal(t, retriever.RouteReasonStrongLead, qc.RouteReason)
	})

	t.Run("two questions without cue words select global", func(t *testing.T) {
		vs := memory.NewFakeVectorStore()
		require.NoError(t, vs.Upsert(t.Context(), "", []domain.Chunk{
			{ID: "d1-0", DocumentID: "d1", Embedding: []float32{1, 0}},
			{ID: "d2-0", DocumentID: "d2", Embedding: []float32{0.4, 0.9}},
		}))
		ret := retriever.NewRetriever(vs, &scopeCountingEmbedder{}, 0.7, 10)
		stage := NewRoutingStageWithConfig(ret, nil, nil, evidenceRoutingConfig(0.08))
		for _, q := range []string{
			"What is Cedar Archive's retention? What is Maple Station's location?",
			"¿Cuál es el horario? ¿Dónde se archiva?",
			"Cedar Archive retention; Maple Station location",
			"What is Cedar Archive's retention? Explain Maple Station's location.",
		} {
			qc := &QueryContext{OriginalQuery: q}
			require.NoError(t, NewDecomposeStage().Run(t.Context(), qc))
			require.NoError(t, stage.Run(t.Context(), qc))
			require.Equal(t, "GLOBAL", qc.RetrievalMode, q)
			require.Equal(t, retriever.RouteReasonMultiNeed, qc.RouteReason, q)
		}
	})

	t.Run("single spanish question does not trigger multi question punctuation", func(t *testing.T) {
		vs := memory.NewFakeVectorStore()
		require.NoError(t, vs.Upsert(t.Context(), "", []domain.Chunk{
			{ID: "d1-0", DocumentID: "d1", Embedding: []float32{1, 0}},
		}))
		ret := retriever.NewRetriever(vs, &scopeCountingEmbedder{}, 0.7, 10)
		stage := NewRoutingStageWithConfig(ret, nil, nil, evidenceRoutingConfig(0.08))
		qc := &QueryContext{OriginalQuery: "¿Cuál es el horario del archivo?"}
		require.NoError(t, stage.Run(t.Context(), qc))
		require.Equal(t, "LOCAL", qc.RetrievalMode)
		require.Equal(t, retriever.RouteReasonStrongLead, qc.RouteReason)
	})

	t.Run("negation excluding entity selects global while negative verb keeps local", func(t *testing.T) {
		vs := memory.NewFakeVectorStore()
		require.NoError(t, vs.Upsert(t.Context(), "", []domain.Chunk{
			{ID: "cedar-0", DocumentID: "cedar", Embedding: []float32{1, 0}},
			{ID: "maple-0", DocumentID: "maple", Embedding: []float32{0.2, 0.98}},
		}))
		ret := retriever.NewRetriever(vs, &scopeCountingEmbedder{}, 0.7, 10)
		previews := &scopePreviews{values: map[string]string{
			"cedar": "Cedar Archive stores backup records.",
			"maple": "Maple Station stores retention logs.",
		}}
		stage := NewRoutingStageWithConfig(ret, nil, previews, evidenceRoutingConfig(0.08))
		for _, q := range []string{
			"Retention policies, not Cedar Archive",
			"Policies excluding Cedar Archive",
			"Políticas sin incluir Cedar Archive",
			"Cedar Archive excluded from retention rules",
		} {
			qc := &QueryContext{OriginalQuery: q}
			require.NoError(t, stage.Run(t.Context(), qc))
			require.Equal(t, "GLOBAL", qc.RetrievalMode, q)
			require.Equal(t, retriever.RouteReasonNegatedEntity, qc.RouteReason, q)
		}

		qc := &QueryContext{OriginalQuery: "Does Cedar Archive not retain logs?"}
		require.NoError(t, stage.Run(t.Context(), qc))
		require.Equal(t, "LOCAL", qc.RetrievalMode)
		require.Equal(t, "cedar", qc.LocalDocumentID)
	})

	t.Run("named homonyms select global", func(t *testing.T) {
		vs := memory.NewFakeVectorStore()
		require.NoError(t, vs.Upsert(t.Context(), "", []domain.Chunk{
			{ID: "c1-0", DocumentID: "c1", Embedding: []float32{1, 0}},
			{ID: "c2-0", DocumentID: "c2", Embedding: []float32{0.5, 0.866}},
		}))
		ret := retriever.NewRetriever(vs, &scopeCountingEmbedder{}, 0.7, 10)
		previews := &scopePreviews{values: map[string]string{
			"c1": "Cedar Archive North stores financial records.",
			"c2": "Cedar Archive South stores incident records.",
		}}
		stage := NewRoutingStageWithConfig(ret, nil, previews, evidenceRoutingConfig(0.08))
		qc := &QueryContext{OriginalQuery: "Cedar Archive retention rules"}
		require.NoError(t, stage.Run(t.Context(), qc))
		require.Equal(t, "GLOBAL", qc.RetrievalMode)
		require.Equal(t, retriever.RouteReasonAmbiguousEntities, qc.RouteReason)
	})

	t.Run("spelling variants and mixed scripts match entity", func(t *testing.T) {
		vs := memory.NewFakeVectorStore()
		require.NoError(t, vs.Upsert(t.Context(), "", []domain.Chunk{
			{ID: "erable-0", DocumentID: "erable", Embedding: []float32{0.4, 0.9}},
			{ID: "cedar-0", DocumentID: "cedar", Embedding: []float32{0.45, 0.89}},
		}))
		ret := retriever.NewRetriever(vs, &scopeCountingEmbedder{}, 0.7, 10)
		previews := &scopePreviews{values: map[string]string{
			"erable": "Érable Central stores archival records.",
			"cedar":  "Cedar Archive stores security records.",
		}}
		stage := NewRoutingStageWithConfig(ret, nil, previews, evidenceRoutingConfig(0.08))
		for _, tc := range []struct {
			query string
			want  string
		}{
			{"Erable Central retention", "erable"},
			{"Ｃｅｄａｒ　Ａｒｃｈｉｖｅ retention", "cedar"},
			{"Ceder Archive retention", "cedar"},
			{"Cedar Archiveの記録について", "cedar"},
		} {
			qc := &QueryContext{OriginalQuery: tc.query}
			require.NoError(t, stage.Run(t.Context(), qc))
			require.Equal(t, "LOCAL", qc.RetrievalMode, tc.query)
			require.Equal(t, tc.want, qc.LocalDocumentID, tc.query)
			require.Equal(t, retriever.RouteReasonNamedEntity, qc.RouteReason, tc.query)
		}
	})

	t.Run("entity past 120-character preview prefix scores negative entity match", func(t *testing.T) {
		vs := memory.NewFakeVectorStore()
		require.NoError(t, vs.Upsert(t.Context(), "", []domain.Chunk{
			{ID: "d1-0", DocumentID: "d1", Embedding: []float32{0.85, 0.52}},
			{ID: "d2-0", DocumentID: "d2", Embedding: []float32{0.84, 0.54}},
		}))
		ret := retriever.NewRetriever(vs, &scopeCountingEmbedder{}, 0.7, 10)
		// Preview stores only the first 120 chars; "Boreal Vault" is past char 120.
		previews := &scopePreviews{values: map[string]string{
			"d1": "Operational procedures and retention schedules for regional infrastructure compliance audits across all documented facilities",
			"d2": "Operational procedures and storage rules for backup sites across regional zones.",
		}}
		stage := NewRoutingStageWithConfig(ret, nil, previews, evidenceRoutingConfig(0.08))
		qc := &QueryContext{OriginalQuery: "Boreal Vault"}
		require.NoError(t, stage.Run(t.Context(), qc))
		require.Equal(t, "GLOBAL", qc.RetrievalMode)
		require.Empty(t, qc.NamedEntity)
		require.Equal(t, retriever.RouteReasonAmbiguousMargin, qc.RouteReason)
	})

	t.Run("missing calibration selects global in evidence mode", func(t *testing.T) {
		vs := memory.NewFakeVectorStore()
		require.NoError(t, vs.Upsert(t.Context(), "", []domain.Chunk{
			{ID: "d1-0", DocumentID: "d1", Embedding: []float32{1, 0}},
		}))
		ret := retriever.NewRetriever(vs, &scopeCountingEmbedder{}, 0.7, 10)
		stage := NewRoutingStageWithConfig(ret, nil, nil, RoutingConfig{Policy: retriever.RoutingPolicyEvidence, Calibrated: false})
		qc := &QueryContext{OriginalQuery: "What is the retention rule?"}
		require.NoError(t, stage.Run(t.Context(), qc))
		require.Equal(t, "GLOBAL", qc.RetrievalMode)
		require.Equal(t, retriever.RouteReasonMissingCalibration, qc.RouteReason)
	})

	t.Run("visibility and preview errors fail closed", func(t *testing.T) {
		vs := memory.NewFakeVectorStore()
		require.NoError(t, vs.Upsert(t.Context(), "", []domain.Chunk{
			{ID: "d1-0", DocumentID: "d1", Embedding: []float32{1, 0}},
		}))
		ret := retriever.NewRetriever(vs, &scopeCountingEmbedder{}, 0.7, 10)
		visErr := errors.New("db down")
		stage := NewRoutingStageWithConfig(ret, scopeVisibility{err: visErr}, nil, evidenceRoutingConfig(0.08))
		err := stage.Run(t.Context(), &QueryContext{OriginalQuery: "archive"})
		require.ErrorIs(t, err, visErr)

		prevErr := errors.New("previews down")
		stage = NewRoutingStageWithConfig(ret, scopeVisibility{visible: map[string]struct{}{"d1": {}}}, errorPreviews{err: prevErr}, evidenceRoutingConfig(0.08))
		err = stage.Run(t.Context(), &QueryContext{OriginalQuery: "archive"})
		require.ErrorIs(t, err, prevErr)
	})
}

type errorPreviews struct{ err error }

func (e errorPreviews) DocumentPreviews(context.Context, []string) (map[string]string, error) {
	return nil, e.err
}
