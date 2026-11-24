package pipeline

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode"

	"golang.org/x/text/width"

	"github.com/Victoroide/quarry-rag/internal/chunker"
	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/retriever"
	"github.com/Victoroide/quarry-rag/internal/store/memory"
	"github.com/stretchr/testify/require"
)

type routingEvalFixture struct {
	CorpusVersion  string             `json:"corpus_version"`
	ChunkTokens    int                `json:"chunk_tokens"`
	ChunkOverlap   int                `json:"chunk_overlap"`
	MaxChunks      int                `json:"max_chunks"`
	LocalThreshold float64            `json:"local_threshold"`
	Dev            routingDevSplit    `json:"dev"`
	Eval           routingHeldOutData `json:"eval"`
}

type routingCorpusDoc struct {
	ID         string `json:"id"`
	Collection string `json:"collection"`
	DocType    string `json:"doc_type"`
	Visible    bool   `json:"visible"`
	Content    string `json:"content"`
	Preview    string `json:"preview"`
}

type routingDevQuery struct {
	ID           string   `json:"id"`
	Slice        string   `json:"slice"`
	Query        string   `json:"query"`
	RequiredDocs []string `json:"required_docs"`
}

type routingDevSplit struct {
	Documents []routingCorpusDoc `json:"documents"`
	Queries   []routingDevQuery  `json:"queries"`
}

type routingEvalQueryVariant struct {
	ID    string `json:"id"`
	Query string `json:"query"`
}

type routingEvalFact struct {
	DocumentID string `json:"document_id"`
	Excerpt    string `json:"excerpt"`
}

// routingEvalSpellingEdit declares the one-character spelling change that turns
// a word of the first form into the word of the second form.
type routingEvalSpellingEdit struct {
	From string `json:"from"`
	To   string `json:"to"`
}

type routingEvalPair struct {
	PairID        string                    `json:"pair_id"`
	Slice         string                    `json:"slice"`
	RequiredDocs  []string                  `json:"required_docs"`
	RequiredFacts []routingEvalFact         `json:"required_facts"`
	A             routingEvalQueryVariant   `json:"a"`
	B             routingEvalQueryVariant   `json:"b"`
	SpellingEdits []routingEvalSpellingEdit `json:"spelling_edits"`
}

type routingEmptyProbeCase struct {
	ID                 string   `json:"id"`
	Query              string   `json:"query"`
	AllowedDocumentIDs []string `json:"allowed_document_ids"`
	Collection         string   `json:"collection"`
}

type routingNegativeEntityCase struct {
	ID         string `json:"id"`
	Query      string `json:"query"`
	Collection string `json:"collection"`
}

type routingHeldOutData struct {
	Documents              []routingCorpusDoc          `json:"documents"`
	Pairs                  []routingEvalPair           `json:"pairs"`
	EmptyProbes            []routingEmptyProbeCase     `json:"empty_probes"`
	VerifiedNegativeEntity []routingNegativeEntityCase `json:"verified_negative_entities"`
}

// replayEmbedder is a frozen deterministic lexical+char3 count-sketch embedder
// used exclusively for zero-cost offline corpus evaluation.
type replayEmbedder struct {
	dimensions int
	calls      int
}

func (e *replayEmbedder) Embed(_ context.Context, text string) ([]float32, error) {
	e.calls++
	vec := make([]float64, min(max(1, e.dimensions), 65535))
	dim := uint64(len(vec))
	toks := tokenizeForEvidenceEntity(text)
	addFeature := func(feat string, weight float64) {
		h := fnv.New64a()
		_, _ = h.Write([]byte(feat))
		hv := h.Sum64()
		sign := 1.0
		if (hv>>63)&1 == 1 {
			sign = -1.0
		}
		vec[hv%dim] += sign * weight
	}
	for i, t := range toks {
		addFeature("w:"+t.norm, 1.8)
		if i+1 < len(toks) {
			addFeature("b:"+t.norm+"_"+toks[i+1].norm, 0.9)
		}
		runes := []rune(t.norm)
		if len(runes) >= 3 && !isDenseScriptRune(runes[0]) {
			padded := append([]rune{'^'}, append(runes, '$')...)
			for j := 0; j+3 <= len(padded); j++ {
				addFeature("c3:"+string(padded[j:j+3]), 0.45)
			}
		} else if len(runes) > 0 {
			for _, r := range runes {
				if unicode.IsLetter(r) || unicode.IsDigit(r) {
					addFeature("d:"+string(r), 1.0)
				}
			}
		}
	}
	var normSq float64
	for _, v := range vec {
		normSq += v * v
	}
	out := make([]float32, e.dimensions)
	if normSq == 0 {
		out[0] = 1
		return out, nil
	}
	inv := 1.0 / math.Sqrt(normSq)
	for i, v := range vec {
		out[i] = float32(v * inv)
	}
	return out, nil
}

func loadRoutingEvalFixture(t *testing.T) routingEvalFixture {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", "routing_eval.json"))
	require.NoError(t, err)
	require.Equal(t, "9080d20fcaa98a6c8c6cb737bf04853cf591517097de3883b7d1034dffbc26d2", fmt.Sprintf("%x", sha256.Sum256(data)), "frozen replay manifest changed")
	var fix routingEvalFixture
	require.NoError(t, json.Unmarshal(data, &fix))
	return fix
}

type evalCorpusStore struct {
	vs       *localRecordingStore
	emb      *replayEmbedder
	vis      scopeVisibility
	previews *scopePreviews
}

func buildEvalCorpusStore(t *testing.T, docs []routingCorpusDoc, chunkTokens, chunkOverlap, dims int) *evalCorpusStore {
	t.Helper()
	chk := chunker.NewChunker(chunkTokens, chunkOverlap)
	emb := &replayEmbedder{dimensions: dims}
	vs := &localRecordingStore{FakeVectorStore: memory.NewFakeVectorStore()}
	visible := make(map[string]struct{})
	allPreviews := make(map[string]string, len(docs))

	for _, doc := range docs {
		previewRunes := []rune(doc.Content)
		if len(previewRunes) > 120 {
			previewRunes = previewRunes[:120]
		}
		require.Equal(t, string(previewRunes), doc.Preview, doc.ID)
		allPreviews[doc.ID] = doc.Preview
		if doc.Visible {
			visible[doc.ID] = struct{}{}
		}
		chunks := chk.Chunk(domain.DocumentID(doc.ID), doc.Content)
		require.NotEmpty(t, chunks, doc.ID)
		for i := range chunks {
			vec, err := emb.Embed(t.Context(), chunks[i].Text)
			require.NoError(t, err)
			chunks[i].Embedding = vec
			chunks[i].Metadata = map[string]any{
				"doc_type":   doc.DocType,
				"collection": doc.Collection,
			}
		}
		require.NoError(t, vs.Upsert(t.Context(), "", chunks))
	}
	emb.calls = 0
	return &evalCorpusStore{
		vs:       vs,
		emb:      emb,
		vis:      scopeVisibility{visible: visible},
		previews: &scopePreviews{values: allPreviews},
	}
}

type evalOutcome struct {
	mode              string
	reason            retriever.RouteReason
	allRequiredRecall float64
	falseLocal        bool
	retrievedDocCount int
	embedCalls        int
	vectorCalls       int
	latency           time.Duration
	negativeChecked   bool
	emptyProbe        bool
	emptyScope        bool
}

func runEvalQuery(t *testing.T, cs *evalCorpusStore, threshold float64, maxChunks int, cfg RoutingConfig, query, collection string, allowed, requiredDocs []string, requestedMode string) evalOutcome {
	t.Helper()
	ret := retriever.NewRetriever(cs.vs, cs.emb, threshold, maxChunks)
	scopedPreviews := &evalScopedPreviews{visible: cs.vis.visible, all: cs.previews.values}
	routing := NewRoutingStageWithConfig(ret, cs.vis, scopedPreviews, cfg)
	retrieval := NewRetrievalStageWithVisibility(ret, cs.vis)

	beforeEmbeds := cs.emb.calls
	beforeSearches := len(cs.vs.filters)
	qc := &QueryContext{
		OriginalQuery:      query,
		MaxChunks:          maxChunks,
		Collection:         collection,
		AllowedDocumentIDs: allowed,
		RequestedMode:      requestedMode,
	}
	start := time.Now()
	_, err := RunRetrievalPipeline(t.Context(), qc, []QueryStage{NewDecomposeStage(), routing, retrieval})
	latency := time.Since(start)
	require.NoError(t, err)
	embedDelta := cs.emb.calls - beforeEmbeds

	gotDocs := make(map[string]struct{}, len(qc.RetrievedChunks))
	for _, sc := range qc.RetrievedChunks {
		gotDocs[string(sc.Chunk.DocumentID)] = struct{}{}
	}
	allCovered := 0.0
	if len(requiredDocs) > 0 {
		covered := 0
		for _, reqID := range requiredDocs {
			if _, ok := gotDocs[reqID]; ok {
				covered++
			}
		}
		if covered == len(requiredDocs) {
			allCovered = 1.0
		}
	}
	falseLocal := len(requiredDocs) > 1 && qc.RetrievalMode == "LOCAL"
	return evalOutcome{
		mode:              qc.RetrievalMode,
		reason:            qc.RouteReason,
		allRequiredRecall: allCovered,
		falseLocal:        falseLocal,
		retrievedDocCount: len(gotDocs),
		embedCalls:        embedDelta,
		vectorCalls:       len(cs.vs.filters) - beforeSearches,
		latency:           latency,
		negativeChecked:   qc.previewNegativeMatch,
		emptyProbe:        qc.RouteReason == retriever.RouteReasonEmptyProbe,
		emptyScope:        qc.RouteReason == retriever.RouteReasonEmptyScope,
	}
}

func recordReplayMeasurements(t *testing.T, dims int, label string, outcomes []evalOutcome) {
	t.Helper()
	var searches, embeds, docs, local int
	timings := make([]time.Duration, 0, len(outcomes))
	for _, out := range outcomes {
		searches += out.vectorCalls
		embeds += out.embedCalls
		docs += out.retrievedDocCount
		if out.mode == "LOCAL" {
			local++
		}
		timings = append(timings, out.latency)
	}
	slices.Sort(timings)
	t.Logf("replay dims=%d policy=%s queries=%d local=%d global=%d retrieved_documents=%d embedding_calls=%d vector_searches=%d latency_p50=%s latency_p95=%s",
		dims, label, len(outcomes), local, len(outcomes)-local, docs, embeds, searches, timings[len(timings)/2], timings[(len(timings)-1)*95/100])
}

type evalScopedPreviews struct {
	visible map[string]struct{}
	all     map[string]string
}

func (p *evalScopedPreviews) DocumentPreviews(_ context.Context, ids []string) (map[string]string, error) {
	out := make(map[string]string, len(ids))
	for _, id := range ids {
		if _, ok := p.visible[id]; !ok {
			continue
		}
		if v, exists := p.all[id]; exists {
			out[id] = v
		}
	}
	return out, nil
}

func fitMarginOnDevSplit(t *testing.T, fix routingEvalFixture, dims int) (bestMargin float64, stats config.RoutingCalibrationStats) {
	t.Helper()
	cs := buildEvalCorpusStore(t, fix.Dev.Documents, fix.ChunkTokens, fix.ChunkOverlap, dims)
	legacyCfg := RoutingConfig{Policy: retriever.RoutingPolicyLegacy}

	var legacySingleRecall float64
	singleTotal := 0
	for _, q := range fix.Dev.Queries {
		if q.Slice == "single_doc" {
			singleTotal++
			out := runEvalQuery(t, cs, fix.LocalThreshold, fix.MaxChunks, legacyCfg, q.Query, "dev", nil, q.RequiredDocs, "")
			legacySingleRecall += out.allRequiredRecall
		}
	}
	legacySingleRecall /= float64(singleTotal)

	type devStepResult struct {
		margin float64
		rank   [3]float64
		stats  config.RoutingCalibrationStats
	}
	var candidates []devStepResult
	for step := 0; step <= 40; step++ {
		margin := float64(step) * 0.01
		cfg := RoutingConfig{Policy: retriever.RoutingPolicyEvidence, Margin: margin, Calibrated: true}
		var sRecall, sLocal, aRecall, aFalseLocal float64
		sCount, aCount := 0, 0
		for _, q := range fix.Dev.Queries {
			out := runEvalQuery(t, cs, fix.LocalThreshold, fix.MaxChunks, cfg, q.Query, "dev", nil, q.RequiredDocs, "")
			if q.Slice == "single_doc" {
				sCount++
				sRecall += out.allRequiredRecall
				if out.mode == "LOCAL" {
					sLocal++
				}
			} else {
				aCount++
				aRecall += out.allRequiredRecall
				if out.falseLocal {
					aFalseLocal++
				}
			}
		}
		sRecall /= float64(sCount)
		sLocal /= float64(sCount)
		aRecall /= float64(aCount)
		aFalseLocal /= float64(aCount)
		loss := legacySingleRecall - sRecall
		if loss > 0.02 {
			continue
		}
		rank := [3]float64{aRecall, -aFalseLocal, sLocal}
		stepStats := config.RoutingCalibrationStats{
			CorpusVersion:           fix.CorpusVersion,
			DocumentCount:           len(fix.Dev.Documents),
			QueryCount:              len(fix.Dev.Queries),
			SingleDocRecall:         sRecall,
			SingleDocRecallLoss:     loss,
			AmbiguousRecall:         aRecall,
			AmbiguousFalseLocalRate: aFalseLocal,
		}
		result := devStepResult{margin: math.Round(margin*100) / 100, rank: rank, stats: stepStats}
		if len(candidates) == 0 || slices.Compare(rank[:], candidates[0].rank[:]) > 0 {
			candidates = []devStepResult{result}
		} else if rank == candidates[0].rank {
			candidates = append(candidates, result)
		}
	}
	require.NotEmpty(t, candidates, "dev split must yield at least one valid margin")
	mid := candidates[len(candidates)/2]
	return mid.margin, mid.stats
}

// pairedInterval95 keeps near-duplicate questions and pairs sharing a required
// document in the same bootstrap sample.
func pairedInterval95(diffs []float64, requiredDocs [][]string) (mean, lower, upper float64) {
	parent := make([]int, len(diffs))
	root := func(i int) int {
		for parent[i] != i {
			i = parent[i]
		}
		return i
	}
	last := make(map[string]int)
	for i, docs := range requiredDocs {
		parent[i] = i
		for _, doc := range docs {
			if j, found := last[doc]; found {
				parent[root(j)] = root(i)
			}
			last[doc] = i
		}
	}

	type cluster struct {
		sum   float64
		count int
	}
	byRoot := make([]cluster, len(diffs))
	for i, diff := range diffs {
		group := &byRoot[root(i)]
		group.sum += diff
		group.count++
		mean += diff
	}
	mean /= float64(len(diffs))
	groups := slices.DeleteFunc(byRoot, func(c cluster) bool { return c.count == 0 })
	const replicates = 10000
	var drawInput [16]byte
	binary.LittleEndian.PutUint64(drawInput[:8], 163)
	samples := make([]float64, replicates)
	for i := range samples {
		var sum float64
		var count int
		for j := range groups {
			binary.LittleEndian.PutUint64(drawInput[8:], uint64(i*len(groups)+j))
			draw := sha256.Sum256(drawInput[:])
			group := groups[binary.LittleEndian.Uint64(draw[:8])%uint64(len(groups))]
			sum += group.sum
			count += group.count
		}
		samples[i] = sum / float64(count)
	}
	slices.Sort(samples)
	return mean, samples[replicates/40], samples[replicates-replicates/40-1]
}

// equivalentQuestions checks that both forms of a pair ask the same request.
// They may differ in case, width and accents, and by the declared
// single-character spelling edits, each of which must appear in the pair's text.
func equivalentQuestions(p routingEvalPair) error {
	words := func(question string) []string {
		return strings.Fields(foldLatinMarks(strings.ToLower(width.Fold.String(question))))
	}
	first, second := words(p.A.Query), words(p.B.Query)
	for _, edit := range p.SpellingEdits {
		if edit.From == edit.To || !editDistanceAtMostOne([]rune(edit.From), []rune(edit.To)) {
			return fmt.Errorf("%q to %q is not a one-character edit", edit.From, edit.To)
		}
		if !slices.Contains(second, edit.From) || !slices.Contains(first, edit.To) {
			return fmt.Errorf("edit %q to %q does not occur in the pair", edit.From, edit.To)
		}
		for i, word := range second {
			if word == edit.From {
				second[i] = edit.To
			}
		}
	}
	if !slices.Equal(first, second) {
		return fmt.Errorf("forms differ beyond declared edits: %q and %q", p.A.Query, p.B.Query)
	}
	return nil
}

func TestEquivalentQuestionsRequireDeclaredEdits(t *testing.T) {
	pair := func(a, b string, edits ...routingEvalSpellingEdit) routingEvalPair {
		return routingEvalPair{A: routingEvalQueryVariant{Query: a}, B: routingEvalQueryVariant{Query: b}, SpellingEdits: edits}
	}
	aurora := routingEvalSpellingEdit{From: "aurore", To: "aurora"}
	for _, tt := range []struct {
		name string
		pair routingEvalPair
		ok   bool
	}{
		{"case and width", pair("Is Aurora open?", "is ａｕｒｏｒａ open？"), true},
		{"accents", pair("Érable hours?", "erable hours?"), true},
		{"declared edit", pair("Is Aurora open?", "is aurore open?", aurora), true},
		{"undeclared edit", pair("Is Aurora open?", "is aurore open?"), false},
		{"edit absent from the second form", pair("Is Aurora open?", "is aurora open?", aurora), false},
		{"edit absent from the first form", pair("Is Aurore open?", "is aurore open?", aurora), false},
		{"edit of several characters", pair("Is Aurora open?", "is aur open?", routingEvalSpellingEdit{From: "aur", To: "aurora"}), false},
		{"different request", pair("Is Aurora open?", "is aurore closed?", aurora), false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.ok, equivalentQuestions(tt.pair) == nil)
		})
	}
}

func TestRoutingCalibrationAndHeldOutPairedEvaluation(t *testing.T) {
	fix := loadRoutingEvalFixture(t)

	// Verify dev and eval document IDs and entity spaces are completely disjoint.
	devIDs := make(map[string]struct{}, len(fix.Dev.Documents))
	for _, d := range fix.Dev.Documents {
		devIDs[d.ID] = struct{}{}
	}
	for _, d := range fix.Eval.Documents {
		require.NotContains(t, devIDs, d.ID, "dev and eval corpora must be split by document")
	}

	evalDocs := make(map[string]string, len(fix.Eval.Documents))
	for _, doc := range fix.Eval.Documents {
		evalDocs[doc.ID] = doc.Content
	}

	// Balance unique single-document targets against the multi-document ambiguity slice.
	var mainPairs, additionalPairs []routingEvalPair
	seenSingle := make(map[string]bool)
	singlePairs, ambiguousPairs := 0, 0
	for _, p := range fix.Eval.Pairs {
		require.NoError(t, equivalentQuestions(p), p.PairID)
		factDocs := make(map[string]bool)
		for _, fact := range p.RequiredFacts {
			require.NotEmpty(t, fact.Excerpt, p.PairID)
			require.Contains(t, evalDocs[fact.DocumentID], fact.Excerpt, p.PairID)
			factDocs[fact.DocumentID] = true
		}
		for _, id := range p.RequiredDocs {
			require.True(t, factDocs[id], p.PairID)
		}
		require.Len(t, factDocs, len(p.RequiredDocs), p.PairID)
		switch p.Slice {
		case "single_doc":
			require.Len(t, p.RequiredDocs, 1, p.PairID)
			if seenSingle[p.RequiredDocs[0]] {
				additionalPairs = append(additionalPairs, p)
				continue
			}
			seenSingle[p.RequiredDocs[0]] = true
			singlePairs++
		case "ambiguous":
			if len(p.RequiredDocs) == 1 {
				additionalPairs = append(additionalPairs, p)
				continue
			}
			ambiguousPairs++
		default:
			t.Fatalf("unexpected eval pair slice %q", p.Slice)
		}
		mainPairs = append(mainPairs, p)
	}
	require.Equal(t, singlePairs, ambiguousPairs, "held-out paired set must be balanced")
	require.GreaterOrEqual(t, ambiguousPairs, 12)

	// Verify Boreal Vault entity only appears past the 120-rune stored preview prefix.
	for _, d := range fix.Eval.Documents {
		if d.ID == "eval-boreal" {
			prefix := string([]rune(d.Content)[:120])
			require.NotContains(t, strings.ToLower(prefix), "boreal")
			require.Contains(t, strings.ToLower(d.Content), "boreal vault")
		}
	}

	for _, dims := range []int{256, 1024} {
		t.Run("dims="+strconv.Itoa(dims), func(t *testing.T) {
			fittedMargin, devStats := fitMarginOnDevSplit(t, fix, dims)
			t.Logf("dev split dims=%d fitted_margin=%.2f stats=%+v", dims, fittedMargin, devStats)

			profile, err := config.LookupRoutingProfile("", config.EmbeddingProfile{
				Provider:   "replay",
				Model:      "quarry-char3-bow-v1",
				Dimensions: dims,
			})
			require.NoError(t, err)
			require.NotNil(t, profile)
			require.Equal(t, fittedMargin, *profile.Margin, "published artifact margin must match dev-split fit")
			require.Equal(t, &devStats, profile.DevSplit, "published artifact stats must match dev-split fit")

			cs := buildEvalCorpusStore(t, fix.Eval.Documents, fix.ChunkTokens, fix.ChunkOverlap, dims)
			legacyCfg := RoutingConfig{Policy: retriever.RoutingPolicyLegacy}
			evidenceCfg := RoutingConfig{Policy: retriever.RoutingPolicyEvidence, Margin: *profile.Margin, Calibrated: true}

			var singleLegacyRecall, singleEvidRecall float64
			var singleCount int
			var ambLegacyRecall, ambEvidRecall float64
			var ambLegacyFalseLocal, ambEvidFalseLocal float64
			var ambQueryCount int
			var ambPairRecallDiffs []float64
			var ambPairFalseLocalDiffs []float64
			var ambPairDocs [][]string
			var missedAmbiguous []string
			var legacyOutcomes, evidenceOutcomes []evalOutcome
			var globalSingle, globalAmbiguous float64

			for _, pair := range mainPairs {
				for _, q := range []routingEvalQueryVariant{pair.A, pair.B} {
					control := runEvalQuery(t, cs, fix.LocalThreshold, fix.MaxChunks,
						RoutingConfig{Policy: retriever.RoutingPolicyEvidence}, q.Query, "eval", nil, pair.RequiredDocs, "")
					require.Equal(t, "GLOBAL", control.mode, q.ID)
					require.Equal(t, 1, control.embedCalls, q.ID)
					if pair.Slice == "single_doc" {
						globalSingle += control.allRequiredRecall
					} else {
						globalAmbiguous += control.allRequiredRecall
					}
				}
				if pair.Slice == "single_doc" {
					for _, q := range []routingEvalQueryVariant{pair.A, pair.B} {
						leg := runEvalQuery(t, cs, fix.LocalThreshold, fix.MaxChunks, legacyCfg, q.Query, "eval", nil, pair.RequiredDocs, "")
						evi := runEvalQuery(t, cs, fix.LocalThreshold, fix.MaxChunks, evidenceCfg, q.Query, "eval", nil, pair.RequiredDocs, "")
						require.Equal(t, 1, leg.embedCalls)
						require.Equal(t, 1, evi.embedCalls)
						require.LessOrEqual(t, evi.vectorCalls, leg.vectorCalls, q.ID)
						legacyOutcomes = append(legacyOutcomes, leg)
						evidenceOutcomes = append(evidenceOutcomes, evi)
						singleCount++
						singleLegacyRecall += leg.allRequiredRecall
						singleEvidRecall += evi.allRequiredRecall
					}
					continue
				}

				// Average the two phrasings before comparing policies for each pair.
				var pairLegRecall, pairEviRecall float64
				var pairLegFL, pairEviFL float64
				for _, q := range []routingEvalQueryVariant{pair.A, pair.B} {
					leg := runEvalQuery(t, cs, fix.LocalThreshold, fix.MaxChunks, legacyCfg, q.Query, "eval", nil, pair.RequiredDocs, "")
					evi := runEvalQuery(t, cs, fix.LocalThreshold, fix.MaxChunks, evidenceCfg, q.Query, "eval", nil, pair.RequiredDocs, "")
					require.Equal(t, 1, leg.embedCalls)
					require.Equal(t, 1, evi.embedCalls)
					require.LessOrEqual(t, evi.vectorCalls, leg.vectorCalls, q.ID)
					legacyOutcomes = append(legacyOutcomes, leg)
					evidenceOutcomes = append(evidenceOutcomes, evi)
					ambQueryCount++
					ambLegacyRecall += leg.allRequiredRecall
					ambEvidRecall += evi.allRequiredRecall
					if evi.allRequiredRecall == 0 {
						missedAmbiguous = append(missedAmbiguous, q.ID)
					}
					pairLegRecall += leg.allRequiredRecall / 2
					pairEviRecall += evi.allRequiredRecall / 2
					if leg.falseLocal {
						ambLegacyFalseLocal++
						pairLegFL += 0.5
					}
					if evi.falseLocal {
						ambEvidFalseLocal++
						pairEviFL += 0.5
					}
				}
				ambPairRecallDiffs = append(ambPairRecallDiffs, pairEviRecall-pairLegRecall)
				ambPairFalseLocalDiffs = append(ambPairFalseLocalDiffs, pairLegFL-pairEviFL)
				ambPairDocs = append(ambPairDocs, pair.RequiredDocs)
			}

			singleLegacyRate := singleLegacyRecall / float64(singleCount)
			singleEvidRate := singleEvidRecall / float64(singleCount)
			singleLoss := singleLegacyRate - singleEvidRate

			ambLegRecallRate := ambLegacyRecall / float64(ambQueryCount)
			ambEviRecallRate := ambEvidRecall / float64(ambQueryCount)
			ambLegFLRate := ambLegacyFalseLocal / float64(ambQueryCount)
			ambEviFLRate := ambEvidFalseLocal / float64(ambQueryCount)

			recallMean, recallLCL, recallUCL := pairedInterval95(ambPairRecallDiffs, ambPairDocs)
			flMean, flLCL, flUCL := pairedInterval95(ambPairFalseLocalDiffs, ambPairDocs)

			t.Logf("held-out dims=%d single_doc(n=%d): legacy=%.3f evidence=%.3f loss=%.3f",
				dims, singleCount, singleLegacyRate, singleEvidRate, singleLoss)
			t.Logf("held-out dims=%d ambiguous(n=%d,pairs=%d): recall legacy=%.3f evidence=%.3f gain=%.3f 95%%CI=[%.3f, %.3f]; false_local legacy=%.3f evidence=%.3f reduction=%.3f 95%%CI=[%.3f, %.3f]",
				dims, ambQueryCount, len(ambPairRecallDiffs),
				ambLegRecallRate, ambEviRecallRate, recallMean, recallLCL, recallUCL,
				ambLegFLRate, ambEviFLRate, flMean, flLCL, flUCL)
			recordReplayMeasurements(t, dims, "legacy", legacyOutcomes)
			recordReplayMeasurements(t, dims, "evidence", evidenceOutcomes)
			t.Logf("always-GLOBAL control dims=%d single=%.0f/%d ambiguous=%.0f/%d false_local=0", dims, globalSingle, singleCount, globalAmbiguous, ambQueryCount)
			t.Logf("ambiguous evidence misses dims=%d query_ids=%v", dims, missedAmbiguous)

			// A distractor leads the probe but the calibrated name resolves the route.
			chunks := append(routingFixtureChunks("north", 3, []float32{1, 0}),
				routingFixtureChunks("south", 3, []float32{.99, .1})...)
			names := map[string]string{"north": "Harbor Registry North.", "south": "Harbor Registry South."}
			const named = "Harbor Registry South manifest retention"
			namedRoute, _, _ := runRoutingFixture(t, evidenceCfg, names, chunks, named, "", 1)
			require.Equal(t, retriever.RouteReasonNamedEntity, namedRoute.RouteReason)
			require.Equal(t, []string{"south"}, candidateDocumentIDs(namedRoute.RetrievedChunks))
			controlRoute, _, _ := runRoutingFixture(t, RoutingConfig{Policy: retriever.RoutingPolicyEvidence}, names, chunks, named, "", 1)
			require.Equal(t, "GLOBAL", controlRoute.RetrievalMode)
			require.Equal(t, []string{"north"}, candidateDocumentIDs(controlRoute.RetrievedChunks))

			require.LessOrEqual(t, singleLoss, 0.02, "single-document recall loss exceeded 2pp")
			require.Greater(t, ambEviRecallRate, ambLegRecallRate, "ambiguous all-required-doc recall must improve")
			require.Less(t, ambEviFLRate, ambLegFLRate, "ambiguous false-LOCAL rate must improve")
			require.Greater(t, recallLCL, 0.0, "lower bound of 95%% paired CI on ambiguous recall gain must be positive")
			require.Greater(t, flLCL, 0.0, "lower bound of 95%% paired CI on false-LOCAL reduction must be positive")
			for _, pair := range additionalPairs {
				for _, q := range []routingEvalQueryVariant{pair.A, pair.B} {
					leg := runEvalQuery(t, cs, fix.LocalThreshold, fix.MaxChunks, legacyCfg, q.Query, "eval", nil, pair.RequiredDocs, "")
					evi := runEvalQuery(t, cs, fix.LocalThreshold, fix.MaxChunks, evidenceCfg, q.Query, "eval", nil, pair.RequiredDocs, "")
					require.Equal(t, 1, evi.embedCalls)
					require.GreaterOrEqual(t, evi.allRequiredRecall, leg.allRequiredRecall, q.ID)
				}
			}

			// Empty probes scored separately from verified negative entity matches.
			skippedScopes, emptyProbes := 0, 0
			for _, ep := range fix.Eval.EmptyProbes {
				out := runEvalQuery(t, cs, fix.LocalThreshold, fix.MaxChunks, evidenceCfg, ep.Query, ep.Collection, ep.AllowedDocumentIDs, nil, "")
				require.Equal(t, "GLOBAL", out.mode)
				require.False(t, out.negativeChecked, "empty probe must score separately from verified negative entity matches: %s", ep.ID)
				if ep.AllowedDocumentIDs != nil && len(ep.AllowedDocumentIDs) == 0 {
					require.Zero(t, out.embedCalls)
					require.Zero(t, out.vectorCalls)
					require.True(t, out.emptyScope, ep.ID)
					skippedScopes++
				} else {
					require.Equal(t, 1, out.embedCalls)
					require.Positive(t, out.vectorCalls)
					require.True(t, out.emptyProbe, ep.ID)
					emptyProbes++
				}
			}
			negEntityCount, negEntityPassed := 0, 0
			for _, neg := range fix.Eval.VerifiedNegativeEntity {
				negEntityCount++
				out := runEvalQuery(t, cs, fix.LocalThreshold, fix.MaxChunks, evidenceCfg, neg.Query, neg.Collection, nil, nil, "")
				require.Equal(t, 1, out.embedCalls)
				require.False(t, out.emptyProbe, neg.ID)
				require.True(t, out.negativeChecked, "verified negative entity match must score separately from empty probes: %s", neg.ID)
				negEntityPassed++
			}
			require.Equal(t, 1, skippedScopes)
			require.Equal(t, 2, emptyProbes)
			t.Logf("separate slices dims=%d: skipped_empty_scopes=%d executed_empty_probes=%d verified_negative_entities=%d/%d",
				dims, skippedScopes, emptyProbes, negEntityPassed, negEntityCount)

			// Explicit modes and scope isolation regression gate.
			for _, mode := range []string{"global", "local"} {
				out := runEvalQuery(t, cs, fix.LocalThreshold, fix.MaxChunks, evidenceCfg, "Harbor Registry", "eval", []string{"eval-aurora"}, []string{"eval-aurora"}, mode)
				require.Equal(t, 1, out.embedCalls)
				require.Equal(t, strings.ToUpper(mode), out.mode)
				if mode == "local" {
					require.Equal(t, 1.0, out.allRequiredRecall)
				}
			}
			// Stale/unauthorized candidate isolation: private collection document never enters results for collection="eval".
			iso := runEvalQuery(t, cs, fix.LocalThreshold, fix.MaxChunks, evidenceCfg, "Glacier Vault Spec Private Appendix", "eval", nil, []string{"eval-glacier"}, "")
			require.Equal(t, 1, iso.embedCalls)
			require.Equal(t, 1.0, iso.allRequiredRecall)
		})
	}
}

func TestReplayModelOutputIsFrozen(t *testing.T) {
	// Golden outputs are part of the replay model version. Any algorithm or
	// weight change requires a new version and a fresh development fit.
	for _, tc := range []struct {
		dim    int
		digest string
	}{
		{256, "5ed8ce911c2cac8c5caf6241a42c5110116651368890e6a246ab6baa011c5558"},
		{1024, "fde87ea01754f99d74d7998749bdfa19e102772b4513f44c96154451bcc9989b"},
	} {
		t.Run(strconv.Itoa(tc.dim), func(t *testing.T) {
			hash := sha256.New()
			emb := &replayEmbedder{dimensions: tc.dim}
			for _, input := range []string{
				"Regional retention schedule", "retention retention schedule",
				"Cedar Archive North", "¿Dónde queda Érable Central?",
				"東京 Metro service?", "Ｃｅｄａｒ Archiveの候補", "",
			} {
				vector, err := emb.Embed(t.Context(), input)
				require.NoError(t, err)
				for _, coordinate := range vector {
					var bits [4]byte
					binary.LittleEndian.PutUint32(bits[:], math.Float32bits(coordinate))
					_, _ = hash.Write(bits[:])
				}
			}
			require.Equal(t, tc.digest, fmt.Sprintf("%x", hash.Sum(nil)), "quarry-char3-bow-v1 output changed")
		})
	}
}
