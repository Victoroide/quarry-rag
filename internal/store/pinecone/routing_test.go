package pinecone_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/api/handler"
	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/pipeline"
	"github.com/Victoroide/quarry-rag/internal/retriever"
	pcstore "github.com/Victoroide/quarry-rag/internal/store/pinecone"
	pgstore "github.com/Victoroide/quarry-rag/internal/store/postgres"
	"github.com/Victoroide/quarry-rag/internal/testutil/pgtest"
	"github.com/google/uuid"
	"github.com/pinecone-io/go-pinecone/v5/pinecone"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestEvidenceRoutingCanonicalizesHostedDocumentAndChunkIDs(t *testing.T) {
	db := newEvidenceDatabase(t, pgtest.Start(t))
	const docID = "aaaaaaaa-2222-4444-8888-cccccccccccc"
	first := domain.Chunk{DocumentID: domain.DocumentID(docID), ID: domain.ChunkID(uuid.NewString()), Text: "Cedar Archive opens each morning."}
	second := domain.Chunk{DocumentID: first.DocumentID, ID: "bbbbbbbb-2222-4444-8888-dddddddddddd", Index: 1, Text: "Cedar Archive closes at night."}
	insertEvidenceDocument(t, db, first, "archive", "guide")
	require.NoError(t, db.InsertChunks(t.Context(), []domain.Chunk{second}))
	_, err := db.Pool.Exec(t.Context(), `UPDATE documents SET content_preview='Cedar Archive.' WHERE id=$1`, docID)
	require.NoError(t, err)
	var matches []*pinecone.ScoredVector
	for i, ch := range []domain.Chunk{first, second, second} {
		doc, id := string(ch.DocumentID), string(ch.ID)
		if i == 1 {
			doc, id = strings.ToUpper(doc), strings.ToUpper(id)
		}
		metadata, mdErr := structpb.NewStruct(map[string]any{"doc_id": doc, "text": ch.Text,
			"chunk_index": ch.Index, "collection": "archive", "doc_type": "guide"})
		require.NoError(t, mdErr)
		matches = append(matches, &pinecone.ScoredVector{Vector: &pinecone.Vector{Id: id, Metadata: metadata}, Score: .9})
	}

	store := pcstore.NewTestDriver(func(_ context.Context, _ *pinecone.QueryByVectorValuesRequest) (*pinecone.QueryVectorsResponse, error) {
		return &pinecone.QueryVectorsResponse{Matches: matches}, nil
	})
	ret := retriever.NewRetriever(store, canonicalRoutingEmbedder{}, .7, 6)
	vis := pgstore.Visibility{Pool: db.Pool, Collection: "archive", DocType: "guide"}
	qc := &pipeline.QueryContext{OriginalQuery: "Cedar Archive hours?", Collection: "archive",
		FilterDocType: "guide", EvidenceVisibility: vis, AllowedDocumentIDs: []string{docID}}
	cfg := pipeline.RoutingConfig{Policy: retriever.RoutingPolicyEvidence, Calibrated: true, Margin: .08}
	_, err = pipeline.RunRetrievalPipeline(t.Context(), qc, []pipeline.QueryStage{
		pipeline.NewRoutingStageWithConfig(ret, vis, nil, cfg), pipeline.NewRetrievalStageWithVisibility(ret, vis),
	})
	require.NoError(t, err)
	require.Len(t, retriever.AggregateDocumentScores(qc.Candidates), 1)
	require.Equal(t, retriever.RouteReasonNamedEntity, qc.RouteReason)
	require.Equal(t, docID, qc.LocalDocumentID)
	require.Len(t, qc.RetrievedChunks, 2)
	require.ElementsMatch(t, []domain.ChunkID{first.ID, second.ID},
		[]domain.ChunkID{qc.RetrievedChunks[0].Chunk.ID, qc.RetrievedChunks[1].Chunk.ID})
}

type canonicalRoutingEmbedder struct{ domain.EmbeddingProvider }

func (canonicalRoutingEmbedder) Embed(context.Context, string) ([]float32, error) {
	return []float32{1, 0, 0}, nil
}

func TestEvidenceRoutingPineconePartitionBudget(t *testing.T) {
	for _, size := range []int{10000, 10001} {
		t.Run(strconv.Itoa(size), func(t *testing.T) {
			assertEvidenceRoutingBudget(t, size)
		})
	}
}

func assertEvidenceRoutingBudget(t *testing.T, size int) {
	t.Helper()
	ids := make([]string, size)
	for i := range ids {
		ids[i] = uuid.NewString()
	}
	type row struct {
		doc, chunk string
		score      float32
	}
	var rows []row
	for _, dat := range []struct {
		doc   string
		score float32
	}{{ids[0], .90}, {ids[size-1], .89}, {ids[0], .88}, {ids[size-1], .87}, {ids[0], .86}, {ids[0], .85}} {
		rows = append(rows, row{dat.doc, uuid.NewString(), dat.score})
	}
	counts := map[retriever.RoutingPolicy]int{}
	for _, cfg := range []pipeline.RoutingConfig{{Policy: retriever.RoutingPolicyLegacy}, {Policy: retriever.RoutingPolicyEvidence, Margin: .08, Calibrated: true}} {
		var batchSizes []int
		var limits []uint32
		store := pcstore.NewTestDriver(func(_ context.Context, req *pinecone.QueryByVectorValuesRequest) (*pinecone.QueryVectorsResponse, error) {
			vals := req.MetadataFilter.GetFields()["doc_id"].GetStructValue().GetFields()["$in"].GetListValue().GetValues()
			batch := make([]string, len(vals))
			for i, v := range vals {
				batch[i] = v.GetStringValue()
			}
			batchSizes = append(batchSizes, len(batch))
			limits = append(limits, req.TopK)
			out := &pinecone.QueryVectorsResponse{}
			for i, e := range rows {
				if !slices.Contains(batch, e.doc) {
					continue
				}
				md, err := structpb.NewStruct(map[string]any{"doc_id": e.doc, "text": "Regional guidance", "chunk_index": i})
				require.NoError(t, err)
				out.Matches = append(out.Matches, &pinecone.ScoredVector{Score: e.score, Vector: &pinecone.Vector{Id: e.chunk, Metadata: md}})
				if len(out.Matches) == int(req.TopK) {
					break
				}
			}
			return out, nil
		})
		ret := retriever.NewRetriever(store, evidenceEmbedder{}, .7, 6)
		qc := &pipeline.QueryContext{OriginalQuery: "Archive?", AllowedDocumentIDs: ids, MaxChunks: 6}
		_, err := pipeline.RunRetrievalPipeline(t.Context(), qc, []pipeline.QueryStage{pipeline.NewDecomposeStage(), pipeline.NewRoutingStageWithConfig(ret, nil, nil, cfg), pipeline.NewRetrievalStage(ret)})
		require.NoError(t, err)
		counts[cfg.Policy] = len(batchSizes)
		if cfg.Policy == retriever.RoutingPolicyEvidence {
			require.Equal(t, "GLOBAL", qc.RetrievalMode)
			require.Len(t, qc.Candidates, 5)
			require.Len(t, qc.RetrievedChunks, 6)
			if size == 10000 {
				require.Equal(t, []int{10000}, batchSizes)
				require.Equal(t, []uint32{6}, limits)
			} else {
				require.Equal(t, []int{10000, 1}, batchSizes)
				require.Equal(t, []uint32{6, 6}, limits)
			}
		}
		t.Logf("policy=%s mode=%s RPCs=%d filter_sizes=%v", cfg.Policy, qc.RetrievalMode, len(batchSizes), batchSizes)
	}
	if counts[retriever.RoutingPolicyEvidence] > counts[retriever.RoutingPolicyLegacy] {
		t.Error("additional hosted calls")
	}
}

// hostedRow is one stored vector. key is the doc_id metadata exactly as the
// provider holds it, which can differ in case from the canonical document UUID.
type hostedRow struct {
	chunk domain.Chunk
	key   string
	score float32
}

// hostedIndex answers searches like the provider: it applies the requested
// doc_id and collection filters byte for byte, so a canonical UUID never
// matches a differently spelled stored key. Rows are listed in score order.
func hostedIndex(t *testing.T, rows []hostedRow, searches *[]*structpb.Struct) func(context.Context, *pinecone.QueryByVectorValuesRequest) (*pinecone.QueryVectorsResponse, error) {
	t.Helper()
	return func(_ context.Context, req *pinecone.QueryByVectorValuesRequest) (*pinecone.QueryVectorsResponse, error) {
		*searches = append(*searches, req.MetadataFilter)
		fields := req.MetadataFilter.GetFields()
		keys, scoped := searchedKeys(req.MetadataFilter)
		collection := fields["collection"].GetStructValue().GetFields()["$eq"].GetStringValue()
		response := &pinecone.QueryVectorsResponse{}
		for _, row := range rows {
			if scoped && !slices.Contains(keys, row.key) || collection != "" && collection != row.chunk.Metadata["collection"] {
				continue
			}
			md, err := structpb.NewStruct(map[string]any{"doc_id": row.key, "chunk_index": row.chunk.Index,
				"text": row.chunk.Text, "collection": row.chunk.Metadata["collection"], "doc_type": row.chunk.Metadata["doc_type"]})
			require.NoError(t, err)
			response.Matches = append(response.Matches, &pinecone.ScoredVector{Vector: &pinecone.Vector{Id: string(row.chunk.ID), Metadata: md}, Score: row.score})
			if len(response.Matches) == int(req.TopK) {
				break
			}
		}
		return response, nil
	}
}

func searchedKeys(filter *structpb.Struct) (keys []string, scoped bool) {
	in, scoped := filter.GetFields()["doc_id"]
	for _, v := range in.GetStructValue().GetFields()["$in"].GetListValue().GetValues() {
		keys = append(keys, v.GetStringValue())
	}
	return keys, scoped
}

func hostedChunk(doc string, index int, text string) domain.Chunk {
	return domain.Chunk{DocumentID: domain.DocumentID(doc), ID: domain.ChunkID(uuid.NewString()), Index: index, Text: text,
		Metadata: map[string]any{"collection": "archive", "doc_type": "guide"}}
}

func insertHostedChunks(t *testing.T, db *pgstore.Client, rows []hostedRow) {
	t.Helper()
	inserted := map[domain.DocumentID]bool{}
	for _, row := range rows {
		if inserted[row.chunk.DocumentID] {
			require.NoError(t, db.InsertChunks(t.Context(), []domain.Chunk{row.chunk}))
			continue
		}
		inserted[row.chunk.DocumentID] = true
		insertEvidenceDocument(t, db, row.chunk, "archive", "guide")
	}
}

func documentLabels(response handler.QueryResponse, names map[string]string) []string {
	labels := make([]string, 0, len(response.ScoredChunks))
	for _, sc := range response.ScoredChunks {
		labels = append(labels, names[strings.ToLower(sc.DocID)])
	}
	return labels
}

// TestHostedDocumentKeySpellingReachesTargetSearch proves that a selected
// document is searched with the spelling the provider stores. A canonical
// lowercase UUID matches nothing when the stored key has uppercase letters.
func TestHostedDocumentKeySpellingReachesTargetSearch(t *testing.T) {
	db := newEvidenceDatabase(t, pgtest.Start(t))
	const first, second = "aaaaaaaa-1111-4222-8333-444444444444", "bbbbbbbb-1111-4222-8333-444444444444"
	names := map[string]string{first: "A", second: "B"}
	chunks := []domain.Chunk{hostedChunk(first, 0, "Room 0 opens at ten."), hostedChunk(second, 0, "Room 1 opens at nine."),
		hostedChunk(second, 1, "Room 2 opens at eight."), hostedChunk(second, 2, "Room 3 opens at seven."),
		hostedChunk(second, 3, "Room 4 opens at six."), hostedChunk(first, 1, "Room 5 opens at noon."), hostedChunk(first, 2, "Room 6 opens at one.")}
	scores := []float32{.99, .95, .94, .93, .92, .73, .72}

	policies := map[string]*config.Config{
		"default policy": {MaxChunksPerQuery: 6},
		"evidence policy": {MaxChunksPerQuery: 6, RoutingPolicy: config.RoutingPolicyEvidence,
			RoutingCalibrationMatched: true, EffectiveRoutingMargin: .03},
	}
	var stored []hostedRow
	for i, ch := range chunks {
		stored = append(stored, hostedRow{chunk: ch, key: string(ch.DocumentID), score: scores[i]})
	}
	insertHostedChunks(t, db, stored)
	for _, upper := range []bool{false, true} {
		rows := slices.Clone(stored)
		for i := range rows {
			if upper && rows[i].key == first {
				rows[i].key = strings.ToUpper(first)
			}
		}
		key := rows[0].key
		for _, request := range []struct{ name, policy, mode string }{
			{"explicit local", "default policy", `"mode":"local",`},
			{"explicit local with evidence policy", "evidence policy", `"mode":"local",`},
			{"automatic local with default policy", "default policy", ``},
			{"automatic local", "evidence policy", ``},
		} {
			t.Run(fmt.Sprintf("uppercase=%t/%s", upper, request.name), func(t *testing.T) {
				var searches []*structpb.Struct
				store := pcstore.NewTestDriver(hostedIndex(t, rows, &searches))
				query := handler.QueryHandler(retriever.NewRetriever(store, evidenceEmbedder{}, .7, 6), nil, policies[request.policy], db.Pool)
				rec := httptest.NewRecorder()
				serveEvidenceQuery(t, query, rec, "application/json",
					`{"query":"Archive?","collection":"archive","answer_mode":"evidence",`+request.mode+`"max_chunks":3}`)
				require.Equal(t, 200, rec.Code, rec.Body.String())
				var response handler.QueryResponse
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
				require.Equal(t, "LOCAL", response.Mode)
				require.Equal(t, []string{"A", "A", "A"}, documentLabels(response, names))
				require.Len(t, searches, 2, "one scoped probe and one target search")
				keys, scoped := searchedKeys(searches[1])
				require.True(t, scoped)
				require.Equal(t, []string{key}, keys)
				for _, filter := range searches {
					require.Equal(t, "archive", filter.GetFields()["collection"].GetStructValue().GetFields()["$eq"].GetStringValue())
				}
			})
		}
	}
}

// TestHostedTargetSearchSendsEveryVerifiedSpelling covers one document whose
// chunks were stored under two spellings of its UUID.
func TestHostedTargetSearchSendsEveryVerifiedSpelling(t *testing.T) {
	db := newEvidenceDatabase(t, pgtest.Start(t))
	const first, second = "aaaaaaaa-1111-4222-8333-444444444444", "bbbbbbbb-1111-4222-8333-444444444444"
	upper := strings.ToUpper(first)
	rows := []hostedRow{
		{hostedChunk(first, 0, "Room 0 opens at ten."), upper, .99},
		{hostedChunk(first, 1, "Room 1 opens at noon."), first, .985},
		{hostedChunk(second, 0, "Room 2 opens at nine."), second, .95},
		{hostedChunk(second, 1, "Room 3 opens at eight."), second, .94},
		{hostedChunk(second, 2, "Room 4 opens at seven."), second, .93},
		{hostedChunk(first, 2, "Room 5 opens at one."), upper, .72},
	}
	insertHostedChunks(t, db, rows)
	var searches []*structpb.Struct
	store := pcstore.NewTestDriver(hostedIndex(t, rows, &searches))
	cfg := &config.Config{MaxChunksPerQuery: 6, RoutingPolicy: config.RoutingPolicyEvidence, RoutingCalibrationMatched: true, EffectiveRoutingMargin: .03}
	query := handler.QueryHandler(retriever.NewRetriever(store, evidenceEmbedder{}, .7, 6), nil, cfg, db.Pool)
	rec := httptest.NewRecorder()
	serveEvidenceQuery(t, query, rec, "application/json", `{"query":"Archive?","collection":"archive","answer_mode":"evidence","max_chunks":3}`)
	require.Equal(t, 200, rec.Code, rec.Body.String())
	var response handler.QueryResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
	require.Equal(t, "LOCAL", response.Mode)
	require.Equal(t, []string{"A", "A", "A"}, documentLabels(response, map[string]string{first: "A", second: "B"}))
	require.Len(t, searches, 2)
	keys, _ := searchedKeys(searches[1])
	require.ElementsMatch(t, []string{upper, first}, keys)
}

func TestHostedTargetSearchRejectsStaleDocumentAttribution(t *testing.T) {
	db := newEvidenceDatabase(t, pgtest.Start(t))
	const target, other = "aaaaaaaa-1111-4222-8333-444444444444", "bbbbbbbb-1111-4222-8333-444444444444"
	first := hostedChunk(target, 0, "Cedar Archive opens at ten.")
	first.ID = "aaaaaaaa-3333-4444-8888-cccccccccccc"
	rows := []hostedRow{
		{first, target, .99},
		{hostedChunk(other, 0, "Maple Station opens at nine."), other, .95},
		{hostedChunk(other, 1, "Lunch starts at noon."), other, .94},
		{hostedChunk(other, 2, "Dinner starts at six."), other, .93},
		{hostedChunk(target, 1, "Afternoon hours start at one."), target, .72},
		{hostedChunk(target, 2, "Evening hours start at five."), target, .71},
	}
	insertHostedChunks(t, db, rows)
	for id, preview := range map[string]string{target: "Cedar Archive.", other: "Maple Station."} {
		_, err := db.Pool.Exec(t.Context(), "UPDATE documents SET content_preview=$1 WHERE id=$2", preview, id)
		require.NoError(t, err)
	}
	alias := first
	alias.ID = domain.ChunkID(strings.ToUpper(string(first.ID)))
	stale := hostedRow{alias, other, .98}
	for _, tc := range []struct {
		name string
		rows []hostedRow
	}{
		{"current metadata", rows},
		{"stale attribution", slices.Insert(slices.Clone(rows), 1, stale)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var searches []*structpb.Struct
			store := pcstore.NewTestDriver(hostedIndex(t, tc.rows, &searches))
			cfg := &config.Config{MaxChunksPerQuery: 6, RoutingPolicy: config.RoutingPolicyEvidence,
				RoutingCalibrationMatched: true, EffectiveRoutingMargin: .03}
			query := handler.QueryHandler(retriever.NewRetriever(store, canonicalRoutingEmbedder{}, .7, 6), nil, cfg, db.Pool)
			rec := httptest.NewRecorder()
			serveEvidenceQuery(t, query, rec, "application/json",
				`{"query":"Cedar Archive hours?","collection":"archive","answer_mode":"evidence","max_chunks":3}`)
			require.Equal(t, 200, rec.Code, rec.Body.String())
			var response handler.QueryResponse
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
			require.Equal(t, "LOCAL", response.Mode)
			require.Len(t, searches, 2)
			keys, scoped := searchedKeys(searches[1])
			require.True(t, scoped)
			require.Equal(t, []string{target}, keys)
			require.Equal(t, []string{"A", "A", "A"}, documentLabels(response, map[string]string{target: "A", other: "B"}))
		})
	}
}

func TestEvidenceRoutingSharesHostedRefillBudget(t *testing.T) {
	db := newEvidenceDatabase(t, pgtest.Start(t))
	target, other := uuid.NewString(), uuid.NewString()
	first := hostedChunk(target, 0, "Cedar Archive opens at ten.")
	second := hostedChunk(target, 1, "Afternoon hours start at one.")
	third := hostedChunk(target, 2, "Evening hours start at five.")
	backup := hostedChunk(other, 0, "Maple Station opens at nine.")
	insertHostedChunks(t, db, []hostedRow{{first, target, .99}, {second, target, .88}, {third, target, .87}, {backup, other, .75}})
	_, err := db.Pool.Exec(t.Context(), "UPDATE documents SET content_preview='Cedar Archive.' WHERE id=$1", target)
	require.NoError(t, err)
	staleRows := func(count int) []hostedRow {
		var rows []hostedRow
		for i := range count {
			rows = append(rows, hostedRow{hostedChunk(target, i+3, "Expired opening hours."), target, .98 - float32(i)*.01})
		}
		return rows
	}
	currentLow := []hostedRow{{second, target, .88}, {third, target, .87}, {backup, other, .75}}
	for _, tc := range []struct {
		name, query, mode string
		rows              []hostedRow
		budget            int
		limits            []uint32
		keys              [][]string
		want              []string
	}{
		{"global reuses the probe then refills", "What are the opening hours? When do they close?", "GLOBAL",
			append(staleRows(5), currentLow...), 1, []uint32{5, 10}, [][]string{nil, nil}, []string{"A", "A", "B"}},
		{"global without an extra request", "What are the opening hours? When do they close?", "GLOBAL",
			append(staleRows(5), currentLow...), 0, []uint32{5}, [][]string{nil}, []string{}},
		{"local refills the selected document", "Cedar Archive hours?", "LOCAL",
			append(append([]hostedRow{{first, target, .99}}, staleRows(3)...), currentLow...),
			1, []uint32{5, 3, 6}, [][]string{nil, {target}, {target}}, []string{"A", "A", "A"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var searches []*structpb.Struct
			var limits []uint32
			index := hostedIndex(t, tc.rows, &searches)
			store := pcstore.NewTestDriver(func(ctx context.Context, req *pinecone.QueryByVectorValuesRequest) (*pinecone.QueryVectorsResponse, error) {
				limits = append(limits, req.TopK)
				return index(ctx, req)
			})
			cfg := &config.Config{MaxChunksPerQuery: 6, RoutingPolicy: config.RoutingPolicyEvidence,
				RoutingCalibrationMatched: true, EffectiveRoutingMargin: .03,
				HostedVisibilityRefill: true, HostedVisibilityExtraRequests: tc.budget}
			query := handler.QueryHandler(retriever.NewRetriever(store, canonicalRoutingEmbedder{}, .7, 6), nil, cfg, db.Pool)
			rec := httptest.NewRecorder()
			serveEvidenceQuery(t, query, rec, "application/json", fmt.Sprintf(
				`{"query":%q,"collection":"archive","answer_mode":"evidence","max_chunks":3}`, tc.query))
			require.Equal(t, 200, rec.Code, rec.Body.String())
			var response handler.QueryResponse
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
			require.Equal(t, tc.mode, response.Mode)
			require.Equal(t, tc.want, documentLabels(response, map[string]string{target: "A", other: "B"}))
			require.Equal(t, tc.limits, limits)
			for i, filter := range searches {
				keys, scoped := searchedKeys(filter)
				require.Equal(t, tc.keys[i], keys)
				require.Equal(t, tc.keys[i] != nil, scoped)
				require.Equal(t, "archive", filter.GetFields()["collection"].GetStructValue().GetFields()["$eq"].GetStringValue())
			}
		})
	}
}

// TestEvidenceCachedGlobalChecksEveryFetchedHit proves that the cached scope
// search is verified in score order before K is applied, so a stale or aliased
// leading hit cannot hide a current chunk that was already fetched.
func TestEvidenceCachedGlobalChecksEveryFetchedHit(t *testing.T) {
	db := newEvidenceDatabase(t, pgtest.Start(t))
	docA, docB := uuid.NewString(), uuid.NewString()
	a := hostedChunk(docA, 0, "The reading room opens at ten.")
	a.ID = "aaaaaaaa-1111-4222-8333-444444444444"
	b := hostedChunk(docB, 0, "The local gallery opens at nine.")
	for _, ch := range []domain.Chunk{a, b} {
		insertEvidenceDocument(t, db, ch, "archive", "guide")
	}
	hit := func(ch domain.Chunk, id, text string, score float32) hostedRow {
		ch.ID, ch.Text = domain.ChunkID(id), text
		return hostedRow{chunk: ch, key: string(ch.DocumentID), score: score}
	}
	staleA := hit(a, string(a.ID), "Earlier reading room hours.", .99)
	for _, tc := range []struct {
		name string
		k    int
		rows []hostedRow
		want []string
	}{
		{"ordered control", 1, []hostedRow{hit(a, string(a.ID), a.Text, .99), hit(b, string(b.ID), b.Text, .97)}, []string{docA}},
		{"stale then current", 1, []hostedRow{staleA, hit(b, string(b.ID), b.Text, .97)}, []string{docB}},
		{"two distinct control", 2, []hostedRow{hit(a, string(a.ID), a.Text, .99), hit(b, string(b.ID), b.Text, .97)}, []string{docA, docB}},
		{"aliases then distinct", 2, []hostedRow{hit(a, string(a.ID), a.Text, .99), hit(a, strings.ToUpper(string(a.ID)), a.Text, .98),
			hit(b, string(b.ID), b.Text, .97)}, []string{docA, docB}},
		{"stale alias then distinct", 2, []hostedRow{staleA, hit(a, strings.ToUpper(string(a.ID)), a.Text, .98),
			hit(b, string(b.ID), b.Text, .97)}, []string{docA, docB}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var searches []*structpb.Struct
			rows := tc.rows
			store := pcstore.NewTestDriver(func(ctx context.Context, req *pinecone.QueryByVectorValuesRequest) (*pinecone.QueryVectorsResponse, error) {
				require.Equal(t, max(5, tc.k), int(req.TopK))
				return hostedIndex(t, rows, &searches)(ctx, req)
			})
			cfg := &config.Config{MaxChunksPerQuery: 6, RoutingPolicy: config.RoutingPolicyEvidence, RoutingCalibrationMatched: true, EffectiveRoutingMargin: .08}
			query := handler.QueryHandler(retriever.NewRetriever(store, canonicalRoutingEmbedder{}, .7, 6), nil, cfg, db.Pool)
			rec := httptest.NewRecorder()
			serveEvidenceQuery(t, query, rec, "application/json", fmt.Sprintf(
				`{"query":"When do the galleries open? Which rooms are open?","collection":"archive","answer_mode":"evidence","max_chunks":%d}`, tc.k))
			require.Equal(t, 200, rec.Code, rec.Body.String())
			var response handler.QueryResponse
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
			require.Equal(t, "GLOBAL", response.Mode)
			var got []string
			for _, sc := range response.ScoredChunks {
				got = append(got, sc.DocID)
			}
			require.Equal(t, tc.want, got)
			require.Len(t, searches, 1, "the cached scope search serves routing and retrieval")
		})
	}
}

// TestHostedTargetFallbackKeepsVerifiedChunks covers a target search whose
// stale hit forces a recheck of rows it already verified. Without allowance for
// a wider search the recheck must still accept them, whichever way the provider
// spells the stored key.
func TestHostedTargetFallbackKeepsVerifiedChunks(t *testing.T) {
	db := newEvidenceDatabase(t, pgtest.Start(t))
	const target, other = "aaaaaaaa-1111-4222-8333-444444444444", "bbbbbbbb-1111-4222-8333-444444444444"
	rows := []hostedRow{
		{hostedChunk(target, 0, "Cedar Archive opens at ten."), target, .99},
		{hostedChunk(other, 0, "Maple Station opens at noon."), other, .985},
		{hostedChunk(other, 1, "Room two closes at eleven."), other, .98},
		{hostedChunk(other, 2, "Room three opens at twelve."), other, .975},
		{hostedChunk(other, 3, "Room four opens at seven."), other, .97},
		{hostedChunk(target, 1, "The reading room opens at eight."), target, .95},
		{hostedChunk(target, 2, "The desk opens at nine."), target, .94},
	}
	insertHostedChunks(t, db, rows)
	for id, preview := range map[string]string{target: "Cedar Archive.", other: "Maple Station."} {
		_, err := db.Pool.Exec(t.Context(), "UPDATE documents SET content_preview=$1 WHERE id=$2", preview, id)
		require.NoError(t, err)
	}
	// The stale hit is absent from PostgreSQL, so verification removes it.
	stale := hostedRow{hostedChunk(target, 3, "Superseded hours."), target, .96}
	for _, tc := range []struct {
		name     string
		upper    bool
		stale    bool
		budget   int
		fail     bool
		want     []string
		searches int
	}{
		{"lowercase key, stale hit, no allowance", false, true, 0, false, []string{"A", "A", "B"}, 2},
		{"uppercase key, stale hit, no allowance", true, true, 0, false, []string{"A", "A", "B"}, 2},
		{"uppercase key, stale hit, failed wider search", true, true, 1, true, []string{"A", "A", "B"}, 3},
		{"uppercase key, stale hit, wider search", true, true, 1, false, []string{"A", "A", "A"}, 3},
		{"uppercase key, no stale hit", true, false, 0, false, []string{"A", "A", "A"}, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := slices.Clone(rows)
			if tc.stale {
				fixture = slices.Insert(fixture, 5, stale)
			}
			targetKey := target
			if tc.upper {
				targetKey = strings.ToUpper(target)
				for i := range fixture {
					if fixture[i].key == target {
						fixture[i].key = targetKey
					}
				}
			}
			var searches []*structpb.Struct
			cfg := &config.Config{MaxChunksPerQuery: 6, RoutingPolicy: config.RoutingPolicyEvidence, RoutingCalibrationMatched: true,
				EffectiveRoutingMargin: .03, HostedVisibilityRefill: true, HostedVisibilityExtraRequests: tc.budget}
			index := hostedIndex(t, fixture, &searches)
			store := pcstore.NewTestDriver(func(ctx context.Context, req *pinecone.QueryByVectorValuesRequest) (*pinecone.QueryVectorsResponse, error) {
				if tc.fail && len(searches) == 2 {
					searches = append(searches, req.MetadataFilter)
					return nil, errors.New("hosted search unavailable")
				}
				return index(ctx, req)
			})
			query := handler.QueryHandler(retriever.NewRetriever(store, canonicalRoutingEmbedder{}, .7, 6), nil, cfg, db.Pool)
			rec := httptest.NewRecorder()
			serveEvidenceQuery(t, query, rec, "application/json",
				`{"query":"Cedar Archive hours?","collection":"archive","filter_doc_type":"guide","answer_mode":"evidence","max_chunks":3}`)
			require.Equal(t, 200, rec.Code, rec.Body.String())
			var response handler.QueryResponse
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
			require.Equal(t, "LOCAL", response.Mode)
			require.Len(t, searches, tc.searches)
			for _, search := range searches[1:] {
				keys, scoped := searchedKeys(search)
				require.True(t, scoped)
				require.Equal(t, []string{targetKey}, keys)
			}
			require.Equal(t, tc.want, documentLabels(response, map[string]string{target: "A", other: "B"}))
		})
	}
}
