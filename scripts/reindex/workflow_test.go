package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	pcstore "github.com/Victoroide/quarry-rag/internal/store/pinecone"
	pgstore "github.com/Victoroide/quarry-rag/internal/store/postgres"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

// stores serves PostgreSQL generation tables and named in-memory Pinecone indexes.
type stores struct {
	pg      *pgstore.Client
	indexes map[string]*pcstore.MemoryIndex
	opened  int
}

func (s *stores) index(name string) *pcstore.MemoryIndex {
	if s.indexes[name] == nil {
		s.indexes[name] = pcstore.NewMemoryIndex(testDims)
	}
	return s.indexes[name]
}

func (s *stores) open(ctx context.Context, spec targetSpec) (target vectorTarget, closeTarget func(), host string, openErr error) {
	s.opened++
	switch spec.Store {
	case "postgres":
		var store *pgstore.VectorStore
		var err error
		if spec.Create {
			store, err = pgstore.CreateGenerationVectorStore(ctx, s.pg, spec.Dimensions, spec.Generation)
		} else {
			store, err = pgstore.OpenGenerationVectorStore(ctx, s.pg, spec.Dimensions, spec.Generation)
		}
		return store, func() {}, "", err
	case "pinecone":
		return pineconeStore{pcstore.NewClientForIndex(s.index(spec.Target), testConfig(), spec.Target+"-host")}, func() {}, spec.Target + "-host", nil
	}
	return nil, nil, "", fmt.Errorf("unsupported store %q", spec.Store)
}

type harness struct {
	t      *testing.T
	f      *fixture
	stores *stores
	deps   deps
	dir    string
	embed  *countingEmbedder
}

// embedCalls reports how many provider calls the run made; a run that never needed a provider made none.
func (h *harness) embedCalls() int {
	if h.embed == nil {
		return 0
	}
	return int(h.embed.calls.Load())
}

func liveConfig(store string) *config.Config {
	cfg := testConfig()
	cfg.VectorStore = store
	cfg.PineconeIndexName = "quarry"
	cfg.EmbeddingProvider = domain.EmbeddingProviderOpenAI
	cfg.OpenAIEmbeddingModel = sourceModel
	cfg.EmbeddingModelRevision = "rev-1"
	cfg.ExtractionChunkVersion = "chunk-1"
	return cfg
}

// newHarness builds a tool environment whose live store is PostgreSQL or an in-memory Pinecone index.
func newHarness(t *testing.T, f *fixture, liveStore string) *harness {
	t.Helper()
	h := &harness{t: t, f: f, dir: t.TempDir()}
	h.stores = &stores{pg: f.pg, indexes: map[string]*pcstore.MemoryIndex{}}
	cfg := liveConfig(liveStore)
	h.deps = deps{cfg: cfg, pg: f.pg, open: h.stores.open, readProfile: readProfileFile,
		newEmbedder: func(_ context.Context, profile domain.EmbeddingProfile) (domain.EmbeddingProvider, error) {
			h.embed = &countingEmbedder{model: profile.Model}
			return h.embed, nil
		}}
	if liveStore == "pinecone" {
		live := h.stores.index("quarry")
		client := pcstore.NewClientForIndex(live, testConfig(), "")
		for _, d := range f.docs {
			if d.Status == "completed" {
				require.NoError(t, client.Upsert(t.Context(), "", sourceChunks(d, f.chunks[d.ID], sourceModel)))
			}
		}
	}
	store, target := cfg.VectorTarget()
	host := ""
	if store == "pinecone" {
		host = target + "-host"
	}
	require.NoError(t, f.pg.PinVectorGeneration(t.Context(), cfg.ActiveVectorGeneration(), store, target, host, cfg.EmbeddingSettings()))
	return h
}

func (h *harness) profileFile(name string, profile domain.EmbeddingProfile) string {
	h.t.Helper()
	data, err := json.Marshal(profile)
	require.NoError(h.t, err)
	path := filepath.Join(h.dir, name)
	require.NoError(h.t, os.WriteFile(path, data, 0o600))
	return path
}

func (h *harness) run(opts options) (map[string]any, error) {
	h.t.Helper()
	var out bytes.Buffer
	if opts.MaxDocuments == 0 {
		base := defaultOptions()
		base.Generation, base.Profile, base.TargetStore, base.TargetIndex = opts.Generation, opts.Profile, opts.TargetStore, opts.TargetIndex
		base.Apply, base.Verify, base.Freeze, base.Thaw, base.Activate, base.Attest = opts.Apply, opts.Verify, opts.Freeze, opts.Thaw, opts.Activate, opts.Attest
		base.MaxEmbedCalls, base.Quiesce, base.SampleSize, base.TopK, base.ScopeDocuments = opts.MaxEmbedCalls, opts.Quiesce, opts.SampleSize, opts.TopK, opts.ScopeDocuments
		base.MinRecall, base.MaxP95 = opts.MinRecall, opts.MaxP95
		base.QueryFixtures = opts.QueryFixtures
		base.After, base.PruneAfter, base.ConfirmTargetDrained = opts.After, opts.PruneAfter, opts.ConfirmTargetDrained
		opts = base
	}
	err := execute(h.t.Context(), h.deps, opts, &out, 10*time.Millisecond)
	var report map[string]any
	if out.Len() > 0 {
		require.NoError(h.t, json.Unmarshal(out.Bytes(), &report), out.String())
	}
	return report, err
}

func verifyOptions(generation string) options {
	return options{Generation: generation, Verify: true, SampleSize: 6, TopK: 5, ScopeDocuments: 3, MinRecall: 0.5, MaxP95: time.Minute}
}

func TestDefaultInventoryWritesNothingAndCallsNoProvider(t *testing.T) {
	f := newFixture(t)
	h := newHarness(t, f, "postgres")
	h.deps.newEmbedder = func(context.Context, domain.EmbeddingProfile) (domain.EmbeddingProvider, error) {
		t.Fatal("an inventory must not construct an embedding provider")
		return nil, nil
	}
	profile := h.profileFile("target.json", testProfile("model-b"))
	before := writeTableCounts(t, f.pg)
	state := relationalState(t, f.pg)

	report, err := h.run(options{Generation: "g2", Profile: profile})
	require.NoError(t, err)
	require.Equal(t, 0, h.stores.opened, "no vector store is opened or created")
	require.Equal(t, before, writeTableCounts(t, f.pg))
	require.Equal(t, state, relationalState(t, f.pg))
	require.EqualValues(t, 0, report["writes"])
	require.Equal(t, "inventory", report["action"])
	docs := report["documents"].(map[string]any)
	require.EqualValues(t, 4, docs["completed"])
	require.EqualValues(t, 1, docs["failed"])
	require.EqualValues(t, 1, docs["processing"])
	require.EqualValues(t, 4, docs["completed_without_original"], "re-extraction would need originals that are not stored")
	require.EqualValues(t, f.completedChunks(), report["chunks"])
	plan := report["plan"].(map[string]any)
	require.EqualValues(t, 4, plan["documents_to_stage"])
	require.EqualValues(t, f.completedChunks(), plan["embedding_calls_upper_bound"])
	copyReport := report["copy"].(map[string]any)
	require.Equal(t, false, copyReport["eligible"])
	require.Contains(t, copyReport["reason"], "unknown")
	require.Equal(t, "unregistered", report["target"].(map[string]any)["state"])
	require.EqualValues(t, 0, report["open_deletions"])
	_, hasOpenIDs := report["open_deletion_ids"]
	require.False(t, hasOpenIDs)
	require.Empty(t, report["blockers"])
}

func TestInventoryReportsTruncationOnlyWhenDocumentsRemain(t *testing.T) {
	pg := vectorDB(t)
	h := newHarness(t, &fixture{pg: pg}, "postgres")
	base := defaultOptions()
	base.Generation = "g2"
	base.Profile = h.profileFile("target.json", testProfile("model-b"))
	tests := []struct {
		name                   string
		documents, limit, page int
		truncated              bool
	}{
		{name: "empty", limit: 1, page: 100},
		{name: "one_at_limit", documents: 1, limit: 1, page: 100},
		{name: "more_remain", documents: 2, limit: 1, page: 100, truncated: true},
		{name: "below_limit", documents: 3, limit: 4, page: 2},
		{name: "page_above_limit", documents: 3, limit: 3, page: 100},
		{name: "exact_limit_over_pages", documents: 4, limit: 4, page: 2},
	}
	inserted := 0
	for _, tt := range tests {
		for inserted < tt.documents {
			insertDoc(t, pg, newDoc("manuals", "manual", "inventory chunk"))
			inserted++
		}
		t.Run(tt.name, func(t *testing.T) {
			caseHarness := *h
			caseHarness.t = t
			opts := base
			opts.MaxDocuments, opts.Page = tt.limit, tt.page
			report, err := caseHarness.run(opts)
			require.NoError(t, err)
			require.Equal(t, tt.truncated, report["truncated"])
			require.EqualValues(t, tt.documents, report["documents"].(map[string]any)["completed"])
			require.EqualValues(t, min(tt.documents, tt.limit), report["plan"].(map[string]any)["documents_to_stage"])
		})
	}
}

func TestInventoryTruncationIgnoresIneligibleTail(t *testing.T) {
	pg := vectorDB(t)
	h := newHarness(t, &fixture{pg: pg}, "postgres")
	for i, status := range []string{"completed", "failed", "processing", "deleting", "completed", "completed"} {
		doc := newDoc("manuals", "manual", "inventory chunk")
		doc.ID = fmt.Sprintf("00000000-0000-4000-8000-%012d", i+1)
		doc.Status = status
		insertDoc(t, pg, doc)
		if i >= 4 {
			held := i == 5
			_, err := pg.Pool.Exec(t.Context(), `INSERT INTO document_deletions
 (document_id,incarnation,kind,vector_mode,held,finalized) VALUES($1,$2,'quarantine','local',$3,$3)`, doc.ID, uuid.NewString(), held)
			require.NoError(t, err)
		}
	}

	opts := defaultOptions()
	opts.Generation = "g2"
	opts.Profile = h.profileFile("target.json", testProfile("model-b"))
	opts.MaxDocuments = 1
	report, err := h.run(opts)
	require.NoError(t, err)
	require.Equal(t, false, report["truncated"])
	require.EqualValues(t, 3, report["documents"].(map[string]any)["completed"])
	require.EqualValues(t, 1, report["plan"].(map[string]any)["documents_to_stage"])
}

type cancelInventoryProbe struct {
	cancel context.CancelFunc
}

func (tr cancelInventoryProbe) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if strings.HasPrefix(data.SQL, "SELECT EXISTS(SELECT 1 FROM documents d WHERE d.status = 'completed'") {
		tr.cancel()
	}
	return ctx
}

func (cancelInventoryProbe) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func TestInventoryProbeErrorWritesNoReport(t *testing.T) {
	pg := vectorDB(t)
	h := newHarness(t, &fixture{pg: pg}, "postgres")
	insertDoc(t, pg, newDoc("manuals", "manual", "inventory chunk"))

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	traced, err := pgstore.NewClient(ctx, pg.Pool.Config().ConnString(), pgstore.WithQueryTracer(cancelInventoryProbe{cancel: cancel}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, traced.Close(context.Background())) })
	h.deps.pg = traced

	opts := defaultOptions()
	opts.Generation = "g2"
	opts.Profile = h.profileFile("target.json", testProfile("model-b"))
	opts.MaxDocuments = 1
	var out bytes.Buffer
	err = execute(ctx, h.deps, opts, &out, 10*time.Millisecond)
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, out.String())
}

func TestInventoryReportsBlockers(t *testing.T) {
	f := newFixture(t)
	h := newHarness(t, f, "postgres")
	wide := testProfile(sourceModel)
	wide.Dimensions = 2001
	report, err := h.run(options{Generation: "g2", Profile: h.profileFile("wide.json", wide)})
	require.NoError(t, err)
	require.Contains(t, fmt.Sprint(report["blockers"]), "2000 dimensions")

	// The live target is protected even when named explicitly.
	report, err = h.run(options{Generation: "legacy", Profile: h.profileFile("same.json", testProfile(sourceModel))})
	require.NoError(t, err)
	require.Contains(t, fmt.Sprint(report["blockers"]), "protected")
	require.Contains(t, fmt.Sprint(report["blockers"]), "attested")

	// A different chunk version is a re-extraction, never a reindex.
	newText := testProfile("model-b")
	newText.ExtractionChunkVersion = "chunk-2"
	_, err = h.run(options{Attest: true, Profile: h.profileFile("attest.json", testProfile(sourceModel)), Apply: true})
	require.NoError(t, err)
	report, err = h.run(options{Generation: "g2", Profile: h.profileFile("text.json", newText)})
	require.NoError(t, err)
	require.Contains(t, fmt.Sprint(report["blockers"]), "re-extraction")

	// A Pinecone target needs an index name, and the live index may not be reused.
	report, err = h.run(options{Generation: "g3", Profile: h.profileFile("g3.json", testProfile("model-b")), TargetStore: "pinecone", TargetIndex: "quarry"})
	require.NoError(t, err)
	require.NotContains(t, fmt.Sprint(report["blockers"]), "live target", "the live store here is PostgreSQL")
}

func TestApplyAgainstTheLiveTargetIsRefused(t *testing.T) {
	f := newFixture(t)
	h := newHarness(t, f, "pinecone")
	profile := h.profileFile("live.json", testProfile(sourceModel))
	_, err := h.run(options{Generation: "g2", Profile: profile, TargetStore: "pinecone", TargetIndex: "quarry", Apply: true})
	require.ErrorContains(t, err, "protected")
	require.Equal(t, f.completedChunks(), h.stores.index("quarry").Len(), "the live index is untouched")
	complete, incomplete, countErr := f.pg.CountGenerationDocuments(t.Context(), "g2")
	require.NoError(t, countErr)
	require.Zero(t, complete+incomplete)
}

func TestAttestationUnlocksCopyingAndMustMatchTheConfiguration(t *testing.T) {
	f := newFixture(t)
	h := newHarness(t, f, "postgres")
	attested := h.profileFile("source.json", testProfile(sourceModel))

	// Without -apply the attestation is only reported.
	report, err := h.run(options{Attest: true, Profile: attested})
	require.NoError(t, err)
	require.Equal(t, false, report["applied"])
	row, err := f.pg.Generation(t.Context(), "legacy")
	require.NoError(t, err)
	require.Nil(t, row)

	// An attestation that contradicts the running configuration is refused.
	_, err = h.run(options{Attest: true, Apply: true, Profile: h.profileFile("wrong.json", testProfile("other-model"))})
	require.ErrorContains(t, err, "contradict")

	_, err = h.run(options{Attest: true, Apply: true, Profile: attested})
	require.NoError(t, err)
	row, err = f.pg.Generation(t.Context(), "legacy")
	require.NoError(t, err)
	require.Equal(t, pgstore.OriginAttested, row.Origin)
	require.Equal(t, pgstore.StateActive, row.State)
	_, err = h.run(options{Attest: true, Apply: true, Profile: h.profileFile("changed.json", func() domain.EmbeddingProfile { p := testProfile(sourceModel); p.ModelRevision = "rev-9"; return p }())})
	require.ErrorIs(t, err, pgstore.ErrGenerationDrift)

	report, err = h.run(options{Generation: "g2", Profile: h.profileFile("same.json", testProfile(sourceModel)), TargetStore: "pinecone", TargetIndex: "quarry-g2"})
	require.NoError(t, err)
	require.Equal(t, true, report["copy"].(map[string]any)["eligible"])
	require.Equal(t, "attested", report["source"].(map[string]any)["profile_origin"])
}

func TestAttestationIsRefusedOnceAGenerationIsActive(t *testing.T) {
	f := newFixture(t)
	h := newHarness(t, f, "postgres")
	_, err := f.pg.Pool.Exec(t.Context(), `UPDATE vector_state SET active_generation='g2'`)
	require.NoError(t, err)
	_, err = h.run(options{Attest: true, Apply: true, Profile: h.profileFile("source.json", testProfile(sourceModel))})
	require.ErrorContains(t, err, "legacy")
}

func TestStageCopiesWithoutProviderCallsAfterAttestation(t *testing.T) {
	f := newFixture(t)
	h := newHarness(t, f, "postgres")
	before := relationalState(t, f.pg)
	_, err := h.run(options{Attest: true, Apply: true, Profile: h.profileFile("source.json", testProfile(sourceModel))})
	require.NoError(t, err)
	report, err := h.run(options{Generation: "g2", Profile: h.profileFile("target.json", testProfile(sourceModel)), TargetStore: "pinecone", TargetIndex: "quarry-g2", Apply: true})
	require.NoError(t, err)
	require.EqualValues(t, f.completedChunks(), report["copied_chunks"])
	require.EqualValues(t, 0, report["embedded_chunks"])
	require.Equal(t, true, report["complete"])
	require.Zero(t, h.embedCalls())
	require.Equal(t, f.completedChunks(), h.stores.index("quarry-g2").Len())
	require.Equal(t, before, relationalState(t, f.pg))
	row, err := f.pg.Generation(t.Context(), "g2")
	require.NoError(t, err)
	require.Equal(t, pgstore.StateStaging, row.State)
	require.Equal(t, pgstore.OriginRecorded, row.Origin)
}

func TestStageWithAnUnknownProfileRecomputesWithinTheBudget(t *testing.T) {
	f := newFixture(t)
	h := newHarness(t, f, "postgres")
	profile := h.profileFile("target.json", testProfile(sourceModel))
	report, err := h.run(options{Generation: "g2", Profile: profile, TargetStore: "pinecone", TargetIndex: "quarry-g2", Apply: true})
	require.NoError(t, err)
	require.Equal(t, "embedding call budget", report["stopped"], "the source profile is unknown and model calls are opt in")
	require.Zero(t, h.stores.index("quarry-g2").Len())

	report, err = h.run(options{Generation: "g2", Profile: profile, TargetStore: "pinecone", TargetIndex: "quarry-g2", Apply: true, MaxEmbedCalls: 1000})
	require.NoError(t, err)
	require.Equal(t, true, report["complete"])
	require.Zero(t, report["copied_chunks"])
	require.EqualValues(t, f.completedChunks(), report["embedded_chunks"])
	require.Equal(t, f.completedChunks(), h.embedCalls())

	verify := verifyOptions("g2")
	preview, err := h.run(verify)
	require.ErrorIs(t, err, errNotVerified)
	sample, ok := preview["sample"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, true, sample["source_profile_query_only"])
	profileHash := testProfile(sourceModel).Hash()
	require.Equal(t, profileHash, sample["source_profile_hash"])
	fixtures := queryFixtures{SourceProfile: profileHash, TargetProfile: profileHash, Queries: map[string]queryVectors{}}
	for _, doc := range f.docs {
		if doc.Status != "completed" {
			continue
		}
		for _, chunk := range f.chunks[doc.ID] {
			fixtures.Queries[string(chunk.ID)] = queryVectors{
				TextHash: fmt.Sprintf("%x", sha256.Sum256([]byte(chunk.Text))),
				Source:   embedWith(sourceModel, chunk.Text),
				Target:   embedWith(sourceModel, chunk.Text),
			}
		}
	}
	verify.QueryFixtures = filepath.Join(h.dir, "queries.json")
	writeFixtures := func(input queryFixtures) {
		t.Helper()
		data, marshalErr := json.Marshal(input)
		require.NoError(t, marshalErr)
		require.NoError(t, os.WriteFile(verify.QueryFixtures, data, 0o600))
	}
	// A frozen verification may record approval only with the full sample.
	require.NoError(t, f.pg.FreezeWriters(t.Context(), "g2"))
	verify.Apply = true
	writeFixtures(queryFixtures{SourceProfile: profileHash, TargetProfile: profileHash})
	incomplete, err := h.run(verify)
	require.ErrorIs(t, err, errNotVerified)
	require.Equal(t, false, incomplete["recorded"])
	_, err = h.run(options{Generation: "g2", Activate: true, Apply: true})
	require.ErrorContains(t, err, "must be verified")
	writeFixtures(fixtures)
	verified, err := h.run(verify)
	require.NoError(t, err)
	require.Equal(t, true, verified["recorded"])
	require.Equal(t, h.embedCalls(), f.completedChunks())
	legacy, err := f.pg.Generation(t.Context(), domain.LegacyGeneration)
	require.NoError(t, err)
	require.Nil(t, legacy)
	activated, err := h.run(options{Generation: "g2", Activate: true, Apply: true})
	require.NoError(t, err)
	require.Equal(t, "g2", activated["active_generation"])
}

func TestStageRefusesDriftedGenerations(t *testing.T) {
	f := newFixture(t)
	h := newHarness(t, f, "postgres")
	first := h.profileFile("first.json", testProfile("model-b"))
	_, err := h.run(options{Generation: "g2", Profile: first, TargetStore: "pinecone", TargetIndex: "quarry-g2", Apply: true, MaxEmbedCalls: 1000})
	require.NoError(t, err)
	changed := testProfile("model-b")
	changed.ModelRevision = "rev-2"
	_, err = h.run(options{Generation: "g2", Profile: h.profileFile("changed.json", changed), TargetStore: "pinecone", TargetIndex: "quarry-g2", Apply: true, MaxEmbedCalls: 1000})
	require.ErrorContains(t, err, "different store, target or profile")
	_, err = h.run(options{Generation: "g2", Profile: first, TargetStore: "pinecone", TargetIndex: "elsewhere", Apply: true, MaxEmbedCalls: 1000})
	require.ErrorContains(t, err, "different store, target or profile")
	_, err = h.run(options{Generation: "g3", Profile: first, TargetStore: "pinecone", TargetIndex: "quarry-g2", Apply: true, MaxEmbedCalls: 1000})
	require.ErrorIs(t, err, pgstore.ErrGenerationDrift, "a target belongs to one generation")
}

func TestInventoryReportsOpenDeletionsWithoutBlockingStaging(t *testing.T) {
	f := newFixture(t)
	h := newHarness(t, f, "postgres")
	h.attestSource()

	deletingDoc := f.docs[0].ID
	_, err := f.pg.Pool.Exec(t.Context(), `UPDATE documents SET status='deleting' WHERE id=$1`, deletingDoc)
	require.NoError(t, err)
	_, err = f.pg.Pool.Exec(t.Context(), `INSERT INTO document_deletions(document_id,incarnation,kind,vector_mode) VALUES($1,$2,'qualified_intent','local')`, deletingDoc, uuid.NewString())
	require.NoError(t, err)

	retiredID := uuid.NewString()
	_, err = f.pg.Pool.Exec(t.Context(), `INSERT INTO document_deletions(document_id,incarnation,kind,vector_mode) VALUES($1,$2,'legacy_reservation','legacy')`, retiredID, uuid.NewString())
	require.NoError(t, err)

	profile := h.profileFile("target.json", testProfile(sourceModel))
	report, err := h.run(options{Generation: "g2", Profile: profile, TargetStore: "pinecone", TargetIndex: "quarry-g2"})
	require.NoError(t, err)
	require.Empty(t, report["blockers"])
	require.EqualValues(t, 2, report["open_deletions"])
	wantIDs := []string{deletingDoc, retiredID}
	slices.Sort(wantIDs)
	require.Equal(t, []any{wantIDs[0], wantIDs[1]}, report["open_deletion_ids"])

	stageReport, err := h.run(options{Generation: "g2", Profile: profile, TargetStore: "pinecone", TargetIndex: "quarry-g2", Apply: true})
	require.NoError(t, err)
	require.Equal(t, true, stageReport["complete"])
}
