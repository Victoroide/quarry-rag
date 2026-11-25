package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	pcstore "github.com/Victoroide/quarry-rag/internal/store/pinecone"
	pgstore "github.com/Victoroide/quarry-rag/internal/store/postgres"
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

func (s *stores) open(ctx context.Context, spec targetSpec) (vectorTarget, func(), error) {
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
		return store, func() {}, err
	case "pinecone":
		return pineconeStore{pcstore.NewClientForIndex(s.index(spec.Target), testConfig())}, func() {}, nil
	}
	return nil, nil, fmt.Errorf("unsupported store %q", spec.Store)
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
	cfg.EmbeddingProvider = domain.EmbeddingProviderBedrock
	cfg.BedrockEmbedModel = sourceModel
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
		client := pcstore.NewClientForIndex(live, testConfig())
		for _, d := range f.docs {
			if d.Status == "completed" {
				require.NoError(t, client.Upsert(t.Context(), "", sourceChunks(d, f.chunks[d.ID], sourceModel)))
			}
		}
	}
	f.pg.SetVectorGeneration(cfg.ActiveVectorGeneration())
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
		base.Apply, base.Attest, base.MaxEmbedCalls = opts.Apply, opts.Attest, opts.MaxEmbedCalls
		opts = base
	}
	err := execute(h.t.Context(), h.deps, opts, &out)
	var report map[string]any
	if out.Len() > 0 {
		require.NoError(h.t, json.Unmarshal(out.Bytes(), &report), out.String())
	}
	return report, err
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
	require.Empty(t, report["blockers"])
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
	require.EqualValues(t, f.completedChunks(), report["embedded_chunks"])
	require.Equal(t, f.completedChunks(), h.embedCalls())
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
