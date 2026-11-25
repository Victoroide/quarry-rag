package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	pg "github.com/Victoroide/quarry-rag/internal/store/postgres"
	"github.com/stretchr/testify/require"
)

type snapshotCorpus struct {
	snapshot pg.DocumentSnapshot
}

func (r snapshotCorpus) DocumentRevisions(_ context.Context, after string, limit int) ([]pg.DocumentRevision, error) {
	if after != "" || limit == 0 {
		return nil, nil
	}
	return []pg.DocumentRevision{r.snapshot.DocumentRevision}, nil
}
func (r snapshotCorpus) HasDocumentsAfter(context.Context, string) (bool, error) {
	return false, nil
}
func (r snapshotCorpus) LoadDocumentSnapshotBounded(context.Context, string, int, int64) (*pg.DocumentSnapshot, error) {
	return &r.snapshot, nil
}

type currentCheckpointLedger struct {
	ledger
	doc, revision, incarnation string
}

func (r currentCheckpointLedger) GenerationCheckpoints(context.Context, string, []string) (map[string]pg.Checkpoint, error) {
	return map[string]pg.Checkpoint{r.doc: {Complete: true, SourceRevision: r.revision, Incarnation: r.incarnation}}, nil
}

func TestCutoverRequiresDeclaredSamplesAndCurrentHits(t *testing.T) {
	doc := newDoc("public", "report", "one stored chunk")
	chunks := []domain.Chunk{{ID: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", DocumentID: domain.DocumentID(doc.ID), Index: 0, TokenCount: 3, Text: doc.Texts[0]}}
	chunks = sourceChunks(doc, chunks, sourceModel)
	snapshot := pg.DocumentSnapshot{DocumentRevision: pg.DocumentRevision{ID: doc.ID, Incarnation: doc.ID, DocType: doc.DocType, Collection: doc.Collection, DeclaredChunks: 1, Chunks: 1, TextBytes: int64(len(chunks[0].Text)), Revision: "revision-one"}, Chunks: chunks}
	for _, tc := range []struct {
		name      string
		requested int
		drift     string
		wantPass  bool
	}{
		{"one declared and measured", 1, "", true},
		{"twenty declared and one measured", 20, "", false},
		{"scoped search returns deleted key", 1, "key", false},
		{"scoped search mislabels an expected key", 1, "collection", false},
		{"empty rankings", 1, "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			src, _ := memoryPinecone()
			dst, _ := memoryPinecone()
			for _, store := range []pineconeStore{{src}, {dst}} {
				if err := store.Upsert(t.Context(), "", chunks); err != nil {
					t.Fatal(err)
				}
			}
			var target vectorReader = dst
			if tc.drift != "" {
				target = scriptedReader{vectorReader: dst, results: func(v []float32, f domain.VectorFilter) []domain.ScoredChunk {
					h, err := dst.QueryFiltered(t.Context(), v, 2, f)
					if err != nil {
						t.Fatal(err)
					}
					stale := h[0]
					if tc.drift == "key" {
						stale.Chunk.ID = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
						stale.Chunk.Text = "old deleted excerpt"
					} else {
						stale.Chunk.Metadata = map[string]any{"collection": "another-collection"}
					}
					return append(h, stale)
				}}
			}
			opts := defaultOptions()
			opts.SampleSize, opts.ScopeDocuments, opts.MinRecall, opts.MaxP95, opts.TopK = tc.requested, 1, 1, time.Second, 2
			var source vectorReader = src
			if tc.name == "empty rankings" {
				empty := func([]float32, domain.VectorFilter) []domain.ScoredChunk { return nil }
				source = scriptedReader{vectorReader: src, results: empty}
				target = scriptedReader{vectorReader: dst, results: empty}
			}
			v := verifier{corpus: snapshotCorpus{snapshot}, ledger: currentCheckpointLedger{doc: doc.ID, revision: "revision-one", incarnation: doc.ID}, target: target, source: source, generation: "g2", profile: testProfile(sourceModel), sourceProfile: testProfile(sourceModel), opts: opts, now: time.Now}
			rep, err := v.run(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			require.Equal(t, tc.wantPass, rep.Passed, "%v", rep.Failures)
			if tc.drift != "" {
				require.Equal(t, 1, rep.Sample.ForbiddenHits)
				require.Equal(t, 1.0, rep.Sample.MeanRecall)
				require.Equal(t, 1.0, rep.Coverage.KeyCoverage)
				require.Zero(t, rep.Unexpected.Records)
			}
		})
	}
}

func TestStagingBedrockUsesOneAttemptPerChargedCall(t *testing.T) {
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = io.WriteString(w, `{"message":"retryable local response"}`)
	}))
	defer srv.Close()
	t.Setenv("AWS_ENDPOINT_URL", srv.URL)
	t.Setenv("AWS_ENDPOINT_URL_BEDROCK_RUNTIME", srv.URL)
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	cfg := &config.Config{EmbeddingProvider: "bedrock", BedrockEmbedModel: "amazon.titan-embed-text-v2:0", AWSRegion: "us-east-1", AWSAccessKeyID: "fixture", AWSSecretKey: "fixture", AWSMaxAttempts: 2, EmbedConcurrency: 1, EmbedTimeout: 12 * time.Second, EmbeddingDimensions: 256, ExtractionChunkVersion: "sample"}
	profile := cfg.EmbeddingSettings()
	e, err := newDeps(cfg, nil).newEmbedder(t.Context(), profile)
	if err != nil {
		t.Fatal(err)
	}
	s := stager{profile: profile, opts: options{MaxEmbedCalls: 1}, embed: e}
	_, _, _, err = s.vectors(t.Context(), &pg.DocumentSnapshot{Chunks: []domain.Chunk{{ID: "a", Text: "one eligible chunk"}}})
	if err == nil {
		t.Fatal("expected a provider error")
	}
	t.Logf("budget=%d counted=%d received=%d", s.opts.MaxEmbedCalls, s.calls, attempts.Load())
	if attempts.Load() != 1 {
		t.Fatalf("provider received %d requests on a one-call budget", attempts.Load())
	}
}
