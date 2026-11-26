package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/chunker"
	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/llm"
	"github.com/Victoroide/quarry-rag/internal/pipeline"
	"github.com/Victoroide/quarry-rag/internal/retriever"
	pcstore "github.com/Victoroide/quarry-rag/internal/store/pinecone"
	"github.com/Victoroide/quarry-rag/internal/testutil/pgtest"
	"github.com/Victoroide/quarry-rag/internal/worker"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

const manifestQueryText = "Compare the archive retention policies."

type manifestQueryEmbedder struct{}

func (manifestQueryEmbedder) Embed(_ context.Context, text string) ([]float32, error) {
	if text == manifestQueryText {
		return []float32{1, 0}, nil
	}
	return []float32{1, 1}, nil
}

type manifestQueryGenerator struct {
	llm.Generator
	calls int
}

func (g *manifestQueryGenerator) Generate(context.Context, llm.Request) (*llm.Response, error) {
	g.calls++
	return &llm.Response{Text: "The archive has three retention policies [1] [2] [3].", StopReason: llm.StopEndTurn}, nil
}

type manifestQueryVisibility struct {
	Visibility
	previews map[string]string
}

func (v *manifestQueryVisibility) VisibleEvidenceWithPreviews(ctx context.Context, hits []domain.ScoredChunk, ids []string) ([]domain.ScoredChunk, map[string]string, error) {
	current, previews, err := v.Visibility.VisibleEvidenceWithPreviews(ctx, hits, ids)
	v.previews = previews
	return current, previews, err
}

type manifestQueryVectors interface {
	domain.VectorStore
	domain.VectorDeleter
	domain.GrantScopedVectorStore
}

type manifestQueryPublication struct {
	incarnation, revision, text string
	scoped                      bool
}

func publishQueryDocument(t *testing.T, c *Client, vectors manifestQueryVectors, req domain.AcceptRequest, accept, manifest bool) manifestQueryPublication {
	t.Helper()
	if accept {
		result, err := c.AcceptDocument(t.Context(), req)
		require.NoError(t, err)
		require.Equal(t, domain.AcceptAccepted, result.Outcome)
	}
	w := worker.New(c, []pipeline.Stage{
		pipeline.NewChunkStage(chunker.NewChunker(32, 4)),
		pipeline.NewEmbedStage(manifestQueryEmbedder{}, 1),
		pipeline.NewPersistStage(vectors),
	}, vectors, worker.Options{Owner: "manifest-query", Workers: 1, PollInterval: 10 * time.Millisecond, HostedVectorManifest: manifest})
	w.Start(t.Context())
	defer w.Shutdown()
	require.Eventually(t, func() bool {
		var status string
		err := c.Pool.QueryRow(t.Context(), `SELECT status FROM documents WHERE id=$1`, req.Document.ID).Scan(&status)
		return err == nil && status == "completed"
	}, 10*time.Second, 10*time.Millisecond)

	pub := manifestQueryPublication{text: req.Document.Content}
	require.NoError(t, c.Pool.QueryRow(t.Context(), `
SELECT d.incarnation::text, d.scoped_vectors, COALESCE(r.active_revision_id::text,'')
FROM documents d LEFT JOIN hosted_vector_revisions r ON r.document_id=d.id
WHERE d.id=$1`, req.Document.ID).Scan(&pub.incarnation, &pub.scoped, &pub.revision))
	return pub
}

func runManifestCorpusQuery(t *testing.T, c *Client, vectors manifestQueryVectors, subject *domain.Principal, mode string, refill bool, ids []string, want map[domain.DocumentID]manifestQueryPublication) {
	t.Helper()
	ctx := t.Context()
	visible := &manifestQueryVisibility{Visibility: Visibility{Pool: c.Pool, Subject: subject}}
	previews := Previews{Pool: c.Pool, Subject: subject}
	qc := &pipeline.QueryContext{
		OriginalQuery: manifestQueryText, RequestedMode: mode, MaxChunks: 10,
		IncludeCitations: true, AnswerMode: domain.AnswerModeGenerated,
		HostedVisibilityRefill: refill, HostedVisibilityExtraRequests: 1,
		EvidenceVisibility: visible, AllowedDocumentIDs: ids,
	}
	if subject != nil {
		reader := GrantReader{Pool: c.Pool}
		req := domain.ReadScopeRequest{DocumentIDs: ids}
		if vectors.GrantScope() == domain.GrantScopeMembership {
			req.Membership = qc.MaxChunks
		}
		scope, err := reader.DeriveReadScope(ctx, *subject, req)
		require.NoError(t, err)
		auth := domain.AuthorizedScope{TenantID: subject.TenantID, PrincipalID: subject.ID}
		if req.Membership != 0 {
			require.False(t, scope.Exceeded)
			auth.Incarnations = make(map[string]string, len(scope.Documents))
			qc.AllowedDocumentIDs = make([]string, 0, len(scope.Documents))
			for _, doc := range scope.Documents {
				auth.Incarnations[doc.ID] = doc.Incarnation
				qc.AllowedDocumentIDs = append(qc.AllowedDocumentIDs, doc.ID)
			}
		}
		qc.Authorization = pipeline.NewReadAuthorization(*subject, time.Now().Add(time.Minute), reader, auth, scope.Revision)
	}
	cfg := &config.Config{BedrockFallbackModel: "fixture", RoutingPolicy: config.RoutingPolicyEvidence}
	amb := llm.NewAmbassador(cfg)
	gen := &manifestQueryGenerator{}
	amb.SetGenerator(gen)
	ret := retriever.NewRetriever(vectors, manifestQueryEmbedder{}, .5, qc.MaxChunks)
	_, err := pipeline.RunRetrievalPipeline(ctx, qc, []pipeline.QueryStage{
		pipeline.NewDecomposeStage(),
		pipeline.NewRoutingStageWithConfig(ret, visible, previews, pipeline.RoutingConfigFromConfig(cfg)),
		pipeline.NewRetrievalStageWithVisibility(ret, visible),
		pipeline.NewSynthesisStage(amb, cfg),
	})
	require.NoError(t, err)
	require.NoError(t, qc.CheckEvidenceScope(ctx))

	checkHits := func(hits []domain.ScoredChunk) {
		t.Helper()
		require.Len(t, hits, len(want))
		seen := make(map[domain.DocumentID]bool, len(hits))
		for _, hit := range hits {
			id := hit.Chunk.DocumentID
			pub, ok := want[id]
			require.True(t, ok, "unexpected document %s", id)
			require.False(t, seen[id], "duplicate evidence for %s", id)
			seen[id] = true
			require.Equal(t, pub.revision, hit.Chunk.RevisionID)
			require.Equal(t, pub.incarnation, hit.ObservedIncarnation)
			require.Equal(t, pub.text, hit.Chunk.Text)
		}
	}
	checkHits(qc.RetrievedChunks)
	checkHits(qc.Excerpts)
	if mode == "auto" {
		checkHits(qc.Candidates)
		wantPreviews := make(map[string]string, len(want))
		for id, pub := range want {
			wantPreviews[string(id)] = pub.text
		}
		require.Len(t, visible.previews, len(wantPreviews))
		for id, preview := range wantPreviews {
			require.Equal(t, preview, visible.previews[id])
		}
	}
	if len(want) == 0 {
		require.Zero(t, gen.calls)
		require.Empty(t, qc.Citations)
		return
	}
	require.Equal(t, 1, gen.calls)
	var cited, expected []domain.DocumentID
	for _, citation := range qc.Citations {
		cited = append(cited, citation.DocumentID)
	}
	for id := range want {
		expected = append(expected, id)
	}
	require.ElementsMatch(t, expected, cited)
}

func TestQueryScopedHostedManifest(t *testing.T) {
	for _, backend := range []string{"pinecone", "postgres"} {
		for _, mode := range []string{"grants", "service"} {
			t.Run(backend+"/"+mode, func(t *testing.T) {
				testManifestQueryCorpus(t, backend, mode)
			})
		}
	}
}

func testManifestQueryCorpus(t *testing.T, backend, mode string) {
	ctx := t.Context()
	var c *Client
	var hosted *pcstore.Client
	var vectors manifestQueryVectors
	if backend == "pinecone" {
		c = manifestDatabase(t)
		cfg := &config.Config{PineconeUpsertBatchSize: 100, PineconeMaxAttempts: 1, PineconeTimeout: time.Second, EmbeddingDimensions: 2}
		hosted = pcstore.NewClientForIndex(pcstore.NewMemoryIndex(2), cfg, "")
		vectors = pcstore.NewDriver(hosted)
	} else {
		var err error
		c, err = NewClient(ctx, pgtest.StartVector(t))
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, c.Close(context.Background())) })
		require.NoError(t, c.RunMigrations(ctx))
		vectors, err = NewVectorStore(ctx, c, 2)
		require.NoError(t, err)
	}

	_, deletionErr := c.ActivateDeletion(ctx)
	require.NoError(t, deletionErr)
	var subject *domain.Principal
	if mode == "grants" {
		f := provisionOwnedFixture(t, c)
		_, err := c.ActivateGrants(ctx, false)
		require.NoError(t, err)
		subject = &f.owner
	}
	c.SetAuthzMode(mode)

	request := func(text string, scoped bool) domain.AcceptRequest {
		req := jobsRequest(text)
		if subject != nil {
			req = ownedRequest(*subject, "", text)
		}
		req.SupportsScopedVectors = scoped
		return req
	}
	manifest := request("The archive preserves maps for seven years.", true)
	first := publishQueryDocument(t, c, vectors, manifest, true, hosted != nil)
	require.Equal(t, hosted != nil, first.scoped)
	active := first
	if hosted != nil {
		active = replaceHostedQueryDocument(t, c, hosted, vectors, manifest, first)
	} else {
		require.Empty(t, active.revision, "transactional vectors publish with their SQL chunks")
	}

	tagged := request("The archive preserves photographs for five years.", true)
	tag := publishQueryDocument(t, c, vectors, tagged, true, false)
	require.Equal(t, hosted != nil, tag.scoped)
	require.Empty(t, tag.revision)
	legacy := request("The archive preserves letters for three years.", false)
	plain := publishQueryDocument(t, c, vectors, legacy, true, hosted != nil)
	require.False(t, plain.scoped)
	if hosted != nil {
		require.NotEmpty(t, plain.revision)
	}

	want := map[domain.DocumentID]manifestQueryPublication{
		manifest.Document.ID: active,
		tagged.Document.ID:   tag,
		legacy.Document.ID:   plain,
	}
	runManifestCorpusQuery(t, c, vectors, subject, "global", false, nil, want)
	runManifestCorpusQuery(t, c, vectors, subject, "auto", hosted != nil, nil, want)
	if hosted != nil {
		runManifestCorpusQuery(t, c, vectors, subject, "global", true, nil, want)
		require.NoError(t, hosted.Delete(ctx, "", []string{domain.FormatRevisionVectorID(manifest.Document.ID, active.revision, 0)}))
		runManifestCorpusQuery(t, c, vectors, subject, "auto", true, []string{string(manifest.Document.ID)}, nil)
	}
}

func replaceHostedQueryDocument(t *testing.T, c *Client, hosted *pcstore.Client, vectors manifestQueryVectors, req domain.AcceptRequest, first manifestQueryPublication) manifestQueryPublication {
	t.Helper()
	ctx := t.Context()
	id := req.Document.ID
	require.NotEmpty(t, first.revision)
	require.NoError(t, c.DeleteDocument(ctx, id))
	if req.Owner != nil {
		req.Owner.Incarnation = uuid.NewString()
	}
	second := publishQueryDocument(t, c, vectors, req, true, true)
	require.NotEqual(t, first.incarnation, second.incarnation)
	require.NotEmpty(t, second.revision)

	// Reopen this fixture at the current incarnation to exercise replacement of
	// an active revision while retaining its provider hits.
	grantExec(t, c, `WITH reopened AS (UPDATE documents SET status='processing' WHERE id=$1 RETURNING id)
		INSERT INTO ingest_jobs (document_id) SELECT id FROM reopened`, id)
	active := publishQueryDocument(t, c, vectors, req, false, true)
	require.Equal(t, second.incarnation, active.incarnation)
	require.NotEmpty(t, active.revision)
	require.NotEqual(t, second.revision, active.revision)

	oldIDs := []string{domain.FormatRevisionVectorID(id, first.revision, 0), domain.FormatRevisionVectorID(id, second.revision, 0)}
	hits, err := hosted.Fetch(ctx, append(oldIDs, domain.FormatRevisionVectorID(id, active.revision, 0)))
	require.NoError(t, err)
	require.Len(t, hits, 3)
	current := hits[domain.FormatRevisionVectorID(id, active.revision, 0)]
	for _, oldID := range oldIDs {
		stale := hits[oldID]
		require.Equal(t, current.ID, stale.ID)
		require.Equal(t, current.Text, stale.Text)
		require.Equal(t, current.Index, stale.Index)
		require.Equal(t, "retired", manifestState(t, c, stale.RevisionID))
		require.True(t, domain.ManifestIdentity(stale))
		stale.Embedding = []float32{1, 0}
		require.NoError(t, hosted.Upsert(ctx, "", []domain.Chunk{stale}))
	}
	return active
}
