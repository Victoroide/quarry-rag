package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/chunker"
	"github.com/Victoroide/quarry-rag/internal/domain"
	pgstore "github.com/Victoroide/quarry-rag/internal/store/postgres"
	"github.com/Victoroide/quarry-rag/internal/testutil/pgtest"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

func publishedDeploymentSQL(t *testing.T, marker string) string {
	t.Helper()
	docs, err := os.OpenRoot("../../docs")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, docs.Close()) })
	body, err := docs.ReadFile("deployment.md")
	require.NoError(t, err)
	for _, block := range regexp.MustCompile("(?s)```sql\n(.*?)```").FindAllStringSubmatch(string(body), -1) {
		if strings.Contains(block[1], marker) {
			return block[1]
		}
	}
	require.FailNow(t, "published SQL block not found", marker)
	return ""
}

func replaceReindexRoleSQL(sqlBlock, role, generation string) string {
	return strings.NewReplacer(
		"quarry_reindex", pgx.Identifier{role}.Sanitize(),
		"<generation>", generation,
	).Replace(sqlBlock)
}

func writeProfileJSON(t *testing.T, dir, name string, profile domain.EmbeddingProfile) string {
	t.Helper()
	data, err := json.Marshal(profile)
	require.NoError(t, err)
	path := filepath.Join(dir, name)
	require.NoError(t, os.WriteFile(path, data, 0o600))
	return path
}

func runCLIAction(t *testing.T, d deps, opts options) (map[string]any, error) {
	t.Helper()
	base := defaultOptions()
	base.Generation, base.Profile = opts.Generation, opts.Profile
	base.TargetStore, base.TargetIndex = opts.TargetStore, opts.TargetIndex
	base.Apply, base.Verify, base.Freeze, base.Thaw, base.Activate, base.Attest = opts.Apply, opts.Verify, opts.Freeze, opts.Thaw, opts.Activate, opts.Attest
	base.MaxEmbedCalls, base.Quiesce = opts.MaxEmbedCalls, opts.Quiesce
	base.SampleSize, base.TopK, base.ScopeDocuments = opts.SampleSize, opts.TopK, opts.ScopeDocuments
	base.MinRecall, base.MaxP95 = opts.MinRecall, opts.MaxP95
	var out bytes.Buffer
	err := execute(t.Context(), d, base, &out, 10*time.Millisecond)
	var report map[string]any
	if out.Len() > 0 {
		require.NoError(t, json.Unmarshal(out.Bytes(), &report), out.String())
	}
	return report, err
}

func seedAssignedDocument(t *testing.T, owner *pgstore.Client, live *pgstore.VectorStore) docSpec {
	t.Helper()
	ctx := t.Context()
	tenantID := uuid.NewString()
	principalID := uuid.NewString()
	var revision int64
	require.NoError(t, owner.Pool.QueryRow(ctx, `SELECT revision FROM grant_catalog_policy_revision`).Scan(&revision))
	batch, err := json.Marshal(map[string]any{
		"batch_id":          uuid.NewString(),
		"expected_revision": revision,
		"tenants":           []map[string]any{{"tenant_id": tenantID}},
		"collections":       []map[string]any{{"tenant_id": tenantID, "collection": "alpha"}},
		"principals":        []map[string]any{{"tenant_id": tenantID, "principal_id": principalID, "issuer": "synthetic", "subject": "reindex-owner", "enabled": true}},
		"create_permissions": []map[string]any{{
			"tenant_id": tenantID, "principal_id": principalID, "collection": "alpha", "can_create": true,
		}},
	})
	require.NoError(t, err)
	_, err = owner.ImportGrantBatch(ctx, bytes.NewReader(batch), false)
	require.NoError(t, err)

	docID := domain.DocumentID(uuid.NewString())
	incarnation := uuid.NewString()
	content := "Assigned document stored for vector reindex privilege checks."
	req := domain.AcceptRequest{
		Document:     domain.Document{ID: docID, Content: content},
		Fingerprint:  chunker.ScopedFingerprint("alpha", content),
		DocType:      "note",
		Collection:   "alpha",
		ContentDedup: domain.ContentDedupOff,
		MaxPending:   100,
		Owner: &domain.OwnerBinding{
			Principal:   domain.Principal{ID: principalID, TenantID: tenantID},
			Incarnation: incarnation,
			ValidUntil:  time.Now().Add(time.Minute),
		},
	}
	accepted, err := owner.AcceptDocument(ctx, req)
	require.NoError(t, err)
	require.Equal(t, domain.AcceptAccepted, accepted.Outcome)

	job, err := owner.ClaimJob(ctx, "seed-worker", time.Minute, 3)
	require.NoError(t, err)
	require.NotNil(t, job)

	chunkID := hexChunkID(string(docID), 0)
	text := "Assigned document chunk for vector reindex role verification."
	chunk := domain.Chunk{
		ID:         domain.ChunkID(chunkID),
		DocumentID: docID,
		Index:      0,
		Text:       text,
		TokenCount: chunker.EstimateTokens(text),
		Embedding:  embedWith(sourceModel, text),
		Metadata:   map[string]any{"doc_type": "note", "collection": "alpha"},
	}
	live.RouteCompletion()
	done, err := owner.CompleteJob(ctx, *job, []domain.Chunk{chunk}, domain.IndexObservation{})
	require.NoError(t, err)
	require.True(t, done)

	return docSpec{ID: string(docID), Collection: "alpha", DocType: "note", Status: "completed", Texts: []string{text}}
}

func TestReindexMaintenanceRoleWithDocumentedGrants(t *testing.T) {
	baseSQL := publishedDeploymentSQL(t, "GRANT USAGE ON SCHEMA public TO quarry_reindex")
	legacySQL := publishedDeploymentSQL(t, "ON public.chunk_vectors TO quarry_reindex")
	precreatedSQL := publishedDeploymentSQL(t, "ON public.chunk_vectors_<generation> TO quarry_reindex")
	selfCreateSQL := publishedDeploymentSQL(t, "GRANT CREATE ON SCHEMA public TO quarry_reindex")

	owner := vectorDB(t)
	ctx := t.Context()
	legacyStore, err := pgstore.CreateGenerationVectorStore(ctx, owner, testDims, domain.LegacyGeneration, pgstore.WithExactScopeMax(10))
	require.NoError(t, err)

	docs := []docSpec{
		newDoc("alpha", "note", "First unassigned chunk for restricted reindex.", "Second unassigned chunk for restricted reindex."),
		newDoc("alpha", "note", "Third unassigned chunk for restricted reindex."),
	}
	for _, d := range docs {
		chunks := insertDoc(t, owner, d)
		require.NoError(t, legacyStore.Upsert(ctx, "", sourceChunks(d, chunks, sourceModel)))
	}
	seedAssignedDocument(t, owner, legacyStore)
	_, err = owner.Pool.Exec(ctx, `INSERT INTO document_pdf_extractions(document_id, profile, manifest, coverage, page_results) VALUES($1, $2, '{}', 'complete', '[{"number":1,"state":"complete"}]')`, docs[0].ID, domain.LayoutPDFProfile)
	require.NoError(t, err)

	cfg := liveConfig("postgres")
	cfg.PGVectorExactScopeMax = 10
	require.NoError(t, owner.PinVectorGeneration(ctx, domain.LegacyGeneration, "postgres", "chunk_vectors", "", cfg.EmbeddingSettings()))

	dsn := owner.Pool.Config().ConnString()
	roleName := pgtest.Role(t, dsn, "quarry_reindex")
	roleIdent := pgx.Identifier{roleName}.Sanitize()
	_, err = owner.Pool.Exec(ctx, `REVOKE CREATE ON SCHEMA public FROM PUBLIC`)
	require.NoError(t, err)
	_, err = owner.Pool.Exec(ctx, `CREATE ROLE `+roleIdent+` LOGIN PASSWORD 'fixture-password'`)
	require.NoError(t, err)
	_, err = owner.Pool.Exec(ctx, `ALTER ROLE `+roleIdent+` SET search_path = public`)
	require.NoError(t, err)

	reindex, err := pgstore.NewClient(ctx, dsn, func(poolCfg *pgxpool.Config) {
		poolCfg.ConnConfig.User = roleName
		poolCfg.ConnConfig.Password = "fixture-password"
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reindex.Close(context.Background())) })

	// Apply base + legacy grants, then verify missing grant_catalog_document_tenant fails closed with 42501.
	_, err = owner.Pool.Exec(ctx, replaceReindexRoleSQL(baseSQL, roleName, ""))
	require.NoError(t, err)
	_, err = owner.Pool.Exec(ctx, replaceReindexRoleSQL(legacySQL, roleName, ""))
	require.NoError(t, err)
	_, err = owner.Pool.Exec(ctx, `REVOKE EXECUTE ON FUNCTION public.grant_catalog_document_tenant(uuid, uuid) FROM `+roleIdent)
	require.NoError(t, err)

	dir := t.TempDir()
	profilePath := writeProfileJSON(t, dir, "target.json", testProfile(sourceModel))
	d := newDeps(cfg, reindex)
	d.newEmbedder = func(_ context.Context, profile domain.EmbeddingProfile) (domain.EmbeddingProvider, error) {
		return &countingEmbedder{model: profile.Model}, nil
	}

	_, err = runCLIAction(t, d, options{Generation: "g2", Profile: profilePath})
	var pgErr *pgconn.PgError
	require.ErrorAs(t, err, &pgErr)
	require.Equal(t, "42501", pgErr.Code, pgErr.Message)
	require.Contains(t, pgErr.Message, "grant_catalog_document_tenant")

	// Restore the documented base grants and pre-create chunk_vectors_g2 without granting schema CREATE.
	_, err = owner.Pool.Exec(ctx, replaceReindexRoleSQL(baseSQL, roleName, ""))
	require.NoError(t, err)
	ddl := strings.NewReplacer("<generation>", "g2", "<dimensions>", strconv.Itoa(testDims)).Replace(publishedDeploymentSQL(t, "CREATE TABLE public.chunk_vectors_<generation>"))
	_, err = owner.Pool.Exec(ctx, ddl)
	require.NoError(t, err)
	_, err = owner.Pool.Exec(ctx, replaceReindexRoleSQL(precreatedSQL, roleName, "g2"))
	require.NoError(t, err)

	inv, err := runCLIAction(t, d, options{Generation: "g2", Profile: profilePath})
	require.NoError(t, err)
	require.Empty(t, inv["blockers"])
	require.EqualValues(t, 3, inv["documents"].(map[string]any)["completed"])

	attest, err := runCLIAction(t, d, options{Attest: true, Apply: true, Profile: profilePath})
	require.NoError(t, err)
	require.Equal(t, true, attest["applied"])

	staged, err := runCLIAction(t, d, options{Generation: "g2", Profile: profilePath, Apply: true})
	require.NoError(t, err)
	require.Equal(t, true, staged["complete"])
	require.EqualValues(t, 3, staged["staged_documents"])

	frozen, err := runCLIAction(t, d, options{Generation: "g2", Freeze: true, Apply: true, Quiesce: 0})
	require.NoError(t, err)
	require.Equal(t, true, frozen["frozen"])

	verified, err := runCLIAction(t, d, options{Generation: "g2", Profile: profilePath, Verify: true, Apply: true, SampleSize: 2, TopK: 5, ScopeDocuments: 3, MinRecall: 0.5, MaxP95: time.Minute})
	require.NoError(t, err)
	require.Equal(t, true, verified["passed"])
	require.Equal(t, true, verified["recorded"])

	activated, err := runCLIAction(t, d, options{Generation: "g2", Activate: true, Apply: true, Quiesce: 0})
	require.NoError(t, err)
	require.Equal(t, "g2", activated["active_generation"])

	thawed, err := runCLIAction(t, d, options{Thaw: true, Apply: true})
	require.NoError(t, err)
	require.Equal(t, false, thawed["frozen"])

	// Serve g2 and insert a new document only in g2, then roll back into legacy chunk_vectors.
	d.cfg.VectorGeneration = "g2"
	g2Store, err := pgstore.OpenGenerationVectorStore(ctx, owner, testDims, "g2", pgstore.WithExactScopeMax(10))
	require.NoError(t, err)
	postCutover := newDoc("alpha", "note", "Written after cutover to g2.")
	postChunks := insertDoc(t, owner, postCutover)
	require.NoError(t, g2Store.Upsert(ctx, "", sourceChunks(postCutover, postChunks, sourceModel)))
	maintenanceSource, err := pgstore.OpenGenerationVectorStore(ctx, reindex, testDims, "g2", pgstore.WithExactScopeMax(10))
	require.NoError(t, err)
	readBack, err := maintenanceSource.Fetch(ctx, []string{string(postChunks[0].ID)})
	require.NoError(t, err)
	require.Contains(t, readBack, string(postChunks[0].ID))

	_, err = runCLIAction(t, d, options{Generation: domain.LegacyGeneration, Freeze: true, Apply: true, Quiesce: 0})
	require.NoError(t, err)
	rollback, err := runCLIAction(t, d, options{Generation: domain.LegacyGeneration, Profile: profilePath, Apply: true})
	require.NoError(t, err)
	require.Equal(t, true, rollback["complete"])
	require.EqualValues(t, 4, rollback["staged_documents"])
	verifiedRollback, err := runCLIAction(t, d, options{Generation: domain.LegacyGeneration, Profile: profilePath, Verify: true, Apply: true, SampleSize: 2, TopK: 5, ScopeDocuments: 3, MinRecall: 0.5, MaxP95: time.Minute})
	require.NoError(t, err)
	require.Equal(t, true, verifiedRollback["passed"])
	activatedRollback, err := runCLIAction(t, d, options{Generation: domain.LegacyGeneration, Activate: true, Apply: true, Quiesce: 0})
	require.NoError(t, err)
	require.Equal(t, domain.LegacyGeneration, activatedRollback["active_generation"])
	_, err = runCLIAction(t, d, options{Thaw: true, Apply: true})
	require.NoError(t, err)

	// Self-created generation g3 fails without schema CREATE/REFERENCES, and succeeds once granted.
	d.cfg.VectorGeneration = ""
	_, err = runCLIAction(t, d, options{Generation: "g3", Profile: profilePath, Apply: true})
	require.True(t, errors.As(err, &pgErr) && pgErr.Code == "42501", "expected 42501 without schema CREATE, got %v", err)
	require.Contains(t, pgErr.Message, "public")

	_, err = owner.Pool.Exec(ctx, replaceReindexRoleSQL(selfCreateSQL, roleName, ""))
	require.NoError(t, err)
	selfStaged, err := runCLIAction(t, d, options{Generation: "g3", Profile: profilePath, Apply: true})
	require.NoError(t, err)
	require.Equal(t, true, selfStaged["complete"])
	require.EqualValues(t, 4, selfStaged["staged_documents"])
	servingRole := pgtest.Role(t, dsn, "quarry_service")
	servingIdent := pgx.Identifier{servingRole}.Sanitize()
	_, err = owner.Pool.Exec(ctx, `CREATE ROLE `+servingIdent)
	require.NoError(t, err)
	_, err = owner.Pool.Exec(ctx, `GRANT USAGE ON SCHEMA public TO `+servingIdent)
	require.NoError(t, err)
	servingSQL := publishedDeploymentSQL(t, "ON public.chunk_vectors_<generation> TO quarry_service")
	servingSQL = strings.NewReplacer("<generation>", "g3", "quarry_service", servingIdent).Replace(servingSQL)
	_, err = reindex.Pool.Exec(ctx, servingSQL)
	require.NoError(t, err)
	tx, err := owner.Pool.Begin(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, tx.Rollback(context.Background())) })
	_, err = tx.Exec(ctx, `SET LOCAL ROLE `+servingIdent)
	require.NoError(t, err)
	var count int
	require.NoError(t, tx.QueryRow(ctx, `SELECT count(*) FROM public.chunk_vectors_g3`).Scan(&count))
}
