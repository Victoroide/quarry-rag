package postgres

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func generationProfile() domain.EmbeddingProfile {
	return domain.EmbeddingProfile{
		Provider: domain.EmbeddingProviderBedrock, Model: "amazon.titan-embed-text-v2:0", ModelRevision: "2024-04",
		Dimensions: 3, Metric: domain.MetricCosine, ExtractionChunkVersion: "chunk-1",
	}
}

func stagingGeneration(name string) Generation {
	return Generation{Name: name, Store: "postgres", Target: VectorTable(name), Profile: generationProfile(), Origin: OriginRecorded}
}

func TestGenerationNamesAndTables(t *testing.T) {
	for _, name := range []string{"legacy", "g2", "next_gen_1", "a23456789012345678901234"} {
		require.True(t, ValidGenerationName(name), name)
	}
	for _, name := range []string{"", "2g", "G2", "g-2", "g 2", "a234567890123456789012345", "g;drop", "g\n"} {
		require.False(t, ValidGenerationName(name), name)
	}
	require.Equal(t, "chunk_vectors", VectorTable(LegacyGeneration))
	require.Equal(t, "chunk_vectors_g2", VectorTable("g2"))
}

func TestVectorStateDefaultsToOpenLegacy(t *testing.T) {
	c := jobsDatabase(t)
	state, err := c.VectorState(t.Context())
	require.NoError(t, err)
	require.Equal(t, VectorState{Active: LegacyGeneration}, state)
	require.NoError(t, c.WritersOpen(t.Context()))
}

func TestRegisterGenerationIsIdempotentAndFailsClosed(t *testing.T) {
	c := jobsDatabase(t)
	first, err := c.RegisterGeneration(t.Context(), stagingGeneration("g2"))
	require.NoError(t, err)
	require.Equal(t, StateStaging, first.State)
	require.Equal(t, generationProfile().Hash(), first.Profile.Hash())
	again, err := c.RegisterGeneration(t.Context(), stagingGeneration("g2"))
	require.NoError(t, err)
	require.Equal(t, first.State, again.State)

	drifted := stagingGeneration("g2")
	drifted.Profile.ModelRevision = "2025-01"
	_, err = c.RegisterGeneration(t.Context(), drifted)
	require.ErrorIs(t, err, ErrGenerationDrift)
	moved := stagingGeneration("g2")
	moved.Target = "chunk_vectors_elsewhere"
	_, err = c.RegisterGeneration(t.Context(), moved)
	require.ErrorIs(t, err, ErrGenerationDrift)

	sameTarget := stagingGeneration("g3")
	sameTarget.Target = VectorTable("g2")
	_, err = c.RegisterGeneration(t.Context(), sameTarget)
	require.ErrorIs(t, err, ErrGenerationDrift)

	for _, bad := range []Generation{
		func() Generation { g := stagingGeneration("G2"); return g }(),
		func() Generation { g := stagingGeneration("g4"); g.Store = "memory"; return g }(),
		func() Generation { g := stagingGeneration("g4"); g.Target = ""; return g }(),
		func() Generation { g := stagingGeneration("g4"); g.Profile.Metric = "dotproduct"; return g }(),
		func() Generation { g := stagingGeneration("g4"); g.Origin = "guessed"; return g }(),
	} {
		_, err = c.RegisterGeneration(t.Context(), bad)
		require.Error(t, err)
	}
	missing, err := c.Generation(t.Context(), "absent")
	require.NoError(t, err)
	require.Nil(t, missing)
}

func TestAttestedLegacyGenerationIsActive(t *testing.T) {
	c := jobsDatabase(t)
	legacy := Generation{Name: LegacyGeneration, Store: "postgres", Target: VectorTable(LegacyGeneration), Profile: generationProfile(), Origin: OriginAttested}
	got, err := c.RegisterGeneration(t.Context(), legacy)
	require.NoError(t, err)
	require.Equal(t, StateActive, got.State)
	require.Equal(t, OriginAttested, got.Origin)
}

func TestFreezeGatesAdmissionClaimsAndDeletes(t *testing.T) {
	c := jobsDatabase(t)
	queued := jobsAccept(t, c, "queued before the freeze")
	require.NoError(t, c.FreezeWriters(t.Context(), "g2"))
	require.NoError(t, c.FreezeWriters(t.Context(), "g2"), "repeating a freeze for the same generation is idempotent")
	require.ErrorContains(t, c.FreezeWriters(t.Context(), "g3"), "already frozen")

	require.ErrorIs(t, c.WritersOpen(t.Context()), domain.ErrWritersFrozen)
	accepted, err := c.AcceptDocument(t.Context(), jobsRequest("rejected while frozen"))
	require.NoError(t, err)
	require.Equal(t, domain.AcceptFrozen, accepted.Outcome)
	var documents int
	require.NoError(t, c.Pool.QueryRow(t.Context(), `SELECT count(*) FROM documents`).Scan(&documents))
	require.Equal(t, 1, documents, "a frozen admission stores nothing")
	job, err := c.ClaimJob(t.Context(), "worker", time.Minute, 5)
	require.NoError(t, err)
	require.Nil(t, job, "a frozen queue yields no work")

	require.NoError(t, c.ThawWriters(t.Context()))
	require.NoError(t, c.WritersOpen(t.Context()))
	job, err = c.ClaimJob(t.Context(), "worker", time.Minute, 5)
	require.NoError(t, err)
	require.NotNil(t, job)
	require.Equal(t, queued, job.Document.ID)
}

func TestFreezeGatesOwnedAcceptanceAndTenantClaims(t *testing.T) {
	c, _ := vectorDatabase(t)
	ctx := t.Context()
	f := provisionOwnedFixture(t, c)
	reservedID, reservedInc := f.reserve(t, f.owner, "")
	newReq := ownedRequest(f.owner, "", "owned before freeze")
	reservedReq := suppliedOwnedRequest(f.owner, "", "reserved before freeze", reservedID, reservedInc)

	require.NoError(t, c.FreezeWriters(ctx, "g2"))
	for _, req := range []domain.AcceptRequest{newReq, reservedReq} {
		res, err := c.AcceptDocument(ctx, req)
		require.NoError(t, err)
		require.Equal(t, domain.AcceptFrozen, res.Outcome)
	}
	require.Equal(t, storeCounts{}, f.counts(t), "frozen owned acceptance stores nothing and binds no owner")
	var resState string
	require.NoError(t, c.Pool.QueryRow(ctx, `SELECT state FROM grant_catalog_document_reservations WHERE document_id=$1`, reservedID).Scan(&resState))
	require.Equal(t, "pending", resState)

	require.NoError(t, c.ThawWriters(ctx))
	require.Equal(t, domain.AcceptAccepted, f.accept(t, newReq).Outcome)
	require.Equal(t, domain.AcceptAccepted, f.accept(t, reservedReq).Outcome)

	require.NoError(t, c.FreezeWriters(ctx, "g2"))
	frozenJob, err := c.ClaimJob(ctx, "worker", time.Minute, 5)
	require.NoError(t, err)
	require.Nil(t, frozenJob, "frozen queue yields no owned jobs")

	staged, err := CreateGenerationVectorStore(ctx, c, 3, "g2")
	require.NoError(t, err)
	verifiedGeneration(t, c, "g2", "digest")
	require.NoError(t, c.ActivateGeneration(ctx, "g2", "digest", 0))
	require.NoError(t, c.ThawWriters(ctx))

	// Legacy-pinned client must refuse both owned acceptance and claiming the owned job.
	_, err = c.AcceptDocument(ctx, ownedRequest(f.owner, "", "mismatched generation"))
	require.ErrorIs(t, err, domain.ErrGenerationMismatch)
	_, err = c.ClaimJob(ctx, "worker", time.Minute, 5)
	require.ErrorIs(t, err, domain.ErrGenerationMismatch)

	staged.RouteCompletion()
	require.NoError(t, c.PinVectorGeneration(ctx, "g2", "postgres", VectorTable("g2"), "", generationProfile()))
	claimed, err := c.ClaimJob(ctx, "worker", time.Minute, 5)
	require.NoError(t, err)
	require.NotNil(t, claimed)
	require.Equal(t, f.tenant, claimed.Tenant)

	ch := chunkFor(claimed.Document.ID, 0)
	ch.Embedding = []float32{1, 0, 0}
	done, err := c.CompleteJob(ctx, *claimed, []domain.Chunk{ch}, domain.IndexObservation{})
	require.NoError(t, err)
	require.True(t, done)

	var inG2 int
	require.NoError(t, c.Pool.QueryRow(ctx, `SELECT count(*) FROM chunk_vectors_g2 WHERE document_id=$1`, string(claimed.Document.ID)).Scan(&inG2))
	require.Equal(t, 1, inG2)
	row, found := f.ownership(t, string(claimed.Document.ID), claimed.Incarnation)
	require.True(t, found)
	require.True(t, row.ready)
}

func TestFreezeWaitsForAdmissionsInFlight(t *testing.T) {
	c := jobsDatabase(t)
	tx, err := c.Pool.Begin(t.Context())
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(context.WithoutCancel(t.Context())) }()
	_, err = tx.Exec(t.Context(), `SELECT pg_advisory_xact_lock($1)`, ingestAdmissionLock)
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() { done <- c.FreezeWriters(t.Context(), "g2") }()
	select {
	case freezeErr := <-done:
		t.Fatalf("freeze returned while an admission held the lock: %v", freezeErr)
	case <-time.After(500 * time.Millisecond):
	}
	require.NoError(t, tx.Commit(t.Context()))
	require.NoError(t, <-done)
}

func TestFreezeWaitsForABlockedJobClaim(t *testing.T) {
	c := jobsDatabase(t)
	id := jobsAccept(t, c, "queued before freezing")
	_, err := c.RegisterGeneration(t.Context(), Generation{Name: "g2", Store: "postgres", Target: VectorTable("g2"), Profile: generationProfile(), Origin: OriginRecorded})
	require.NoError(t, err)
	tx, err := c.Pool.Begin(t.Context())
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(context.Background()) }()
	_, err = tx.Exec(t.Context(), "LOCK TABLE ingest_jobs IN SHARE MODE")
	require.NoError(t, err)
	var holder int
	require.NoError(t, tx.QueryRow(t.Context(), "SELECT pg_backend_pid()").Scan(&holder))
	type claim struct {
		job *domain.Job
		err error
	}
	claimed := make(chan claim, 1)
	go func() {
		job, claimErr := c.ClaimJob(t.Context(), "old-replica", time.Minute, 3)
		claimed <- claim{job, claimErr}
	}()
	require.Eventually(t, func() bool {
		var waiting bool
		queryErr := c.Pool.QueryRow(t.Context(), `SELECT EXISTS(SELECT 1 FROM pg_stat_activity
 WHERE $1 = ANY(pg_blocking_pids(pid)) AND query LIKE '%UPDATE ingest_jobs j SET state=%')`, holder).Scan(&waiting)
		return queryErr == nil && waiting
	}, 5*time.Second, 10*time.Millisecond)
	ctx, cancel := context.WithTimeout(t.Context(), 250*time.Millisecond)
	defer cancel()
	require.Error(t, c.FreezeWriters(ctx, "g2"), "freeze cannot publish while the lease update is blocked")
	state, err := c.VectorState(t.Context())
	require.NoError(t, err)
	require.False(t, state.Frozen)
	require.NoError(t, tx.Commit(t.Context()))
	got := <-claimed
	require.NoError(t, got.err)
	require.NotNil(t, got.job)
	require.Equal(t, id, got.job.Document.ID)
	require.NoError(t, c.FreezeWriters(t.Context(), "g2"))
	quiet, err := c.WriterQuiesce(t.Context(), 0)
	require.NoError(t, err)
	require.Equal(t, 1, quiet.RunningJobs)
	require.NoError(t, c.RecordVerification(t.Context(), "g2", "digest", json.RawMessage(`{}`)))
	require.ErrorContains(t, c.ActivateGeneration(t.Context(), "g2", "digest", 0), "1 in-flight jobs")
	require.NoError(t, c.FailJob(t.Context(), *got.job, "finished"))
	require.NoError(t, c.ActivateGeneration(t.Context(), "g2", "digest", 0))
}

func TestQuiesceCountsRunningLeases(t *testing.T) {
	c := jobsDatabase(t)
	jobsAccept(t, c, "claimed before the freeze")
	job, err := c.ClaimJob(t.Context(), "worker", time.Minute, 5)
	require.NoError(t, err)
	require.NotNil(t, job)
	status, err := c.WriterQuiesce(t.Context(), 0)
	require.NoError(t, err)
	require.False(t, status.Frozen)
	require.NoError(t, c.FreezeWriters(t.Context(), "g2"))
	status, err = c.WriterQuiesce(t.Context(), 0)
	require.NoError(t, err)
	require.True(t, status.Frozen)
	require.Equal(t, 1, status.RunningJobs)
	require.False(t, status.Quiet())
	require.NoError(t, c.FailJob(t.Context(), *job, "stop"))
	status, err = c.WriterQuiesce(t.Context(), 0)
	require.NoError(t, err)
	require.True(t, status.Quiet())
	status, err = c.WriterQuiesce(t.Context(), time.Hour)
	require.NoError(t, err)
	require.False(t, status.Quiet(), "the settling window has not elapsed")
	_, err = c.Pool.Exec(t.Context(), `INSERT INTO document_deletions(document_id,incarnation,kind,vector_mode) VALUES($1,$2,'quarantine','local')`, uuid.NewString(), uuid.NewString())
	require.NoError(t, err)
	status, err = c.WriterQuiesce(t.Context(), 0)
	require.NoError(t, err)
	require.Equal(t, 1, status.OpenDeletions)
	require.False(t, status.Quiet())
}

func TestReplicaOnAnotherGenerationCannotWrite(t *testing.T) {
	c := jobsDatabase(t)
	jobsAccept(t, c, "queued")
	c.SetVectorGeneration("g2")
	require.ErrorIs(t, c.WritersOpen(t.Context()), domain.ErrGenerationMismatch)
	_, err := c.AcceptDocument(t.Context(), jobsRequest("refused"))
	require.ErrorIs(t, err, domain.ErrGenerationMismatch)
	job, err := c.ClaimJob(t.Context(), "worker", time.Minute, 5)
	require.ErrorIs(t, err, domain.ErrGenerationMismatch)
	require.Nil(t, job)
	c.SetVectorGeneration(LegacyGeneration)
	require.NoError(t, c.WritersOpen(t.Context()))
}

func TestPinVectorGeneration(t *testing.T) {
	c := jobsDatabase(t)
	want := generationProfile()
	pin := func(generation, store, target string, profile domain.EmbeddingProfile) error {
		return c.PinVectorGeneration(t.Context(), generation, store, target, profile)
	}
	require.NoError(t, pin(LegacyGeneration, "postgres", "chunk_vectors", want), "an unregistered legacy target is accepted")
	require.ErrorIs(t, pin("g2", "postgres", VectorTable("g2"), want), domain.ErrGenerationMismatch, "g2 is not active")

	legacy := Generation{Name: LegacyGeneration, Store: "postgres", Target: "chunk_vectors", Profile: want, Origin: OriginAttested}
	_, err := c.RegisterGeneration(t.Context(), legacy)
	require.NoError(t, err)
	require.NoError(t, pin(LegacyGeneration, "postgres", "chunk_vectors", want))
	require.ErrorIs(t, pin(LegacyGeneration, "pinecone", "chunk_vectors", want), ErrGenerationDrift)
	require.ErrorIs(t, pin(LegacyGeneration, "postgres", "chunk_vectors_g2", want), ErrGenerationDrift)
	for name, mutate := range map[string]func(*domain.EmbeddingProfile){
		"provider":   func(p *domain.EmbeddingProfile) { p.Provider = domain.EmbeddingProviderOpenAI },
		"model":      func(p *domain.EmbeddingProfile) { p.Model = "other-model" },
		"dimensions": func(p *domain.EmbeddingProfile) { p.Dimensions = 4 },
		"revision":   func(p *domain.EmbeddingProfile) { p.ModelRevision = "other-revision" },
		"text":       func(p *domain.EmbeddingProfile) { p.ExtractionChunkVersion = "other-text" },
	} {
		drifted := want
		mutate(&drifted)
		require.ErrorIs(t, pin(LegacyGeneration, "postgres", "chunk_vectors", drifted), ErrGenerationDrift, name)
	}
}

func verifiedGeneration(t *testing.T, c *Client, name, digest string) {
	t.Helper()
	_, err := c.RegisterGeneration(t.Context(), stagingGeneration(name))
	require.NoError(t, err)
	require.NoError(t, c.FreezeWriters(t.Context(), name))
	require.NoError(t, c.RecordVerification(t.Context(), name, digest, json.RawMessage(`{"passed":true}`)))
}

func TestActivationRequiresFrozenVerifiedMatchingCorpus(t *testing.T) {
	c := jobsDatabase(t)
	_, err := c.RegisterGeneration(t.Context(), stagingGeneration("g2"))
	require.NoError(t, err)
	require.ErrorContains(t, c.ActivateGeneration(t.Context(), "g2", "digest", 0), "frozen")
	require.ErrorContains(t, c.RecordVerification(t.Context(), "g2", "digest", json.RawMessage(`{}`)), "frozen")

	require.NoError(t, c.FreezeWriters(t.Context(), "g3"))
	require.ErrorContains(t, c.RecordVerification(t.Context(), "g2", "digest", json.RawMessage(`{}`)), "frozen for")
	require.NoError(t, c.ThawWriters(t.Context()))

	require.NoError(t, c.FreezeWriters(t.Context(), "g2"))
	require.ErrorContains(t, c.ActivateGeneration(t.Context(), "g2", "digest", 0), "verified")
	require.NoError(t, c.RecordVerification(t.Context(), "g2", "digest", json.RawMessage(`{"passed":true}`)))
	require.ErrorContains(t, c.ActivateGeneration(t.Context(), "g2", "other-digest", 0), "corpus changed")
	require.ErrorContains(t, c.ActivateGeneration(t.Context(), "g2", "digest", time.Hour), "settling")
	require.ErrorContains(t, c.ActivateGeneration(t.Context(), "absent", "digest", 0), "not registered")
	state, err := c.VectorState(t.Context())
	require.NoError(t, err)
	require.Equal(t, LegacyGeneration, state.Active)

	require.NoError(t, c.ActivateGeneration(t.Context(), "g2", "digest", 0))
	state, err = c.VectorState(t.Context())
	require.NoError(t, err)
	require.Equal(t, "g2", state.Active)
	require.True(t, state.Frozen, "activation keeps writers frozen until the operator resumes them")
	got, err := c.Generation(t.Context(), "g2")
	require.NoError(t, err)
	require.Equal(t, StateActive, got.State)
	require.ErrorContains(t, c.ActivateGeneration(t.Context(), "g2", "digest", 0), "already active")
}

func TestActivationBlockedByRunningLease(t *testing.T) {
	c := jobsDatabase(t)
	jobsAccept(t, c, "claimed")
	job, err := c.ClaimJob(t.Context(), "worker", time.Minute, 5)
	require.NoError(t, err)
	require.NotNil(t, job)
	verifiedGeneration(t, c, "g2", "digest")
	require.ErrorContains(t, c.ActivateGeneration(t.Context(), "g2", "digest", 0), "in-flight")
	require.NoError(t, c.FailJob(t.Context(), *job, "stop"))
	require.NoError(t, c.ActivateGeneration(t.Context(), "g2", "digest", 0))
}

func TestActivationRetiresThePreviousGenerationAndPinsReplicas(t *testing.T) {
	c := jobsDatabase(t)
	legacy := Generation{Name: LegacyGeneration, Store: "postgres", Target: "chunk_vectors", Profile: generationProfile(), Origin: OriginAttested}
	_, err := c.RegisterGeneration(t.Context(), legacy)
	require.NoError(t, err)
	verifiedGeneration(t, c, "g2", "digest")
	require.NoError(t, c.ActivateGeneration(t.Context(), "g2", "digest", 0))
	previous, err := c.Generation(t.Context(), LegacyGeneration)
	require.NoError(t, err)
	require.Equal(t, StateRetired, previous.State)

	require.NoError(t, c.ThawWriters(t.Context()))
	require.ErrorIs(t, c.WritersOpen(t.Context()), domain.ErrGenerationMismatch, "the legacy replica is pinned out")
	c.SetVectorGeneration("g2")
	require.NoError(t, c.WritersOpen(t.Context()))
	require.NoError(t, c.PinVectorGeneration(t.Context(), "g2", "postgres", VectorTable("g2"), generationProfile()))
}

func TestCheckpointLedger(t *testing.T) {
	c := jobsDatabase(t)
	_, err := c.RegisterGeneration(t.Context(), stagingGeneration("g2"))
	require.NoError(t, err)
	doc := uuid.NewString()
	seen, err := c.GenerationCheckpoints(t.Context(), "g2", []string{doc})
	require.NoError(t, err)
	require.Empty(t, seen)

	recorded, err := c.BeginGenerationDocument(t.Context(), "g2", doc, []string{"b", "a"})
	require.NoError(t, err)
	require.Equal(t, []string{"a", "b"}, recorded)
	seen, err = c.GenerationCheckpoints(t.Context(), "g2", []string{doc})
	require.NoError(t, err)
	require.Equal(t, Checkpoint{DocumentID: doc, ChunkIDs: []string{"a", "b"}}, seen[doc])

	// A second intent widens the possibly written set so that no key is forgotten.
	recorded, err = c.BeginGenerationDocument(t.Context(), "g2", doc, []string{"b", "c"})
	require.NoError(t, err)
	require.Equal(t, []string{"a", "b", "c"}, recorded)
	seen, err = c.GenerationCheckpoints(t.Context(), "g2", []string{doc})
	require.NoError(t, err)
	require.Equal(t, []string{"a", "b", "c"}, seen[doc].ChunkIDs)
	require.False(t, seen[doc].Complete)

	incarnation := uuid.NewString()
	require.NoError(t, c.CompleteGenerationDocument(t.Context(), "g2", doc, "rev-1", incarnation, []string{"b", "c"}, 1, 1))
	seen, err = c.GenerationCheckpoints(t.Context(), "g2", []string{doc})
	require.NoError(t, err)
	require.Equal(t, Checkpoint{DocumentID: doc, SourceRevision: "rev-1", Incarnation: incarnation, ChunkIDs: []string{"b", "c"}, Complete: true, Copied: 1, Embedded: 1}, seen[doc])

	complete, incomplete, err := c.CountGenerationDocuments(t.Context(), "g2")
	require.NoError(t, err)
	require.Equal(t, [2]int{1, 0}, [2]int{complete, incomplete})

	// The source never contained this document as completed, so its keys are orphaned.
	orphans, err := c.OrphanedGenerationDocuments(t.Context(), "g2", 10)
	require.NoError(t, err)
	require.Len(t, orphans, 1)
	require.Equal(t, doc, orphans[0].DocumentID)
	require.NoError(t, c.ForgetGenerationDocument(t.Context(), "g2", doc))
	orphans, err = c.OrphanedGenerationDocuments(t.Context(), "g2", 10)
	require.NoError(t, err)
	require.Empty(t, orphans)

	_, err = c.BeginGenerationDocument(t.Context(), "absent", doc, []string{"a"})
	require.Error(t, err)
	require.Error(t, c.CompleteGenerationDocument(t.Context(), "g2", doc, "rev", uuid.NewString(), []string{"a"}, 0, 0), "completion needs a prior intent")
	require.Error(t, c.CompleteGenerationDocument(t.Context(), "g2", doc, "rev", "", []string{"a"}, 0, 0), "completion records the observed incarnation")
}

func TestCheckpointWritesInvalidateVerificationAndRefuseLiveTargets(t *testing.T) {
	c := jobsDatabase(t)
	verifiedGeneration(t, c, "g2", "digest")
	doc := uuid.NewString()
	_, err := c.BeginGenerationDocument(t.Context(), "g2", doc, []string{"a"})
	require.NoError(t, err)
	got, err := c.Generation(t.Context(), "g2")
	require.NoError(t, err)
	require.Equal(t, StateStaging, got.State, "a write after verification invalidates it")
	require.Empty(t, got.CorpusDigest)
	require.ErrorContains(t, c.ActivateGeneration(t.Context(), "g2", "digest", 0), "verified")

	require.NoError(t, c.RecordVerification(t.Context(), "g2", "digest", json.RawMessage(`{}`)))
	require.NoError(t, c.ActivateGeneration(t.Context(), "g2", "digest", 0))
	_, err = c.BeginGenerationDocument(t.Context(), "g2", doc, []string{"a"})
	require.ErrorIs(t, err, ErrLiveTarget)
	require.ErrorIs(t, c.CompleteGenerationDocument(t.Context(), "g2", doc, "rev", uuid.NewString(), []string{"a"}, 0, 0), ErrLiveTarget)
	require.ErrorIs(t, c.ForgetGenerationDocument(t.Context(), "g2", doc), ErrLiveTarget)
}

func TestSourceCorpusRevisionsFollowStoredContent(t *testing.T) {
	c := jobsDatabase(t)
	insert := func(collection, docType, text string) string {
		id := uuid.NewString()
		_, err := c.Pool.Exec(t.Context(), `INSERT INTO documents(id,content,status,collection,doc_type,chunk_count) VALUES($1,'body','completed',$2,$3,1)`, id, collection, docType)
		require.NoError(t, err)
		_, err = c.Pool.Exec(t.Context(), `INSERT INTO chunks(id,document_id,chunk_index,text,token_count) VALUES($1,$2,0,$3,2)`, uuid.NewString(), id, text)
		require.NoError(t, err)
		return id
	}
	first := insert("a", "report", "first text")
	second := insert("b", "note", "second text")
	for _, status := range []string{"processing", "failed"} {
		_, err := c.Pool.Exec(t.Context(), `INSERT INTO documents(id,content,status) VALUES($1,'body',$2)`, uuid.NewString(), status)
		require.NoError(t, err)
	}
	page, err := c.DocumentRevisions(t.Context(), "", 10)
	require.NoError(t, err)
	require.Len(t, page, 2, "only completed documents are migrated")
	byID := map[string]DocumentRevision{}
	for _, d := range page {
		byID[d.ID] = d
		require.Len(t, d.Revision, 64)
	}
	require.Equal(t, "a", byID[first].Collection)
	require.Equal(t, "note", byID[second].DocType)
	require.Equal(t, 1, byID[first].Chunks)
	require.Equal(t, 1, byID[first].DeclaredChunks)
	var incarnation string
	require.NoError(t, c.Pool.QueryRow(t.Context(), `SELECT COALESCE(incarnation, id)::text FROM documents WHERE id=$1`, first).Scan(&incarnation))
	require.Equal(t, incarnation, byID[first].Incarnation)
	// Document identity is immutable, so a historical row is inserted without an incarnation.
	historical := uuid.NewString()
	_, err = c.Pool.Exec(t.Context(), `INSERT INTO documents(id,content,status,chunk_count,incarnation) VALUES($1,'body','completed',0,NULL)`, historical)
	require.NoError(t, err)
	page, err = c.DocumentRevisions(t.Context(), "", 10)
	require.NoError(t, err)
	for _, d := range page {
		if d.ID == historical {
			require.Equal(t, historical, d.Incarnation, "historical rows use their ID as the incarnation")
		}
	}
	_, err = c.Pool.Exec(t.Context(), `DELETE FROM documents WHERE id=$1`, historical)
	require.NoError(t, err)
	_, err = c.Pool.Exec(t.Context(), `UPDATE documents SET chunk_count=3 WHERE id=$1`, first)
	require.NoError(t, err)
	page, err = c.DocumentRevisions(t.Context(), "", 10)
	require.NoError(t, err)
	for _, d := range page {
		if d.ID == first {
			require.Equal(t, 3, d.DeclaredChunks)
			require.Equal(t, 1, d.Chunks, "an inconsistent count stays visible to the caller")
			require.Equal(t, byID[first].Revision, d.Revision, "the declared count does not change the stored content revision")
		}
	}

	limited, err := c.DocumentRevisions(t.Context(), "", 1)
	require.NoError(t, err)
	require.Len(t, limited, 1)
	rest, err := c.DocumentRevisions(t.Context(), limited[0].ID, 10)
	require.NoError(t, err)
	require.Len(t, rest, 1)
	require.NotEqual(t, limited[0].ID, rest[0].ID)

	before := byID[first].Revision
	for name, change := range map[string]string{
		"text":       `UPDATE chunks SET text='edited' WHERE document_id=$1`,
		"count":      `UPDATE chunks SET token_count=9 WHERE document_id=$1`,
		"collection": `UPDATE documents SET collection='z' WHERE id=$1`,
		"type":       `UPDATE documents SET doc_type='memo' WHERE id=$1`,
	} {
		t.Run(name, func(t *testing.T) {
			_, execErr := c.Pool.Exec(t.Context(), change, first)
			require.NoError(t, execErr)
			after, loadErr := c.DocumentRevisions(t.Context(), "", 10)
			require.NoError(t, loadErr)
			for _, d := range after {
				if d.ID == first {
					require.NotEqual(t, before, d.Revision)
					before = d.Revision
				}
			}
		})
	}

	t.Run("incarnation", func(t *testing.T) {
		// Recreating a document under the same UUID and with identical content still changes its revision.
		var chunkKey string
		require.NoError(t, c.Pool.QueryRow(t.Context(), `SELECT id::text FROM chunks WHERE document_id=$1`, first).Scan(&chunkKey))
		_, execErr := c.Pool.Exec(t.Context(), `DELETE FROM documents WHERE id=$1`, first)
		require.NoError(t, execErr)
		_, execErr = c.Pool.Exec(t.Context(), `INSERT INTO documents(id,content,status,collection,doc_type,chunk_count) VALUES($1,'body','completed','z','memo',1)`, first)
		require.NoError(t, execErr)
		_, execErr = c.Pool.Exec(t.Context(), `INSERT INTO chunks(id,document_id,chunk_index,text,token_count) VALUES($1,$2,0,'edited',9)`, chunkKey, first)
		require.NoError(t, execErr)
		after, loadErr := c.DocumentRevisions(t.Context(), "", 10)
		require.NoError(t, loadErr)
		for _, d := range after {
			if d.ID == first {
				require.NotEqual(t, before, d.Revision)
			}
		}
	})

	snapshot, err := c.LoadDocumentSnapshot(t.Context(), second)
	require.NoError(t, err)
	require.NotNil(t, snapshot)
	require.Equal(t, byID[second].Revision, snapshot.Revision)
	require.Equal(t, "note", snapshot.DocType)
	require.Len(t, snapshot.Chunks, 1)
	chunk := snapshot.Chunks[0]
	require.Len(t, chunk.ID, 32, "chunk keys use the stored undashed form")
	require.NotContains(t, string(chunk.ID), "-")
	require.Equal(t, "second text", chunk.Text)
	require.Equal(t, domain.DocumentID(second), chunk.DocumentID)

	_, err = c.Pool.Exec(t.Context(), `UPDATE documents SET status='failed' WHERE id=$1`, second)
	require.NoError(t, err)
	snapshot, err = c.LoadDocumentSnapshot(t.Context(), second)
	require.NoError(t, err)
	require.Nil(t, snapshot, "a document that is no longer completed has no snapshot")
}

func TestSourceCorpusIncludesTenantForOwnedDocuments(t *testing.T) {
	f := newOwnedFixture(t)
	req := ownedRequest(f.owner, "team", "owned corpus text")
	require.Equal(t, domain.AcceptAccepted, f.accept(t, req).Outcome)
	job := f.claim(t)
	done, err := f.c.CompleteJob(t.Context(), *job, []domain.Chunk{chunkFor(req.Document.ID, 0)}, domain.IndexObservation{})
	require.NoError(t, err)
	require.True(t, done)

	unownedID := uuid.NewString()
	_, err = f.c.Pool.Exec(t.Context(), `INSERT INTO documents(id,content,status,collection,doc_type,chunk_count) VALUES($1,'unowned','completed','','note',1)`, unownedID)
	require.NoError(t, err)
	_, err = f.c.Pool.Exec(t.Context(), `INSERT INTO chunks(id,document_id,chunk_index,text,token_count) VALUES($1,$2,0,'text',1)`, uuid.NewString(), unownedID)
	require.NoError(t, err)

	page, err := f.c.DocumentRevisions(t.Context(), "", 10)
	require.NoError(t, err)
	byID := map[string]DocumentRevision{}
	for _, rev := range page {
		byID[rev.ID] = rev
	}
	require.Equal(t, f.tenant, byID[string(req.Document.ID)].TenantID)
	require.Empty(t, byID[unownedID].TenantID)
	snap, err := f.c.LoadDocumentSnapshot(t.Context(), string(req.Document.ID))
	require.NoError(t, err)
	require.NotNil(t, snap)
	require.Equal(t, f.tenant, snap.TenantID)

	beforeRev := byID[unownedID].Revision
	f.importBatch(t, func(b map[string]any) {
		b["assignments"] = []grantEntry{grantAssignment(t, f.c, f.tenant, f.owner.ID, unownedID)}
	})
	snap, err = f.c.LoadDocumentSnapshot(t.Context(), unownedID)
	require.NoError(t, err)
	require.NotNil(t, snap)
	require.Equal(t, f.tenant, snap.TenantID)
	require.NotEqual(t, beforeRev, snap.Revision, "assigning a document updates its source revision")
}

func TestSourceCorpusSkipsDocumentsBeingDeleted(t *testing.T) {
	c := jobsDatabase(t)
	var ids []string
	for i := range 3 {
		id := uuid.NewString()
		ids = append(ids, id)
		_, err := c.Pool.Exec(t.Context(), `INSERT INTO documents(id,content,status,chunk_count) VALUES($1,'body','completed',1)`, id)
		require.NoError(t, err)
		_, err = c.Pool.Exec(t.Context(), `INSERT INTO chunks(id,document_id,chunk_index,text,token_count) VALUES($1,$2,0,$3,1)`, uuid.NewString(), id, "text")
		require.NoError(t, err, i)
	}
	receipt := `INSERT INTO document_deletions(document_id,incarnation,kind,vector_mode,finalized,held) VALUES($1,$2,'quarantine','local',$3,$4)`
	_, err := c.Pool.Exec(t.Context(), receipt, ids[0], uuid.NewString(), false, false)
	require.NoError(t, err)
	_, err = c.Pool.Exec(t.Context(), receipt, ids[1], uuid.NewString(), true, true)
	require.NoError(t, err)
	page, err := c.DocumentRevisions(t.Context(), "", 10)
	require.NoError(t, err)
	require.Len(t, page, 1, "documents with an open or held deletion receipt are not migrated")
	require.Equal(t, ids[2], page[0].ID)
	for _, id := range ids[:2] {
		snapshot, loadErr := c.LoadDocumentSnapshot(t.Context(), id)
		require.NoError(t, loadErr)
		require.Nil(t, snapshot)
	}
	_, err = c.Pool.Exec(t.Context(), `UPDATE document_deletions SET finalized=TRUE, held=FALSE WHERE document_id=$1`, ids[0])
	require.NoError(t, err)
	page, err = c.DocumentRevisions(t.Context(), "", 10)
	require.NoError(t, err)
	require.Len(t, page, 2, "a finalized receipt no longer blocks the document")
}

func TestGateToleratesASchemaThatPredatesVectorState(t *testing.T) {
	c := migrationTestClient(t, startTestDatabase(t))
	applyMigrationsExcept(t, c, 16)
	require.NoError(t, c.WritersOpen(t.Context()), "no record can freeze writers before migration 016")
	accepted, err := c.AcceptDocument(t.Context(), jobsRequest("accepted before the generation records exist"))
	require.NoError(t, err)
	require.Equal(t, domain.AcceptAccepted, accepted.Outcome)
	job, err := c.ClaimJob(t.Context(), "worker", time.Minute, 5)
	require.NoError(t, err)
	require.NotNil(t, job)

	c.SetVectorGeneration("g2")
	require.ErrorIs(t, c.WritersOpen(t.Context()), domain.ErrGenerationMismatch, "a named generation cannot be served without its record")
	c.SetVectorGeneration(LegacyGeneration)

	require.NoError(t, c.RunMigrations(t.Context()))
	require.NoError(t, c.FreezeWriters(t.Context(), "g2"))
	require.ErrorIs(t, c.WritersOpen(t.Context()), domain.ErrWritersFrozen)
}

func TestOrphansFollowSourceEligibility(t *testing.T) {
	c := jobsDatabase(t)
	_, err := c.RegisterGeneration(t.Context(), stagingGeneration("g2"))
	require.NoError(t, err)
	ids := map[string]string{}
	for _, name := range []string{"current", "failed", "deleting", "removed"} {
		id := uuid.NewString()
		ids[name] = id
		status := "completed"
		if name == "failed" {
			status = "failed"
		}
		if name != "removed" {
			_, err = c.Pool.Exec(t.Context(), `INSERT INTO documents(id,content,status) VALUES($1,'body',$2)`, id, status)
			require.NoError(t, err)
		}
		_, err = c.BeginGenerationDocument(t.Context(), "g2", id, []string{name})
		require.NoError(t, err)
	}
	_, err = c.Pool.Exec(t.Context(), `INSERT INTO document_deletions(document_id,incarnation,kind,vector_mode) VALUES($1,$2,'quarantine','local')`, ids["deleting"], uuid.NewString())
	require.NoError(t, err)
	orphans, err := c.OrphanedGenerationDocuments(t.Context(), "g2", 10)
	require.NoError(t, err)
	var got []string
	for _, o := range orphans {
		got = append(got, o.ChunkIDs...)
	}
	require.ElementsMatch(t, []string{"failed", "deleting", "removed"}, got, "a completed document being deleted is not eligible either")
}
