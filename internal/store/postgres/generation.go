package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

// LegacyGeneration names the vector target used before generations existed.
const LegacyGeneration = domain.LegacyGeneration

// Generation states.
const (
	StateStaging  = "staging"
	StateVerified = "verified"
	StateActive   = "active"
	StateRetired  = "retired"
)

// Profile origins distinguish what a stored profile rests on.
const (
	OriginRecorded = "recorded"
	OriginAttested = "attested"
)

var (
	// ErrGenerationDrift marks a generation whose recorded identity differs from the requested one.
	ErrGenerationDrift = errors.New("vector generation identity drifted")
	// ErrFixtureReceipts marks a target switch that would leave an active evaluation fixture without a worker authorized to use its receipt.
	ErrFixtureReceipts = errors.New("active evaluation fixtures are bound to a different vector store")
	// ErrLiveTarget marks a write aimed at the active vector generation.
	ErrLiveTarget = errors.New("the active vector generation is protected from migration writes")
)

// ValidGenerationName reports whether name can identify a generation and name its table.
func ValidGenerationName(name string) bool { return domain.ValidGenerationName(name) }

// VectorTable returns the PostgreSQL table holding a generation's vectors.
func VectorTable(generation string) string { return domain.VectorTable(generation) }

// Generation is one vector target and the embedding profile it stores.
type Generation struct {
	Name         string
	Store        string
	Target       string
	Host         string
	Profile      domain.EmbeddingProfile
	Origin       string
	State        string
	CorpusDigest string
	VerifiedAt   *time.Time
	Verification json.RawMessage
	EverActive   bool
	PendingWrite bool
}

// VectorState is the singleton coordinating generations and the writer freeze.
type VectorState struct {
	Active           string
	Frozen           bool
	FrozenAt         *time.Time
	FreezeGeneration string
	LegacyStore      string
	LegacyTarget     string
	LegacyHost       string
}

const maxReportedOpenDeletions = 20

// Quiesce reports whether stopped writers have settled.
type Quiesce struct {
	Frozen          bool
	Settled         bool
	RunningJobs     int
	OpenDeletions   int
	OpenDeletionIDs []string `json:"open_deletion_ids,omitempty"`
}

// FormatOpenDeletions formats bounded open deletion document IDs for status and error messages.
func FormatOpenDeletions(total int, ids []string) string {
	if total <= 0 || len(ids) == 0 {
		return ""
	}
	summary := strings.Join(ids, ", ")
	if total > len(ids) {
		summary += fmt.Sprintf(", +%d more", total-len(ids))
	}
	return " (" + summary + ")"
}

// Quiet reports that a freeze is in force and nothing is still writing.
func (q Quiesce) Quiet() bool {
	return q.Frozen && q.Settled && q.RunningJobs == 0 && q.OpenDeletions == 0
}

// Checkpoint is the durable progress of one document in a target generation.
type Checkpoint struct {
	DocumentID     string
	SourceRevision string
	Incarnation    string
	ChunkIDs       []string
	Complete       bool
	Copied         int
	Embedded       int
}

func (g Generation) validate() error {
	if !ValidGenerationName(g.Name) {
		return fmt.Errorf("postgres: invalid generation name %q", g.Name)
	}
	if g.Store != "postgres" && g.Store != "pinecone" {
		return fmt.Errorf("postgres: generation store must be postgres or pinecone")
	}
	if g.Target == "" {
		return errors.New("postgres: generation target is required")
	}
	if (g.Store == "pinecone") != (g.Host != "") {
		return errors.New("postgres: a resolved host is required for Pinecone and applies only to Pinecone")
	}
	if g.Origin != OriginRecorded && g.Origin != OriginAttested {
		return fmt.Errorf("postgres: generation origin must be %q or %q", OriginRecorded, OriginAttested)
	}
	if err := g.Profile.Validate(); err != nil {
		return fmt.Errorf("postgres: %w", err)
	}
	return nil
}

type vectorPin struct {
	generation  string
	profileHash string
}

// SetVectorGeneration names the generation for clients without a serving
// profile, such as fake-mode clients. A recorded profile requires a full pin.
func (c *Client) SetVectorGeneration(name string) {
	c.vectorPin.Store(&vectorPin{generation: name})
}

func (c *Client) servingPin() vectorPin {
	if pin := c.vectorPin.Load(); pin != nil {
		return *pin
	}
	return vectorPin{generation: LegacyGeneration}
}

type queryRower interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

type ledgerDatabase interface {
	queryRower
	Begin(context.Context) (pgx.Tx, error)
	Query(context.Context, string, ...any) (pgx.Rows, error)
}

// GenerationLedger tracks write intent and completion in PostgreSQL.
type GenerationLedger struct {
	db ledgerDatabase
}

func (c *Client) gate(ctx context.Context, q queryRower) error {
	mine := c.servingPin()
	if !c.vectorStateReady.Load() {
		// A schema older than migration 016 has no freeze to honor and only the legacy target.
		var present bool
		if err := q.QueryRow(ctx, `SELECT to_regclass('vector_state') IS NOT NULL`).Scan(&present); err != nil {
			return fmt.Errorf("postgres: read vector state: %w", err)
		}
		if !present {
			if mine.generation != LegacyGeneration {
				return fmt.Errorf("%w: replica serves %q, but no generation is recorded", domain.ErrGenerationMismatch, mine.generation)
			}
			return nil
		}
		c.vectorStateReady.Store(true)
	}
	var active, profileHash string
	var frozen bool
	if err := q.QueryRow(ctx, `SELECT active, frozen, profile_hash FROM vector_writer_state()`).Scan(&active, &frozen, &profileHash); err != nil {
		return fmt.Errorf("postgres: read vector state: %w", err)
	}
	if active != mine.generation || profileHash != "" && profileHash != mine.profileHash {
		return fmt.Errorf("%w: replica serves %q with profile %q, active is %q with profile %q", domain.ErrGenerationMismatch, mine.generation, mine.profileHash, active, profileHash)
	}
	if frozen {
		return domain.ErrWritersFrozen
	}
	return c.checkAuthzMode(ctx, q)
}

// WritersOpen implements domain.WriterGate.
func (c *Client) WritersOpen(ctx context.Context) error { return c.gate(ctx, c.Pool) }

var _ domain.WriterGate = (*Client)(nil)

// AcquireWriter holds the freeze barrier across a maintenance operation.
func (c *Client) AcquireWriter(ctx context.Context) (func(), error) {
	select {
	case c.writerSlots <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	// The lock must not consume a pool connection while the operation uses
	// the pool for document metadata or while a freeze is waiting for it.
	conn, err := pgx.ConnectConfig(ctx, c.Pool.Config().ConnConfig)
	if err != nil {
		<-c.writerSlots
		return nil, err
	}
	tx, err := conn.Begin(ctx)
	release := func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		if tx != nil {
			_ = tx.Rollback(cleanupCtx)
		}
		_ = conn.Close(cleanupCtx)
		<-c.writerSlots
	}
	if err != nil {
		release()
		return nil, err
	}
	if err = c.gate(ctx, tx); err != nil {
		release()
		return nil, err
	}
	return release, nil
}

var _ domain.WriterLocker = (*Client)(nil)

// VectorState reads the active generation and the freeze flag.
func (c *Client) VectorState(ctx context.Context) (VectorState, error) {
	return readVectorState(ctx, c.Pool)
}

func readVectorState(ctx context.Context, q queryRower) (VectorState, error) {
	var s VectorState
	err := scanVectorState(q.QueryRow(ctx, `SELECT `+vectorStateColumns+` FROM vector_state`), &s)
	if err != nil {
		return s, fmt.Errorf("postgres: read vector state: %w", err)
	}
	return s, nil
}

const vectorStateColumns = `active_generation, frozen, frozen_at, COALESCE(freeze_generation, ''),
 COALESCE(legacy_store, ''), COALESCE(legacy_target, ''), legacy_host`

func scanVectorState(row pgx.Row, state *VectorState) error {
	return row.Scan(&state.Active, &state.Frozen, &state.FrozenAt, &state.FreezeGeneration, &state.LegacyStore, &state.LegacyTarget, &state.LegacyHost)
}

// Generation returns a registered generation, or nil when it is not registered.
func (c *Client) Generation(ctx context.Context, name string) (*Generation, error) {
	return readGeneration(ctx, c.Pool, name, "")
}

func readGeneration(ctx context.Context, q queryRower, name, lock string) (*Generation, error) {
	var g Generation
	var profile []byte
	var digest *string
	err := q.QueryRow(ctx, `SELECT name, store, target, host, profile, profile_origin, state, corpus_digest, verified_at, COALESCE(verification, 'null'::jsonb), ever_active, pending_write
 FROM vector_generations WHERE name=$1 `+lock, name).Scan(&g.Name, &g.Store, &g.Target, &g.Host, &profile, &g.Origin, &g.State, &digest, &g.VerifiedAt, &g.Verification, &g.EverActive, &g.PendingWrite)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("postgres: read generation %q: %w", name, err)
	}
	if err = json.Unmarshal(profile, &g.Profile); err != nil {
		return nil, fmt.Errorf("postgres: decode generation %q profile: %w", name, err)
	}
	if digest != nil {
		g.CorpusDigest = *digest
	}
	return &g, nil
}

// RegisterGeneration records a generation, or confirms that an existing record
// is identical. A different store, target or profile fails closed.
func (c *Client) RegisterGeneration(ctx context.Context, g Generation) (Generation, error) {
	if err := g.validate(); err != nil {
		return Generation{}, err
	}
	profile, err := json.Marshal(g.Profile)
	if err != nil {
		return Generation{}, fmt.Errorf("postgres: encode generation profile: %w", err)
	}
	tx, err := c.Pool.Begin(ctx)
	if err != nil {
		return Generation{}, err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if g.Name == LegacyGeneration {
		if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, ingestAdmissionLock); err != nil {
			return Generation{}, fmt.Errorf("postgres: wait for admissions before attestation: %w", err)
		}
	}
	state, err := lockVectorState(ctx, tx)
	if err != nil {
		return Generation{}, err
	}
	if state.LegacyStore != "" {
		sameName := g.Store == state.LegacyStore && g.Target == state.LegacyTarget
		sameHost := g.Host == state.LegacyHost
		if g.Name == LegacyGeneration && (!sameName || !sameHost) ||
			g.Name != LegacyGeneration && (sameName || g.Host != "" && sameHost) {
			return Generation{}, fmt.Errorf("%w: %s %q and the recorded legacy target belong to different generations", ErrGenerationDrift, g.Store, g.Target)
		}
	}
	initial := StateStaging
	if state.Active == g.Name {
		initial = StateActive
	}
	tag, err := tx.Exec(ctx, `INSERT INTO vector_generations(name, store, target, host, profile, profile_hash, profile_origin, state, ever_active)
 VALUES($1,$2,$3,$4,$5,$6,$7,$8,$8='active') ON CONFLICT (name) DO NOTHING`, g.Name, g.Store, g.Target, g.Host, profile, g.Profile.Hash(), g.Origin, initial)
	if err != nil {
		if isUniqueViolation(err) {
			return Generation{}, fmt.Errorf("%w: target %s %q already belongs to another generation", ErrGenerationDrift, g.Store, g.Target)
		}
		return Generation{}, fmt.Errorf("postgres: register generation %q: %w", g.Name, err)
	}
	got, err := readGeneration(ctx, tx, g.Name, "")
	if err != nil {
		return Generation{}, err
	}
	if got.Store != g.Store || got.Target != g.Target || got.Host != g.Host || got.Profile.Hash() != g.Profile.Hash() {
		return Generation{}, fmt.Errorf("%w: generation %q is already registered with a different store, target or profile", ErrGenerationDrift, g.Name)
	}
	if initial == StateActive && tag.RowsAffected() != 0 {
		// The state lock drains synchronous mutations and prevents new claims.
		// Existing lease holders must finish before publishing the profile.
		var running bool
		if err = tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM ingest_jobs WHERE state='running')`).Scan(&running); err != nil {
			return Generation{}, fmt.Errorf("postgres: inspect job leases before attestation: %w", err)
		}
		if running {
			return Generation{}, errors.New("postgres: wait for in-flight jobs before attesting the active generation")
		}
		if _, err = tx.Exec(ctx, `UPDATE vector_state SET active_profile_hash=$1`, g.Profile.Hash()); err != nil {
			return Generation{}, fmt.Errorf("postgres: publish attested profile: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return Generation{}, err
	}
	return *got, nil
}

type legacyBindMode uint8

const (
	legacyBindExisting legacyBindMode = iota
	legacyBindAlways
	legacyBindExistingPostgresTable
)

// PinVectorGeneration fails closed unless the replica's configured generation
// is the active one and its target and embedding settings match the record.
func (c *Client) PinVectorGeneration(ctx context.Context, generation, store, target, host string, configured domain.EmbeddingProfile) error {
	return c.pinVectorGeneration(ctx, generation, store, target, host, configured, legacyBindAlways)
}

// PinExistingVectorGeneration pins maintenance to the binding established by serving startup.
func (c *Client) PinExistingVectorGeneration(ctx context.Context, generation, store, target, host string, configured domain.EmbeddingProfile) error {
	return c.pinVectorGeneration(ctx, generation, store, target, host, configured, legacyBindExisting)
}

// PinCleanupVectorGeneration pins document cleanup to the active binding, binding
// an existing legacy chunk_vectors table on first PostgreSQL use.
func (c *Client) PinCleanupVectorGeneration(ctx context.Context, generation, store, target, host string, configured domain.EmbeddingProfile) error {
	return c.pinVectorGeneration(ctx, generation, store, target, host, configured, legacyBindExistingPostgresTable)
}

func (c *Client) pinVectorGeneration(ctx context.Context, generation, store, target, host string, configured domain.EmbeddingProfile, bindMode legacyBindMode) error {
	tx, err := c.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	switch bindMode {
	case legacyBindAlways:
		if generation == LegacyGeneration {
			if _, err = tx.Exec(ctx, `SELECT vector_bind_legacy($1,$2,$3)`, store, target, host); err != nil {
				return fmt.Errorf("postgres: bind legacy target: %w", err)
			}
		}
	case legacyBindExistingPostgresTable:
		if generation == LegacyGeneration && store == "postgres" && target == VectorTable(LegacyGeneration) {
			if _, err = tx.Exec(ctx, `SELECT vector_bind_legacy('postgres','chunk_vectors','')
FROM pg_attribute a
WHERE a.attrelid = to_regclass(format('%I.chunk_vectors', current_schema()))
  AND a.attname = 'embedding' AND NOT a.attisdropped AND a.atttypmod > 0
  AND a.atttypid = to_regtype('vector')`); err != nil {
				return fmt.Errorf("postgres: bind existing legacy vector table: %w", err)
			}
		}
	case legacyBindExisting:
	}
	var active, activeHash, recordedStore, recordedTarget, recordedHost string
	err = tx.QueryRow(ctx, `SELECT active, profile_hash, store, target, host FROM vector_writer_state()`).
		Scan(&active, &activeHash, &recordedStore, &recordedTarget, &recordedHost)
	if err != nil {
		return fmt.Errorf("postgres: read active vector target: %w", err)
	}
	if active != generation {
		return fmt.Errorf("%w: configured generation %q, active generation %q", domain.ErrGenerationMismatch, generation, active)
	}
	if recordedStore == "" || recordedTarget == "" {
		return fmt.Errorf("%w: start a serving replica to bind the physical target", ErrGenerationDrift)
	}
	if recordedStore != store || recordedTarget != target || recordedHost != host {
		return fmt.Errorf("%w: generation %q targets %s %q (%s), configured %s %q (%s)", ErrGenerationDrift, generation, recordedStore, recordedTarget, recordedHost, store, target, host)
	}
	profileHash := configured.Hash()
	if activeHash != "" && activeHash != profileHash {
		return fmt.Errorf("%w: generation %q records profile %s, configured %s", ErrGenerationDrift, generation, activeHash, profileHash)
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	c.vectorPin.Store(&vectorPin{generation: generation, profileHash: profileHash})
	return nil
}

// FreezeWriters pauses admission, job claims and deletions for a migration.
// It publishes the freeze only after admitted writes have crossed the barrier.
func (c *Client) FreezeWriters(ctx context.Context, generation string) error {
	if !ValidGenerationName(generation) {
		return fmt.Errorf("postgres: invalid generation name %q", generation)
	}
	tx, err := c.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if _, err = tx.Exec(ctx, `SET LOCAL statement_timeout=0`); err != nil {
		return fmt.Errorf("postgres: configure writer drain: %w", err)
	}
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, ingestAdmissionLock); err != nil {
		return fmt.Errorf("postgres: wait for admissions: %w", err)
	}
	// Admissions are blocked now, so no fixture can appear before the freeze is published.
	if fixtureErr := requireServiceableFixtures(ctx, tx, generation); fixtureErr != nil {
		return fixtureErr
	}
	tag, err := tx.Exec(ctx, `UPDATE vector_state SET frozen=TRUE, frozen_at=CASE WHEN frozen THEN frozen_at ELSE clock_timestamp() END, freeze_generation=$1
 WHERE NOT frozen OR freeze_generation=$1`, generation)
	if err != nil {
		return fmt.Errorf("postgres: freeze writers: %w", err)
	}
	if tag.RowsAffected() == 0 {
		state, stateErr := readVectorState(ctx, tx)
		if stateErr != nil {
			return stateErr
		}
		return fmt.Errorf("postgres: writers are already frozen for generation %q", state.FreezeGeneration)
	}
	return tx.Commit(ctx)
}

// ThawWriters resumes admission, job claims and deletions.
func (c *Client) ThawWriters(ctx context.Context) error {
	state, err := c.VectorState(ctx)
	if err != nil || !state.Frozen {
		return err
	}
	fence, err := c.FenceGeneration(ctx, state.FreezeGeneration)
	if err != nil {
		return err
	}
	defer fence.Close(ctx)
	if _, err = fence.conn.Exec(ctx, `WITH opened AS (
 UPDATE vector_state SET frozen=FALSE, frozen_at=NULL, freeze_generation=NULL
 WHERE frozen_at=$1 AND freeze_generation=$2 RETURNING active_generation
) UPDATE vector_generations SET state=CASE WHEN ever_active THEN 'retired' ELSE 'staging' END,
 corpus_digest=NULL, verification=NULL, verified_at=NULL, updated_at=NOW()
 WHERE state='verified' AND name <> (SELECT active_generation FROM opened)`, state.FrozenAt, state.FreezeGeneration); err != nil {
		return fmt.Errorf("postgres: resume writers: %w", err)
	}
	return nil
}

// WriterQuiesce reports whether the freeze has settled for at least window and
// no leased job or deletion receipt remains open.
func (c *Client) WriterQuiesce(ctx context.Context, window time.Duration) (Quiesce, error) {
	var q Quiesce
	err := c.Pool.QueryRow(ctx, `SELECT frozen, COALESCE(frozen_at + make_interval(secs=>$1) <= NOW(), FALSE),
 (SELECT count(*) FROM ingest_jobs WHERE state='running' AND lease_until > NOW()),
 (SELECT count(*) FROM document_deletions WHERE NOT finalized),
 ARRAY(SELECT document_id::text FROM document_deletions WHERE NOT finalized ORDER BY document_id LIMIT $2)
 FROM vector_state`, window.Seconds(), maxReportedOpenDeletions).
		Scan(&q.Frozen, &q.Settled, &q.RunningJobs, &q.OpenDeletions, &q.OpenDeletionIDs)
	if err != nil {
		return q, fmt.Errorf("postgres: read writer state: %w", err)
	}
	if len(q.OpenDeletionIDs) == 0 {
		q.OpenDeletionIDs = nil
	}
	return q, nil
}

func frozenFor(state VectorState, generation string) error {
	switch {
	case !state.Frozen:
		return fmt.Errorf("postgres: writers are not frozen for generation %q", generation)
	case state.FreezeGeneration != generation:
		return fmt.Errorf("postgres: writers are frozen for generation %q, not %q", state.FreezeGeneration, generation)
	}
	return nil
}

// RecordVerification stores a passing verification of a generation under the freeze.
func (c *Client) RecordVerification(ctx context.Context, generation, corpusDigest string, report json.RawMessage) error {
	fence, err := c.FenceGeneration(ctx, generation)
	if err != nil {
		return err
	}
	defer fence.Close(ctx)
	return fence.RecordVerification(ctx, time.Time{}, corpusDigest, report)
}

// BeginVerification revokes any approval before checking the target again.
func (f *GenerationFence) BeginVerification(ctx context.Context) (time.Time, error) {
	tx, err := f.conn.Begin(ctx)
	if err != nil {
		return time.Time{}, err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	state, err := lockVectorState(ctx, tx)
	if err != nil {
		return time.Time{}, err
	}
	if freezeErr := frozenFor(state, f.name); freezeErr != nil {
		return time.Time{}, freezeErr
	}
	tag, err := tx.Exec(ctx, `UPDATE vector_generations
 SET state=CASE WHEN ever_active THEN 'retired' ELSE 'staging' END,
 corpus_digest=NULL, verification=NULL, verified_at=NULL, updated_at=NOW()
 WHERE name=$1 AND state <> 'active'`, f.name)
	if err != nil {
		return time.Time{}, fmt.Errorf("postgres: invalidate verification: %w", err)
	}
	if tag.RowsAffected() != 1 || state.FrozenAt == nil {
		return time.Time{}, fmt.Errorf("postgres: cannot begin verification of generation %q", f.name)
	}
	return *state.FrozenAt, tx.Commit(ctx)
}

// RecordVerification approves this target only under the same writer freeze.
func (f *GenerationFence) RecordVerification(ctx context.Context, frozenAt time.Time, corpusDigest string, report json.RawMessage) error {
	if corpusDigest == "" {
		return errors.New("postgres: a verification needs a corpus digest")
	}
	tx, err := f.conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	state, err := lockVectorState(ctx, tx)
	if err != nil {
		return err
	}
	if freezeErr := frozenFor(state, f.name); freezeErr != nil {
		return freezeErr
	}
	if !frozenAt.IsZero() && (state.FrozenAt == nil || !state.FrozenAt.Equal(frozenAt)) {
		return errors.New("postgres: writers were thawed during verification")
	}
	tag, err := tx.Exec(ctx, `UPDATE vector_generations SET state='verified', corpus_digest=$2, verification=$3, verified_at=NOW(), updated_at=NOW()
 WHERE name=$1 AND state IN ('staging','verified','retired') AND NOT pending_write`, f.name, corpusDigest, []byte(report))
	if err != nil {
		return fmt.Errorf("postgres: record verification: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("postgres: generation %q is missing, already active or has an unconfirmed target write", f.name)
	}
	return tx.Commit(ctx)
}

// requireServiceableFixtures refuses to move serving to a generation that binds
// a different physical store than an active evaluation fixture. The fixture's
// jobs and its receipt-qualified deletion belong to workers attached to the
// bound store, and the replicas of the previous generation are fenced after the
// switch. An unknown legacy binding matches no store. Deleting a fixture by its
// original receipt retires the reservation, so it no longer blocks the switch.
// A generation that is unregistered or already active changes nothing.
func requireServiceableFixtures(ctx context.Context, q queryRower, generation string) error {
	state, err := readVectorState(ctx, q)
	if err != nil {
		return err
	}
	if state.Active == generation {
		return nil
	}
	target, err := readGeneration(ctx, q, generation, "")
	if err != nil || target == nil {
		return err
	}
	var stranded int
	err = q.QueryRow(ctx, `SELECT count(*) FROM evaluation_fixtures WHERE active_document_id IS NOT NULL AND vector_identity IS DISTINCT FROM $1`,
		domain.FixtureVectorIdentity(target.Store, target.Target, target.Host)).Scan(&stranded)
	if err != nil {
		return fmt.Errorf("postgres: read evaluation fixture bindings: %w", err)
	}
	if stranded > 0 {
		return fmt.Errorf("%w: %d active fixtures would lose their receipts on generation %q; thaw writers if they are frozen, delete each fixture by its original receipt and retry", ErrFixtureReceipts, stranded, generation)
	}
	return nil
}

func lockVectorState(ctx context.Context, tx pgx.Tx) (VectorState, error) {
	var s VectorState
	err := scanVectorState(tx.QueryRow(ctx, `SELECT `+vectorStateColumns+` FROM vector_state FOR UPDATE`), &s)
	if err != nil {
		return s, fmt.Errorf("postgres: lock vector state: %w", err)
	}
	return s, nil
}

// ActivateGeneration makes a verified generation the one replicas must serve.
// It requires the freeze for that generation, a settled and idle system, and a
// corpus unchanged since verification. Writers stay frozen until ThawWriters.
func (c *Client) ActivateGeneration(ctx context.Context, name, corpusDigest string, window time.Duration) error {
	fence, err := c.FenceGeneration(ctx, name)
	if err != nil {
		return err
	}
	defer fence.Close(ctx)
	return fence.Activate(ctx, corpusDigest, window)
}

// Activate makes the fenced target active.
func (f *GenerationFence) Activate(ctx context.Context, corpusDigest string, window time.Duration) error {
	name := f.name
	tx, err := f.conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	state, err := lockVectorState(ctx, tx)
	if err != nil {
		return err
	}
	target, err := readGeneration(ctx, tx, name, "FOR UPDATE")
	if err != nil {
		return err
	}
	if target == nil {
		return fmt.Errorf("postgres: generation %q is not registered", name)
	}
	if state.Active == name {
		return fmt.Errorf("postgres: generation %q is already active", name)
	}
	if freezeErr := frozenFor(state, name); freezeErr != nil {
		return freezeErr
	}
	if fixtureErr := requireServiceableFixtures(ctx, tx, name); fixtureErr != nil {
		return fixtureErr
	}
	var settled bool
	var running, open int
	var openIDs []string
	err = tx.QueryRow(ctx, `SELECT frozen_at + make_interval(secs=>$1) <= NOW(),
 (SELECT count(*) FROM ingest_jobs WHERE state='running' AND lease_until > NOW()),
 (SELECT count(*) FROM document_deletions WHERE NOT finalized),
 ARRAY(SELECT document_id::text FROM document_deletions WHERE NOT finalized ORDER BY document_id LIMIT $2)
 FROM vector_state`, window.Seconds(), maxReportedOpenDeletions).Scan(&settled, &running, &open, &openIDs)
	if err != nil {
		return fmt.Errorf("postgres: read writer state: %w", err)
	}
	if !settled {
		return fmt.Errorf("postgres: the freeze is still settling; wait %s after freezing", window)
	}
	if running > 0 || open > 0 {
		return fmt.Errorf("postgres: %d in-flight jobs and %d open deletions%s must finish before activation",
			running, open, FormatOpenDeletions(open, openIDs))
	}
	if target.State != StateVerified || target.PendingWrite {
		return fmt.Errorf("postgres: generation %q is %s; it must be verified", name, target.State)
	}
	if target.CorpusDigest != corpusDigest {
		return fmt.Errorf("postgres: corpus changed since verification of generation %q", name)
	}
	if _, err = tx.Exec(ctx, `UPDATE vector_generations SET state='retired', updated_at=NOW() WHERE name=$1 AND state='active'`, state.Active); err != nil {
		return fmt.Errorf("postgres: retire generation %q: %w", state.Active, err)
	}
	if _, err = tx.Exec(ctx, `UPDATE vector_generations SET state='active', ever_active=TRUE, updated_at=NOW() WHERE name=$1`, name); err != nil {
		return fmt.Errorf("postgres: activate generation %q: %w", name, err)
	}
	if _, err = tx.Exec(ctx, `UPDATE vector_state SET active_generation=$1, active_profile_hash=$2`, name, target.Profile.Hash()); err != nil {
		return fmt.Errorf("postgres: record active generation: %w", err)
	}
	return tx.Commit(ctx)
}

// guardGenerationWrite locks the generation row and refuses live or missing targets.
// A write invalidates any earlier verification.
func guardGenerationWrite(ctx context.Context, tx pgx.Tx, generation string) error {
	state, err := readVectorState(ctx, tx)
	if err != nil {
		return err
	}
	if state.Active == generation {
		return fmt.Errorf("%w: %q", ErrLiveTarget, generation)
	}
	tag, err := tx.Exec(ctx, `UPDATE vector_generations SET state='staging', corpus_digest=NULL, verification=NULL, verified_at=NULL, updated_at=NOW()
 WHERE name=$1 AND state IN ('staging','verified','retired') AND NOT pending_write`, generation)
	if err != nil {
		return fmt.Errorf("postgres: mark generation %q staging: %w", generation, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("postgres: generation %q is missing or has an unconfirmed target write", generation)
	}
	return nil
}

// GenerationCheckpoints returns the ledger entries for the supplied documents.
func (l GenerationLedger) GenerationCheckpoints(ctx context.Context, generation string, documentIDs []string) (map[string]Checkpoint, error) {
	out := make(map[string]Checkpoint, len(documentIDs))
	if len(documentIDs) == 0 {
		return out, nil
	}
	rows, err := l.db.Query(ctx, `SELECT document_id::text, source_revision, COALESCE(incarnation::text, ''), chunk_ids, complete, copied, embedded
 FROM vector_generation_documents WHERE generation=$1 AND document_id=ANY($2::uuid[])`, generation, documentIDs)
	if err != nil {
		return nil, fmt.Errorf("postgres: read generation checkpoints: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var cp Checkpoint
		if err = rows.Scan(&cp.DocumentID, &cp.SourceRevision, &cp.Incarnation, &cp.ChunkIDs, &cp.Complete, &cp.Copied, &cp.Embedded); err != nil {
			return nil, fmt.Errorf("postgres: read generation checkpoint: %w", err)
		}
		out[cp.DocumentID] = cp
	}
	return out, rows.Err()
}

// BeginGenerationDocument records the intent to write keys for a document and
// returns every key the ledger now holds for it. The recorded set only grows
// until completion, so an interrupted write never strands a key the ledger
// cannot find.
func (l GenerationLedger) BeginGenerationDocument(ctx context.Context, generation, documentID string, chunkIDs []string) ([]string, error) {
	tx, err := l.db.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if guardErr := guardGenerationWrite(ctx, tx, generation); guardErr != nil {
		return nil, guardErr
	}
	var existing []string
	err = tx.QueryRow(ctx, `SELECT chunk_ids FROM vector_generation_documents WHERE generation=$1 AND document_id=$2 FOR UPDATE`, generation, documentID).Scan(&existing)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("postgres: read generation ledger: %w", err)
	}
	union := slices.Clone(existing)
	union = append(union, chunkIDs...)
	slices.Sort(union)
	union = slices.Compact(union)
	if _, err = tx.Exec(ctx, `INSERT INTO vector_generation_documents(generation, document_id, source_revision, chunk_ids, complete)
 VALUES($1,$2,'',$3,FALSE)
 ON CONFLICT (generation, document_id) DO UPDATE SET source_revision='', incarnation=NULL, chunk_ids=EXCLUDED.chunk_ids, complete=FALSE, copied=0, embedded=0, checkpointed_at=NOW()`, generation, documentID, union); err != nil {
		return nil, fmt.Errorf("postgres: record generation intent: %w", err)
	}
	return union, tx.Commit(ctx)
}

// CompleteGenerationDocument marks a document's target keys current for a source revision.
func (l GenerationLedger) CompleteGenerationDocument(ctx context.Context, generation, documentID, revision, incarnation string, chunkIDs []string, copied, embedded int) error {
	if revision == "" || incarnation == "" {
		return errors.New("postgres: a checkpoint needs a source revision and incarnation")
	}
	tx, err := l.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if guardErr := guardGenerationWrite(ctx, tx, generation); guardErr != nil {
		return guardErr
	}
	ids := slices.Clone(chunkIDs)
	slices.Sort(ids)
	tag, err := tx.Exec(ctx, `UPDATE vector_generation_documents SET source_revision=$3, incarnation=$4::uuid, chunk_ids=$5, complete=TRUE, copied=$6, embedded=$7, checkpointed_at=NOW()
 WHERE generation=$1 AND document_id=$2`, generation, documentID, revision, incarnation, ids, copied, embedded)
	if err != nil {
		return fmt.Errorf("postgres: complete generation checkpoint: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("postgres: no write intent recorded for document %s", documentID)
	}
	return tx.Commit(ctx)
}

// CountGenerationDocuments counts ledger entries by completion.
func (c *Client) CountGenerationDocuments(ctx context.Context, generation string) (complete, incomplete int, err error) {
	err = c.Pool.QueryRow(ctx, `SELECT count(*) FILTER (WHERE complete), count(*) FILTER (WHERE NOT complete)
 FROM vector_generation_documents WHERE generation=$1`, generation).Scan(&complete, &incomplete)
	if err != nil {
		return 0, 0, fmt.Errorf("postgres: count generation documents: %w", err)
	}
	return complete, incomplete, nil
}

// OrphanedGenerationDocuments lists ledger entries whose document is no longer
// an eligible completed document in the source, so their target keys must be removed.
func (l GenerationLedger) OrphanedGenerationDocuments(ctx context.Context, generation string, limit int) ([]Checkpoint, error) {
	rows, err := l.db.Query(ctx, `SELECT l.document_id::text, l.source_revision, COALESCE(l.incarnation::text, ''), l.chunk_ids, l.complete, l.copied, l.embedded
 `+orphanedLedger+` ORDER BY l.document_id LIMIT $2`, generation, limit)
	if err != nil {
		return nil, fmt.Errorf("postgres: list orphaned generation documents: %w", err)
	}
	defer rows.Close()
	var out []Checkpoint
	for rows.Next() {
		var cp Checkpoint
		if err = rows.Scan(&cp.DocumentID, &cp.SourceRevision, &cp.Incarnation, &cp.ChunkIDs, &cp.Complete, &cp.Copied, &cp.Embedded); err != nil {
			return nil, fmt.Errorf("postgres: read orphaned generation document: %w", err)
		}
		out = append(out, cp)
	}
	return out, rows.Err()
}

const orphanedLedger = `FROM vector_generation_documents l
 WHERE l.generation=$1 AND NOT EXISTS (SELECT 1 FROM documents d WHERE d.id=l.document_id AND ` + eligibleDocument + `)`

// HasOrphanedGenerationDocuments checks for remaining cleanup without loading keys.
func (l GenerationLedger) HasOrphanedGenerationDocuments(ctx context.Context, generation string) (bool, error) {
	var found bool
	err := l.db.QueryRow(ctx, `SELECT EXISTS(SELECT 1 `+orphanedLedger+`)`, generation).Scan(&found)
	return found, err
}

// CountOrphanedGenerationDocuments counts every ledger entry whose document is
// no longer an eligible source, without loading its keys.
func (l GenerationLedger) CountOrphanedGenerationDocuments(ctx context.Context, generation string) (int, error) {
	var n int
	if err := l.db.QueryRow(ctx, `SELECT count(*) `+orphanedLedger, generation).Scan(&n); err != nil {
		return 0, fmt.Errorf("postgres: count orphaned generation documents: %w", err)
	}
	return n, nil
}

// ForgetGenerationDocument removes a ledger entry after its target keys are gone.
func (l GenerationLedger) ForgetGenerationDocument(ctx context.Context, generation, documentID string) error {
	tx, err := l.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if guardErr := guardGenerationWrite(ctx, tx, generation); guardErr != nil {
		return guardErr
	}
	if _, err = tx.Exec(ctx, `DELETE FROM vector_generation_documents WHERE generation=$1 AND document_id=$2`, generation, documentID); err != nil {
		return fmt.Errorf("postgres: forget generation document: %w", err)
	}
	return tx.Commit(ctx)
}
