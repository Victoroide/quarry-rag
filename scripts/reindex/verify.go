package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"slices"
	"sort"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	pcstore "github.com/Victoroide/quarry-rag/internal/store/pinecone"
	pgstore "github.com/Victoroide/quarry-rag/internal/store/postgres"
)

var errNotVerified = errors.New("the target did not pass verification")

type verifyReport struct {
	Action       string           `json:"action"`
	Generation   string           `json:"generation"`
	ProfileHash  string           `json:"profile_hash"`
	CorpusDigest string           `json:"corpus_digest"`
	Passed       bool             `json:"passed"`
	Recorded     bool             `json:"recorded"`
	Failures     []string         `json:"failures"`
	Expected     expectedCounts   `json:"expected"`
	Coverage     coverageReport   `json:"coverage"`
	Unexpected   unexpectedReport `json:"unexpected"`
	Sample       sampleReport     `json:"sample"`
}

type expectedCounts struct {
	Documents int `json:"documents"`
	Chunks    int `json:"chunks"`
}

type coverageReport struct {
	KeyCoverage       float64 `json:"key_coverage"`
	ProfileCoverage   float64 `json:"profile_coverage"`
	Missing           int     `json:"missing_keys"`
	BadVectors        int     `json:"invalid_vectors"`
	Mismatched        int     `json:"mismatched_records"`
	DocumentsMissing  int     `json:"documents_without_checkpoint"`
	DocumentsStale    int     `json:"documents_with_stale_checkpoint"`
	Unstable          int     `json:"documents_changed_during_scan"`
	Inconsistent      int     `json:"inconsistent_documents"`
	TaggedVectors     int     `json:"documents_with_tagged_vectors"`
	ManifestedVectors int     `json:"documents_with_manifested_vectors,omitempty"`
	NotReembedded     int     `json:"documents_without_full_reembedding,omitempty"`
}

type unexpectedReport struct {
	Checked bool     `json:"listing_checked"`
	Records int      `json:"out_of_scope_records"`
	Sample  []string `json:"sample"`
}

type sampleReport struct {
	Requested       int      `json:"requested"`
	Queries         int      `json:"queries"`
	Skipped         int      `json:"skipped"`
	MeanRecall      float64  `json:"mean_recall"`
	HitRate         float64  `json:"self_hit_rate"`
	ForbiddenHits   int      `json:"forbidden_hits"`
	P95Milliseconds float64  `json:"p95_ms"`
	MinRecall       float64  `json:"min_recall"`
	MaxP95          string   `json:"max_p95"`
	TopK            int      `json:"top_k"`
	ScopeDocuments  int      `json:"scope_documents"`
	SourceProfile   string   `json:"source_profile_hash,omitempty"`
	SourceQueryOnly bool     `json:"source_profile_query_only,omitempty"`
	ChunkIDs        []string `json:"chunk_ids,omitempty"`
}

type scopeDoc struct{ id, chunkID, revisionID, collection string }

type sampleChunk struct {
	id, doc, collection string
	index               int
	textHash            [sha256.Size]byte
}

// verifier compares a staged target with PostgreSQL, the authority, and with the
// current target, using only scoped queries.
type verifier struct {
	corpus          corpus
	ledger          ledger
	target          vectorReader
	source          vectorReader
	generation      string
	profile         domain.EmbeddingProfile
	sourceProfile   domain.EmbeddingProfile
	sourceQueryOnly bool
	// taggedTarget is true for a store whose records carry the document
	// incarnation they were written for, so each record must name it.
	taggedTarget bool
	opts         options
	now          func() time.Time
}

func (v *verifier) fail(rep *verifyReport, format string, args ...any) {
	rep.Failures = append(rep.Failures, fmt.Sprintf(format, args...))
}

func (v *verifier) run(ctx context.Context) (verifyReport, error) {
	rep := verifyReport{Action: string(actionVerify), Generation: v.generation, ProfileHash: v.profile.Hash(), Failures: []string{}}
	rep.Sample = sampleReport{Requested: v.opts.SampleSize, MinRecall: v.opts.MinRecall, MaxP95: v.opts.MaxP95.String(), TopK: v.opts.TopK, ScopeDocuments: v.opts.ScopeDocuments, SourceQueryOnly: v.sourceQueryOnly}
	rep.Unexpected.Sample = []string{}
	expected := map[string]scopeDoc{}
	var sample []sampleChunk
	var docs []scopeDoc
	digest := sha256.New()
	var chunkCount, loadedChunks int
	var textBytes int64
	valid, present := 0, 0
	after := ""
scan:
	for rep.Expected.Documents < v.opts.MaxDocuments {
		limit := min(v.opts.Page, v.opts.MaxDocuments-rep.Expected.Documents)
		page, err := v.corpus.DocumentRevisions(ctx, after, limit)
		if err != nil {
			return rep, err
		}
		ids := make([]string, len(page))
		for i, rev := range page {
			ids[i] = rev.ID
		}
		checkpoints, err := v.ledger.GenerationCheckpoints(ctx, v.generation, ids)
		if err != nil {
			return rep, err
		}
		for _, rev := range page {
			after = rev.ID
			rep.Expected.Documents++
			_, _ = fmt.Fprintf(digest, "%s|%s\n", rev.ID, rev.Revision)
			docs = append(docs, scopeDoc{id: rev.ID, collection: rev.Collection})
			if rev.DeclaredChunks != rev.Chunks {
				rep.Coverage.Inconsistent++
				continue
			}
			if rev.ScopedVectors {
				rep.Coverage.TaggedVectors++
				continue
			}
			if !v.taggedTarget && rev.ActiveRevisionID != "" {
				rep.Coverage.ManifestedVectors++
				continue
			}
			cp, seen := checkpoints[rev.ID]
			switch {
			case !seen || !cp.Complete:
				rep.Coverage.DocumentsMissing++
			case cp.SourceRevision != rev.Revision || cp.Incarnation != rev.Incarnation:
				rep.Coverage.DocumentsStale++
			case v.sourceQueryOnly && (cp.Copied != 0 || cp.Embedded != rev.Chunks):
				rep.Coverage.NotReembedded++
			}
			snap, err := v.corpus.LoadDocumentSnapshotBounded(ctx, rev.ID, v.opts.MaxChunks-loadedChunks, v.opts.MaxBytes-textBytes)
			if errors.Is(err, pgstore.ErrSnapshotLimit) {
				v.fail(&rep, "verification exceeds -max-chunks or -max-bytes, so completeness cannot be proven")
				break scan
			}
			if err != nil {
				return rep, err
			}
			if snap == nil {
				rep.Coverage.Unstable++
				continue
			}
			loadedChunks += len(snap.Chunks)
			textBytes += snap.TextBytes
			if snap.Revision != rev.Revision {
				rep.Coverage.Unstable++
				continue
			}
			chunkCount += len(snap.Chunks)
			p, ok, err := v.compareDocument(ctx, snap, &rep, expected, &sample)
			if err != nil {
				return rep, err
			}
			present += p
			valid += ok
		}
		if len(page) < limit {
			break
		}
	}
	rep.Expected.Chunks = chunkCount
	if rep.Expected.Documents == v.opts.MaxDocuments && len(rep.Failures) == 0 {
		more, err := v.corpus.HasDocumentsAfter(ctx, after)
		if err != nil {
			return rep, err
		}
		if more {
			v.fail(&rep, "verification exceeds -max-documents, so completeness cannot be proven")
		}
	}
	if len(rep.Failures) != 0 {
		return rep, nil
	}
	rep.CorpusDigest = hex.EncodeToString(digest.Sum(nil))
	if chunkCount > 0 {
		rep.Coverage.KeyCoverage = float64(present) / float64(chunkCount)
		rep.Coverage.ProfileCoverage = float64(valid) / float64(chunkCount)
	}
	if err := v.listUnexpected(ctx, expected, &rep); err != nil {
		return rep, err
	}
	if err := v.matchedQueries(ctx, rep.CorpusDigest, sample, docs, expected, &rep); err != nil {
		return rep, err
	}
	v.judge(&rep)
	return rep, nil
}

// compareDocument checks every chunk of a document against the target and
// returns how many keys are present and how many hold a valid, matching record.
func (v *verifier) compareDocument(ctx context.Context, snap *pgstore.DocumentSnapshot, rep *verifyReport, expected map[string]scopeDoc, sample *[]sampleChunk) (present, valid int, err error) {
	ids := make([]string, len(snap.Chunks))
	for i, c := range snap.Chunks {
		ids[i] = targetVectorID(snap.ID, snap.ActiveRevisionID, c, v.taggedTarget)
		expected[ids[i]] = scopeDoc{id: snap.ID, chunkID: string(c.ID), revisionID: snap.ActiveRevisionID, collection: snap.Collection}
		*sample = append(*sample, sampleChunk{id: ids[i], doc: snap.ID, collection: snap.Collection, index: c.Index, textHash: sha256.Sum256([]byte(c.Text))})
	}
	got := make(map[string]domain.Chunk, len(ids))
	for start := 0; start < len(ids); start += pcstore.FetchBatchSize {
		part, fetchErr := v.target.Fetch(ctx, ids[start:min(start+pcstore.FetchBatchSize, len(ids))])
		if fetchErr != nil {
			return 0, 0, fmt.Errorf("read target vectors: %w", fetchErr)
		}
		for id, c := range part {
			got[id] = c
		}
	}
	for _, want := range snap.Chunks {
		record, ok := got[targetVectorID(snap.ID, snap.ActiveRevisionID, want, v.taggedTarget)]
		if !ok {
			rep.Coverage.Missing++
			continue
		}
		present++
		collection, _ := record.Metadata["collection"].(string)
		docType, _ := record.Metadata["doc_type"].(string)
		tenant, _ := record.Metadata[domain.VectorTenantKey].(string)
		switch {
		case validVector(record.Embedding, v.profile.Dimensions) != nil:
			rep.Coverage.BadVectors++
		case record.DocumentID != want.DocumentID || record.ID != want.ID || record.Index != want.Index || record.Text != want.Text ||
			record.TokenCount != want.TokenCount || collection != snap.Collection || docType != snap.DocType ||
			v.taggedTarget && (record.VectorIncarnation != snap.Incarnation || tenant != snap.TenantID || record.RevisionID != snap.ActiveRevisionID):
			rep.Coverage.Mismatched++
		default:
			valid++
		}
	}
	return present, valid, nil
}

// listUnexpected enumerates the target's keys; any key outside the expected set is out of scope.
func (v *verifier) listUnexpected(ctx context.Context, expected map[string]scopeDoc, rep *verifyReport) error {
	token := ""
	scanned := 0
	for {
		limit := min(pcstore.MaxListPage, v.opts.MaxChunks-scanned)
		if limit == 0 {
			v.fail(rep, "target key listing exceeds -max-chunks, so completeness cannot be proven")
			return nil
		}
		ids, next, err := v.target.ListIDs(ctx, token, limit)
		if errors.Is(err, pcstore.ErrListingUnsupported) {
			v.fail(rep, "the target cannot list its keys, so out-of-scope records cannot be excluded")
			return nil
		}
		scanned += len(ids)
		if err != nil {
			return fmt.Errorf("list target keys: %w", err)
		}
		for _, id := range ids {
			if _, ok := expected[id]; !ok {
				rep.Unexpected.Records++
				if len(rep.Unexpected.Sample) < 20 {
					rep.Unexpected.Sample = append(rep.Unexpected.Sample, id)
				}
			}
		}
		if next == "" {
			rep.Unexpected.Checked = true
			return nil
		}
		token = next
	}
}

func score(seed, id string) string {
	sum := sha256.Sum256([]byte(seed + "|" + id))
	return hex.EncodeToString(sum[:])
}

// matchedQueries compares scoped rankings for a query independent of the staged records.
func (v *verifier) matchedQueries(ctx context.Context, seed string, chunks []sampleChunk, docs []scopeDoc, expected map[string]scopeDoc, rep *verifyReport) error {
	sorted := slices.Clone(chunks)
	sort.Slice(sorted, func(i, j int) bool { return score(seed, sorted[i].id) < score(seed, sorted[j].id) })
	sorted = sorted[:min(len(sorted), v.opts.SampleSize)]
	if len(sorted) == 0 {
		return nil
	}
	queries, err := v.queryVectors(ctx, sorted, rep)
	if err != nil {
		return err
	}
	byCollection := map[string][]string{}
	for _, d := range docs {
		byCollection[d.collection] = append(byCollection[d.collection], d.id)
	}
	var latencies []time.Duration
	var recallSum float64
	hits := 0
	for _, c := range sorted {
		q, ok := queries[c.id]
		if !ok || q.TextHash != hex.EncodeToString(c.textHash[:]) ||
			validVector(q.Source, v.sourceProfile.Dimensions) != nil || validVector(q.Target, v.profile.Dimensions) != nil {
			rep.Sample.Skipped++
			continue
		}
		allow := scopeFor(c, byCollection[c.collection], v.opts.ScopeDocuments)
		filter := domain.VectorFilter{DocumentIDs: allow, Collection: c.collection}
		// A scoped query is the only kind this tool sends: an unrestricted one is never a validation shortcut.
		if filter.DocumentIDs == nil || filter.MatchesNothing() {
			return errors.New("refusing an unscoped verification query")
		}
		start := v.now()
		targetHits, err := v.target.QueryFiltered(ctx, q.Target, v.opts.TopK, filter)
		elapsed := v.now().Sub(start)
		if err != nil {
			return fmt.Errorf("query the target: %w", err)
		}
		sourceHits, err := v.source.QueryFiltered(ctx, q.Source, v.opts.TopK, filter)
		if err != nil {
			return fmt.Errorf("query the current target: %w", err)
		}
		allowed := map[string]struct{}{}
		for _, id := range allow {
			allowed[id] = struct{}{}
		}
		expectedHit := func(chunk domain.Chunk) (string, bool) {
			hitCollection, _ := chunk.Metadata["collection"].(string)
			key := hitKey(chunk)
			want, ok := expected[key]
			if !ok {
				return key, false
			}
			_, inScope := allowed[want.id]
			if !inScope || want.id != string(chunk.DocumentID) || want.collection != hitCollection ||
				want.chunkID != string(chunk.ID) || (v.taggedTarget && want.revisionID != chunk.RevisionID) {
				return key, false
			}
			return key, true
		}
		inTarget := map[string]struct{}{}
		for _, h := range targetHits {
			key, ok := expectedHit(h.Chunk)
			if !ok {
				rep.Sample.ForbiddenHits++
				continue
			}
			inTarget[key] = struct{}{}
		}
		baseline := make(map[string]struct{}, len(sourceHits))
		for _, h := range sourceHits {
			if id, ok := expectedHit(h.Chunk); ok {
				baseline[id] = struct{}{}
			}
		}
		if len(baseline) == 0 {
			rep.Sample.Skipped++
			continue
		}
		rep.Sample.Queries++
		latencies = append(latencies, elapsed)
		if _, ok := inTarget[c.id]; ok {
			hits++
		}
		overlap := 0
		for id := range baseline {
			if _, ok := inTarget[id]; ok {
				overlap++
			}
		}
		recallSum += float64(overlap) / float64(len(baseline))
	}
	if rep.Sample.Queries > 0 {
		rep.Sample.MeanRecall = recallSum / float64(rep.Sample.Queries)
		rep.Sample.HitRate = float64(hits) / float64(rep.Sample.Queries)
		slices.Sort(latencies)
		p95 := latencies[int(math.Ceil(0.95*float64(len(latencies))))-1]
		rep.Sample.P95Milliseconds = float64(p95) / float64(time.Millisecond)
	}
	return nil
}

func hitKey(c domain.Chunk) string {
	if c.VectorID != "" {
		return c.VectorID
	}
	return string(c.ID)
}

func fetchAll(ctx context.Context, r vectorReader, ids []string) (map[string]domain.Chunk, error) {
	out := make(map[string]domain.Chunk, len(ids))
	for start := 0; start < len(ids); start += pcstore.FetchBatchSize {
		part, err := r.Fetch(ctx, ids[start:min(start+pcstore.FetchBatchSize, len(ids))])
		if err != nil {
			return nil, fmt.Errorf("read vectors: %w", err)
		}
		for id, c := range part {
			out[id] = c
		}
	}
	return out, nil
}

// scopeFor returns the sample chunk's document plus a deterministic selection of
// other documents of its collection, at most limit in all.
func scopeFor(c sampleChunk, collectionDocs []string, limit int) []string {
	others := make([]string, 0, len(collectionDocs))
	for _, id := range collectionDocs {
		if id != c.doc {
			others = append(others, id)
		}
	}
	sort.Slice(others, func(i, j int) bool { return score(c.id, others[i]) < score(c.id, others[j]) })
	return append([]string{c.doc}, others[:min(len(others), limit-1)]...)
}

// judge applies the declared cutover criteria; anything unmeasured counts as failed.
func (v *verifier) judge(rep *verifyReport) {
	c := rep.Coverage
	if c.Inconsistent > 0 {
		v.fail(rep, "%d documents declare a chunk count that differs from their stored chunks", c.Inconsistent)
	}
	if c.TaggedVectors > 0 {
		v.fail(rep, "%d documents store tagged vectors, which a reindex does not stage", c.TaggedVectors)
	}
	if c.ManifestedVectors > 0 {
		v.fail(rep, "%d documents own hosted vector revisions, which this target does not store", c.ManifestedVectors)
	}
	if c.DocumentsMissing > 0 || c.DocumentsStale > 0 {
		v.fail(rep, "%d documents lack a current checkpoint and %d have a stale one", c.DocumentsMissing, c.DocumentsStale)
	}
	if c.NotReembedded > 0 {
		v.fail(rep, "%d documents lack a fully recomputed checkpoint for an unattested source", c.NotReembedded)
	}
	if c.Unstable > 0 {
		v.fail(rep, "%d documents changed during the scan; freeze writers first", c.Unstable)
	}
	if c.Missing > 0 {
		v.fail(rep, "%d expected keys are missing from the target", c.Missing)
	}
	if c.BadVectors > 0 {
		v.fail(rep, "%d vectors have the wrong dimension or an invalid value", c.BadVectors)
	}
	if c.Mismatched > 0 {
		v.fail(rep, "%d records differ from PostgreSQL in document, chunk, position, text, token count, type, collection, tenant, revision or incarnation", c.Mismatched)
	}
	if rep.Unexpected.Records > 0 {
		v.fail(rep, "%d records in the target are outside the expected set", rep.Unexpected.Records)
	}
	s := rep.Sample
	switch {
	case rep.Expected.Chunks == 0:
		v.fail(rep, "no chunks exist, so no acceptance sample can be drawn")
	case s.Queries == 0 || s.Queries < s.Requested:
		v.fail(rep, "only %d of %d sample queries ran with a usable baseline (%d unmeasurable)", s.Queries, s.Requested, s.Skipped)
	}
	if s.ForbiddenHits > 0 {
		v.fail(rep, "%d results fell outside the query scope", s.ForbiddenHits)
	}
	if s.Queries > 0 && s.MeanRecall < s.MinRecall {
		v.fail(rep, "mean recall %.3f is below the declared %.3f", s.MeanRecall, s.MinRecall)
	}
	if s.Queries > 0 && s.P95Milliseconds > float64(v.opts.MaxP95)/float64(time.Millisecond) {
		v.fail(rep, "p95 latency %.1f ms exceeds the declared %s", s.P95Milliseconds, s.MaxP95)
	}
	rep.Passed = len(rep.Failures) == 0
}

// verify checks a staged generation and, with -apply under the freeze, records a passing result.
func (d deps) verify(ctx context.Context, opts options, p targetPlan, out io.Writer) error {
	var fence *pgstore.GenerationFence
	var frozenAt time.Time
	if opts.Apply {
		var err error
		fence, err = d.pg.FenceGeneration(ctx, p.generation)
		if err != nil {
			return err
		}
		defer fence.Close(ctx)
		frozenAt, err = fence.BeginVerification(ctx)
		if err != nil {
			return err
		}
	}
	row, err := d.pg.Generation(ctx, p.generation)
	if err != nil {
		return err
	}
	if row == nil {
		return fmt.Errorf("generation %q is not registered", p.generation)
	}
	if row.PendingWrite {
		return fmt.Errorf("generation %q has an unconfirmed target write", row.Name)
	}
	state, err := d.pg.VectorState(ctx)
	if err != nil {
		return err
	}
	if state.Active == p.generation || row.State == pgstore.StateActive {
		return fmt.Errorf("generation %q is active; verify a staged generation", p.generation)
	}
	if opts.Profile != "" {
		given, readErr := d.loadTargetProfile(opts)
		if readErr != nil {
			return readErr
		}
		if given.Hash() != row.Profile.Hash() {
			return fmt.Errorf("%w: the profile file differs from the recorded profile", pgstore.ErrGenerationDrift)
		}
	}
	sourceProfile, _, sourceHost, err := d.sourceProfile(ctx, p.live)
	if err != nil {
		return err
	}
	sourceQueryOnly := sourceProfile == nil
	if sourceQueryOnly {
		// The running serving encoder supplies query provenance only. The
		// historical vectors remain unattested and cannot be used for copying.
		settings := d.cfg.EmbeddingSettings()
		if configErr := settings.Validate(); configErr != nil {
			return fmt.Errorf("configure the live serving query encoder for verification: %w", configErr)
		}
		sourceProfile = &settings
	}
	target, closeTarget, _, err := d.openBound(ctx, targetSpec{Store: row.Store, Target: row.Target, Generation: row.Name, Dimensions: row.Profile.Dimensions}, row.Host)
	if err != nil {
		return fmt.Errorf("open the target: %w", err)
	}
	defer closeTarget()
	source, closeSource, _, err := d.openBound(ctx, targetSpec{Store: p.live.store, Target: p.live.target, Generation: p.live.generation, Dimensions: sourceProfile.Dimensions}, sourceHost)
	if err != nil {
		return fmt.Errorf("open the current target as the baseline: %w", err)
	}
	defer closeSource()
	v := &verifier{corpus: d.pg, ledger: d.pg, target: target, source: source, generation: row.Name, profile: row.Profile, sourceProfile: *sourceProfile, sourceQueryOnly: sourceQueryOnly, taggedTarget: row.Store == "pinecone", opts: opts, now: time.Now}
	rep, err := v.run(ctx)
	if err != nil {
		return err
	}
	if rep.Passed && opts.Apply {
		encoded, marshalErr := json.Marshal(rep)
		if marshalErr != nil {
			return marshalErr
		}
		if recordErr := fence.RecordVerification(ctx, frozenAt, rep.CorpusDigest, encoded); recordErr != nil {
			return recordErr
		}
		rep.Recorded = true
	}
	if encodeErr := json.NewEncoder(out).Encode(rep); encodeErr != nil {
		return encodeErr
	}
	if !rep.Passed {
		return errNotVerified
	}
	return nil
}
