package main

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	pgstore "github.com/Victoroide/quarry-rag/internal/store/postgres"
	"github.com/google/uuid"
	"github.com/pinecone-io/go-pinecone/v5/pinecone"
	"google.golang.org/protobuf/types/known/structpb"
)

const defaultVectorNamespace = "__default__"
const supersededSampleLimit = 20

type supersededReport struct {
	Selected      int      `json:"superseded_unrevisioned_vectors"`
	Deleted       int      `json:"deleted_vectors"`
	Scanned       int      `json:"scanned_vectors"`
	NextPageToken string   `json:"next_page_token"`
	Sample        []string `json:"sample,omitempty"`
}

type supersededLookup func(context.Context, []string) (map[string]bool, error)
type supersededDelete func(context.Context, []string) (acknowledged bool, err error)

func runSuperseded(ctx context.Context, cfg *config.Config, index vectorIndex, pg *pgstore.Client, host string, opts options, diagnostics io.Writer) (supersededReport, error) {
	generation := cfg.ActiveVectorGeneration()
	store, target := cfg.VectorTarget()
	profile := cfg.EmbeddingSettings()
	scope := fmt.Sprintf("%x", sha256.Sum256([]byte(strings.Join([]string{generation, target, host, defaultVectorNamespace}, "\x00"))))

	var fence *pgstore.GenerationFence
	if opts.Apply {
		var err error
		fence, err = pg.FenceGeneration(ctx, generation)
		if err != nil {
			return supersededReport{}, err
		}
		defer fence.Close(ctx)
	}
	if err := pg.PinExistingVectorGeneration(ctx, generation, store, target, host, profile); err != nil {
		return supersededReport{}, fmt.Errorf("pin active cleanup target: %w", err)
	}
	lookup := func(ctx context.Context, ids []string) (map[string]bool, error) {
		return lookupSupersededDocuments(ctx, pg, ids)
	}
	if !opts.Apply {
		return reconcileSuperseded(ctx, index, lookup, nil, scope, opts)
	}

	sender, err := uuid.NewRandom()
	if err != nil {
		return supersededReport{}, fmt.Errorf("generate active cleanup sender UUID: %w", err)
	}
	if _, diagnosticErr := fmt.Fprintf(diagnostics, "active cleanup sender %s for generation %q\n", sender, generation); diagnosticErr != nil {
		return supersededReport{}, fmt.Errorf("report active cleanup sender: %w", diagnosticErr)
	}
	epoch, err := prepareActiveCleanup(ctx, pg, fence, generation, opts.ConfirmTargetDrained, opts.ConfirmSenderStopped, diagnostics)
	if err != nil {
		return supersededReport{}, err
	}
	remove := func(ctx context.Context, ids []string) (bool, error) {
		reservation, err := fence.ReserveActiveCleanup(ctx, epoch, target, host, profile, ids, sender)
		if err != nil {
			return false, err
		}
		if _, err := fmt.Fprintf(diagnostics, "committed active cleanup reservation %s from sender %s for exact IDs %q\n", reservation, sender, ids); err != nil {
			return false, fmt.Errorf("report committed active cleanup reservation %s: %w", reservation, err)
		}
		if err := ctx.Err(); err != nil {
			return false, fmt.Errorf("dispatch active cleanup reservation %s for exact IDs %q: %w", reservation, ids, err)
		}
		deleteCtx, cancel := context.WithTimeout(ctx, domain.WriteRequestTimeout)
		defer cancel()
		if err := index.DeleteVectorsById(deleteCtx, ids); err != nil {
			return false, fmt.Errorf("delete exact IDs %q for active cleanup reservation %s: %w", ids, reservation, err)
		}
		if err := fence.ClearActiveCleanup(ctx, epoch, reservation, ids, sender); err != nil {
			return true, fmt.Errorf("record acknowledgment for active cleanup reservation %s and exact IDs %q: %w", reservation, ids, err)
		}
		return true, nil
	}
	return reconcileSuperseded(ctx, index, lookup, remove, scope, opts)
}

func prepareActiveCleanup(ctx context.Context, pg *pgstore.Client, fence *pgstore.GenerationFence, generation string, confirmed, stoppedSender uuid.UUID, diagnostics io.Writer) (time.Time, error) {
	state, err := pg.VectorState(ctx)
	if err != nil {
		return time.Time{}, err
	}
	if !state.Frozen || state.FrozenAt == nil || state.Active != generation || state.FreezeGeneration != generation {
		return time.Time{}, fmt.Errorf("freeze writers for active generation %q before applying cleanup", generation)
	}
	epoch := *state.FrozenAt
	batch, err := fence.ActiveCleanupBatch(ctx, epoch)
	if err != nil {
		return time.Time{}, err
	}
	hasConfirmation := confirmed != uuid.Nil || stoppedSender != uuid.Nil
	if batch == nil && hasConfirmation {
		return time.Time{}, errors.New("stale active cleanup confirmation: no batch is pending")
	}
	if batch != nil && (confirmed != batch.Reservation || stoppedSender != batch.Sender) {
		pending := fmt.Sprintf("unconfirmed active cleanup for generation %q, reservation %s, sender %s and exact IDs %q: verify sender shutdown and then provider settlement before using -confirm-sender-stopped=%s -confirm-target-drained=%s",
			generation, batch.Reservation, batch.Sender, batch.IDs, batch.Sender, batch.Reservation)
		if hasConfirmation {
			return time.Time{}, fmt.Errorf("stale active cleanup confirmation: %s", pending)
		}
		return time.Time{}, errors.New(pending)
	}
	quiet, err := pg.WriterQuiesce(ctx, domain.WriteRequestTimeout)
	if err != nil {
		return time.Time{}, err
	}
	if !quiet.Quiet() {
		return time.Time{}, fmt.Errorf("writers must be frozen for at least %s with no in-flight jobs or open deletions (%d jobs, %d deletions%s)",
			domain.WriteRequestTimeout, quiet.RunningJobs, quiet.OpenDeletions, pgstore.FormatOpenDeletions(quiet.OpenDeletions, quiet.OpenDeletionIDs))
	}
	if batch != nil {
		if _, err := fmt.Fprintf(diagnostics, "clearing active cleanup reservation %s from sender %s for exact IDs %q after operator confirmation of sender shutdown and provider settlement, and restarting the scan\n", confirmed, stoppedSender, batch.IDs); err != nil {
			return time.Time{}, err
		}
		if err := fence.ClearActiveCleanup(ctx, epoch, confirmed, batch.IDs, stoppedSender); err != nil {
			return time.Time{}, err
		}
	}
	return epoch, nil
}

func reconcileSuperseded(ctx context.Context, index vectorIndex, lookup supersededLookup, remove supersededDelete, scope string, opts options) (supersededReport, error) {
	report := supersededReport{NextPageToken: opts.PageToken}
	token, err := decodeSupersededPageToken(scope, opts.PageToken)
	if err != nil {
		return report, err
	}
	// Validate the catalog and its grants before scanning, including an empty index.
	if _, err := lookup(ctx, nil); err != nil {
		return report, err
	}
	tokens := map[string]bool{token: true}
	seen := make(map[string]bool)
	for pages := 0; pages < opts.ScanLimit && report.Scanned < opts.ScanLimit && report.Selected < opts.Limit; pages++ {
		if err := ctx.Err(); err != nil {
			return report, err
		}
		limit := min(opts.Page, opts.ScanLimit-report.Scanned, opts.Limit-report.Selected)
		if limit < 1 || limit > 1000 {
			return report, errors.New("vector listing page must be between 1 and 1000")
		}
		req := &pinecone.ListVectorsRequest{Limit: new(uint32(limit))}
		if token != "" {
			req.PaginationToken = &token
		}
		page, err := index.ListVectors(ctx, req)
		if err != nil {
			return report, fmt.Errorf("list vectors in active index (serverless listing required): %w", err)
		}
		if page == nil || len(page.VectorIds) > limit || !isDefaultVectorNamespace(page.Namespace) {
			return report, errors.New("invalid vector listing for the bound default namespace")
		}
		report.Scanned += len(page.VectorIds)
		var next string
		if page.NextPaginationToken != nil {
			next = *page.NextPaginationToken
			if !validProviderPageToken(next) {
				return report, errors.New("malformed pagination token in vector listing")
			}
			if tokens[next] {
				return report, errors.New("repeated pagination token in vector listing")
			}
			tokens[next] = true
		}
		ids, err := selectSupersededPage(ctx, index, lookup, page.VectorIds, seen)
		if err != nil {
			return report, err
		}
		report.Selected += len(ids)
		report.Sample = append(report.Sample, ids[:min(len(ids), supersededSampleLimit-len(report.Sample))]...)
		if len(ids) > 0 && remove != nil {
			acknowledged, err := remove(ctx, ids)
			if acknowledged {
				report.Deleted += len(ids)
			}
			if err != nil {
				return report, err
			}
		}
		report.NextPageToken = ""
		if next == "" {
			break
		}
		token = next
		report.NextPageToken = scope + "." + base64.RawURLEncoding.EncodeToString([]byte(token))
	}
	return report, nil
}

func decodeSupersededPageToken(scope, cursor string) (string, error) {
	if cursor == "" {
		return "", nil
	}
	binding, encoded, ok := strings.Cut(cursor, ".")
	if !ok || binding != scope {
		return "", errors.New("page token does not belong to the bound index and default namespace")
	}
	token, err := base64.RawURLEncoding.Strict().DecodeString(encoded)
	if err != nil || !validProviderPageToken(string(token)) {
		return "", errors.New("malformed page token")
	}
	return string(token), nil
}

func validProviderPageToken(token string) bool {
	return token != "" && utf8.ValidString(token) && strings.IndexFunc(token, func(r rune) bool {
		return !unicode.IsGraphic(r) || unicode.IsSpace(r)
	}) == -1
}

func isDefaultVectorNamespace(namespace string) bool {
	return namespace == "" || namespace == defaultVectorNamespace
}

func selectSupersededPage(ctx context.Context, index vectorIndex, lookup supersededLookup, listed []*string, seen map[string]bool) ([]string, error) {
	var ids []string
	for _, id := range listed {
		if id == nil || !isBareChunkID(*id) || seen[*id] {
			continue
		}
		seen[*id] = true
		ids = append(ids, *id)
	}
	if len(ids) == 0 {
		return nil, nil
	}
	owners := make(map[string]string, len(ids))
	if err := fetchSupersededOwners(ctx, index, ids, owners); err != nil {
		return nil, err
	}

	documents := make([]string, 0, len(owners))
	for _, id := range ids {
		if doc, known := owners[id]; known {
			documents = append(documents, doc)
		}
	}
	if len(documents) == 0 {
		return nil, nil
	}
	eligible, err := lookup(ctx, documents)
	if err != nil {
		return nil, err
	}
	var selected []string
	for _, id := range ids {
		if doc, known := owners[id]; known && eligible[doc] {
			selected = append(selected, id)
		}
	}
	return selected, nil
}

func isBareChunkID(id string) bool {
	if len(id) != 32 {
		return false
	}
	parsed, err := uuid.Parse(id)
	return err == nil && strings.ReplaceAll(parsed.String(), "-", "") == id
}

func supersededVectorDocument(id string, vector *pinecone.Vector) string {
	if vector == nil || vector.Id != id || !isBareChunkID(id) {
		return ""
	}
	fields := vector.Metadata.GetFields()
	for _, field := range []string{"revision_id", domain.VectorRevisionKey, domain.VectorAttemptKey} {
		if _, present := fields[field]; present {
			return ""
		}
	}
	for _, field := range []string{domain.VectorIncarnationKey, domain.VectorTenantKey} {
		if value, present := fields[field]; present && vectorMetadataUUID(value) == "" {
			return ""
		}
	}
	return vectorMetadataUUID(fields["doc_id"])
}

func vectorMetadataUUID(value *structpb.Value) string {
	raw := value.GetStringValue()
	// Parse alone accepts UUIDs surrounded by any two characters.
	if uuid.Validate(raw) != nil {
		return ""
	}
	id, err := uuid.Parse(raw)
	if err != nil {
		return ""
	}
	return id.String()
}

func lookupSupersededDocuments(ctx context.Context, pg *pgstore.Client, ids []string) (map[string]bool, error) {
	rows, err := pg.Pool.Query(ctx, `SELECT d.id::text
		FROM documents d
		JOIN hosted_vector_revisions r ON r.document_id = d.id AND r.incarnation = COALESCE(d.incarnation, d.id)
		JOIN hosted_vector_manifests m ON m.revision_id = r.active_revision_id
			AND m.document_id = d.id AND m.incarnation = r.incarnation AND m.state = 'active'
		WHERE d.id = ANY($1::uuid[]) AND d.status = 'completed' AND NOT d.vector_mutation_pending
			AND NOT EXISTS (SELECT 1 FROM ingest_jobs j WHERE j.document_id = d.id)
			AND NOT document_deletion_reserved(d.id)`, ids)
	if err != nil {
		return nil, fmt.Errorf("look up superseded vector owners: %w", err)
	}
	defer rows.Close()
	found := make(map[string]bool)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		found[id] = true
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read superseded vector owners: %w", err)
	}
	return found, nil
}
