package main

import (
	"context"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/Victoroide/quarry-rag/client"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/evaluation"
	"github.com/Victoroide/quarry-rag/internal/textutil"
)

func submit(ctx context.Context, c *client.Client, l *ledger, doc evaluation.Document) stepResult {
	result := stepResult{Document: doc.Key, Step: "create"}
	id := l.namespace.DocumentID(doc.Key)
	if !evaluation.ValidReceipt(l.Receipt) {
		result.Outcome = "ownership_inconclusive"
		return result
	}
	probeCtx, probe := fixtureExchange(ctx, l, "")
	_, err := c.GetEvaluationFixture(probeCtx, id)
	result.Outcome, result.Status = failure(err)
	if probe.state == domain.FixtureBorrowed && result.Status == http.StatusConflict {
		result.Outcome = "preexisting"
		return result
	}
	if result.Status != http.StatusNotFound || probe.state != domain.FixtureMissing {
		if result.Status == http.StatusNotFound || err == nil {
			result.Outcome = "ownership_inconclusive"
		}
		return result
	}
	// Persist intent after proving absence and before dispatch. Durable ingestion
	// may complete even when the caller loses the response.
	if saveErr := l.set(doc.Key, ledgerEntry{State: "pending"}); saveErr != nil {
		result.Outcome, result.Status = "ledger_error", 0
		return result
	}
	ingestCtx, capture := fixtureExchange(ctx, l, "")
	accepted, err := c.IngestEvaluationFixture(ingestCtx, client.IngestRequest{
		DocumentID: id, Content: doc.Text, Collection: l.namespace.Collection(doc.Distractor),
		Metadata: map[string]any{"title": doc.Title},
	})
	if err != nil {
		result.Outcome, result.Status = failure(err)
		if capture.started.IsZero() || rejectedIngest(err) {
			if saveErr := l.set(doc.Key, ledgerEntry{}); saveErr != nil {
				result.Outcome = "ledger_error"
			}
		}
		return result
	}
	if accepted.DocumentID != id || accepted.Status == "already_ingested" {
		result.Outcome, result.Status = "borrowed_or_duplicate", 0
		if capture.state == domain.FixtureBorrowed {
			if err := l.set(doc.Key, ledgerEntry{}); err != nil {
				result.Outcome = "ledger_error"
			}
		}
		return result
	}
	if accepted.Status != "processing" {
		result.Outcome, result.Status = "invalid_response", 0
		return result
	}
	if capture.state != domain.FixtureOwned || !evaluation.ValidIncarnation(capture.revision) {
		result.Outcome, result.Status = "ownership_inconclusive", 0
		return result
	}
	result.Outcome, result.Status = "accepted", 0
	if err := l.set(doc.Key, ledgerEntry{State: "accepted", Incarnation: capture.revision}); err != nil {
		result.Outcome = "ledger_error"
	}
	return result
}

func sameRevision(expected, observed string) bool {
	return evaluation.ValidIncarnation(observed) && (expected == "" || expected == observed)
}

func ownershipIssue(doc *client.DocumentIncarnationSummary, l *ledger, spec evaluation.Document, proof *exchange) string {
	if proof.state != domain.FixtureOwned || doc.ID != l.namespace.DocumentID(spec.Key) ||
		proof.revision != doc.Incarnation || !evaluation.ValidIncarnation(doc.Incarnation) {
		return "ownership_inconclusive"
	}
	if doc.Collection != l.namespace.Collection(spec.Distractor) || !sameRevision(l.Entries[spec.Key].Incarnation, doc.Incarnation) {
		return "ownership_mismatch"
	}
	return ""
}

func cleanup(ctx context.Context, c *client.Client, l *ledger) []stepResult {
	var results []stepResult
	for _, doc := range evaluation.Documents() {
		if entry, ok := l.Entries[doc.Key]; ok {
			results = append(results, deleteOne(ctx, c, l, doc, entry))
		}
	}
	return results
}

func deleteOne(ctx context.Context, c *client.Client, l *ledger, doc evaluation.Document, entry ledgerEntry) stepResult {
	result := stepResult{Document: doc.Key, Step: "delete"}
	if !evaluation.ValidReceipt(l.Receipt) {
		result.Outcome = "ownership_inconclusive"
		return result
	}
	id := l.namespace.DocumentID(doc.Key)
	statusCtx, proof := fixtureExchange(ctx, l, "")
	current, err := c.GetEvaluationFixture(statusCtx, id)
	if err != nil {
		result.Outcome, result.Status = failure(err)
		if result.Status == http.StatusNotFound && proof.state == domain.FixtureDeleted && sameRevision(entry.Incarnation, proof.revision) {
			return completeCleanup(l, doc, result, "deleted")
		}
		if entry.State == "pending" && entry.Incarnation == "" {
			switch {
			case result.Status == http.StatusNotFound && proof.state == domain.FixtureRetired:
				return completeCleanup(l, doc, result, "retired")
			case result.Status == http.StatusNotFound && proof.state == domain.FixtureMissing,
				result.Status == http.StatusConflict && proof.state == domain.FixtureBorrowed:
				return reconcilePending(ctx, c, l, doc, result)
			}
		}
		if result.Status == http.StatusConflict && proof.state == domain.FixtureBorrowed {
			result.Outcome = "ownership_mismatch"
		}
		if proof.state == domain.FixtureWrongStore {
			result.Outcome = "store_mismatch"
		} else if result.Status == http.StatusNotFound || result.Status == http.StatusPreconditionFailed {
			result.Outcome = "ownership_inconclusive"
		}
		return result
	}
	if issue := ownershipIssue(current, l, doc, proof); issue != "" {
		result.Outcome = issue
		return result
	}
	entry.State, entry.Incarnation = "deleting", current.Incarnation
	if saveErr := l.set(doc.Key, entry); saveErr != nil {
		result.Outcome = "ledger_error"
		return result
	}
	deleteCtx, receipt := fixtureExchange(ctx, l, entry.Incarnation)
	deleted, err := c.DeleteEvaluationFixture(deleteCtx, id)
	if err != nil {
		result.Outcome, result.Status = failure(err)
		if receipt.state == domain.FixtureWrongStore {
			result.Outcome = "store_mismatch"
		}
		return result
	}
	if !deleted.Deleted || deleted.DocID != id || receipt.state != domain.FixtureDeleted ||
		receipt.revision != entry.Incarnation {
		result.Outcome = "invalid_response"
		return result
	}
	return completeCleanup(l, doc, result, "deleted")
}

// reconcilePending retires an absent admission atomically or records the
// incarnation if admission won the race. It never sweeps vectors.
func reconcilePending(ctx context.Context, c *client.Client, l *ledger, doc evaluation.Document, result stepResult) stepResult {
	id := l.namespace.DocumentID(doc.Key)
	ctx, proof := fixtureExchange(ctx, l, "")
	removed, err := c.DeleteEvaluationFixture(ctx, id, func(r *http.Request) {
		r.Header.Set(evaluation.ReconcileHeader, "absent")
	})
	if err != nil {
		result.Outcome, result.Status = failure(err)
		if proof.state == domain.FixtureWrongStore {
			result.Outcome = "store_mismatch"
		}
		if result.Status == http.StatusConflict {
			switch {
			case proof.state == domain.FixtureOwned && evaluation.ValidIncarnation(proof.revision):
				result.Outcome = "ownership_pending"
				if saveErr := l.set(doc.Key, ledgerEntry{State: "accepted", Incarnation: proof.revision}); saveErr != nil {
					result.Outcome = "ledger_error"
				}
			case proof.state == domain.FixtureBorrowed && proof.revision == "":
				return completeCleanup(l, doc, result, "preexisting")
			}
		}
		return result
	}
	result.Status = http.StatusOK
	if removed.DocID == id {
		switch {
		case !removed.Deleted && proof.state == domain.FixtureRetired && proof.revision == "":
			return completeCleanup(l, doc, result, "retired")
		case removed.Deleted && proof.state == domain.FixtureDeleted && evaluation.ValidIncarnation(proof.revision):
			return completeCleanup(l, doc, result, "deleted")
		}
	}
	result.Outcome = "invalid_response"
	return result
}

func completeCleanup(l *ledger, doc evaluation.Document, result stepResult, outcome string) stepResult {
	result.Outcome = outcome
	if err := l.set(doc.Key, ledgerEntry{}); err != nil {
		result.Outcome = "ledger_error"
	}
	return result
}

func waitVisible(ctx context.Context, c *client.Client, l *ledger, doc evaluation.Document, deadline, interval time.Duration) visibility {
	ctx, cancel := context.WithTimeout(ctx, deadline)
	defer cancel()
	result := visibility{Document: doc.Key, Outcome: "failed", EventClock: "unmeasured"}
	id := l.namespace.DocumentID(doc.Key)
	for {
		pollCtx, proof := fixtureExchange(ctx, l, "")
		current, err := c.GetEvaluationFixture(pollCtx, id)
		observed := time.Now()
		if err != nil {
			result.Reason, result.Status = failure(err)
			if ctx.Err() != nil || result.Reason == "request_budget" {
				result.Outcome = "censored"
			}
			return result
		}
		if issue := ownershipIssue(current, l, doc, proof); issue != "" {
			result.Reason = issue
			return result
		}
		entry := l.Entries[doc.Key]
		if entry.Incarnation == "" && current.Incarnation != "" {
			entry.State = "accepted"
			entry.Incarnation = current.Incarnation
			if err := l.set(doc.Key, entry); err != nil {
				result.Reason = "ledger_error"
				return result
			}
		}
		switch current.Status {
		case "completed":
			result.Completed = true
			result = pollEvidence(ctx, c, l.namespace, doc, result, observed, interval)
			if current.CompletedAt != nil {
				result.CompletedAt = current.CompletedAt.UTC().Format(time.RFC3339Nano)
				result.recordCompletionBounds(*current.CompletedAt, proof, observed)
			}
			return result
		case "failed":
			result.Reason = "ingestion_failed"
			return result
		case "processing":
		default:
			result.Reason = "invalid_status"
			return result
		}
		if err := wait(ctx, interval); err != nil {
			result.Outcome = "censored"
			result.Reason, result.Status = failure(err)
			return result
		}
	}
}

func pollEvidence(ctx context.Context, c *client.Client, ns evaluation.Namespace, doc evaluation.Document, result visibility, completed time.Time, interval time.Duration) visibility {
	id := ns.DocumentID(doc.Key)
	witness, _, _ := strings.Cut(doc.Text, ". ")
	if len(doc.Spans) != 0 {
		witness = doc.Spans[0].Text
	}
	normalized := fixtureText(doc.Text)
	req := client.QueryRequest{
		Query: witness, AnswerMode: client.AnswerModeEvidence, Mode: "global", MaxChunks: defaultK,
		Collection: ns.Collection(doc.Distractor), AllowedDocumentIDs: []string{id},
	}
	for {
		response, err := c.Query(ctx, req, client.WithAcceptLanguage(doc.Language))
		if err != nil {
			result.Reason, result.Status = failure(err)
			break
		}
		if response.ScoredChunks == nil || response.FinishReason != client.FinishReasonEvidenceOnly &&
			response.FinishReason != "no_evidence" {
			result.Reason = "invalid_response"
			return result
		}
		if slices.ContainsFunc(response.ScoredChunks, func(chunk client.ScoredChunkResponse) bool { return chunk.DocID != id }) {
			result.Reason = "out_of_scope"
			return result
		}
		if response.FinishReason == client.FinishReasonEvidenceOnly &&
			slices.ContainsFunc(response.ScoredChunks, func(chunk client.ScoredChunkResponse) bool {
				body := fixtureText(chunk.Text)
				return chunk.Excerpt > 0 && body != "" && strings.Contains(normalized, body)
			}) {
			result.Outcome = "observed"
			firstVisible := time.Now()
			result.FirstVisibleAt = firstVisible.UTC().Format(time.RFC3339Nano)
			delay := milliseconds(firstVisible.Sub(completed))
			result.DelayMS = &delay
			return result
		}
		if err := wait(ctx, interval); err != nil {
			result.Reason, result.Status = failure(err)
			break
		}
	}
	if ctx.Err() != nil || result.Reason == "request_budget" {
		result.Outcome = "censored"
		delay := milliseconds(time.Since(completed))
		result.CensoredMS = &delay
	}
	return result
}

func fixtureText(text string) string {
	return strings.Join(strings.Fields(textutil.SanitizeForPrompt(text)), " ")
}

func wait(ctx context.Context, interval time.Duration) error {
	timer := time.NewTimer(interval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func milliseconds(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }

func checkQuery(ctx context.Context, c *client.Client, ns evaluation.Namespace, receipt string, q evaluation.Question, o options) sample {
	ids := make([]string, 0, len(q.Allowed))
	for _, key := range q.Allowed {
		ids = append(ids, ns.DocumentID(key))
	}
	req := client.QueryRequest{
		Query: q.Text, AnswerMode: client.AnswerModeGenerated, Mode: "global", MaxChunks: o.K, IncludeCitations: true,
		Collection: ns.Collection(false), AllowedDocumentIDs: ids,
	}
	capture := &exchange{namespace: ns.ID.String(), receipt: receipt, noProof: !o.Instrument}
	ctx = context.WithValue(ctx, exchangeKey{}, capture)
	start := time.Now()
	response, err := c.Query(ctx, req, client.WithAcceptLanguage(q.Language))
	elapsed := milliseconds(time.Since(start))
	if err != nil {
		s := newSample(q)
		s.Milliseconds = elapsed
		s.Error, s.HTTPStatus = failure(err)
		return s
	}
	var matches []evaluation.Match
	var proofError string
	if o.Instrument && capture.proof != "" {
		matches, err = evaluation.DecodeMatches(capture.proof)
		if err != nil {
			proofError = "invalid_observation"
		}
	} else if o.Instrument {
		proofError = "observation_unavailable"
	}
	s := observe(q, ns, response, matches, o.K, o.IncludeRaw)
	if capture.execution == "offline" || !s.Empty && !s.NoEvidence && len(response.ScoredChunks) > 0 {
		// The transport withholds provider attestation from unverified peers.
		fields := strings.Split(capture.execution, ",")
		if capture.execution == "offline" || len(fields) == 3 && evaluation.ProviderExecution(fields[0], fields[1], fields[2]) != "" {
			s.Execution = capture.execution
			if len(fields) == 3 && evaluation.ValidUpstreams(capture.upstreams, fields[1], fields[2]) {
				s.Upstreams = capture.upstreams
			}
		}
	}
	if proofError != "" {
		s.ObservationError = proofError
	}
	s.Milliseconds = elapsed
	return s
}
