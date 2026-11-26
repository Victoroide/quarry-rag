package postgres

import (
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/chunker"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

const ownedLegacyPDFText = "Historical parser text"
const ownedLegacyPDFBytes = "historical PDF bytes"

func (f *ownedFixture) legacyPDF(t *testing.T, status, source, fileType, fingerprint string, assign bool) domain.AcceptRequest {
	t.Helper()
	id := uuid.NewString()
	key := id + "/historical.pdf"
	grantExec(t, f.c, `INSERT INTO documents(id,incarnation,content,status,text_source,file_type,s3_key,content_dedup,fingerprint)
		VALUES($1,NULL,$2,$3,$4,$5,$6,'collection',$7)`, id, ownedLegacyPDFText, status, source, fileType, key, fingerprint)
	if status == "processing" {
		grantExec(t, f.c, `INSERT INTO ingest_jobs(document_id) VALUES($1)`, id)
	}
	if assign {
		f.importBatch(t, func(b map[string]any) {
			b["assignments"] = []grantEntry{grantAssignment(t, f.c, f.tenant, f.owner.ID, id)}
		})
	}
	req := mixedPDFRequest()
	req.Document.ID = domain.DocumentID(id)
	req.Document.S3Key = nil
	req.CallerSupplied = true
	req.ContentDedup = domain.ContentDedupOff
	req.Owner = &domain.OwnerBinding{Principal: f.owner, Incarnation: id, ValidUntil: time.Now().Add(time.Minute)}
	req.LegacyOCRFingerprint = chunker.ScopedFingerprint("", ownedLegacyPDFBytes)
	return req
}

type legacyPDFState struct {
	document []byte
	jobs     int
}

func (f *ownedFixture) legacyPDFState(t *testing.T, id domain.DocumentID) legacyPDFState {
	t.Helper()
	var state legacyPDFState
	require.NoError(t, f.c.Pool.QueryRow(t.Context(), `SELECT to_jsonb(d),
		(SELECT count(*) FROM ingest_jobs j WHERE j.document_id=d.id)
		FROM documents d WHERE d.id=$1`, string(id)).Scan(&state.document, &state.jobs))
	return state
}

func (f *ownedFixture) requireLegacyPDFRequeued(t *testing.T, id domain.DocumentID) {
	t.Helper()
	var status, key string
	var jobCount int
	var pdfRequired bool
	require.NoError(t, f.c.Pool.QueryRow(t.Context(), `SELECT d.status,d.s3_key,
		(SELECT count(*) FROM ingest_jobs j WHERE j.document_id=d.id), j.pdf_required
		FROM documents d JOIN ingest_jobs j ON j.document_id=d.id WHERE d.id=$1`, string(id)).
		Scan(&status, &key, &jobCount, &pdfRequired))
	require.Equal(t, "processing", status)
	require.Equal(t, string(id)+"/historical.pdf", key)
	require.Equal(t, 1, jobCount)
	require.False(t, pdfRequired)
}

func TestOwnedRetryResolvesHistoricalParserIdentity(t *testing.T) {
	f := newOwnedFixture(t)
	parserFingerprint := chunker.ScopedFingerprint("", ownedLegacyPDFText)
	shared := f.legacyPDF(t, "completed", "parser", "pdf", parserFingerprint, false)
	grantExec(t, f.c, `INSERT INTO document_fingerprints(fingerprint,document_id) VALUES($1,$2)`, parserFingerprint, string(shared.Document.ID))
	sharedState := func(t *testing.T) []byte {
		t.Helper()
		var rows []byte
		require.NoError(t, f.c.Pool.QueryRow(t.Context(), `SELECT jsonb_agg(to_jsonb(f) ORDER BY fingerprint) FROM document_fingerprints f`).Scan(&rows))
		return rows
	}
	fingerprints := sharedState(t)
	assertSharedUnchanged := func(t *testing.T) {
		t.Helper()
		require.Equal(t, fingerprints, sharedState(t))
	}
	for _, tc := range []struct {
		name, status string
		text         string
		want         domain.AcceptOutcome
	}{
		{"failed_same_text", "failed", ownedLegacyPDFText, domain.AcceptRequeued},
		{"completed", "completed", ownedLegacyPDFText, domain.AcceptDuplicate},
		{"processing", "processing", ownedLegacyPDFText, domain.AcceptInProgress},
		{"different_text", "failed", "Different parser text", domain.AcceptConflict},
		{"same_text_different_bytes", "failed", ownedLegacyPDFText, domain.AcceptRequeued},
	} {
		t.Run(tc.name, func(t *testing.T) {
			defer assertSharedUnchanged(t)
			req := f.legacyPDF(t, tc.status, "parser", "pdf", parserFingerprint, true)
			if tc.name == "same_text_different_bytes" {
				original := "different bytes with identical parser text"
				req.Fingerprint = chunker.ScopedFingerprint("", domain.MixedPDFProfile+"\x00"+original)
				req.LegacyOCRFingerprint = chunker.ScopedFingerprint("", original)
			}
			before := f.legacyPDFState(t, req.Document.ID)
			require.Equal(t, domain.AcceptNeedsPDFText, f.accept(t, req).Outcome)
			require.Equal(t, before, f.legacyPDFState(t, req.Document.ID))
			req.LegacyParserFingerprint = chunker.ScopedFingerprint("", tc.text)
			require.Equal(t, tc.want, f.accept(t, req).Outcome)
			if tc.want == domain.AcceptRequeued {
				f.requireLegacyPDFRequeued(t, req.Document.ID)
			} else {
				require.Equal(t, before, f.legacyPDFState(t, req.Document.ID))
			}
		})
	}
	t.Run("legacy_ocr_identity", func(t *testing.T) {
		defer assertSharedUnchanged(t)
		req := f.legacyPDF(t, "failed", "ocr", "pdf", chunker.ScopedFingerprint("", ownedLegacyPDFBytes), true)
		require.Equal(t, domain.AcceptRequeued, f.accept(t, req).Outcome)
		f.requireLegacyPDFRequeued(t, req.Document.ID)
	})
	t.Run("mixed_profile_conflict", func(t *testing.T) {
		defer assertSharedUnchanged(t)
		req := f.legacyPDF(t, "failed", "parser", "pdf", chunker.Fingerprint("different profile bytes"), true)
		grantExec(t, f.c, `INSERT INTO document_pdf_extractions(document_id,profile,manifest,coverage)
			VALUES($1,$2,'[]','complete')`, string(req.Document.ID), domain.MixedPDFProfile)
		before := f.legacyPDFState(t, req.Document.ID)
		require.Equal(t, domain.AcceptConflict, f.accept(t, req).Outcome)
		require.Equal(t, before, f.legacyPDFState(t, req.Document.ID))
	})
	t.Run("layout_profile_unchanged", func(t *testing.T) {
		defer assertSharedUnchanged(t)
		req := f.legacyPDF(t, "failed", "parser", "pdf", parserFingerprint, true)
		req.PDF.Profile = domain.LayoutPDFProfile
		before := f.legacyPDFState(t, req.Document.ID)
		require.Equal(t, domain.AcceptConflict, f.accept(t, req).Outcome)
		require.Equal(t, before, f.legacyPDFState(t, req.Document.ID))
	})
	t.Run("anonymous_unchanged", func(t *testing.T) {
		defer assertSharedUnchanged(t)
		req := f.legacyPDF(t, "failed", "parser", "pdf", parserFingerprint, true)
		req.Owner, req.ContentDedup = nil, domain.ContentDedupCollection
		before := f.legacyPDFState(t, req.Document.ID)
		require.Equal(t, domain.AcceptConflict, f.accept(t, req).Outcome)
		require.Equal(t, before, f.legacyPDFState(t, req.Document.ID))
	})
	t.Run("checks_precede_signal", func(t *testing.T) {
		defer assertSharedUnchanged(t)
		req := f.legacyPDF(t, "failed", "parser", "pdf", parserFingerprint, true)
		otherCollection := req
		otherCollection.Collection = "team"
		require.Equal(t, domain.AcceptConflict, f.accept(t, otherCollection).Outcome)
		fixture := req
		fixture.EvaluationReceipt, fixture.EvaluationVectorIdentity = "fixture receipt", "fixture vectors"
		_, err := f.c.AcceptDocument(t.Context(), fixture)
		require.ErrorIs(t, err, errOwnedPolicy)
		result, err := f.c.acceptDocument(t.Context(), fixture, false, false)
		require.NoError(t, err)
		require.Equal(t, domain.AcceptConflict, result.Outcome)
		grantExec(t, f.c, `UPDATE documents SET status='deleting' WHERE id=$1`, string(req.Document.ID))
		before := f.legacyPDFState(t, req.Document.ID)
		require.Equal(t, domain.AcceptDeletionConflict, f.accept(t, req).Outcome)
		require.Equal(t, before, f.legacyPDFState(t, req.Document.ID))
	})
	t.Run("stored_non_pdf", func(t *testing.T) {
		defer assertSharedUnchanged(t)
		req := f.legacyPDF(t, "failed", "parser", "txt", parserFingerprint, true)
		before := f.legacyPDFState(t, req.Document.ID)
		require.Equal(t, domain.AcceptConflict, f.accept(t, req).Outcome)
		require.Equal(t, before, f.legacyPDFState(t, req.Document.ID))
	})
	t.Run("non_owner", func(t *testing.T) {
		defer assertSharedUnchanged(t)
		req := f.legacyPDF(t, "failed", "parser", "pdf", parserFingerprint, true)
		before := f.legacyPDFState(t, req.Document.ID)
		for _, principal := range []domain.Principal{f.peer, f.outsider, f.disabled} {
			copyReq := req
			owner := *req.Owner
			owner.Principal, copyReq.Owner = principal, &owner
			want := domain.AcceptNotFound
			if principal == f.disabled {
				want = domain.AcceptForbidden
			}
			require.Equal(t, want, f.accept(t, copyReq).Outcome)
		}
		oldIncarnation := req
		owner := *req.Owner
		owner.Incarnation, oldIncarnation.Owner = uuid.NewString(), &owner
		require.Equal(t, domain.AcceptNotFound, f.accept(t, oldIncarnation).Outcome)
		require.Equal(t, before, f.legacyPDFState(t, req.Document.ID))
		unassigned := f.legacyPDF(t, "failed", "parser", "pdf", parserFingerprint, false)
		before = f.legacyPDFState(t, unassigned.Document.ID)
		require.Equal(t, domain.AcceptNotFound, f.accept(t, unassigned).Outcome)
		require.Equal(t, before, f.legacyPDFState(t, unassigned.Document.ID))
	})
	t.Run("revoked_between", func(t *testing.T) {
		defer assertSharedUnchanged(t)
		req := f.legacyPDF(t, "failed", "parser", "pdf", parserFingerprint, true)
		before := f.legacyPDFState(t, req.Document.ID)
		require.Equal(t, domain.AcceptNeedsPDFText, f.accept(t, req).Outcome)
		f.setCreate(t, f.owner, "", false)
		req.LegacyParserFingerprint = parserFingerprint
		require.Equal(t, domain.AcceptForbidden, f.accept(t, req).Outcome)
		require.Equal(t, before, f.legacyPDFState(t, req.Document.ID))
	})
}
