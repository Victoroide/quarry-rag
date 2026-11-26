package handler

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/api/middleware"
	"github.com/Victoroide/quarry-rag/internal/chunker"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/evaluation"
	"github.com/Victoroide/quarry-rag/internal/extractor"
	"github.com/Victoroide/quarry-rag/internal/testutil"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// ownedStore records every call so a test can prove which ones never happened.
type ownedStore struct {
	domain.ChunkStore
	targets      map[string]domain.OwnedTarget
	forbidden    bool
	authorized   []string
	accepted     []domain.AcceptRequest
	result       domain.AcceptOutcome
	acceptErr    error
	storedKey    *string
	sharedReads  int
	keyLookupErr error
}

func (s *ownedStore) AuthorizeOwnedIngest(_ context.Context, _ domain.Principal, _ string, supplied domain.DocumentID) (domain.OwnedTarget, error) {
	s.authorized = append(s.authorized, string(supplied))
	if s.forbidden {
		return domain.OwnedTarget{Decision: domain.OwnedForbidden}, nil
	}
	if supplied == "" {
		return domain.OwnedTarget{Decision: domain.OwnedNew}, nil
	}
	if target, ok := s.targets[string(supplied)]; ok {
		return target, nil
	}
	return domain.OwnedTarget{Decision: domain.OwnedNotFound}, nil
}

func (s *ownedStore) AcceptDocument(_ context.Context, req domain.AcceptRequest) (domain.AcceptResult, error) {
	s.accepted = append(s.accepted, req)
	return domain.AcceptResult{DocumentID: req.Document.ID, Outcome: s.result, Incarnation: req.Owner.Incarnation}, s.acceptErr
}

func (s *ownedStore) CheckFingerprint(context.Context, string) (domain.DocumentID, bool, error) {
	s.sharedReads++
	return "", false, nil
}

func (s *ownedStore) GetDocument(context.Context, domain.DocumentID) (*domain.Document, error) {
	s.sharedReads++
	return nil, nil
}

func (s *ownedStore) GetDocumentS3Key(context.Context, domain.DocumentID) (*string, error) {
	return s.storedKey, s.keyLookupErr
}

type ownedObjects struct {
	uploads   []string
	deleted   []string
	uploadErr error
}

func (o *ownedObjects) Upload(_ context.Context, id, _, _ string, _ extractor.FileType, r io.Reader) (string, error) {
	if _, err := io.Copy(io.Discard, r); err != nil {
		return "", err
	}
	key := "object/" + id
	o.uploads = append(o.uploads, key)
	return key, o.uploadErr
}

func (o *ownedObjects) Delete(_ context.Context, key string) error {
	o.deleted = append(o.deleted, key)
	return nil
}

type ownedHarness struct {
	store     *ownedStore
	objects   *ownedObjects
	extracted int
	handler   http.HandlerFunc
	principal domain.Principal
}

func newOwnedHarness(t *testing.T, opts ...IngestOption) *ownedHarness {
	t.Helper()
	h := &ownedHarness{
		store:     &ownedStore{result: domain.AcceptAccepted, targets: map[string]domain.OwnedTarget{}},
		objects:   &ownedObjects{},
		principal: domain.Principal{ID: uuid.NewString(), TenantID: uuid.NewString()},
	}
	h.rebuild(1<<20, opts...)
	return h
}

// rebuild installs a handler whose extractor reads the upload as text.
func (h *ownedHarness) rebuild(maxUpload int64, extra ...IngestOption) {
	opts := append([]IngestOption{WithOwnedIngestion(), WithContentDedup(domain.ContentDedupOff), textExtractor(h)}, extra...)
	h.handler = IngestHandler(nil, h.store, h.objects, maxUpload, opts...)
}

func (h *ownedHarness) serve(r *http.Request) *httptest.ResponseRecorder {
	return h.serveAs(r, h.principal, time.Now().Add(time.Minute))
}

func (h *ownedHarness) serveAs(r *http.Request, principal domain.Principal, expires time.Time) *httptest.ResponseRecorder {
	r = r.WithContext(middleware.ContextWithSubject(r.Context(), middleware.VerifiedSubject{Principal: principal, ExpiresAt: expires}))
	rec := httptest.NewRecorder()
	h.handler(rec, r)
	return rec
}

const ownedBody = "a longer synthetic document body"

type uploadPart struct{ name, value string }

// ownedUpload builds a multipart body whose fields and file appear in the given order.
func ownedUpload(t *testing.T, fileFirst bool, data []byte, fields ...uploadPart) *http.Request {
	t.Helper()
	var body bytes.Buffer
	mw := multipart.NewWriter(&body)
	writeFile := func() {
		part, err := mw.CreateFormFile("file", "notes.txt")
		require.NoError(t, err)
		_, err = part.Write(data)
		require.NoError(t, err)
	}
	if fileFirst {
		writeFile()
	}
	for _, field := range fields {
		require.NoError(t, mw.WriteField(field.name, field.value))
	}
	if !fileFirst {
		writeFile()
	}
	require.NoError(t, mw.Close())
	r := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/ingest", &body)
	r.Header.Set("Content-Type", mw.FormDataContentType())
	return r
}

func ownedJSON(t *testing.T, body string) *http.Request {
	t.Helper()
	return validationJSONRequest(t, "/ingest", body)
}

func textExtractor(h *ownedHarness) IngestOption {
	return WithExtractor(func(_ context.Context, _ extractor.FileType, data []byte) (string, error) {
		h.extracted++
		return string(data), nil
	})
}

func TestOwnedIngestRefusesWithoutVerifiedSubject(t *testing.T) {
	h := newOwnedHarness(t)
	rec := httptest.NewRecorder()
	h.handler(rec, ownedJSON(t, `{"content":"private"}`))
	require.Equal(t, http.StatusUnauthorized, rec.Code)

	rec = h.serveAs(ownedJSON(t, `{"content":"private"}`), h.principal, time.Now().Add(-time.Second))
	require.Equal(t, http.StatusUnauthorized, rec.Code, "an expired credential never starts an acceptance")
	require.Empty(t, h.store.authorized)
	require.Empty(t, h.store.accepted)
}

func TestOwnedIngestRequiresPrivateIdentity(t *testing.T) {
	store := &ownedStore{result: domain.AcceptAccepted}
	for name, opts := range map[string][]IngestOption{
		"default collection identity": {WithOwnedIngestion()},
		"explicit collection":         {WithOwnedIngestion(), WithContentDedup(domain.ContentDedupCollection)},
	} {
		t.Run(name, func(t *testing.T) {
			handler := IngestHandler(nil, store, nil, 1<<20, opts...)
			r := ownedJSON(t, `{"content":"private"}`)
			r = r.WithContext(middleware.ContextWithSubject(r.Context(), middleware.VerifiedSubject{
				Principal: domain.Principal{ID: uuid.NewString(), TenantID: uuid.NewString()}, ExpiresAt: time.Now().Add(time.Minute)}))
			rec := httptest.NewRecorder()
			handler(rec, r)
			require.Equal(t, http.StatusServiceUnavailable, rec.Code)
		})
	}
	require.Empty(t, store.authorized)
	require.Empty(t, store.accepted)
}

func TestOwnedIngestRequiresOwnerAwareStore(t *testing.T) {
	handler := IngestHandler(nil, &validationAcceptStore{}, nil, 1<<20, WithOwnedIngestion(), WithContentDedup(domain.ContentDedupOff))
	r := ownedJSON(t, `{"content":"private"}`)
	r = r.WithContext(middleware.ContextWithSubject(r.Context(), middleware.VerifiedSubject{
		Principal: domain.Principal{ID: uuid.NewString(), TenantID: uuid.NewString()}, ExpiresAt: time.Now().Add(time.Minute)}))
	rec := httptest.NewRecorder()
	handler(rec, r)
	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
}

func TestOwnedJSONIngestStampsTheVerifiedOwner(t *testing.T) {
	h := newOwnedHarness(t)
	rec := h.serve(ownedJSON(t, `{"content":"private text","collection":"team","doc_type":"note","metadata":{"topic":"a"}}`))
	require.Equal(t, http.StatusAccepted, rec.Code, rec.Body.String())
	require.Equal(t, "private, no-store", rec.Header().Get("Cache-Control"))
	require.Len(t, h.store.accepted, 1)
	req := h.store.accepted[0]
	require.Equal(t, h.principal, req.Owner.Principal)
	_, err := uuid.Parse(req.Owner.Incarnation)
	require.NoError(t, err)
	require.NotEqual(t, string(req.Document.ID), req.Owner.Incarnation)
	require.Equal(t, domain.ContentDedupOff, req.ContentDedup)
	require.True(t, req.SupportsScopedVectors)
	require.False(t, req.CallerSupplied)
	require.Equal(t, "team", req.Collection)
	require.Equal(t, chunker.ScopedFingerprint("team", "private text"), req.Fingerprint)
	require.Zero(t, h.store.sharedReads)
}

func TestOwnedJSONSuppliedIdentityDecisions(t *testing.T) {
	reserved, existing := uuid.NewString(), uuid.NewString()
	reservedIncarnation, existingIncarnation := uuid.NewString(), uuid.NewString()
	targets := map[string]domain.OwnedTarget{
		reserved: {Decision: domain.OwnedReserved, Incarnation: reservedIncarnation},
		existing: {Decision: domain.OwnedExisting, Incarnation: existingIncarnation},
	}
	for _, tc := range []struct {
		name, id, incarnation string
		outcome               domain.AcceptOutcome
		status                int
	}{
		{"reservation", reserved, reservedIncarnation, domain.AcceptAccepted, http.StatusAccepted},
		{"own retry", existing, existingIncarnation, domain.AcceptRequeued, http.StatusAccepted},
		{"own duplicate", existing, existingIncarnation, domain.AcceptDuplicate, http.StatusOK},
		{"own processing", existing, existingIncarnation, domain.AcceptInProgress, http.StatusAccepted},
		{"own conflict", existing, existingIncarnation, domain.AcceptConflict, http.StatusConflict},
		{"own retry at capacity", existing, existingIncarnation, domain.AcceptQueueFull, http.StatusServiceUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newOwnedHarness(t)
			h.store.targets, h.store.result = targets, tc.outcome
			rec := h.serve(ownedJSON(t, `{"content":"private","document_id":"`+strings.ToUpper(tc.id)+`"}`))
			require.Equal(t, tc.status, rec.Code, rec.Body.String())
			require.Equal(t, []string{tc.id}, h.store.authorized, "authorization receives the canonical identity")
			require.Len(t, h.store.accepted, 1)
			require.Equal(t, tc.incarnation, h.store.accepted[0].Owner.Incarnation)
			require.True(t, h.store.accepted[0].CallerSupplied)
			require.Zero(t, h.store.sharedReads)
		})
	}
}

func TestOwnedUnusableIdentitiesAreIndistinguishable(t *testing.T) {
	// Missing, unreserved, deleted, unassigned and foreign identities are one decision.
	// The harness cannot tell them apart, so the handler must answer identically
	// and touch nothing: no shared lookup, no extraction, no object write, no acceptance.
	run := func(name string, request func(id string) *http.Request) {
		t.Run(name, func(t *testing.T) {
			var first *httptest.ResponseRecorder
			for range 3 {
				h := newOwnedHarness(t)
				h.rebuild(1 << 20)
				rec := h.serve(request(uuid.NewString()))
				require.Equal(t, http.StatusNotFound, rec.Code, rec.Body.String())
				require.Zero(t, h.store.sharedReads)
				require.Empty(t, h.store.accepted)
				require.Empty(t, h.objects.uploads)
				require.Zero(t, h.extracted)
				if first == nil {
					first = rec
					continue
				}
				require.Equal(t, first.Body.String(), rec.Body.String())
				require.Equal(t, first.Header(), rec.Header())
			}
		})
	}
	run("json", func(id string) *http.Request { return ownedJSON(t, `{"content":"private","document_id":"`+id+`"}`) })
	run("multipart", func(id string) *http.Request {
		return ownedUpload(t, true, []byte(ownedBody), uploadPart{"document_id", id})
	})
}

func TestOwnedForbiddenCollectionStopsBeforeAnyEffect(t *testing.T) {
	for _, fileFirst := range []bool{true, false} {
		t.Run(fmt.Sprintf("file first %v", fileFirst), func(t *testing.T) {
			h := newOwnedHarness(t)
			h.store.forbidden = true
			rec := h.serve(ownedUpload(t, fileFirst, []byte(ownedBody), uploadPart{"collection", "locked"}, uploadPart{"document_id", uuid.NewString()}))
			require.Equal(t, http.StatusForbidden, rec.Code, rec.Body.String())
			require.Zero(t, h.extracted)
			require.Empty(t, h.objects.uploads)
			require.Empty(t, h.store.accepted)
			require.Zero(t, h.store.sharedReads)
		})
	}
}

func TestOwnedMultipartBehaviorIsIndependentOfFieldOrder(t *testing.T) {
	fields := []uploadPart{{"collection", "team"}, {"doc_type", "note"}, {"metadata", `{"a":1}`}}
	var accepted []domain.AcceptRequest
	for _, fileFirst := range []bool{true, false} {
		h := newOwnedHarness(t)
		h.rebuild(1 << 20)
		rec := h.serve(ownedUpload(t, fileFirst, []byte(ownedBody), fields...))
		require.Equal(t, http.StatusAccepted, rec.Code, rec.Body.String())
		require.Len(t, h.store.accepted, 1)
		accepted = append(accepted, h.store.accepted[0])
	}
	require.Equal(t, accepted[0].Collection, accepted[1].Collection)
	require.Equal(t, accepted[0].DocType, accepted[1].DocType)
	require.Equal(t, accepted[0].Fingerprint, accepted[1].Fingerprint)
	require.Equal(t, accepted[0].Document.Metadata, accepted[1].Document.Metadata)

	for _, fileFirst := range []bool{true, false} {
		h := newOwnedHarness(t)
		h.rebuild(64)
		rec := h.serve(ownedUpload(t, fileFirst, bytes.Repeat([]byte("x"), 4096), fields...))
		require.Equal(t, http.StatusRequestEntityTooLarge, rec.Code, "the limit applies whatever the field order")
		require.Empty(t, h.store.authorized)
	}
}

func TestOwnedMultipartRejectsRepeatedFieldsAndQueryFields(t *testing.T) {
	id := uuid.NewString()
	for name, parts := range map[string][]uploadPart{
		"document":   {{"document_id", id}, {"document_id", id}},
		"collection": {{"collection", "a"}, {"collection", "b"}},
		"doc type":   {{"doc_type", "a"}, {"doc_type", "b"}},
		"metadata":   {{"metadata", `{}`}, {"metadata", `{}`}},
	} {
		t.Run(name, func(t *testing.T) {
			h := newOwnedHarness(t)
			rec := h.serve(ownedUpload(t, false, []byte(ownedBody), parts...))
			require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
			require.Empty(t, h.store.authorized)
		})
	}
	h := newOwnedHarness(t)
	r := ownedUpload(t, false, []byte(ownedBody))
	r.URL.RawQuery = "document_id=" + id + "&collection=elsewhere"
	require.Equal(t, http.StatusAccepted, h.serve(r).Code)
	require.Equal(t, []string{""}, h.store.authorized, "a query parameter cannot supply a form field")
}

func TestOwnedIngestRejectsReservedMetadata(t *testing.T) {
	for _, key := range []string{
		"tenant_id", "Tenant_ID", "document_revision", "Document_Revision",
		"vector_attempt", "Vector_Attempt", "document_incarnation", "Document_Incarnation",
		"owner_id", "principal_id", "incarnation", "doc_id", "document_id",
		"collection", "doc_type", "chunk_index", "kind", "text",
	} {
		t.Run(key, func(t *testing.T) {
			h := newOwnedHarness(t)
			rec := h.serve(ownedJSON(t, `{"content":"private","metadata":{"`+key+`":"x"}}`))
			require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
			rec = h.serve(ownedUpload(t, false, []byte(ownedBody), uploadPart{"metadata", `{"` + key + `":"x"}`}))
			require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
			require.Empty(t, h.store.authorized)
			require.Empty(t, h.store.accepted)
		})
	}
	h := newOwnedHarness(t)
	require.Equal(t, http.StatusAccepted, h.serve(ownedJSON(t, `{"content":"private","metadata":{"topic":"a","nested":{"tenant_id":"b"}}}`)).Code)
}

func TestOwnedUploadObjectIsPinnedToTheIntendedIncarnation(t *testing.T) {
	reserved, reservedIncarnation := uuid.NewString(), uuid.NewString()
	h := newOwnedHarness(t)
	h.rebuild(1 << 20)
	h.store.targets[reserved] = domain.OwnedTarget{Decision: domain.OwnedReserved, Incarnation: reservedIncarnation}
	require.Equal(t, http.StatusAccepted, h.serve(ownedUpload(t, false, []byte(ownedBody), uploadPart{"document_id", reserved})).Code)
	require.Len(t, h.objects.uploads, 1)
	require.True(t, strings.HasPrefix(h.objects.uploads[0], "object/"+reserved+"/"+reservedIncarnation+"/"), h.objects.uploads[0])
	require.Equal(t, reservedIncarnation, h.store.accepted[0].Owner.Incarnation)

	require.Equal(t, http.StatusAccepted, h.serve(ownedUpload(t, false, []byte(ownedBody))).Code)
	require.Len(t, h.objects.uploads, 2)
	created := h.store.accepted[1]
	require.True(t, strings.HasPrefix(h.objects.uploads[1], "object/"+string(created.Document.ID)+"/"+created.Owner.Incarnation+"/"), h.objects.uploads[1])
	require.NotEqual(t, h.objects.uploads[0], h.objects.uploads[1])
}

func TestOwnedExistingRetryWritesNoObject(t *testing.T) {
	existing, incarnation := uuid.NewString(), uuid.NewString()
	for _, tc := range []struct {
		outcome domain.AcceptOutcome
		status  int
	}{{domain.AcceptQueueFull, http.StatusServiceUnavailable}, {domain.AcceptConflict, http.StatusConflict}, {domain.AcceptRequeued, http.StatusAccepted}} {
		h := newOwnedHarness(t)
		h.rebuild(1 << 20)
		h.store.targets[existing] = domain.OwnedTarget{Decision: domain.OwnedExisting, Incarnation: incarnation}
		h.store.result = tc.outcome
		rec := h.serve(ownedUpload(t, false, []byte(ownedBody), uploadPart{"document_id", existing}))
		require.Equal(t, tc.status, rec.Code, rec.Body.String())
		require.Empty(t, h.objects.uploads)
		require.Empty(t, h.objects.deleted)
		require.Len(t, h.store.accepted, 1)
	}
}

func TestOwnedRefusedAcceptanceReleasesOnlyTheAttemptObject(t *testing.T) {
	other := "object/replacement"
	for _, tc := range []struct {
		name    string
		outcome domain.AcceptOutcome
		err     error
		stored  *string
		status  int
		deleted bool
	}{
		{"permission revoked", domain.AcceptForbidden, nil, nil, http.StatusForbidden, true},
		{"identity retired", domain.AcceptNotFound, nil, nil, http.StatusNotFound, true},
		{"queue full", domain.AcceptQueueFull, nil, nil, http.StatusServiceUnavailable, true},
		{"uncertain with a replacement's key", domain.AcceptConflict, errors.New("connection lost"), &other, http.StatusServiceUnavailable, true},
		{"uncertain without a stored key", domain.AcceptConflict, errors.New("connection lost"), nil, http.StatusServiceUnavailable, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newOwnedHarness(t)
			h.rebuild(1 << 20)
			h.store.result, h.store.acceptErr, h.store.storedKey = tc.outcome, tc.err, tc.stored
			rec := h.serve(ownedUpload(t, false, []byte(ownedBody)))
			require.Equal(t, tc.status, rec.Code, rec.Body.String())
			require.Len(t, h.objects.uploads, 1)
			require.Equal(t, []string{h.objects.uploads[0]}, h.objects.deleted)
			require.NotContains(t, h.objects.deleted, other)
		})
	}
	t.Run("uncertain acceptance that did commit keeps its object", func(t *testing.T) {
		h := newOwnedHarness(t)
		attempt := "object/attempt"
		h.store.storedKey = &attempt
		releaseAttemptObject(t.Context(), h.store, h.objects, "doc", attempt, errors.New("response lost"))
		require.Empty(t, h.objects.deleted, "the stored key equal to the attempt's key proves the commit")
		h.store.storedKey, h.store.keyLookupErr = nil, errors.New("unreadable")
		releaseAttemptObject(t.Context(), h.store, h.objects, "doc", attempt, errors.New("response lost"))
		require.Empty(t, h.objects.deleted, "an unreadable state keeps the object for reconciliation")
	})
	t.Run("failed upload accepts nothing", func(t *testing.T) {
		h := newOwnedHarness(t)
		h.rebuild(1 << 20)
		h.objects.uploadErr = errors.New("storage down")
		rec := h.serve(ownedUpload(t, false, []byte(ownedBody)))
		require.Equal(t, http.StatusServiceUnavailable, rec.Code)
		require.Empty(t, h.store.accepted)
	})
}

func TestOwnedIdenticalBytesFromTwoPrincipalsStayIndependent(t *testing.T) {
	h := newOwnedHarness(t)
	h.rebuild(1 << 20)
	second := domain.Principal{ID: uuid.NewString(), TenantID: uuid.NewString()}
	for _, principal := range []domain.Principal{h.principal, second} {
		rec := h.serveAs(ownedUpload(t, false, []byte(ownedBody)), principal, time.Now().Add(time.Minute))
		require.Equal(t, http.StatusAccepted, rec.Code, rec.Body.String())
	}
	require.Len(t, h.store.accepted, 2)
	a, b := h.store.accepted[0], h.store.accepted[1]
	require.NotEqual(t, a.Document.ID, b.Document.ID)
	require.NotEqual(t, a.Owner.Incarnation, b.Owner.Incarnation)
	require.NotEqual(t, a.Owner.Principal, b.Owner.Principal)
	require.NotEqual(t, *a.Document.S3Key, *b.Document.S3Key)
	require.Zero(t, h.store.sharedReads)
}

func TestOwnedDeferredOCRUsesPrivateIdentity(t *testing.T) {
	h := newOwnedHarness(t, WithOCR(true), WithExtractor(func(context.Context, extractor.FileType, []byte) (string, error) {
		return "", extractor.ErrNoText
	}))
	data := []byte("%PDF-1.4\nsynthetic image-only PDF")
	r := ocrUpload(t, "scan.pdf", "team", data)
	rec := h.serve(r)
	require.Equal(t, http.StatusAccepted, rec.Code, rec.Body.String())
	require.Len(t, h.store.accepted, 1)
	req := h.store.accepted[0]
	require.Equal(t, "ocr_pending", req.Document.TextSource)
	require.Empty(t, req.Document.Content)
	require.True(t, req.RequireSource)
	require.Equal(t, domain.ContentDedupOff, req.ContentDedup)
	require.Equal(t, chunker.ScopedFingerprint("team", string(data)), req.Fingerprint)
	require.Len(t, h.objects.uploads, 1)
	require.Zero(t, h.store.sharedReads)
}

func TestOwnedMixedPDFNeverUsesSharedOrLegacyLookups(t *testing.T) {
	h := newOwnedHarness(t, WithMixedPDFOCR(func(ctx context.Context, data []byte) (domain.PDFPages, error) {
		return extractor.ExtractPDFPages(ctx, bytes.NewReader(data))
	}))
	data := testutil.PDFWithPageAnnotation("", "BT /F1 14 Tf 20 150 Td (Operations register) Tj ET", "")
	rec := h.serve(ocrUpload(t, "register.pdf", "team", data))
	require.Equal(t, http.StatusAccepted, rec.Code, rec.Body.String())
	require.Len(t, h.store.accepted, 1, "one authorized acceptance replaces every shared pre-lookup")
	req := h.store.accepted[0]
	require.NotNil(t, req.PDF)
	require.False(t, req.TryLegacyPDFText)
	require.Equal(t, domain.ContentDedupOff, req.ContentDedup)
	require.Equal(t, mixedPDFFingerprint("team", string(data)), req.Fingerprint)
	require.Zero(t, h.store.sharedReads)
	require.Len(t, h.objects.uploads, 1)
}

func TestOwnedDocumentContentFingerprintStaysPrivate(t *testing.T) {
	// Two principals uploading the same JSON text each get their own document.
	h := newOwnedHarness(t)
	for range 2 {
		require.Equal(t, http.StatusAccepted, h.serve(ownedJSON(t, `{"content":"same text"}`)).Code)
	}
	require.Len(t, h.store.accepted, 2)
	require.NotEqual(t, h.store.accepted[0].Document.ID, h.store.accepted[1].Document.ID)
	require.Zero(t, h.store.sharedReads)
}

func TestOwnedEvaluationReceiptsAreRefused(t *testing.T) {
	h := newOwnedHarness(t)
	r := ownedJSON(t, `{"content":"private"}`)
	r.Header.Set(evaluation.ReceiptHeader, "receipt")
	rec := h.serve(r)
	require.NotEqual(t, http.StatusAccepted, rec.Code)
	require.Empty(t, h.store.accepted)
}

func TestOwnedIngestMapsExpiredAcceptanceToUnauthorized(t *testing.T) {
	reserved, reservedIncarnation := uuid.NewString(), uuid.NewString()
	existing, existingIncarnation := uuid.NewString(), uuid.NewString()
	expires := time.Now().Add(time.Minute)

	for _, tc := range []struct {
		name          string
		request       func() *http.Request
		target        *domain.OwnedTarget
		suppliedID    string
		expectUpload  bool
		expectDeleted bool
	}{
		{
			name:    "json new",
			request: func() *http.Request { return ownedJSON(t, `{"content":"private"}`) },
		},
		{
			name:         "multipart new",
			request:      func() *http.Request { return ownedUpload(t, false, []byte(ownedBody)) },
			expectUpload: true, expectDeleted: true,
		},
		{
			name: "multipart reserved",
			request: func() *http.Request {
				return ownedUpload(t, false, []byte(ownedBody), uploadPart{"document_id", reserved})
			},
			target:       &domain.OwnedTarget{Decision: domain.OwnedReserved, Incarnation: reservedIncarnation},
			suppliedID:   reserved,
			expectUpload: true, expectDeleted: true,
		},
		{
			name: "multipart retry",
			request: func() *http.Request {
				return ownedUpload(t, false, []byte(ownedBody), uploadPart{"document_id", existing})
			},
			target:     &domain.OwnedTarget{Decision: domain.OwnedExisting, Incarnation: existingIncarnation},
			suppliedID: existing,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newOwnedHarness(t)
			h.rebuild(1 << 20)
			if tc.target != nil {
				h.store.targets[tc.suppliedID] = *tc.target
			}
			h.store.result = domain.AcceptUnauthorized
			rec := h.serveAs(tc.request(), h.principal, expires)
			require.Equal(t, http.StatusUnauthorized, rec.Code, rec.Body.String())
			require.Len(t, h.store.accepted, 1)
			require.True(t, h.store.accepted[0].Owner.ValidUntil.Equal(expires))
			if tc.expectUpload {
				require.Len(t, h.objects.uploads, 1)
			} else {
				require.Empty(t, h.objects.uploads)
			}
			if tc.expectDeleted {
				require.Equal(t, h.objects.uploads, h.objects.deleted)
			} else {
				require.Empty(t, h.objects.deleted)
			}
		})
	}
}
