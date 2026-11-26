package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/api/middleware"
	"github.com/Victoroide/quarry-rag/internal/cleanup"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

type scopedMemoryEntry struct {
	summary     domain.DocumentSummary
	owner       domain.Principal
	s3Key       *string
	readers     map[string]bool
	downloaders map[string]bool
	deleting    bool
	finalized   bool
	step        int
}

// scopedMockStore implements ScopedDocumentLister, ScopedDocumentFileReader, and ScopedDocumentDeleter
// while also embedding legacy methods that must never be invoked in scoped mode.
type scopedMockStore struct {
	domain.ChunkStore
	mu              sync.Mutex
	docs            map[string]*scopedMemoryEntry
	active          map[string]bool
	revision        int64
	listErr         error
	statusErr       error
	policyErr       error
	fileErr         error
	beginDeleteErr  error
	resumeDeleteErr error
	legacyCalls     int
	signAttempts    int
	beforePolicy    func()
	beforeSign      func()
	beforeBegin     func()
	afterBegin      func()
	stepHook        func(step int) error
	now             func() time.Time
}

func newScopedMockStore(principals ...domain.Principal) *scopedMockStore {
	active := make(map[string]bool, len(principals))
	for _, p := range principals {
		active[p.ID] = true
	}
	return &scopedMockStore{
		docs:     make(map[string]*scopedMemoryEntry),
		active:   active,
		revision: 1,
	}
}

func (s *scopedMockStore) ListDocuments(context.Context) ([]domain.DocumentRef, error) {
	s.legacyCalls++
	return nil, errors.New("legacy lister must not be called")
}

func (s *scopedMockStore) StreamDocuments(context.Context, func(domain.DocumentSummary) error) error {
	s.legacyCalls++
	return errors.New("unscoped stream must not be called")
}

func (s *scopedMockStore) ListDocumentsPage(context.Context, string, int) ([]domain.DocumentSummary, error) {
	s.legacyCalls++
	return nil, errors.New("unscoped page must not be called")
}

func (s *scopedMockStore) GetDocumentSummary(context.Context, domain.DocumentID) (*domain.DocumentSummary, error) {
	s.legacyCalls++
	return nil, errors.New("unscoped summary must not be called")
}

func (s *scopedMockStore) WithDocumentFile(context.Context, domain.DocumentID, func(context.Context, string) error) (bool, error) {
	s.legacyCalls++
	return false, errors.New("unscoped file must not be called")
}

func (s *scopedMockStore) GetDocumentS3Key(context.Context, domain.DocumentID) (*string, error) {
	s.legacyCalls++
	return nil, errors.New("unscoped key lookup must not be called")
}

func (s *scopedMockStore) DeletionActive(context.Context) (bool, error) {
	s.legacyCalls++
	return false, errors.New("unscoped deletion check must not be called")
}

func (s *scopedMockStore) DeleteWithCleanup(context.Context, domain.DocumentID, domain.DeleteOperations) error {
	s.legacyCalls++
	return errors.New("unscoped deletion must not be called")
}

func (s *scopedMockStore) GetDocument(context.Context, domain.DocumentID) (*domain.Document, error) {
	s.legacyCalls++
	return nil, errors.New("unscoped get document must not be called")
}

func (s *scopedMockStore) ScopedPolicyState(_ context.Context, principal domain.Principal) (domain.ScopedPolicyState, error) {
	if s.beforePolicy != nil {
		s.beforePolicy()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.policyErr != nil {
		return domain.ScopedPolicyState{}, s.policyErr
	}
	return domain.ScopedPolicyState{Active: s.active[principal.ID], Revision: s.revision}, nil
}

func (s *scopedMockStore) canReadLocked(entry *scopedMemoryEntry, principal domain.Principal) bool {
	if entry.deleting || entry.owner.TenantID != principal.TenantID {
		return false
	}
	if entry.owner.ID == principal.ID {
		return true
	}
	return entry.readers[principal.ID]
}

func (s *scopedMockStore) ListScopedDocumentsPage(_ context.Context, principal domain.Principal, after string, limit int, collection string, includeIncarnation bool) (domain.ScopedDocumentPage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listErr != nil {
		return domain.ScopedDocumentPage{}, s.listErr
	}
	if !s.active[principal.ID] {
		return domain.ScopedDocumentPage{Active: false, Revision: s.revision}, nil
	}
	ids := make([]string, 0, len(s.docs))
	for id := range s.docs {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	docs := make([]domain.DocumentSummary, 0)
	for _, id := range ids {
		if after != "" && id <= after {
			continue
		}
		entry := s.docs[id]
		if !s.canReadLocked(entry, principal) {
			continue
		}
		if collection != "" && entry.summary.Collection != collection {
			continue
		}
		doc := entry.summary
		doc.S3Key = entry.s3Key
		if !includeIncarnation {
			doc.Incarnation = ""
		}
		docs = append(docs, doc)
		if len(docs) == limit {
			break
		}
	}
	return domain.ScopedDocumentPage{Active: true, Revision: s.revision, Documents: docs}, nil
}

func (s *scopedMockStore) GetScopedDocumentSummary(_ context.Context, principal domain.Principal, id domain.DocumentID, includeIncarnation bool) (domain.ScopedDocumentSummary, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.statusErr != nil {
		return domain.ScopedDocumentSummary{}, s.statusErr
	}
	if !s.active[principal.ID] {
		return domain.ScopedDocumentSummary{Active: false, Revision: s.revision}, nil
	}
	entry, ok := s.docs[string(id)]
	if !ok || !s.canReadLocked(entry, principal) {
		return domain.ScopedDocumentSummary{Active: true, Revision: s.revision}, nil
	}
	doc := entry.summary
	doc.S3Key = entry.s3Key
	if !includeIncarnation {
		doc.Incarnation = ""
	}
	return domain.ScopedDocumentSummary{Active: true, Revision: s.revision, Document: &doc}, nil
}

func (s *scopedMockStore) WithScopedDocumentFile(ctx context.Context, principal domain.Principal, id domain.DocumentID, sign func(context.Context, string) error) (domain.ScopedFileResult, error) {
	s.mu.Lock()
	if s.fileErr != nil {
		err := s.fileErr
		s.mu.Unlock()
		return domain.ScopedFileResult{}, err
	}
	if !s.active[principal.ID] {
		s.mu.Unlock()
		return domain.ScopedFileResult{Active: false}, nil
	}
	entry, ok := s.docs[string(id)]
	allowed := ok && !entry.deleting && entry.owner.TenantID == principal.TenantID &&
		(entry.owner.ID == principal.ID || (entry.readers[principal.ID] && entry.downloaders[principal.ID]))
	var key *string
	if allowed {
		key = entry.s3Key
	}
	s.mu.Unlock()
	if !allowed || key == nil {
		return domain.ScopedFileResult{Active: true, Found: false}, nil
	}
	if s.beforeSign != nil {
		s.beforeSign()
	}
	s.signAttempts++
	if signErr := sign(ctx, *key); signErr != nil {
		return domain.ScopedFileResult{Active: true}, signErr
	}
	return domain.ScopedFileResult{Active: true, Found: true}, nil
}

func (s *scopedMockStore) BeginScopedDeletion(ctx context.Context, principal domain.Principal, validUntil time.Time, id domain.DocumentID, ops domain.DeleteOperations) (incarnation string, complete bool, err error) {
	if s.beforeBegin != nil {
		s.beforeBegin()
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return "", false, ctxErr
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.beginDeleteErr != nil {
		return "", false, s.beginDeleteErr
	}
	if !s.active[principal.ID] {
		return "", false, domain.ErrForbidden
	}
	entry, ok := s.docs[string(id)]
	if !ok {
		return "", false, domain.ErrDocumentNotFound
	}
	if entry.owner.TenantID != principal.TenantID || entry.owner.ID != principal.ID {
		return "", false, domain.ErrDocumentNotFound
	}
	nowFn := time.Now
	if s.now != nil {
		nowFn = s.now
	}
	if !nowFn().Before(validUntil) {
		return "", false, domain.ErrUnauthorized
	}
	if ops.ExpectedIncarnation != "" && ops.ExpectedIncarnation != entry.summary.Incarnation {
		return "", false, domain.ErrDeletionPreconditionFailed
	}
	if entry.finalized {
		if ops.ExpectedIncarnation != "" {
			return entry.summary.Incarnation, true, nil
		}
		return "", false, domain.ErrDocumentNotFound
	}
	if entry.s3Key != nil && ops.Source == nil {
		return "", false, domain.ErrFileStorageUnavailable
	}
	entry.deleting = true
	if s.afterBegin != nil {
		s.afterBegin()
	}
	return entry.summary.Incarnation, false, nil
}

func (s *scopedMockStore) ResumeScopedDeletion(ctx context.Context, id domain.DocumentID, incarnation string, ops domain.DeleteOperations) error {
	if s.resumeDeleteErr != nil {
		return s.resumeDeleteErr
	}
	s.mu.Lock()
	entry, ok := s.docs[string(id)]
	if !ok || entry.summary.Incarnation != incarnation || !entry.deleting {
		s.mu.Unlock()
		return domain.ErrDocumentNotFound
	}
	step := entry.step
	key := entry.s3Key
	s.mu.Unlock()

	for ; step < 3; step++ {
		if s.stepHook != nil {
			if err := s.stepHook(step); err != nil {
				return err
			}
		}
		switch step {
		case 0:
			if ops.LegacyNamespace != nil {
				if err := ops.LegacyNamespace(ctx, string(id)); err != nil {
					return err
				}
			}
		case 1:
			if ops.RevisionVectors != nil {
				if err := ops.RevisionVectors(ctx, string(id), incarnation); err != nil {
					return err
				}
			} else if ops.LegacyVectors != nil {
				if err := ops.LegacyVectors(ctx, string(id)); err != nil {
					return err
				}
			}
		case 2:
			if key != nil && ops.Source != nil {
				if err := ops.Source(ctx, *key); err != nil {
					return err
				}
			}
		}
		s.mu.Lock()
		entry.step = step + 1
		s.mu.Unlock()
	}
	s.mu.Lock()
	entry.finalized = true
	delete(s.docs, string(id))
	s.mu.Unlock()
	return nil
}

type expiringSignerSpy struct {
	mu    sync.Mutex
	calls []signedCall
	err   error
}

type signedCall struct {
	key string
	ttl time.Duration
}

func (s *expiringSignerSpy) PresignGet(context.Context, string) (string, error) {
	return "", errors.New("unbounded presign must not be called in scoped mode")
}

func (s *expiringSignerSpy) PresignGetWithTTL(_ context.Context, key string, ttl time.Duration) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, signedCall{key: key, ttl: ttl})
	if s.err != nil {
		return "", s.err
	}
	return (&url.URL{Scheme: "https", Host: "signed.example", Path: "/" + key}).String(), nil
}

func (s *expiringSignerSpy) recorded() []signedCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]signedCall(nil), s.calls...)
}

type deleteStorageSpy struct {
	mu      sync.Mutex
	vectors []string
	objects []string
	errOn   string
}

func (d *deleteStorageSpy) DeleteAllVectorsInNamespace(_ context.Context, ns string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.vectors = append(d.vectors, "namespace:"+ns)
	if d.errOn == "namespace" {
		return errors.New("pinecone namespace failure")
	}
	return nil
}

func (d *deleteStorageSpy) DeleteByDocID(_ context.Context, id string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.vectors = append(d.vectors, "doc:"+id)
	if d.errOn == "vectors" {
		return errors.New("pinecone doc failure")
	}
	return nil
}

func (d *deleteStorageSpy) DeleteByRevision(_ context.Context, id, inc string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.vectors = append(d.vectors, "revision:"+id+"/"+inc)
	if d.errOn == "revision" {
		return errors.New("pinecone revision failure")
	}
	return nil
}

func (d *deleteStorageSpy) Delete(_ context.Context, key string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.objects = append(d.objects, key)
	if d.errOn == "source" {
		return errors.New("s3 delete failure")
	}
	return nil
}

func serveScoped(h http.HandlerFunc, method, target string, principal *domain.Principal, expires time.Time) *httptest.ResponseRecorder {
	req := httptest.NewRequestWithContext(context.Background(), method, target, http.NoBody)
	if strings.Contains(target, "/documents/") {
		trimmed := strings.TrimPrefix(target, "/documents/")
		idPart, _, _ := strings.Cut(trimmed, "?")
		idPart, _, _ = strings.Cut(idPart, "/")
		req.SetPathValue("doc_id", idPart)
	}
	if principal != nil {
		req = req.WithContext(middleware.ContextWithSubject(req.Context(), middleware.VerifiedSubject{Principal: *principal, ExpiresAt: expires}))
	}
	rec := httptest.NewRecorder()
	h(rec, req)
	return rec
}

func TestScopedListingAndStatusEnforceGrantsAndInvariance(t *testing.T) {
	tenantA, tenantB := uuid.NewString(), uuid.NewString()
	ownerA := domain.Principal{ID: uuid.NewString(), TenantID: tenantA}
	peerA := domain.Principal{ID: uuid.NewString(), TenantID: tenantA}
	ownerB := domain.Principal{ID: uuid.NewString(), TenantID: tenantB}
	store := newScopedMockStore(ownerA, peerA, ownerB)

	now := time.Date(2026, 2, 1, 10, 0, 0, 0, time.UTC)
	ids := []string{
		"00000000-0000-4000-8000-000000000001",
		"00000000-0000-4000-8000-000000000002",
		"00000000-0000-4000-8000-000000000003",
		"00000000-0000-4000-8000-000000000004",
		"00000000-0000-4000-8000-000000000005",
	}
	key1 := "documents/doc/" + ids[0] + "/secret-a.pdf"
	key2 := "documents/doc/" + ids[2] + "/secret-b.pdf"
	keyHidden := "documents/doc/" + ids[1] + "/hidden.pdf"
	name1, nameHidden, pdfType := "visible.pdf", "hidden-secret.pdf", "pdf"
	errRaw1, errRaw2, errHidden := "s3: secret/bucket/key failed", "pinecone: internal host 10.0.0.1 failed", "hidden postgres DSN secret"

	store.docs[ids[0]] = &scopedMemoryEntry{
		owner: ownerA, s3Key: &key1, readers: map[string]bool{peerA.ID: true},
		summary: domain.DocumentSummary{ID: ids[0], DocType: "manual", Collection: "", Status: "completed", ChunkCount: 3, FileName: &name1, FileType: &pdfType, CreatedAt: now, UpdatedAt: now, CompletedAt: &now, Incarnation: uuid.NewString()},
	}
	store.docs[ids[1]] = &scopedMemoryEntry{
		owner: ownerB, s3Key: &keyHidden,
		summary: domain.DocumentSummary{ID: ids[1], DocType: "secret", Collection: "team", Status: "failed", FileName: &nameHidden, CreatedAt: now, UpdatedAt: now, LastError: &errHidden, Incarnation: uuid.NewString()},
	}
	store.docs[ids[2]] = &scopedMemoryEntry{
		owner: ownerA, s3Key: &key2,
		summary: domain.DocumentSummary{ID: ids[2], DocType: "guide", Collection: "team", Status: "failed", FileName: &name1, FileType: &pdfType, CreatedAt: now, UpdatedAt: now, LastError: &errRaw1, Incarnation: uuid.NewString()},
	}
	store.docs[ids[3]] = &scopedMemoryEntry{
		owner:   peerA,
		summary: domain.DocumentSummary{ID: ids[3], DocType: "note", Collection: "", Status: "processing", CreatedAt: now, UpdatedAt: now, Incarnation: uuid.NewString()},
	}
	store.docs[ids[4]] = &scopedMemoryEntry{
		owner:   ownerA,
		summary: domain.DocumentSummary{ID: ids[4], DocType: "note", Collection: "", Status: "processing", CreatedAt: now, UpdatedAt: now, Incarnation: uuid.NewString()},
	}

	listH := ListDocumentsHandler(store, WithScopedListing(), withScopedStreamBatchSize(2))
	statusH := DocumentStatusHandler(store, WithScopedStatus())

	// Baseline output for ownerA (stream, paginated, and status).
	streamRec := serveScoped(listH, http.MethodGet, "/documents", &ownerA, time.Now().Add(time.Minute))
	require.Equal(t, http.StatusOK, streamRec.Code, streamRec.Body.String())
	require.Equal(t, "private, no-store", streamRec.Header().Get("Cache-Control"))
	require.NotContains(t, streamRec.Body.String(), "s3_key")
	require.NotContains(t, streamRec.Body.String(), errRaw1)
	require.Contains(t, streamRec.Body.String(), `"last_error":"processing_failed"`)
	require.Contains(t, streamRec.Body.String(), `"count":3`)

	page1 := serveScoped(listH, http.MethodGet, "/documents?limit=2", &ownerA, time.Now().Add(time.Minute))
	require.Equal(t, http.StatusOK, page1.Code)
	require.Equal(t, "private, no-store", page1.Header().Get("Cache-Control"))
	var pageBody struct {
		Documents []map[string]any `json:"documents"`
		Count     int              `json:"count"`
		NextAfter *string          `json:"next_after"`
	}
	require.NoError(t, json.Unmarshal(page1.Body.Bytes(), &pageBody))
	require.Equal(t, 2, pageBody.Count)
	require.NotNil(t, pageBody.NextAfter)
	require.Equal(t, ids[2], *pageBody.NextAfter)

	// Cursor set to hidden ids[1] acts solely as exclusive comparison.
	afterHidden := serveScoped(listH, http.MethodGet, "/documents?limit=2&after="+strings.ToUpper(ids[1]), &ownerA, time.Now().Add(time.Minute))
	require.Equal(t, http.StatusOK, afterHidden.Code)
	require.Contains(t, afterHidden.Body.String(), ids[2])
	require.Contains(t, afterHidden.Body.String(), ids[4])
	require.NotContains(t, afterHidden.Body.String(), ids[0])

	// Empty collection searches across ownerA's tenant (ids[0], ids[2], ids[4]); peerA cannot see ownerA's default-collection doc ids[4].
	peerDefault := serveScoped(listH, http.MethodGet, "/documents?limit=10", &peerA, time.Now().Add(time.Minute))
	require.Equal(t, http.StatusOK, peerDefault.Code)
	require.Contains(t, peerDefault.Body.String(), ids[0])
	require.Contains(t, peerDefault.Body.String(), ids[3])
	require.NotContains(t, peerDefault.Body.String(), ids[4])

	// Unauthorized collection yields empty page.
	unauthCollection := serveScoped(listH, http.MethodGet, "/documents?limit=10&collection=locked", &peerA, time.Now().Add(time.Minute))
	require.Equal(t, http.StatusOK, unauthCollection.Code)
	require.Contains(t, unauthCollection.Body.String(), `"documents":[],"count":0,"next_after":null`)

	// Mutate hidden documents' filenames/errors, ownerA's S3 key, and ownerA's internal error within the same category:
	// visible output must be byte-for-byte identical.
	newHiddenName := "different-hidden.pdf"
	newHiddenErr := "completely different hidden failure"
	newKey2 := "documents/doc/" + ids[2] + "/moved-secret.pdf"
	store.docs[ids[1]].summary.FileName = &newHiddenName
	store.docs[ids[1]].summary.LastError = &newHiddenErr
	store.docs[ids[3]].summary.FileName = &newHiddenName
	store.docs[ids[2]].s3Key = &newKey2
	store.docs[ids[2]].summary.LastError = &errRaw2
	extraHidden := uuid.NewString()
	store.docs[extraHidden] = &scopedMemoryEntry{
		owner: ownerB, summary: domain.DocumentSummary{ID: extraHidden, Collection: "team", Status: "completed", CreatedAt: now, UpdatedAt: now},
	}

	streamAfter := serveScoped(listH, http.MethodGet, "/documents", &ownerA, time.Now().Add(time.Minute))
	require.Equal(t, streamRec.Body.String(), streamAfter.Body.String(), "streamed output must be byte-for-byte invariant")
	pageAfter := serveScoped(listH, http.MethodGet, "/documents?limit=2", &ownerA, time.Now().Add(time.Minute))
	require.Equal(t, page1.Body.String(), pageAfter.Body.String(), "paginated output must be byte-for-byte invariant")

	// Status: peerA can read ids[0] (read grant), ownerA sees bounded failure category on ids[2], neither sees hidden ids[1].
	peerStatus := serveScoped(statusH, http.MethodGet, "/documents/"+ids[0], &peerA, time.Now().Add(time.Minute))
	require.Equal(t, http.StatusOK, peerStatus.Code)
	require.Equal(t, "private, no-store", peerStatus.Header().Get("Cache-Control"))
	require.NotContains(t, peerStatus.Body.String(), "s3_key")

	ownerStatus := serveScoped(statusH, http.MethodGet, "/documents/"+ids[2]+"?include_incarnation=true", &ownerA, time.Now().Add(time.Minute))
	require.Equal(t, http.StatusOK, ownerStatus.Code)
	require.Contains(t, ownerStatus.Body.String(), `"last_error":"processing_failed"`)
	require.Contains(t, ownerStatus.Body.String(), store.docs[ids[2]].summary.Incarnation)
	require.NotContains(t, ownerStatus.Body.String(), errRaw2)

	// Uniform 404 across missing, foreign tenant, same-tenant denied (even with include_incarnation=true).
	missingRec := serveScoped(statusH, http.MethodGet, "/documents/"+uuid.NewString()+"?include_incarnation=true", &ownerA, time.Now().Add(time.Minute))
	foreignRec := serveScoped(statusH, http.MethodGet, "/documents/"+ids[1]+"?include_incarnation=true", &ownerA, time.Now().Add(time.Minute))
	deniedRec := serveScoped(statusH, http.MethodGet, "/documents/"+ids[3]+"?include_incarnation=true", &ownerA, time.Now().Add(time.Minute))
	for _, rec := range []*httptest.ResponseRecorder{missingRec, foreignRec, deniedRec} {
		require.Equal(t, http.StatusNotFound, rec.Code)
		require.Equal(t, missingRec.Body.String(), rec.Body.String())
		require.Equal(t, missingRec.Header(), rec.Header())
		require.Equal(t, "private, no-store", rec.Header().Get("Cache-Control"))
	}
	require.Zero(t, store.legacyCalls)
}

func TestScopedListingRechecksPolicyAndSubjectAcrossBatches(t *testing.T) {
	tenant := uuid.NewString()
	owner := domain.Principal{ID: uuid.NewString(), TenantID: tenant}
	store := newScopedMockStore(owner)
	now := time.Now()
	for _, id := range []string{
		"00000000-0000-4000-8000-000000000011",
		"00000000-0000-4000-8000-000000000012",
		"00000000-0000-4000-8000-000000000013",
	} {
		store.docs[id] = &scopedMemoryEntry{
			owner:   owner,
			summary: domain.DocumentSummary{ID: id, Status: "completed", CreatedAt: now, UpdatedAt: now},
		}
	}

	// Unrelated policy revision bump once succeeds cleanly on retry.
	var bumps int
	store.beforePolicy = func() {
		store.mu.Lock()
		defer store.mu.Unlock()
		if bumps == 0 {
			store.revision++
			bumps++
		}
	}
	listH := ListDocumentsHandler(store, WithScopedListing(), withScopedStreamBatchSize(2))
	rec := serveScoped(listH, http.MethodGet, "/documents?limit=2", &owner, time.Now().Add(time.Minute))
	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, rec.Body.String(), `"count":2`)

	// Disabling principal mid-stream aborts batch 2 before closing trailer.
	bumps = 1
	checks := 0
	store.beforePolicy = func() {
		checks++
		if checks == 2 {
			store.mu.Lock()
			store.active[owner.ID] = false
			store.mu.Unlock()
		}
	}
	streamRec := serveScoped(listH, http.MethodGet, "/documents", &owner, time.Now().Add(time.Minute))
	require.Contains(t, streamRec.Body.String(), "00000000-0000-4000-8000-000000000011")
	require.NotContains(t, streamRec.Body.String(), "00000000-0000-4000-8000-000000000013")
	require.NotContains(t, streamRec.Body.String(), `"count":`)

	// Disabled principal before first byte returns 403.
	store.beforePolicy = nil
	denied := serveScoped(listH, http.MethodGet, "/documents", &owner, time.Now().Add(time.Minute))
	require.Equal(t, http.StatusForbidden, denied.Code)
	require.Equal(t, "private, no-store", denied.Header().Get("Cache-Control"))

	// Unscoped store refuses fallback with 503.
	unscopedStore := &checkpointedStore{}
	unscopedH := ListDocumentsHandler(unscopedStore, WithScopedListing())
	unscopedRec := serveScoped(unscopedH, http.MethodGet, "/documents", &owner, time.Now().Add(time.Minute))
	require.Equal(t, http.StatusServiceUnavailable, unscopedRec.Code)
	require.Equal(t, "private, no-store", unscopedRec.Header().Get("Cache-Control"))
	unscopedStatusH := DocumentStatusHandler(unscopedStore, WithScopedStatus())
	require.Equal(t, http.StatusServiceUnavailable, serveScoped(unscopedStatusH, http.MethodGet, "/documents/"+uuid.NewString(), &owner, time.Now().Add(time.Minute)).Code)
	unscopedFileH := FileHandler(unscopedStore, &expiringSignerSpy{}, WithScopedFile())
	require.Equal(t, http.StatusServiceUnavailable, serveScoped(unscopedFileH, http.MethodGet, "/documents/"+uuid.NewString()+"/file", &owner, time.Now().Add(time.Minute)).Code)
	unscopedDeleteH := DeleteHandler(unscopedStore, nil, nil, WithScopedDeletion())
	require.Equal(t, http.StatusServiceUnavailable, serveScoped(unscopedDeleteH, http.MethodDelete, "/documents/"+uuid.NewString(), &owner, time.Now().Add(time.Minute)).Code)

	// Token expiring before page release returns 401.
	fixedNow := time.Date(2026, 2, 1, 12, 0, 0, 0, time.UTC)
	clockNow := fixedNow
	store.active[owner.ID] = true
	store.beforePolicy = func() { clockNow = fixedNow.Add(2 * time.Minute) }
	expiringListH := ListDocumentsHandler(store, WithScopedListing(), withListingClock(func() time.Time { return clockNow }))
	expiredList := serveScoped(expiringListH, http.MethodGet, "/documents?limit=2", &owner, fixedNow.Add(time.Minute))
	require.Equal(t, http.StatusUnauthorized, expiredList.Code)

	clockNow = fixedNow
	expiringStatusH := DocumentStatusHandler(store, WithScopedStatus(), withStatusClock(func() time.Time { return clockNow }))
	expiredStatus := serveScoped(expiringStatusH, http.MethodGet, "/documents/00000000-0000-4000-8000-000000000011", &owner, fixedNow.Add(time.Minute))
	require.Equal(t, http.StatusUnauthorized, expiredStatus.Code)
	store.beforePolicy = nil
}

func TestScopedFileSigningTTLAndUniformNotFound(t *testing.T) {
	tenantA, tenantB := uuid.NewString(), uuid.NewString()
	owner := domain.Principal{ID: uuid.NewString(), TenantID: tenantA}
	downloader := domain.Principal{ID: uuid.NewString(), TenantID: tenantA}
	readOnly := domain.Principal{ID: uuid.NewString(), TenantID: tenantA}
	outsider := domain.Principal{ID: uuid.NewString(), TenantID: tenantB}
	store := newScopedMockStore(owner, downloader, readOnly, outsider)

	docWithFile := uuid.NewString()
	docNoFile := uuid.NewString()
	key := "documents/doc/" + docWithFile + "/source.pdf"
	store.docs[docWithFile] = &scopedMemoryEntry{
		owner:       owner,
		s3Key:       &key,
		readers:     map[string]bool{downloader.ID: true, readOnly.ID: true},
		downloaders: map[string]bool{downloader.ID: true},
		summary:     domain.DocumentSummary{ID: docWithFile, Status: "completed"},
	}
	store.docs[docNoFile] = &scopedMemoryEntry{
		owner:   owner,
		summary: domain.DocumentSummary{ID: docNoFile, Status: "completed"},
	}

	fixedNow := time.Date(2026, 2, 1, 12, 0, 0, 0, time.UTC)
	signer := &expiringSignerSpy{}
	fileH := FileHandler(store, signer, WithScopedFile(), withFileClock(func() time.Time { return fixedNow }))

	// Fresh assertion (>900s) caps at 900s; shorter assertion (45.9s) floors to 45s.
	for _, tc := range []struct {
		name      string
		principal domain.Principal
		remaining time.Duration
		wantTTL   int
	}{
		{"owner long assertion", owner, 1200 * time.Second, 900},
		{"grantee short assertion", downloader, 45*time.Second + 900*time.Millisecond, 45},
		{"exact one second", owner, time.Second, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := serveScoped(fileH, http.MethodGet, "/documents/"+docWithFile+"/file", &tc.principal, fixedNow.Add(tc.remaining))
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			require.Equal(t, "private, no-store", rec.Header().Get("Cache-Control"))
			var resp FileResponse
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			require.Equal(t, tc.wantTTL, resp.ExpiresIn)
		})
	}
	calls := signer.recorded()
	require.Len(t, calls, 3)
	require.Equal(t, signedCall{key: key, ttl: 900 * time.Second}, calls[0])
	require.Equal(t, signedCall{key: key, ttl: 45 * time.Second}, calls[1])
	require.Equal(t, signedCall{key: key, ttl: time.Second}, calls[2])

	// Less than 1 second remaining refuses signing without calling S3.
	beforeCalls := len(signer.recorded())
	subSecond := serveScoped(fileH, http.MethodGet, "/documents/"+docWithFile+"/file", &owner, fixedNow.Add(999*time.Millisecond))
	require.Equal(t, http.StatusUnauthorized, subSecond.Code)
	require.Equal(t, "private, no-store", subSecond.Header().Get("Cache-Control"))
	require.Len(t, signer.recorded(), beforeCalls)

	// Crossing below 1s during DB lookup also refuses before signing.
	clockNow := fixedNow
	advancingH := FileHandler(store, signer, WithScopedFile(), withFileClock(func() time.Time { return clockNow }))
	store.beforeSign = func() { clockNow = fixedNow.Add(1500 * time.Millisecond) }
	raced := serveScoped(advancingH, http.MethodGet, "/documents/"+docWithFile+"/file", &owner, fixedNow.Add(2*time.Second))
	require.Equal(t, http.StatusUnauthorized, raced.Code)
	require.Len(t, signer.recorded(), beforeCalls)
	store.beforeSign = nil

	// Removed download grant, missing doc, foreign tenant, no original, and disabled file storage share exact 404 envelope.
	disabledSignerH := FileHandler(store, nil, WithScopedFile(), withFileClock(func() time.Time { return fixedNow }))
	cases := []*httptest.ResponseRecorder{
		serveScoped(fileH, http.MethodGet, "/documents/"+docWithFile+"/file", &readOnly, fixedNow.Add(time.Minute)),
		serveScoped(fileH, http.MethodGet, "/documents/"+docWithFile+"/file", &outsider, fixedNow.Add(time.Minute)),
		serveScoped(fileH, http.MethodGet, "/documents/"+uuid.NewString()+"/file", &owner, fixedNow.Add(time.Minute)),
		serveScoped(fileH, http.MethodGet, "/documents/"+docNoFile+"/file", &owner, fixedNow.Add(time.Minute)),
		serveScoped(disabledSignerH, http.MethodGet, "/documents/"+docWithFile+"/file", &owner, fixedNow.Add(time.Minute)),
	}
	for i, rec := range cases {
		require.Equal(t, http.StatusNotFound, rec.Code, "case %d", i)
		require.Equal(t, cases[0].Body.String(), rec.Body.String(), "case %d body", i)
		require.Equal(t, cases[0].Header(), rec.Header(), "case %d headers", i)
		require.Equal(t, "private, no-store", rec.Header().Get("Cache-Control"))
	}
	require.Len(t, signer.recorded(), beforeCalls, "no signing call after any denial")
	require.Zero(t, store.legacyCalls)
}

func TestScopedDeletionAuthorizationFailuresAndCancellation(t *testing.T) {
	tenantA, tenantB := uuid.NewString(), uuid.NewString()
	owner := domain.Principal{ID: uuid.NewString(), TenantID: tenantA}
	grantee := domain.Principal{ID: uuid.NewString(), TenantID: tenantA}
	outsider := domain.Principal{ID: uuid.NewString(), TenantID: tenantB}
	store := newScopedMockStore(owner, grantee, outsider)

	docID := uuid.NewString()
	inc := uuid.NewString()
	key := "documents/doc/" + docID + "/" + inc + "/a/orig.pdf"
	store.docs[docID] = &scopedMemoryEntry{
		owner:       owner,
		s3Key:       &key,
		readers:     map[string]bool{grantee.ID: true},
		downloaders: map[string]bool{grantee.ID: true},
		summary:     domain.DocumentSummary{ID: docID, Incarnation: inc, Status: "completed"},
	}

	storage := &deleteStorageSpy{}
	deleteH := DeleteHandler(store, storage, storage, WithScopedDeletion(), withCleanupTargetsForTest("originals", "index"))

	// Grantee (even with read+download), outsider, and missing UUID receive identical 404 and touch zero stores.
	granteeRec := serveScoped(deleteH, http.MethodDelete, "/documents/"+docID, &grantee, time.Now().Add(time.Minute))
	outsiderRec := serveScoped(deleteH, http.MethodDelete, "/documents/"+docID, &outsider, time.Now().Add(time.Minute))
	missingRec := serveScoped(deleteH, http.MethodDelete, "/documents/"+uuid.NewString(), &owner, time.Now().Add(time.Minute))
	for _, rec := range []*httptest.ResponseRecorder{granteeRec, outsiderRec, missingRec} {
		require.Equal(t, http.StatusNotFound, rec.Code)
		require.Equal(t, missingRec.Body.String(), rec.Body.String())
		require.Equal(t, missingRec.Header(), rec.Header())
		require.Equal(t, "private, no-store", rec.Header().Get("Cache-Control"))
	}
	require.Empty(t, storage.vectors)
	require.Empty(t, storage.objects)

	// Cancellation BEFORE intent commit leaves document live and touches zero stores.
	ctxCanceled, cancel := context.WithCancel(context.Background())
	cancel()
	req := httptest.NewRequestWithContext(ctxCanceled, http.MethodDelete, "/documents/"+docID, http.NoBody)
	req.SetPathValue("doc_id", docID)
	req = req.WithContext(middleware.ContextWithSubject(req.Context(), middleware.VerifiedSubject{Principal: owner, ExpiresAt: time.Now().Add(time.Minute)}))
	rec := httptest.NewRecorder()
	deleteH(rec, req)
	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
	require.Equal(t, "private, no-store", rec.Header().Get("Cache-Control"))
	require.False(t, store.docs[docID].deleting)
	require.Empty(t, storage.vectors)
	require.Empty(t, storage.objects)

	// Provider failures on each step retain target and checkpoint progress; retry after logout is denied until re-authenticated.
	storage.errOn = "namespace"
	failStep0 := serveScoped(deleteH, http.MethodDelete, "/documents/"+docID, &owner, time.Now().Add(time.Minute))
	require.Equal(t, http.StatusInternalServerError, failStep0.Code)
	require.NotContains(t, failStep0.Body.String(), "pinecone namespace failure")

	// Retry after logout (missing or expired token) is refused before any resume.
	loggedOut := serveScoped(deleteH, http.MethodDelete, "/documents/"+docID, nil, time.Time{})
	require.Equal(t, http.StatusUnauthorized, loggedOut.Code)
	expiredRetry := serveScoped(deleteH, http.MethodDelete, "/documents/"+docID, &owner, time.Now().Add(-time.Second))
	require.Equal(t, http.StatusUnauthorized, expiredRetry.Code)

	storage.errOn = "revision"
	failStep1 := serveScoped(deleteH, http.MethodDelete, "/documents/"+docID, &owner, time.Now().Add(time.Minute))
	require.Equal(t, http.StatusInternalServerError, failStep1.Code)

	storage.errOn = "source"
	failStep2 := serveScoped(deleteH, http.MethodDelete, "/documents/"+docID, &owner, time.Now().Add(time.Minute))
	require.Equal(t, http.StatusInternalServerError, failStep2.Code)

	// Disconnect AFTER intent commits still completes cleanup for the exact incarnation & key.
	storage.errOn = ""
	liveCtx, liveCancel := context.WithCancel(context.Background())
	store.afterBegin = liveCancel
	reqAfter := httptest.NewRequestWithContext(liveCtx, http.MethodDelete, "/documents/"+docID, http.NoBody)
	reqAfter.SetPathValue("doc_id", docID)
	reqAfter = reqAfter.WithContext(middleware.ContextWithSubject(reqAfter.Context(), middleware.VerifiedSubject{Principal: owner, ExpiresAt: time.Now().Add(time.Minute)}))
	recAfter := httptest.NewRecorder()
	deleteH(recAfter, reqAfter)
	store.afterBegin = nil
	require.Equal(t, http.StatusOK, recAfter.Code, recAfter.Body.String())
	require.Nil(t, store.docs[docID])
	require.Contains(t, storage.vectors, "revision:"+docID+"/"+inc)
	require.Equal(t, []string{key, key}, storage.objects)
	require.Zero(t, store.legacyCalls)
	// Expired credential at entry via delete clock returns 401 without touching store.
	fixedNow := time.Date(2026, 2, 1, 12, 0, 0, 0, time.UTC)
	clockDeleteH := DeleteHandler(store, storage, storage, WithScopedDeletion(), withDeleteClock(func() time.Time { return fixedNow }))
	expiredAtEntry := serveScoped(clockDeleteH, http.MethodDelete, "/documents/"+docID, &owner, fixedNow.Add(-time.Second))
	require.Equal(t, http.StatusUnauthorized, expiredAtEntry.Code)

	// Pre-commit deadline exceeded with valid credential returns 503; with expired credential returns 401.
	docTimeoutID := uuid.NewString()
	store.docs[docTimeoutID] = &scopedMemoryEntry{
		owner:   owner,
		summary: domain.DocumentSummary{ID: docTimeoutID, Incarnation: uuid.NewString(), Status: "completed"},
	}
	clockNow := fixedNow
	store.now = func() time.Time { return clockNow }
	advancingDeleteH := DeleteHandler(store, storage, storage, WithScopedDeletion(), withDeleteClock(func() time.Time { return clockNow }))

	store.beginDeleteErr = fmt.Errorf("begin tx: %w", context.DeadlineExceeded)
	preCommitValid := serveScoped(advancingDeleteH, http.MethodDelete, "/documents/"+docTimeoutID, &owner, fixedNow.Add(time.Minute))
	require.Equal(t, http.StatusServiceUnavailable, preCommitValid.Code, preCommitValid.Body.String())
	require.JSONEq(t, `{"error":"unavailable","message":"document deletion canceled"}`, preCommitValid.Body.String())
	require.False(t, store.docs[docTimeoutID].deleting)

	store.beforeBegin = func() { clockNow = fixedNow.Add(2 * time.Minute) }
	preCommitExpired := serveScoped(advancingDeleteH, http.MethodDelete, "/documents/"+docTimeoutID, &owner, fixedNow.Add(time.Minute))
	require.Equal(t, http.StatusUnauthorized, preCommitExpired.Code, preCommitExpired.Body.String())
	store.beforeBegin = nil
	store.beginDeleteErr = nil

	// Post-commit deadline exceeded (after intent commits) while credential is valid returns 500 internal, not 503.
	clockNow = fixedNow
	store.resumeDeleteErr = fmt.Errorf("delete vectors: %w", context.DeadlineExceeded)
	postCommitValid := serveScoped(advancingDeleteH, http.MethodDelete, "/documents/"+docTimeoutID, &owner, fixedNow.Add(time.Minute))
	require.Equal(t, http.StatusInternalServerError, postCommitValid.Code, postCommitValid.Body.String())
	require.JSONEq(t, `{"error":"internal","message":"failed to delete document"}`, postCommitValid.Body.String())
	require.True(t, store.docs[docTimeoutID].deleting)

	// Post-commit deadline exceeded when credential expired maps to 401.
	clockNow = fixedNow
	store.afterBegin = func() { clockNow = fixedNow.Add(2 * time.Minute) }
	postCommitExpired := serveScoped(advancingDeleteH, http.MethodDelete, "/documents/"+docTimeoutID, &owner, fixedNow.Add(time.Minute))
	require.Equal(t, http.StatusUnauthorized, postCommitExpired.Code, postCommitExpired.Body.String())
	store.afterBegin = nil
	store.resumeDeleteErr = nil
	store.now = nil
}

func withCleanupTargetsForTest(bucket, index string) DeleteOption {
	return WithCleanupTargets(cleanup.Targets{SourceBucket: bucket, VectorIndex: index})
}

func FuzzScopedFailureCategoryAndTTL(f *testing.F) {
	f.Add("no_text", int64(900))
	f.Add("ocr_result_expired", int64(45))
	f.Add("ocr: timeout on bucket s3://secret", int64(1))
	f.Add("embedding model rejected the input length", int64(0))
	f.Add("pq: password authentication failed", int64(10000))
	f.Fuzz(func(t *testing.T, raw string, millis int64) {
		cat := publicFailureCategory(raw)
		allowed := map[string]bool{
			"no_text": true, "ocr_result_expired": true, "ocr_text_too_large": true,
			"embedding_limit": true, "no_chunks": true, "ocr_failed": true, "processing_failed": true,
		}
		require.True(t, allowed[cat], "unexpected category %q", cat)
		now := time.Unix(1700000000, 0)
		exp := now.Add(time.Duration(millis) * time.Millisecond)
		ttl, ok := remainingSigningSeconds(exp, now)
		if millis < 1000 {
			require.False(t, ok)
		} else {
			require.True(t, ok)
			require.GreaterOrEqual(t, ttl, 1)
			require.LessOrEqual(t, ttl, 900)
		}
	})
}
