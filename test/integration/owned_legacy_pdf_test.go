package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/api/handler"
	"github.com/Victoroide/quarry-rag/internal/api/middleware"
	"github.com/Victoroide/quarry-rag/internal/chunker"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/extractor"
	pgstore "github.com/Victoroide/quarry-rag/internal/store/postgres"
	"github.com/Victoroide/quarry-rag/internal/testutil"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestOwnedRetryOfHistoricalParserPDF(t *testing.T) {
	store, err := pgstore.NewClient(t.Context(), startPostgres(t))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close(context.Background())) })
	require.NoError(t, store.RunMigrations(t.Context()))

	const collection = "team"
	owner := domain.Principal{ID: uuid.NewString(), TenantID: uuid.NewString()}
	importBatch := func(t *testing.T, fields map[string]any) {
		t.Helper()
		var rev int64
		require.NoError(t, store.Pool.QueryRow(t.Context(), `SELECT revision FROM grant_catalog_policy_revision`).Scan(&rev))
		fields["batch_id"], fields["expected_revision"] = uuid.NewString(), rev
		raw, marshalErr := json.Marshal(fields)
		require.NoError(t, marshalErr)
		_, importErr := store.ImportGrantBatch(t.Context(), bytes.NewReader(raw), false)
		require.NoError(t, importErr)
	}
	createPermission := func(allowed bool) []map[string]any {
		return []map[string]any{{
			"tenant_id": owner.TenantID, "principal_id": owner.ID, "collection": collection, "can_create": allowed,
		}}
	}
	importBatch(t, map[string]any{
		"tenants":     []map[string]any{{"tenant_id": owner.TenantID}},
		"collections": []map[string]any{{"tenant_id": owner.TenantID, "collection": collection}},
		"principals": []map[string]any{{
			"tenant_id": owner.TenantID, "principal_id": owner.ID, "issuer": "urn:fixture:historical-pdf", "subject": "owner", "enabled": true,
		}},
		"create_permissions": createPermission(true),
	})

	makePDF := func(label string) []byte {
		return testutil.PDFWithPageAnnotation("", "BT /F1 14 Tf 20 150 Td ("+label+") Tj ET", "")
	}
	original := makePDF("Historical operations register")
	legacy := extractor.New(extractor.FileTypePDF)
	text, err := legacy.Extract(t.Context(), bytes.NewReader(original))
	require.NoError(t, err)
	fingerprint := chunker.ScopedFingerprint(collection, strings.ReplaceAll(text, "\x00", ""))
	for _, tc := range []struct {
		name, initialStatus, finalStatus, response string
		data                                       []byte
		revoked                                    bool
		wantStatus, wantJobs                       int
	}{
		{"same_bytes", "failed", "processing", "processing", original, false, http.StatusAccepted, 1},
		{"same_text_different_bytes", "failed", "processing", "processing", append(bytes.Clone(original), '\n'), false, http.StatusAccepted, 1},
		{"different_bytes", "failed", "failed", "conflict", makePDF("Different operations register"), false, http.StatusConflict, 0},
		{"completed", "completed", "completed", "already_ingested", original, false, http.StatusOK, 0},
		{"revoked_during_extraction", "failed", "failed", "forbidden", original, true, http.StatusForbidden, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			id := uuid.NewString()
			key := id + "/historical.pdf"
			_, err := store.Pool.Exec(ctx, `INSERT INTO documents(id,incarnation,content,status,collection,text_source,file_type,s3_key,content_dedup,fingerprint)
				VALUES($1,NULL,$2,$3,$4,'parser','pdf',$5,'collection',$6)`, id, text, tc.initialStatus, collection, key, fingerprint)
			require.NoError(t, err)
			var version string
			require.NoError(t, store.Pool.QueryRow(ctx, `SELECT source_version FROM grant_catalog_source_snapshot($1)`, id).Scan(&version))
			importBatch(t, map[string]any{"assignments": []map[string]any{{
				"tenant_id": owner.TenantID, "principal_id": owner.ID, "collection": collection, "document_id": id, "source_version": version,
			}}})

			var inspected, parsed int
			ingest := handler.IngestHandler(nil, store, nil, 1<<20,
				handler.WithOwnedIngestion(), handler.WithContentDedup(domain.ContentDedupOff),
				handler.WithMixedPDFOCR(func(ctx context.Context, data []byte) (domain.PDFPages, error) {
					inspected++
					return extractor.ExtractPDFPages(ctx, bytes.NewReader(data))
				}),
				handler.WithExtractor(func(ctx context.Context, ft extractor.FileType, data []byte) (string, error) {
					parsed++
					require.Equal(t, extractor.FileTypePDF, ft)
					if tc.revoked {
						importBatch(t, map[string]any{"create_permissions": createPermission(false)})
					}
					return legacy.Extract(ctx, bytes.NewReader(data))
				}))

			req := historicalPDFUpload(t, id, collection, tc.data)
			req = req.WithContext(middleware.ContextWithSubject(req.Context(), middleware.VerifiedSubject{
				Principal: owner,
				ExpiresAt: time.Now().Add(time.Minute),
			}))
			rec := httptest.NewRecorder()
			ingest.ServeHTTP(rec, req)
			require.Equal(t, tc.wantStatus, rec.Code, rec.Body.String())
			require.Contains(t, rec.Body.String(), `"`+tc.response+`"`)
			require.Equal(t, 1, inspected)
			require.Equal(t, 1, parsed)

			var status, storedKey, storedFingerprint string
			var jobs, mixedJobs, extractions, shared int
			require.NoError(t, store.Pool.QueryRow(ctx, `SELECT status,s3_key,fingerprint,
				(SELECT count(*) FROM ingest_jobs j WHERE j.document_id=d.id),
				(SELECT count(*) FROM ingest_jobs j WHERE j.document_id=d.id AND j.pdf_required),
				(SELECT count(*) FROM document_pdf_extractions p WHERE p.document_id=d.id),
				(SELECT count(*) FROM document_fingerprints f WHERE f.document_id=d.id)
				FROM documents d WHERE id=$1`, id).Scan(&status, &storedKey, &storedFingerprint, &jobs, &mixedJobs, &extractions, &shared))
			require.Equal(t, tc.finalStatus, status)
			require.Equal(t, tc.wantJobs, jobs)
			require.Equal(t, key, storedKey)
			require.Equal(t, fingerprint, storedFingerprint)
			require.Zero(t, mixedJobs)
			require.Zero(t, extractions)
			require.Zero(t, shared)
		})
	}
}

func historicalPDFUpload(t *testing.T, id, collection string, data []byte) *http.Request {
	t.Helper()
	var body bytes.Buffer
	mw := multipart.NewWriter(&body)
	require.NoError(t, mw.WriteField("document_id", id))
	require.NoError(t, mw.WriteField("collection", collection))
	part, err := mw.CreateFormFile("file", "historical.pdf")
	require.NoError(t, err)
	_, err = part.Write(data)
	require.NoError(t, err)
	require.NoError(t, mw.Close())
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/ingest", &body)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	return req
}
