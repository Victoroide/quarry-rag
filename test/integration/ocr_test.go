package integration

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"image"
	"image/jpeg"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/api/handler"
	"github.com/Victoroide/quarry-rag/internal/chunker"
	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/embedder"
	"github.com/Victoroide/quarry-rag/internal/extractor"
	"github.com/Victoroide/quarry-rag/internal/ocr"
	"github.com/Victoroide/quarry-rag/internal/pipeline"
	"github.com/Victoroide/quarry-rag/internal/store/memory"
	"github.com/Victoroide/quarry-rag/internal/testutil"
	"github.com/Victoroide/quarry-rag/internal/worker"
	"github.com/aws/smithy-go/encoding/cbor"
	"github.com/google/uuid"
	"github.com/ledongthuc/pdf"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type storedTextDetector struct {
	text string
	err  error
}

func TestMixedPDFExplicitRetryRestartsExpiredDetection(t *testing.T) {
	store := durableDatabase(t)
	objects := &privateUploadObjects{saved: map[string]string{}}
	detector, observed := idempotentPDFDetector(t)
	vectors := memory.NewFakeVectorStore()
	stages := []pipeline.Stage{pipeline.NewChunkStage(chunker.NewChunker(64, 8)), pipeline.NewEmbedStage(embedder.NewFakeEmbedder(256), 2), pipeline.NewPersistStage(vectors)}
	w := worker.New(store, stages, vectors, worker.Options{Workers: 1, PollInterval: 10 * time.Millisecond, OCR: detector})
	w.Start(t.Context())
	defer w.Shutdown()
	upload := handler.IngestHandler(w, store, objects, 1<<20, handler.WithMixedPDFOCR(func(ctx context.Context, data []byte) (domain.PDFPages, error) {
		return extractor.ExtractPDFPages(ctx, bytes.NewReader(data))
	}))
	original := string(testutil.PDFWithPageStreams(`q 612 0 0 792 0 0 cm /Scan Do Q`))
	submit := func(id string) *httptest.ResponseRecorder {
		response := httptest.NewRecorder()
		upload(response, privatePDF(t, original, map[string]string{"document_id": id}))
		return response
	}
	initial := submit("")
	accepted := privateResponse(t, initial, http.StatusAccepted)
	durableStatus(t, store, accepted.DocumentID, "failed")
	require.Equal(t, []string{"job-1"}, observed())
	var window, source string
	require.NoError(t, store.Pool.QueryRow(t.Context(), `SELECT p.window_id::text,d.s3_key FROM document_pdf_extractions p
		JOIN documents d ON d.id=p.document_id WHERE d.id=$1`, accepted.DocumentID).Scan(&window, &source))
	sameJob, err := detector.ExtractPages(t.Context(), source, domain.MixedPDFProfile, window, 1, nil)
	require.NoError(t, err)
	require.Empty(t, sameJob.Pages[1].Text)
	require.Equal(t, []string{"job-1", "job-1"}, observed())
	// The first successful start was recent even though the original intent is old.
	_, err = store.Pool.Exec(t.Context(), `UPDATE document_pdf_extractions SET started_at=NOW()-interval '8 days' WHERE document_id=$1`, accepted.DocumentID)
	require.NoError(t, err)
	require.Equal(t, http.StatusConflict, submit("").Code)
	require.Equal(t, http.StatusConflict, submit(accepted.DocumentID).Code)
	require.Equal(t, []string{"job-1", "job-1"}, observed())
	_, err = store.Pool.Exec(t.Context(), `UPDATE document_pdf_extractions SET accepted_at=NOW()-interval '8 days' WHERE document_id=$1`, accepted.DocumentID)
	require.NoError(t, err)
	require.Equal(t, accepted.DocumentID, privateResponse(t, submit(accepted.DocumentID), http.StatusAccepted).DocumentID)
	durableStatus(t, store, accepted.DocumentID, "completed")
	require.Equal(t, []string{"job-1", "job-1", "job-2"}, observed())
	var newWindow string
	require.NoError(t, store.Pool.QueryRow(t.Context(), `SELECT window_id::text FROM document_pdf_extractions WHERE document_id=$1`, accepted.DocumentID).Scan(&newWindow))
	require.NotEqual(t, window, newWindow)
	require.Len(t, objects.saved, 1)
}

func idempotentPDFDetector(t *testing.T) (detector *ocr.Client, observed func() []string) {
	t.Helper()
	var mu sync.Mutex
	jobs := map[cbor.String]cbor.String{}
	var started []string
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if !assert.NoError(t, err) {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		value, err := cbor.Decode(body)
		if !assert.NoError(t, err) {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		fields := value.(cbor.Map)
		out := cbor.Map{}
		switch {
		case strings.HasSuffix(r.URL.Path, "/StartDocumentTextDetection"):
			token := fields["ClientRequestToken"].(cbor.String)
			mu.Lock()
			job := jobs[token]
			if job == "" {
				job = cbor.String(fmt.Sprintf("job-%d", len(jobs)+1))
				jobs[token] = job
			}
			started = append(started, string(job))
			mu.Unlock()
			out["JobId"] = job
		case strings.HasSuffix(r.URL.Path, "/GetDocumentTextDetection"):
			blocks := cbor.List{cbor.Map{"BlockType": cbor.String("PAGE"), "Page": cbor.Uint(1)}}
			if fields["JobId"] == cbor.String("job-2") {
				blocks = append(blocks, cbor.Map{"BlockType": cbor.String("LINE"), "Page": cbor.Uint(1), "Text": cbor.String("Recovered scanned archive")})
			}
			out["Blocks"] = blocks
			out["JobStatus"] = cbor.String("SUCCEEDED")
		default:
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/cbor")
		w.Header().Set("Smithy-Protocol", "rpc-v2-cbor")
		_, err = w.Write(cbor.Encode(out))
		assert.NoError(t, err)
	}))
	t.Cleanup(endpoint.Close)
	t.Setenv("AWS_ENDPOINT_URL_TEXTRACT", endpoint.URL)
	t.Setenv("AWS_IGNORE_CONFIGURED_ENDPOINT_URLS", "false")
	t.Setenv("AWS_USE_FIPS_ENDPOINT", "false")
	t.Setenv("AWS_USE_DUALSTACK_ENDPOINT", "false")
	detector, err := ocr.New(t.Context(), &config.Config{
		AWSRegion: "us-east-1", AWSAccessKeyID: "fixture-key", AWSSecretKey: "fixture-secret",
		S3Bucket: "fixture-bucket", OCRTimeout: 5 * time.Second,
	})
	require.NoError(t, err)
	return detector, func() []string {
		mu.Lock()
		defer mu.Unlock()
		return slices.Clone(started)
	}
}

func TestMixedPDFExplicitRetriesAcrossModes(t *testing.T) {
	const heading = `BT /F1 12 Tf (Archive report) Tj ET`
	legacyInline := testutil.PDFWithPageStreams(heading, `q BI /W 1 /H 1 /BPC 8 /CS /G ID x EI Q`)
	legacyBlank := testutil.PDFWithPageStreams("")
	digital := testutil.PDFWithPageStreams(heading)
	store := durableDatabase(t)
	objects := &privateUploadObjects{saved: map[string]string{}}
	legacy := handler.IngestHandler(nil, store, objects, 1<<20, handler.WithOCR(true))
	extract := handler.WithMixedPDFOCR(func(ctx context.Context, data []byte) (domain.PDFPages, error) {
		return extractor.ExtractPDFPages(ctx, bytes.NewReader(data))
	})
	mixed := handler.IngestHandler(nil, store, objects, 1<<20, extract)
	private := handler.WithContentDedup(domain.ContentDedupOff)
	privateMixed := handler.IngestHandler(nil, store, objects, 1<<20, private, extract)
	privateLegacy := handler.IngestHandler(nil, store, objects, 1<<20, private, handler.WithOCR(true))
	for _, tc := range []struct {
		name          string
		data          []byte
		first, second http.Handler
	}{
		{"historical parser with inline image", legacyInline, legacy, mixed},
		{"historical OCR with blank input", legacyBlank, legacy, mixed},
		{"mixed original identity in legacy mode", digital, mixed, legacy},
		{"private mixed original identity in legacy mode", digital, privateMixed, privateLegacy},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fields := map[string]string{"document_id": uuid.NewString(), "collection": "archive"}
			for _, route := range []http.Handler{tc.first, tc.second} {
				response := httptest.NewRecorder()
				route.ServeHTTP(response, privatePDF(t, string(tc.data), fields))
				require.Equal(t, http.StatusAccepted, response.Code, response.Body.String())
				require.Contains(t, response.Body.String(), fields["document_id"])
			}
			fields["collection"] = "other"
			response := httptest.NewRecorder()
			tc.second.ServeHTTP(response, privatePDF(t, string(tc.data), fields))
			require.Equal(t, http.StatusConflict, response.Code, response.Body.String())
		})
	}
	response := httptest.NewRecorder()
	mixed.ServeHTTP(response, privatePDF(t, string(legacyInline), map[string]string{"document_id": uuid.NewString(), "collection": "archive"}))
	require.Equal(t, http.StatusUnprocessableEntity, response.Code, response.Body.String())
	require.Len(t, objects.saved, 4)
}

func TestMixedPDFAnonymousRetryUsesLegacyTextIdentity(t *testing.T) {
	const stream = `BT /F1 12 Tf 14 TL (Archive heading) Tj (Second line) ' ET`
	const collection = "archive"
	original := string(testutil.PDFWithPageStreams(stream))
	store := durableDatabase(t)
	objects := &privateUploadObjects{saved: map[string]string{}}
	legacy := handler.IngestHandler(nil, store, objects, 1<<20, handler.WithOCR(true))
	mixed := handler.IngestHandler(nil, store, objects, 1<<20, handler.WithMixedPDFOCR(func(ctx context.Context, data []byte) (domain.PDFPages, error) {
		return extractor.ExtractPDFPages(ctx, bytes.NewReader(data))
	}), handler.WithMaxPending(1))
	fields := map[string]string{"collection": collection}
	first := httptest.NewRecorder()
	legacy(first, privatePDF(t, original, fields))
	accepted := privateResponse(t, first, http.StatusAccepted)
	job, err := store.ClaimJob(t.Context(), "legacy", time.Minute, 5)
	require.NoError(t, err)
	require.Equal(t, "Archive headingSecond line", job.Document.Content)
	done, err := store.CompleteJob(t.Context(), *job, nil, domain.IndexObservation{})
	require.NoError(t, err)
	require.True(t, done)
	for i := range 2 {
		response := httptest.NewRecorder()
		mixed(response, privatePDF(t, original, fields))
		duplicate := privateResponse(t, response, http.StatusOK)
		require.Equal(t, accepted.DocumentID, duplicate.DocumentID)
		if i == 0 {
			queued := httptest.NewRecorder()
			legacy(queued, privateJSON(t, handler.IngestRequest{Content: "A different queued item"}))
			require.Equal(t, http.StatusAccepted, queued.Code)
		}
	}
	require.Len(t, objects.saved, 1)
}

func TestMixedPDFAnonymousRetryUsesLegacyOCROriginal(t *testing.T) {
	var scan bytes.Buffer
	require.NoError(t, jpeg.Encode(&scan, image.NewGray(image.Rect(0, 0, 12, 12)), nil))
	raster := fmt.Appendf(nil, "<< /Type /XObject /Subtype /Image /Width 12 /Height 12 /BitsPerComponent 8 /ColorSpace /DeviceGray /Filter /DCTDecode /Length %d >>\nstream\n", scan.Len())
	raster = append(raster, scan.Bytes()...)
	raster = append(raster, "\nendstream"...)
	jpegScan := testutil.PDFWithPageResources([]string{"q 180 0 0 180 10 10 cm /Photo Do Q"}, "/Photo 5 0 R", [][]byte{raster})
	newScan := testutil.PDFWithPageResources([]string{"q 160 0 0 160 20 20 cm /Photo Do Q"}, "/Photo 5 0 R", [][]byte{raster})
	store := durableDatabase(t)
	objects := &privateUploadObjects{saved: map[string]string{}}
	legacy := handler.IngestHandler(nil, store, objects, 1<<20, handler.WithOCR(true))
	pages := handler.WithMixedPDFOCR(func(ctx context.Context, data []byte) (domain.PDFPages, error) {
		return extractor.ExtractPDFPages(ctx, bytes.NewReader(data))
	})
	mixed := handler.IngestHandler(nil, store, objects, 1<<20, pages)
	privateMixed := handler.IngestHandler(nil, store, objects, 1<<20, pages, handler.WithContentDedup(domain.ContentDedupOff))
	send := func(t *testing.T, upload http.Handler, data []byte, collection string) *httptest.ResponseRecorder {
		t.Helper()
		response := httptest.NewRecorder()
		upload.ServeHTTP(response, privatePDF(t, string(data), map[string]string{"collection": collection}))
		return response
	}
	for _, tc := range []struct {
		name string
		data []byte
	}{
		{"JPEG scan", jpegScan},
		{"blank PDF", testutil.PDFWithPageStreams("")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			accepted := privateResponse(t, send(t, legacy, tc.data, "archive"), http.StatusAccepted)
			job, err := store.ClaimJob(ctx, "legacy", time.Minute, 5)
			require.NoError(t, err)
			require.NotNil(t, job)
			require.Equal(t, accepted.DocumentID, string(job.Document.ID))
			require.Equal(t, chunker.ScopedFingerprint("archive", string(tc.data)), job.Document.Fingerprint)
			require.NoError(t, store.StoreOCRText(ctx, *job, "Recovered archive text"))
			completed, err := store.CompleteJob(ctx, *job, nil, domain.IndexObservation{})
			require.NoError(t, err)
			require.True(t, completed)
			for _, upload := range []http.Handler{legacy, mixed} {
				duplicate := privateResponse(t, send(t, upload, tc.data, "archive"), http.StatusOK)
				require.Equal(t, accepted.DocumentID, duplicate.DocumentID)
			}
			for _, response := range []*httptest.ResponseRecorder{
				send(t, mixed, tc.data, "another-collection"),
				send(t, privateMixed, tc.data, "archive"),
			} {
				if tc.name == "blank PDF" {
					require.Equal(t, http.StatusUnprocessableEntity, response.Code, response.Body.String())
					continue
				}
				acceptedOther := privateResponse(t, response, http.StatusAccepted)
				summary, err := store.GetDocumentSummary(ctx, domain.DocumentID(acceptedOther.DocumentID))
				require.NoError(t, err)
				require.NotNil(t, summary)
				require.Equal(t, domain.PDFPending, summary.PDFCoverage)
				// Drain mixed-PDF work so subsequent legacy ClaimJob calls are not blocked by pdf_required jobs.
				mixedJob, err := store.ClaimPDFJob(ctx, "mixed", time.Minute, 5)
				require.NoError(t, err)
				require.NotNil(t, mixedJob)
				require.Equal(t, acceptedOther.DocumentID, string(mixedJob.Document.ID))
				_, err = store.BeginPDFOCR(ctx, *mixedJob)
				require.NoError(t, err)
				require.NoError(t, store.StorePDFOCR(ctx, *mixedJob, domain.PDFResult{
					Text:     "Recovered archive text",
					Coverage: domain.PDFComplete,
					Pages:    []domain.PDFPageResult{{Number: 1, Source: "ocr", State: "text"}},
				}))
				done, err := store.CompleteJob(ctx, *mixedJob, nil, domain.IndexObservation{})
				require.NoError(t, err)
				require.True(t, done)
			}
		})
	}
	response := send(t, mixed, newScan, "archive")
	acceptedNew := privateResponse(t, response, http.StatusAccepted)
	summary, err := store.GetDocumentSummary(t.Context(), domain.DocumentID(acceptedNew.DocumentID))
	require.NoError(t, err)
	require.NotNil(t, summary)
	require.Equal(t, domain.PDFPending, summary.PDFCoverage)
	require.Len(t, objects.saved, 5)
	var docs int
	require.NoError(t, store.Pool.QueryRow(t.Context(), `SELECT count(*) FROM documents`).Scan(&docs))
	require.Equal(t, 5, docs)
}

func TestMixedPDFInconclusiveLegacyIdentityKeepsOriginalDocument(t *testing.T) {
	const stream = `BT /F1 12 Tf 14 TL (Archive heading) Tj (Second line) ' ET`
	const collection = "archive"
	original := string(testutil.PDFWithPageStreams(stream))
	store := durableDatabase(t)
	objects := &privateUploadObjects{saved: map[string]string{}}
	legacy := handler.IngestHandler(nil, store, objects, 1<<20, handler.WithOCR(true))
	var compatibility error
	var calls int
	mixed := handler.IngestHandler(nil, store, objects, 1<<20, handler.WithMixedPDFOCR(func(ctx context.Context, data []byte) (domain.PDFPages, error) {
		return extractor.ExtractPDFPages(ctx, bytes.NewReader(data))
	}), handler.WithExtractor(func(ctx context.Context, ft extractor.FileType, data []byte) (string, error) {
		calls++
		if compatibility != nil {
			return "", compatibility
		}
		return extractor.New(ft).Extract(ctx, bytes.NewReader(data))
	}))
	fields := map[string]string{"collection": collection}
	first := httptest.NewRecorder()
	legacy(first, privatePDF(t, original, fields))
	accepted := privateResponse(t, first, http.StatusAccepted)
	job, err := store.ClaimJob(t.Context(), "legacy", time.Minute, 5)
	require.NoError(t, err)
	require.Equal(t, "Archive headingSecond line", job.Document.Content)
	done, err := store.CompleteJob(t.Context(), *job, nil, domain.IndexObservation{})
	require.NoError(t, err)
	require.True(t, done)
	documents := func() (count int) {
		require.NoError(t, store.Pool.QueryRow(t.Context(), `SELECT count(*) FROM documents WHERE collection=$1`, collection).Scan(&count))
		return count
	}
	for _, failure := range []struct {
		name   string
		err    error
		status int
	}{
		{"unavailable", extractor.ErrExtractionUnavailable, http.StatusServiceUnavailable},
		{"timeout", extractor.ErrExtractionTimeout, http.StatusUnprocessableEntity},
		{"canceled", context.Canceled, http.StatusUnprocessableEntity},
	} {
		t.Run(failure.name, func(t *testing.T) {
			compatibility, calls = failure.err, 0
			response := httptest.NewRecorder()
			mixed(response, privatePDF(t, original, fields))
			require.Equal(t, failure.status, response.Code, response.Body.String())
			require.Equal(t, 1, calls)
			require.Len(t, objects.saved, 1)
			require.Equal(t, 1, documents())
		})
	}
	compatibility = nil
	healthy := httptest.NewRecorder()
	mixed(healthy, privatePDF(t, original, fields))
	duplicate := privateResponse(t, healthy, http.StatusOK)
	require.Equal(t, accepted.DocumentID, duplicate.DocumentID)
	require.Len(t, objects.saved, 1)
	require.Equal(t, 1, documents())
}

func TestMixedPDFUploadThroughTextractAndPostgres(t *testing.T) {
	const header = `BT /F1 12 Tf (Archive report) Tj ET`
	const bilingual = ` BT /F1 12 Tf (English and caf\351 fran\347ais) Tj ET`
	scanned, err := os.ReadFile("testdata/mixed-scans.pdf")
	require.NoError(t, err)
	original := string(scanned)
	clean := string(testutil.PDFWithPageStreams(header+bilingual, "", header))
	overlay := string(testutil.PDFWithPageStreams(`q 612 0 0 792 0 0 cm /Scan Do Q ` + header))
	layout := string(testutil.PDFWithPageResources([]string{
		header + ` 36 600 m 576 600 l S`, `/Heading Do`,
		`BT /F1 12 Tf 14 TL (Archive report) Tj 0 0 (Second line) " ET`,
		`BT /F1 12 Tf ET /Inherited Do`,
	}, `/Heading 5 0 R /Inherited 6 0 R`, [][]byte{
		[]byte(fmt.Sprintf("<< /Type /XObject /Subtype /Form /BBox [0 0 612 792] /Resources << /Font << /F1 3 0 R >> >> /Length %d >>\nstream\n%s\nendstream", len(header), header)),
		[]byte(fmt.Sprintf("<< /Type /XObject /Subtype /Form /BBox [0 0 612 792] /Matrix [1 0 0 1 4 6] /Length %d >>\nstream\n%s\nendstream", len(`BT 36 700 Td (Inherited archive heading) Tj ET`), `BT 36 700 Td (Inherited archive heading) Tj ET`)),
	}))
	const selected = `q /GS1 gs BT 1 0 0 1 30 60 Tm (Archived digital heading) Tj ET Q`
	graphicsState := string(testutil.PDFWithPageResources([]string{"/Heading Do"}, "/Heading 5 0 R", [][]byte{
		[]byte(fmt.Sprintf("<< /Type /XObject /Subtype /Form /BBox [0 0 500 500] /Resources << /ExtGState << /GS1 << /Type /ExtGState /Font [3 0 R 12] >> >> >> /Length %d >>\nstream\n%s\nendstream", len(selected), selected)),
	}))
	const wantMixed = "First scanned page\n\nArchive report\nEnglish and café français\n\nMiddle scanned page\n\nArchive report\n\nLast scanned page"
	const wantClean = "Archive report\nEnglish and café français\n\nArchive report"
	store := durableDatabase(t)
	objects := &privateUploadObjects{saved: map[string]string{}}
	var starts, reads atomic.Int32
	endpoint := mixedTextractEndpoint(t, objects, original, &starts, &reads)
	t.Cleanup(endpoint.Close)
	t.Setenv("AWS_ENDPOINT_URL_TEXTRACT", endpoint.URL)
	t.Setenv("AWS_IGNORE_CONFIGURED_ENDPOINT_URLS", "false")
	t.Setenv("AWS_USE_FIPS_ENDPOINT", "false")
	t.Setenv("AWS_USE_DUALSTACK_ENDPOINT", "false")
	detector, err := ocr.New(t.Context(), &config.Config{
		AWSRegion: "us-east-1", AWSAccessKeyID: "fixture-key", AWSSecretKey: "fixture-secret",
		S3Bucket: "fixture-bucket", OCRTimeout: 5 * time.Second,
	})
	require.NoError(t, err)
	vectors := memory.NewFakeVectorStore()
	stages := []pipeline.Stage{pipeline.NewChunkStage(chunker.NewChunker(64, 8)), pipeline.NewEmbedStage(embedder.NewFakeEmbedder(256), 2), pipeline.NewPersistStage(vectors)}
	w := worker.New(store, stages, vectors, worker.Options{Workers: 2, PollInterval: 10 * time.Millisecond, OCR: detector})
	w.Start(t.Context())
	defer w.Shutdown()
	extract := handler.WithMixedPDFOCR(func(ctx context.Context, data []byte) (domain.PDFPages, error) {
		return extractor.ExtractPDFPages(ctx, bytes.NewReader(data))
	})
	upload := handler.IngestHandler(w, store, objects, 1<<20, extract)
	for _, tc := range []struct {
		name, original, text string
		starts, reads        int32
		coverage             domain.PDFCoverage
	}{
		{"digital", clean, wantClean, 0, 0, domain.PDFComplete},
		{"digital layout", layout, "Archive report\n\nArchive report\n\nArchive report\nSecond line\n\nInherited archive heading", 0, 0, domain.PDFComplete},
		{"digital graphics state font", graphicsState, "Archived digital heading", 0, 0, domain.PDFComplete},
		{"readable overlay", overlay, "Archive report", 0, 0, domain.PDFPartial},
		{"mixed", original, wantMixed, 1, 2, domain.PDFComplete},
	} {
		t.Run(tc.name, func(t *testing.T) {
			response := httptest.NewRecorder()
			upload(response, privatePDF(t, tc.original, map[string]string{"collection": "archive"}))
			require.Equal(t, http.StatusAccepted, response.Code, response.Body.String())
			var accepted handler.IngestResponse
			require.NoError(t, json.Unmarshal(response.Body.Bytes(), &accepted))
			durableStatus(t, store, accepted.DocumentID, "completed")
			var stored string
			require.NoError(t, store.Pool.QueryRow(t.Context(), `SELECT content FROM documents WHERE id=$1`, accepted.DocumentID).Scan(&stored))
			require.Equal(t, tc.text, stored)
			summary, err := store.GetDocumentSummary(t.Context(), domain.DocumentID(accepted.DocumentID))
			require.NoError(t, err)
			require.Equal(t, tc.coverage, summary.PDFCoverage)
			require.Positive(t, summary.ChunkCount)
			require.Equal(t, tc.starts, starts.Load())
			require.Equal(t, tc.reads, reads.Load())
			duplicate := httptest.NewRecorder()
			upload(duplicate, privatePDF(t, tc.original, map[string]string{"collection": "archive"}))
			require.Equal(t, http.StatusOK, duplicate.Code, duplicate.Body.String())
			require.NoError(t, json.Unmarshal(duplicate.Body.Bytes(), &accepted))
			require.Equal(t, summary.ID, accepted.DocumentID)
			require.Equal(t, tc.starts, starts.Load())
		})
	}
	t.Run("damaged raster", func(t *testing.T) {
		broken := testutil.PDFWithPageResources([]string{`/Broken Do`}, `/Broken 5 0 R`, [][]byte{
			[]byte("<< /Type /XObject /Subtype /Image /Width 2 /Height 2 /ColorSpace /DeviceGray /BitsPerComponent 8 /Length 1 >>\nstream\n\x00\nendstream"),
		})
		response := httptest.NewRecorder()
		before := len(objects.saved)
		upload(response, privatePDF(t, string(broken), nil))
		require.Equal(t, http.StatusUnprocessableEntity, response.Code, response.Body.String())
		require.Len(t, objects.saved, before)
		require.EqualValues(t, 1, starts.Load())
	})
	t.Run("object failure", func(t *testing.T) {
		id := uuid.NewString()
		failedUpload := handler.IngestHandler(w, store, &privateUploadObjects{fail: true}, 1<<20, extract)
		response := httptest.NewRecorder()
		failedUpload(response, privatePDF(t, original, map[string]string{"document_id": id}))
		require.Equal(t, http.StatusServiceUnavailable, response.Code, response.Body.String())
		summary, err := store.GetDocumentSummary(t.Context(), domain.DocumentID(id))
		require.NoError(t, err)
		require.Nil(t, summary)
		require.EqualValues(t, 1, starts.Load())
	})
}

func mixedTextractEndpoint(t *testing.T, objects *privateUploadObjects, original string, starts, reads *atomic.Int32) *httptest.Server {
	t.Helper()
	page := func(n cbor.Uint) cbor.Map { return cbor.Map{"BlockType": cbor.String("PAGE"), "Page": n} }
	line := func(n cbor.Uint, text cbor.String) cbor.Map {
		return cbor.Map{"BlockType": cbor.String("LINE"), "Page": n, "Text": text}
	}
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		payload, err := io.ReadAll(r.Body)
		if !assert.NoError(t, err) {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		decoded, err := cbor.Decode(payload)
		if !assert.NoError(t, err) {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		fields := decoded.(cbor.Map)
		out := cbor.Map{}
		switch {
		case strings.HasSuffix(r.URL.Path, "/StartDocumentTextDetection"):
			starts.Add(1)
			obj := fields["DocumentLocation"].(cbor.Map)["S3Object"].(cbor.Map)
			assert.Equal(t, cbor.String("fixture-bucket"), obj["Bucket"])
			key := string(obj["Name"].(cbor.String))
			objects.mu.Lock()
			stored := objects.saved[key]
			objects.mu.Unlock()
			assert.Equal(t, original, stored)
			assertScannedPagePlacement(t, stored)
			assert.Len(t, fields["ClientRequestToken"], 64)
			out["JobId"] = cbor.String("fixture-job")
		case strings.HasSuffix(r.URL.Path, "/GetDocumentTextDetection"):
			reads.Add(1)
			assert.Equal(t, cbor.String("fixture-job"), fields["JobId"])
			out["JobStatus"] = cbor.String("SUCCEEDED")
			out["DocumentMetadata"] = cbor.Map{"Pages": cbor.Uint(6)}
			if fields["NextToken"] == nil {
				out["Blocks"] = cbor.List{page(1), page(2), line(1, "First scanned page"), line(2, "Duplicate bilingual text"), line(6, "Last scanned page")}
				out["NextToken"] = cbor.String("second-batch")
			} else {
				assert.Equal(t, cbor.String("second-batch"), fields["NextToken"])
				out["Blocks"] = cbor.List{page(3), page(4), page(5), page(6), line(3, "Middle scanned page"), line(4, "Ignored on blank page"), line(5, "Duplicate header")}
			}
		default:
			t.Errorf("unexpected detection path %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/cbor")
		w.Header().Set("Smithy-Protocol", "rpc-v2-cbor")
		_, err = w.Write(cbor.Encode(out))
		assert.NoError(t, err)
	}))
}

func assertScannedPagePlacement(t *testing.T, original string) {
	t.Helper()
	reader, err := pdf.NewReader(strings.NewReader(original), int64(len(original)))
	if !assert.NoError(t, err) {
		return
	}
	assert.Equal(t, 6, reader.NumPage())
	for _, expected := range []struct {
		page          int
		label, digest string
	}{
		{1, "First scanned page", "1fa676fb0c528a1dcfb640fbd094bcfe95e83b0843255e7011e901d1b1d73ab8"},
		{3, "Middle scanned page", "473e243aea2b8c2b97b938e2272a7cca868432605627ba55d5f89b2e6a9c7f96"},
		{6, "Last scanned page", "f8652c9cbfe1fe99ed1e2628f789b1a6d00849fa1298a615abfd196657cf6e85"},
	} {
		page := reader.Page(expected.page)
		contents := page.V.Key("Contents").Reader()
		commands, readErr := io.ReadAll(contents)
		assert.NoError(t, contents.Close())
		if !assert.NoError(t, readErr) {
			continue
		}
		assert.Contains(t, string(commands), "/Scan Do")
		scan := page.Resources().Key("XObject").Key("Scan")
		if !assert.Equal(t, "Image", scan.Key("Subtype").Name(), expected.label) {
			continue
		}
		pixels := scan.Reader()
		data, readErr := io.ReadAll(pixels)
		assert.NoError(t, pixels.Close())
		assert.NoError(t, readErr)
		assert.Len(t, data, 510*660)
		assert.Equal(t, expected.digest, fmt.Sprintf("%x", sha256.Sum256(data)), expected.label)
	}
}

func (d storedTextDetector) Extract(context.Context, string) (string, error) { return d.text, d.err }

func TestOCRDurableProcessingAndFailure(t *testing.T) {
	for _, tc := range []struct {
		name, text, state string
		err               error
	}{
		{"detected", "The scanned archive retains records for seven years.", "completed", nil},
		{"empty", "", "failed", domain.Permanent(ocr.ErrNoText)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := durableDatabase(t)
			vectors := memory.NewFakeVectorStore()
			key := "synthetic-original"
			const original = "synthetic PDF bytes"
			const collection = "archive"
			fp := chunker.ScopedFingerprint(collection, original)
			req := domain.AcceptRequest{Document: domain.Document{ID: domain.DocumentID(uuid.NewString()), S3Key: &key, TextSource: "ocr_pending"}, Collection: collection, Fingerprint: fp, MaxPending: 100}
			accepted, err := c.AcceptDocument(t.Context(), req)
			require.NoError(t, err)
			require.Equal(t, domain.AcceptAccepted, accepted.Outcome)
			w := worker.New(c, []pipeline.Stage{pipeline.NewChunkStage(chunker.NewChunker(32, 4)), pipeline.NewEmbedStage(embedder.NewFakeEmbedder(256), 2), pipeline.NewPersistStage(vectors)}, vectors, worker.Options{Workers: 1, Lease: time.Second, JobTimeout: time.Minute, PollInterval: 10 * time.Millisecond, OCR: storedTextDetector{text: tc.text, err: tc.err}})
			w.Start(t.Context())
			defer w.Shutdown()
			durableStatus(t, c, string(accepted.DocumentID), tc.state)
			var content, preview, source, fingerprint string
			var lastError *string
			var count int
			require.NoError(t, c.Pool.QueryRow(t.Context(), `SELECT content,content_preview,text_source,fingerprint,last_error,chunk_count FROM documents WHERE id=$1`, string(accepted.DocumentID)).Scan(&content, &preview, &source, &fingerprint, &lastError, &count))
			require.Equal(t, fp, fingerprint)
			if tc.err != nil {
				require.Equal(t, "no_text", *lastError)
				require.Empty(t, content)
				require.Zero(t, count)
				return
			}
			require.Equal(t, tc.text, content)
			require.Equal(t, tc.text, preview)
			require.Equal(t, "ocr", source)
			require.Positive(t, count)
			var chunks int
			require.NoError(t, c.Pool.QueryRow(t.Context(), `SELECT count(*) FROM chunks WHERE document_id=$1`, string(accepted.DocumentID)).Scan(&chunks))
			require.Equal(t, count, chunks)
			matches, err := vectors.QueryFiltered(t.Context(), nil, 10, domain.VectorFilter{Collection: collection})
			require.NoError(t, err)
			require.Len(t, matches, count)
			for _, match := range matches {
				require.Equal(t, accepted.DocumentID, match.Chunk.DocumentID)
			}
			req.Document.ID = domain.DocumentID(uuid.NewString())
			duplicate, err := c.AcceptDocument(t.Context(), req)
			require.NoError(t, err)
			require.Equal(t, domain.AcceptDuplicate, duplicate.Outcome)
			require.Equal(t, accepted.DocumentID, duplicate.DocumentID)
		})
	}
}
