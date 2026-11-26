package integration

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"image"
	"image/jpeg"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func integrationJPEGBytes(t *testing.T, w, h int) []byte {
	t.Helper()
	var buf bytes.Buffer
	require.NoError(t, jpeg.Encode(&buf, image.NewGray(image.Rect(0, 0, w, h)), nil))
	return buf.Bytes()
}

func integrationJPEGObject(w, h int, data []byte) []byte {
	obj := fmt.Appendf(nil, "<< /Type /XObject /Subtype /Image /Width %d /Height %d /BitsPerComponent 8 /ColorSpace /DeviceGray /Filter /DCTDecode /Length %d >>\nstream\n", w, h, len(data))
	obj = append(obj, data...)
	obj = append(obj, "\nendstream"...)
	return obj
}

func TestMixedPDFJPEGScanThroughTextractAndPostgres(t *testing.T) {
	store := durableDatabase(t)
	objects := &privateUploadObjects{saved: map[string]string{}}

	var starts atomic.Int32
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if !assert.NoError(t, err) {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		decoded, err := cbor.Decode(body)
		if !assert.NoError(t, err) {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		out := cbor.Map{}
		switch {
		case strings.HasSuffix(r.URL.Path, "/StartDocumentTextDetection"):
			starts.Add(1)
			out["JobId"] = cbor.String("jpeg-job")
		case strings.HasSuffix(r.URL.Path, "/GetDocumentTextDetection"):
			out["JobStatus"] = cbor.String("SUCCEEDED")
			out["DocumentMetadata"] = cbor.Map{"Pages": cbor.Uint(2)}
			out["Blocks"] = cbor.List{
				cbor.Map{"BlockType": cbor.String("PAGE"), "Page": cbor.Uint(1)},
				cbor.Map{"BlockType": cbor.String("PAGE"), "Page": cbor.Uint(2)},
				cbor.Map{"BlockType": cbor.String("LINE"), "Page": cbor.Uint(1), "Text": cbor.String("Ignored page-1 detector line")},
				cbor.Map{"BlockType": cbor.String("LINE"), "Page": cbor.Uint(2), "Text": cbor.String("Recovered JPEG scan text")},
			}
		default:
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_ = decoded
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

	vectors := memory.NewFakeVectorStore()
	stages := []pipeline.Stage{
		pipeline.NewChunkStage(chunker.NewChunker(64, 8)),
		pipeline.NewEmbedStage(embedder.NewFakeEmbedder(256), 2),
		pipeline.NewPersistStage(vectors),
	}
	w := worker.New(store, stages, vectors, worker.Options{Workers: 2, PollInterval: 10 * time.Millisecond, OCR: detector})
	w.Start(t.Context())
	defer w.Shutdown()

	extract := handler.WithMixedPDFOCR(func(ctx context.Context, data []byte) (domain.PDFPages, error) {
		return extractor.ExtractPDFPages(ctx, bytes.NewReader(data))
	})
	upload := handler.IngestHandler(w, store, objects, 1<<20, extract)

	t.Run("digital control", func(t *testing.T) {
		digital := testutil.PDFWithPageStreams(`BT /F1 12 Tf (Digital control archive page) Tj ET`)
		rec := httptest.NewRecorder()
		upload(rec, privatePDF(t, string(digital), map[string]string{"collection": "archive"}))
		accepted := privateResponse(t, rec, http.StatusAccepted)
		durableStatus(t, store, accepted.DocumentID, "completed")
		require.Zero(t, starts.Load())
		summary, err := store.GetDocumentSummary(t.Context(), domain.DocumentID(accepted.DocumentID))
		require.NoError(t, err)
		require.Equal(t, domain.PDFComplete, summary.PDFCoverage)
	})

	jpegBytes := integrationJPEGBytes(t, 12, 12)
	scanPDF := testutil.PDFWithPageResources([]string{
		`BT /F1 12 Tf (Archive digital page) Tj ET`,
		`q 180 0 0 180 10 10 cm /Photo Do Q`,
	}, "/Photo 5 0 R", [][]byte{integrationJPEGObject(12, 12, jpegBytes)})

	var scanDocID string
	t.Run("digital page plus JPEG scan", func(t *testing.T) {
		rec := httptest.NewRecorder()
		upload(rec, privatePDF(t, string(scanPDF), map[string]string{"collection": "archive"}))
		accepted := privateResponse(t, rec, http.StatusAccepted)
		scanDocID = accepted.DocumentID
		durableStatus(t, store, scanDocID, "completed")

		var stored string
		require.NoError(t, store.Pool.QueryRow(t.Context(), `SELECT content FROM documents WHERE id=$1`, scanDocID).Scan(&stored))
		require.Equal(t, "Archive digital page\n\nRecovered JPEG scan text", stored)

		summary, err := store.GetDocumentSummary(t.Context(), domain.DocumentID(scanDocID))
		require.NoError(t, err)
		require.Equal(t, domain.PDFComplete, summary.PDFCoverage)
		require.EqualValues(t, 1, starts.Load())
	})

	t.Run("duplicate scan skips detection", func(t *testing.T) {
		rec := httptest.NewRecorder()
		upload(rec, privatePDF(t, string(scanPDF), map[string]string{"collection": "archive"}))
		duplicate := privateResponse(t, rec, http.StatusOK)
		require.Equal(t, scanDocID, duplicate.DocumentID)
		require.EqualValues(t, 1, starts.Load())
	})

	t.Run("rejects invalid JPEG inputs before storage", func(t *testing.T) {
		truncated := integrationJPEGObject(12, 12, jpegBytes[:len(jpegBytes)-2])
		mismatched := integrationJPEGObject(13, 12, jpegBytes)
		oversized := integrationJPEGObject(10001, 1, jpegBytes)

		for _, tc := range []struct {
			name string
			pdf  []byte
		}{
			{"truncated JPEG", testutil.PDFWithPageResources([]string{`q 180 0 0 180 10 10 cm /Photo Do Q`}, "/Photo 5 0 R", [][]byte{truncated})},
			{"frame mismatch", testutil.PDFWithPageResources([]string{`q 180 0 0 180 10 10 cm /Photo Do Q`}, "/Photo 5 0 R", [][]byte{mismatched})},
			{"Width 10001", testutil.PDFWithPageResources([]string{`q 180 0 0 180 10 10 cm /Photo Do Q`}, "/Photo 5 0 R", [][]byte{oversized})},
		} {
			t.Run(tc.name, func(t *testing.T) {
				beforeObjects := len(objects.saved)
				var beforeDocs int
				require.NoError(t, store.Pool.QueryRow(t.Context(), `SELECT count(*) FROM documents`).Scan(&beforeDocs))

				rec := httptest.NewRecorder()
				upload(rec, privatePDF(t, string(tc.pdf), map[string]string{"collection": "archive"}))
				require.Equal(t, http.StatusUnprocessableEntity, rec.Code, rec.Body.String())
				require.Contains(t, rec.Body.String(), `"code":"unparseable"`)
				require.Len(t, objects.saved, beforeObjects)
				var afterDocs int
				require.NoError(t, store.Pool.QueryRow(t.Context(), `SELECT count(*) FROM documents`).Scan(&afterDocs))
				require.Equal(t, beforeDocs, afterDocs)
				require.EqualValues(t, 1, starts.Load())
			})
		}
	})
}

func TestMixedPDFJPEGDuplicateSkipsInspection(t *testing.T) {
	store := durableDatabase(t)
	objects := &privateUploadObjects{saved: map[string]string{}}
	jpegBytes := integrationJPEGBytes(t, 12, 12)
	scanPDF := testutil.PDFWithPageResources([]string{
		`q 180 0 0 180 10 10 cm /Photo Do Q`,
	}, "/Photo 5 0 R", [][]byte{integrationJPEGObject(12, 12, jpegBytes)})

	var failingCalls atomic.Int32
	healthy := handler.IngestHandler(nil, store, objects, 1<<20, handler.WithMixedPDFOCR(func(ctx context.Context, data []byte) (domain.PDFPages, error) {
		return extractor.ExtractPDFPages(ctx, bytes.NewReader(data))
	}))
	failingReplica := handler.IngestHandler(nil, store, objects, 1<<20, handler.WithMixedPDFOCR(func(context.Context, []byte) (domain.PDFPages, error) {
		failingCalls.Add(1)
		return nil, errors.New("inspector unavailable on mixed replica")
	}))

	first := httptest.NewRecorder()
	healthy(first, privatePDF(t, string(scanPDF), map[string]string{"collection": "archive"}))
	accepted := privateResponse(t, first, http.StatusAccepted)

	t.Run("while processing returns 202 without inspection", func(t *testing.T) {
		inProgress := httptest.NewRecorder()
		failingReplica(inProgress, privatePDF(t, string(scanPDF), map[string]string{"collection": "archive"}))
		inProgressRsp := privateResponse(t, inProgress, http.StatusAccepted)
		require.Equal(t, accepted.DocumentID, inProgressRsp.DocumentID)
		require.Equal(t, "processing", inProgressRsp.Status)
		require.Zero(t, failingCalls.Load())
	})

	t.Run("after completion returns 200 without inspection", func(t *testing.T) {
		ctx := t.Context()
		job, err := store.ClaimPDFJob(ctx, "mixed", time.Minute, 5)
		require.NoError(t, err)
		require.NotNil(t, job)
		_, err = store.BeginPDFOCR(ctx, *job)
		require.NoError(t, err)
		require.NoError(t, store.StorePDFOCR(ctx, *job, domain.PDFResult{
			Text:     "Recovered JPEG scan text",
			Coverage: domain.PDFComplete,
			Pages:    []domain.PDFPageResult{{Number: 1, Source: "ocr", State: "text"}},
		}))
		done, err := store.CompleteJob(ctx, *job, nil, domain.IndexObservation{})
		require.NoError(t, err)
		require.True(t, done)

		second := httptest.NewRecorder()
		failingReplica(second, privatePDF(t, string(scanPDF), map[string]string{"collection": "archive"}))
		duplicate := privateResponse(t, second, http.StatusOK)
		require.Equal(t, accepted.DocumentID, duplicate.DocumentID)
		require.Equal(t, "already_ingested", duplicate.Status)
		require.Zero(t, failingCalls.Load())
	})
}
