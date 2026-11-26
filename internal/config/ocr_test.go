package config

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPDFOCRMode(t *testing.T) {
	for _, tc := range []struct{ mode, provider, fake, isolate, want string }{
		{"", "none", "true", "true", "image_only"},
		{"mixed", "textract", "false", "true", "mixed"},
		{"mixed", "none", "false", "true", "OCR_PROVIDER"},
		{"mixed", "textract", "true", "true", "GO_RAG_FAKE_MODE"},
		{"mixed", "textract", "false", "false", "EXTRACT_ISOLATION"},
		{"unknown", "none", "true", "true", "OCR_PDF_MODE"},
	} {
		t.Run(tc.mode+tc.provider+tc.fake+tc.isolate, func(t *testing.T) {
			t.Setenv("DATABASE_URL", "host=localhost dbname=rag")
			t.Setenv("AWS_REGION", "us-east-1")
			t.Setenv("SERVICE_TOKEN", "synthetic-service-token")
			t.Setenv("VECTOR_STORE", "postgres")
			t.Setenv("S3_BUCKET", "synthetic-bucket")
			t.Setenv("OCR_PDF_MODE", tc.mode)
			t.Setenv("OCR_PROVIDER", tc.provider)
			t.Setenv("GO_RAG_FAKE_MODE", tc.fake)
			t.Setenv("EXTRACT_ISOLATION", tc.isolate)
			cfg, err := Load()
			if tc.want == "mixed" || tc.want == "image_only" {
				require.NoError(t, err)
				require.Equal(t, tc.want, cfg.OCRPDFMode)
			} else {
				require.ErrorContains(t, err, tc.want)
			}
		})
	}
}

func TestPDFLayoutMode(t *testing.T) {
	for _, tc := range []struct{ mode, provider, fake, isolate, want string }{
		{"", "none", "true", "true", "plain"},
		{"plain", "none", "true", "true", "plain"},
		{"textract", "textract", "false", "true", "textract"},
		{"textract", "none", "false", "true", "OCR_PROVIDER"},
		{"textract", "textract", "true", "true", "GO_RAG_FAKE_MODE"},
		{"textract", "textract", "false", "false", "EXTRACT_ISOLATION"},
		{"unknown", "none", "true", "true", "PDF_LAYOUT_MODE"},
	} {
		t.Run(tc.mode+tc.provider+tc.fake+tc.isolate, func(t *testing.T) {
			t.Setenv("DATABASE_URL", "host=localhost dbname=rag")
			t.Setenv("AWS_REGION", "us-east-1")
			t.Setenv("SERVICE_TOKEN", "synthetic-service-token")
			t.Setenv("VECTOR_STORE", "postgres")
			t.Setenv("S3_BUCKET", "synthetic-bucket")
			t.Setenv("PDF_LAYOUT_MODE", tc.mode)
			t.Setenv("OCR_PROVIDER", tc.provider)
			t.Setenv("GO_RAG_FAKE_MODE", tc.fake)
			t.Setenv("EXTRACT_ISOLATION", tc.isolate)
			cfg, err := Load()
			if tc.want == "plain" || tc.want == "textract" {
				require.NoError(t, err)
				require.Equal(t, tc.want, cfg.PDFLayoutMode)
			} else {
				require.ErrorContains(t, err, tc.want)
			}
		})
	}
}

func TestOCRConfiguration(t *testing.T) {
	for _, tc := range []struct{ name, provider, bucket, timeout, jobTimeout, wantError string }{
		{name: "default"},
		{name: "enabled", provider: "textract", bucket: "synthetic-bucket"},
		{name: "unknown", provider: "other", wantError: "OCR_PROVIDER"},
		{name: "missing bucket", provider: "textract", wantError: "S3_BUCKET"},
		{name: "invalid timeout", timeout: "later", wantError: "OCR_TIMEOUT"},
		{name: "zero timeout", timeout: "0s", wantError: "OCR_TIMEOUT"},
		{name: "negative timeout", timeout: "-1m", wantError: "OCR_TIMEOUT"},
		{name: "equal timeout", provider: "textract", bucket: "synthetic-bucket", timeout: "15m", wantError: "OCR_TIMEOUT"},
		{name: "larger timeout", provider: "textract", bucket: "synthetic-bucket", timeout: "20m", wantError: "OCR_TIMEOUT"},
		{name: "short job with disabled OCR", jobTimeout: "5m"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("GO_RAG_FAKE_MODE", "true")
			t.Setenv("DATABASE_URL", "host=localhost dbname=rag")
			t.Setenv("OCR_PROVIDER", tc.provider)
			t.Setenv("S3_BUCKET", tc.bucket)
			t.Setenv("OCR_TIMEOUT", tc.timeout)
			t.Setenv("INGEST_JOB_TIMEOUT", tc.jobTimeout)
			_, err := Load()
			if tc.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantError) {
					t.Fatalf("want %s error, got %v", tc.wantError, err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
		})
	}
}
