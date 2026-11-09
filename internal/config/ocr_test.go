package config

import (
	"strings"
	"testing"
)

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
