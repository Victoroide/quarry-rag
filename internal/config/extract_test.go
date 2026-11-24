package config

import (
	"strings"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/extractor"
	"github.com/stretchr/testify/require"
)

func TestLoadExtractionSettings(t *testing.T) {
	t.Setenv("GO_RAG_FAKE_MODE", "true")
	t.Setenv("DATABASE_URL", "host=localhost dbname=rag")
	for _, key := range []string{"EXTRACT_ISOLATION", "EXTRACT_TIMEOUT", "EXTRACT_CONCURRENCY", "EXTRACT_MEMORY_LIMIT_MB", "EXTRACT_QUEUE_TIMEOUT", "DOC_STORIES"} {
		t.Setenv(key, "")
	}
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.ExtractIsolation || cfg.ExtractTimeout != 30*time.Second || cfg.ExtractConcurrency != 2 || cfg.ExtractMemoryLimitMB != 512 || cfg.ExtractQueueTimeout != 5*time.Second {
		t.Fatalf("incorrect extraction defaults: %+v", cfg)
	}
	if cfg.DOCStories != 0 {
		t.Fatal("optional DOC stories enabled by default")
	}
	for _, tt := range []struct{ key, value string }{
		{"EXTRACT_ISOLATION", "invalid"}, {"EXTRACT_TIMEOUT", "invalid"}, {"EXTRACT_TIMEOUT", "999ms"}, {"EXTRACT_TIMEOUT", "11m"},
		{"EXTRACT_CONCURRENCY", "0"}, {"EXTRACT_CONCURRENCY", "65"}, {"EXTRACT_CONCURRENCY", "invalid"},
		{"EXTRACT_MEMORY_LIMIT_MB", "63"}, {"EXTRACT_MEMORY_LIMIT_MB", "8193"},
		{"EXTRACT_QUEUE_TIMEOUT", "-1s"}, {"EXTRACT_QUEUE_TIMEOUT", "61s"}, {"EXTRACT_QUEUE_TIMEOUT", "invalid"},
		{"DOC_STORIES", "body,properties"}, {"DOC_STORIES", "body,,comments"}, {"DOC_STORIES", "footnotes,footnotes"},
	} {
		t.Run(tt.key+"/"+tt.value, func(t *testing.T) {
			t.Setenv(tt.key, tt.value)
			if _, loadErr := Load(); loadErr == nil || !strings.Contains(loadErr.Error(), tt.key) {
				t.Fatalf("invalid %s accepted: %v", tt.key, loadErr)
			}
		})
	}
	t.Setenv("EXTRACT_ISOLATION", "false")
	t.Setenv("EXTRACT_TIMEOUT", "1m")
	t.Setenv("EXTRACT_CONCURRENCY", "4")
	t.Setenv("EXTRACT_MEMORY_LIMIT_MB", "1024")
	t.Setenv("EXTRACT_QUEUE_TIMEOUT", "0s")
	t.Setenv("DOC_STORIES", "body, headers, comments")
	cfg, err = Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ExtractIsolation || cfg.ExtractTimeout != time.Minute || cfg.ExtractConcurrency != 4 || cfg.ExtractMemoryLimitMB != 1024 || cfg.ExtractQueueTimeout != 0 {
		t.Fatalf("incorrect extraction settings: %+v", cfg)
	}
	if want, parseErr := extractor.ParseDOCStories("body,comments,headers"); parseErr != nil || cfg.DOCStories != want || want == 0 {
		t.Fatalf("configured DOC stories: got %v, want %v (%v)", cfg.DOCStories, want, parseErr)
	}
}

func TestLoadDOCXStories(t *testing.T) {
	t.Setenv("GO_RAG_FAKE_MODE", "true")
	t.Setenv("DATABASE_URL", "host=localhost dbname=rag")
	for _, tt := range []struct{ value, canonical string }{
		{"", "body,footnotes,endnotes"},
		{"comments, body ,headers,footers", "body,headers,footers,comments"},
		{"endnotes", "endnotes"},
	} {
		t.Run(tt.canonical, func(t *testing.T) {
			t.Setenv("DOCX_STORIES", tt.value)
			cfg, err := Load()
			require.NoError(t, err)
			want, err := extractor.ParseDOCXStories(tt.canonical)
			require.NoError(t, err)
			require.Equal(t, want, cfg.DOCXStories)
		})
	}
	for _, value := range []string{" ", "BODY", "images", "body,", "body,,headers", "body, body"} {
		t.Run(value, func(t *testing.T) {
			t.Setenv("DOCX_STORIES", value)
			_, err := Load()
			require.ErrorContains(t, err, "DOCX_STORIES")
		})
	}
}
