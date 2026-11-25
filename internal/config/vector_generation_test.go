package config

import (
	"strings"
	"testing"
)

func TestVectorGenerationDefaultsToLegacy(t *testing.T) {
	t.Setenv("VECTOR_GENERATION", "")
	cfg := pineconeLoadConfig(t)
	if cfg.VectorGeneration != "" || cfg.ActiveVectorGeneration() != "legacy" {
		t.Fatalf("unexpected default generation %q / %q", cfg.VectorGeneration, cfg.ActiveVectorGeneration())
	}
}

func TestVectorGenerationSelection(t *testing.T) {
	for _, name := range []string{"legacy", "g2", "next_gen_1"} {
		t.Run(name, func(t *testing.T) {
			t.Setenv("GO_RAG_FAKE_MODE", "false")
			t.Setenv("DATABASE_URL", "host=localhost dbname=rag")
			t.Setenv("SERVICE_TOKEN", "synthetic-service-token")
			t.Setenv("VECTOR_STORE", "postgres")
			t.Setenv("EMBEDDING_PROVIDER", "openai")
			t.Setenv("LLM_PROVIDER", "openai")
			t.Setenv("OPENAI_BASE_URL", "http://localhost:11434/v1")
			t.Setenv("OPENAI_CHAT_MODEL", "chat")
			t.Setenv("OPENAI_EMBEDDING_MODEL", "embed")
			t.Setenv("VECTOR_GENERATION", name)
			cfg, err := Load()
			if err != nil {
				t.Fatal(err)
			}
			if cfg.ActiveVectorGeneration() != name {
				t.Fatalf("got %q", cfg.ActiveVectorGeneration())
			}
		})
	}
}

func TestVectorGenerationRejectsUnsafeNamesAndFakeMode(t *testing.T) {
	for _, name := range []string{"G2", "2g", "g-2", "g 2", "g;drop", strings.Repeat("a", 25)} {
		t.Run(name, func(t *testing.T) {
			t.Setenv("GO_RAG_FAKE_MODE", "true")
			t.Setenv("DATABASE_URL", "host=localhost dbname=rag")
			t.Setenv("VECTOR_GENERATION", name)
			if _, err := Load(); err == nil || !strings.Contains(err.Error(), "VECTOR_GENERATION") {
				t.Fatalf("expected VECTOR_GENERATION error, got %v", err)
			}
		})
	}
	t.Run("fake mode", func(t *testing.T) {
		t.Setenv("GO_RAG_FAKE_MODE", "true")
		t.Setenv("DATABASE_URL", "host=localhost dbname=rag")
		t.Setenv("VECTOR_GENERATION", "g2")
		if _, err := Load(); err == nil || !strings.Contains(err.Error(), "VECTOR_GENERATION") {
			t.Fatalf("offline fake mode keeps no generations, got %v", err)
		}
	})
}
