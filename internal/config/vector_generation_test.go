package config

import (
	"strings"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/domain"
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

func TestVectorTarget(t *testing.T) {
	for _, tc := range []struct {
		name   string
		cfg    Config
		store  string
		target string
	}{
		{name: "legacy postgres", cfg: Config{VectorStore: "postgres"}, store: "postgres", target: "chunk_vectors"},
		{name: "generation postgres", cfg: Config{VectorStore: "postgres", VectorGeneration: "g2"}, store: "postgres", target: "chunk_vectors_g2"},
		{name: "explicit legacy postgres", cfg: Config{VectorStore: "postgres", VectorGeneration: "legacy"}, store: "postgres", target: "chunk_vectors"},
		{name: "pinecone index", cfg: Config{VectorStore: "pinecone", PineconeIndexName: "quarry-g2", VectorGeneration: "g2"}, store: "pinecone", target: "quarry-g2"},
		{name: "default pinecone", cfg: Config{PineconeIndexName: "quarry"}, store: "pinecone", target: "quarry"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store, target := tc.cfg.VectorTarget()
			if store != tc.store || target != tc.target {
				t.Fatalf("got %s %s", store, target)
			}
		})
	}
}

func TestEmbeddingSettings(t *testing.T) {
	bedrock := (&Config{EmbeddingProvider: "bedrock", BedrockEmbedModel: "amazon.titan-embed-text-v2:0", EmbeddingDimensions: 1024}).EmbeddingSettings()
	if bedrock != (domain.EmbeddingProfile{Provider: "bedrock", Model: "amazon.titan-embed-text-v2:0", ModelRevision: "amazon.titan-embed-text-v2:0", Dimensions: 1024, Metric: domain.MetricCosine}) {
		t.Fatalf("unexpected settings %+v", bedrock)
	}
	compatible := (&Config{EmbeddingProvider: "openai", OpenAIEmbeddingModel: "embed-small", BedrockEmbedModel: "ignored", EmbeddingDimensions: 384}).EmbeddingSettings()
	if compatible != (domain.EmbeddingProfile{Provider: "openai", Model: "embed-small", Dimensions: 384, Metric: domain.MetricCosine}) {
		t.Fatalf("unexpected settings %+v", compatible)
	}
	cfg := Config{EmbeddingProvider: "openai", OpenAIEmbeddingModel: "embed-small", EmbeddingModelRevision: "deployment-1",
		EmbeddingDimensions: 384, ExtractionChunkVersion: "release", ChunkTokenSize: 512, ChunkOverlap: 64, OCRProvider: "none"}
	profile := cfg.EmbeddingSettings()
	if profile.ModelRevision != "deployment-1" || profile.ExtractionChunkVersion != "release/size=512/overlap=64/ocr=none/doc=0/docx=0" {
		t.Fatalf("unexpected deployment or text identity %+v", profile)
	}
	cfg.ChunkOverlap++
	if cfg.EmbeddingSettings().Hash() == profile.Hash() {
		t.Fatal("changing chunk overlap must change the profile identity")
	}
}
