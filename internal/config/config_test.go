package config

import (
	"slices"
	"strings"
	"testing"
	"time"
)

func TestLoadRejectsInvalidSynthesisBudget(t *testing.T) {
	for _, tc := range []struct{ key, value string }{
		{"SYNTHESIS_CONTEXT_TOKENS", "255"},
		{"SYNTHESIS_CONTEXT_TOKENS", "100001"},
		{"SYNTHESIS_CONTEXT_TOKENS", "0"},
		{"SYNTHESIS_CONTEXT_TOKENS", "-1"},
		{"SYNTHESIS_CONTEXT_TOKENS", "256.5"},
		{"SYNTHESIS_CONTEXT_TOKENS", "invalid"},
		{"SYNTHESIS_CONTEXT_TOKENS", "99999999999999999999999"},
		{"SYNTHESIS_RELATIVE_SCORE_FLOOR", "-0.01"},
		{"SYNTHESIS_RELATIVE_SCORE_FLOOR", "1.01"},
		{"SYNTHESIS_RELATIVE_SCORE_FLOOR", "NaN"},
		{"SYNTHESIS_RELATIVE_SCORE_FLOOR", "+Inf"},
		{"SYNTHESIS_RELATIVE_SCORE_FLOOR", "-Inf"},
		{"SYNTHESIS_RELATIVE_SCORE_FLOOR", "invalid"},
	} {
		t.Run(tc.key+"/"+tc.value, func(t *testing.T) {
			awsConfigEnvironment(t)
			t.Setenv("SYNTHESIS_CONTEXT_TOKENS", "")
			t.Setenv("SYNTHESIS_RELATIVE_SCORE_FLOOR", "")
			t.Setenv(tc.key, tc.value)
			if _, err := Load(); err == nil || !strings.Contains(err.Error(), tc.key) {
				t.Fatalf("expected %s error, got %v", tc.key, err)
			}
		})
	}
}

func TestLoadSynthesisBudgetDefaultsAndBounds(t *testing.T) {
	for _, tc := range []struct {
		name, tokens, floor string
		wantTokens          int
		wantFloor           float64
	}{
		{"defaults", "", "", 2400, .55},
		{"lower bounds", "256", "0", 256, 0},
		{"upper bounds", "100000", "1", 100000, 1},
		{"overrides", "4096", "0.8", 4096, .8},
	} {
		t.Run(tc.name, func(t *testing.T) {
			awsConfigEnvironment(t)
			t.Setenv("SYNTHESIS_CONTEXT_TOKENS", tc.tokens)
			t.Setenv("SYNTHESIS_RELATIVE_SCORE_FLOOR", tc.floor)
			cfg, err := Load()
			if err != nil {
				t.Fatal(err)
			}
			if cfg.SynthesisContextTokens != tc.wantTokens || cfg.SynthesisRelativeScoreFloor != tc.wantFloor {
				t.Fatalf("unexpected synthesis budget: tokens=%d floor=%g", cfg.SynthesisContextTokens, cfg.SynthesisRelativeScoreFloor)
			}
		})
	}
}

func TestLoadBedrockGuardrails(t *testing.T) {
	for _, tc := range []struct {
		name, id, version string
		invalid           bool
	}{
		{name: "disabled"},
		{name: "draft", id: "guardrail", version: "DRAFT"},
		{name: "published", id: "guardrail", version: "12"},
		{name: "missing version", id: "guardrail", invalid: true},
		{name: "missing identifier", version: "1", invalid: true},
		{name: "zero", id: "guardrail", version: "0", invalid: true},
		{name: "negative", id: "guardrail", version: "-1", invalid: true},
		{name: "signed", id: "guardrail", version: "+1", invalid: true},
		{name: "fractional", id: "guardrail", version: "1.5", invalid: true},
		{name: "leading zero", id: "guardrail", version: "01", invalid: true},
		{name: "lowercase draft", id: "guardrail", version: "draft", invalid: true},
		{name: "blank identifier", id: " ", version: "1", invalid: true},
		{name: "padded identifier", id: "guardrail ", version: "1", invalid: true},
		{name: "blank version", id: "guardrail", version: " ", invalid: true},
		{name: "padded version", id: "guardrail", version: " 1 ", invalid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			awsConfigEnvironment(t)
			t.Setenv("BEDROCK_GUARDRAIL_ID", tc.id)
			t.Setenv("BEDROCK_GUARDRAIL_VERSION", tc.version)
			_, err := Load()
			if tc.invalid {
				if err == nil || !strings.Contains(err.Error(), "BEDROCK_GUARDRAIL") {
					t.Fatalf("expected guardrail configuration error, got %v", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestLoadReranking(t *testing.T) {
	for _, tc := range []struct {
		name, timeout string
		want          time.Duration
		invalid       bool
	}{
		{"default", "", 5 * time.Second, false},
		{"lower bound", "1s", time.Second, false},
		{"upper bound", "60s", time.Minute, false},
		{"fractional", "1.5s", 1500 * time.Millisecond, false},
		{"zero", "0s", 0, true},
		{"negative", "-1s", 0, true},
		{"too short", "999ms", 0, true},
		{"too long", "60.001s", 0, true},
		{"malformed", "five", 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			awsConfigEnvironment(t)
			t.Setenv("RERANK_MODEL_ARN", "arn:aws:bedrock:us-east-1::foundation-model/test")
			t.Setenv("RERANK_TIMEOUT", tc.timeout)
			cfg, err := Load()
			if tc.invalid {
				if err == nil || !strings.Contains(err.Error(), "RERANK_TIMEOUT") {
					t.Fatalf("expected timeout error, got %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if cfg.RerankTimeout != tc.want || cfg.RerankModelARN != "arn:aws:bedrock:us-east-1::foundation-model/test" {
				t.Fatalf("unexpected reranking configuration: %v", cfg.RerankTimeout)
			}
		})
	}
	for _, arn := range []string{" arn:aws:bedrock:us-east-1::foundation-model/test", "cohere.rerank-v3-5:0", "arn:aws:bedrock:us-east-1::foundation-model/test\u007f", "arn:aws:bedrock:us-east-1::foundation-model/test\u00a0"} {
		t.Run(arn, func(t *testing.T) {
			awsConfigEnvironment(t)
			t.Setenv("RERANK_MODEL_ARN", arn)
			t.Setenv("RERANK_TIMEOUT", "5s")
			if _, err := Load(); err == nil || !strings.Contains(err.Error(), "RERANK_MODEL_ARN") {
				t.Fatalf("expected ARN validation error, got %v", err)
			}
		})
	}

	awsConfigEnvironment(t)
	t.Setenv("RERANK_MODEL_ARN", "")
	t.Setenv("RERANK_TIMEOUT", "")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.RerankModelARN != "" {
		t.Fatal("reranking enabled by default")
	}
}

func TestLoadIndexVisibilityTimeout(t *testing.T) {
	for _, tc := range []struct {
		name, value, jobTimeout string
		want                    time.Duration
		invalid                 bool
	}{
		{name: "default", want: 30 * time.Second},
		{name: "disabled", value: "0s"},
		{name: "fractional", value: "250ms", want: 250 * time.Millisecond},
		{name: "upper bound", value: "5m", want: 5 * time.Minute},
		{name: "negative", value: "-1ns", invalid: true},
		{name: "above bound", value: "5m1ns", invalid: true},
		{name: "malformed", value: "invalid", invalid: true},
		{name: "equal job timeout", value: "5m", jobTimeout: "5m", invalid: true},
		{name: "above job timeout", value: "5m", jobTimeout: "3m", invalid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			awsConfigEnvironment(t)
			t.Setenv("INDEX_VISIBILITY_TIMEOUT", tc.value)
			if tc.jobTimeout != "" {
				t.Setenv("INGEST_JOB_TIMEOUT", tc.jobTimeout)
			}
			cfg, err := Load()
			if tc.invalid {
				if err == nil || !strings.Contains(err.Error(), "INDEX_VISIBILITY_TIMEOUT") {
					t.Fatalf("expected INDEX_VISIBILITY_TIMEOUT error, got %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if cfg.IndexVisibilityTimeout != tc.want {
				t.Fatalf("visibility timeout = %v, want %v", cfg.IndexVisibilityTimeout, tc.want)
			}
		})
	}
}

// baseValidConfig returns a Config that passes validate() in real (non-fake)
// mode, so each test can flip exactly one field and assert the guard it targets.
func baseValidConfig() *Config {
	return &Config{
		ServerPort: "8081", ChunkTokenSize: 512, ChunkOverlap: 64, LocalRetrievalThreshold: 0.7, MaxChunksPerQuery: 10, MaxUploadBytes: 20 << 20, EmbeddingDimensions: 1024, SynthesisModel: "fallback",
		DatabaseURL:       "postgres://localhost:5432/db?sslmode=disable",
		AWSRegion:         "us-east-1",
		AWSAccessKeyID:    "AKIA_TEST",
		AWSSecretKey:      "secret_test",
		PineconeAPIKey:    "pc_test",
		PineconeIndexName: "quarry-rag",
		WorkerQueueSize:   1000,
		IngestWorkers:     2, IngestMaxAttempts: 5, IngestLease: 2 * time.Minute, IngestJobTimeout: 15 * time.Minute, IngestPollInterval: time.Second,
		EmbedConcurrency:        10,
		ServiceToken:            "a-strong-service-token",
		FakeMode:                false,
		PineconeUpsertBatchSize: 100,
		PineconeTimeout:         10 * time.Second,
		PineconeMaxAttempts:     5,
		ExtractIsolation:        true, ExtractTimeout: 30 * time.Second, ExtractConcurrency: 2, ExtractMemoryLimitMB: 512, ExtractQueueTimeout: 5 * time.Second,
		MaxAllowedDocumentIDs: 25000,
		AWSMaxAttempts:        4, EmbedTimeout: 15 * time.Second,
		ShutdownDrainDelay: 5 * time.Second, HealthCacheTTL: 10 * time.Second,
		QueryTimeout: time.Minute, SynthesisTimeout: 45 * time.Second, QueryConcurrency: 32, QueryQueueTimeout: 2 * time.Second,
		HTTPReadTimeout: time.Minute, HTTPWriteTimeout: 75 * time.Second, HTTPIdleTimeout: time.Minute,
		SynthesisContextTokens: 2400, SynthesisRelativeScoreFloor: .55,
		RerankTimeout: 5 * time.Second,
	}
}

func TestIndexNameRequiredWithExternalServices(t *testing.T) {
	awsConfigEnvironment(t)
	t.Setenv("PINECONE_INDEX_NAME", "")
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "PINECONE_INDEX_NAME") {
		t.Fatalf("expected missing PINECONE_INDEX_NAME error, got %v", err)
	}
}

func TestIndexNameOptionalInOfflineMode(t *testing.T) {
	t.Setenv("GO_RAG_FAKE_MODE", "true")
	t.Setenv("DATABASE_URL", "host=localhost dbname=rag")
	t.Setenv("PINECONE_INDEX_NAME", "")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.PineconeIndexName != "" {
		t.Fatalf("expected no implicit index name, got %q", cfg.PineconeIndexName)
	}
}

func TestPineconeEnvironmentIsIgnoredWithWarning(t *testing.T) {
	for _, fakeMode := range []string{"false", "true"} {
		t.Run(fakeMode, func(t *testing.T) {
			awsConfigEnvironment(t)
			t.Setenv("GO_RAG_FAKE_MODE", fakeMode)
			t.Setenv("PINECONE_ENVIRONMENT", "unused-environment")
			cfg, err := Load()
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Contains(cfg.Warnings, "PINECONE_ENVIRONMENT is ignored") {
				t.Fatalf("expected ignored environment warning, got %v", cfg.Warnings)
			}
			if cfg.PineconeIndexName != "quarry-rag" {
				t.Fatalf("ignored environment changed index name: %q", cfg.PineconeIndexName)
			}
		})
	}
}

func TestExplicitIndexNameHasNoWarning(t *testing.T) {
	awsConfigEnvironment(t)
	t.Setenv("PINECONE_INDEX_NAME", "quarry-rag")
	t.Setenv("PINECONE_ENVIRONMENT", "")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.PineconeIndexName != "quarry-rag" || len(cfg.Warnings) != 0 {
		t.Fatalf("unexpected index configuration: index=%q warnings=%v", cfg.PineconeIndexName, cfg.Warnings)
	}
}

func TestValidate_NonFakeRequiresServiceToken(t *testing.T) {
	cfg := baseValidConfig()
	cfg.ServiceToken = ""
	if err := cfg.validate(); err == nil {
		t.Fatal("expected validate() to fail when SERVICE_TOKEN is empty in non-fake mode")
	}

	cfg.ServiceToken = "   " // whitespace must not count as a token
	if err := cfg.validate(); err == nil {
		t.Fatal("expected validate() to fail when SERVICE_TOKEN is only whitespace in non-fake mode")
	}
}

func TestValidate_NonFakeWithServiceTokenOK(t *testing.T) {
	cfg := baseValidConfig()
	if err := cfg.validate(); err != nil {
		t.Fatalf("expected valid non-fake config to pass, got %v", err)
	}
}

func TestValidate_FakeModeAllowsEmptyServiceToken(t *testing.T) {
	cfg := baseValidConfig()
	cfg.FakeMode = true
	cfg.ServiceToken = ""
	// Fake mode also runs without cloud creds.
	cfg.AWSRegion, cfg.AWSAccessKeyID, cfg.AWSSecretKey, cfg.PineconeAPIKey = "", "", "", ""
	if err := cfg.validate(); err != nil {
		t.Fatalf("expected fake-mode config with empty SERVICE_TOKEN to pass, got %v", err)
	}
}

func TestValidateOperationalBounds(t *testing.T) {
	for _, tt := range []struct {
		name   string
		mutate func(*Config)
	}{
		{"port", func(c *Config) { c.ServerPort = "70000" }},
		{"chunk size", func(c *Config) { c.ChunkTokenSize = 0 }},
		{"overlap", func(c *Config) { c.ChunkOverlap = c.ChunkTokenSize }},
		{"threshold", func(c *Config) { c.LocalRetrievalThreshold = 2 }},
		{"chunks", func(c *Config) { c.MaxChunksPerQuery = -1 }},
		{"upload", func(c *Config) { c.MaxUploadBytes = 0 }},
		{"dimensions", func(c *Config) { c.EmbeddingDimensions = 3 }},
		{"synthesis", func(c *Config) { c.SynthesisModel = "unknown" }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := baseValidConfig()
			tt.mutate(c)
			if c.validate() == nil {
				t.Fatal("invalid configuration accepted")
			}
		})
	}
}

func TestLoadRejectsMalformedNumericEnvironment(t *testing.T) {
	t.Setenv("GO_RAG_FAKE_MODE", "true")
	t.Setenv("DATABASE_URL", "postgres://localhost/rag")
	t.Setenv("WORKER_QUEUE_SIZE", "not-a-number")
	if _, err := Load(); err == nil {
		t.Fatal("malformed numeric configuration accepted")
	}
}

func TestLoadParsesEnvironmentOnceAndRejectsMalformedValues(t *testing.T) {
	for _, setting := range []struct{ key, value string }{{"MAX_UPLOAD_BYTES", "bad"}, {"LOCAL_RETRIEVAL_THRESHOLD", "bad"}, {"GO_RAG_FAKE_MODE", "bad"}, {"EMBEDDING_DIMENSIONS", "9999999999999999999999999999"}} {
		t.Run(setting.key, func(t *testing.T) {
			t.Setenv("DATABASE_URL", "host=localhost dbname=rag")
			t.Setenv("GO_RAG_FAKE_MODE", "true")
			t.Setenv(setting.key, setting.value)
			if _, err := Load(); err == nil || !strings.Contains(err.Error(), setting.key) {
				t.Fatalf("expected %s parse failure: %v", setting.key, err)
			}
		})
	}
}
