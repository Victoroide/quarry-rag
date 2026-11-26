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
		{"SYNTHESIS_CONTEXT_POLICY", "auto"},
		{"SYNTHESIS_CONTEXT_POLICY", "RANKED"},
		{"SYNTHESIS_CONTEXT_POLICY", "invalid"},
	} {
		t.Run(tc.key+"/"+tc.value, func(t *testing.T) {
			awsConfigEnvironment(t)
			t.Setenv("SYNTHESIS_CONTEXT_TOKENS", "")
			t.Setenv("SYNTHESIS_RELATIVE_SCORE_FLOOR", "")
			t.Setenv("SYNTHESIS_CONTEXT_POLICY", "")
			t.Setenv(tc.key, tc.value)
			if _, err := Load(); err == nil || !strings.Contains(err.Error(), tc.key) {
				t.Fatalf("expected %s error, got %v", tc.key, err)
			}
		})
	}
}

func TestLoadSynthesisBudgetDefaultsAndBounds(t *testing.T) {
	for _, tc := range []struct {
		name, tokens, floor, policy string
		wantTokens                  int
		wantFloor                   float64
		wantPolicy                  string
	}{
		{"defaults", "", "", "", 2400, .55, SynthesisContextPolicyRanked},
		{"lower bounds", "256", "0", "ranked", 256, 0, SynthesisContextPolicyRanked},
		{"upper bounds", "100000", "1", "diverse", 100000, 1, SynthesisContextPolicyDiverse},
		{"overrides", "4096", "0.8", "diverse", 4096, .8, SynthesisContextPolicyDiverse},
	} {
		t.Run(tc.name, func(t *testing.T) {
			awsConfigEnvironment(t)
			t.Setenv("SYNTHESIS_CONTEXT_TOKENS", tc.tokens)
			t.Setenv("SYNTHESIS_RELATIVE_SCORE_FLOOR", tc.floor)
			t.Setenv("SYNTHESIS_CONTEXT_POLICY", tc.policy)
			cfg, err := Load()
			if err != nil {
				t.Fatal(err)
			}
			if cfg.SynthesisContextTokens != tc.wantTokens || cfg.SynthesisRelativeScoreFloor != tc.wantFloor || cfg.SynthesisContextPolicy != tc.wantPolicy {
				t.Fatalf("unexpected synthesis budget: tokens=%d floor=%g policy=%q", cfg.SynthesisContextTokens, cfg.SynthesisRelativeScoreFloor, cfg.SynthesisContextPolicy)
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
		OCRProvider: "none", OCRTimeout: 10 * time.Minute,
		DatabaseURL:       "postgres://localhost:5432/db?sslmode=disable",
		AWSRegion:         "us-east-1",
		AWSAccessKeyID:    "AKIA_TEST",
		AWSSecretKey:      "secret_test",
		PineconeAPIKey:    "pc_test",
		PineconeIndexName: "quarry-rag",
		WorkerQueueSize:   1000,
		IngestWorkers:     2, IngestMaxAttempts: 5, IngestLease: 2 * time.Minute, IngestJobTimeout: 15 * time.Minute, IngestPollInterval: time.Second,
		EmbedConcurrency:          10,
		ServiceToken:              "a-strong-service-token",
		FakeMode:                  false,
		PineconeUpsertBatchSize:   100,
		PineconeTimeout:           10 * time.Second,
		PineconeMaxAttempts:       5,
		HostedVectorSweepInterval: 10 * time.Second, HostedVectorSweepPageSize: 100, HostedVectorSweepBudget: 1000, HostedVectorDrainDelay: 30 * time.Second, HostedVectorConvergenceDelay: 5 * time.Second,
		ExtractIsolation: true, ExtractTimeout: 30 * time.Second, ExtractConcurrency: 2, ExtractMemoryLimitMB: 512, ExtractQueueTimeout: 5 * time.Second,
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

func TestLoadVectorStoreSelection(t *testing.T) {
	for _, tc := range []struct {
		name, store, fake   string
		noPinecone, invalid bool
	}{
		{name: "postgres without Pinecone", store: "postgres", noPinecone: true},
		{name: "default needs Pinecone", noPinecone: true, invalid: true},
		{name: "pinecone needs Pinecone", store: "pinecone", noPinecone: true, invalid: true},
		{name: "fake postgres", store: "postgres", fake: "true", noPinecone: true},
		{name: "unknown", store: "unknown", invalid: true},
		{name: "unknown fake", store: "unknown", fake: "true", invalid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			awsConfigEnvironment(t)
			t.Setenv("VECTOR_STORE", tc.store)
			if tc.fake != "" {
				t.Setenv("GO_RAG_FAKE_MODE", tc.fake)
			}
			if tc.noPinecone {
				t.Setenv("PINECONE_API_KEY", "")
				t.Setenv("PINECONE_INDEX_NAME", "")
			}
			_, err := Load()
			if tc.invalid {
				if err == nil {
					t.Fatal("expected invalid store configuration")
				}
			} else if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestLoadPGVectorExactScopeMax(t *testing.T) {
	awsConfigEnvironment(t)
	t.Setenv("PGVECTOR_EXACT_SCOPE_MAX", "")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.PGVectorExactScopeMax != 0 {
		t.Fatalf("expected default PGVECTOR_EXACT_SCOPE_MAX=0, got %d", cfg.PGVectorExactScopeMax)
	}

	for _, tc := range []struct {
		name  string
		value string
		want  int
	}{
		{"zero", "0", 0},
		{"positive", "250", 250},
		{"upper bound", "100000", 100000},
	} {
		t.Run("valid/"+tc.name, func(t *testing.T) {
			awsConfigEnvironment(t)
			t.Setenv("PGVECTOR_EXACT_SCOPE_MAX", tc.value)
			cfg, err := Load()
			if err != nil {
				t.Fatal(err)
			}
			if cfg.PGVectorExactScopeMax != tc.want {
				t.Fatalf("PGVectorExactScopeMax = %d, want %d", cfg.PGVectorExactScopeMax, tc.want)
			}
		})
	}

	for _, tc := range []struct {
		name  string
		value string
	}{
		{"negative", "-1"},
		{"above bound", "100001"},
		{"float", "10.5"},
		{"malformed", "invalid"},
		{"overflow", "99999999999999999999999"},
	} {
		t.Run("invalid/"+tc.name, func(t *testing.T) {
			awsConfigEnvironment(t)
			t.Setenv("PGVECTOR_EXACT_SCOPE_MAX", tc.value)
			if _, err := Load(); err == nil || !strings.Contains(err.Error(), "PGVECTOR_EXACT_SCOPE_MAX") {
				t.Fatalf("expected PGVECTOR_EXACT_SCOPE_MAX error, got %v", err)
			}
		})
	}
}

func TestLoadHostedVectorManifestConfig(t *testing.T) {
	awsConfigEnvironment(t)
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.HostedVectorManifest {
		t.Fatal("HOSTED_VECTOR_MANIFEST must default to false")
	}
	if cfg.HostedVectorSweepInterval != 10*time.Second || cfg.HostedVectorSweepPageSize != 100 ||
		cfg.HostedVectorSweepBudget != 1000 || cfg.HostedVectorDrainDelay != 30*time.Second ||
		cfg.HostedVectorConvergenceDelay != 5*time.Second {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}

	t.Setenv("HOSTED_VECTOR_MANIFEST", "true")
	t.Setenv("HOSTED_VECTOR_SWEEP_INTERVAL", "0s")
	t.Setenv("HOSTED_VECTOR_SWEEP_PAGE_SIZE", "250")
	t.Setenv("HOSTED_VECTOR_SWEEP_BUDGET", "5000")
	t.Setenv("HOSTED_VECTOR_DRAIN_DELAY", "45s")
	t.Setenv("HOSTED_VECTOR_CONVERGENCE_DELAY", "2s")
	cfg, err = Load()
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.HostedVectorManifest || cfg.HostedVectorSweepInterval != 0 || cfg.HostedVectorSweepPageSize != 250 ||
		cfg.HostedVectorSweepBudget != 5000 || cfg.HostedVectorDrainDelay != 45*time.Second ||
		cfg.HostedVectorConvergenceDelay != 2*time.Second {
		t.Fatalf("unexpected overrides: %+v", cfg)
	}

	for _, tc := range []struct{ key, val string }{
		{"HOSTED_VECTOR_MANIFEST", "maybe"},
		{"HOSTED_VECTOR_SWEEP_INTERVAL", "50ms"},
		{"HOSTED_VECTOR_SWEEP_INTERVAL", "-1s"},
		{"HOSTED_VECTOR_SWEEP_INTERVAL", "2h"},
		{"HOSTED_VECTOR_SWEEP_PAGE_SIZE", "0"},
		{"HOSTED_VECTOR_SWEEP_PAGE_SIZE", "1001"},
		{"HOSTED_VECTOR_SWEEP_BUDGET", "0"},
		{"HOSTED_VECTOR_SWEEP_BUDGET", "100001"},
		{"HOSTED_VECTOR_DRAIN_DELAY", "-1s"},
		{"HOSTED_VECTOR_DRAIN_DELAY", "2h"},
		{"HOSTED_VECTOR_CONVERGENCE_DELAY", "-1s"},
		{"HOSTED_VECTOR_CONVERGENCE_DELAY", "2h"},
	} {
		t.Run("invalid/"+tc.key+"/"+tc.val, func(t *testing.T) {
			awsConfigEnvironment(t)
			t.Setenv(tc.key, tc.val)
			if _, err := Load(); err == nil || !strings.Contains(err.Error(), tc.key) {
				t.Fatalf("expected %s error, got %v", tc.key, err)
			}
		})
	}
}
