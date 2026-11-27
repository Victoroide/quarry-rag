package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/api/handler"
	"github.com/go-jose/go-jose/v4"
	"github.com/stretchr/testify/require"
)

func TestStartupRejectsInvalidNoEvidenceCatalog(t *testing.T) {
	t.Setenv("GO_RAG_FAKE_MODE", "true")
	t.Setenv("DATABASE_URL", "invalid postgres configuration")
	path := filepath.Join(t.TempDir(), "no-evidence.json")
	t.Setenv("NO_EVIDENCE_CATALOG_FILE", path)
	require.ErrorContains(t, run(), "NO_EVIDENCE_CATALOG_FILE")
	for _, tc := range []struct{ name, body, detail string }{
		{"built-in tag", `{"en":"replacement"}`, "duplicate language tag"},
		{"German alias with script", `{"ger-Latn":"replacement"}`, "duplicate language tag"},
		{"repeated variant", `{"sl-rozaj-rozaj":"text"}`, "invalid language tag"},
		{"repeated extension", `{"ar-u-nu-latn-u-ca-gregory":"text"}`, "invalid language tag"},
		{"deprecated and preferred calendar", `{"ar-u-ca-islamicc":"first answer","ar-u-ca-islamic-civil":"second answer"}`, "duplicate language tag"},
		{"deprecated and preferred transform", `{"ar-t-m0-names":"first answer","ar-t-m0-prprname":"second answer"}`, "duplicate language tag"},
		{"duplicate Unicode attribute", `{"ar-u-foo-foo-nu-latn":"first answer","ar-u-foo-nu-latn":"second answer"}`, "duplicate language tag"},
		{"transform key in Unicode locale extension", `{"ar-u-m0-names":"text"}`, "invalid language tag extension"},
		{"wrong extended language prefix", `{"en-cmn":"text"}`, "invalid language tag"},
		{"multiple extended languages", `{"zh-cmn-yue":"text"}`, "invalid language tag"},
		{"reserved primary language", `{"root":"text"}`, "invalid language tag"},
		{"deprecated and preferred language", `{"dek":"first","sqm":"second"}`, "duplicate language tag"},
		{"extended and primary language", `{"zh-hnm":"first","hnm":"second"}`, "duplicate language tag"},
		{"extended language under another prefix", `{"en-hnm":"text"}`, "invalid language tag"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.NoError(t, os.WriteFile(path, []byte(tc.body), 0o600))
			err := run()
			require.ErrorContains(t, err, "NO_EVIDENCE_CATALOG_FILE")
			require.ErrorContains(t, err, tc.detail)
		})
	}
}

func TestStartupAcceptsRegisteredNoEvidenceLanguages(t *testing.T) {
	t.Setenv("GO_RAG_FAKE_MODE", "true")
	t.Setenv("DATABASE_URL", "invalid postgres configuration")
	path := filepath.Join(t.TempDir(), "no-evidence.json")
	t.Setenv("NO_EVIDENCE_CATALOG_FILE", path)
	for _, tc := range []struct{ name, body string }{
		{"extended Hainanese", `{"zh-hnm":"text"}`},
		{"primary Hainanese", `{"hnm":"text"}`},
		{"extended Cantonese", `{"zh-yue":"text"}`},
		{"extended Bhutanese Sign Language", `{"sgn-dyl":"text"}`},
		{"Classical Sanskrit", `{"cls":"text"}`},
		{"Interslavic with script", `{"isv-Latn":"text"}`},
		{"deprecated language", `{"dek":"text"}`},
		{"preferred language", `{"sqm":"text"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.NoError(t, os.WriteFile(path, []byte(tc.body), 0o600))
			err := run()
			require.Error(t, err)
			require.NotContains(t, err.Error(), "NO_EVIDENCE_CATALOG_FILE")
		})
	}
}

func TestServeWaitsForActiveRequests(t *testing.T) {
	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	release := make(chan struct{})
	srv := &http.Server{ReadHeaderTimeout: time.Second, Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { close(started); <-release; w.WriteHeader(204) })}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- serve(ctx, srv, listener, handler.NewProbes(nil, nil, nil, time.Minute), 0) }()
	requestDone := make(chan error, 1)
	go func() {
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+listener.Addr().String(), http.NoBody)
		if err != nil {
			requestDone <- err
			return
		}
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			err = resp.Body.Close()
		}
		requestDone <- err
	}()
	<-started
	cancel()
	select {
	case err := <-result:
		t.Fatalf("shutdown returned before active request completed: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if err := <-requestDone; err != nil {
		t.Fatal(err)
	}
	if err := <-result; err != nil {
		t.Fatal(err)
	}
}

// testBinaryCommand prepares a child process that runs one test of the current
// test binary. The executable always comes from os.Executable, never from input.
func testBinaryCommand(ctx context.Context, t *testing.T, test string) *exec.Cmd {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	return exec.CommandContext(ctx, executable, "-test.run=^"+test+"$") // #nosec G204 -- executable is the current test binary from os.Executable.
}

func TestServeSecondSignalSkipsDrain(t *testing.T) {
	if os.Getenv("QUARRY_TEST_DRAIN_CHILD") == "1" {
		listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		p := handler.NewProbes(nil, nil, nil, time.Minute)
		srv := &http.Server{ReadHeaderTimeout: time.Second, Handler: handler.LivenessHandler()}
		ctx, stop := signal.NotifyContext(t.Context(), syscall.SIGINT, syscall.SIGTERM)
		defer stop()
		go func() {
			for !p.Draining() {
				time.Sleep(time.Millisecond)
			}
			fmt.Println("draining")
		}()
		fmt.Println("ready")
		if err := serve(ctx, srv, listener, p, time.Minute); err != nil {
			t.Fatal(err)
		}
		return
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	cmd := testBinaryCommand(ctx, t, "TestServeSecondSignalSkipsDrain")
	cmd.Env = append(os.Environ(), "QUARRY_TEST_DRAIN_CHILD=1")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	scanner := bufio.NewScanner(stdout)
	for _, want := range []string{"ready", "draining"} {
		if !scanner.Scan() || scanner.Text() != want {
			cancel()
			waitErr := cmd.Wait()
			t.Fatalf("child state = %q, want %q: %v, %v, %s", scanner.Text(), want, scanner.Err(), waitErr, stderr.String())
		}
		if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
			cancel()
			waitErr := cmd.Wait()
			t.Fatalf("signal child: %v; wait: %v", err, waitErr)
		}
	}
	if err := cmd.Wait(); err != nil {
		t.Fatalf("second signal did not finish shutdown: %v, %s", err, stderr.String())
	}
}

func TestServeDrainsBeforeShutdown(t *testing.T) {
	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := handler.NewProbes(nil, nil, nil, time.Minute)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /livez", handler.LivenessHandler())
	mux.HandleFunc("GET /readyz", handler.ReadinessHandler(p))
	srv := &http.Server{ReadHeaderTimeout: time.Second, Handler: mux}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- serve(ctx, srv, listener, p, 200*time.Millisecond) }()
	cancel()
	deadline := time.After(time.Second)
	for !p.Draining() {
		select {
		case <-deadline:
			t.Fatal("shutdown did not mark readiness draining")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	for _, tt := range []struct {
		path string
		code int
	}{{"/livez", http.StatusOK}, {"/readyz", http.StatusServiceUnavailable}} {
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+listener.Addr().String()+tt.path, http.NoBody)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		if err := resp.Body.Close(); err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode != tt.code {
			t.Errorf("%s during drain = %d", tt.path, resp.StatusCode)
		}
	}
	select {
	case err := <-result:
		t.Fatalf("server stopped before drain elapsed: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	if err := <-result; err != nil {
		t.Fatal(err)
	}
}

// TestServerEnablesLegacyDeletionReservations pins the production wiring: the
// router the server builds reserves every ID-only deletion under the activation
// lock, with no local setting that could turn it off.
func TestServerEnablesLegacyDeletionReservations(t *testing.T) {
	file, err := parser.ParseFile(token.NewFileSet(), "main.go", nil, 0)
	require.NoError(t, err)
	var literals int
	ast.Inspect(file, func(node ast.Node) bool {
		literal, ok := node.(*ast.CompositeLit)
		if !ok {
			return true
		}
		selector, ok := literal.Type.(*ast.SelectorExpr)
		if !ok || selector.Sel.Name != "RouterDeps" {
			return true
		}
		literals++
		var enabled bool
		for _, element := range literal.Elts {
			pair, isPair := element.(*ast.KeyValueExpr)
			if !isPair || pair.Key.(*ast.Ident).Name != "LegacyDeletionReservations" {
				continue
			}
			value, isIdent := pair.Value.(*ast.Ident)
			enabled = isIdent && value.Name == "true"
		}
		require.True(t, enabled, "the server router must enable legacy deletion reservations unconditionally")
		return true
	})
	require.Equal(t, 1, literals, "the server builds exactly one router")
}

func TestServerFatalStageCoverage(t *testing.T) {
	file, err := parser.ParseFile(token.NewFileSet(), "main.go", nil, 0)
	require.NoError(t, err)

	var serverBody *ast.BlockStmt
	for _, decl := range file.Decls {
		if fn, ok := decl.(*ast.FuncDecl); ok && fn.Recv == nil && fn.Name.Name == "runServer" {
			serverBody = fn.Body
			break
		}
	}
	require.NotNil(t, serverBody)
	var stages []string
	ast.Inspect(serverBody, func(node ast.Node) bool {
		switch stmt := node.(type) {
		case *ast.FuncLit:
			return false
		case *ast.ReturnStmt:
			require.Len(t, stmt.Results, 2, "runServer must return a stage separately from its error")
			stage, ok := stmt.Results[0].(*ast.Ident)
			require.True(t, ok)
			stages = append(stages, stage.Name)
		}
		return true
	})
	require.Equal(t, []string{
		"fatalConfiguration", "fatalNoEvidenceCatalog", "fatalCitationVerifier", "fatalTracing",
		"fatalPostgres", "fatalMigrations", "fatalSubjectVerifier", "fatalSubjectTrust",
		"fatalSynthesisProfile", "fatalPostgresVectors", "fatalVectorGeneration", "fatalPinecone",
		"fatalEmbedder", "fatalLLMClient", "fatalS3", "fatalQueryComponents", "fatalReranker",
		"fatalHostname", "fatalOCR", "fatalAPIListen", "fatalMetricsListen", "fatalServe",
	}, stages)
}

func TestRunBoundsEarlyFatalErrors(t *testing.T) {
	configureFatalStartupTest(t)
	const configCanary = "config-input-canary"
	const fileCanary = "missing-catalog-canary.json"
	t.Setenv("NO_EVIDENCE_CATALOG_FILE", filepath.Join(t.TempDir(), fileCanary))
	t.Run("grants configuration", func(t *testing.T) {
		t.Setenv("WORKER_QUEUE_SIZE", configCanary)
		require.EqualError(t, run(), "startup: configuration: operation failed")
	})
	t.Run("service configuration", func(t *testing.T) {
		t.Setenv("AUTHZ_MODE", "service")
		t.Setenv("WORKER_QUEUE_SIZE", configCanary)
		err := run()
		require.ErrorContains(t, err, "startup: configuration error:")
		require.ErrorContains(t, err, configCanary)
	})
	t.Run("grants catalog", func(t *testing.T) {
		require.EqualError(t, run(), "startup: no-evidence catalog: operation failed")
	})
	for _, mode := range []string{"service", "", "unset"} {
		t.Run("service catalog "+mode, func(t *testing.T) {
			t.Setenv("AUTHZ_MODE", mode)
			if mode == "unset" {
				require.NoError(t, os.Unsetenv("AUTHZ_MODE"))
			}
			err := run()
			require.ErrorContains(t, err, "startup: NO_EVIDENCE_CATALOG_FILE:")
			require.ErrorContains(t, err, fileCanary)
		})
	}
	t.Run("invalid mode", func(t *testing.T) {
		t.Setenv("AUTHZ_MODE", " service ")
		require.EqualError(t, run(), "startup: configuration: operation failed")
	})
}

func TestRunBoundsPostgresOpenFailure(t *testing.T) {
	const child = "QUARRY_TEST_POSTGRES_FATAL_CHILD"
	if os.Getenv(child) == "1" {
		main()
		return
	}
	configureFatalStartupTest(t)
	const canary = "postgres-uri-canary"
	dsn := url.URL{Scheme: "postgres", Host: canary + ".invalid", RawQuery: "sslmode=invalid"}
	t.Setenv("DATABASE_URL", dsn.String())

	for _, mode := range []string{"grants", "service"} {
		t.Run(mode, func(t *testing.T) {
			t.Setenv("AUTHZ_MODE", mode)
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			cmd := testBinaryCommand(ctx, t, "TestRunBoundsPostgresOpenFailure")
			cmd.Env = append(os.Environ(), child+"=1")
			var stderr bytes.Buffer
			cmd.Stderr = &stderr
			var exit *exec.ExitError
			require.ErrorAs(t, cmd.Run(), &exit)
			require.Equal(t, 1, exit.ExitCode(), stderr.String())

			const prefix = "server error: "
			require.Equal(t, 1, strings.Count(stderr.String(), prefix), stderr.String())
			var message string
			for line := range strings.SplitSeq(stderr.String(), "\n") {
				var record struct {
					Message string `json:"msg"`
				}
				if json.Unmarshal([]byte(line), &record) == nil {
					if strings.HasPrefix(record.Message, prefix) {
						message = record.Message
					}
				} else if _, detail, ok := strings.Cut(line, prefix); ok {
					message = prefix + detail
				}
			}
			if mode == "grants" {
				require.Equal(t, "server error: startup: postgres: operation failed", message)
				require.NotContains(t, stderr.String(), canary)
			} else {
				require.True(t, strings.HasPrefix(message, "server error: startup: postgres: "), message)
				require.Contains(t, message, canary)
			}
		})
	}
}

// configureFatalStartupTest sets a grants configuration that can reach startup
// without any external provider calls. Tests set their own database or listener.
func configureFatalStartupTest(t *testing.T) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "startup-trust.json")
	trust, err := json.Marshal(struct {
		ExpiresAt time.Time         `json:"expires_at"`
		Keys      []jose.JSONWebKey `json:"keys"`
	}{
		ExpiresAt: time.Now().Add(5 * time.Minute),
		Keys:      []jose.JSONWebKey{{Key: key.Public(), KeyID: "startup-test"}},
	})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, trust, 0o600))

	issuer := url.URL{Scheme: "https", Host: "startup.example"}
	provider := url.URL{Scheme: "http", Host: "127.0.0.1:1"}
	for name, value := range map[string]string{
		"AUTHZ_MODE": "grants", "AUTHZ_ISSUER": issuer.String(), "AUTHZ_AUDIENCE": "startup-test",
		"AUTHZ_JWKS_FILE": path, "SERVICE_TOKEN": "synthetic-startup-token", "SERVICE_TOKENS": "",
		"DATABASE_URL": "host=unused.invalid dbname=startup", "VECTOR_STORE": "postgres", "VECTOR_GENERATION": "",
		"INGEST_CONTENT_DEDUP": "off", "GO_RAG_FAKE_MODE": "false", "EVALUATION_COLLECTION_PREFIX": "",
		"OPENAI_BASE_URL": provider.String(), "OPENAI_API_KEY": "synthetic-startup-key",
		"OPENAI_CHAT_MODEL": "chat", "OPENAI_EMBEDDING_MODEL": "embed",
		"EMBEDDING_PROVIDER": "openai", "LLM_PROVIDER": "openai",
		"OTEL_TRACES_EXPORTER": "none", "OTEL_SDK_DISABLED": "true", "LANGSMITH_TRACING": "false",
		"OTEL_INSTRUMENTATION_GENAI_CAPTURE_MESSAGE_CONTENT": "false",
		"NO_EVIDENCE_CATALOG_FILE":                           "", "CITATION_SUPPORT_MODEL": "",
		"CITATION_SUPPORT_ENDPOINT": "", "CITATION_SUPPORT_TOKEN": "", "CITATION_SUPPORT_CALIBRATION_FILE": "",
		"S3_BUCKET": "", "RERANK_MODEL_ARN": "", "OCR_PROVIDER": "none", "METRICS_ADDR": "",
	} {
		t.Setenv(name, value)
	}
}
