package integration

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/api"
	"github.com/Victoroide/quarry-rag/internal/api/handler"
	"github.com/Victoroide/quarry-rag/internal/cleanup"
	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/store/memory"
	pgstore "github.com/Victoroide/quarry-rag/internal/store/postgres"
	"github.com/Victoroide/quarry-rag/internal/testutil/pgtest"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

const preUpgradeCommit = "4e5fa7cbdfecb1c4fb61521ada0ad78e9f8cf84c"

func applyPreDeletionMigrations(t *testing.T, c *pgstore.Client) {
	t.Helper()
	root, err := os.OpenRoot("../../internal/store/postgres/migrations")
	require.NoError(t, err)
	defer func() { require.NoError(t, root.Close()) }()
	entries, err := os.ReadDir("../../internal/store/postgres/migrations")
	require.NoError(t, err)
	_, err = c.Pool.Exec(t.Context(), `CREATE TABLE schema_migrations (
		version INTEGER PRIMARY KEY, name TEXT NOT NULL, checksum TEXT NOT NULL,
		applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW())`)
	require.NoError(t, err)
	for _, entry := range entries {
		prefix, _, _ := strings.Cut(entry.Name(), "_")
		version, parseErr := strconv.Atoi(prefix)
		require.NoError(t, parseErr)
		if version > 9 {
			break
		}
		body, readErr := root.ReadFile(entry.Name())
		require.NoError(t, readErr)
		_, err = c.Pool.Exec(t.Context(), string(body))
		require.NoError(t, err)
		_, err = c.Pool.Exec(t.Context(), `INSERT INTO schema_migrations(version,name,checksum) VALUES($1,$2,$3)`,
			version, entry.Name(), fmt.Sprintf("%x", sha256.Sum256(body)))
		require.NoError(t, err)
	}
}

func repoRoot(t *testing.T) string {
	t.Helper()
	out, err := exec.CommandContext(t.Context(), "git", "rev-parse", "--show-toplevel").CombinedOutput()
	require.NoError(t, err, string(out))
	return strings.TrimSpace(string(out))
}

// buildPreUpgradeRouter checks out the pinned pre-upgrade revision into a
// temporary directory and compiles its real HTTP router and Pinecone adapter.
func buildPreUpgradeRouter(t *testing.T) string {
	t.Helper()
	root := repoRoot(t)
	check := exec.CommandContext(t.Context(), "git", "-C", root, "cat-file", "-e", preUpgradeCommit+"^{commit}") // #nosec G204 -- pinned commit hash
	if err := check.Run(); err != nil {
		fetch := exec.CommandContext(t.Context(), "git", "-C", root, "fetch", "--no-tags", "--depth=1", "origin", preUpgradeCommit) // #nosec G204 -- pinned commit hash
		out, fetchErr := fetch.CombinedOutput()
		require.NoError(t, fetchErr, string(out))
	}
	srcDir := t.TempDir()
	archiveCmd := exec.CommandContext(t.Context(), "git", "-C", root, "archive", preUpgradeCommit) // #nosec G204 -- pinned commit hash
	var tarBuf bytes.Buffer
	archiveCmd.Stdout = &tarBuf
	var errBuf bytes.Buffer
	archiveCmd.Stderr = &errBuf
	require.NoError(t, archiveCmd.Run(), errBuf.String())

	extractCmd := exec.CommandContext(t.Context(), "tar", "-x", "-C", srcDir) // #nosec G204 -- temp directory created by test
	extractCmd.Stdin = &tarBuf
	out, err := extractCmd.CombinedOutput()
	require.NoError(t, err, string(out))

	exportSrc := `package pinecone

import (
	"context"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"google.golang.org/protobuf/types/known/structpb"
)

func PreUpgradeDriver(filterDelete func(context.Context, *structpb.Struct) error) domain.VectorDeleter {
	conn := &fakeIndexConn{filterDelete: filterDelete}
	client := &Client{
		defaultConn: conn,
		dial:        func(string) (indexConn, error) { return conn, nil },
		batchSize:   100,
		timeout:     time.Second,
		maxAttempts: 1,
		dims:        8,
	}
	return NewDriver(client)
}
`
	routerSrc := `package pinecone_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/api"
	"github.com/Victoroide/quarry-rag/internal/config"
	pc "github.com/Victoroide/quarry-rag/internal/store/pinecone"
	pg "github.com/Victoroide/quarry-rag/internal/store/postgres"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestPreUpgradeRouterProcess(t *testing.T) {
	id := os.Getenv("PREUPGRADE_TARGET")
	require.NotEmpty(t, id)
	db, err := pg.NewClient(t.Context(), os.Getenv("PREUPGRADE_DB"))
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close(context.Background())) }()
	calls := 0
	driver := pc.PreUpgradeDriver(func(_ context.Context, filter *structpb.Struct) error {
		calls++
		require.Equal(t, id, filter.GetFields()["doc_id"].GetStructValue().GetFields()["$eq"].GetStringValue())
		require.Len(t, filter.GetFields(), 1)
		if calls == 2 {
			body, marshalErr := filter.MarshalJSON()
			require.NoError(t, marshalErr)
			require.NoError(t, os.WriteFile(os.Getenv("PREUPGRADE_FILTER"), body, 0o600))
			if os.Getenv("PREUPGRADE_CODE") == "500" {
				return status.Error(codes.DeadlineExceeded, "recorded operation; no reply")
			}
		}
		return nil
	})
	router := api.NewRouter(&api.RouterDeps{ChunkStore: db, PineconeDriver: driver, Config: &config.Config{ServiceToken: "preupgrade-token"}})
	req := httptest.NewRequestWithContext(t.Context(), http.MethodDelete, "/documents/"+id, nil)
	req.Header.Set("X-Service-Token", "preupgrade-token")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	want, err := strconv.Atoi(os.Getenv("PREUPGRADE_CODE"))
	require.NoError(t, err)
	require.Equal(t, want, rec.Code, rec.Body.String())
	require.Equal(t, 2, calls)
}
`
	pcDir := filepath.Join(srcDir, "internal", "store", "pinecone")
	require.NoError(t, os.WriteFile(filepath.Join(pcDir, "preupgrade_export_test.go"), []byte(exportSrc), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(pcDir, "preupgrade_router_test.go"), []byte(routerSrc), 0o600))

	binPath := filepath.Join(t.TempDir(), "preupgrade-router")
	buildCmd := exec.CommandContext(t.Context(), "go", "test", "-c", "-o", binPath, "./internal/store/pinecone") // #nosec G204 -- temp build directory
	buildCmd.Dir = srcDir
	buildOut, err := buildCmd.CombinedOutput()
	require.NoError(t, err, string(buildOut))
	return binPath
}

func buildCleanupBinary(t *testing.T) string {
	t.Helper()
	binPath := filepath.Join(t.TempDir(), "quarry-cleanup")
	cmd := exec.CommandContext(t.Context(), "go", "build", "-o", binPath, "../../scripts/cleanup") // #nosec G204 -- repo cleanup binary
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, string(out))
	return binPath
}

func runPreUpgradeDelete(t *testing.T, binPath, dbURL, id string, wantCode int) string {
	t.Helper()
	filterFile := filepath.Join(t.TempDir(), "filter.json")
	cmd := exec.CommandContext(t.Context(), binPath, "-test.run=^TestPreUpgradeRouterProcess$", "-test.v", "-test.timeout=30s") // #nosec G204 -- compiled test binary
	cmd.Env = append(os.Environ(),
		"PREUPGRADE_DB="+dbURL,
		"PREUPGRADE_TARGET="+id,
		"PREUPGRADE_CODE="+strconv.Itoa(wantCode),
		"PREUPGRADE_FILTER="+filterFile,
	)
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, string(out))
	data, err := os.ReadFile(filterFile) // #nosec G304 -- file path created in t.TempDir()
	require.NoError(t, err)
	var filter map[string]map[string]string
	require.NoError(t, json.Unmarshal(data, &filter))
	require.Equal(t, map[string]map[string]string{"doc_id": {"$eq": id}}, filter)
	return filter["doc_id"]["$eq"]
}

// runCleanupCLI runs the cleanup executable and returns only its standard
// output, where it prints its JSON result. Startup logs go to standard error,
// which is kept apart and appended to the error so a failure stays diagnosable.
func runCleanupCLI(t *testing.T, binPath, dbURL, vectorStore, indexName string, args ...string) ([]byte, error) {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), binPath, args...) // #nosec G204 -- compiled cleanup binary
	cmd.Env = append(os.Environ(),
		"DATABASE_URL="+dbURL,
		"VECTOR_STORE="+vectorStore,
		"PINECONE_INDEX_NAME="+indexName,
		"GO_RAG_FAKE_MODE=false",
		"S3_BUCKET=",
		"AWS_REGION=us-east-1",
		"SERVICE_TOKEN=upgrade-cli",
	)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		return stdout.Bytes(), fmt.Errorf("%w: %s", err, stderr.String())
	}
	return stdout.Bytes(), nil
}

func TestRunCleanupCLIParsesOnlyStandardOutput(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("the stand-in executable is a POSIX shell script")
	}
	dir := t.TempDir()
	script := filepath.Join(dir, "cleanup")
	body := "#!/bin/sh\n" +
		"echo '2026/11/26 12:00:00 WARN service tokens shorter than 32 bytes' >&2\n" +
		"echo '2026/11/26 12:00:00 INFO postgres connected' >&2\n" +
		"echo '{\"held\":[]}'\n" +
		"if [ \"$1\" = \"--fail\" ]; then echo 'precondition failed' >&2; exit 3; fi\n"
	require.NoError(t, os.WriteFile(script, []byte(body), 0o700)) // #nosec G306 -- executable stand-in in t.TempDir()

	out, err := runCleanupCLI(t, script, "unused", "postgres", "", "--list-held")
	require.NoError(t, err)
	var parsed struct {
		Held []domain.HeldDeletion `json:"held"`
	}
	require.NoError(t, json.Unmarshal(out, &parsed), string(out))
	require.Empty(t, parsed.Held)

	out, err = runCleanupCLI(t, script, "unused", "postgres", "", "--fail")
	require.Error(t, err)
	require.Contains(t, err.Error(), "precondition failed", "stderr stays in the failure message")
	require.NotContains(t, string(out), "WARN", "stderr never reaches the parsed output")
}

func TestRetiredProviderMutationWaitsForUUIDQuarantine(t *testing.T) {
	if testing.Short() {
		t.Skip("requires a Docker daemon")
	}
	preUpgradeBin := buildPreUpgradeRouter(t)
	cleanupBin := buildCleanupBinary(t)

	for _, priorCompatibleReceipt := range []bool{false, true} {
		name := "case_a_missing_receipt_on_009"
		if priorCompatibleReceipt {
			name = "case_b_unfinished_legacy_receipt_row_removed"
		}
		t.Run(name, func(t *testing.T) {
			ctx := t.Context()
			dbURL := pgtest.StartVector(t)
			db, err := pgstore.NewClient(ctx, dbURL)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, db.Close(context.Background())) })
			applyPreDeletionMigrations(t, db)

			id := uuid.NewString()
			_, err = db.Pool.Exec(ctx, `INSERT INTO documents(id,content,status) VALUES($1,'old hosted evidence','completed')`, id)
			require.NoError(t, err)

			var expectedIncarnation string
			if priorCompatibleReceipt {
				require.NoError(t, db.RunMigrations(ctx))
				_, err = pgstore.NewVectorStore(ctx, db, 3)
				require.NoError(t, err)
				expectedIncarnation = storedIncarnation(t, db, id)
				stores := newDeletionStores()
				stores.hook("namespace", func(context.Context) error {
					return errors.New("namespace call failed before removal")
				})
				compatServer := deletionServer(t, db, "compat-token", deletionRoutes{
					vectors: stores,
					objects: stores,
					signer:  stores,
					options: []handler.DeleteOption{
						handler.WithLegacyReservations(),
						handler.WithCleanupTargets(cleanup.Targets{VectorIndex: "idx-upgrade"}),
					},
				})
				require.Equal(t, http.StatusInternalServerError, sendDelete(t, compatServer, "compat-token", id, "").code)
				r := receiptOf(t, db, id)
				require.False(t, r.finalized)
				require.True(t, r.held)
			}

			// Spawn the actual pre-upgrade router and Pinecone adapter in a separate
			// process: it deletes the metadata row and fails on the final vector filter.
			recordedFilterTarget := runPreUpgradeDelete(t, preUpgradeBin, dbURL, id, http.StatusInternalServerError)
			require.Equal(t, id, recordedFilterTarget)
			require.Empty(t, documentState(t, db, id))

			vectors := memory.NewFakeVectorStore()
			require.NoError(t, vectors.Upsert(ctx, "", []domain.Chunk{{
				ID: "old-vector", DocumentID: domain.DocumentID(id), Embedding: []float32{1},
			}}))

			if !priorCompatibleReceipt {
				require.NoError(t, db.RunMigrations(ctx))
				_, err = pgstore.NewVectorStore(ctx, db, 3)
				require.NoError(t, err)
			}

			// Run the actual quarantine command twice and confirm identical recovery pairs.
			type quarantineResp struct {
				Quarantined []domain.HeldDeletion `json:"quarantined"`
			}
			out1, err := runCleanupCLI(t, cleanupBin, dbURL, "postgres", "", "--quarantine", id)
			require.NoError(t, err, string(out1))
			var q1 quarantineResp
			require.NoError(t, json.Unmarshal(out1, &q1), string(out1))
			require.Len(t, q1.Quarantined, 1)

			out2, err := runCleanupCLI(t, cleanupBin, dbURL, "postgres", "", "--quarantine", id)
			require.NoError(t, err, string(out2))
			var q2 quarantineResp
			require.NoError(t, json.Unmarshal(out2, &q2), string(out2))
			require.Equal(t, q1.Quarantined[0], q2.Quarantined[0], "repeated quarantine preserves the same pair and receipt")

			inc := q1.Quarantined[0].Incarnation
			if priorCompatibleReceipt {
				require.Equal(t, expectedIncarnation, inc, "unfinished legacy receipt incarnation must be preserved")
				require.Equal(t, "legacy_reservation", q1.Quarantined[0].Kind)
				require.Equal(t, "idx-upgrade", q1.Quarantined[0].VectorIndex)
			} else {
				require.Equal(t, "quarantine", q1.Quarantined[0].Kind)
			}

			listOut, listErr := runCleanupCLI(t, cleanupBin, dbURL, "postgres", "", "--list-held")
			require.NoError(t, listErr, string(listOut))
			var listed struct {
				Held []domain.HeldDeletion `json:"held"`
			}
			require.NoError(t, json.Unmarshal(listOut, &listed), string(listOut))
			require.Len(t, listed.Held, 1)
			require.Equal(t, inc, listed.Held[0].Incarnation)
			require.False(t, listed.Held[0].DocumentPresent)

			// New admission is blocked with 409 while the old filter is outstanding.
			activateDeletion(t, db)
			router := api.NewRouter(&api.RouterDeps{
				ChunkStore:     db,
				VectorStore:    vectors,
				PineconeDriver: vectors,
				Config:         &config.Config{ServiceToken: "upgrade-service", FakeMode: false},
			})
			server := httptest.NewServer(router)
			t.Cleanup(server.Close)
			blocked := doRequest(t, server, http.MethodPost, "/ingest", "upgrade-service", nil,
				`{"document_id":"`+id+`","content":"blocked replacement"}`)
			require.Equal(t, http.StatusConflict, blocked.code, blocked.body)

			// Apply the recorded old filter, then verify wrong-token release fails and right-token releases.
			require.NoError(t, vectors.DeleteByDocID(ctx, recordedFilterTarget))
			remainingOld, err := vectors.QueryAllNamespaces(ctx, []float32{1}, 10, "")
			require.NoError(t, err)
			require.Empty(t, remainingOld)

			_, wrongErr := runCleanupCLI(t, cleanupBin, dbURL, "postgres", "", "--release-held", id, uuid.NewString())
			require.Error(t, wrongErr)

			if priorCompatibleReceipt {
				// Case B recorded vector_index="idx-upgrade"; verify legacy release cleans
				// without issuing any broad DeleteAllVectorsInNamespace / DeleteByDocID.
				require.NoError(t, db.ReleaseHeldDeletion(ctx, domain.DocumentID(id), inc, domain.DeleteOperations{
					VectorIndex: "idx-upgrade",
					LegacyNamespace: func(context.Context, string) error {
						t.Fatal("legacy release must issue zero document-wide namespace deletes")
						return nil
					},
					LegacyVectors: func(context.Context, string) error {
						t.Fatal("legacy release must issue zero document-wide vector deletes")
						return nil
					},
				}))
			} else {
				relOut, relErr := runCleanupCLI(t, cleanupBin, dbURL, "postgres", "", "--release-held", id, inc)
				require.NoError(t, relErr, string(relOut))
			}

			// Reuse now returns 202 with a new incarnation and preserves its replacement vectors.
			admitted := doRequest(t, server, http.MethodPost, "/ingest", "upgrade-service", nil,
				`{"document_id":"`+id+`","content":"fresh replacement"}`)
			require.Equal(t, http.StatusAccepted, admitted.code, admitted.body)
			fresh := storedIncarnation(t, db, id)
			require.NotEqual(t, inc, fresh)
			require.NoError(t, vectors.Upsert(ctx, "", []domain.Chunk{{
				ID: "replacement-vector", DocumentID: domain.DocumentID(id), Embedding: []float32{1},
				Metadata: map[string]any{domain.VectorRevisionKey: fresh},
			}}))
			require.Equal(t, http.StatusPreconditionRequired, sendDelete(t, server, "upgrade-service", id, "").code)
			matches, err := vectors.QueryAllNamespaces(ctx, []float32{1}, 10, "")
			require.NoError(t, err)
			require.Len(t, matches, 1)
			require.Equal(t, fresh, matches[0].Chunk.Metadata[domain.VectorRevisionKey])
		})
	}
}

func TestCaseBSourceFailureAndWrongTokenKeepHold(t *testing.T) {
	if testing.Short() {
		t.Skip("requires a Docker daemon")
	}
	ctx := t.Context()
	db := durableDatabase(t)
	id := uuid.NewString()
	key := "source/" + id + "/original.txt"
	acceptDeletable(t, db, id, "source legacy doc", false, key)
	inc := storedIncarnation(t, db, id)

	_, err := db.Pool.Exec(ctx, `INSERT INTO document_deletions(document_id,incarnation,kind,source_key,source_bucket,vector_mode,vector_index,step,held,finalized)
		VALUES($1,$2,'legacy_reservation',$3,'originals','legacy','idx-1',0,TRUE,FALSE)`, id, inc, key)
	require.NoError(t, err)
	_, err = db.Pool.Exec(ctx, `DELETE FROM documents WHERE id=$1`, id)
	require.NoError(t, err)

	// Missing S3, source delete error, and wrong incarnation keep the UUID held.
	require.ErrorIs(t, db.ReleaseHeldDeletion(ctx, domain.DocumentID(id), inc, domain.DeleteOperations{
		SourceBucket: "originals", VectorIndex: "idx-1",
	}), domain.ErrFileStorageUnavailable)
	require.ErrorContains(t, db.ReleaseHeldDeletion(ctx, domain.DocumentID(id), inc, domain.DeleteOperations{
		SourceBucket: "originals", VectorIndex: "idx-1",
		Source: func(context.Context, string) error { return errors.New("s3 timeout") },
	}), "s3 timeout")
	require.ErrorIs(t, db.ReleaseHeldDeletion(ctx, domain.DocumentID(id), uuid.NewString(), domain.DeleteOperations{
		SourceBucket: "originals", VectorIndex: "idx-1",
		Source: func(context.Context, string) error { return nil },
	}), domain.ErrDeletionPreconditionFailed)

	// Verify receipt stays unfinished and held, and retry succeeds cleanly without broad vector calls.
	r := receiptOf(t, db, id)
	require.False(t, r.finalized)
	require.True(t, r.held)

	var sourceCalls int
	require.NoError(t, db.ReleaseHeldDeletion(ctx, domain.DocumentID(id), inc, domain.DeleteOperations{
		SourceBucket: "originals", VectorIndex: "idx-1",
		LegacyNamespace: func(context.Context, string) error {
			t.Fatal("legacy release must not call LegacyNamespace")
			return nil
		},
		LegacyVectors: func(context.Context, string) error {
			t.Fatal("legacy release must not call LegacyVectors")
			return nil
		},
		Source: func(_ context.Context, target string) error {
			require.Equal(t, key, target)
			sourceCalls++
			return nil
		},
	}))
	require.Equal(t, 1, sourceCalls)
	r = receiptOf(t, db, id)
	require.True(t, r.finalized)
	require.False(t, r.held)
	require.Equal(t, domain.AcceptAccepted, admission(t, db, id, "replacement after source cleanup"))
}
