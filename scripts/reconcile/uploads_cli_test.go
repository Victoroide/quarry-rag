package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/config"
	s3store "github.com/Victoroide/quarry-rag/internal/store/s3"
	"github.com/stretchr/testify/require"
)

func configureSourceUploadStorage(t *testing.T, r *rig, endpoint string) {
	t.Helper()
	t.Setenv("DATABASE_URL", r.pg.Pool.Config().ConnString())
	t.Setenv("GO_RAG_FAKE_MODE", "false")
	t.Setenv("S3_BUCKET", r.bucket.name)
	t.Setenv("S3_EXPECTED_BUCKET_OWNER", "123456789012")
	t.Setenv("AWS_REGION", "us-east-1")
	t.Setenv("AWS_ACCESS_KEY_ID", "fixture-key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "fixture-secret")
	t.Setenv("AWS_SESSION_TOKEN", "")
	t.Setenv("AWS_MAX_ATTEMPTS", "1")
	t.Setenv("AWS_IGNORE_CONFIGURED_ENDPOINT_URLS", "false")
	t.Setenv("AWS_ENDPOINT_URL_S3", endpoint)
}

func TestSourceUploadCommandNeedsOnlyPostgresAndS3(t *testing.T) {
	r := newRig(t, newDatabase(t), "source-upload-command", false)
	stageUnlinked(t, r, 1)
	key := r.bucket.keys()[0]
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Header.Get("x-amz-expected-bucket-owner") != "123456789012" {
			t.Errorf("request missing expected bucket owner: %s %s", req.Method, req.URL)
		}
		switch {
		case req.Method == http.MethodGet && req.URL.Query().Has("versions") && req.URL.Query().Get("prefix") == key:
			w.Header().Set("Content-Type", "application/xml")
			_, _ = fmt.Fprintf(w, "<ListVersionsResult><IsTruncated>false</IsTruncated><Version><Key>%s</Key><VersionId>null</VersionId><Size>10</Size></Version></ListVersionsResult>", key)
		case req.Method == http.MethodHead && req.URL.Query().Get("versionId") == "null":
			w.WriteHeader(http.StatusOK)
		default:
			t.Errorf("unexpected request: %s %s", req.Method, req.URL)
			w.WriteHeader(http.StatusForbidden)
		}
	}))
	t.Cleanup(server.Close)
	configureSourceUploadStorage(t, r, server.URL)
	t.Setenv("VECTOR_STORE", "pinecone")
	t.Setenv("PINECONE_API_KEY", "")
	t.Setenv("PINECONE_INDEX_NAME", "")
	t.Setenv("SERVICE_TOKEN", "")
	t.Setenv("SERVICE_TOKENS", "")

	cmd := exec.CommandContext(t.Context(), "make", "-s", "reconcile", "ARGS=-source-uploads")
	cmd.Dir = "../.."
	var output, logs bytes.Buffer
	cmd.Stdout, cmd.Stderr = &output, &logs
	err := cmd.Run()
	require.NoError(t, err, logs.String())
	var report uploadReport
	require.NoError(t, json.Unmarshal(output.Bytes(), &report))
	require.Equal(t, "inventory", report.Mode)
	require.Len(t, report.Uploads, 1)
	require.Equal(t, key, report.Uploads[0].Key)
	require.Equal(t, actionDelete, report.Uploads[0].Action)
	require.Equal(t, 1, report.Uploads[0].Removals)
	require.Zero(t, report.DeletedVersions)
	require.Equal(t, map[string]int{"staged": 1}, r.states())
}

func TestMissingVersionIDLeavesTheWholeKeyPending(t *testing.T) {
	r := newRig(t, newDatabase(t), "missing-version-id", true)
	stageUnlinked(t, r, 2)
	keys := r.bucket.keys()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		prefix := req.URL.Query().Get("prefix")
		if req.Method != http.MethodGet || !req.URL.Query().Has("versions") || (prefix != keys[0] && prefix != keys[1]) {
			t.Errorf("unexpected request: %s %s", req.Method, req.URL)
			w.WriteHeader(http.StatusForbidden)
			return
		}
		key := keys[0]
		if prefix == keys[1] {
			key = keys[1]
		}
		w.Header().Set("Content-Type", "application/xml")
		_, _ = fmt.Fprintf(w, "<ListVersionsResult><IsTruncated>false</IsTruncated><Version><Key>%s</Key><VersionId>valid-version</VersionId><Size>10</Size></Version>", key)
		if key == keys[0] {
			_, _ = fmt.Fprintf(w, "<DeleteMarker><Key>%s</Key></DeleteMarker>", key)
		} else {
			_, _ = fmt.Fprintf(w, "<Version><Key>%s</Key><Size>10</Size></Version>", key)
		}
		_, _ = fmt.Fprint(w, "</ListVersionsResult>")
	}))
	t.Cleanup(server.Close)
	configureSourceUploadStorage(t, r, server.URL)
	cfg, err := config.LoadSourceUploadMaintenance()
	require.NoError(t, err)
	objects, err := s3store.New(t.Context(), cfg)
	require.NoError(t, err)
	report, err := reconcileSourceUploads(t.Context(), r.pg, objects, r.bucket.name, applyOptions())
	require.NoError(t, err)
	require.Len(t, report.Uploads, 2)
	for _, row := range report.Uploads {
		require.Equal(t, actionPending, row.Action)
		require.Equal(t, reasonVersionIDMissing, row.Reason)
		require.Zero(t, row.Removals)
	}
	require.Zero(t, report.DeletedVersions)
	require.Equal(t, 2, report.Pending)
	require.Len(t, report.Journal, 1)
	require.Equal(t, reasonVersionIDMissing, report.Journal[0].Reason)
}
