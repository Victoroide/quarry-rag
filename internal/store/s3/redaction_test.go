package s3

import (
	"bytes"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/extractor"
	"github.com/Victoroide/quarry-rag/internal/logctx"
	"github.com/Victoroide/quarry-rag/internal/tracing"
	"github.com/stretchr/testify/require"
)

func TestClientRedactsObjectKeysWhenEnabled(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(server.Close)

	var logs bytes.Buffer
	prevLog := slog.Default()
	slog.SetDefault(slog.New(logctx.NewHandler(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))))
	t.Cleanup(func() { slog.SetDefault(prevLog) })

	client := s3EndpointClient(t, server.URL, "")
	ctx := tracing.WithRedactIdentifiers(t.Context(), true)
	const secretKey = "tenant-a/secret-doc-uuid/incarnation-uuid"

	_, uploadErr := client.Upload(ctx, "secret-doc-uuid/incarnation-uuid", domain.DefaultDocType, "secret-name.txt", extractor.FileTypeText, strings.NewReader("classified"))
	require.Error(t, uploadErr)
	require.NotContains(t, uploadErr.Error(), "secret-doc-uuid")
	require.NotContains(t, uploadErr.Error(), "secret-name")

	delErr := client.Delete(ctx, secretKey)
	require.Error(t, delErr)
	require.NotContains(t, delErr.Error(), secretKey)

	require.NotContains(t, logs.String(), "secret-doc-uuid")
	require.NotContains(t, logs.String(), secretKey)
}
