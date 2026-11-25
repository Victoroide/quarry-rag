package integration

import (
	"bytes"
	"context"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/api"
	"github.com/Victoroide/quarry-rag/internal/api/handler"
	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/extractor"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	if len(os.Args) == 2 && os.Args[1] == extractor.ChildArg && os.Getenv(extractor.ChildEnv) == "1" {
		os.Exit(extractor.RunChild(os.Stdin, os.Stdout))
	}
	os.Exit(m.Run())
}

func docStoryUpload(t *testing.T, data []byte, fields map[string]string) *http.Request {
	t.Helper()
	var body bytes.Buffer
	w := multipart.NewWriter(&body)
	part, err := w.CreateFormFile("file", "stories.doc")
	require.NoError(t, err)
	_, err = part.Write(data)
	require.NoError(t, err)
	for k, v := range fields {
		require.NoError(t, w.WriteField(k, v))
	}
	require.NoError(t, w.Close())
	r := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/ingest", &body)
	r.Header.Set("Content-Type", w.FormDataContentType())
	return r
}

func TestDOCStoryProfilesPreserveIngestIdentity(t *testing.T) {
	c := durableDatabase(t)
	data, err := os.ReadFile("../../internal/extractor/testdata/stories.doc")
	require.NoError(t, err)
	objects := &privateUploadObjects{saved: make(map[string]string)}
	profile := func(value string) http.HandlerFunc {
		stories, parseErr := extractor.ParseDOCStories(value)
		require.NoError(t, parseErr)
		return handler.IngestHandler(nil, c, objects, 1<<20, handler.WithExtractor(func(ctx context.Context, ft extractor.FileType, source []byte) (string, error) {
			return extractor.NewWithDOCStories(ft, stories).Extract(ctx, bytes.NewReader(source))
		}))
	}
	const body = "Body €  !"
	responses := parallelIngest(profile("body"),
		docStoryUpload(t, data, map[string]string{"doc_type": "manual"}),
		docStoryUpload(t, data, map[string]string{"doc_type": "record"}),
		docStoryUpload(t, data, map[string]string{"doc_type": "manual", "collection": "archive"}),
	)
	original := privateResponse(t, responses[0], http.StatusAccepted).DocumentID
	require.Equal(t, original, privateResponse(t, responses[1], http.StatusAccepted).DocumentID)
	archive := privateResponse(t, responses[2], http.StatusAccepted).DocumentID
	require.NotEqual(t, original, archive)
	var content, label string
	require.NoError(t, c.Pool.QueryRow(t.Context(), `SELECT content,doc_type FROM documents WHERE id=$1`, original).Scan(&content, &label))
	require.Equal(t, body, content)
	require.Contains(t, []string{"manual", "record"}, label)
	originalKey, err := c.GetDocumentS3Key(t.Context(), domain.DocumentID(original))
	require.NoError(t, err)
	require.NotNil(t, originalKey)
	require.Equal(t, string(data), objects.saved[*originalKey])
	require.Len(t, objects.saved, 2)
	for range 2 {
		job, err := c.ClaimJob(t.Context(), "doc-stories", time.Minute, 3)
		require.NoError(t, err)
		require.NotNil(t, job)
		completed, err := c.CompleteJob(t.Context(), *job, nil, domain.IndexObservation{})
		require.NoError(t, err)
		require.True(t, completed)
	}

	// The indexed note has no readable text. The profile change keeps the fingerprint.
	rec := httptest.NewRecorder()
	profile("body,footnotes").ServeHTTP(rec, docStoryUpload(t, data, nil))
	require.Equal(t, handler.IngestResponse{DocumentID: original, Status: "already_ingested"}, privateResponse(t, rec, http.StatusOK))
	require.Len(t, objects.saved, 2)

	withComments := profile("body,comments")
	rec = httptest.NewRecorder()
	withComments.ServeHTTP(rec, docStoryUpload(t, data, map[string]string{"document_id": original}))
	require.Equal(t, http.StatusConflict, rec.Code, rec.Body.String())
	objects.fail = true
	rec = httptest.NewRecorder()
	withComments.ServeHTTP(rec, docStoryUpload(t, data, nil))
	require.Equal(t, http.StatusServiceUnavailable, rec.Code, rec.Body.String())
	require.Equal(t, string(data), objects.saved[*originalKey])
	require.Len(t, objects.saved, 2)
	objects.fail = false
	rec = httptest.NewRecorder()
	withComments.ServeHTTP(rec, docStoryUpload(t, data, nil))
	replacement := privateResponse(t, rec, http.StatusAccepted).DocumentID
	require.NotEqual(t, original, replacement)
	require.Equal(t, string(data), objects.saved[*originalKey])
	require.Len(t, objects.saved, 3)
	for id, want := range map[string]string{original: body, archive: body, replacement: body + "\n\nComments\nPrivate comment"} {
		require.NoError(t, c.Pool.QueryRow(t.Context(), `SELECT content FROM documents WHERE id=$1`, id).Scan(&content))
		require.Equal(t, want, content)
	}
}

func TestDOCStoryProfileThroughRouter(t *testing.T) {
	c := durableDatabase(t)
	data, err := os.ReadFile("../../internal/extractor/testdata/stories.doc")
	require.NoError(t, err)
	t.Setenv("GO_RAG_FAKE_MODE", "true")
	t.Setenv("DATABASE_URL", "host=localhost dbname=rag")
	t.Setenv("DOC_STORIES", "body,headers")
	for name, isolated := range map[string]string{"in-process": "false", "isolated": "true"} {
		t.Run(name, func(t *testing.T) {
			t.Setenv("EXTRACT_ISOLATION", isolated)
			cfg, err := config.Load()
			require.NoError(t, err)
			router := api.NewRouter(&api.RouterDeps{ChunkStore: c, Config: cfg})
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, docStoryUpload(t, data, map[string]string{"collection": name}))
			id := privateResponse(t, rec, http.StatusAccepted).DocumentID
			var content string
			require.NoError(t, c.Pool.QueryRow(t.Context(), `SELECT content FROM documents WHERE id=$1`, id).Scan(&content))
			require.Equal(t, "Body €  !\n\nHeaders\n頭頁 Header", content)
		})
	}
}

func TestDOCStoryProducerUploads(t *testing.T) {
	c := durableDatabase(t)
	for _, tc := range []struct{ name, option, label string }{
		{"footnote", "footnotes", "Footnotes"},
		{"endingnote", "endnotes", "Endnotes"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data, err := os.ReadFile("../../internal/extractor/testdata/poi/" + tc.name + ".doc")
			require.NoError(t, err)
			var body string
			for _, profile := range []string{"body", tc.option} {
				stories, parseErr := extractor.ParseDOCStories(profile)
				require.NoError(t, parseErr)
				router := api.NewRouter(&api.RouterDeps{ChunkStore: c, Config: &config.Config{
					ExtractIsolation: true, DOCStories: stories, MaxUploadBytes: 1 << 20,
				}})
				rec := httptest.NewRecorder()
				router.ServeHTTP(rec, docStoryUpload(t, data, map[string]string{"collection": tc.name + "-" + profile}))
				id := privateResponse(t, rec, http.StatusAccepted).DocumentID
				var content string
				require.NoError(t, c.Pool.QueryRow(t.Context(), `SELECT content FROM documents WHERE id=$1`, id).Scan(&content))
				heading := "\n\n" + tc.label + "\n"
				if profile == "body" {
					body = content
					require.NotEmpty(t, body)
					require.NotContains(t, content, heading)
				} else {
					require.NotEmpty(t, strings.TrimPrefix(content, body+heading))
					require.True(t, strings.HasPrefix(content, body+heading), "selected text must start with the unchanged body and heading")
				}
			}
		})
	}
}
