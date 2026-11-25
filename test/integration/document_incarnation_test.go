package integration

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/api"
	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	pgstore "github.com/Victoroide/quarry-rag/internal/store/postgres"
	"github.com/google/jsonschema-go/jsonschema"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v4"
	"go.yaml.in/yaml/v4/plugin/mergekey"
)

func TestLegacyDocumentSummarySchema(t *testing.T) {
	c := durableDatabase(t)
	id := uuid.NewString()
	_, err := c.Pool.Exec(t.Context(), `INSERT INTO documents(id,content,collection) VALUES($1,'archive content','archive')`, id)
	require.NoError(t, err)
	legacy, err := os.ReadFile("testdata/legacy_document_schemas.yaml")
	require.NoError(t, err)
	var schemas map[string]any
	require.NoError(t, yaml.Load(legacy, &schemas))
	openAPI, err := os.ReadFile("../../docs/openapi.yaml")
	require.NoError(t, err)
	var published struct {
		Paths map[string]struct {
			Get struct {
				Responses map[string]struct {
					Content map[string]struct{ Schema map[string]any }
				}
			}
		}
		Components struct{ Schemas map[string]any }
	}
	require.NoError(t, yaml.Load(openAPI, &published, yaml.WithPlugin(mergekey.New(mergekey.WithDisable(true)))))
	router := incarnationRouter(c)
	for _, tc := range []struct {
		name, path, route, legacySchema string
		paged                           bool
	}{
		{"status", "/documents/" + id, "/documents/{doc_id}", "DocumentSummary", false},
		{"stream", "/documents?collection=archive", "/documents", "DocumentList", false},
		{"page", "/documents?collection=archive&limit=1", "/documents", "DocumentList", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			legacyValidator := documentResponseValidator(t, schemas, map[string]any{"$ref": "#/components/schemas/" + tc.legacySchema})
			contract := published.Paths[tc.route].Get.Responses["200"].Content["application/json"].Schema
			require.NotEmpty(t, contract)
			publishedValidator := documentResponseValidator(t, published.Components.Schemas, contract)
			for _, option := range []string{"", "include_incarnation=false", "include_incarnation=True", "include_incarnation=true&include_incarnation=false",
				"include_index_visibility=false", "include_index_visibility=True", "include_index_visibility=true&include_index_visibility=true"} {
				rec := incarnationRead(t, router, withQuery(tc.path, option), "incarnation-reader")
				require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
				var body map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
				require.NoError(t, legacyValidator.Validate(body), tc.path)
				require.NoError(t, publishedValidator.Validate(body), tc.path)
				if tc.legacySchema == "DocumentList" {
					require.Len(t, body["documents"], 1)
					_, hasCursor := body["next_after"]
					require.Equal(t, tc.paged, hasCursor)
				}
			}
			rec := incarnationRead(t, router, withQuery(tc.path, "include_incarnation=true"), "incarnation-reader")
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			var extended map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &extended))
			require.Error(t, legacyValidator.Validate(extended), "the legacy schema must reject the opt-in field")
			require.NoError(t, publishedValidator.Validate(extended), "the published schema must accept the opt-in body")
			for _, option := range []string{"include_index_visibility=true", "include_index_visibility=true&include_incarnation=true"} {
				rec := incarnationRead(t, router, withQuery(tc.path, option), "incarnation-reader")
				require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
				var observed map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &observed))
				require.Error(t, legacyValidator.Validate(observed), "the legacy schema must reject opted-in visibility")
				require.NoError(t, publishedValidator.Validate(observed))
				if tc.legacySchema == "DocumentList" {
					var page struct {
						Documents []map[string]any `json:"documents"`
					}
					require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &page))
					require.Len(t, page.Documents, 1)
					observed = page.Documents[0]
				}
				require.Equal(t, "unknown", observed["index_visibility"])
				require.NotContains(t, observed, "index_visibility_observed_at")
			}
		})
	}
}

func documentResponseValidator(t *testing.T, schemas, contract map[string]any) *jsonschema.Resolved {
	t.Helper()
	root, err := json.Marshal(map[string]any{"$defs": schemas, "allOf": []any{contract}})
	require.NoError(t, err)
	var schema jsonschema.Schema
	require.NoError(t, json.Unmarshal([]byte(strings.ReplaceAll(string(root), "#/components/schemas/", "#/$defs/")), &schema))
	validator, err := schema.Resolve(nil)
	require.NoError(t, err)
	return validator
}

func TestDocumentIncarnationIsSeparateFromHTTPRepresentation(t *testing.T) {
	c := durableDatabase(t)
	ctx := t.Context()
	id, other := uuid.NewString(), uuid.NewString()
	result, err := c.AcceptDocument(ctx, domain.AcceptRequest{
		Document:   domain.Document{ID: domain.DocumentID(id), Content: "incarnation status"},
		Collection: "archive", MaxPending: 12,
	})
	require.NoError(t, err)
	require.Equal(t, domain.AcceptAccepted, result.Outcome)
	_, err = c.Pool.Exec(ctx, `INSERT INTO documents(id,content,collection) VALUES($1,'unrelated content','other')`, other)
	require.NoError(t, err)
	paths := []string{
		"/documents/" + id + "?include_incarnation=true",
		"/documents?collection=archive&include_incarnation=true",
		"/documents?collection=archive&limit=1&include_incarnation=true",
	}
	read := func(store *pgstore.Client, state string) (string, string) {
		var identity, validator string
		for i, path := range paths {
			router := incarnationRouter(store)
			unauthenticated := incarnationRead(t, router, path, "")
			require.Equal(t, http.StatusUnauthorized, unauthenticated.Code)
			rec := incarnationRead(t, router, path, "incarnation-reader")
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			var body json.RawMessage = rec.Body.Bytes()
			if i == 0 {
				validator = rec.Header().Get("ETag")
			} else {
				var page struct {
					Documents []json.RawMessage `json:"documents"`
					Count     int               `json:"count"`
					NextAfter *string           `json:"next_after"`
				}
				require.NoError(t, json.Unmarshal(body, &page))
				require.Len(t, page.Documents, 1)
				require.Equal(t, 1, page.Count)
				if i == 2 {
					require.Equal(t, &id, page.NextAfter)
				}
				body = page.Documents[0]
			}
			var summary struct {
				ID          string `json:"document_id"`
				Status      string `json:"status"`
				Incarnation string `json:"incarnation"`
			}
			require.NoError(t, json.Unmarshal(body, &summary))
			require.Equal(t, id, summary.ID)
			require.Equal(t, state, summary.Status)
			if i == 0 {
				_, parseErr := uuid.Parse(summary.Incarnation)
				require.NoError(t, parseErr)
				require.NotEqual(t, id, summary.Incarnation)
				identity = summary.Incarnation
			}
			require.Equal(t, identity, summary.Incarnation)
		}
		return identity, validator
	}
	before, beforeETag := read(c, "processing")
	job, err := c.ClaimJob(ctx, "incarnation-reader", time.Minute, 5)
	require.NoError(t, err)
	require.NotNil(t, job)
	completed, err := c.CompleteJob(ctx, *job, nil, domain.IndexObservation{})
	require.NoError(t, err)
	require.True(t, completed)
	restarted, err := pgstore.NewClient(ctx, c.Pool.Config().ConnString())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, restarted.Close(context.Background())) })
	require.NoError(t, restarted.RunMigrations(ctx))
	after, afterETag := read(restarted, "completed")
	require.Equal(t, before, after)
	require.False(t, beforeETag != "" && beforeETag == afterETag && !strings.HasPrefix(beforeETag, "W/"), "different status bodies shared a strong ETag")

	for _, query := range []string{"collection=archive&include_incarnation=true", "collection=archive&include_incarnation=true&limit=1&after=" + id} {
		rec := incarnationRead(t, incarnationRouter(restarted), "/documents?"+query, "incarnation-reader")
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		require.NotContains(t, rec.Body.String(), other)
		if strings.Contains(query, "after=") {
			require.JSONEq(t, `{"documents":[],"count":0,"next_after":null}`, rec.Body.String())
		}
	}
	rec := incarnationRead(t, incarnationRouter(restarted), paths[0]+"&include_incarnation=%", "incarnation-reader")
	require.Equal(t, http.StatusOK, rec.Code)
	require.NotContains(t, rec.Body.String(), `"incarnation"`)
}

func incarnationRouter(store *pgstore.Client) http.Handler {
	return api.NewRouter(&api.RouterDeps{ChunkStore: store, Config: &config.Config{ServiceToken: "incarnation-reader"}})
}

func incarnationRead(t *testing.T, h http.Handler, path, token string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, path, http.NoBody)
	req.Header.Set("X-Service-Token", token)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func withQuery(path, query string) string {
	if query == "" {
		return path
	}
	if strings.Contains(path, "?") {
		return path + "&" + query
	}
	return path + "?" + query
}
