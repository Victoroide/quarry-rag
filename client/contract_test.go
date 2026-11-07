package client_test

import (
	"context"
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/Victoroide/quarry-rag/client"
	"github.com/Victoroide/quarry-rag/internal/api/handler"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type summaryStore struct {
	domain.ChunkStore
	docs []domain.DocumentSummary
}

func (s summaryStore) StreamDocuments(context.Context, func(domain.DocumentSummary) error) error {
	return nil
}

func (s summaryStore) ListDocumentsPage(context.Context, string, int) ([]domain.DocumentSummary, error) {
	return s.docs, nil
}

func (s summaryStore) GetDocumentSummary(context.Context, domain.DocumentID) (*domain.DocumentSummary, error) {
	return &s.docs[0], nil
}

func TestDocumentPageRoundTrip(t *testing.T) {
	doc := domain.DocumentSummary{ID: "00000000-0000-4000-8000-000000000001", DocType: "guide", Collection: "tenant-a", Status: "completed", ChunkCount: 3}
	server := httptest.NewServer(handler.ListDocumentsHandler(summaryStore{docs: []domain.DocumentSummary{doc}}))
	defer server.Close()
	c, err := client.New(server.URL)
	require.NoError(t, err)
	page, err := c.ListDocuments(t.Context(), client.ListOptions{Limit: 1})
	require.NoError(t, err)
	require.Len(t, page.Documents, 1)
	encoded, err := json.Marshal(page.Documents[0])
	require.NoError(t, err)
	expected, err := json.Marshal(doc)
	require.NoError(t, err)
	assert.JSONEq(t, string(expected), string(encoded))
	assert.Equal(t, 1, page.Count)
	require.NotNil(t, page.NextAfter)
	assert.Equal(t, doc.ID, *page.NextAfter)
}

func TestDocumentStatusRoundTrip(t *testing.T) {
	for _, collection := range []string{"", "tenant-a"} {
		t.Run(collection, func(t *testing.T) {
			doc := domain.DocumentSummary{ID: "00000000-0000-4000-8000-000000000001", Collection: collection, Status: "processing"}
			mux := http.NewServeMux()
			mux.HandleFunc("GET /documents/{doc_id}", handler.DocumentStatusHandler(summaryStore{docs: []domain.DocumentSummary{doc}}))
			server := httptest.NewServer(mux)
			defer server.Close()
			c, err := client.New(server.URL)
			require.NoError(t, err)
			summary, err := c.GetDocument(t.Context(), doc.ID)
			require.NoError(t, err)
			encoded, err := json.Marshal(summary)
			require.NoError(t, err)
			var fields map[string]any
			require.NoError(t, json.Unmarshal(encoded, &fields))
			assert.Equal(t, collection, fields["collection"])
			assert.Equal(t, doc.ID, summary.ID)
			assert.Equal(t, "processing", summary.Status)
		})
	}
}

func TestJSONContract(t *testing.T) {
	for _, pair := range []struct{ local, remote any }{
		{client.IngestRequest{}, handler.IngestRequest{}},
		{client.IngestResponse{}, handler.IngestResponse{}},
		{client.QueryRequest{}, handler.QueryRequest{}},
		{client.QueryResponse{}, handler.QueryResponse{}},
		{client.ScoredChunkResponse{}, handler.ScoredChunkResponse{}},
		{client.TokenUsage{}, domain.TokenUsage{}},
		{client.DocumentSummary{}, domain.DocumentSummary{}},
		{client.DeleteResponse{}, handler.DeleteResponse{}},
		{client.FileURL{}, handler.FileResponse{}},
		{client.ErrorResponse{}, handler.APIError{}},
	} {
		t.Run(reflect.TypeOf(pair.local).Name(), func(t *testing.T) {
			compareJSONTypes(t, reflect.TypeOf(pair.local), reflect.TypeOf(pair.remote))
		})
	}
	t.Run("ExtractionErrorResponse", func(t *testing.T) {
		// The extraction envelope is anonymous; read its declared type from the source.
		file, err := parser.ParseFile(token.NewFileSet(), "../internal/api/handler/extract_errors.go", nil, 0)
		require.NoError(t, err)
		var declarations []*ast.StructType
		ast.Inspect(file, func(node ast.Node) bool {
			if s, ok := node.(*ast.StructType); ok {
				declarations = append(declarations, s)
			}
			return true
		})
		require.Len(t, declarations, 1)
		var fields []reflect.StructField
		for _, f := range declarations[0].Fields.List {
			kind, ok := f.Type.(*ast.Ident)
			require.True(t, ok)
			require.Equal(t, "string", kind.Name)
			require.Len(t, f.Names, 1)
			tag, err := strconv.Unquote(f.Tag.Value)
			require.NoError(t, err)
			fields = append(fields, reflect.StructField{Name: f.Names[0].Name, Type: reflect.TypeFor[string](), Tag: reflect.StructTag(tag)})
		}
		compareJSONTypes(t, reflect.TypeFor[client.ExtractionErrorResponse](), reflect.StructOf(fields))
	})
}

func compareJSONTypes(t *testing.T, local, remote reflect.Type) {
	t.Helper()
	require.Equal(t, remote.Kind(), local.Kind())
	switch local.Kind() {
	case reflect.Pointer, reflect.Slice, reflect.Array:
		compareJSONTypes(t, local.Elem(), remote.Elem())
	case reflect.Map:
		compareJSONTypes(t, local.Key(), remote.Key())
		compareJSONTypes(t, local.Elem(), remote.Elem())
	case reflect.Struct:
		fields := func(typ reflect.Type) map[string]reflect.StructField {
			result := map[string]reflect.StructField{}
			for i := range typ.NumField() {
				f := typ.Field(i)
				if f.IsExported() && f.Tag.Get("json") != "-" {
					result[strings.Split(f.Tag.Get("json"), ",")[0]] = f
				}
			}
			return result
		}
		left, right := fields(local), fields(remote)
		require.Len(t, left, len(right), "JSON fields must match in both directions")
		for name, r := range right {
			l, ok := left[name]
			require.True(t, ok, "missing JSON field %s", name)
			require.Equal(t, r.Tag.Get("json"), l.Tag.Get("json"))
			compareJSONTypes(t, l.Type, r.Type)
		}
	}
}
