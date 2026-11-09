package api

import (
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/api/handler"
	"github.com/Victoroide/quarry-rag/internal/api/middleware"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v3"
)

func readOpenAPI(t *testing.T) map[string]any {
	t.Helper()
	data, err := os.ReadFile("../../docs/openapi.yaml")
	require.NoError(t, err)
	var doc map[string]any
	require.NoError(t, yaml.Unmarshal(data, &doc))
	require.Equal(t, "3.1.0", doc["openapi"])
	return doc
}

func openAPIObject(t *testing.T, value any) map[string]any {
	t.Helper()
	object, ok := value.(map[string]any)
	require.True(t, ok, "expected object, got %T", value)
	return object
}

func openAPISchemas(t *testing.T, doc map[string]any) map[string]any {
	t.Helper()
	return openAPIObject(t, openAPIObject(t, doc["components"])["schemas"])
}

func parseAPISource(t *testing.T, path string) *ast.File {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
	require.NoError(t, err)
	return file
}

func literalString(t *testing.T, expr ast.Expr) string {
	t.Helper()
	literal, ok := expr.(*ast.BasicLit)
	require.True(t, ok, "expected string literal, got %T", expr)
	value, err := strconv.Unquote(literal.Value)
	require.NoError(t, err)
	return value
}

func TestOpenAPIRoutes(t *testing.T) {
	doc := readOpenAPI(t)
	paths := openAPIObject(t, doc["paths"])
	codeRoutes := map[string]bool{}
	codePaths := map[string]bool{}
	ast.Inspect(parseAPISource(t, "router.go"), func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || (selector.Sel.Name != "Handle" && selector.Sel.Name != "HandleFunc") {
			return true
		}
		require.Len(t, call.Args, 2)
		pattern := literalString(t, call.Args[0])
		method, path, found := strings.Cut(pattern, " ")
		require.True(t, found, "route must declare a method: %s", pattern)
		require.NotContains(t, codeRoutes, pattern, "duplicate router registration")
		protected := false
		ast.Inspect(call.Args[1], func(node ast.Node) bool {
			if selector, ok := node.(*ast.SelectorExpr); ok && selector.Sel.Name == "RequireServiceTokens" {
				protected = true
			}
			return true
		})
		codeRoutes[pattern] = protected
		codePaths[path] = true
		require.Contains(t, paths, path, "%s missing from OpenAPI", pattern)
		pathItem := openAPIObject(t, paths[path])
		require.Contains(t, pathItem, strings.ToLower(method), "%s missing from OpenAPI", pattern)
		operation := openAPIObject(t, pathItem[strings.ToLower(method)])
		security := []any{}
		if protected {
			security = []any{map[string]any{"ServiceToken": []any{}}}
		}
		require.Equal(t, security, operation["security"], "%s authentication", pattern)
		return true
	})
	specRoutes := map[string]bool{}
	for path, value := range paths {
		for key := range openAPIObject(t, value) {
			if slices.Contains([]string{"get", "put", "post", "delete", "options", "head", "patch", "trace"}, key) {
				pattern := strings.ToUpper(key) + " " + path
				specRoutes[pattern] = true
			}
		}
	}
	require.NotEmpty(t, codeRoutes)
	require.Equal(t, slices.Sorted(maps.Keys(codePaths)), slices.Sorted(maps.Keys(paths)), "router and OpenAPI paths differ")
	require.Equal(t, slices.Sorted(maps.Keys(codeRoutes)), slices.Sorted(maps.Keys(specRoutes)), "router and OpenAPI routes differ")
	security := openAPIObject(t, openAPIObject(t, doc["components"])["securitySchemes"])
	require.Equal(t, map[string]any{"type": "apiKey", "in": "header", "name": "X-Service-Token"}, security["ServiceToken"])
}

func TestOpenAPIMiddlewareErrorEnvelope(t *testing.T) {
	properties := openAPIObject(t, openAPIObject(t, openAPISchemas(t, readOpenAPI(t))["APIError"])["properties"])
	recorder := httptest.NewRecorder()
	middleware.RequireServiceTokens([]string{"token"}, nil)(recorder, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/documents", http.NoBody))
	var body map[string]any
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &body))
	require.ElementsMatch(t, slices.Collect(maps.Keys(properties)), slices.Collect(maps.Keys(body)), "middleware error envelope differs")
}

func TestOpenAPIJSONFields(t *testing.T) {
	schemas := openAPISchemas(t, readOpenAPI(t))
	types := map[string]reflect.Type{
		"HealthStatus":        reflect.TypeFor[handler.HealthStatus](),
		"IngestRequest":       reflect.TypeFor[handler.IngestRequest](),
		"IngestResponse":      reflect.TypeFor[handler.IngestResponse](),
		"QueryRequest":        reflect.TypeFor[handler.QueryRequest](),
		"QueryResponse":       reflect.TypeFor[handler.QueryResponse](),
		"ScoredChunkResponse": reflect.TypeFor[handler.ScoredChunkResponse](),
		"CitationResponse":    reflect.TypeFor[handler.CitationResponse](),
		"TokenUsage":          reflect.TypeFor[domain.TokenUsage](),
		"DocumentSummary":     reflect.TypeFor[domain.DocumentSummary](),
		"DocumentRef":         reflect.TypeFor[domain.DocumentRef](),
		"DeleteResponse":      reflect.TypeFor[handler.DeleteResponse](),
		"FileResponse":        reflect.TypeFor[handler.FileResponse](),
		"APIError":            reflect.TypeFor[handler.APIError](),
	}
	for name, typ := range types {
		t.Run(name, func(t *testing.T) {
			schema := openAPIObject(t, schemas[name])
			properties := openAPIObject(t, schema["properties"])
			fields := map[string]reflect.Type{}
			nullable := map[string]bool{}
			required := []string{}
			for field := range typ.Fields() {
				if field.IsExported() {
					fieldName := strings.Split(field.Tag.Get("json"), ",")[0]
					if fieldName == "" {
						fieldName = field.Name
					}
					if fieldName != "-" {
						fields[fieldName] = field.Type
						nullable[fieldName] = field.Type.Kind() == reflect.Pointer && !strings.Contains(field.Tag.Get("json"), ",omit")
						if !strings.Contains(field.Tag.Get("json"), ",omit") {
							required = append(required, fieldName)
						}
					}
				}
			}
			require.Equal(t, slices.Sorted(maps.Keys(fields)), slices.Sorted(maps.Keys(properties)), "%s JSON fields differ", name)
			switch name {
			case "IngestRequest":
				required = []string{"content"}
			case "QueryRequest":
				required = []string{"query"}
			}
			require.ElementsMatch(t, required, schema["required"], "%s required fields differ", name)
			for field, typ := range fields {
				property := openAPIObject(t, properties[field])
				checkOpenAPIType(t, schemas, property, typ)
				if typ.Kind() != reflect.Map && typ.Kind() != reflect.Slice {
					types, _ := property["type"].([]any)
					require.Equal(t, nullable[field], slices.Contains(types, "null"), "%s.%s nullability", name, field)
				}
			}
		})
	}

	inline := map[string]struct{ file, function string }{
		"ProbeStatus":     {"handler/health.go", "writeProbeStatus"},
		"ExtractionError": {"handler/extract_errors.go", "writeExtractionError"},
		"DocumentList":    {"handler/document_listing.go", "listDocumentSummaries"},
	}
	for name, source := range inline {
		fields := []string{}
		ast.Inspect(sourceFunction(t, source.file, source.function), func(node ast.Node) bool {
			if typ, ok := node.(*ast.StructType); ok {
				for _, field := range typ.Fields.List {
					require.NotNil(t, field.Tag)
					tag := reflect.StructTag(literalString(t, field.Tag))
					fields = append(fields, strings.Split(tag.Get("json"), ",")[0])
				}
			}
			return true
		})
		require.NotEmpty(t, fields)
		properties := openAPIObject(t, openAPIObject(t, schemas[name])["properties"])
		require.ElementsMatch(t, fields, slices.Collect(maps.Keys(properties)), "%s inline JSON fields differ", name)
	}

	// New handler payload types must be associated with a schema as well.
	files, err := filepath.Glob("handler/*.go")
	require.NoError(t, err)
	for _, path := range files {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		ast.Inspect(parseAPISource(t, path), func(node ast.Node) bool {
			spec, ok := node.(*ast.TypeSpec)
			if !ok {
				return true
			}
			if typ, ok := spec.Type.(*ast.StructType); ok {
				for _, field := range typ.Fields.List {
					if field.Tag != nil && reflect.StructTag(literalString(t, field.Tag)).Get("json") != "" {
						require.Contains(t, types, spec.Name.Name, "add reflection coverage for handler payload")
					}
				}
			}
			return true
		})
	}
	for name, value := range schemas {
		if _, object := openAPIObject(t, value)["properties"]; object {
			_, named := types[name]
			_, anonymous := inline[name]
			require.True(t, named || anonymous || name == "MultipartIngestRequest", "schema %s has no matching Go payload", name)
		}
	}
}

func sourceFunction(t *testing.T, path, name string) *ast.FuncDecl {
	t.Helper()
	for _, decl := range parseAPISource(t, path).Decls {
		if fn, ok := decl.(*ast.FuncDecl); ok && fn.Name.Name == name {
			return fn
		}
	}
	t.Fatalf("function %s missing from %s", name, path)
	return nil
}

func checkOpenAPIType(t *testing.T, schemas, schema map[string]any, typ reflect.Type) {
	t.Helper()
	if typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	if ref, ok := schema["$ref"].(string); ok {
		require.Equal(t, "#/components/schemas/"+typ.Name(), ref)
		require.Contains(t, schemas, typ.Name())
		return
	}
	if typ == reflect.TypeFor[time.Time]() {
		require.Equal(t, "date-time", schema["format"])
		typ = reflect.TypeFor[string]()
	}
	want := ""
	switch typ.Kind() {
	case reflect.String:
		want = "string"
	case reflect.Bool:
		want = "boolean"
	case reflect.Int, reflect.Int32, reflect.Int64:
		want = "integer"
	case reflect.Float64:
		want = "number"
	case reflect.Map:
		want = "object"
		if typ.Elem().Kind() != reflect.Interface {
			checkOpenAPIType(t, schemas, openAPIObject(t, schema["additionalProperties"]), typ.Elem())
		}
	case reflect.Slice:
		want = "array"
		checkOpenAPIType(t, schemas, openAPIObject(t, schema["items"]), typ.Elem())
	default:
		t.Fatalf("schema must reference Go type %s", typ)
	}
	if values, ok := schema["type"].([]any); ok {
		require.Contains(t, values, want)
	} else {
		require.Equal(t, want, schema["type"], "%s schema type", typ)
	}
}

func TestOpenAPIMultipartFields(t *testing.T) {
	fields := map[string]bool{}
	ast.Inspect(sourceFunction(t, "handler/ingest.go", "handleFileIngest"), func(node ast.Node) bool {
		switch expr := node.(type) {
		case *ast.CallExpr:
			if sel, ok := expr.Fun.(*ast.SelectorExpr); ok && slices.Contains([]string{"FormFile", "FormValue", "PostFormValue"}, sel.Sel.Name) {
				fields[literalString(t, expr.Args[0])] = true
			}
		case *ast.IndexExpr:
			if sel, ok := expr.X.(*ast.SelectorExpr); ok && (sel.Sel.Name == "Value" || sel.Sel.Name == "File") {
				fields[literalString(t, expr.Index)] = true
			}
		}
		return true
	})
	schemas := openAPISchemas(t, readOpenAPI(t))
	properties := openAPIObject(t, openAPIObject(t, schemas["MultipartIngestRequest"])["properties"])
	require.Equal(t, slices.Sorted(maps.Keys(fields)), slices.Sorted(maps.Keys(properties)), "multipart fields differ")
}

func TestOpenAPIReferences(t *testing.T) {
	doc := readOpenAPI(t)
	var visit func(any)
	visit = func(value any) {
		switch value := value.(type) {
		case map[string]any:
			if ref, ok := value["$ref"].(string); ok {
				require.True(t, strings.HasPrefix(ref, "#/"), "only local references are supported")
				var target any = doc
				for _, segment := range strings.Split(strings.TrimPrefix(ref, "#/"), "/") {
					target = openAPIObject(t, target)[segment]
					require.NotNil(t, target, "unresolved reference %s", ref)
				}
			}
			for _, child := range value {
				visit(child)
			}
		case []any:
			for _, child := range value {
				visit(child)
			}
		}
	}
	visit(doc)
}

func TestOpenAPIPayloadBindings(t *testing.T) {
	doc := readOpenAPI(t)
	paths := openAPIObject(t, doc["paths"])
	for _, route := range []struct {
		method, path, request, response string
		statuses                        []string
	}{
		{"get", "/health", "", "HealthStatus", []string{"200", "503"}},
		{"get", "/livez", "", "ProbeStatus", []string{"200"}},
		{"get", "/readyz", "", "ProbeStatus", []string{"200", "503"}},
		{"post", "/ingest", "IngestRequest", "IngestResponse", []string{"200", "202"}},
		{"post", "/query", "QueryRequest", "QueryResponse", []string{"200"}},
		{"get", "/documents", "", "DocumentList", []string{"200"}},
		{"get", "/documents/{doc_id}", "", "DocumentSummary", []string{"200"}},
		{"delete", "/documents/{doc_id}", "", "DeleteResponse", []string{"200"}},
		{"get", "/documents/{doc_id}/file", "", "FileResponse", []string{"200"}},
	} {
		t.Run(route.method+" "+route.path, func(t *testing.T) {
			path := openAPIObject(t, paths[route.path])
			operation := openAPIObject(t, path[route.method])
			require.Contains(t, path["parameters"], map[string]any{"$ref": "#/components/parameters/RequestID"})
			if strings.Contains(route.path, "{doc_id}") {
				require.Contains(t, path["parameters"], map[string]any{"$ref": "#/components/parameters/DocumentID"})
			}
			if route.request != "" {
				body := openAPIObject(t, operation["requestBody"])
				require.Equal(t, true, body["required"])
				checkOpenAPIContent(t, body, route.request)
				if route.path == "/ingest" {
					content := openAPIObject(t, body["content"])
					media := openAPIObject(t, content["multipart/form-data"])
					require.Equal(t, map[string]any{"$ref": "#/components/schemas/MultipartIngestRequest"}, media["schema"])
				}
			} else {
				require.NotContains(t, operation, "requestBody")
			}
			responses := openAPIObject(t, operation["responses"])
			for _, status := range route.statuses {
				response := resolveOpenAPIObject(t, doc, responses[status])
				checkOpenAPIContent(t, response, route.response)
			}
			if route.path == "/query" {
				parameters, ok := operation["parameters"].([]any)
				require.True(t, ok, "query must document Accept-Language")
				var acceptLanguage map[string]any
				for _, value := range parameters {
					parameter := resolveOpenAPIObject(t, doc, value)
					if parameter["name"] == "Accept-Language" && parameter["in"] == "header" {
						acceptLanguage = parameter
					}
				}
				require.NotNil(t, acceptLanguage)
				require.Equal(t, "string", openAPIObject(t, acceptLanguage["schema"])["type"])
				response := resolveOpenAPIObject(t, doc, responses["200"])
				headers := openAPIObject(t, response["headers"])
				contentLanguage := resolveOpenAPIObject(t, doc, headers["Content-Language"])
				require.Equal(t, "string", openAPIObject(t, contentLanguage["schema"])["type"])
			}
			for _, value := range responses {
				response := resolveOpenAPIObject(t, doc, value)
				headers := openAPIObject(t, response["headers"])
				require.Equal(t, map[string]any{"$ref": "#/components/headers/RequestID"}, headers["X-Request-ID"])
			}
		})
	}
}

func checkOpenAPIContent(t *testing.T, object map[string]any, name string) {
	t.Helper()
	content := openAPIObject(t, object["content"])
	media := openAPIObject(t, content["application/json"])
	require.Equal(t, map[string]any{"$ref": "#/components/schemas/" + name}, media["schema"])
}

func resolveOpenAPIObject(t *testing.T, doc map[string]any, value any) map[string]any {
	t.Helper()
	object := openAPIObject(t, value)
	if ref, ok := object["$ref"].(string); ok {
		for _, segment := range strings.Split(strings.TrimPrefix(ref, "#/"), "/") {
			doc = openAPIObject(t, doc[segment])
		}
		return doc
	}
	return object
}
