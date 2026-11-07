package main

import (
	"context"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
)

var directoryDescriptions = map[string]string{
	"client":           "Go client for the HTTP API",
	"cmd/quarry-mcp":   "Local MCP server over stdio",
	"internal/tracing": "OpenTelemetry export and span helpers",
	"cmd":              "Executable entry points", "cmd/server": "Wiring and graceful shutdown",
	"docs": "Service documentation", "docs/diagrams": "Architecture, schema and layout", "docs/diagrams/src": "Diagram placement sources",
	"internal": "Private service packages", "internal/api": "HTTP router", "internal/api/handler": "Request handlers", "internal/api/middleware": "Authentication, logs and metrics",
	"internal/awsconf": "AWS credentials and retry policy", "internal/chunker": "Text windows and fingerprints", "internal/cleanup": "Document deletion across stores", "internal/config": "Configuration and validation",
	"internal/domain": "Shared types and interfaces", "internal/embedder": "Titan V2 and offline embeddings", "internal/extractor": "File parsing and process isolation", "internal/llm": "Answer routing and Converse",
	"internal/logctx": "Request log attributes", "internal/metrics": "Prometheus text metrics", "internal/pipeline": "Ingestion and query stages", "internal/retriever": "LOCAL and GLOBAL retrieval",
	"internal/store": "Storage adapters", "internal/store/memory": "Offline vector index", "internal/store/pinecone": "Hosted vector index", "internal/store/postgres": "Documents, chunks and durable jobs", "internal/store/s3": "Original files and signed downloads",
	"internal/testutil": "Test support", "internal/testutil/pgtest": "Disposable database containers", "internal/textutil": "Unicode normalization", "internal/worker": "Leased ingestion and retry handling",
	"scripts": "Operational commands", "scripts/cleanup": "Delete selected documents", "scripts/langeval": "Answer language and latency checks", "scripts/reconcile": "Find and remove orphan vectors", "scripts/simulate": "Exercise the service API", "scripts/validate": "Check retrieval behavior",
	"test": "Service test suites", "test/integration": "Database and API integration", "test/smoke": "Opt-in live service checks", "test/unit": "Package behavior checks",
	"tools": "Documentation support", "tools/diagrams": "Vector image sources and checks", "tools/diagrams/fonts": "Bundled typefaces and license",
}

func repositoryDirs(root string, depth int, descriptions map[string]string) ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "git", "ls-files", "-z")
	cmd.Dir = root
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("tracked files: %w", err)
	}
	set := map[string]bool{}
	for _, file := range strings.Split(string(out), "\x00") {
		if file == "" {
			continue
		}
		if _, err := os.Stat(filepath.Join(root, file)); os.IsNotExist(err) {
			continue
		} else if err != nil {
			return nil, err
		}
		parts := strings.Split(filepath.ToSlash(filepath.Dir(file)), "/")
		excluded := false
		for _, part := range parts {
			if part == ".git" || part == ".github" || part == "vendor" || part == "testdata" {
				excluded = true
			}
		}
		if excluded {
			continue
		}
		for i := 1; i <= len(parts) && i <= depth; i++ {
			dir := strings.Join(parts[:i], "/")
			if dir != "." {
				set[dir] = true
			}
		}
	}
	var dirs []string
	for dir := range set {
		if descriptions[dir] == "" {
			return nil, fmt.Errorf("directory %s has no description", dir)
		}
		dirs = append(dirs, dir)
	}
	slices.Sort(dirs)
	for p := range descriptions {
		if !slices.Contains(dirs, p) {
			return nil, fmt.Errorf("description refers to absent directory %s", p)
		}
	}
	return dirs, nil
}

func registeredRoutes(src []byte) ([]string, error) {
	f, err := parser.ParseFile(token.NewFileSet(), "router.go", src, 0)
	if err != nil {
		return nil, err
	}
	var routes []string
	ast.Inspect(f, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || (sel.Sel.Name != "Handle" && sel.Sel.Name != "HandleFunc") || len(call.Args) == 0 {
			return true
		}
		lit, ok := call.Args[0].(*ast.BasicLit)
		if !ok || lit.Kind != token.STRING {
			err = fmt.Errorf("route pattern must be a string literal")
			return false
		}
		s, e := strconv.Unquote(lit.Value)
		if e != nil {
			err = e
			return false
		}
		routes = append(routes, s)
		return true
	})
	slices.Sort(routes)
	return routes, err
}

func splitSQL(s string) []string {
	var parts []string
	start, depth := 0, 0
	quoted := false
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '\'':
			if quoted && i+1 < len(s) && s[i+1] == '\'' {
				i++
				continue
			}
			quoted = !quoted
		case '(':
			if !quoted {
				depth++
			}
		case ')':
			if !quoted {
				depth--
			}
		case ',':
			if !quoted && depth == 0 {
				parts = append(parts, strings.TrimSpace(s[start:i]))
				start = i + 1
			}
		}
	}
	return append(parts, strings.TrimSpace(s[start:]))
}

func parseColumn(s string) (column, error) {
	fields := strings.Fields(s)
	if len(fields) < 2 {
		return column{}, fmt.Errorf("invalid column %q", s)
	}
	c := column{Name: strings.Trim(fields[0], `"`)}
	rest := strings.TrimSpace(strings.TrimPrefix(s, fields[0]))
	constraint := regexp.MustCompile(`(?i)\s+(?:PRIMARY|NOT|NULL|DEFAULT|REFERENCES|UNIQUE|CHECK|CONSTRAINT|COLLATE|GENERATED)\b`)
	end := len(rest)
	if loc := constraint.FindStringIndex(rest); loc != nil {
		end = loc[0]
	}
	c.Type = strings.ToUpper(strings.Join(strings.Fields(rest[:end]), " "))
	if strings.Contains(strings.ToUpper(rest), "PRIMARY KEY") {
		c.Key = "PK"
	}
	ref := regexp.MustCompile(`(?i)REFERENCES\s+(\w+)\s*\(\s*(\w+)\s*\)`).FindStringSubmatch(rest)
	if ref != nil {
		c.Reference = ref[1] + "." + ref[2]
		if c.Key != "" {
			c.Key += "/"
		}
		c.Key += "FK"
	}
	return c, nil
}

func parseSQL(sql string) (sqlSchema, error) {
	sql = regexp.MustCompile(`(?m)--[^\n]*`).ReplaceAllString(sql, "")
	schema := sqlSchema{}
	create := regexp.MustCompile(`(?i)CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)\s*\(`)
	for _, loc := range create.FindAllStringSubmatchIndex(sql, -1) {
		table := sql[loc[2]:loc[3]]
		start := loc[1]
		depth := 1
		end := start
		quote := false
		for ; end < len(sql) && depth > 0; end++ {
			if sql[end] == '\'' {
				if quote && end+1 < len(sql) && sql[end+1] == '\'' {
					end++
					continue
				}
				quote = !quote
			}
			if !quote {
				if sql[end] == '(' {
					depth++
				}
				if sql[end] == ')' {
					depth--
				}
			}
		}
		if depth != 0 {
			return nil, fmt.Errorf("unclosed table %s", table)
		}
		schema[table] = map[string]column{}
		for _, part := range splitSQL(sql[start : end-1]) {
			upper := strings.ToUpper(strings.TrimSpace(part))
			if strings.HasPrefix(upper, "CONSTRAINT ") || strings.HasPrefix(upper, "PRIMARY KEY") || strings.HasPrefix(upper, "FOREIGN KEY") || strings.HasPrefix(upper, "UNIQUE ") || strings.HasPrefix(upper, "CHECK ") {
				return nil, fmt.Errorf("table constraint needs an explicit parser rule: %s", part)
			}
			c, err := parseColumn(part)
			if err != nil {
				return nil, err
			}
			schema[table][c.Name] = c
		}
	}
	alter := regexp.MustCompile(`(?is)ALTER\s+TABLE\s+(\w+)\s+([^;]+)`)
	for _, m := range alter.FindAllStringSubmatch(sql, -1) {
		for _, part := range splitSQL(m[2]) {
			add := regexp.MustCompile(`(?is)^ADD\s+COLUMN\s+(?:IF\s+NOT\s+EXISTS\s+)?(.+)$`).FindStringSubmatch(part)
			drop := regexp.MustCompile(`(?i)^DROP\s+COLUMN\s+(?:IF\s+EXISTS\s+)?(\w+)`).FindStringSubmatch(part)
			cols, ok := schema[m[1]]
			if !ok {
				return nil, fmt.Errorf("alteration of unknown table %s", m[1])
			}
			if add == nil && drop == nil {
				change := regexp.MustCompile(`(?is)^ALTER\s+COLUMN\s+(\w+)\s+(?:SET\s+DATA\s+)?TYPE\s+(.+)$`).FindStringSubmatch(part)
				if change != nil {
					c, ok := cols[change[1]]
					if !ok {
						return nil, fmt.Errorf("type change of unknown column %s", change[1])
					}
					c.Type = strings.ToUpper(strings.Join(strings.Fields(change[2]), " "))
					cols[change[1]] = c
					continue
				}
				if regexp.MustCompile(`(?is)^ALTER\s+COLUMN\s+\w+\s+(?:(?:SET\s+DEFAULT\s+.+)|(?:DROP\s+DEFAULT)|(?:(?:SET|DROP)\s+NOT\s+NULL))$`).MatchString(part) {
					continue
				}
				return nil, fmt.Errorf("unsupported table alteration: %s", part)
			}
			if add != nil {
				c, err := parseColumn(add[1])
				if err != nil {
					return nil, err
				}
				cols[c.Name] = c
			} else {
				delete(cols, drop[1])
			}
		}
	}
	return schema, nil
}

func checkSources(root string, ds []diagram) (err error) {
	rootFS, err := os.OpenRoot(root)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, rootFS.Close()) }()
	paths, err := fs.Glob(rootFS.FS(), "internal/store/postgres/migrations/*.sql")
	if err != nil {
		return err
	}
	if len(paths) == 0 {
		return fmt.Errorf("no SQL migrations")
	}
	var sql strings.Builder
	for _, p := range paths {
		b, readErr := rootFS.ReadFile(p)
		if readErr != nil {
			return readErr
		}
		sql.Write(b)
		sql.WriteByte('\n')
	}
	actual, err := parseSQL(sql.String())
	if err != nil {
		return err
	}
	declared := sqlSchema{}
	var routes []string
	routePattern := regexp.MustCompile(`^(?:GET|POST|PUT|PATCH|DELETE|HEAD|OPTIONS) /`)
	for _, d := range ds {
		for _, n := range d.Nodes {
			if d.Name == "database-schema" {
				declared[n.ID] = map[string]column{}
				for _, c := range n.Columns {
					declared[n.ID][c.Name] = c
				}
			}
			if d.Name == "system-architecture" {
				for _, line := range n.Lines {
					if routePattern.MatchString(line) {
						if n.Prefix != "" {
							method, path, _ := strings.Cut(line, " ")
							path = n.Prefix + strings.TrimSuffix(path, "/")
							routes = append(routes, method+" "+path)
						} else {
							routes = append(routes, line)
						}
					}
				}
			}
		}
	}
	if !reflect.DeepEqual(actual, declared) {
		return fmt.Errorf("SQL columns, types or keys differ from the data model\nactual: %v\ndeclared: %v", actual, declared)
	}
	src, err := rootFS.ReadFile("internal/api/router.go")
	if err != nil {
		return err
	}
	actualRoutes, err := registeredRoutes(src)
	if err != nil {
		return err
	}
	slices.Sort(routes)
	if !slices.Equal(routes, actualRoutes) {
		return fmt.Errorf("diagram routes %v differ from registered routes %v", routes, actualRoutes)
	}
	_, err = repositoryDirs(root, 3, directoryDescriptions)
	return err
}
