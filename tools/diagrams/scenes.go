package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

func buildDiagrams(root string) (result []diagram, err error) {
	rootFS, err := os.OpenRoot(root)
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, rootFS.Close()) }()
	dirs, err := repositoryDirs(root, 3, directoryDescriptions)
	if err != nil {
		return nil, err
	}
	ds := []diagram{architecture(), dataModel(), repositoryTree(dirs)}
	for i := range ds {
		d := &ds[i]
		b, err := rootFS.ReadFile(filepath.Join("docs", "diagrams", "src", d.Name+".json"))
		if err != nil {
			return nil, err
		}
		var p placement
		decoder := json.NewDecoder(strings.NewReader(string(b)))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&p); err != nil {
			return nil, err
		}
		d.W = p.Width
		d.H = p.Height
		d.Groups = p.Groups
		for j := range d.Nodes {
			n := &d.Nodes[j]
			pos, ok := p.Nodes[n.ID]
			if !ok {
				return nil, fmt.Errorf("%s has no placement for %s", d.Name, n.ID)
			}
			n.Box = pos
			if d.Name == "system-architecture" {
				n.Column = fmt.Sprint(math.Round((pos.X - 16) / 208))
				n.Row = fmt.Sprint(math.Round((pos.Y - 260) / 96))
				if pos.Y < 260 {
					n.Row = "entry"
				}
			}
			delete(p.Nodes, n.ID)
		}
		if len(p.Nodes) != 0 {
			return nil, fmt.Errorf("%s has obsolete placements: %v", d.Name, p.Nodes)
		}
	}
	return ds, nil
}

func architecture() diagram {
	d := diagram{Name: "system-architecture", Title: "Quarry service architecture", Duration: 16, MaxW: 840, MaxH: 760,
		Description: "Quarry service architecture. The Go client and local stdio MCP server call the HTTP API. Public probes serve GET /health, GET /livez and GET /readyz. Token-protected routes serve ingestion, queries and document operations. Ingestion normally extracts binary uploads in a sandbox, decodes text uploads in process, optionally saves originals in S3, and accepts documents and durable jobs in PostgreSQL. The ingestion processor claims a job, chunks text, embeds with Bedrock Titan V2, persists vectors in Pinecone, then completes the job and saves chunks in PostgreSQL. Queries decompose, route LOCAL or GLOBAL with Bedrock embeddings, retrieve scoped visible evidence from Pinecone, and synthesize with Bedrock Converse. A representative ingestion span flows through OpenTelemetry to optional OTLP and LangSmith exporters. Document handlers serve GET /documents, GET /documents/{doc_id}, GET /documents/{doc_id}/file and DELETE /documents/{doc_id}."}
	add := func(id, title string, lines ...string) {
		d.Nodes = append(d.Nodes, node{ID: id, Title: title, Lines: lines})
	}
	add("client", "API clients", "Go client · MCP stdio")
	add("router", "HTTP router", "ID · metrics · logs · auth")
	add("probes", "Public probes", "GET /health", "GET /livez", "GET /readyz")
	add("documents", "Document handlers", "GET /", "GET /{doc_id}", "GET /{doc_id}/file", "DELETE /{doc_id}")
	d.Nodes[len(d.Nodes)-1].Prefix = "/documents"
	add("ingest", "IngestHandler", "POST /ingest")
	add("sandbox", "Extraction sandbox", "Binary uploads · isolation")
	add("s3", "Amazon S3", "Original files (optional)")
	add("postgres", "PostgreSQL", "Metadata · chunks · jobs")
	add("job", "ingest_jobs", "Durable queue")
	add("worker", "Ingestion processor", "Lease · retry · complete")
	add("chunk", "ChunkStage", "Text windows")
	add("embed", "EmbedStage", "Chunk embeddings")
	add("bedrock-ingest", "Bedrock embeddings", "Titan Text V2")
	add("persist", "PersistStage", "Default namespace")
	add("pinecone", "Pinecone", "Vector index")
	add("query", "QueryHandler", "POST /query")
	add("decompose", "DecomposeStage", "Multiple needs")
	add("route", "RoutingStage", "Embed · LOCAL / GLOBAL")
	add("retrieve", "RetrievalStage", "Scoped Pinecone search")
	add("synth", "SynthesisStage", "Grounded answer")
	add("converse", "Bedrock Converse", "Answer synthesis")
	add("tracing", "Trace export", "OpenTelemetry · OTLP", "LangSmith (optional)")
	e := func(id, from, to string, points ...point) {
		d.Edges = append(d.Edges, edge{ID: id, From: from, To: to, Points: points})
	}

	e("client-router", "client", "router", point{200, 152}, point{224, 152})
	e("router-probes", "router", "probes", point{408, 152}, point{432, 152})
	e("router-documents", "router", "documents", point{368, 124}, point{368, 68}, point{732, 68}, point{732, 84})
	e("router-ingest", "router", "ingest", point{260, 180}, point{260, 240}, point{108, 240}, point{108, 260})
	e("router-query", "router", "query", point{316, 180}, point{316, 228}, point{524, 228}, point{524, 260})
	e("ingest-sandbox", "ingest", "sandbox", point{200, 288}, point{224, 288})
	e("ingest-s3", "ingest", "s3", point{108, 316}, point{108, 452})
	e("ingest-postgres", "ingest", "postgres", point{160, 316}, point{160, 336}, point{260, 336}, point{260, 452})
	e("postgres-job", "postgres", "job", point{260, 508}, point{260, 528}, point{108, 528}, point{108, 548})
	e("job-worker", "job", "worker", point{200, 576}, point{224, 576})
	e("worker-postgres", "worker", "postgres", point{316, 548}, point{316, 508})
	e("worker-chunk", "worker", "chunk", point{316, 604}, point{316, 644})
	e("chunk-embed", "chunk", "embed", point{408, 672}, point{432, 672})
	e("embed-bedrock", "embed", "bedrock-ingest", point{524, 644}, point{524, 604})
	e("embed-persist", "embed", "persist", point{616, 672}, point{640, 672})
	e("persist-pinecone", "persist", "pinecone", point{732, 644}, point{732, 604})
	e("query-decompose", "query", "decompose", point{616, 288}, point{640, 288})
	e("decompose-route", "decompose", "route", point{684, 316}, point{684, 336}, point{420, 336}, point{420, 480}, point{432, 480})
	e("route-bedrock", "route", "bedrock-ingest", point{524, 508}, point{524, 548})
	e("route-retrieve", "route", "retrieve", point{616, 480}, point{640, 480})
	e("retrieve-pinecone", "retrieve", "pinecone", point{732, 508}, point{732, 548})
	e("retrieve-synth", "retrieve", "synth", point{732, 452}, point{732, 412})
	e("synth-converse", "synth", "converse", point{640, 384}, point{616, 384})
	e("worker-tracing", "worker", "tracing", point{280, 604}, point{280, 624}, point{108, 624}, point{108, 634})
	order := []string{"client-router", "router-ingest", "ingest-sandbox", "ingest-s3", "ingest-postgres", "postgres-job", "job-worker", "worker-chunk", "chunk-embed", "embed-bedrock", "embed-persist", "persist-pinecone", "worker-postgres", "client-router", "router-query", "query-decompose", "decompose-route", "route-bedrock", "route-retrieve", "retrieve-pinecone", "retrieve-synth", "synth-converse"}
	at := 0.1
	for _, id := range order {
		for j := range d.Edges {
			e := &d.Edges[j]
			if e.ID != id {
				continue
			}
			e.Times = append(e.Times, at)
			arrival := at + pulseDuration(edgeLength(*e))
			for k := range d.Nodes {
				n := &d.Nodes[k]
				if n.ID == e.From {
					n.Times = append(n.Times, at)
				}
				if n.ID == e.To {
					n.Times = append(n.Times, arrival+targetHighlightDelay)
				}
			}
			at = arrival + .015
		}
	}

	return d
}

func dataModel() diagram {
	d := diagram{Name: "database-schema", Title: "PostgreSQL data model", Duration: 8, MaxW: 840, MaxH: 1000,
		Description: "Data model. documents contains id, content, metadata, status, chunk_count, created_at, file_name, file_type, s3_key, content_preview, doc_type, fingerprint, last_error, updated_at, completed_at and collection. chunks contains id, document_id, chunk_index, text, token_count and created_at. document_fingerprints contains fingerprint, document_id and created_at. ingest_jobs contains document_id, state, attempts, lease_owner, lease_until, next_attempt_at, last_error, created_at and updated_at. Each child references documents.id with cascading deletion. A document has zero or more chunks and fingerprints and at most one ingestion job. PK denotes primary key and FK denotes foreign key; SQL types appear beside every column. Fingerprints are scoped to the collection; document_fingerprints stores the scoped digest."}
	d.Nodes = []node{
		{ID: "documents", Title: "documents", Times: []float64{0.3, 1.5, 2.7}, Columns: []column{
			{"id", "UUID", "PK", ""}, {"content", "TEXT", "", ""}, {"metadata", "JSONB", "", ""}, {"status", "TEXT", "", ""}, {"chunk_count", "INTEGER", "", ""}, {"created_at", "TIMESTAMPTZ", "", ""}, {"file_name", "TEXT", "", ""}, {"file_type", "TEXT", "", ""}, {"s3_key", "TEXT", "", ""}, {"content_preview", "TEXT", "", ""}, {"doc_type", "TEXT", "", ""}, {"fingerprint", "TEXT", "", ""}, {"last_error", "TEXT", "", ""}, {"updated_at", "TIMESTAMPTZ", "", ""}, {"completed_at", "TIMESTAMPTZ", "", ""}, {"collection", "TEXT", "", ""}}},
		{ID: "chunks", Title: "chunks", Times: []float64{0.6}, Columns: []column{{"id", "UUID", "PK", ""}, {"document_id", "UUID", "FK", "documents.id"}, {"chunk_index", "INTEGER", "", ""}, {"text", "TEXT", "", ""}, {"token_count", "INTEGER", "", ""}, {"created_at", "TIMESTAMPTZ", "", ""}}},
		{ID: "document_fingerprints", Title: "document_fingerprints", Times: []float64{1.8}, Columns: []column{{"fingerprint", "TEXT", "PK", ""}, {"document_id", "UUID", "FK", "documents.id"}, {"created_at", "TIMESTAMPTZ", "", ""}}},
		{ID: "ingest_jobs", Title: "ingest_jobs", Times: []float64{3.0}, Columns: []column{{"document_id", "UUID", "PK/FK", "documents.id"}, {"state", "TEXT", "", ""}, {"attempts", "INTEGER", "", ""}, {"lease_owner", "TEXT", "", ""}, {"lease_until", "TIMESTAMPTZ", "", ""}, {"next_attempt_at", "TIMESTAMPTZ", "", ""}, {"last_error", "TEXT", "", ""}, {"created_at", "TIMESTAMPTZ", "", ""}, {"updated_at", "TIMESTAMPTZ", "", ""}}},
	}
	for _, v := range []struct {
		id, to, card     string
		x, sourceY, y, t float64
	}{
		{"documents-chunks", "chunks", "0..n", 428, 162, 200, 0.3},
		{"documents-fingerprints", "document_fingerprints", "0..n", 416, 174, 478, 1.5},
		{"documents-jobs", "ingest_jobs", "0..1", 404, 186, 644, 2.7},
	} {
		l := &label{Text: v.card, X: 464, Y: v.y - 14, Size: edgeLabelSize, Kind: "muted", Owner: v.id}
		d.Edges = append(d.Edges, edge{ID: v.id, From: "documents", To: v.to, Label: l, Times: []float64{v.t}, Points: []point{{368, v.sourceY}, {v.x, v.sourceY}, {v.x, v.y}, {472, v.y}}})
	}

	for i := range d.Nodes {
		d.Nodes[i].Times = nil
	}
	for _, e := range d.Edges {
		for i := range d.Nodes {
			if d.Nodes[i].ID == e.From {
				d.Nodes[i].Times = append(d.Nodes[i].Times, e.Times...)
			}
			if d.Nodes[i].ID == e.To {
				d.Nodes[i].Times = []float64{e.Times[0] + pulseDuration(edgeLength(e)) + targetHighlightDelay}
			}
		}
	}
	d.Labels = []label{{Text: "PK: primary key · FK: foreign key · 0..n: zero or more · 0..1: zero or one · Parent → child; cascading delete.", X: 16, Y: 918, Size: sublabelSize, Kind: "muted"}}
	return d
}

func repositoryTree(dirs []string) diagram {
	d := diagram{Name: "repository-layout", Title: "Repository layout", Duration: 8, MaxW: 840, MaxH: 1000}
	for _, path := range dirs {
		n := node{ID: path, Title: filepath.Base(path) + "/", Lines: []string{directoryDescriptions[path]}, Borderless: true}
		flow := []string{"cmd/server", "internal/api", "internal/pipeline", "internal/retriever", "internal/llm", "internal/store"}
		if i := slices.Index(flow, path); i >= 0 {
			n.Times = []float64{0.2 + float64(i)*0.6}
		}
		d.Nodes = append(d.Nodes, n)
	}
	var content []string
	for _, n := range d.Nodes {
		content = append(content, n.ID+"/ "+n.Lines[0])
	}
	d.Description = "Repository layout, from the directory listing to depth three. " + strings.Join(content, ". ") + ". The highlight follows cmd/server, internal/api, internal/pipeline, internal/retriever, internal/llm and internal/store."
	return d
}

func sceneLabels(d diagram) []label {
	f, _ := loadFonts()
	labels := []label{{Text: d.Title, X: 16, Y: 32, Size: titleSize, Bold: true}, {Text: map[string]string{"system-architecture": "INGESTION AND QUERY FLOWS", "database-schema": "TABLES, COLUMNS AND REFERENTIAL KEYS", "repository-layout": "DIRECTORY TREE · DEPTH 3"}[d.Name], X: 16, Y: 56, Size: groupSize, Bold: true, Kind: "group"}}
	labels = append(labels, d.Labels...)
	for _, g := range d.Groups {
		labels = append(labels, label{Text: g.Title, X: g.Box.X + 16, Y: g.Box.Y + 24, Size: groupSize, Bold: true, Kind: "group"})
	}
	for _, n := range d.Nodes {
		if d.Name == "repository-layout" {
			title := label{Text: n.Title, X: n.Box.X + 12, Y: n.Box.Y + 21, Size: titleSize, Bold: true, Owner: n.ID}
			labels = append(labels, title)
			width := 0.0
			if f != nil {
				b, err := f.measure(title)
				if err == nil {
					width = b.W
				}
			}
			labels = append(labels, label{Text: n.Lines[0], X: title.X + width + 12, Y: title.Y, Size: sublabelSize, Owner: n.ID, Kind: "muted"})
			continue
		}
		labels = append(labels, label{Text: n.Title, X: n.Box.X + 16, Y: n.Box.Y + 21, Size: titleSize, Bold: true, Owner: n.ID})
		lines := n.Lines
		if n.Prefix != "" {
			lines = append([]string{"Base " + n.Prefix}, lines...)
		}
		for i, line := range lines {
			labels = append(labels, label{Text: line, X: n.Box.X + 16, Y: n.Box.Y + 41 + float64(i)*20, Size: sublabelSize, Owner: n.ID, Kind: "muted"})
		}
		for i, c := range n.Columns {
			y := n.Box.Y + 72 + float64(i)*26
			if c.Key != "" {
				labels = append(labels, label{Text: c.Key, X: n.Box.X + 12, Y: y, Size: edgeLabelSize, Owner: n.ID, Kind: "accent"})
			}
			labels = append(labels, label{Text: c.Name, X: n.Box.X + 64, Y: y, Size: sublabelSize, Owner: n.ID}, label{Text: c.Type, X: n.Box.X + 228, Y: y, Size: sublabelSize, Owner: n.ID, Kind: "muted"})
		}
	}
	for _, e := range d.Edges {
		if e.Label != nil {
			l := *e.Label
			if f != nil {
				b, err := f.measure(l)
				if err == nil {
					l.X -= b.W
				}
			}
			labels = append(labels, l)
		}
	}
	return slices.DeleteFunc(labels, func(l label) bool { return l.Text == "" })
}
