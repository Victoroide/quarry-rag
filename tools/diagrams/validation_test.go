package main

import (
	"cmp"
	"errors"
	"maps"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestMigrationParser(t *testing.T) {
	sql := `CREATE TABLE IF NOT EXISTS parents (id UUID PRIMARY KEY, label VARCHAR(120), amount NUMERIC(10,2));
 CREATE TABLE children (id UUID PRIMARY KEY, parent_id UUID REFERENCES parents(id));
 ALTER TABLE parents ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ, ADD COLUMN old TEXT,
     ADD CONSTRAINT amount_range CHECK (amount > 0 AND amount < 100);
 DO $$ BEGIN ALTER TABLE parents ADD COLUMN active BOOLEAN; END $$;
 ALTER TABLE parents DROP COLUMN IF EXISTS old;`
	got, err := parseSQL(sql)
	if err != nil {
		t.Fatal(err)
	}
	if len(got["parents"]) != 5 || got["parents"]["amount"].Type != "NUMERIC(10,2)" || got["children"]["parent_id"].Reference != "parents.id" {
		t.Fatalf("incorrect schema: %#v", got)
	}
	if _, ok := got["parents"]["old"]; ok {
		t.Fatal("dropped column retained")
	}
	if _, err := parseSQL(`ALTER TABLE unknown ADD COLUMN x TEXT;`); err == nil {
		t.Fatal("unknown table accepted")
	}
	replaced, replaceErr := parseSQL(`CREATE TABLE parents (id UUID PRIMARY KEY, kind TEXT CHECK (kind IN ('a'))); ALTER TABLE parents DROP CONSTRAINT parents_kind_check; ALTER TABLE parents ADD CONSTRAINT parents_kind_check CHECK (kind IN ('a', 'b'));`)
	if replaceErr != nil || len(replaced["parents"]) != 2 {
		t.Fatalf("a replaced check must leave the columns alone: %v %#v", replaceErr, replaced)
	}
	if _, err := parseSQL(`CREATE TABLE parents (id UUID PRIMARY KEY); ALTER TABLE parents DROP CONSTRAINT parents_pkey;`); err == nil {
		t.Fatal("dropping a key must not be accepted silently")
	}
	grant, grantErr := parseSQL(`CREATE TABLE parents (id UUID PRIMARY KEY); ALTER TABLE grant_catalog_unmodeled ADD COLUMN origin TEXT NOT NULL DEFAULT 'imported' CHECK (origin IN ('imported', 'accepted'));`)
	if grantErr != nil || len(grant) != 1 {
		t.Fatalf("operator reference storage must not be modeled or rejected: %v %#v", grantErr, grant)
	}
}

func TestRouteParser(t *testing.T) {
	got, err := registeredRoutes([]byte(`package api;func routes(){mux.HandleFunc("GET /health", health); mux.Handle("POST /new",h); other("not a route")}`))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(got, ",") != "GET /health,POST /new" {
		t.Fatalf("routes: %v", got)
	}
}

func TestDirectoryDescriptions(t *testing.T) {
	root := t.TempDir()
	for _, p := range []string{"cmd/server", "internal/api/handler", ".git/objects", ".github/workflows", "vendor/x", "testdata/x"} {
		if err := os.MkdirAll(filepath.Join(root, p), 0o750); err != nil {
			t.Fatal(err)
		}
	}
	descriptions := map[string]string{"cmd": "binaries", "cmd/server": "entry", "internal": "packages", "internal/api": "HTTP"}
	trackDirectories(t, root, descriptions)
	dirs, err := repositoryDirs(root, 2, descriptions)
	if err != nil {
		t.Fatal(err)
	}
	if len(dirs) != 4 {
		t.Fatalf("directories: %v", dirs)
	}
	if err := os.Mkdir(filepath.Join(root, "new"), 0o750); err != nil {
		t.Fatal(err)
	}
	trackDirectories(t, root, map[string]string{"new": "new"})
	if _, err := repositoryDirs(root, 2, descriptions); err == nil {
		t.Fatal("new directory accepted without a description")
	}
	if err := os.RemoveAll(filepath.Join(root, "new")); err != nil {
		t.Fatal(err)
	}
	descriptions["gone"] = "stale"
	if _, err := repositoryDirs(root, 2, descriptions); err == nil {
		t.Fatal("stale description accepted")
	}
}

func TestRejectGeometryDefects(t *testing.T) {
	fonts, err := loadFonts()
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name string
		want string
		d    diagram
	}{
		{"node overlap", `nodes b and a overlap`, diagram{Nodes: []node{{ID: "a", Box: box{20, 20, 100, 80}}, {ID: "b", Box: box{80, 20, 100, 80}}}}},
		{"edge through node", `violates clearance of node c`, diagram{Nodes: []node{{ID: "a", Box: box{20, 20, 100, 80}}, {ID: "b", Box: box{220, 20, 100, 80}}, {ID: "c", Box: box{160, 40, 20, 40}}}, Edges: []edge{{ID: "ab", From: "a", To: "b", Points: []point{{120, 60}, {220, 60}}}}}},
		{"edge clearance", `violates clearance of node c`, diagram{Nodes: []node{{ID: "c", Box: box{100, 100, 80, 60}}}, Edges: []edge{{ID: "line", Points: []point{{50, 95}, {250, 95}}}}}},
		{"label overlap", `labels "labels" and "Overlapping" overlap`, diagram{Labels: []label{{Text: "Overlapping", X: 40, Y: 50, Size: 14}, {Text: "labels", X: 50, Y: 50, Size: 14}}}},
		{"label on edge", `overlaps edge line`, diagram{Labels: []label{{Text: "Crossing", X: 40, Y: 50, Size: 14}}, Edges: []edge{{ID: "line", Points: []point{{30, 45}, {200, 45}}}}}},
		{"text overflow", `overflows or touches its node border`, diagram{Nodes: []node{{ID: "a", Title: "Much too wide for this box", Box: box{20, 20, 55, 64}}}}},
		{"label on border", `overflows or touches its node border`, diagram{Nodes: []node{{ID: "a", Box: box{20, 20, 100, 64}}}, Labels: []label{{Text: "Border", X: 20, Y: 40, Size: 14, Owner: "a"}}}},
		{"edge crossing", `cross or overlap`, diagram{Edges: []edge{{ID: "a", Points: []point{{20, 60}, {200, 60}}}, {ID: "b", Points: []point{{100, 20}, {100, 120}}}}}},
		{"diagonal edge", `is not orthogonal`, diagram{Edges: []edge{{ID: "a", Points: []point{{20, 20}, {200, 80}}}}}},
		{"viewBox overflow", `violates viewBox margin`, diagram{Labels: []label{{Text: "Outside", X: 495, Y: 50, Size: 14}}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tc.d.W = 500
			tc.d.H = 500
			if err := validate(tc.d, fonts); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("wanted %q; got %v", tc.want, err)
			}
		})
	}
	valid := diagram{W: 500, H: 300, Nodes: []node{{ID: "a", Title: "Source", Box: box{20, 20, 120, 64}}, {ID: "b", Title: "Target", Box: box{240, 20, 120, 64}}}, Edges: []edge{{ID: "ab", From: "a", To: "b", Points: []point{{140, 52}, {240, 52}}}}}
	if err := validate(valid, fonts); err != nil {
		t.Fatalf("valid scene: %v", err)
	}
}

func TestSourcesMatchCode(t *testing.T) {
	ds, err := buildDiagrams("../..")
	if err != nil {
		t.Fatal(err)
	}
	if len(ds) != 4 {
		t.Fatalf("diagrams: %d", len(ds))
	}
	if err := checkSources("../..", ds); err != nil {
		t.Fatal(err)
	}
	client := diagramNodeByID(t, diagramByName(t, ds, "system-architecture"), "client")
	client.Lines = append(client.Lines, "GET /new")
	// The comparison is bidirectional: nonexistent routes are also stale content.
	if err := checkSources("../..", ds); err == nil {
		t.Fatal("extra route accepted")
	}
}

func TestSourceChangesInvalidateDiagram(t *testing.T) {
	var err error
	root := t.TempDir()
	for p := range directoryDescriptions {
		if err = os.MkdirAll(filepath.Join(root, p), 0o750); err != nil {
			t.Fatal(err)
		}
	}
	copyFile := func(rel string) {
		b, readErr := readWithin("../..", rel)
		if readErr != nil {
			t.Fatal(readErr)
		}
		p := filepath.Join(root, rel)
		if err = os.MkdirAll(filepath.Dir(p), 0o750); err != nil {
			t.Fatal(err)
		}
		if err = writeWithin(root, rel, b); err != nil {
			t.Fatal(err)
		}
	}
	migrations, err := filepath.Glob("../../internal/store/postgres/migrations/*.sql")
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range migrations {
		copyFile(strings.TrimPrefix(p, "../../"))
	}
	copyFile("internal/api/router.go")
	for _, name := range []string{"system-architecture", "database-schema", "database-operations", "repository-layout"} {
		copyFile("docs/diagrams/src/" + name + ".json")
	}
	trackDirectories(t, root, directoryDescriptions)
	ds, err := buildDiagrams(root)
	if err != nil {
		t.Fatal(err)
	}
	if err = checkSources(root, ds); err != nil {
		t.Fatal(err)
	}
	migration := filepath.Join(root, "internal", "store", "postgres", "migrations", "999_new.sql")
	if err = os.WriteFile(migration, []byte("ALTER TABLE documents ADD COLUMN new_value TEXT;"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err = checkSources(root, ds); err == nil {
		t.Fatal("new SQL column accepted")
	}
	if err = os.Remove(migration); err != nil {
		t.Fatal(err)
	}
	b, err := readWithin(root, "internal/api/router.go")
	if err != nil {
		t.Fatal(err)
	}
	b = []byte(strings.Replace(string(b), `mux := http.NewServeMux()`, `mux := http.NewServeMux(); mux.HandleFunc("POST /new", nil)`, 1))
	if err = writeWithin(root, "internal/api/router.go", b); err != nil {
		t.Fatal(err)
	}
	if err = checkSources(root, ds); err == nil {
		t.Fatal("new registered route accepted")
	}
}

func TestSharedEndpointDoesNotExcuseAnotherCrossing(t *testing.T) {
	f, err := loadFonts()
	if err != nil {
		t.Fatal(err)
	}
	d := diagram{W: 600, H: 500, Nodes: []node{{ID: "source", Box: box{20, 180, 100, 80}}}, Edges: []edge{
		{ID: "a", From: "source", Points: []point{{120, 220}, {180, 220}, {180, 340}, {380, 340}}},
		{ID: "b", From: "source", Points: []point{{120, 220}, {240, 220}, {240, 400}}},
	}}
	if err := validate(d, f); err == nil || !strings.Contains(err.Error(), "cross or overlap") {
		t.Fatalf("second crossing hidden by common endpoint: %v", err)
	}
}

func TestRepositoryTreeHierarchy(t *testing.T) {
	ds, err := buildDiagrams("../..")
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range ds {
		if d.Name != "repository-layout" {
			continue
		}
		if len(d.Groups) != 7 {
			t.Fatal("repository sections must follow top-level directories")
		}
		if len(d.Nodes) != len(directoryDescriptions) {
			t.Fatal("directory listing is incomplete")
		}
		columns := map[bool]box{}
		for _, g := range d.Groups {
			col := g.Box.X > d.W/2
			if previous, ok := columns[col]; ok {
				if math.Abs(g.Box.X-previous.X) > .5 || math.Abs(g.Box.X+g.Box.W-previous.X-previous.W) > .5 {
					t.Error("section edges are misaligned")
				}
			} else {
				columns[col] = g.Box
			}
		}
		labels := sceneLabels(d)
		byID := map[string]node{}
		for _, n := range d.Nodes {
			byID[n.ID] = n
		}
		for _, n := range d.Nodes {
			if parent, ok := byID[filepath.ToSlash(filepath.Dir(n.ID))]; ok {
				if parent.Box.Y >= n.Box.Y || (parent.Box.X < 420) != (n.Box.X < 420) {
					t.Errorf("parent of %s must be above it in the same column", n.ID)
				}
			}
			depth := len(strings.Split(n.ID, "/")) - 1
			found := false
			for _, g := range d.Groups {
				if contains(g.Box, n.Box) {
					found = true
					if depth == 0 {
						for _, l := range labels {
							if l.Owner == n.ID && l.Text == n.Title {
								if math.Abs(l.X-(g.Box.X+24)) > .5 || math.Abs(l.Y-(g.Box.Y+29)) > .5 {
									t.Errorf("section label inset differs for %s", n.ID)
								}
							}
						}
					}
					if math.Abs(n.Box.X-(g.Box.X+12+float64(depth)*24)) > .5 {
						t.Errorf("incorrect indentation for %s", n.ID)
					}
				}
			}
			if !found {
				t.Errorf("directory %s is outside its subtree column", n.ID)
			}
		}
	}
}

func TestPulseSequenceIsSingularAndSeamless(t *testing.T) {
	ds, err := buildDiagrams("../..")
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range ds {
		type trip struct{ start, arrival float64 }
		var trips []trip
		for _, e := range d.Edges {
			for _, at := range e.Times {
				if at <= 0 || at+pulseSeconds >= d.Duration {
					t.Errorf("%s pulse crosses loop seam", e.ID)
				}
				trips = append(trips, trip{at, at + pulseDuration(edgeLength(e))})
			}
		}
		slices.SortFunc(trips, func(a, b trip) int { return cmp.Compare(a.start, b.start) })
		for i := 1; i < len(trips); i++ {
			// The next pulse leaves once the previous target has faded in.
			if trips[i].start-trips[i-1].arrival < .5-0.00001 {
				t.Errorf("%s pulse at %.2f leaves before the one at %.2f lands and lights", d.Name, trips[i].start, trips[i-1].start)
			}
		}
	}
}

func TestStrictGeometry(t *testing.T) {
	f, err := loadFonts()
	if err != nil {
		t.Fatal(err)
	}
	cases := []diagram{
		{Edges: []edge{{ID: "a", Points: []point{{20, 100}, {200, 100}}}, {ID: "b", Points: []point{{100, 100}, {250, 100}}}}},
		{Edges: []edge{{ID: "a", Points: []point{{20, 100}, {200, 100}}}, {ID: "b", Points: []point{{20, 110}, {200, 110}}}}},
		{Nodes: []node{{ID: "source", Box: box{20, 180, 100, 80}}}, Edges: []edge{{ID: "a", From: "source", Points: []point{{120, 220}, {180, 220}}}, {ID: "b", From: "source", Points: []point{{120, 220}, {120, 300}}}}},
		{Groups: []group{{Box: box{120, 120, 100, 100}}}, Edges: []edge{{ID: "a", Points: []point{{20, 160}, {300, 160}}}}},
		{Groups: []group{{Category: "corpus", Box: box{100, 100, 200, 200}}}, Nodes: []node{{ID: "wrong", Category: "queue", Box: box{120, 150, 100, 80}}}},
	}
	wants := []string{"cross or overlap", "parallel edges", "share a node port", "crosses unrelated group", "category differs"}
	for i, d := range cases {
		d.W = 600
		d.H = 500
		if err := validate(d, f); err == nil || !strings.Contains(err.Error(), wants[i]) {
			t.Errorf("strict geometry defect %d: wanted %q; got %v", i, wants[i], err)
		}
	}
}

func TestCanvasAndInspectionTextSize(t *testing.T) {
	ds, err := buildDiagrams("../..")
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range ds {
		if d.W > 960 {
			t.Errorf("%s width %g > 960", d.Name, d.W)
		}
		if d.H > d.MaxH {
			t.Errorf("%s height %g > %g", d.Name, d.H, d.MaxH)
		}
		if want := math.Ceil(d.H/10) * 10; d.MaxH != want {
			t.Errorf("%s MaxH %g, want %g", d.Name, d.MaxH, want)
		}
		if d.Name == "system-architecture" && d.MaxH != 760 {
			t.Errorf("system-architecture MaxH %g, want 760", d.MaxH)
		}
		for _, l := range sceneLabels(d) {
			if l.Size*880/d.W < 12 {
				t.Errorf("%s %q is %.2f px at 880", d.Name, l.Text, l.Size*880/d.W)
			}
		}
	}
}

func TestCollinearGaps(t *testing.T) {
	f, err := loadFonts()
	if err != nil {
		t.Fatal(err)
	}
	for _, gap := range []float64{4, 200} {
		d := diagram{W: 600, H: 500, Edges: []edge{{ID: "a", Points: []point{{20, 100}, {100, 100}}}, {ID: "b", Points: []point{{100 + gap, 100}, {180 + gap, 100}}}}}
		err := validate(d, f)
		if gap < 12 {
			if err == nil || !strings.Contains(err.Error(), "collinear") {
				t.Fatalf("gap %g: %v", gap, err)
			}
		} else if err != nil {
			t.Fatal(err)
		}
	}
}

func TestMatchingCategoriesAreAccepted(t *testing.T) {
	f, err := loadFonts()
	if err != nil {
		t.Fatal(err)
	}
	d := diagram{W: 600, H: 500, Groups: []group{{Category: "corpus", Box: box{100, 100, 200, 200}}}, Nodes: []node{{ID: "same", Category: "corpus", Box: box{120, 150, 100, 80}}}}
	if err := validate(d, f); err != nil {
		t.Fatal(err)
	}
}

func trackDirectories(t *testing.T, root string, dirs map[string]string) {
	t.Helper()
	if _, err := gitFixture(t, root, false); err != nil {
		t.Fatal(err)
	}
	for dir := range dirs {
		path := filepath.Join(root, dir, "fixture")
		if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte("fixture"), 0o600); err != nil {
			t.Fatal(err)
		}
		if out, err := gitFixture(t, root, true); err != nil {
			t.Fatalf("%v: %s", err, out)
		}
	}
}

func gitFixture(t *testing.T, root string, add bool) ([]byte, error) {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), "git", "init", "-q")
	if add {
		cmd = exec.CommandContext(t.Context(), "git", "add", "-N", ".")
	}
	cmd.Dir = root
	return cmd.CombinedOutput()
}

func readWithin(root, path string) (data []byte, err error) {
	rootFS, err := os.OpenRoot(root)
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, rootFS.Close()) }()
	return rootFS.ReadFile(path)
}
func writeWithin(root, path string, data []byte) (err error) {
	rootFS, err := os.OpenRoot(root)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, rootFS.Close()) }()
	return rootFS.WriteFile(path, data, 0o600)
}

func TestCanvasMarginsPortsAndClearances(t *testing.T) {
	f, err := loadFonts()
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name, want string
		d          diagram
	}{
		{"edge margin", "edge e violates viewBox margin", diagram{Edges: []edge{{ID: "e", Points: []point{{10, 100}, {100, 100}}}}}},
		{"label margin", "label \"Near\" violates viewBox margin", diagram{Labels: []label{{Text: "Near", X: 10, Y: 100, Size: 14}}}},
		{"corner port", "port is too close to node corner", diagram{Nodes: []node{{ID: "a", Box: box{40, 100, 100, 56}}}, Edges: []edge{{ID: "e", From: "a", Points: []point{{140, 105}, {180, 105}}}}}},
		{"label gap", "labels \"A\" and \"B\" overlap", diagram{Labels: []label{{Text: "B", X: 100, Y: 100, Size: 14}, {Text: "A", X: 113, Y: 100, Size: 14}}}},
		{"arrow clearance", "label \"Tip\" overlaps edge e", diagram{Labels: []label{{Text: "Tip", X: 180, Y: 90, Size: 14}}, Edges: []edge{{ID: "e", Points: []point{{100, 100}, {200, 100}}}}}},
		{"shared port", "share a node port", diagram{Nodes: []node{{ID: "a", Box: box{40, 100, 100, 56}}}, Edges: []edge{{ID: "e", From: "a", Points: []point{{140, 128}, {180, 128}}}, {ID: "f", From: "a", Points: []point{{140, 128}, {140, 200}}}}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tc.d.W = 500
			tc.d.H = 500
			err := validate(tc.d, f)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("wanted %q; got %v", tc.want, err)
			}
		})
	}
}

func TestNodeBordersCannotOverlap(t *testing.T) {
	f, err := loadFonts()
	if err != nil {
		t.Fatal(err)
	}
	d := diagram{W: 500, H: 300, Nodes: []node{{ID: "a", Box: box{40, 100, 100, 56}}, {ID: "b", Box: box{140.5, 100, 100, 56}}}}
	if err := validate(d, f); err == nil || !strings.Contains(err.Error(), "nodes b and a overlap") {
		t.Fatalf("border overlap: %v", err)
	}
	for i := range d.Nodes {
		d.Nodes[i].Borderless = true
	}
	if err := validate(d, f); err != nil {
		t.Fatalf("borderless rows: %v", err)
	}
}

func TestMigrationTypeChanges(t *testing.T) {
	for _, form := range []string{"TYPE BIGINT", "SET DATA TYPE BIGINT"} {
		got, err := parseSQL("CREATE TABLE chunks (token_count INTEGER); ALTER TABLE chunks ALTER COLUMN token_count " + form + ";")
		if err != nil {
			t.Fatal(err)
		}
		if got["chunks"]["token_count"].Type != "BIGINT" {
			t.Fatalf("type change ignored: %v", got)
		}
	}
	if _, err := parseSQL("CREATE TABLE chunks (token_count INTEGER); ALTER TABLE chunks ALTER COLUMN token_count SET STATISTICS 50;"); err == nil || !strings.Contains(err.Error(), "unsupported") {
		t.Fatalf("unsupported alteration: %v", err)
	}
}

func TestUntrackedEditorDirectoryIsIgnored(t *testing.T) {
	root := t.TempDir()
	dirs := map[string]string{"cmd": "entry"}
	trackDirectories(t, root, dirs)
	if err := os.MkdirAll(filepath.Join(root, ".vscode"), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := writeWithin(root, ".vscode/settings.json", []byte("{}")); err != nil {
		t.Fatal(err)
	}
	if _, err := repositoryDirs(root, 3, dirs); err != nil {
		t.Fatal(err)
	}
}

func TestArchitectureGridAndDirections(t *testing.T) {
	ds, err := buildDiagrams("../..")
	if err != nil {
		t.Fatal(err)
	}
	d := diagramByName(t, ds, "system-architecture")
	for _, n := range d.Nodes {
		if math.Abs(n.Box.W-184) > .5 {
			t.Errorf("%s width %g", n.ID, n.Box.W)
		}
	}
	for _, n := range d.Nodes {
		x := 16 + 208*math.Round((n.Box.X-16)/208)
		if math.Abs(n.Box.X-x) > .5 {
			t.Errorf("%s leaves its column grid", n.ID)
		}
		if n.Row == "entry" {
			want := 56.0
			if n.ID == "router" {
				want = 76
			}
			if n.ID == "probes" {
				want = 96
			}
			if n.ID == "documents" {
				want = 136
			}
			if n.Box.H != want || math.Abs(n.Box.Y+n.Box.H/2-152) > .5 {
				t.Errorf("%s entry size or center", n.ID)
			}
		} else if math.Abs(n.Box.Y+n.Box.H/2-(288+96*math.Round((n.Box.Y+n.Box.H/2-288)/96))) > .5 {
			t.Errorf("%s leaves its row grid", n.ID)
		}
	}
	positions := map[string]box{}
	for _, n := range d.Nodes {
		positions[n.ID] = n.Box
	}
	if !(positions["chunk"].X < positions["embed"].X && positions["embed"].X < positions["persist"].X) {
		t.Error("chunk, embed and persist must read left to right")
	}

	have := map[string]bool{}
	for _, e := range d.Edges {
		have[e.From+">"+e.To] = true
	}
	for _, v := range []string{"retrieve>pinecone", "route>bedrock-ingest", "worker>postgres"} {
		if !have[v] {
			t.Errorf("missing relationship %s", v)
		}
	}
	f, err := loadFonts()
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"column", "row"} {
		scenes, err := buildDiagrams("../..")
		if err != nil {
			t.Fatal(err)
		}
		bad := diagramByName(t, scenes, "system-architecture")
		if kind == "column" {
			diagramNodeByID(t, bad, "client").Box.X += 0.6
		} else {
			for i := range bad.Edges {
				e := &bad.Edges[i]
				if e.ID == "ingest-sandbox" {
					for j := range e.Points {
						e.Points[j][1] += 0.6
					}
				}
			}
		}
		err = validate(*bad, f)
		if err == nil || !strings.Contains(err.Error(), kind+" alignment") {
			t.Errorf("%s shift not caught: %v", kind, err)
		}
	}
}

func TestDataModelRowsAndPorts(t *testing.T) {
	ds, err := buildDiagrams("../..")
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range ds {
		if d.Name != "database-schema" && d.Name != "database-operations" {
			continue
		}
		nodes := map[string]node{}
		for _, n := range d.Nodes {
			nodes[n.ID] = n
		}
		for i, e := range d.Edges {
			parent := nodes[e.From]
			child := nodes[e.To]
			if d.Name == "database-schema" {
				if e.From != "documents" || math.Abs(e.Points[0][1]-(parent.Box.Y+48+float64(i)*12)) > .5 {
					t.Errorf("%s source must lie within the id row", e.ID)
				}
			} else if e.From != "documents-ref" || math.Abs(e.Points[0][1]-(parent.Box.Y+parent.Box.H/2)) > .5 {
				t.Errorf("%s source must center on the documents reference node", e.ID)
			}
			found := false
			for j, c := range child.Columns {
				if c.Reference == "documents.id" {
					found = true
					if math.Abs(e.Points[len(e.Points)-1][1]-(child.Box.Y+schemaColumnBaseline(j)-6)) > .5 {
						t.Errorf("%s must point to its child FK row", e.ID)
					}
				}
			}
			if !found {
				t.Errorf("%s has no referencing key", e.ID)
			}
		}
		for _, n := range d.Nodes {
			for i, c := range n.Columns {
				found := false
				for _, l := range sceneLabels(d) {
					if l.Owner == n.ID && l.Text == c.Name {
						found = true
						if math.Abs(l.Y-(n.Box.Y+schemaColumnBaseline(i))) > .5 {
							t.Errorf("%s.%s row spacing", n.ID, c.Name)
						}
					}
				}
				if !found {
					t.Errorf("missing column %s.%s", n.ID, c.Name)
				}
			}
		}
	}
}

func TestMotionRestsBeforeLoopRepeats(t *testing.T) {
	ds, err := buildDiagrams("../..")
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range ds {
		for _, e := range d.Edges {
			for _, at := range e.Times {
				if at+pulseDuration(edgeLength(e))+.001 > d.Duration-2.5 {
					t.Errorf("%s pulse runs into the rest before the loop repeats", e.ID)
				}
			}
		}
		for _, n := range d.Nodes {
			for _, at := range n.Times {
				if at+2*highlightFade+highlightHold > d.Duration-2.5 {
					t.Errorf("%s highlight runs into the rest before the loop repeats", n.ID)
				}
			}
		}
	}
}

func TestEdgeLengthLimit(t *testing.T) {
	f, err := loadFonts()
	if err != nil {
		t.Fatal(err)
	}
	d := diagram{Name: "system-architecture", W: 840, H: 760, Edges: []edge{{ID: "long", Points: []point{{16, 100}, {467, 100}}}}}
	if err := validate(d, f); err == nil || !strings.Contains(err.Error(), "exceeds 450 units") {
		t.Fatalf("long edge accepted: %v", err)
	}
	d.Edges[0].Points[1][0] = 466
	if err := validate(d, f); err != nil {
		t.Fatalf("450-unit edge rejected: %v", err)
	}
}

func TestCanvasFitsLastContent(t *testing.T) {
	ds, err := buildDiagrams("../..")
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range ds {
		if d.Name == "database-schema" || d.Name == "database-operations" {
			bottom := 0.0
			for _, n := range d.Nodes {
				bottom = math.Max(bottom, n.Box.Y+n.Box.H)
			}
			if len(d.Labels) != 1 || d.Labels[0].Y != bottom+40 || d.H != bottom+62 {
				t.Errorf("%s legend and canvas must follow the last table", d.Name)
			}
		}
		if d.Name == "repository-layout" {
			bottom := 0.0
			for _, group := range d.Groups {
				bottom = math.Max(bottom, group.Box.Y+group.Box.H)
			}
			if d.H != bottom+16 {
				t.Error("repository canvas must leave a 16-unit inset below the last group")
			}
		}
		for _, l := range sceneLabels(d) {
			if l.Owner == "" && (l.Text == d.Title || l.Y == 56) && l.X != 16 {
				t.Errorf("%s title inset", d.Name)
			}
		}
	}
}

func cloneDiagrams(src []diagram) []diagram {
	out := make([]diagram, len(src))
	for i, d := range src {
		out[i] = d
		out[i].Nodes = make([]node, len(d.Nodes))
		for j, n := range d.Nodes {
			out[i].Nodes[j] = n
			out[i].Nodes[j].Lines = slices.Clone(n.Lines)
			out[i].Nodes[j].Columns = slices.Clone(n.Columns)
			out[i].Nodes[j].Times = slices.Clone(n.Times)
		}
		out[i].Edges = make([]edge, len(d.Edges))
		for j, e := range d.Edges {
			out[i].Edges[j] = e
			out[i].Edges[j].Points = slices.Clone(e.Points)
			out[i].Edges[j].Times = slices.Clone(e.Times)
			if e.Label != nil {
				lbl := *e.Label
				out[i].Edges[j].Label = &lbl
			}
		}
		out[i].Groups = slices.Clone(d.Groups)
		out[i].Labels = slices.Clone(d.Labels)
	}
	return out
}

func diagramByName(t *testing.T, ds []diagram, name string) *diagram {
	t.Helper()
	i := slices.IndexFunc(ds, func(d diagram) bool { return d.Name == name })
	if i < 0 {
		t.Fatalf("scene %s is missing", name)
	}
	return &ds[i]
}

func diagramNodeByID(t *testing.T, d *diagram, id string) *node {
	t.Helper()
	i := slices.IndexFunc(d.Nodes, func(n node) bool { return n.ID == id })
	if i < 0 {
		t.Fatalf("scene %s is missing node %s", d.Name, id)
	}
	return &d.Nodes[i]
}

func TestSchemaSceneColumnsUniqueAndComplete(t *testing.T) {
	ds, err := buildDiagrams("../..")
	if err != nil {
		t.Fatal(err)
	}
	if err = checkSources("../..", ds); err != nil {
		t.Fatalf("main + migrations check failed: %v", err)
	}
	cols, err := collectSchemaColumns(ds)
	if err != nil {
		t.Fatal(err)
	}
	rootFS, err := os.OpenRoot("../..")
	if err != nil {
		t.Fatal(err)
	}
	migrated, err := readMigrationSchema(rootFS)
	if closeErr := errors.Join(err, rootFS.Close()); closeErr != nil {
		t.Fatal(closeErr)
	}
	if got, want := slices.Sorted(maps.Keys(cols)), slices.Sorted(maps.Keys(migrated)); !slices.Equal(got, want) {
		t.Fatalf("modeled tables %v, want migration tables %v", got, want)
	}

	ops := diagramByName(t, ds, "database-operations")
	ref := diagramNodeByID(t, ops, "documents-ref")
	if ref.Title != "documents.id" || !slices.Equal(ref.Lines, []string{"Document lifecycle"}) || len(ref.Columns) != 0 {
		t.Fatalf("unexpected operations reference node: %+v", ref)
	}

	withRefCols := cloneDiagrams(ds)
	diagramNodeByID(t, diagramByName(t, withRefCols, "database-operations"), "documents-ref").Columns = []column{{Name: "id", Type: "UUID"}}
	if err = checkSources("../..", withRefCols); err == nil {
		t.Fatal("reference node with columns was accepted")
	}

	for _, tc := range []struct {
		scene, table, column string
	}{
		{"database-schema", "chunks", "document_id"},
		{"database-schema", "documents", "untagged_since"},
		{"database-operations", "vector_state", "active_cleanup_ids"},
		{"database-operations", "vector_state", "active_cleanup_reservation"},
	} {
		t.Run("missing "+tc.table+"."+tc.column, func(t *testing.T) {
			omitted := cloneDiagrams(ds)
			n := diagramNodeByID(t, diagramByName(t, omitted, tc.scene), tc.table)
			index := slices.IndexFunc(n.Columns, func(c column) bool { return c.Name == tc.column })
			if index < 0 {
				t.Fatalf("%s is absent from %s", tc.column, tc.table)
			}
			n.Columns = slices.Delete(n.Columns, index, index+1)
			if checkErr := checkSources("../..", omitted); checkErr == nil || !strings.Contains(checkErr.Error(), "SQL columns, types or keys differ") {
				t.Fatalf("checking omitted column: %v", checkErr)
			}
		})
	}

	dupCol := cloneDiagrams(ds)
	chunks := diagramNodeByID(t, diagramByName(t, dupCol, "database-schema"), "chunks")
	chunks.Columns = append(chunks.Columns, chunks.Columns[0])
	if err = checkSources("../..", dupCol); err == nil || !strings.Contains(err.Error(), "duplicate schema column chunks.") {
		t.Fatalf("checking duplicate column: %v", err)
	}

	dupTable := cloneDiagrams(ds)
	dupOps := diagramByName(t, dupTable, "database-operations")
	dupChunks := diagramNodeByID(t, diagramByName(t, dupTable, "database-schema"), "chunks")
	dupOps.Nodes = append(dupOps.Nodes, *dupChunks)
	if err = checkSources("../..", dupTable); err == nil || !strings.Contains(err.Error(), "duplicate schema table chunks") {
		t.Fatalf("checking duplicate table: %v", err)
	}

	for _, tc := range []struct {
		name string
		edit func(*column)
	}{
		{"retyped", func(c *column) { c.Type = "TEXT" }},
		{"miskeyed", func(c *column) { c.Key = "" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			changed := cloneDiagrams(ds)
			n := diagramNodeByID(t, diagramByName(t, changed, "database-schema"), "documents")
			index := slices.IndexFunc(n.Columns, func(c column) bool { return c.Name == "id" })
			if index < 0 {
				t.Fatal("documents.id is missing")
			}
			tc.edit(&n.Columns[index])
			if checkErr := checkSources("../..", changed); checkErr == nil || !strings.Contains(checkErr.Error(), "SQL columns, types or keys differ") {
				t.Fatalf("checking %s column: %v", tc.name, checkErr)
			}
		})
	}

	// Moved table with stale placement must fail buildDiagrams.
	root := t.TempDir()
	for p := range directoryDescriptions {
		if mkErr := os.MkdirAll(filepath.Join(root, p), 0o750); mkErr != nil {
			t.Fatal(mkErr)
		}
	}
	for _, name := range []string{"system-architecture", "database-schema", "database-operations", "repository-layout"} {
		rel := filepath.Join("docs", "diagrams", "src", name+".json")
		b, readErr := readWithin("../..", rel)
		if readErr != nil {
			t.Fatal(readErr)
		}
		if mkErr := os.MkdirAll(filepath.Join(root, filepath.Dir(rel)), 0o750); mkErr != nil {
			t.Fatal(mkErr)
		}
		if wrErr := writeWithin(root, rel, b); wrErr != nil {
			t.Fatal(wrErr)
		}
	}
	trackDirectories(t, root, directoryDescriptions)
	schemaPath := filepath.Join("docs", "diagrams", "src", "database-schema.json")
	schemaJSON, err := readWithin(root, schemaPath)
	if err != nil {
		t.Fatal(err)
	}
	// Simulate moving source_uploads to the operations scene without updating placements.
	staleSchema := strings.Replace(string(schemaJSON), `"source_uploads":`, `"moved_source_uploads":`, 1)
	if err = writeWithin(root, schemaPath, []byte(staleSchema)); err != nil {
		t.Fatal(err)
	}
	if _, err = buildDiagrams(root); err == nil {
		t.Fatal("moved table with stale placement accepted")
	}

	// Preserve grant_catalog_* exclusions in parseSQL.
	grantSchema, err := parseSQL(`CREATE TABLE documents (id UUID PRIMARY KEY); CREATE TABLE grant_catalog_roles (id UUID PRIMARY KEY); ALTER TABLE grant_catalog_roles ADD COLUMN role TEXT; ALTER TABLE documents ADD COLUMN grant_catalog_note TEXT;`)
	if err != nil {
		t.Fatalf("grant_catalog exclusion failed: %v", err)
	}
	if len(grantSchema) != 1 || len(grantSchema["documents"]) != 1 {
		t.Fatalf("grant_catalog exclusion altered modeled schema: %#v", grantSchema)
	}
}

func TestSchemaScenesFitAndPreserveRelationships(t *testing.T) {
	f, err := loadFonts()
	if err != nil {
		t.Fatal(err)
	}
	ds, err := buildDiagrams("../..")
	if err != nil {
		t.Fatal(err)
	}
	archDoc, err := readWithin("../..", "docs/architecture.md")
	if err != nil {
		t.Fatal(err)
	}
	var schemas []diagram
	seenTargets := map[string]bool{}
	for _, d := range ds {
		if d.Name != "database-schema" && d.Name != "database-operations" {
			continue
		}
		schemas = append(schemas, d)
		if err := validate(d, f); err != nil {
			t.Fatalf("%s failed validation: %v", d.Name, err)
		}
		if d.W != 840 || d.MaxW != 840 || d.H > d.MaxH {
			t.Errorf("%s canvas (%g x %g) exceeds limits (%g x %g)", d.Name, d.W, d.H, d.MaxW, d.MaxH)
		}
		bottom := 0.0
		for _, n := range d.Nodes {
			bottom = math.Max(bottom, n.Box.Y+n.Box.H)
		}
		if len(d.Labels) != 1 || d.Labels[0].Y != bottom+40 || d.H != bottom+62 {
			t.Errorf("%s legend/canvas spacing mismatch: bottom=%g labels=%+v H=%g", d.Name, bottom, d.Labels, d.H)
		}
		for _, l := range sceneLabels(d) {
			if px := l.Size * 880 / d.W; px < 12 {
				t.Errorf("%s label %q is %.2f px at 880", d.Name, l.Text, px)
			}
		}
		if !strings.Contains(string(archDoc), `alt="`+d.Description+`" src="diagrams/`+d.Name+`-light.svg"`) {
			t.Errorf("docs/architecture.md missing matching alt text for %s", d.Name)
		}
		for _, e := range d.Edges {
			seenTargets[e.To] = true
			if d.Name == "database-operations" {
				if e.From != "documents-ref" || e.To != "hosted_vector_revisions" {
					t.Errorf("unexpected ops edge: %+v", e)
				}
				if len(e.Points) < 4 || e.Points[1][0] <= 368 || e.Points[1][0] >= 484 {
					t.Errorf("ops edge does not route through central gutter: %+v", e.Points)
				}
				for _, want := range []string{"0..1", "documents.id", "cascading deletion", "pending active cleanup reservation", "PK denotes primary key", "FK denotes foreign key", "SQL types appear beside every column"} {
					if !strings.Contains(d.Description, want) {
						t.Errorf("ops description missing %q", want)
					}
				}
			}
		}
	}
	if len(schemas) != 2 {
		t.Fatalf("expected 2 schema scenes, got %d", len(schemas))
	}
	wantTargets := []string{"chunks", "document_fingerprints", "ingest_jobs", "document_pdf_extractions", "evaluation_fixtures", "source_uploads", "hosted_vector_revisions"}
	for _, target := range wantTargets {
		if !seenTargets[target] {
			t.Errorf("missing documents.id relationship to %s across schema pair", target)
		}
	}
}

func TestSchemaScenesHaveCapacityForNewColumns(t *testing.T) {
	f, err := loadFonts()
	if err != nil {
		t.Fatal(err)
	}
	ds, err := buildDiagrams("../..")
	if err != nil {
		t.Fatal(err)
	}
	measure := func(scenes []diagram) (map[string]int, int) {
		per := map[string]int{}
		total := 0
		for _, d := range scenes {
			for _, theme := range []string{"light", "dark"} {
				b, renderErr := render(d, theme, f)
				if renderErr != nil {
					t.Fatal(renderErr)
				}
				per[d.Name+"-"+theme] = len(b)
				total += len(b)
			}
		}
		return per, total
	}
	basePer, baseTotal := measure(ds)

	extended := cloneDiagrams(ds)
	additions := map[string][]column{
		"documents":               {{Name: "retention_class_label", Type: "TEXT"}, {Name: "source_ingest_revision", Type: "TEXT"}},
		"chunks":                  {{Name: "chunk_checksum_sha256", Type: "TEXT"}},
		"ingest_jobs":             {{Name: "worker_affinity_zone", Type: "TEXT"}, {Name: "queue_priority_weight", Type: "INTEGER"}},
		"source_uploads":          {{Name: "checksum_verified_at", Type: "TIMESTAMPTZ"}},
		"document_deletions":      {{Name: "retention_grace_until", Type: "TIMESTAMPTZ"}, {Name: "deletion_audit_actor", Type: "TEXT"}},
		"vector_generations":      {{Name: "generation_locked_at", Type: "TIMESTAMPTZ"}},
		"hosted_vector_manifests": {{Name: "manifest_digest_sha256", Type: "TEXT"}, {Name: "retention_policy_name", Type: "TEXT"}},
		"hosted_vector_cleanups":  {{Name: "cleanup_backoff_limit", Type: "INTEGER"}},
	}
	added := 0
	repackSchema := func(d *diagram) {
		var leftIdx, rightIdx []int
		for i, n := range d.Nodes {
			if cols, ok := additions[n.ID]; ok {
				added += len(cols)
				d.Nodes[i].Columns = append(d.Nodes[i].Columns, cols...)
				d.Nodes[i].Box.H += float64(len(cols)) * schemaRowHeight
			}
			if d.Nodes[i].Box.X < 400 {
				leftIdx = append(leftIdx, i)
			} else {
				rightIdx = append(rightIdx, i)
			}
		}
		pack := func(idxs []int) float64 {
			y := 78.0
			for _, idx := range idxs {
				d.Nodes[idx].Box.Y = y
				y += d.Nodes[idx].Box.H + 8
			}
			if len(idxs) == 0 {
				return 0
			}
			return y - 8
		}
		bottom := math.Max(pack(leftIdx), pack(rightIdx))
		d.Labels[0].Y = bottom + 40
		d.H = bottom + 62

		nodesByID := map[string]node{}
		for _, n := range d.Nodes {
			nodesByID[n.ID] = n
		}
		for i := range d.Edges {
			e := &d.Edges[i]
			child := nodesByID[e.To]
			for j, c := range child.Columns {
				if c.Reference == "documents.id" {
					y := child.Box.Y + schemaColumnBaseline(j) - 6
					e.Points[len(e.Points)-2][1] = y
					e.Points[len(e.Points)-1][1] = y
					if e.Label != nil {
						e.Label.Y = y - 14
					}
				}
			}
		}
	}
	projectedHeightLimits := map[string]float64{"database-schema": 1500, "database-operations": 1650}
	for name, limit := range projectedHeightLimits {
		d := diagramByName(t, extended, name)
		repackSchema(d)
		if d.H > limit {
			t.Errorf("extended %s height %g > %g", name, d.H, limit)
		}
		d.MaxH = math.Ceil(d.H/10) * 10
		if err := validate(*d, f); err != nil {
			t.Fatalf("extended %s failed layout validation: %v", name, err)
		}
	}
	if added != 12 {
		t.Fatalf("growth fixture added %d columns, want 12", added)
	}
	extPer, extTotal := measure(extended)
	t.Logf("baseline total=%d (schema=%d, ops=%d); +12 cols total=%d (schema=%d, ops=%d, delta=%+d)",
		baseTotal, basePer["database-schema-light"], basePer["database-operations-light"],
		extTotal, extPer["database-schema-light"], extPer["database-operations-light"], extTotal-baseTotal)
	for _, key := range []string{"database-schema-light", "database-schema-dark", "database-operations-light", "database-operations-dark"} {
		if extPer[key] >= 100000 {
			t.Errorf("extended %s is %d >= 100000", key, extPer[key])
		}
	}
	if extTotal >= 640000 {
		t.Errorf("extended combined size is %d >= 640000", extTotal)
	}
}

func TestParseSQLSchemaPanelColumnCoverage(t *testing.T) {
	malformed := []string{
		`CREATE TABLE broken (id UUID PRIMARY KEY`,
		`CREATE TABLE broken (id UUID, CONSTRAINT pk PRIMARY KEY (id));`,
		`CREATE TABLE broken (badcol);`,
		`CREATE TABLE broken (id UUID PRIMARY KEY); ALTER TABLE broken ADD COLUMN;`,
	}
	for _, sql := range malformed {
		if _, err := parseSQL(sql); err == nil {
			t.Errorf("malformed SQL accepted: %s", sql)
		}
	}

	valid := `CREATE TABLE alpha (id UUID PRIMARY KEY);
ALTER TABLE alpha ADD COLUMN IF NOT EXISTS label_one TEXT, ADD COLUMN label_two TIMESTAMPTZ;`
	got, err := parseSQL(valid)
	if err != nil {
		t.Fatalf("supported ADD COLUMN failed: %v", err)
	}
	if len(got["alpha"]) != 3 || got["alpha"]["label_two"].Type != "TIMESTAMPTZ" {
		t.Fatalf("unexpected parsed columns: %#v", got)
	}

	// Verify ADD COLUMN surfaces through checkSources against split schema panels.
	root := t.TempDir()
	for p := range directoryDescriptions {
		if mkErr := os.MkdirAll(filepath.Join(root, p), 0o750); mkErr != nil {
			t.Fatal(mkErr)
		}
	}
	for _, pattern := range []string{"internal/api/router.go", "internal/store/postgres/migrations/*.sql", "docs/diagrams/src/*.json"} {
		matches, globErr := filepath.Glob(filepath.Join("..", "..", pattern))
		if globErr != nil {
			t.Fatal(globErr)
		}
		for _, p := range matches {
			rel, relErr := filepath.Rel("../..", p)
			if relErr != nil {
				t.Fatal(relErr)
			}
			b, readErr := readWithin("../..", rel)
			if readErr != nil {
				t.Fatal(readErr)
			}
			if mkErr := os.MkdirAll(filepath.Join(root, filepath.Dir(rel)), 0o750); mkErr != nil {
				t.Fatal(mkErr)
			}
			if wrErr := writeWithin(root, rel, b); wrErr != nil {
				t.Fatal(wrErr)
			}
		}
	}
	trackDirectories(t, root, directoryDescriptions)
	ds, err := buildDiagrams(root)
	if err != nil {
		t.Fatal(err)
	}
	mig := filepath.Join(root, "internal", "store", "postgres", "migrations", "999_ops_col.sql")
	if err = os.WriteFile(mig, []byte("ALTER TABLE vector_state ADD COLUMN extra_state_flag BOOLEAN;"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err = checkSources(root, ds); err == nil {
		t.Fatal("missing operations column in diagram accepted")
	}
	state := diagramNodeByID(t, diagramByName(t, ds, "database-operations"), "vector_state")
	state.Columns = append(state.Columns, column{Name: "extra_state_flag", Type: "BOOLEAN"})
	if err = checkSources(root, ds); err != nil {
		t.Fatalf("updated operations panel rejected: %v", err)
	}
}
