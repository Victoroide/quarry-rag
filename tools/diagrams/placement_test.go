package main

import (
	"bytes"
	"encoding/json/jsontext"
	jsonv2 "encoding/json/v2"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestDecodePlacement(t *testing.T) {
	for _, tc := range []struct {
		name    string
		input   string
		cause   error
		message string
	}{
		{name: "empty"},
		{name: "whitespace only", input: " \r\n\t"},
		{name: "null", input: `null`, message: "expected a placement object"},
		{name: "array", input: `[]`},
		{name: "string", input: `"placement"`},
		{name: "number", input: `1`},
		{name: "boolean", input: `true`},
		{name: "malformed root", input: `{Width:840}`},
		{name: "truncated root", input: `{"Width":840`},
		{name: "malformed child", input: `{"Width":840,"Nodes":{"a":{"X":1,}}}`},
		{name: "truncated child", input: `{"Width":840,"Groups":[{"Box":{"X":`},
		{name: "invalid UTF-8", input: "{\"Groups\":[{\"Title\":\"\xff\"}]}"},
		{name: "invalid UTF-16 escape", input: `{"Groups":[{"Title":"\ud800"}]}`},
		{name: "trailing garbage", input: `{} !`},
		{name: "trailing scalar", input: `{} true`},
		{name: "trailing object", input: `{} {}`},
		{name: "trailing array", input: `{} []`},
		{name: "duplicate width", input: `{"Width":840,"Width":820}`, cause: jsontext.ErrDuplicateName},
		{name: "duplicate nodes", input: `{"Nodes":{"a":{}},"Nodes":{"b":{}}}`, cause: jsontext.ErrDuplicateName},
		{name: "duplicate node ID", input: `{"Nodes":{"a":{"X":1},"a":{"X":2}}}`, cause: jsontext.ErrDuplicateName},
		{name: "duplicate group field with equal values", input: `{"Groups":[{"Title":"g","Title":"g"}]}`, cause: jsontext.ErrDuplicateName},
		{name: "duplicate group box", input: `{"Groups":[{"Box":{"X":1},"Box":{"Y":2}}]}`, cause: jsontext.ErrDuplicateName},
		{name: "duplicate nested coordinate", input: `{"Nodes":{"a":{"Y":1,"Y":2}}}`, cause: jsontext.ErrDuplicateName},
		{name: "escaped duplicate node ID", input: `{"Nodes":{"a":{},"\u0061":{}}}`, cause: jsontext.ErrDuplicateName},
		{name: "escaped duplicate field", input: `{"Width":840,"\u0057idth":840}`, cause: jsontext.ErrDuplicateName},
		{name: "root aliases", input: `{"width":840,"Width":840}`, cause: jsontext.ErrDuplicateName},
		{name: "group aliases", input: `{"Groups":[{"Box":{},"box":{}}]}`, cause: jsontext.ErrDuplicateName},
		{name: "nested aliases", input: `{"Groups":[{"Box":{"X":1,"x":2}}]}`, cause: jsontext.ErrDuplicateName},
		{name: "dashed field", input: `{"Wi-dth":840}`, cause: jsonv2.ErrUnknownName},
		{name: "underscored field", input: `{"Wi_dth":840}`, cause: jsonv2.ErrUnknownName},
		{name: "unknown root field", input: `{"Depth":840}`, cause: jsonv2.ErrUnknownName},
		{name: "unknown group field", input: `{"Groups":[{"Name":"g"}]}`, cause: jsonv2.ErrUnknownName},
		{name: "unknown group box field", input: `{"Groups":[{"Box":{"Z":1}}]}`, cause: jsonv2.ErrUnknownName},
		{name: "unknown node box field", input: `{"Nodes":{"a":{"Z":1}}}`, cause: jsonv2.ErrUnknownName},
		{name: "wrong width type", input: `{"Width":"840"}`},
		{name: "wrong nodes type", input: `{"Nodes":[]}`},
		{name: "wrong groups type", input: `{"Groups":{}}`},
		{name: "wrong coordinate type", input: `{"Nodes":{"a":{"X":true}}}`},
		{name: "number overflow", input: `{"Width":1e999}`},
		{name: "excessive nesting", input: strings.Repeat("[", 12_000) + strings.Repeat("]", 12_000)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := decodePlacement([]byte(tc.input))
			if err == nil {
				t.Fatalf("invalid placement accepted: %+v", got)
			}
			if tc.cause != nil && !errors.Is(err, tc.cause) {
				t.Errorf("got %v, want an error wrapping %v", err, tc.cause)
			}
			if tc.message != "" && !strings.Contains(err.Error(), tc.message) {
				t.Errorf("got %v, want %q", err, tc.message)
			}
			if !reflect.DeepEqual(got, placement{}) {
				t.Errorf("partial placement returned: %+v", got)
			}
		})
	}

	for _, tc := range []struct {
		name  string
		input string
		want  placement
	}{
		{
			name:  "canonical object with JSON whitespace",
			input: `{"Width":840,"Height":760,"Nodes":{"a":{"X":1,"Y":2,"W":3,"H":4}},"Groups":[{"Title":"g","Category":"c","Box":{"X":5}}]}` + " \r\n\t",
			want: placement{Width: 840, Height: 760, Nodes: map[string]box{"a": {X: 1, Y: 2, W: 3, H: 4}},
				Groups: []group{{Title: "g", Category: "c", Box: box{X: 5}}}},
		},
		{
			name:  "case-insensitive fields and case-sensitive node IDs",
			input: `{"width":840,"nOdEs":{"a":{"x":1},"A":{"x":2}},"groups":[{"title":"g1","box":{"w":3}},{"title":"g2","box":{"w":4}}]}`,
			want: placement{Width: 840, Nodes: map[string]box{"a": {X: 1}, "A": {X: 2}},
				Groups: []group{{Title: "g1", Box: box{W: 3}}, {Title: "g2", Box: box{W: 4}}}},
		},
		{name: "omitted fields", input: `{}`},
		{name: "null optional values", input: `{"Width":null,"Nodes":null,"Groups":null}`},
		{name: "empty collections", input: `{"Nodes":{},"Groups":[]}`, want: placement{Nodes: map[string]box{}, Groups: []group{}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := decodePlacement([]byte(tc.input))
			if err != nil || !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("got %+v, %v; want %+v", got, err, tc.want)
			}
		})
	}

	t.Run("wide unique node map", func(t *testing.T) {
		const count = 4096
		nodes := make([]string, count)
		for i := range nodes {
			nodes[i] = fmt.Sprintf(`"node-%d":{"X":%d}`, i, i)
		}
		got, err := decodePlacement([]byte(`{"Nodes":{` + strings.Join(nodes, ",") + "}}"))
		if err != nil || len(got.Nodes) != count {
			t.Fatalf("decoded %d of %d nodes: %v", len(got.Nodes), count, err)
		}
		for i := range nodes {
			key := fmt.Sprintf("node-%d", i)
			if node, ok := got.Nodes[key]; !ok || node.X != float64(i) {
				t.Errorf("wrong placement for %s: %+v", key, node)
			}
		}
	})
}

func FuzzDecodePlacement(f *testing.F) {
	root := filepath.Join("..", "..")
	sources, err := filepath.Glob(filepath.Join(root, "docs", "diagrams", "src", "*.json"))
	if err != nil {
		f.Fatal(err)
	}
	if len(sources) == 0 {
		f.Fatal("no placement sources found")
	}
	for _, path := range sources {
		rel, relErr := filepath.Rel(root, path)
		if relErr != nil {
			f.Fatal(relErr)
		}
		data, readErr := readWithin(root, rel)
		if readErr != nil {
			f.Fatal(readErr)
		}
		f.Add(data)
	}
	for _, seed := range []string{
		`{}`, `null`, `{} {}`, `{"Width":1,"width":2}`,
		`{"Nodes":{"a":{},"\u0061":{}}}`, `{"Nodes":{"a":{"X":`,
		"{\"Groups\":[{\"Title\":\"\xff\"}]}",
	} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		got, err := decodePlacement(data)
		again, againErr := decodePlacement(data)
		if (err == nil) != (againErr == nil) || !reflect.DeepEqual(got, again) {
			t.Fatalf("nondeterministic decode: %+v, %v and %+v, %v", got, err, again, againErr)
		}
		if err != nil {
			if !reflect.DeepEqual(got, placement{}) {
				t.Fatalf("partial placement returned: %+v", got)
			}
			return
		}
		for _, suffix := range []string{" {}", " !"} {
			got, err := decodePlacement(append(bytes.Clone(data), suffix...))
			if err == nil || !reflect.DeepEqual(got, placement{}) {
				t.Errorf("appended %q: got %+v, %v", suffix, got, err)
			}
		}
	})
}
