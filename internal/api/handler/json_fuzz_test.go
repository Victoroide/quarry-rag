package handler

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"
)

func FuzzDecodeJSONObject(f *testing.F) {
	for _, body := range []string{
		`{}`, `{"query":"Archive hours?","max_chunks":3,"allowed_document_ids":["document"]}`,
		`{"content":"Readable document text.","metadata":{"page":1,"labels":["archive"]}}`,
		`{"query":"first","query":"second"}`, `{"query":"first","QUERY":"second"}`,
		`{"query":"first","\u0071uery":"second"}`, `{"unknown":true}`, `[]`, `null`, `{} {}`, `{"query":`,
	} {
		f.Add([]byte(body))
	}
	f.Fuzz(func(t *testing.T, body []byte) {
		if len(body) > 64<<10 {
			t.Skip()
		}
		checkJSONObjectRoundTrip[QueryRequest](t, body)
		checkJSONObjectRoundTrip[IngestRequest](t, body)
	})
}

func checkJSONObjectRoundTrip[T any](t *testing.T, body []byte) {
	t.Helper()
	var value T
	if err := decodeJSONObject(body, &value); err != nil {
		return
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	if token, err := decoder.Token(); err != nil || token != json.Delim('{') {
		t.Fatal("successful decode was not an object")
	}
	var keys []string
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			t.Fatal(err)
		}
		key, ok := token.(string)
		if !ok {
			t.Fatal("non-string object key")
		}
		for _, previous := range keys {
			if strings.EqualFold(previous, key) {
				t.Fatal("successful decode accepted a duplicate top-level key")
			}
		}
		keys = append(keys, key)
		var field json.RawMessage
		if err := decoder.Decode(&field); err != nil {
			t.Fatal(err)
		}
	}
	if token, err := decoder.Token(); err != nil || token != json.Delim('}') {
		t.Fatal("successful decode did not end the object")
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		t.Fatal("successful decode accepted trailing data")
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal %T: %v", value, err)
	}
	var again T
	if err := decodeJSONObject(encoded, &again); err != nil {
		t.Fatalf("decode marshaled %T: %v", value, err)
	}
	if !reflect.DeepEqual(value, again) {
		t.Fatalf("%T changed after JSON round trip", value)
	}
}
