package client_test

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/Victoroide/quarry-rag/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIngest(t *testing.T) {
	want := client.IngestRequest{DocumentID: "00000000-0000-4000-8000-000000000001", Content: "synthetic text", DocType: "guide", Metadata: map[string]any{"category": "example"}}
	c := newClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "/api/ingest", r.URL.Path)
		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))
		assert.Equal(t, "application/json", r.Header.Get("Accept"))
		var got client.IngestRequest
		assert.NoError(t, json.NewDecoder(r.Body).Decode(&got))
		assert.Equal(t, want, got)
		w.WriteHeader(http.StatusAccepted)
		writeJSON(t, w, client.IngestResponse{DocumentID: want.DocumentID, Status: "processing"})
	})
	got, err := c.Ingest(t.Context(), want)
	require.NoError(t, err)
	assert.Equal(t, &client.IngestResponse{DocumentID: want.DocumentID, Status: "processing"}, got)
}

func TestIngestCollection(t *testing.T) {
	for _, collection := range []string{"", "tenant-a"} {
		t.Run(collection, func(t *testing.T) {
			c := newClient(t, func(w http.ResponseWriter, r *http.Request) {
				var body map[string]any
				assert.NoError(t, json.NewDecoder(r.Body).Decode(&body))
				if collection == "" {
					assert.NotContains(t, body, "collection")
				} else {
					assert.Equal(t, "tenant-a", body["collection"])
				}
				w.WriteHeader(http.StatusAccepted)
				writeJSON(t, w, client.IngestResponse{DocumentID: "00000000-0000-4000-8000-000000000001", Status: "processing"})
			})
			_, err := c.Ingest(t.Context(), client.IngestRequest{Content: "synthetic text", Collection: collection})
			require.NoError(t, err)
		})
	}
}

func TestIngestFileCollection(t *testing.T) {
	for _, collection := range []string{"", "tenant-a"} {
		t.Run(collection, func(t *testing.T) {
			c := newClient(t, func(w http.ResponseWriter, r *http.Request) {
				r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
				form, err := r.MultipartReader()
				if !assert.NoError(t, err) {
					return
				}
				var collections []string
				for {
					part, partErr := form.NextPart()
					if errors.Is(partErr, io.EOF) {
						break
					}
					if !assert.NoError(t, partErr) {
						return
					}
					if part.FormName() == "collection" {
						value, readErr := io.ReadAll(part)
						assert.NoError(t, readErr)
						collections = append(collections, string(value))
					}
				}
				if collection == "" {
					assert.Empty(t, collections)
				} else {
					assert.Equal(t, []string{"tenant-a"}, collections)
				}
				w.WriteHeader(http.StatusAccepted)
				writeJSON(t, w, client.IngestResponse{DocumentID: "00000000-0000-4000-8000-000000000001", Status: "processing"})
			})
			_, err := c.IngestFile(t.Context(), client.FileUpload{Reader: strings.NewReader("synthetic file content"), Filename: "sample.md", Collection: collection})
			require.NoError(t, err)
		})
	}
}

func TestIngestFileStreamsMultipart(t *testing.T) {
	reader, writer := io.Pipe()
	defer func() { require.NoError(t, reader.Close()) }()
	c := newClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "/api/ingest", r.URL.Path)
		assert.EqualValues(t, -1, r.ContentLength)
		multipart, err := r.MultipartReader()
		if !assert.NoError(t, err) {
			return
		}
		fields := map[string]string{}
		for {
			part, nextErr := multipart.NextPart()
			if errors.Is(nextErr, io.EOF) {
				break
			}
			if !assert.NoError(t, nextErr) {
				return
			}
			if part.FormName() == "file" {
				assert.Equal(t, "sample.md", part.FileName())
				// Producing content after the multipart header arrives proves streaming.
				go func() { _, _ = io.WriteString(writer, "synthetic file content"); _ = writer.Close() }()
			}
			data, readErr := io.ReadAll(part)
			assert.NoError(t, readErr)
			fields[part.FormName()] = string(data)
		}
		assert.Equal(t, "synthetic file content", fields["file"])
		assert.Equal(t, "guide", fields["doc_type"])
		assert.Equal(t, "00000000-0000-4000-8000-000000000001", fields["document_id"])
		var metadata map[string]any
		assert.NoError(t, json.Unmarshal([]byte(fields["metadata"]), &metadata))
		assert.Equal(t, map[string]any{"category": "example"}, metadata)
		w.WriteHeader(http.StatusAccepted)
		writeJSON(t, w, client.IngestResponse{DocumentID: "00000000-0000-4000-8000-000000000001", Status: "processing"})
	})
	got, err := c.IngestFile(t.Context(), client.FileUpload{Reader: reader, Filename: "sample.md", DocumentID: "00000000-0000-4000-8000-000000000001", DocType: "guide", Metadata: map[string]any{"category": "example"}})
	require.NoError(t, err)
	assert.Equal(t, "processing", got.Status)
}

type brokenReader struct{}

func (brokenReader) Read([]byte) (int, error) { return 0, errors.New("source unreadable") }

func TestIngestFileReaderFailure(t *testing.T) {
	c := newClient(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusBadRequest)
	})
	_, err := c.IngestFile(t.Context(), client.FileUpload{Reader: brokenReader{}, Filename: "sample.txt"})
	require.Error(t, err)
	_, err = c.IngestFile(t.Context(), client.FileUpload{Filename: "sample.txt"})
	require.Error(t, err)
	_, err = c.IngestFile(t.Context(), client.FileUpload{Reader: strings.NewReader("synthetic"), Filename: "sample.txt", Metadata: map[string]any{"invalid": make(chan int)}})
	require.Error(t, err)
}
