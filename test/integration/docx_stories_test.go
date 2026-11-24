package integration

import (
	"bytes"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/api"
	"github.com/Victoroide/quarry-rag/internal/api/handler"
	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/testutil/docxtest"
	"github.com/Victoroide/quarry-rag/internal/wordprocessingml"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

const docxBodyText = "This text is shared across collections."

func docxUploadWithHeader(t *testing.T, collection, id, header string, change ...func(map[string]string)) *http.Request {
	t.Helper()
	word, office := wordprocessingml.Word, wordprocessingml.OfficeRelationships
	parts := map[string]string{
		"word/document.xml": docxtest.Document(word, office, `
			<w:p><w:r><w:t>`+docxBodyText+`</w:t><w:commentReference w:id="1"/></w:r></w:p>
			<w:sectPr><w:headerReference r:id="h" w:type="default"/></w:sectPr>
			`),
		"word/_rels/document.xml.rels": docxtest.Relationships(
			docxtest.Relationship(office, "header", "h", "stories/heading.xml"),
			docxtest.Relationship(office, "comments", "c", "stories/discussion.xml")),
		"word/stories/heading.xml": docxtest.Story(word, "hdr", header),
		"word/stories/discussion.xml": docxtest.Story(word, "comments", `
			<w:comment w:id="1" w:author="Fixture annotation author" w:initials="FAA">
			<w:p><w:r><w:t>Selected annotation text.</w:t></w:r></w:p>
			</w:comment>`),
	}
	for _, modify := range change {
		if modify != nil {
			modify(parts)
		}
	}
	return docxUpload(t, collection, id, docxtest.Package(t, parts))
}

func docxUpload(t *testing.T, collection, id string, data []byte) *http.Request {
	t.Helper()
	var body bytes.Buffer
	form := multipart.NewWriter(&body)
	part, err := form.CreateFormFile("file", "stories.docx")
	require.NoError(t, err)
	_, err = part.Write(data)
	require.NoError(t, err)
	require.NoError(t, form.WriteField("collection", collection))
	if id != "" {
		require.NoError(t, form.WriteField("document_id", id))
	}
	require.NoError(t, form.Close())

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/ingest", &body)
	req.Header.Set("Content-Type", form.FormDataContentType())
	return req
}

func TestIngestDOCXStoriesStayScopedAndAtomic(t *testing.T) {
	store := durableDatabase(t)
	t.Setenv("GO_RAG_FAKE_MODE", "true")
	t.Setenv("DATABASE_URL", "host=localhost dbname=rag")
	t.Setenv("EXTRACT_ISOLATION", "false")
	const good = "<w:p><w:r><w:t>Selected heading text.</w:t></w:r></w:p>"
	const malformed = good + "<w:truncated>"
	router := func(t *testing.T, stories string) http.Handler {
		t.Helper()
		t.Setenv("DOCX_STORIES", stories)
		cfg, err := config.Load()
		require.NoError(t, err)
		return api.NewRouter(&api.RouterDeps{Config: cfg, ChunkStore: store})
	}
	post := func(h http.Handler, req *http.Request) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec
	}

	defaults := router(t, "")
	selected, replica := router(t, "body,headers,comments"), router(t, "comments,headers,body")
	defaultIDs, selectedIDs := map[string]string{}, map[string]string{}
	for _, collection := range []string{"", "archive"} {
		original := privateResponse(t, post(defaults, privateJSON(t, handler.IngestRequest{
			Content: docxBodyText, Collection: collection,
		})), http.StatusAccepted)
		// An unselected header does not affect extraction or the original fingerprint.
		defaultDOCX := privateResponse(t, post(defaults, docxUploadWithHeader(t, collection, "", malformed)), http.StatusAccepted)
		require.Equal(t, original, defaultDOCX)
		defaultIDs[collection] = original.DocumentID

		accepted := post(selected, docxUploadWithHeader(t, collection, "", good))
		require.NotContains(t, accepted.Body.String(), "Fixture annotation author")
		enriched := privateResponse(t, accepted, http.StatusAccepted)
		require.NotEqual(t, original.DocumentID, enriched.DocumentID)
		selectedIDs[collection] = enriched.DocumentID
		retry := privateResponse(t, post(replica, docxUploadWithHeader(t, collection, "", good)), http.StatusAccepted)
		require.Equal(t, enriched, retry)
		doc, err := store.GetDocument(t.Context(), domain.DocumentID(enriched.DocumentID))
		require.NoError(t, err)
		require.NotNil(t, doc)
		require.Equal(t, docxBodyText+"\n\n[DOCX header]\nSelected heading text.\n\n[DOCX comment]\nSelected annotation text.", doc.Content)
		require.Equal(t, map[string]any{"source_file": "stories.docx", "doc_type": "document"}, doc.Metadata)

		conflict := post(selected, docxUploadWithHeader(t, collection, original.DocumentID, good))
		require.Equal(t, http.StatusConflict, conflict.Code)
		originalDoc, err := store.GetDocument(t.Context(), domain.DocumentID(original.DocumentID))
		require.NoError(t, err)
		require.NotNil(t, originalDoc)
		require.Equal(t, docxBodyText, originalDoc.Content)
	}
	require.NotEqual(t, defaultIDs[""], defaultIDs["archive"])
	require.NotEqual(t, selectedIDs[""], selectedIDs["archive"])

	const countAccepted = `SELECT (SELECT count(*) FROM documents),
		(SELECT count(*) FROM ingest_jobs), (SELECT count(*) FROM document_fingerprints)`
	readCounts := func() [3]int {
		t.Helper()
		var counts [3]int
		require.NoError(t, store.Pool.QueryRow(t.Context(), countAccepted).Scan(&counts[0], &counts[1], &counts[2]))
		return counts
	}
	before := readCounts()
	require.Equal(t, 4, before[0])
	require.Equal(t, 4, before[1])

	t.Run("selected header namespace binding", func(t *testing.T) {
		t.Setenv("EXTRACT_ISOLATION", "true")
		isolated := router(t, "body,headers")
		const secondLine = `<extra:p><extra:r><extra:t>Second heading line.</extra:t></extra:r></extra:p>`
		declared := good + strings.Replace(secondLine, "<extra:p>", `<extra:p xmlns:extra="`+wordprocessingml.Word+`">`, 1)
		initial := readCounts()
		accepted := privateResponse(t, post(isolated, docxUploadWithHeader(t, "archive", "", declared)), http.StatusAccepted)
		doc, err := store.GetDocument(t.Context(), domain.DocumentID(accepted.DocumentID))
		require.NoError(t, err)
		require.NotNil(t, doc)
		require.Equal(t, docxBodyText+"\n\n[DOCX header]\nSelected heading text.\nSecond heading line.", doc.Content)
		afterControl := readCounts()
		require.Equal(t, [3]int{initial[0] + 1, initial[1] + 1, initial[2] + 1}, afterControl)

		failedID := uuid.NewString()
		rec := post(isolated, docxUploadWithHeader(t, "archive", failedID, good+secondLine))
		if rec.Code != http.StatusUnprocessableEntity {
			t.Errorf("malformed header returned %d, want %d: %s", rec.Code, http.StatusUnprocessableEntity, rec.Body)
		}
		require.Equal(t, afterControl, readCounts())
		require.JSONEq(t, `{"error":"extraction","message":"document could not be parsed","code":"unparseable"}`, rec.Body.String())
		doc, err = store.GetDocument(t.Context(), domain.DocumentID(failedID))
		require.NoError(t, err)
		require.Nil(t, doc)
	})

	t.Run("selection applies in process beside DOC_STORIES", func(t *testing.T) {
		t.Setenv("DOC_STORIES", "headers")
		inProcess := router(t, "body,headers")
		accepted := privateResponse(t, post(inProcess, docxUploadWithHeader(t, "archive", "", "<w:p><w:r><w:t>In-process heading text.</w:t></w:r></w:p>")), http.StatusAccepted)
		doc, err := store.GetDocument(t.Context(), domain.DocumentID(accepted.DocumentID))
		require.NoError(t, err)
		require.NotNil(t, doc)
		require.Equal(t, docxBodyText+"\n\n[DOCX header]\nIn-process heading text.", doc.Content)
	})

	t.Run("selected header must understand its namespaces", func(t *testing.T) {
		t.Setenv("EXTRACT_ISOLATION", "true")
		isolated := router(t, "body,headers")
		alternate := func(mustUnderstand string) string {
			return `<mc:AlternateContent xmlns:mc="` + wordprocessingml.Compatibility + `" xmlns:ex="urn:fixture:extension" mc:Ignorable="ex">` +
				`<mc:Choice Requires="w" mc:MustUnderstand="` + mustUnderstand + `">` + good + `<ex:line>Extension-only line.</ex:line></mc:Choice>` +
				`<mc:Fallback><w:p><w:r><w:t>Fallback heading text.</w:t></w:r></w:p></mc:Fallback></mc:AlternateContent>`
		}
		initial := readCounts()
		accepted := privateResponse(t, post(isolated, docxUploadWithHeader(t, "archive", "", alternate("w"))), http.StatusAccepted)
		doc, err := store.GetDocument(t.Context(), domain.DocumentID(accepted.DocumentID))
		require.NoError(t, err)
		require.NotNil(t, doc)
		require.Equal(t, docxBodyText+"\n\n[DOCX header]\nSelected heading text.", doc.Content)
		afterControl := readCounts()
		require.Equal(t, [3]int{initial[0] + 1, initial[1] + 1, initial[2] + 1}, afterControl)

		failedID := uuid.NewString()
		rec := post(isolated, docxUploadWithHeader(t, "archive", failedID, alternate("ex")))
		require.Equal(t, http.StatusUnprocessableEntity, rec.Code, rec.Body.String())
		require.JSONEq(t, `{"error":"extraction","message":"document could not be parsed","code":"unparseable"}`, rec.Body.String())
		require.Equal(t, afterControl, readCounts())
		doc, err = store.GetDocument(t.Context(), domain.DocumentID(failedID))
		require.NoError(t, err)
		require.Nil(t, doc)
	})

	for _, tt := range []struct {
		name, header string
		change       func(map[string]string)
	}{
		{"truncated header", malformed, nil},
		{"repeated xml attribute", `<w:p><w:r><w:t xml:space="preserve" xml:space="default">Malformed heading.</w:t></w:r></w:p>`, nil},
		{"missing comment ID", good, func(parts map[string]string) {
			name := "word/document.xml"
			parts[name] = strings.Replace(parts[name], `<w:commentReference w:id="1"/>`, `<w:commentReference w:id="1"/><w:commentReference w:id="2"/>`, 1)
		}},
		{"missing comments relationship", good, func(parts map[string]string) {
			name := "word/_rels/document.xml.rels"
			start := strings.Index(parts[name], `<Relationship Id="c"`)
			end := strings.Index(parts[name][start:], "/>") + start + 2
			parts[name] = parts[name][:start] + parts[name][end:]
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			before := readCounts()
			failedID := uuid.NewString()
			rec := post(selected, docxUploadWithHeader(t, "archive", failedID, tt.header, tt.change))
			require.Equal(t, http.StatusUnprocessableEntity, rec.Code, rec.Body.String())
			require.JSONEq(t, `{"error":"extraction","message":"document could not be parsed","code":"unparseable"}`, rec.Body.String())
			require.Equal(t, before, readCounts())
			doc, err := store.GetDocument(t.Context(), domain.DocumentID(failedID))
			require.NoError(t, err)
			require.Nil(t, doc)
		})
	}

	t.Run("renamed main exceeds combined XML work", func(t *testing.T) {
		before := readCounts()
		failedID := uuid.NewString()
		data := docxtest.RenamedMainAtWorkLimit(t, 16<<20, 50<<20+1)
		rec := post(selected, docxUpload(t, "archive", failedID, data))
		require.Equal(t, http.StatusUnprocessableEntity, rec.Code, rec.Body.String())
		require.JSONEq(t, `{"error":"extraction","message":"document could not be parsed","code":"unparseable"}`, rec.Body.String())
		require.Equal(t, before, readCounts())
	})
}
