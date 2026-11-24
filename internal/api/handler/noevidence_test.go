package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/Victoroide/quarry-rag/client"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/llm"
	"github.com/Victoroide/quarry-rag/internal/pipeline"
	"github.com/Victoroide/quarry-rag/internal/retriever"
	"github.com/Victoroide/quarry-rag/internal/store/memory"
	"github.com/stretchr/testify/require"
)

const catalogAnswer = `لَمْ نَجِدْ معلوماتٍ عن "<الأرشيف>" & السجلّات.`

const privateArchiveID = "00000000-0000-4000-8000-000000000001"

func queryTestNoEvidenceCatalog(t *testing.T) *pipeline.NoEvidenceCatalog {
	t.Helper()
	return loadQueryTestCatalog(t, map[string]string{"ar": catalogAnswer})
}

func loadQueryTestCatalog(t *testing.T, entries map[string]string) *pipeline.NoEvidenceCatalog {
	t.Helper()
	body, err := json.Marshal(entries)
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "catalog.json")
	require.NoError(t, os.WriteFile(path, body, 0o600))
	catalog, err := pipeline.LoadNoEvidenceCatalog(path)
	require.NoError(t, err)
	return catalog
}

func TestQueryCatalogNegotiatesExactTagsAndRanges(t *testing.T) {
	const german = "Es wurden keine relevanten Informationen gefunden, um diese Frage zu beantworten."
	const english = "No relevant information was found to answer this question."
	const valencian = "No s'ha trobat informació rellevant."
	const catalan = "No s'ha trobat informació pertinent."
	catalog := loadQueryTestCatalog(t, map[string]string{
		"ca": catalan, "ca-valencia": valencian, "ar": catalogAnswer, "de-1901": "Traditional German spelling",
	})
	h := QueryHandler(retriever.NewRetriever(nil, nil, .7, 10), nil, streamQueryConfig(), nil, WithNoEvidenceCatalog(catalog))
	for _, tc := range []struct{ name, header, tag, answer string }{
		{"Valencian", "ca-valencia", "ca-valencia", valencian},
		{"Catalan fallback", "ca-ES", "ca", catalan},
		{"weighted exact", "ca;q=0.4, ca-valencia;q=0.7", "ca-valencia", valencian},
		{"German alias", "ger-Latn", "de", german},
		{"German variant", "de-1901", "de-1901", "Traditional German spelling"},
		{"uppercase weight", "ar;Q=0.9", "ar", catalogAnswer},
		{"exponent weight", "ar;q=1e-1", "en", english},
		{"excess precision", "ar;q=0.9000", "en", english},
		{"malformed range", "ar_EG", "en", english},
		{"mixed malformed list", "ar, ca;q=1e-1", "en", english},
	} {
		t.Run(tc.name, func(t *testing.T) { requireNoEvidenceTranslation(t, h, tc.header, tc.tag, tc.answer) })
	}
}

func TestQueryCatalogPreservesScriptAndRegionalFallback(t *testing.T) {
	const english = "No relevant information was found to answer this question."
	const german = "Es wurden keine relevanten Informationen gefunden, um diese Frage zu beantworten."
	const traditional = "沒有找到相關資訊。"
	const portuguese = "Não encontrámos informação relevante."
	ret := retriever.NewRetriever(nil, nil, .7, 10)
	builtins := QueryHandler(ret, nil, streamQueryConfig(), nil)
	catalog := loadQueryTestCatalog(t, map[string]string{
		"zh": "没有找到相关信息。", "zh-Hant-TW": traditional, "pt-PT": portuguese,
	})
	configured := QueryHandler(ret, nil, streamQueryConfig(), nil, WithNoEvidenceCatalog(catalog))
	for _, tc := range []struct {
		name, header, tag, answer string
		handler                   http.HandlerFunc
	}{
		{"unsupported built-in script", "fr-Brai", "en", english, builtins},
		{"unsupported script with another preference", "fr-Brai, de;q=0.8", "de", german, builtins},
		{"configured compatible script", "zh-Hant-HK", "zh-Hant-TW", traditional, configured},
		{"configured compatible region", "pt-AO", "pt-PT", portuguese, configured},
		{"configured compatible region with suffix", "pt-AO-x-extra", "pt-PT", portuguese, configured},
	} {
		t.Run(tc.name, func(t *testing.T) { requireNoEvidenceTranslation(t, tc.handler, tc.header, tc.tag, tc.answer) })
	}
}

func TestQueryCatalogServesRegisteredSpellingsAsOneLanguage(t *testing.T) {
	const english = "No relevant information was found to answer this question."
	const hainanese = "Hainanese fixed answer."
	const suma = "Suma fixed answer."
	const interslavic = "Interslavic fixed answer."
	const signLanguage = "American Sign Language fixed answer."
	catalog := loadQueryTestCatalog(t, map[string]string{"zh-hnm": hainanese, "dek": suma, "isv-Latn": interslavic, "sgn-US": signLanguage})
	h := QueryHandler(retriever.NewRetriever(nil, nil, .7, 10), nil, streamQueryConfig(), nil, WithNoEvidenceCatalog(catalog))
	for _, tc := range []struct{ name, header, tag, answer string }{
		{"primary Hainanese", "hnm", "hnm", hainanese},
		{"extended Hainanese", "zh-hnm", "hnm", hainanese},
		{"weighted uppercase extended Hainanese", "de;q=0.5, ZH-HNM;q=0.9", "hnm", hainanese},
		{"Hainanese region", "hnm-CN", "en", english},
		{"deprecated Suma", "dek", "sqm", suma},
		{"preferred Suma", "sqm", "sqm", suma},
		{"Interslavic script", "isv-latn", "isv-Latn", interslavic},
		{"other Interslavic script", "isv-Cyrl", "en", english},
		{"deprecated sign language tag", "sgn-US", "ase", signLanguage},
		{"preferred sign language", "ase", "ase", signLanguage},
		{"extended sign language", "sgn-ase", "ase", signLanguage},
	} {
		t.Run(tc.name, func(t *testing.T) { requireNoEvidenceTranslation(t, h, tc.header, tc.tag, tc.answer) })
	}
}

func TestQueryCatalogCanonicalizesCompoundLanguageTags(t *testing.T) {
	catalog := loadQueryTestCatalog(t, map[string]string{
		"isv-MM":                   "Interslavic fixed text",
		"he-Gara":                  "Hebrew fixed text",
		"hnm-u-ca-gregory-nu-latn": "Hainanese fixed text",
	})
	h := QueryHandler(retriever.NewRetriever(nil, nil, .7, 10), nil, streamQueryConfig(), nil, WithNoEvidenceCatalog(catalog))
	for _, tc := range []struct{ header, tag, answer string }{
		{"isv-BU", "isv-MM", "Interslavic fixed text"},
		{"iw-Gara", "he-Gara", "Hebrew fixed text"},
		{"hnm-u-nu-latn-ca-gregory", "hnm-u-ca-gregory-nu-latn", "Hainanese fixed text"},
	} {
		t.Run(tc.header, func(t *testing.T) { requireNoEvidenceTranslation(t, h, tc.header, tc.tag, tc.answer) })
	}
}

func TestQueryCatalogCanonicalizesLanguageExtensions(t *testing.T) {
	for _, pair := range []struct{ alternate, canonical string }{
		{"ar-u-ca-islamicc", "ar-u-ca-islamic-civil"},
		{"isv-u-nu-latn-ca-islamicc", "isv-u-ca-islamic-civil-nu-latn"},
		{"ar-u-kn-true", "ar-u-kn"},
		{"isv-u-kn-true", "isv-u-kn"},
		{"ar-t-m0-xml-d0-hex", "ar-t-d0-hex-m0-xml"},
		{"isv-t-m0-xml-d0-hex", "isv-t-d0-hex-m0-xml"},
		{"und-t-es-m0-xml-d0-hex", "und-t-es-d0-hex-m0-xml"},
	} {
		for _, spelling := range []struct{ installed, requested string }{
			{pair.alternate, pair.canonical},
			{pair.canonical, pair.alternate},
		} {
			t.Run(spelling.installed, func(t *testing.T) {
				const answer = "Fixed answer."
				catalog := loadQueryTestCatalog(t, map[string]string{spelling.installed: answer})
				h := QueryHandler(retriever.NewRetriever(nil, nil, .7, 10), nil, streamQueryConfig(), nil, WithNoEvidenceCatalog(catalog))
				requireNoEvidenceTranslation(t, h, spelling.requested, pair.canonical, answer)
			})
		}
	}
}

func TestQueryCatalogNegotiatesEquivalentTransforms(t *testing.T) {
	const preferred = "Proper names."
	const distinct = "UNGEGN."
	const german = "Es wurden keine relevanten Informationen gefunden, um diese Frage zu beantworten."
	for _, base := range []string{"ar", "isv"} {
		t.Run(base, func(t *testing.T) {
			canonical := base + "-t-m0-prprname"
			ungegn := base + "-t-m0-ungegn"
			catalog := loadQueryTestCatalog(t, map[string]string{canonical: preferred, ungegn: distinct})
			h := QueryHandler(retriever.NewRetriever(nil, nil, .7, 10), nil, streamQueryConfig(), nil, WithNoEvidenceCatalog(catalog))
			for _, tc := range []struct{ request, tag, answer string }{
				{base + "-t-m0-names", canonical, preferred},
				{canonical, canonical, preferred},
				{ungegn, ungegn, distinct},
				{"isv-t-m0-ies-jes", "de", german},
			} {
				t.Run(tc.request, func(t *testing.T) {
					requireNoEvidenceTranslation(t, h, tc.request+", de;q=0.4", tc.tag, tc.answer)
				})
			}
		})
	}
}

func TestQueryCatalogTruncatesLanguageRangesInRequestOrder(t *testing.T) {
	for _, tc := range []struct {
		name, header, tag, answer string
		entries                   map[string]string
	}{
		{"Greenlandic orthography", "kl-tunumiit-kleinsch", "kl-kleinsch-tunumiit", "Orthography", map[string]string{"kl": "Generic", "kl-tunumiit": "Dialect", "kl-tunumiit-kleinsch": "Orthography"}},
		{"Greenlandic dialect", "kl-tunumiit-kleinsch", "kl-tunumiit", "Dialect", map[string]string{"kl": "Generic", "kl-tunumiit": "Dialect"}},
		{"Greenlandic base", "kl-tunumiit-kleinsch", "kl", "Generic", map[string]string{"kl": "Generic"}},
		{"Unicode field order", "ar-u-nu-latn-ca-gregory", "ar-u-nu-latn", "Numerals", map[string]string{"ar-u-nu-latn": "Numerals", "ar-u-ca-gregory-nu": "Calendar"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			catalog := loadQueryTestCatalog(t, tc.entries)
			h := QueryHandler(retriever.NewRetriever(nil, nil, .7, 10), nil, streamQueryConfig(), nil, WithNoEvidenceCatalog(catalog))
			requireNoEvidenceTranslation(t, h, tc.header, tc.tag, tc.answer)
		})
	}
}

func TestQueryCatalogLooksUpPrivateUseParents(t *testing.T) {
	const parent = "Private catalog translation."
	const child = "Private child translation."
	const german = "Es wurden keine relevanten Informationen gefunden, um diese Frage zu beantworten."
	for _, tc := range []struct {
		name, header, tag, answer string
		entries                   map[string]string
	}{
		{"short parent", "x-lab-qa, de;q=0.4", "x-lab", parent, map[string]string{"x-lab": parent}},
		{"long parent", "x-team-us, de;q=0.4", "x-team", parent, map[string]string{"x-team": parent}},
		{"closest parent", "x-lab-qa-ops, de;q=0.4", "x-lab-qa", child, map[string]string{"x-lab": parent, "x-lab-qa": child}},
		{"exact child", "x-lab-qa, de;q=0.4", "x-lab-qa", child, map[string]string{"x-lab": parent, "x-lab-qa": child}},
		{"absent parent", "x-lab-qa, de;q=0.4", "de", german, map[string]string{"x-team": parent}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			catalog := loadQueryTestCatalog(t, tc.entries)
			h := QueryHandler(retriever.NewRetriever(nil, nil, .7, 10), nil, streamQueryConfig(), nil, WithNoEvidenceCatalog(catalog))
			requireNoEvidenceTranslation(t, h, tc.header, tc.tag, tc.answer)
		})
	}
}

func TestQueryCatalogServesDistinctPrivateUseTags(t *testing.T) {
	entries := map[string]string{
		"x-heploc": "First private text", "x-alalc97": "Second private text",
		"x-team-heploc": "First team text", "x-team-alalc97": "Second team text",
		"ja-x-heploc": "First Japanese private text", "ja-x-alalc97": "Second Japanese private text",
	}
	catalog := loadQueryTestCatalog(t, entries)
	h := QueryHandler(retriever.NewRetriever(nil, nil, .7, 10), nil, streamQueryConfig(), nil, WithNoEvidenceCatalog(catalog))
	for tag, answer := range entries {
		t.Run(tag, func(t *testing.T) { requireNoEvidenceTranslation(t, h, tag, tag, answer) })
	}
}

func requireNoEvidenceTranslation(t *testing.T, h http.HandlerFunc, header, tag, answer string) {
	t.Helper()
	for _, accept := range []string{"application/json", "text/event-stream"} {
		t.Run(accept, func(t *testing.T) {
			req := streamQueryRequest(t.Context(), `{"query":"archive","allowed_document_ids":[]}`)
			req.Header.Set("Accept", accept)
			req.Header.Set("Accept-Language", header)
			rec := httptest.NewRecorder()
			h(rec, req)
			headers := requireQueryResponseHeaders(t, rec, http.StatusOK, accept)
			require.Equal(t, tag, headers.Get("Content-Language"))
			if accept == "text/event-stream" {
				events := recordedEvents(t, rec.Body.String())
				require.Len(t, events, 3)
				require.Equal(t, "delta", events[1].name)
				var got string
				require.NoError(t, json.Unmarshal(events[1].data["text"], &got))
				require.Equal(t, answer, got)
				return
			}
			var got QueryResponse
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
			require.Equal(t, answer, got.Answer)
		})
	}
}

type noEvidenceProviders struct {
	*memory.FakeVectorStore
	embeds  atomic.Int32
	queries atomic.Int32
	models  atomic.Int32
	embedOK bool
}

func (p *noEvidenceProviders) Embed(context.Context, string) ([]float32, error) {
	p.embeds.Add(1)
	if !p.embedOK {
		return nil, domain.ErrUpstreamThrottled
	}
	return []float32{1}, nil
}

func (p *noEvidenceProviders) QueryFiltered(ctx context.Context, vector []float32, topK int, filter domain.VectorFilter) ([]domain.ScoredChunk, error) {
	p.queries.Add(1)
	return p.FakeVectorStore.QueryFiltered(ctx, vector, topK, filter)
}

func (p *noEvidenceProviders) Generate(context.Context, llm.Request) (*llm.Response, error) {
	p.models.Add(1)
	return nil, domain.ErrUpstreamThrottled
}

func (p *noEvidenceProviders) Stream(context.Context, llm.Request, func(string) error) (*llm.Response, error) {
	p.models.Add(1)
	return nil, domain.ErrUpstreamThrottled
}

func TestQueryCatalogEmptyAllowlist(t *testing.T) {
	cfg := streamQueryConfig()
	cfg.RequireDocumentAllowlist = true
	providers := &noEvidenceProviders{FakeVectorStore: memory.NewFakeVectorStore()}
	require.NoError(t, providers.Upsert(t.Context(), "", []domain.Chunk{{ID: "private", DocumentID: privateArchiveID, Text: "Private archive evidence", Embedding: []float32{1}}}))
	amb := llm.NewAmbassador(cfg)
	amb.SetGenerator(providers)
	var reranks atomic.Int32
	rerank := queryReranker(func(context.Context, string, []domain.ScoredChunk) ([]domain.ScoredChunk, error) {
		reranks.Add(1)
		return nil, domain.ErrUpstreamThrottled
	})
	h := QueryHandler(retriever.NewRetriever(providers, providers, .7, 10), amb, cfg, nil,
		WithNoEvidenceCatalog(queryTestNoEvidenceCatalog(t)), WithReranker(rerank))

	const input = `{"query":"archive","allowed_document_ids":[]}`
	for _, accept := range []string{"application/json", "text/event-stream"} {
		t.Run(accept, func(t *testing.T) {
			req := streamQueryRequest(t.Context(), input)
			req.Header.Set("Accept", accept)
			req.Header.Add("Accept-Language", "en;q=0.3")
			req.Header.Add("Accept-Language", "ar-EG;q=0.9")
			rec := httptest.NewRecorder()
			rec.Header().Add("Vary", "Origin")
			h(rec, req)
			headers := requireQueryResponseHeaders(t, rec, http.StatusOK, accept)
			require.Equal(t, "ar", headers.Get("Content-Language"))
			require.Contains(t, strings.Join(headers.Values("Vary"), ", "), "Accept-Language")
			require.Contains(t, headers.Values("Vary"), "Origin")
			require.NotContains(t, rec.Body.String(), "Private archive evidence")
			require.NotContains(t, rec.Body.String(), "<الأرشيف>")
			if accept == "application/json" {
				var got QueryResponse
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
				require.Equal(t, catalogAnswer, got.Answer)
				require.Equal(t, "no_evidence", got.FinishReason)
				require.True(t, got.EarlyExit)
				require.Zero(t, got.TokenUsage)
				require.Zero(t, got.ChunksUsed)
				require.Zero(t, got.ContextChunks)
				require.Zero(t, got.ContextTokens)
				require.Empty(t, got.ScoredChunks)
				return
			}
			events := recordedEvents(t, rec.Body.String())
			require.Len(t, events, 3)
			require.Equal(t, "context", events[0].name)
			require.Equal(t, "delta", events[1].name)
			require.Equal(t, "done", events[2].name)
			require.JSONEq(t, `0`, string(events[0].data["chunks_used"]))
			require.JSONEq(t, `0`, string(events[0].data["context_chunks"]))
			require.JSONEq(t, `[]`, string(events[0].data["scored_chunks"]))
			var answer string
			require.NoError(t, json.Unmarshal(events[1].data["text"], &answer))
			require.Equal(t, catalogAnswer, answer)
			require.JSONEq(t, `"no_evidence"`, string(events[2].data["finish_reason"]))
			require.JSONEq(t, `true`, string(events[2].data["early_exit"]))
			require.JSONEq(t, `0`, string(events[2].data["context_tokens"]))
			require.JSONEq(t, `{"input_tokens":0,"output_tokens":0,"total_tokens":0}`, string(events[2].data["token_usage"]))
		})
	}

	srv := httptest.NewServer(h)
	defer srv.Close()
	c, err := client.New(srv.URL)
	require.NoError(t, err)
	request := client.QueryRequest{Query: "archive", AllowedDocumentIDs: []string{}}
	option := client.WithAcceptLanguage("ar-EG")
	got, err := c.Query(t.Context(), request, option)
	require.NoError(t, err)
	require.Equal(t, catalogAnswer, got.Answer)
	require.Equal(t, "no_evidence", got.FinishReason)
	require.Zero(t, got.TokenUsage)
	var answer string
	var done client.QueryStreamDone
	err = c.QueryStream(t.Context(), request, func(event client.Event) error {
		switch event.Type {
		case "delta":
			var delta QueryStreamDelta
			if decodeErr := json.Unmarshal(event.Data, &delta); decodeErr != nil {
				return decodeErr
			}
			answer += delta.Text
		case "done":
			return json.Unmarshal(event.Data, &done)
		}
		return nil
	}, option)
	require.NoError(t, err)
	require.Equal(t, got.Answer, answer)
	require.Equal(t, got.FinishReason, done.FinishReason)
	require.Equal(t, got.TokenUsage, done.TokenUsage)
	require.Zero(t, providers.embeds.Load())
	require.Zero(t, providers.queries.Load())
	require.Zero(t, providers.models.Load())
	require.Zero(t, reranks.Load())
}

func TestQueryCatalogNoEvidenceWithinScope(t *testing.T) {
	const allowedArchiveID = "00000000-0000-4000-8000-000000000002"
	cfg := streamQueryConfig()
	providers := &noEvidenceProviders{FakeVectorStore: memory.NewFakeVectorStore(), embedOK: true}
	amb := llm.NewAmbassador(cfg)
	amb.SetGenerator(providers)
	h := QueryHandler(retriever.NewRetriever(providers, providers, .7, 10), amb, cfg, nil,
		WithNoEvidenceCatalog(queryTestNoEvidenceCatalog(t)))
	for _, text := range []string{"First private archive contents", "Revised private archive contents"} {
		require.NoError(t, providers.Upsert(t.Context(), "", []domain.Chunk{{ID: "private", DocumentID: privateArchiveID, Text: text, Embedding: []float32{1}}}))
		body := fmt.Sprintf(`{"query":"archive","mode":"global","allowed_document_ids":[%q]}`, allowedArchiveID)
		req := streamQueryRequest(t.Context(), body)
		req.Header.Set("Accept", "application/json")
		req.Header.Set("Accept-Language", "ar-EG")
		rec := httptest.NewRecorder()
		h(rec, req)
		headers := requireQueryResponseHeaders(t, rec, http.StatusOK, "application/json")
		require.Equal(t, "ar", headers.Get("Content-Language"))
		var got QueryResponse
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
		require.Equal(t, catalogAnswer, got.Answer)
		require.Equal(t, "no_evidence", got.FinishReason)
		require.Zero(t, got.TokenUsage)
		require.Empty(t, got.ScoredChunks)
		require.NotContains(t, rec.Body.String(), text)
	}
	require.EqualValues(t, 2, providers.embeds.Load())
	require.EqualValues(t, 2, providers.queries.Load())
	require.Zero(t, providers.models.Load())
}

func TestQueryCatalogDoesNotLocalizeProviderFailures(t *testing.T) {
	for _, embedOK := range []bool{false, true} {
		cfg := streamQueryConfig()
		providers := &noEvidenceProviders{FakeVectorStore: memory.NewFakeVectorStore(), embedOK: embedOK}
		require.NoError(t, providers.Upsert(t.Context(), "", []domain.Chunk{{ID: "archive", Text: "Archive evidence", Embedding: []float32{1}}}))
		amb := llm.NewAmbassador(cfg)
		amb.SetGenerator(providers)
		h := QueryHandler(retriever.NewRetriever(providers, providers, .7, 10), amb, cfg, nil,
			WithNoEvidenceCatalog(queryTestNoEvidenceCatalog(t)))
		for _, accept := range []string{"application/json", "text/event-stream"} {
			req := streamQueryRequest(t.Context(), `{"query":"archive","mode":"global"}`)
			req.Header.Set("Accept", accept)
			req.Header.Set("Accept-Language", "ar")
			rec := httptest.NewRecorder()
			h(rec, req)
			require.Empty(t, rec.Header().Get("Content-Language"))
			require.NotContains(t, rec.Body.String(), "no_evidence")
			require.NotContains(t, rec.Body.String(), catalogAnswer)
			if embedOK && accept == "text/event-stream" {
				requireQueryResponseHeaders(t, rec, http.StatusOK, accept)
				events := recordedEvents(t, rec.Body.String())
				require.Len(t, events, 2)
				require.Equal(t, "context", events[0].name)
				require.Equal(t, "error", events[1].name)
				require.JSONEq(t, `"upstream_throttled"`, string(events[1].data["error"]))
			} else {
				deadlineAssertError(t, rec, http.StatusServiceUnavailable, "upstream_throttled")
			}
		}
	}
}
