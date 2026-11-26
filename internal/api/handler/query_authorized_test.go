package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/embedder"
	"github.com/Victoroide/quarry-rag/internal/llm"
	"github.com/Victoroide/quarry-rag/internal/pipeline"
	"github.com/Victoroide/quarry-rag/internal/retriever"
	"github.com/Victoroide/quarry-rag/internal/store/memory"
	"github.com/Victoroide/quarry-rag/internal/subjectauth"
	"github.com/Victoroide/quarry-rag/internal/tracing"
	"github.com/stretchr/testify/require"
)

var grantKinds = []domain.GrantScopeKind{domain.GrantScopeRelation, domain.GrantScopeMembership}

const (
	acceptJSON = "application/json"
	acceptSSE  = "text/event-stream"
)

// scoredDocs returns the documents of a JSON response or of an SSE context frame.
func scoredDocs(t *testing.T, rec *httptest.ResponseRecorder) (docs []string, used int) {
	t.Helper()
	var chunks []ScoredChunkResponse
	if rec.Header().Get("Content-Type") == acceptSSE {
		events := recordedEvents(t, rec.Body.String())
		require.Equal(t, "context", events[0].name)
		require.NoError(t, json.Unmarshal(events[0].data["scored_chunks"], &chunks))
		require.NoError(t, json.Unmarshal(events[0].data["chunks_used"], &used))
	} else {
		var response QueryResponse
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
		chunks, used = response.ScoredChunks, response.ChunksUsed
	}
	for _, chunk := range chunks {
		docs = append(docs, chunk.DocID)
	}
	return docs, used
}

func requirePrivate(t *testing.T, rec *httptest.ResponseRecorder) {
	t.Helper()
	require.Equal(t, "private, no-store", rec.Header().Get("Cache-Control"))
}

func requireErrorBody(t *testing.T, body, kind, message string) {
	t.Helper()
	require.JSONEq(t, `{"error":"`+kind+`","message":"`+message+`"}`, strings.TrimSpace(body))
}

// requireScoped checks that every filter the store was handed named the
// principal's tenant and, for a membership store, only its readable documents.
func requireScoped(t *testing.T, rig *grantRig, kind domain.GrantScopeKind, principal string, readable []string) {
	t.Helper()
	rig.store.mu.Lock()
	defer rig.store.mu.Unlock()
	require.NotEmpty(t, rig.store.filters)
	require.Zero(t, rig.store.unscoped, "an unscoped search reached the store")
	for _, filter := range rig.store.filters {
		require.NotNil(t, filter.Authorized)
		require.Equal(t, principal, filter.Authorized.PrincipalID)
		require.Equal(t, principalOf(principal).TenantID, filter.Authorized.TenantID)
		if filter.DocumentIDs != nil {
			for _, id := range filter.DocumentIDs {
				require.Contains(t, readable, id, "a filter named a document the principal cannot read")
			}
		}
		if kind == domain.GrantScopeMembership {
			require.NotNil(t, filter.DocumentIDs)
			keys := make([]string, 0, len(filter.Authorized.Incarnations))
			for id := range filter.Authorized.Incarnations {
				keys = append(keys, id)
			}
			slices.Sort(keys)
			require.Equal(t, readable, keys)
		} else {
			require.Nil(t, filter.Authorized.Incarnations)
		}
	}
}

func TestAuthorizedQueryConfinesEverySearchAndModelInput(t *testing.T) {
	people := []struct {
		principal string
		readable  []string
		forbidden []string // text and identifiers that must never leave the service
	}{
		{userA1, []string{docVisible, docShared}, []string{hiddenText, docHidden, docOther, "Zephyr"}},
		{userA2, []string{docShared, docHidden}, []string{visibleText, docVisible, docOther}},
		{userB1, []string{docOther}, []string{hiddenText, sharedText, docHidden, docVisible, docShared}},
	}
	for _, kind := range grantKinds {
		for _, person := range people {
			for _, mode := range []string{"", "global", "local"} {
				for _, answer := range []string{"generated", "evidence"} {
					for _, accept := range []string{acceptJSON, acceptSSE} {
						name := string(kind) + "/" + person.principal[len(person.principal)-2:] + "/" + mode + "/" + answer + "/" + accept
						t.Run(name, func(t *testing.T) {
							rig := newGrantRig(t, kind, nil)
							body := `{"query":"What do the archive documents say?","mode":"` + mode + `","answer_mode":"` + answer + `","include_citations":` + map[bool]string{true: "true", false: "false"}[answer == "generated"] + `}`
							rec := rig.serve(t, person.principal, body, accept)
							require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
							requirePrivate(t, rec)
							docs, used := scoredDocs(t, rec)
							require.ElementsMatch(t, person.readable, docs)
							require.Equal(t, len(person.readable), used)
							requireScoped(t, rig, kind, person.principal, person.readable)
							sent := rig.everythingSent()
							for _, forbidden := range person.forbidden {
								require.NotContains(t, rec.Body.String(), forbidden)
								require.NotContains(t, sent, forbidden)
							}
							if answer == "evidence" {
								require.Zero(t, rig.converser.prompts, "evidence-only queries never call a model")
								require.Empty(t, rig.reranked, "evidence-only queries never rerank")
							} else {
								require.NotEmpty(t, rig.converser.prompts)
							}
						})
					}
				}
			}
		}
	}
}

func TestAuthorizedQueryAllowlistSemantics(t *testing.T) {
	for _, kind := range grantKinds {
		for _, tc := range []struct {
			name, field string
			wantEmpty   bool
			wantDocs    []string
		}{
			{"omitted uses every readable document", ``, false, []string{docVisible, docShared}},
			{"null uses every readable document", `,"allowed_document_ids":null`, false, []string{docVisible, docShared}},
			{"explicit empty never widens", `,"allowed_document_ids":[]`, true, nil},
			{"foreign and missing valid ids drop out", `,"allowed_document_ids":["` + docHidden + `","` + docOther + `","` + docMissing + `"]`, true, nil},
			{"one survivor among foreign ids", `,"allowed_document_ids":["` + docHidden + `","` + docShared + `","` + docMissing + `"]`, false, []string{docShared}},
			{"alias spellings canonicalize", `,"allowed_document_ids":["` + strings.ToUpper(docShared) + `"]`, false, []string{docShared}},
		} {
			t.Run(string(kind)+"/"+tc.name, func(t *testing.T) {
				rig := newGrantRig(t, kind, nil)
				for _, accept := range []string{acceptJSON, acceptSSE} {
					rig.w.mu.Lock()
					rig.w.log = nil
					rig.w.mu.Unlock()
					rec := rig.serve(t, userA1, `{"query":"What do the archive documents say?","mode":"global"`+tc.field+`}`, accept)
					require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
					docs, _ := scoredDocs(t, rec)
					require.ElementsMatch(t, tc.wantDocs, docs)
					requirePrivate(t, rec)
					if tc.wantEmpty {
						require.Contains(t, rec.Body.String(), `"finish_reason":"no_evidence"`)
						require.Zero(t, rig.providerCalls(), "an empty intersection reaches no provider")
						rig.store.mu.Lock()
						require.Empty(t, rig.store.filters)
						rig.store.mu.Unlock()
					}
					require.NotContains(t, rec.Body.String(), hiddenText)
				}
			})
		}
	}
}

func TestAuthorizedQueryEmptyIntersectionReturnsLocalizedContract(t *testing.T) {
	for _, kind := range grantKinds {
		rig := newGrantRig(t, kind, nil)
		req, cancel := rig.request(t.Context(), userA1, `{"query":"archive","allowed_document_ids":[]}`, acceptJSON, time.Time{})
		req.Header.Set("Accept-Language", "es-MX")
		rec := httptest.NewRecorder()
		rig.handler.ServeHTTP(rec, req)
		cancel()
		require.Equal(t, http.StatusOK, rec.Code)
		require.Equal(t, "es", rec.Header().Get("Content-Language"))
		require.Contains(t, rec.Body.String(), "No se encontró información relevante")
		require.Zero(t, rig.providerCalls())
	}
}

func TestAuthorizedQueryRejectsOmissionWhenAllowlistRequired(t *testing.T) {
	for _, kind := range grantKinds {
		rig := newGrantRig(t, kind, func(cfg *config.Config) { cfg.RequireDocumentAllowlist = true })
		for _, field := range []string{``, `,"allowed_document_ids":null`} {
			rec := rig.serve(t, userA1, `{"query":"archive"`+field+`}`, acceptJSON)
			require.Equal(t, http.StatusBadRequest, rec.Code)
			requirePrivate(t, rec)
		}
		require.Empty(t, rig.w.derived, "validation precedes any grant lookup")
		require.Zero(t, rig.providerCalls())
		rec := rig.serve(t, userA1, `{"query":"archive","mode":"global","allowed_document_ids":["`+docShared+`"]}`, acceptJSON)
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	}
}

func TestAuthorizedQueryRejectsOverlargeHostedScopeBeforeEmbedding(t *testing.T) {
	rig := newGrantRig(t, domain.GrantScopeMembership, func(cfg *config.Config) { cfg.MaxAllowedDocumentIDs = 1 })
	for _, accept := range []string{acceptJSON, acceptSSE} {
		rec := rig.serve(t, userA1, `{"query":"archive","mode":"global"}`, accept)
		require.Equal(t, http.StatusUnprocessableEntity, rec.Code)
		requirePrivate(t, rec)
		requireErrorBody(t, rec.Body.String(), "scope_too_large", "authorized document scope exceeds the supported size")
	}
	require.Zero(t, rig.providerCalls(), "capacity is checked before any paid work")
	// An inline list within capacity narrows the scope below the limit.
	rec := rig.serve(t, userA1, `{"query":"archive","mode":"global","allowed_document_ids":["`+docShared+`"]}`, acceptJSON)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	// A relation store has no membership list, so capacity does not apply.
	relation := newGrantRig(t, domain.GrantScopeRelation, func(cfg *config.Config) { cfg.MaxAllowedDocumentIDs = 1 })
	rec = relation.serve(t, userA1, `{"query":"archive","mode":"global"}`, acceptJSON)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
}

type unscopedStore struct {
	domain.VectorStore
	calls int
}

func (s *unscopedStore) QueryAllNamespaces(context.Context, []float32, int, string) ([]domain.ScoredChunk, error) {
	s.calls++
	return []domain.ScoredChunk{{Chunk: domain.Chunk{DocumentID: docHidden, Text: hiddenText}, Score: 1}}, nil
}

func (s *unscopedStore) QueryFiltered(context.Context, []float32, int, domain.VectorFilter) ([]domain.ScoredChunk, error) {
	s.calls++
	return nil, nil
}

func TestAuthorizedQueryFailsClosedWithoutScopeCapableStore(t *testing.T) {
	w := newGrantWorld()
	for name, store := range map[string]domain.VectorStore{"filtered": &unscopedStore{}, "unfiltered only": unfilteredOnly{}} {
		t.Run(name, func(t *testing.T) {
			emb := &grantEmbedder{w: w}
			ret := retriever.NewRetriever(store, emb, .7, 10)
			handler := QueryHandler(ret, nil, streamQueryConfig(), nil, WithAuthorizedRetrieval(w))
			rig := &grantRig{w: w, embedder: emb}
			req, cancel := rig.request(t.Context(), userA1, `{"query":"archive","mode":"global"}`, acceptJSON, time.Time{})
			defer cancel()
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
			require.Equal(t, http.StatusServiceUnavailable, rec.Code)
			requirePrivate(t, rec)
			requireErrorBody(t, rec.Body.String(), "unavailable", "authorization unavailable")
			require.Empty(t, emb.queries)
			if unscoped, ok := store.(*unscopedStore); ok {
				require.Zero(t, unscoped.calls, "no search ran")
			}
		})
	}
}

type unfilteredOnly struct{ domain.VectorStore }

func (unfilteredOnly) QueryAllNamespaces(context.Context, []float32, int, string) ([]domain.ScoredChunk, error) {
	panic("an authorized request used the unfiltered search")
}

func TestAuthorizedQueryRefusesUnverifiedRequests(t *testing.T) {
	rig := newGrantRig(t, domain.GrantScopeRelation, nil)
	for name, req := range map[string]*http.Request{
		"no subject": streamQueryRequest(t.Context(), `{"query":"archive"}`),
	} {
		rec := httptest.NewRecorder()
		rig.handler.ServeHTTP(rec, req)
		require.Equal(t, http.StatusUnauthorized, rec.Code, name)
		requirePrivate(t, rec)
	}
	rec := rig.serveWith(t, userA1, `{"query":"archive"}`, acceptJSON, time.Now().Add(-time.Second))
	require.Equal(t, http.StatusUnauthorized, rec.Code)
	require.Zero(t, rig.providerCalls())
	rig.w.disable(userA1)
	rec = rig.serve(t, userA1, `{"query":"archive","mode":"global"}`, acceptJSON)
	require.Equal(t, http.StatusForbidden, rec.Code)
	requireErrorBody(t, rec.Body.String(), "forbidden", "access denied")
	require.Zero(t, rig.providerCalls())
}

func TestAuthorizedQueryDropsHostileCandidatesBeforeRankingOrModelUse(t *testing.T) {
	visible, hidden, other := newGrantWorld().document(docVisible), newGrantWorld().document(docHidden), newGrantWorld().document(docOther)
	stale := visible.hit(true)
	stale.Chunk.Metadata[domain.VectorRevisionKey] = "99999999-9999-4999-8999-999999999999"
	stale.Chunk.Text = "Stale body from a replaced incarnation."
	staleSameBody := visible.hit(true)
	staleSameBody.Chunk.ID = "stale-same-body"
	staleSameBody.Chunk.Metadata[domain.VectorRevisionKey] = "99999999-9999-4999-8999-999999999999"
	staleSameBody.Score = .97
	foreignLabel := visible.hit(true)
	foreignLabel.Chunk.ID = "foreign-label"
	foreignLabel.Chunk.Metadata[domain.VectorTenantKey] = tenantB
	foreignLabel.Score = .96
	hostile := []domain.ScoredChunk{hidden.hit(true), other.hit(true), hidden.hit(false), stale, staleSameBody, foreignLabel,
		{Chunk: domain.Chunk{ID: "unknown", DocumentID: docMissing, Text: "Nobody owns this."}, Score: 1}}
	for _, kind := range grantKinds {
		for _, mode := range []string{"", "global", "local"} {
			t.Run(string(kind)+"/"+mode, func(t *testing.T) {
				rig := newGrantRig(t, kind, nil)
				rig.store.hostile = hostile
				rec := rig.serve(t, userA1, `{"query":"What do the archive documents say?","mode":"`+mode+`"}`, acceptJSON)
				require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
				docs, used := scoredDocs(t, rec)
				require.ElementsMatch(t, []string{docVisible, docShared}, docs)
				require.Equal(t, 2, used)
				for _, forbidden := range []string{hiddenText, "Stale body", "Nobody owns", docHidden, docOther, docMissing} {
					require.NotContains(t, rec.Body.String(), forbidden)
					require.NotContains(t, rig.everythingSent(), forbidden)
				}
				for _, prompt := range rig.converser.prompts {
					require.NotContains(t, prompt, "Stale body")
				}
			})
		}
	}
}

func TestAuthorizedQueryNeverReadsAHiddenPreview(t *testing.T) {
	const query = `{"query":"What does Zephyr Industries say about archives?"}`
	for _, kind := range grantKinds {
		t.Run(string(kind), func(t *testing.T) {
			rig := newGrantRig(t, kind, nil)
			rec := rig.serve(t, userA1, query, acceptJSON)
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			docs, _ := scoredDocs(t, rec)
			require.NotContains(t, docs, docHidden)
			for _, ids := range rig.w.previews {
				require.NotContains(t, ids, docHidden, "a hidden preview was read for routing")
			}
			require.NotContains(t, rec.Body.String(), "Zephyr")
			// The same query from the reader of the document routes onto it, so
			// the preview is the signal that decides the route.
			reader := newGrantRig(t, kind, nil)
			rec = reader.serve(t, userA2, query, acceptJSON)
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			docs, _ = scoredDocs(t, rec)
			require.Contains(t, docs, docHidden)
			var response QueryResponse
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
			require.Equal(t, "LOCAL", response.Mode)
		})
	}
}

func TestAuthorizedQueryAutoRoutingWithFailedProbeStaysScoped(t *testing.T) {
	for _, kind := range grantKinds {
		t.Run(string(kind), func(t *testing.T) {
			rig := newGrantRig(t, kind, nil)
			rig.store.failSearch = map[int]error{1: errDatabase}
			rec := rig.serve(t, userA1, `{"query":"What do the archive documents say?"}`, acceptJSON)
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			var response QueryResponse
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
			require.Equal(t, "GLOBAL", response.Mode)
			docs, _ := scoredDocs(t, rec)
			require.ElementsMatch(t, []string{docVisible, docShared}, docs)
			require.GreaterOrEqual(t, len(rig.store.filters), 2, "the failed probe is followed by a scoped search")
			requireScoped(t, rig, kind, userA1, []string{docVisible, docShared})
		})
	}
}

func TestAuthorizedQueryTopUpAndLocalSearchesCarryTheScope(t *testing.T) {
	for _, kind := range grantKinds {
		t.Run(string(kind), func(t *testing.T) {
			rig := newGrantRig(t, kind, nil)
			rec := rig.serve(t, userA1, `{"query":"What do the archive documents say?","mode":"local","max_chunks":5}`, acceptJSON)
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			docs, _ := scoredDocs(t, rec)
			require.ElementsMatch(t, []string{docVisible, docShared}, docs, "the sparse LOCAL result is topped up inside the scope")
			rig.store.mu.Lock()
			filters := slices.Clone(rig.store.filters)
			rig.store.mu.Unlock()
			require.GreaterOrEqual(t, len(filters), 3, "probe, selected document and top-up")
			requireScoped(t, rig, kind, userA1, []string{docVisible, docShared})
			local := 0
			for _, filter := range filters {
				if len(filter.DocumentIDs) == 1 {
					require.Equal(t, docVisible, filter.DocumentIDs[0])
					local++
				}
			}
			require.Equal(t, 1, local)
		})
	}
}

func TestAuthorizedBackendsAgreeOnAuthorizedInputs(t *testing.T) {
	type outcome struct {
		Mode   string
		Used   int
		Chunks []ScoredChunkResponse
		Tokens domain.TokenUsage
	}
	run := func(kind domain.GrantScopeKind, principal, body string) outcome {
		rig := newGrantRig(t, kind, nil)
		rec := rig.serve(t, principal, body, acceptJSON)
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		var response QueryResponse
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
		slices.SortFunc(response.ScoredChunks, func(a, b ScoredChunkResponse) int { return strings.Compare(a.DocID, b.DocID) })
		return outcome{Mode: response.Mode, Used: response.ChunksUsed, Chunks: response.ScoredChunks, Tokens: response.TokenUsage}
	}
	for _, principal := range []string{userA1, userA2, userB1} {
		for _, mode := range []string{"", "global", "local"} {
			body := `{"query":"What do the archive documents say?","mode":"` + mode + `"}`
			require.Equal(t, run(domain.GrantScopeRelation, principal, body), run(domain.GrantScopeMembership, principal, body),
				"%s %q", principal, mode)
		}
	}
}

func TestAuthorizedQueryCitationsReferenceOnlyAuthorizedExcerpts(t *testing.T) {
	for _, kind := range grantKinds {
		rig := newGrantRig(t, kind, nil)
		rec := rig.serve(t, userA1, `{"query":"What do the archive documents say?","mode":"global","include_citations":true}`, acceptJSON)
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		var response QueryResponse
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
		require.NotEmpty(t, response.Citations)
		for _, citation := range response.Citations {
			require.Contains(t, []string{docVisible, docShared}, citation.DocID)
		}
	}
}

func TestAuthorizedQueryDeterministicPrompt(t *testing.T) {
	// Identical labels and text in another tenant never change what the model receives.
	for _, kind := range grantKinds {
		withOther := newGrantRig(t, kind, nil)
		without := newGrantRig(t, kind, nil)
		without.w.documents = without.w.documents[:3]
		body := `{"query":"What do the archive documents say?","mode":"global"}`
		require.Equal(t, http.StatusOK, withOther.serve(t, userA1, body, acceptJSON).Code)
		require.Equal(t, http.StatusOK, without.serve(t, userA1, body, acceptJSON).Code)
		require.Equal(t, boundaryless(without.converser.prompts), boundaryless(withOther.converser.prompts))
	}
}

var excerptBoundary = regexp.MustCompile(`excerpt-[0-9a-f]+`)

// boundaryless removes the random excerpt delimiter that each request draws.
func boundaryless(prompts []string) []string {
	out := make([]string, len(prompts))
	for i, prompt := range prompts {
		out[i] = excerptBoundary.ReplaceAllString(prompt, "excerpt")
	}
	return out
}

func TestAuthorizedQueryServiceModeStillUsesNoScope(t *testing.T) {
	w := newGrantWorld()
	store := &grantStore{w: w, kind: domain.GrantScopeRelation}
	cfg := streamQueryConfig()
	ret := retriever.NewRetriever(store, embedder.NewFakeEmbedder(8), .7, 10)
	rec := httptest.NewRecorder()
	QueryHandler(ret, nil, cfg, nil).ServeHTTP(rec, streamQueryRequest(t.Context(), `{"query":"archive","mode":"global","answer_mode":"evidence"}`))
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.NotEmpty(t, store.filters)
	for _, filter := range store.filters {
		require.Nil(t, filter.Authorized)
	}
	require.NotEqual(t, "private, no-store", rec.Header().Get("Cache-Control"))
	require.Empty(t, w.derived)
}

func TestAuthorizedQueryStageFailuresNeverFallBackToAnUnscopedSearch(t *testing.T) {
	for _, kind := range grantKinds {
		for _, tc := range []struct {
			name   string
			tune   func(*grantWorld)
			status int
			kind   string
			text   string
		}{
			{"scope lookup", func(w *grantWorld) { w.deriveErr = errDatabase }, http.StatusServiceUnavailable, "unavailable", "authorization unavailable"},
			{"evidence lookup", func(w *grantWorld) { w.evidenceErr = errDatabase }, http.StatusServiceUnavailable, "unavailable", "authorization unavailable"},
			{"state lookup", func(w *grantWorld) { w.stateErr = errDatabase }, http.StatusServiceUnavailable, "unavailable", "authorization unavailable"},
			{"stale replica", func(w *grantWorld) { w.stale = 1 }, http.StatusServiceUnavailable, "unavailable", "authorization unavailable"},
			{"trust data unavailable", func(w *grantWorld) { w.trustErr = subjectauth.ErrAuthorityUnavailable }, http.StatusServiceUnavailable, "unavailable", "authorization unavailable"},
			{"signing key removed", func(w *grantWorld) { w.trustErr = subjectauth.ErrInvalidCredential }, http.StatusUnauthorized, "unauthorized", "invalid or missing credentials"},
		} {
			for _, mode := range []string{"", "global"} {
				t.Run(string(kind)+"/"+tc.name+"/"+mode, func(t *testing.T) {
					rig := newGrantRig(t, kind, nil)
					tc.tune(rig.w)
					for _, accept := range []string{acceptJSON, acceptSSE} {
						rec := rig.serve(t, userA1, `{"query":"What do the archive documents say?","mode":"`+mode+`"}`, accept)
						require.Equal(t, tc.status, rec.Code, rec.Body.String())
						requirePrivate(t, rec)
						require.NotContains(t, rec.Body.String(), hiddenText)
						require.NotContains(t, rec.Body.String(), visibleText)
						require.NotContains(t, rec.Body.String(), errDatabase.Error())
						require.Zero(t, rig.store.unscoped)
						require.Empty(t, rig.converser.prompts)
					}
				})
			}
		}
	}
}

func TestAuthorizedQueryScopeLookupFailureStopsBeforeAnyProvider(t *testing.T) {
	for _, kind := range grantKinds {
		rig := newGrantRig(t, kind, nil)
		rig.w.deriveErr = errDatabase
		rec := rig.serve(t, userA1, `{"query":"archive"}`, acceptJSON)
		require.Equal(t, http.StatusServiceUnavailable, rec.Code)
		requirePrivate(t, rec)
		require.NotContains(t, rec.Body.String(), errDatabase.Error())
		require.Zero(t, rig.providerCalls())
	}
}

func TestAuthorizedFailuresMapToBoundedEnvelopes(t *testing.T) {
	for _, tc := range []struct {
		err    error
		status int
		kind   string
		text   string
	}{
		{pipeline.ErrCredentialExpired, http.StatusUnauthorized, "unauthorized", "invalid or missing credentials"},
		{pipeline.ErrReadAuthorityChanged, http.StatusForbidden, "forbidden", "access denied"},
		{domain.ErrReadDenied, http.StatusForbidden, "forbidden", "access denied"},
		{fmt.Errorf("stage: %w: %w", pipeline.ErrAuthorizationUnavailable, errDatabase), http.StatusServiceUnavailable, "unavailable", "authorization unavailable"},
		{fmt.Errorf("retriever: %w", domain.ErrGrantScopeUnsupported), http.StatusServiceUnavailable, "unavailable", "authorization unavailable"},
	} {
		code, retry, body := queryErrorResponse(tc.err)
		require.Equal(t, tc.status, code)
		require.Empty(t, retry)
		require.Equal(t, APIError{Error: tc.kind, Message: tc.text}, body)
	}
	// The envelope never repeats what failed, which can hold identifiers.
	_, _, body := queryErrorResponse(fmt.Errorf("%w: %w", pipeline.ErrAuthorizationUnavailable, errDatabase))
	require.NotContains(t, body.Message, errDatabase.Error())
}

type tracedGrantStore struct {
	*grantStore
}

func (*tracedGrantStore) DeleteByDocID(context.Context, string) error               { return nil }
func (*tracedGrantStore) DeleteAllVectorsInNamespace(context.Context, string) error { return nil }

type tracedRelationGrantStore struct{ *tracedGrantStore }

func (*tracedRelationGrantStore) TransactionalVectors()           {}
func (*tracedRelationGrantStore) ObservesVectorIncarnation() bool { return true }
func (s *tracedRelationGrantStore) QueryFilteredWithExactness(ctx context.Context, embedding []float32, topK int, filter domain.VectorFilter) ([]domain.ScoredChunk, bool, error) {
	chunks, err := s.QueryFiltered(ctx, embedding, topK, filter)
	return chunks, true, err
}

type tracedMembershipGrantStore struct{ *tracedGrantStore }

func (*tracedMembershipGrantStore) VisibilityRefillLimit() int  { return 10 }
func (*tracedMembershipGrantStore) ManagesVectorDispatch() bool { return true }
func (*tracedMembershipGrantStore) DeleteByRevision(context.Context, string, string) error {
	return nil
}
func (*tracedMembershipGrantStore) DeleteByAttempt(context.Context, string, string, string) error {
	return nil
}
func (*tracedMembershipGrantStore) UpsertWithVisibility(context.Context, string, []domain.Chunk) (domain.VisibilityProbe, error) {
	return nil, nil
}
func (s *tracedMembershipGrantStore) QueryCandidates(ctx context.Context, embedding []float32, topK int, filter domain.VectorFilter) ([]domain.ScoredChunk, error) {
	return s.QueryFiltered(ctx, embedding, topK, filter)
}
func (s *tracedMembershipGrantStore) QueryFilteredBudgeted(ctx context.Context, embedding []float32, topK int, filter domain.VectorFilter, _ int) ([]domain.ScoredChunk, int, error) {
	chunks, err := s.QueryFiltered(ctx, embedding, topK, filter)
	return chunks, 1, err
}

func TestAuthorizedQuerySucceedsThroughTracedVectorStore(t *testing.T) {
	for _, tc := range []struct {
		name   string
		kind   domain.GrantScopeKind
		system string
		wrap   func(*tracedGrantStore) domain.VectorStore
	}{
		{"relation", domain.GrantScopeRelation, "postgresql", func(s *tracedGrantStore) domain.VectorStore { return &tracedRelationGrantStore{s} }},
		{"membership", domain.GrantScopeMembership, "pinecone", func(s *tracedGrantStore) domain.VectorStore { return &tracedMembershipGrantStore{s} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := newGrantWorld()
			base := &grantStore{w: w, kind: tc.kind}
			raw := tc.wrap(&tracedGrantStore{grantStore: base})
			wrapped := tracing.WrapVectorStore(raw, tc.system)
			require.NotEqual(t, raw, wrapped, "WrapVectorStore must wrap the deletable filtered store")

			emb := &grantEmbedder{w: w}
			conv := &grantConverser{w: w}
			cfg := streamQueryConfig()
			cfg.QueryConcurrency, cfg.QueryQueueTimeout = 4, time.Hour
			amb := llm.NewAmbassador(cfg)
			amb.SetClient(conv)
			ret := retriever.NewRetriever(wrapped, emb, .7, 10)
			h := QueryHandler(ret, amb, cfg, nil, WithAuthorizedRetrieval(w))
			rig := &grantRig{w: w, store: base, embedder: emb, converser: conv, handler: h, cfg: cfg}

			for _, accept := range []string{acceptJSON, acceptSSE} {
				rec := rig.serve(t, userA1, `{"query":"What do the archive documents say?","mode":"global"}`, accept)
				require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
				requirePrivate(t, rec)
				docs, used := scoredDocs(t, rec)
				require.ElementsMatch(t, []string{docVisible, docShared}, docs)
				require.Equal(t, 2, used)
				require.NotContains(t, rec.Body.String(), hiddenText)
				require.NotContains(t, rec.Body.String(), docHidden)
				require.NotContains(t, rec.Body.String(), docOther)
			}
			requireScoped(t, rig, tc.kind, userA1, []string{docVisible, docShared})
		})
	}

	// An incapable store wrapped by tracing still fails closed with 503.
	w := newGrantWorld()
	wrappedMem := tracing.WrapVectorStore(memory.NewFakeVectorStore(), "memory")
	emb := &grantEmbedder{w: w}
	ret := retriever.NewRetriever(wrappedMem, emb, .7, 10)
	h := QueryHandler(ret, nil, streamQueryConfig(), nil, WithAuthorizedRetrieval(w))
	rig := &grantRig{w: w, embedder: emb}
	req, cancel := rig.request(t.Context(), userA1, `{"query":"archive","mode":"global"}`, acceptJSON, time.Time{})
	defer cancel()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
	requirePrivate(t, rec)
	requireErrorBody(t, rec.Body.String(), "unavailable", "authorization unavailable")
	require.Empty(t, emb.queries)
}
