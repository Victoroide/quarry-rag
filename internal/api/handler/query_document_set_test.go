package handler

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/logctx"
	"github.com/Victoroide/quarry-rag/internal/tracing"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

const (
	setFull    = "20000000-0000-4000-8000-000000000001" // A1: docVisible, docShared
	setShared  = "20000000-0000-4000-8000-000000000002" // A1: docShared only
	setEmpty   = "20000000-0000-4000-8000-000000000003" // A1: no member
	setPeers   = "20000000-0000-4000-8000-000000000004" // A2: docShared, docHidden
	setWider   = "20000000-0000-4000-8000-000000000005" // A1: docVisible plus a document A1 cannot read
	setExpired = "20000000-0000-4000-8000-000000000006" // A1: expired
	setTenantB = "20000000-0000-4000-8000-000000000007" // B1
	setAbsent  = "20000000-0000-4000-8000-0000000000ff"

	setQuery = `"query":"What do the archive documents say?"`
)

// addSet registers a set of the owner's tenant that names the documents'
// current incarnations.
func (w *grantWorld) addSet(owner, id string, documents ...string) *grantSet {
	w.mu.Lock()
	defer w.mu.Unlock()
	set := &grantSet{id: id, revision: uuid.NewString(), owner: owner, tenant: principalOf(owner).TenantID, members: map[string]string{}}
	for _, document := range documents {
		set.members[document] = w.document(document).incarnation
	}
	w.sets[id] = set
	return set
}

func (w *grantWorld) expireSet(id string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.sets[id].expired = true
}

func newSetWorld(w *grantWorld) {
	w.addSet(userA1, setFull, docVisible, docShared)
	w.addSet(userA1, setShared, docShared)
	w.addSet(userA1, setEmpty)
	w.addSet(userA2, setPeers, docShared, docHidden)
	w.addSet(userA1, setWider, docVisible, docHidden)
	w.addSet(userA1, setExpired, docVisible).expired = true
	w.addSet(userB1, setTenantB, docOther)
}

func newSetRig(t *testing.T, kind domain.GrantScopeKind, tune func(*config.Config)) *grantRig {
	t.Helper()
	rig := newGrantRig(t, kind, func(cfg *config.Config) {
		cfg.DocumentSetsEnabled = true
		if tune != nil {
			tune(cfg)
		}
	})
	newSetWorld(rig.w)
	return rig
}

func setBody(set, extra string) string {
	body := `{` + setQuery + `,"document_set_id":"` + set + `"`
	if extra != "" {
		body += "," + extra
	}
	return body + "}"
}

func TestDocumentSetQueryReadsOnlyTheIntersection(t *testing.T) {
	for _, kind := range grantKinds {
		for _, tc := range []struct {
			name, principal, set string
			readable             []string
			forbidden            []string
		}{
			{"a narrower set", userA1, setShared, []string{docShared}, []string{visibleText, docVisible, hiddenText, docHidden, docOther}},
			{"the full set", userA1, setFull, []string{docVisible, docShared}, []string{hiddenText, docHidden, docOther}},
			{"a set never adds access", userA1, setWider, []string{docVisible}, []string{hiddenText, docHidden, sharedText, docOther}},
			{"the peer's own set", userA2, setPeers, []string{docShared, docHidden}, []string{visibleText, docVisible, docOther}},
		} {
			for _, mode := range []string{"", "global", "local"} {
				for _, accept := range []string{acceptJSON, acceptSSE} {
					t.Run(string(kind)+"/"+tc.name+"/"+mode+"/"+accept, func(t *testing.T) {
						rig := newSetRig(t, kind, nil)
						rec := rig.serve(t, tc.principal, setBody(tc.set, `"mode":"`+mode+`"`), accept)
						require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
						requirePrivate(t, rec)
						docs, used := scoredDocs(t, rec)
						require.ElementsMatch(t, tc.readable, docs)
						require.Equal(t, len(tc.readable), used)
						requireScopedToSet(t, rig, kind, tc.principal, tc.set, tc.readable)
						sent := rig.everythingSent() + rec.Body.String()
						for _, text := range tc.forbidden {
							require.NotContains(t, sent, text, "text outside the set reached a provider or the caller")
						}
						rig.w.mu.Lock()
						for _, ids := range rig.w.previews {
							for _, id := range ids {
								require.NotContains(t, tc.forbidden, id, "a preview outside the set was read")
							}
						}
						rig.w.mu.Unlock()
					})
				}
			}
		}
	}
}

// requireScopedToSet checks that every search carried the set, and for a
// hosted store only the members the principal may read now.
func requireScopedToSet(t *testing.T, rig *grantRig, kind domain.GrantScopeKind, principal, set string, readable []string) {
	t.Helper()
	rig.store.mu.Lock()
	defer rig.store.mu.Unlock()
	require.NotEmpty(t, rig.store.filters)
	require.Zero(t, rig.store.unscoped)
	for _, filter := range rig.store.filters {
		require.NotNil(t, filter.Authorized)
		require.Equal(t, principal, filter.Authorized.PrincipalID)
		require.Equal(t, principalOf(principal).TenantID, filter.Authorized.TenantID)
		require.NotNil(t, filter.Authorized.DocumentSet, "a valid reference never causes an unrestricted query")
		require.Equal(t, set, filter.Authorized.DocumentSet.ID)
		if kind == domain.GrantScopeMembership {
			require.NotNil(t, filter.DocumentIDs)
			for _, id := range filter.DocumentIDs {
				require.Contains(t, readable, id)
			}
			keys := make([]string, 0, len(filter.Authorized.Incarnations))
			for id := range filter.Authorized.Incarnations {
				keys = append(keys, id)
			}
			require.ElementsMatch(t, readable, keys)
		}
	}
}

func TestDocumentSetFollowsLiveGrants(t *testing.T) {
	for _, kind := range grantKinds {
		t.Run(string(kind), func(t *testing.T) {
			rig := newSetRig(t, kind, nil)
			rec := rig.serve(t, userA1, setBody(setFull, ""), acceptJSON)
			docs, _ := scoredDocs(t, rec)
			require.ElementsMatch(t, []string{docVisible, docShared}, docs)

			rig.w.revoke(userA1, docShared)
			rec = rig.serve(t, userA1, setBody(setFull, ""), acceptJSON)
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			docs, _ = scoredDocs(t, rec)
			require.Equal(t, []string{docVisible}, docs, "a revoked member leaves at the next request")

			// A later grant to a document outside the set cannot add it.
			rig.w.document(docHidden).readers[userA1] = true
			rec = rig.serve(t, userA1, setBody(setFull, ""), acceptJSON)
			docs, _ = scoredDocs(t, rec)
			require.Equal(t, []string{docVisible}, docs)
			rec = rig.serve(t, userA1, `{`+setQuery+`}`, acceptJSON)
			docs, _ = scoredDocs(t, rec)
			require.ElementsMatch(t, []string{docVisible, docHidden}, docs, "without a set the request keeps its contract")
		})
	}
}

func TestRecreatedMemberNeverYieldsContentUnderAnObsoleteGrant(t *testing.T) {
	for _, kind := range grantKinds {
		t.Run(string(kind), func(t *testing.T) {
			rig := newSetRig(t, kind, nil)
			rig.w.document(docShared).incarnation = "99999999-9999-4999-8999-999999999999"
			rec := rig.serve(t, userA1, setBody(setFull, ""), acceptJSON)
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			docs, _ := scoredDocs(t, rec)
			require.Equal(t, []string{docVisible}, docs)
			require.NotContains(t, rec.Body.String(), sharedText)
		})
	}
}

func TestUnusableReferencesReturnOneNotFound(t *testing.T) {
	const message = `{"error":"not_found","message":"document set not found"}`
	for _, kind := range grantKinds {
		for _, tc := range []struct{ name, principal, set string }{
			{"absent", userA1, setAbsent},
			{"foreign owner in the tenant", userA1, setPeers},
			{"another tenant", userA1, setTenantB},
			{"another tenant's owner uses a local set", userB1, setFull},
			{"expired", userA1, setExpired},
			{"nil UUID", userA1, uuid.Nil.String()},
			{"upper case spelling of an absent set", userA1, strings.ToUpper(setAbsent)},
		} {
			for _, inline := range []string{"", `"allowed_document_ids":[]`, `"allowed_document_ids":["` + docVisible + `"]`, `"allowed_document_ids":null`} {
				for _, accept := range []string{acceptJSON, acceptSSE} {
					t.Run(string(kind)+"/"+tc.name+"/"+inline+"/"+accept, func(t *testing.T) {
						rig := newSetRig(t, kind, nil)
						rec := rig.serve(t, tc.principal, setBody(tc.set, inline), accept)
						require.Equal(t, http.StatusNotFound, rec.Code, rec.Body.String())
						require.JSONEq(t, message, strings.TrimSpace(rec.Body.String()), "a foreign reference reads like an absent one")
						requirePrivate(t, rec)
						require.Zero(t, rig.providerCalls(), "no provider work for an unusable reference")
						require.Zero(t, rig.store.hostedCalls)
						rig.store.mu.Lock()
						require.Empty(t, rig.store.filters)
						rig.store.mu.Unlock()
					})
				}
			}
		}
	}
}

func TestInvalidReferencesReturnBadRequest(t *testing.T) {
	for _, raw := range []string{
		`null`, `""`, `"x"`, `123`, `true`, `[]`, `{}`, `["` + setFull + `"]`,
		`"20000000-0000-4000-8000-0000000000zz"`, `"{` + setFull + `}"`, `" ` + setFull + `"`, `"` + setFull + ` "`,
		`"urn:uuid:` + setFull + `"`, `"` + strings.ReplaceAll(setFull, "-", "") + `"`, `"` + setFull + `0"`,
	} {
		for _, kind := range grantKinds {
			t.Run(string(kind)+"/"+raw, func(t *testing.T) {
				rig := newSetRig(t, kind, nil)
				body := `{` + setQuery + `,"document_set_id":` + raw + `}`
				rec := rig.serve(t, userA1, body, acceptJSON)
				require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
				requireErrorBody(t, rec.Body.String(), "validation", "document_set_id must be a UUID")
				require.Zero(t, rig.providerCalls())
				rig.w.mu.Lock()
				require.Empty(t, rig.w.derived, "nothing was read for a malformed reference")
				rig.w.mu.Unlock()
			})
		}
	}
}

func TestReferenceIsRefusedWhereSetsAreNotEnabled(t *testing.T) {
	body := setBody(setFull, "")
	t.Run("disabled in grants mode", func(t *testing.T) {
		for _, kind := range grantKinds {
			rig := newGrantRig(t, kind, nil)
			newSetWorld(rig.w)
			for _, set := range []string{setFull, setAbsent} {
				rec := rig.serve(t, userA1, setBody(set, ""), acceptJSON)
				require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
				requireErrorBody(t, rec.Body.String(), "validation", "document_set_id is not supported")
			}
			rec := rig.serve(t, userA1, setBody("not-a-uuid", ""), acceptJSON)
			require.Equal(t, http.StatusBadRequest, rec.Code, "a malformed reference is refused whether or not sets are enabled")
			require.Zero(t, rig.providerCalls())
			rec = rig.serve(t, userA1, `{"query":"What do the archive documents say?","document_set_id":null}`, acceptJSON)
			require.Equal(t, http.StatusBadRequest, rec.Code)
		}
	})
	t.Run("service mode", func(t *testing.T) {
		cfg := streamQueryConfig()
		cfg.DocumentSetsEnabled = true
		handler := streamQueryHandler(cfg, true, nil)
		req := streamQueryRequest(t.Context(), body)
		req.Header.Set("Accept", acceptJSON)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
		requireErrorBody(t, rec.Body.String(), "validation", "document_set_id is not supported")
	})
}

func TestEnabledSetsWithoutACapableBackendRefuseEverySetQuery(t *testing.T) {
	t.Run("relation store that cannot join a set", func(t *testing.T) {
		rig := newSetRigWith(t, domain.GrantScopeRelation, func(rig *grantRig) { rig.store.noSets = true })
		rec := rig.serve(t, userA1, setBody(setFull, ""), acceptJSON)
		require.Equal(t, http.StatusServiceUnavailable, rec.Code, rec.Body.String())
		requireErrorBody(t, rec.Body.String(), "unavailable", "authorization unavailable")
		require.Zero(t, rig.providerCalls())
		rec = rig.serve(t, userA1, `{`+setQuery+`}`, acceptJSON)
		require.Equal(t, http.StatusOK, rec.Code, "queries without a reference keep working")
	})
	t.Run("authority that cannot check sets", func(t *testing.T) {
		rig := newSetRigWithAuthority(t, domain.GrantScopeMembership, func(w *grantWorld) domain.ReadAuthority { return struct{ domain.ReadAuthority }{w} })
		rec := rig.serve(t, userA1, setBody(setFull, ""), acceptJSON)
		require.Equal(t, http.StatusServiceUnavailable, rec.Code, rec.Body.String())
		require.Zero(t, rig.providerCalls())
	})
}

func TestDocumentSetErrorResponseTelemetry(t *testing.T) {
	for _, tc := range []struct {
		name   string
		rig    func(*testing.T) *grantRig
		body   string
		status int
	}{
		{"sets disabled", func(t *testing.T) *grantRig {
			return newGrantRig(t, domain.GrantScopeRelation, nil)
		}, setBody(setFull, ""), http.StatusBadRequest},
		{"invalid allowlist", func(t *testing.T) *grantRig {
			return newSetRig(t, domain.GrantScopeRelation, nil)
		}, setBody(setFull, `"allowed_document_ids":["invalid"]`), http.StatusBadRequest},
		{"missing capability", func(t *testing.T) *grantRig {
			return newSetRigWith(t, domain.GrantScopeRelation, func(rig *grantRig) { rig.store.noSets = true })
		}, setBody(setFull, ""), http.StatusServiceUnavailable},
		{"missing set", func(t *testing.T) *grantRig {
			return newSetRig(t, domain.GrantScopeRelation, nil)
		}, setBody(setAbsent, ""), http.StatusNotFound},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rig := tc.rig(t)
			logs := captureLeveledLogs(t)
			ctx := tracing.WithRedactIdentifiers(logctx.WithRequestID(t.Context(), "document-set-request"), true)
			req, cancel := rig.request(ctx, userA1, tc.body, acceptJSON, time.Time{})
			defer cancel()
			writer := &leveledFailWriter{*httptest.NewRecorder()}
			rig.handler.ServeHTTP(writer, req)

			require.Equal(t, tc.status, writer.Code)
			require.Zero(t, rig.providerCalls())
			var record map[string]any
			require.NoError(t, json.Unmarshal(logs.Bytes(), &record))
			require.Equal(t, "encode error response", record["msg"])
			require.Equal(t, "document-set-request", record["request_id"])
			require.Equal(t, "operation failed", record["error"])
		})
	}
}

func TestEmptyIntersectionPerformsNoProviderWork(t *testing.T) {
	for _, kind := range grantKinds {
		for _, tc := range []struct{ name, set, extra string }{
			{"an empty set", setEmpty, ""},
			{"an explicit empty list", setFull, `"allowed_document_ids":[]`},
			{"an inline list outside the set", setShared, `"allowed_document_ids":["` + docVisible + `"]`},
			{"a type with no match", setFull, `"filter_doc_type":"report"`},
			{"a collection with no match", setFull, `"collection":"elsewhere"`},
		} {
			for _, accept := range []string{acceptJSON, acceptSSE} {
				for _, answer := range []string{"generated", "evidence"} {
					t.Run(string(kind)+"/"+tc.name+"/"+accept+"/"+answer, func(t *testing.T) {
						rig := newSetRig(t, kind, nil)
						rec := rig.serve(t, userA1, setBody(tc.set, `"answer_mode":"`+answer+`"`+withComma(tc.extra)), accept)
						require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
						docs, used := scoredDocs(t, rec)
						require.Empty(t, docs)
						require.Zero(t, used)
						require.Zero(t, rig.providerCalls(), "embedding, search, rerank and generation are all skipped")
						require.Zero(t, rig.store.hostedCalls)
					})
				}
			}
		}
	}
}

func withComma(extra string) string {
	if extra == "" {
		return ""
	}
	return "," + extra
}

func TestResolvedReferenceSatisfiesTheAllowlistRequirement(t *testing.T) {
	for _, kind := range grantKinds {
		t.Run(string(kind), func(t *testing.T) {
			rig := newSetRig(t, kind, func(cfg *config.Config) { cfg.RequireDocumentAllowlist = true })
			rec := rig.serve(t, userA1, `{`+setQuery+`}`, acceptJSON)
			require.Equal(t, http.StatusBadRequest, rec.Code)
			requireErrorBody(t, rec.Body.String(), "validation", "allowed_document_ids is required")
			rec = rig.serve(t, userA1, setBody(setFull, ""), acceptJSON)
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			rec = rig.serve(t, userA1, setBody(setAbsent, ""), acceptJSON)
			require.Equal(t, http.StatusNotFound, rec.Code, "an unresolved reference never satisfies the requirement")
			rec = rig.serve(t, userA1, setBody(setFull, `"allowed_document_ids":[]`), acceptJSON)
			require.Equal(t, http.StatusOK, rec.Code)
			docs, _ := scoredDocs(t, rec)
			require.Empty(t, docs, "an explicit empty list still means no matches")
		})
	}
}

func TestSetLargerThanTheCeilingIsRefusedBeforeAnyProviderWork(t *testing.T) {
	rig := newSetRig(t, domain.GrantScopeMembership, func(cfg *config.Config) { cfg.DocumentSetMaxMembers = 1 })
	rec := rig.serve(t, userA1, setBody(setFull, ""), acceptJSON)
	require.Equal(t, http.StatusUnprocessableEntity, rec.Code, rec.Body.String())
	requireErrorBody(t, rec.Body.String(), "scope_too_large", "authorized document scope exceeds the supported size")
	require.Zero(t, rig.providerCalls())
	rec = rig.serve(t, userA1, setBody(setShared, ""), acceptJSON)
	require.Equal(t, http.StatusOK, rec.Code, "a set within the ceiling is served")
}

// bigSet gives the principal n documents it may read and a set that names them all.
func bigSet(rig *grantRig, n int) string {
	const id = "20000000-0000-4000-8000-0000000000aa"
	docs := make([]string, 0, n)
	rig.w.mu.Lock()
	rig.w.documents = rig.w.documents[:0]
	for i := range n {
		doc := &grantDocument{
			id: fmt.Sprintf("30000000-0000-4000-8000-%012x", i+1), incarnation: fmt.Sprintf("31000000-0000-4000-8000-%012x", i+1), tenant: tenantA,
			collection: "archive", docType: "guide", text: fmt.Sprintf("Archive guide number %d.", i), preview: "Guide", score: .5, readers: map[string]bool{userA1: true},
		}
		rig.w.documents = append(rig.w.documents, doc)
		docs = append(docs, doc.id)
	}
	rig.w.mu.Unlock()
	rig.w.addSet(userA1, id, docs...)
	return id
}

func TestHostedSearchBudgetPricesTheRouteBeforeAnyProviderWork(t *testing.T) {
	const partitionSize = 2
	for _, tc := range []struct {
		name                string
		members, limit      int
		mode                string
		status              int
		maxCalls, exactCall int // exactCall > 0 requires exactly that many hosted calls
	}{
		{"auto fits the full route", 4, 10, "", http.StatusOK, 5, 0},
		{"auto falls back to one global pass", 6, 5, "", http.StatusOK, 3, 3},
		{"auto at the ceiling is one global pass", 10, 5, "", http.StatusOK, 5, 5},
		{"explicit global fits", 10, 5, "global", http.StatusOK, 5, 5},
		{"explicit local fits its full route", 4, 5, "local", http.StatusOK, 5, 0},
		{"explicit local does not fit", 6, 5, "local", http.StatusUnprocessableEntity, 0, 0},
		{"explicit global does not fit", 12, 5, "global", http.StatusUnprocessableEntity, 0, 0},
		{"auto cannot fit even one pass", 12, 5, "", http.StatusUnprocessableEntity, 0, 0},
		{"one request budget", 2, 1, "", http.StatusOK, 1, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rig := newSetRig(t, domain.GrantScopeMembership, func(cfg *config.Config) { cfg.DocumentSetMaxBatches = tc.limit })
			rig.store.partitionSize = partitionSize
			set := bigSet(rig, tc.members)
			body := `{` + setQuery + `,"document_set_id":"` + set + `"`
			if tc.mode != "" {
				body += `,"mode":"` + tc.mode + `"`
			}
			rec := rig.serve(t, userA1, body+`}`, acceptJSON)
			require.Equal(t, tc.status, rec.Code, rec.Body.String())
			if tc.status == http.StatusUnprocessableEntity {
				requireErrorBody(t, rec.Body.String(), "scope_too_large", "authorized document scope exceeds the supported size")
				require.Zero(t, rig.providerCalls(), "a route that cannot fit makes no paid call, not even the embedding")
				require.Zero(t, rig.store.hostedCalls)
				return
			}
			require.LessOrEqual(t, rig.store.hostedCalls, tc.limit, "every hosted request counts against the budget")
			require.LessOrEqual(t, rig.store.hostedCalls, tc.maxCalls)
			if tc.exactCall > 0 {
				require.Equal(t, tc.exactCall, rig.store.hostedCalls)
				rig.store.mu.Lock()
				require.Len(t, rig.store.filters, 1, "one global pass, with no probe")
				rig.store.mu.Unlock()
			}
		})
	}
}

func TestHostedSearchNeverExceedsTheBudgetForAnyPartitionCount(t *testing.T) {
	for _, policy := range []string{config.RoutingPolicyLegacy, config.RoutingPolicyEvidence} {
		for members := 2; members <= 12; members += 2 {
			for limit := 1; limit <= 10; limit++ {
				for _, mode := range []string{"", "global", "local"} {
					t.Run(fmt.Sprintf("%s/%d members/%d batches/%s", policy, members, limit, mode), func(t *testing.T) {
						rig := newSetRig(t, domain.GrantScopeMembership, func(cfg *config.Config) {
							cfg.DocumentSetMaxBatches = limit
							cfg.RoutingPolicy, cfg.RoutingCalibrationMatched, cfg.EffectiveRoutingMargin = policy, true, .1
						})
						rig.store.partitionSize = 2
						set := bigSet(rig, members)
						body := `{` + setQuery + `,"document_set_id":"` + set + `","mode":"` + mode + `"}`
						rec := rig.serve(t, userA1, body, acceptJSON)
						partitions := members / 2
						fits := map[string]bool{"": partitions <= limit, "global": partitions <= limit, "local": 2*partitions < limit}[mode]
						if !fits {
							require.Equal(t, http.StatusUnprocessableEntity, rec.Code, rec.Body.String())
							require.Zero(t, rig.providerCalls(), "a rejected route makes zero paid calls")
							require.Zero(t, rig.store.hostedCalls)
							return
						}
						require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
						require.LessOrEqual(t, rig.store.hostedCalls, limit)
					})
				}
			}
		}
	}
}

func TestRelationStoreNeedsNoHostedBudget(t *testing.T) {
	rig := newSetRig(t, domain.GrantScopeRelation, func(cfg *config.Config) { cfg.DocumentSetMaxBatches = 1 })
	set := bigSet(rig, 40)
	rec := rig.serve(t, userA1, `{`+setQuery+`,"document_set_id":"`+set+`"}`, acceptJSON)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	rig.store.mu.Lock()
	defer rig.store.mu.Unlock()
	for _, filter := range rig.store.filters {
		require.Nil(t, filter.Authorized.Budget, "a single SQL statement has no hosted attempts to count")
		require.Nil(t, filter.DocumentIDs, "the membership is joined in SQL, never listed")
	}
}

func TestSetQueriesAreCheckedBeforeEveryHostedBatch(t *testing.T) {
	rig := newSetRig(t, domain.GrantScopeMembership, nil)
	rig.store.partitionSize = 1
	rig.store.afterQuery = func(n int) {
		if n == 1 {
			rig.w.expireSet(setFull)
		}
	}
	rec := rig.serve(t, userA1, setBody(setFull, ""), acceptJSON)
	require.Equal(t, http.StatusNotFound, rec.Code, rec.Body.String())
	requireErrorBody(t, rec.Body.String(), "not_found", "document set not found")
	log := rig.w.logSnapshot()
	require.NotContains(t, log, "send:llm", "no model request follows the expiry")
	require.NotContains(t, rig.everythingSent(), sharedText)
}

func TestExpiryBetweenAdmissionAndReleaseWithholdsEveryDisclosure(t *testing.T) {
	for _, kind := range grantKinds {
		for _, tc := range []struct {
			name, extra string
			hook        func(*grantRig)
		}{
			{"before the model request", `"mode":"global"`, func(rig *grantRig) { rig.store.afterQuery = func(int) { rig.w.expireSet(setFull) } }},
			{"before a rerank", `"mode":"global"`, func(rig *grantRig) { rig.store.afterQuery = func(int) { rig.w.expireSet(setFull) } }},
			{"before the JSON release", `"mode":"global"`, func(rig *grantRig) { rig.converser.afterGenerate = func() { rig.w.expireSet(setFull) } }},
			{"between evidence-only checks", `"mode":"global","answer_mode":"evidence"`, func(rig *grantRig) { rig.store.afterQuery = func(int) { rig.w.expireSet(setFull) } }},
		} {
			t.Run(string(kind)+"/"+tc.name, func(t *testing.T) {
				rig := newSetRig(t, kind, nil)
				tc.hook(rig)
				rec := rig.serve(t, userA1, setBody(setFull, tc.extra), acceptJSON)
				require.Equal(t, http.StatusNotFound, rec.Code, rec.Body.String())
				requireErrorBody(t, rec.Body.String(), "not_found", "document set not found")
				require.NotContains(t, rec.Body.String(), "Answer")
				require.NotContains(t, rec.Body.String(), visibleText)
			})
		}
		t.Run(string(kind)+"/stream", func(t *testing.T) {
			rig := newSetRig(t, kind, nil)
			rig.converser.beforeFrame = func(n int) {
				if n == 2 {
					rig.w.expireSet(setFull)
				}
			}
			rec := rig.serve(t, userA1, setBody(setFull, `"mode":"global"`), acceptSSE)
			events := recordedEvents(t, rec.Body.String())
			require.Equal(t, "context", events[0].name)
			last := events[len(events)-1]
			require.Equal(t, "error", last.name)
			require.JSONEq(t, `"not_found"`, string(last.data["error"]))
			require.JSONEq(t, `"document set not found"`, string(last.data["message"]))
			for _, event := range events {
				require.NotEqual(t, "done", event.name)
			}
			require.Len(t, rig.converser.deltas, 1, "nothing is sent after the expiry")
		})
	}
}

func TestPrincipalDeactivationEndsASetQuery(t *testing.T) {
	for _, kind := range grantKinds {
		t.Run(string(kind), func(t *testing.T) {
			rig := newSetRig(t, kind, nil)
			rig.store.afterQuery = func(int) { rig.w.disable(userA1) }
			rec := rig.serve(t, userA1, setBody(setFull, `"mode":"global"`), acceptJSON)
			require.Equal(t, http.StatusForbidden, rec.Code, rec.Body.String())
			require.NotContains(t, rec.Body.String(), visibleText)
			require.NotContains(t, rig.w.logSnapshot(), "send:llm")
		})
	}
}

func TestSetStateUnavailableFailsClosed(t *testing.T) {
	for _, kind := range grantKinds {
		t.Run(string(kind), func(t *testing.T) {
			rig := newSetRig(t, kind, nil)
			rig.store.afterQuery = func(int) { rig.w.stateErr = errDatabase }
			rec := rig.serve(t, userA1, setBody(setFull, `"mode":"global"`), acceptJSON)
			require.Equal(t, http.StatusServiceUnavailable, rec.Code, rec.Body.String())
			require.NotContains(t, rec.Body.String(), errDatabase.Error())
			require.NotContains(t, rig.w.logSnapshot(), "send:llm")
		})
	}
}

func TestSetQueryHostedRefillIsNotUsed(t *testing.T) {
	rig := newSetRig(t, domain.GrantScopeMembership, func(cfg *config.Config) {
		cfg.HostedVisibilityRefill, cfg.HostedVisibilityExtraRequests = true, 3
	})
	rec := rig.serve(t, userA1, setBody(setFull, `"mode":"global"`), acceptJSON)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	rig.store.mu.Lock()
	defer rig.store.mu.Unlock()
	require.Len(t, rig.store.filters, 1, "one search; no optional refill request spends the budget")
}

func newSetRigWith(t *testing.T, kind domain.GrantScopeKind, prepare func(*grantRig)) *grantRig {
	t.Helper()
	rig := newGrantRigWith(t, kind, func(cfg *config.Config) { cfg.DocumentSetsEnabled = true }, prepare, nil)
	newSetWorld(rig.w)
	return rig
}

func newSetRigWithAuthority(t *testing.T, kind domain.GrantScopeKind, wrap func(*grantWorld) domain.ReadAuthority) *grantRig {
	t.Helper()
	rig := newGrantRigWith(t, kind, func(cfg *config.Config) { cfg.DocumentSetsEnabled = true }, nil, wrap)
	newSetWorld(rig.w)
	return rig
}

func FuzzDocumentSetReference(f *testing.F) {
	for _, seed := range []string{`null`, `""`, `"` + setFull + `"`, `"` + strings.ToUpper(setFull) + `"`, `123`, `[]`, `{}`, `"x"`, "\"" + setFull + "\x00\"", `"2` + setFull[1:] + `"`} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, raw string) {
		var req QueryRequest
		err := decodeJSONObject([]byte(`{"query":"q","document_set_id":`+raw+`}`), &req)
		if err != nil {
			require.Empty(t, string(req.DocumentSetID), "a refused reference leaves nothing behind")
			return
		}
		parsed, parseErr := uuid.Parse(string(req.DocumentSetID))
		require.NoError(t, parseErr)
		require.Equal(t, parsed.String(), string(req.DocumentSetID), "an accepted reference is canonical")
		require.Len(t, string(req.DocumentSetID), 36)
	})
}

func TestSetQueryCitationsDiagnosticsAndEventsNeverNameADocumentOutsideTheSet(t *testing.T) {
	forbidden := []string{docHidden, docOther, hiddenText, "Zephyr", docVisible, visibleText}
	for _, kind := range grantKinds {
		for _, accept := range []string{acceptJSON, acceptSSE} {
			t.Run(string(kind)+"/"+accept, func(t *testing.T) {
				rig := newSetRig(t, kind, nil)
				rec := rig.serve(t, userA1, setBody(setShared, `"mode":"global","include_citations":true`), accept)
				require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
				body := rec.Body.String()
				require.Contains(t, body, docShared)
				for _, text := range forbidden {
					require.NotContains(t, body+rig.everythingSent(), text)
				}
				if accept == acceptJSON {
					var response QueryResponse
					require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
					require.NotEmpty(t, response.Citations)
					for _, citation := range response.Citations {
						require.Equal(t, docShared, citation.DocID)
					}
					return
				}
				events := recordedEvents(t, body)
				require.Equal(t, "done", events[len(events)-1].name)
				var citations []CitationResponse
				require.NoError(t, json.Unmarshal(events[len(events)-1].data["citations"], &citations))
				require.NotEmpty(t, citations)
				for _, citation := range citations {
					require.Equal(t, docShared, citation.DocID)
				}
			})
		}
	}
}

func TestClientCannotSupplyServerOwnedSetState(t *testing.T) {
	for _, field := range []string{"owner", "owner_principal_id", "principal_id", "tenant_id", "revision", "expires_at", "document_set_revision", "max_batches", "batch_budget", "members"} {
		for _, kind := range grantKinds {
			t.Run(string(kind)+"/"+field, func(t *testing.T) {
				rig := newSetRig(t, kind, nil)
				rec := rig.serve(t, userA1, setBody(setFull, `"`+field+`":"x"`), acceptJSON)
				require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
				require.Contains(t, rec.Body.String(), "unknown field")
				require.Zero(t, rig.providerCalls())
			})
		}
	}
}

func TestSetQueryEmbedsTheQuestionOnce(t *testing.T) {
	for _, kind := range grantKinds {
		for _, mode := range []string{"", "global", "local"} {
			t.Run(string(kind)+"/"+mode, func(t *testing.T) {
				rig := newSetRig(t, kind, nil)
				rec := rig.serve(t, userA1, setBody(setFull, `"mode":"`+mode+`"`), acceptJSON)
				require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
				rig.embedder.mu.Lock()
				defer rig.embedder.mu.Unlock()
				require.Len(t, rig.embedder.queries, 1, "probe, local pass, top-up and global pass share one embedding")
			})
		}
	}
}
