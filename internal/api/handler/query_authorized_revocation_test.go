package handler

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/stretchr/testify/require"
)

const (
	forbiddenKind    = "forbidden"
	forbiddenMessage = "access denied"
)

// revocations are the two ways a decision can change under a running read.
var revocations = map[string]func(*grantWorld){
	"grant revoked":      func(w *grantWorld) { w.revoke(userA1, docVisible) },
	"principal disabled": func(w *grantWorld) { w.disable(userA1) },
}

func TestAuthorizedQueryRevocationBeforeAnyDisclosure(t *testing.T) {
	const generated = `{"query":"What do the archive documents say?","mode":"global"}`
	for _, kind := range grantKinds {
		for name, revoke := range revocations {
			for _, accept := range []string{acceptJSON, acceptSSE} {
				for _, point := range []string{"after routing", "after reranking", "during generation"} {
					if point == "during generation" && accept == acceptSSE {
						continue // covered frame by frame below
					}
					t.Run(string(kind)+"/"+name+"/"+accept+"/"+point, func(t *testing.T) {
						rig := newGrantRig(t, kind, nil)
						body := generated
						switch point {
						case "after routing":
							body = `{"query":"What do the archive documents say?"}`
							// The probe has been checked and routing is done when the
							// scoped retrieval search starts.
							rig.store.afterQuery = func(n int) {
								if n == 2 {
									revoke(rig.w)
								}
							}
						case "after reranking":
							rig.onRerank = func() { revoke(rig.w) }
						case "during generation":
							rig.converser.afterGenerate = func() { revoke(rig.w) }
						}
						rec := rig.serve(t, userA1, body, accept)
						requirePrivate(t, rec)
						require.Equal(t, http.StatusForbidden, rec.Code, rec.Body.String())
						requireErrorBody(t, rec.Body.String(), forbiddenKind, forbiddenMessage)
						for _, text := range []string{visibleText, sharedText, hiddenText, "Answer one", docVisible, docShared} {
							require.NotContains(t, rec.Body.String(), text)
						}
						if point != "during generation" {
							require.Zero(t, rig.converser.prompts, "no model input after the decision changed")
						}
					})
				}
			}
		}
	}
}

func TestAuthorizedQueryRevocationAfterStreamingBeginsEndsWithABoundedError(t *testing.T) {
	for _, kind := range grantKinds {
		for name, revoke := range revocations {
			for _, frame := range []int{1, 2, 3} {
				t.Run(fmt.Sprintf("%s/%s/frame%d", kind, name, frame), func(t *testing.T) {
					rig := newGrantRig(t, kind, nil)
					rig.converser.beforeFrame = func(n int) {
						if n == frame {
							revoke(rig.w)
						}
					}
					rec := rig.serve(t, userA1, `{"query":"What do the archive documents say?","mode":"global"}`, acceptSSE)
					requirePrivate(t, rec)
					require.Equal(t, http.StatusOK, rec.Code)
					events := recordedEvents(t, rec.Body.String())
					names := make([]string, len(events))
					for i, event := range events {
						names[i] = event.name
					}
					want := []string{"context"}
					for range frame - 1 {
						want = append(want, "delta")
					}
					require.Equal(t, append(want, "error"), names, "no frame follows the changed decision")
					last := events[len(events)-1]
					require.JSONEq(t, `"`+forbiddenKind+`"`, string(last.data["error"]))
					require.JSONEq(t, `"`+forbiddenMessage+`"`, string(last.data["message"]))
					require.Len(t, last.data, 2, "the terminal error carries nothing else")
					require.Len(t, rig.converser.deltas, frame-1, "upstream stopped at the rejected frame")
					require.NotContains(t, rec.Body.String(), "done")
				})
			}
		}
	}
}

func TestAuthorizedQueryRevocationBeforeTheClosingFrame(t *testing.T) {
	for _, kind := range grantKinds {
		for name, revoke := range revocations {
			t.Run(string(kind)+"/"+name, func(t *testing.T) {
				rig := newGrantRig(t, kind, nil)
				rig.converser.afterFrames = func() { revoke(rig.w) }
				rec := rig.serve(t, userA1, `{"query":"What do the archive documents say?","mode":"global","include_citations":true}`, acceptSSE)
				events := recordedEvents(t, rec.Body.String())
				names := make([]string, len(events))
				for i, event := range events {
					names[i] = event.name
				}
				require.Equal(t, []string{"context", "delta", "delta", "delta", "error"}, names, "no done frame, citations or usage follow the revocation")
				require.NotContains(t, rec.Body.String(), "citations")
				require.NotContains(t, rec.Body.String(), "token_usage")
			})
		}
	}
}

func TestAuthorizedQueryRevocationBeforeJSONRelease(t *testing.T) {
	for _, kind := range grantKinds {
		for _, answer := range []string{"generated", "evidence"} {
			t.Run(string(kind)+"/"+answer, func(t *testing.T) {
				rig := newGrantRig(t, kind, nil)
				switch answer {
				case "generated":
					// The answer exists when the grant changes; only the release check remains.
					rig.converser.afterGenerate = func() { rig.w.revoke(userA1, docShared) }
				default:
					// Evidence-only has no provider call. Its only check after retrieval is
					// the release check, which reads the principal and then the evidence.
					rig.w.onCheck = func(kind string, n int) {
						if kind == "state" && n == 1 {
							rig.w.revoke(userA1, docShared)
						}
					}
				}
				rec := rig.serve(t, userA1, `{"query":"What do the archive documents say?","mode":"global","answer_mode":"`+answer+`"}`, acceptJSON)
				require.Equal(t, http.StatusForbidden, rec.Code, rec.Body.String())
				requireErrorBody(t, rec.Body.String(), forbiddenKind, forbiddenMessage)
				require.NotContains(t, rec.Body.String(), sharedText)
			})
		}
	}
}

func TestAuthorizedQueryDisclosuresFollowTheirImmediateCheck(t *testing.T) {
	for _, kind := range grantKinds {
		for _, tc := range []struct{ name, body, accept string }{
			{"generated json", `{"query":"What do the archive documents say?","mode":"global"}`, acceptJSON},
			{"generated stream", `{"query":"What do the archive documents say?","mode":"global"}`, acceptSSE},
			{"evidence json", `{"query":"What do the archive documents say?","mode":"global","answer_mode":"evidence"}`, acceptJSON},
			{"evidence stream", `{"query":"What do the archive documents say?","mode":"global","answer_mode":"evidence"}`, acceptSSE},
			{"no evidence", `{"query":"What do the archive documents say?","allowed_document_ids":[]}`, acceptJSON},
		} {
			t.Run(string(kind)+"/"+tc.name, func(t *testing.T) {
				rig := newGrantRig(t, kind, nil)
				rec := rig.serve(t, userA1, tc.body, tc.accept)
				require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
				log := rig.w.logSnapshot()
				disclosures := 0
				for i, entry := range log {
					if entry == "send:llm" || entry == "send:rerank" || entry == "frame" {
						disclosures++
						require.GreaterOrEqual(t, i, 2, "%v", log)
						require.Equal(t, []string{"state", "evidence"}, log[i-2:i], "%s at %d follows its own check: %v", entry, i, log)
					}
				}
				if strings.Contains(tc.name, "no evidence") {
					require.Equal(t, "state", log[len(log)-1], "a response without evidence still checks the principal")
				} else {
					if tc.accept == acceptJSON {
						require.Equal(t, []string{"state", "evidence"}, log[len(log)-2:], "JSON release follows its own check")
					}
					if tc.name != "evidence json" {
						require.Positive(t, disclosures)
					}
				}
			})
		}
	}
}

func TestAuthorizedQueryCancellationBeforeAndAfterHeaders(t *testing.T) {
	for _, kind := range grantKinds {
		t.Run(string(kind)+"/before headers", func(t *testing.T) {
			rig := newGrantRig(t, kind, nil)
			req, cancel := rig.request(t.Context(), userA1, `{"query":"What do the archive documents say?","mode":"global"}`, acceptJSON, time.Time{})
			cancel()
			rec := httptest.NewRecorder()
			rig.handler.ServeHTTP(rec, req)
			require.Empty(t, rec.Body.String(), "a canceled request is not answered")
			require.Zero(t, rig.providerCalls())
		})
		t.Run(string(kind)+"/after headers", func(t *testing.T) {
			rig := newGrantRig(t, kind, nil)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			req, release := rig.request(ctx, userA1, `{"query":"What do the archive documents say?","mode":"global"}`, acceptSSE, time.Time{})
			defer release()
			rig.converser.beforeFrame = func(n int) {
				if n == 2 {
					cancel()
				}
			}
			rec := &frameRecorder{ResponseRecorder: httptest.NewRecorder(), w: rig.w}
			rig.handler.ServeHTTP(rec, req)
			events := recordedEvents(t, rec.Body.String())
			require.Equal(t, "context", events[0].name)
			for _, event := range events {
				require.NotEqual(t, "done", event.name)
			}
			require.LessOrEqual(t, len(rig.converser.deltas), 1, "nothing is disclosed after the cancellation")
		})
	}
}

func TestAuthorizedQueryCredentialExpiryBoundsEveryDisclosure(t *testing.T) {
	const wantKind, wantMessage = "unauthorized", "invalid or missing credentials"
	for _, kind := range grantKinds {
		t.Run(string(kind)+"/json", func(t *testing.T) {
			rig := newGrantRig(t, kind, nil)
			expires := time.Now().Add(400 * time.Millisecond)
			rig.converser.afterGenerate = func() { time.Sleep(time.Until(expires) + 20*time.Millisecond) }
			rec := rig.serveWith(t, userA1, `{"query":"What do the archive documents say?","mode":"global"}`, acceptJSON, expires)
			require.Equal(t, http.StatusUnauthorized, rec.Code, rec.Body.String())
			requireErrorBody(t, rec.Body.String(), wantKind, wantMessage)
			require.NotContains(t, rec.Body.String(), "Answer")
		})
		t.Run(string(kind)+"/stream", func(t *testing.T) {
			rig := newGrantRig(t, kind, nil)
			expires := time.Now().Add(400 * time.Millisecond)
			rig.converser.beforeFrame = func(n int) {
				if n == 2 {
					time.Sleep(time.Until(expires) + 20*time.Millisecond)
				}
			}
			rec := rig.serveWith(t, userA1, `{"query":"What do the archive documents say?","mode":"global"}`, acceptSSE, expires)
			events := recordedEvents(t, rec.Body.String())
			last := events[len(events)-1]
			require.Equal(t, "error", last.name)
			require.JSONEq(t, `"`+wantKind+`"`, string(last.data["error"]))
			require.Len(t, rig.converser.deltas, 1)
		})
	}
}

func TestAuthorizedQueryDependencyFailureAfterHeadersEndsWithABoundedError(t *testing.T) {
	for _, kind := range grantKinds {
		for _, tc := range []struct {
			name, kind, message string
			fail                func(*grantWorld)
		}{
			{"database", "unavailable", "authorization unavailable", func(w *grantWorld) { w.stateErr = errDatabase }},
			{"evidence lookup", "unavailable", "authorization unavailable", func(w *grantWorld) { w.evidenceErr = errDatabase }},
			{"trust data", "unavailable", "authorization unavailable", func(w *grantWorld) { w.trustErr = domain.ErrReadDenied }},
		} {
			t.Run(string(kind)+"/"+tc.name, func(t *testing.T) {
				rig := newGrantRig(t, kind, nil)
				rig.converser.beforeFrame = func(n int) {
					if n == 2 {
						tc.fail(rig.w)
					}
				}
				rec := rig.serve(t, userA1, `{"query":"What do the archive documents say?","mode":"global"}`, acceptSSE)
				events := recordedEvents(t, rec.Body.String())
				last := events[len(events)-1]
				require.Equal(t, "error", last.name)
				require.NotContains(t, rec.Body.String(), errDatabase.Error())
				require.Len(t, rig.converser.deltas, 1)
				require.Zero(t, rig.store.unscoped)
			})
		}
	}
}
