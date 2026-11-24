package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/client"
	"github.com/Victoroide/quarry-rag/internal/api/handler"
	"github.com/Victoroide/quarry-rag/internal/api/middleware"
	"github.com/Victoroide/quarry-rag/internal/chunker"
	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/embedder"
	"github.com/Victoroide/quarry-rag/internal/evaluation"
	"github.com/Victoroide/quarry-rag/internal/llm"
	"github.com/Victoroide/quarry-rag/internal/retriever"
	"github.com/Victoroide/quarry-rag/internal/store/memory"
	"github.com/Victoroide/quarry-rag/internal/store/postgres"
	"github.com/Victoroide/quarry-rag/internal/testutil/pgtest"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const fixtureTestVectorStore = "memory:fixture-test"

func TestConcurrentFixtureAdmissionDoesNotBorrowProcessingDocument(t *testing.T) {
	db := fixtureTestDatabase(t)
	ns := evaluation.Namespace{Prefix: "stage", ID: uuid.New()}
	doc := evaluation.Documents()[0]
	ingest := handler.EvaluationFixtureIngestHandler(nil, db, handler.WithEvaluationFixtures(ns.Prefix, fixtureTestVectorStore))
	mux := http.NewServeMux()
	mux.HandleFunc("GET /evaluation/fixtures/{doc_id}", handler.EvaluationFixtureStatusHandler(db, ns.Prefix))
	mux.HandleFunc("POST /evaluation/fixtures", func(w http.ResponseWriter, r *http.Request) {
		_, err := admitOrdinary(r.Context(), db, ns, doc, doc.Text)
		if err != nil {
			t.Errorf("concurrent admission: %v", err)
		}
		ingest.ServeHTTP(w, r)
	})
	l, c := fixtureTestClient(t, ns, mux, 5)
	got := submit(t.Context(), c, l, doc)
	require.Equal(t, http.StatusConflict, got.Status)
	require.Empty(t, l.remainingIDs())

	// The borrowed processing document consumes the only queue slot.
	fullNS := evaluation.Namespace{Prefix: "stage", ID: uuid.New()}
	full := http.NewServeMux()
	full.HandleFunc("GET /evaluation/fixtures/{doc_id}", handler.EvaluationFixtureStatusHandler(db, fullNS.Prefix))
	full.HandleFunc("POST /evaluation/fixtures", handler.EvaluationFixtureIngestHandler(nil, db,
		handler.WithEvaluationFixtures(fullNS.Prefix, fixtureTestVectorStore), handler.WithMaxPending(1)))
	l, c = fixtureTestClient(t, fullNS, full, 2)
	require.Equal(t, http.StatusServiceUnavailable, submit(t.Context(), c, l, doc).Status)
	require.Empty(t, l.remainingIDs())
}

type interruptedFixtureVectors struct {
	domain.VectorDeleter
	sweeps atomic.Int32
}

func (v *interruptedFixtureVectors) DeleteByDocID(ctx context.Context, id string) error {
	if v.sweeps.Add(1) == 1 {
		return errors.New("fixture vector sweep interrupted")
	}
	return v.VectorDeleter.DeleteByDocID(ctx, id)
}

func TestReplacementAttemptAfterFixtureStatusCannotBeDeleted(t *testing.T) {
	db := fixtureTestDatabase(t)
	ns := evaluation.Namespace{Prefix: "stage", ID: uuid.New()}
	doc := evaluation.Documents()[0]
	id := ns.DocumentID(doc.Key)
	mux := http.NewServeMux()
	mux.HandleFunc("POST /evaluation/fixtures", handler.EvaluationFixtureIngestHandler(nil, db, handler.WithEvaluationFixtures(ns.Prefix, fixtureTestVectorStore)))
	mux.HandleFunc("GET /evaluation/fixtures/{doc_id}", handler.EvaluationFixtureStatusHandler(db, ns.Prefix))
	vectors := &interruptedFixtureVectors{VectorDeleter: memory.NewFakeVectorStore()}
	remove := handler.EvaluationFixtureDeleteHandler(db, vectors, handler.EvaluationFixtureOptions{Prefix: ns.Prefix, VectorIdentity: fixtureTestVectorStore})
	replaced := make(chan bool, 1)
	var deleteRequests atomic.Int32
	mux.HandleFunc("DELETE /evaluation/fixtures/{doc_id}", func(w http.ResponseWriter, r *http.Request) {
		if deleteRequests.Add(1) == 2 {
			rec := httptest.NewRecorder()
			remove.ServeHTTP(rec, r)
			if rec.Code != http.StatusOK {
				t.Errorf("durable cleanup returned %d", rec.Code)
			}
			loseFixtureReply(t, w)
			return
		}
		if err := db.DeleteDocument(r.Context(), domain.DocumentID(id)); err == nil {
			t.Error("bare SQL deletion bypassed active fixture reservation")
		}
		admission, err := admitOrdinary(r.Context(), db, ns, doc, "Replacement with different content.")
		replaced <- err == nil && admission.Outcome == domain.AcceptAccepted
		remove.ServeHTTP(w, r)
	})
	l, c := fixtureTestClient(t, ns, mux, 8)
	require.Equal(t, "accepted", submit(t.Context(), c, l, doc).Outcome)
	wrong := httptest.NewRequestWithContext(t.Context(), http.MethodDelete, "/evaluation/fixtures/"+id, http.NoBody)
	wrong.SetPathValue("doc_id", id)
	wrong.Header.Set(evaluation.FixtureHeader, ns.ID.String())
	wrong.Header.Set(evaluation.ReceiptHeader, l.Receipt)
	wrong.Header.Set(evaluation.IncarnationHeader, uuid.NewString())
	rec := httptest.NewRecorder()
	remove.ServeHTTP(rec, wrong)
	require.Equal(t, http.StatusPreconditionFailed, rec.Code)
	require.Zero(t, vectors.sweeps.Load())
	require.Equal(t, http.StatusInternalServerError, cleanup(t.Context(), c, l)[0].Status)
	require.Equal(t, "deleting", l.Entries[doc.Key].State)
	original, err := db.GetDocument(t.Context(), domain.DocumentID(id))
	require.NoError(t, err)
	require.NotNil(t, original)
	select {
	case accepted := <-replaced:
		require.False(t, accepted, "the reserved ID allowed replacement between the read and DELETE")
	default:
		t.Fatal("the request did not exercise the replacement boundary")
	}
	require.Equal(t, "client_error", cleanup(t.Context(), c, l)[0].Outcome)
	require.Equal(t, "deleting", l.Entries[doc.Key].State)
	require.Equal(t, "deleted", cleanup(t.Context(), c, l)[0].Outcome)
	require.Empty(t, l.remainingIDs())
	require.EqualValues(t, 2, vectors.sweeps.Load())
	_, err = admitOrdinary(t.Context(), db, ns, doc, "Replacement after cleanup.")
	require.Error(t, err)
}

func TestLegacyDeploymentDoesNotReceiveUnqualifiedWrites(t *testing.T) {
	ns := evaluation.Namespace{Prefix: "stage", ID: uuid.New()}
	doc := evaluation.Documents()[0]
	mux := http.NewServeMux()
	mux.HandleFunc("POST /ingest", func(http.ResponseWriter, *http.Request) {
		t.Error("legacy ingestion received a staged fixture")
	})
	mux.HandleFunc("DELETE /documents/{doc_id}", func(http.ResponseWriter, *http.Request) {
		t.Error("legacy deletion received a staged fixture")
	})
	l, c := fixtureTestClient(t, ns, mux, 2)
	require.Equal(t, "ownership_inconclusive", submit(t.Context(), c, l, doc).Outcome)
	require.Empty(t, l.remainingIDs())
	require.NoError(t, l.set(doc.Key, ledgerEntry{State: "pending"}))
	require.Equal(t, "ownership_inconclusive", cleanup(t.Context(), c, l)[0].Outcome)
	require.Equal(t, []string{ns.DocumentID(doc.Key)}, l.remainingIDs())
}

func loseFixtureReply(t *testing.T, w http.ResponseWriter) {
	t.Helper()
	h, ok := w.(http.Hijacker)
	if !ok {
		t.Error("missing test connection hijacker")
		return
	}
	conn, _, err := h.Hijack()
	if err != nil {
		t.Errorf("lose delete reply: %v", err)
		return
	}
	_ = conn.Close()
}

func TestFailedOrLostDeleteKeepsLedgerUntilReceipt(t *testing.T) {
	for _, tc := range []string{"500", "lost_reply"} {
		t.Run(tc, func(t *testing.T) {
			ns := evaluation.Namespace{Prefix: "stage", ID: uuid.New()}
			doc := evaluation.Documents()[0]
			incarnation := uuid.NewString()
			var deletes atomic.Int32
			var reconciled atomic.Bool
			mux := http.NewServeMux()
			mux.HandleFunc("GET /evaluation/fixtures/{doc_id}", func(w http.ResponseWriter, r *http.Request) {
				if deletes.Load() > 0 {
					if reconciled.Load() {
						w.Header().Set(evaluation.StateHeader, "deleted")
						w.Header().Set(evaluation.IncarnationHeader, incarnation)
					}
					http.NotFound(w, r)
					return
				}
				w.Header().Set(evaluation.StateHeader, "owned")
				writeFixtureStatus(w, r, ns.Collection(false), incarnation, time.Now())
			})
			mux.HandleFunc("DELETE /evaluation/fixtures/{doc_id}", func(w http.ResponseWriter, _ *http.Request) {
				deletes.Add(1)
				if tc == "500" {
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
				loseFixtureReply(t, w)
			})
			l, c := fixtureTestClient(t, ns, mux, 6)
			require.NoError(t, l.set(doc.Key, ledgerEntry{State: "accepted", Incarnation: incarnation}))
			require.NotEqual(t, "deleted", cleanup(t.Context(), c, l)[0].Outcome)
			require.Equal(t, "deleting", l.Entries[doc.Key].State)
			require.NotEqual(t, "already_absent", cleanup(t.Context(), c, l)[0].Outcome)
			require.Equal(t, []string{ns.DocumentID(doc.Key)}, l.remainingIDs())
			reconciled.Store(true)
			require.Equal(t, "deleted", cleanup(t.Context(), c, l)[0].Outcome)
			require.Empty(t, l.remainingIDs())
			require.EqualValues(t, 1, deletes.Load())
		})
	}
}

func TestLegacyProjectionRetainsPinnedFixture(t *testing.T) {
	ns := evaluation.Namespace{Prefix: "stage", ID: uuid.New()}
	doc := evaluation.Documents()[0]
	id, revision := ns.DocumentID(doc.Key), uuid.NewString()
	var gets, deletes atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("GET /evaluation/fixtures/{doc_id}", func(w http.ResponseWriter, r *http.Request) {
		if gets.Add(1) == 1 {
			writeFixtureStatus(w, r, ns.Collection(false), "", time.Now())
			return
		}
		w.Header().Set(evaluation.StateHeader, "owned")
		writeFixtureStatus(w, r, ns.Collection(false), revision, time.Now())
	})
	mux.HandleFunc("DELETE /evaluation/fixtures/{doc_id}", func(w http.ResponseWriter, _ *http.Request) {
		deletes.Add(1)
		w.Header().Set(evaluation.StateHeader, "deleted")
		w.Header().Set(evaluation.IncarnationHeader, revision)
		_ = json.NewEncoder(w).Encode(client.DeleteResponse{Deleted: true, DocID: id})
	})
	l, c := fixtureTestClient(t, ns, mux, 6)
	require.NoError(t, l.set(doc.Key, ledgerEntry{State: "accepted", Incarnation: revision}))
	require.Equal(t, "ownership_inconclusive", cleanup(t.Context(), c, l)[0].Outcome)
	require.Equal(t, revision, l.Entries[doc.Key].Incarnation)
	require.Zero(t, deletes.Load())
	require.Equal(t, "deleted", cleanup(t.Context(), c, l)[0].Outcome)
	require.Empty(t, l.remainingIDs())
}

func TestFixtureLedgerLockSurvivesRenameAndReleasesOnExit(t *testing.T) {
	path := os.Getenv("QUARRY_EVAL_TEST_LOCK_PATH")
	target := targetConfig{Name: "test", BaseURL: "lock-target"}
	if path != "" {
		l, err := createLedger(path, target, evaluation.Namespace{Prefix: "stage", ID: uuid.New()})
		require.NoError(t, err)
		require.NoError(t, l.set("atlas", ledgerEntry{State: "pending"}))
		_, err = io.WriteString(os.Stdout, "ready\n")
		require.NoError(t, err)
		_, err = io.Copy(io.Discard, os.Stdin)
		require.NoError(t, err)
		return
	}
	path = filepath.Join(t.TempDir(), "ledger.json")
	executable, err := os.Executable()
	require.NoError(t, err)
	childInput, in, err := os.Pipe()
	require.NoError(t, err)
	defer func() { _ = childInput.Close() }()
	out, childOutput, err := os.Pipe()
	require.NoError(t, err)
	defer func() { _ = childOutput.Close() }()
	proc, err := os.StartProcess(executable, []string{executable, "-test.run=^TestFixtureLedgerLockSurvivesRenameAndReleasesOnExit$"}, &os.ProcAttr{
		Env: append(os.Environ(), "QUARRY_EVAL_TEST_LOCK_PATH="+path), Files: []*os.File{childInput, childOutput, os.Stderr},
	})
	require.NoError(t, err)
	require.NoError(t, childInput.Close())
	require.NoError(t, childOutput.Close())
	t.Cleanup(func() {
		_ = in.Close()
		_ = out.Close()
		_ = proc.Kill()
		_, _ = proc.Wait()
	})
	var ready [6]byte
	_, err = io.ReadFull(out, ready[:])
	require.NoError(t, err)
	require.Equal(t, "ready\n", string(ready[:]))
	_, lockErr := openLedger(path, target)
	require.NoError(t, in.Close())
	exited, err := proc.Wait()
	require.NoError(t, err)
	require.True(t, exited.Success())
	require.ErrorContains(t, lockErr, "acquire fixture ledger lock", "another writer opened the live ledger after rename")
	l, err := openLedger(path, target)
	require.NoError(t, err)
	require.Equal(t, ledgerEntry{State: "pending"}, l.Entries["atlas"])
	require.NoError(t, l.Close())
	l, err = openLedger(path, target)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, l.Close()) })
	require.Len(t, l.remainingIDs(), 1)
}

func TestRejectedFixtureIngestClearsIntent(t *testing.T) {
	for _, tc := range []struct {
		code       string
		httpStatus int
	}{{"queue", 503}, {"validation", 400}, {"unauthorized", 401}} {
		t.Run(tc.code, func(t *testing.T) {
			ns := evaluation.Namespace{Prefix: "stage", ID: uuid.New()}
			doc := evaluation.Documents()[0]
			mux := http.NewServeMux()
			mux.HandleFunc("GET /evaluation/fixtures/{doc_id}", func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set(evaluation.StateHeader, "missing")
				http.NotFound(w, r)
			})
			reject := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if tc.httpStatus == http.StatusUnauthorized {
					t.Error("unauthenticated request reached admission")
				}
				w.WriteHeader(tc.httpStatus)
				_ = json.NewEncoder(w).Encode(client.ErrorResponse{Error: tc.code})
			})
			if tc.httpStatus == http.StatusUnauthorized {
				reject = middleware.RequireServiceTokens([]string{"rotated-token"}, reject)
			}
			mux.HandleFunc("POST /evaluation/fixtures", reject)
			l, c := fixtureTestClient(t, ns, mux, 2)
			require.Equal(t, tc.httpStatus, submit(t.Context(), c, l, doc).Status)
			require.Empty(t, l.remainingIDs())
		})
	}
}

func TestCancellationBeforeIngestDispatchClearsIntent(t *testing.T) {
	ns := evaluation.Namespace{Prefix: "stage", ID: uuid.New()}
	doc := evaluation.Documents()[0]
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /evaluation/fixtures/{doc_id}", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(evaluation.StateHeader, "missing")
		http.NotFound(w, r)
	})
	mux.HandleFunc("POST /evaluation/fixtures", func(http.ResponseWriter, *http.Request) { t.Error("canceled ingest reached server") })
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	target := targetConfig{Name: "test", BaseURL: server.URL}
	l, err := createLedger(filepath.Join(t.TempDir(), "ledger.json"), target, ns)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, l.Close()) })
	wrapped := budgetTransport{base: server.Client().Transport, budget: &requestBudget{limit: 2}}
	hc := &http.Client{Transport: testRoundTrip(func(req *http.Request) (*http.Response, error) {
		if req.Method == http.MethodPost {
			cancel()
		}
		return wrapped.RoundTrip(req)
	})}
	c, err := client.New(target.BaseURL, client.WithHTTPClient(hc))
	require.NoError(t, err)
	require.Equal(t, "canceled", submit(ctx, c, l, doc).Outcome)
	require.Empty(t, l.remainingIDs())
}

func TestCleanupReconcilesInterruptedAndDelayedSubmission(t *testing.T) {
	db := fixtureTestDatabase(t)
	doc := evaluation.Documents()[0]
	for _, delayed := range []bool{false, true} {
		name := "before dispatch"
		if delayed {
			name = "delayed admission"
		}
		t.Run(name, func(t *testing.T) {
			ns := evaluation.Namespace{Prefix: "stage", ID: uuid.New()}
			mux := http.NewServeMux()
			ingest := handler.EvaluationFixtureIngestHandler(nil, db, handler.WithEvaluationFixtures(ns.Prefix, fixtureTestVectorStore))
			reached, unblock := make(chan struct{}), make(chan struct{})
			mux.HandleFunc("POST /evaluation/fixtures", func(w http.ResponseWriter, r *http.Request) {
				if delayed {
					close(reached)
					<-unblock
				}
				ingest.ServeHTTP(w, r)
			})
			mux.HandleFunc("GET /evaluation/fixtures/{doc_id}", handler.EvaluationFixtureStatusHandler(db, ns.Prefix))
			mux.HandleFunc("DELETE /evaluation/fixtures/{doc_id}", handler.EvaluationFixtureDeleteHandler(db, memory.NewFakeVectorStore(), handler.EvaluationFixtureOptions{Prefix: ns.Prefix, VectorIdentity: fixtureTestVectorStore}))
			server := httptest.NewServer(mux)
			t.Cleanup(server.Close)
			target := targetConfig{Name: "test", BaseURL: server.URL}
			l, err := createLedger(filepath.Join(t.TempDir(), "ledger.json"), target, ns)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, l.Close()) })
			c, err := newLiveClient(target, "", &requestBudget{limit: 2}, 5*time.Second)
			require.NoError(t, err)
			require.NoError(t, l.set(doc.Key, ledgerEntry{State: "pending"}))
			if delayed {
				ctx, _ := fixtureExchange(t.Context(), l, "")
				late, clientErr := newLiveClient(target, "", &requestBudget{limit: 1}, 5*time.Second)
				require.NoError(t, clientErr)
				lateAbort := sync.OnceFunc(func() { close(unblock) })
				defer lateAbort()
				result := make(chan error, 1)
				go func() {
					_, submitErr := late.IngestEvaluationFixture(ctx, client.IngestRequest{DocumentID: ns.DocumentID(doc.Key), Collection: ns.Collection(false), Content: doc.Text})
					result <- submitErr
				}()
				select {
				case <-reached:
				case <-time.After(3 * time.Second):
					t.Fatal("late ingest did not reach handler")
				}
				got := cleanup(t.Context(), c, l)
				assert.Equal(t, "retired", got[0].Outcome)
				lateAbort()
				_, code := failure(<-result)
				assert.Equal(t, http.StatusConflict, code)
			} else {
				require.Equal(t, "retired", cleanup(t.Context(), c, l)[0].Outcome)
			}
			require.Empty(t, l.remainingIDs())
			row, err := db.GetDocument(t.Context(), domain.DocumentID(ns.DocumentID(doc.Key)))
			require.NoError(t, err)
			require.Nil(t, row)
		})
	}
}

func TestReconciliationPinsAdmissionThatWonAfterMissingStatus(t *testing.T) {
	db := fixtureTestDatabase(t)
	ns := evaluation.Namespace{Prefix: "stage", ID: uuid.New()}
	doc := evaluation.Documents()[0]
	id := ns.DocumentID(doc.Key)
	status := handler.EvaluationFixtureStatusHandler(db, ns.Prefix)
	ingest := handler.EvaluationFixtureIngestHandler(nil, db, handler.WithEvaluationFixtures(ns.Prefix, fixtureTestVectorStore))
	remove := handler.EvaluationFixtureDeleteHandler(db, memory.NewFakeVectorStore(), handler.EvaluationFixtureOptions{
		Prefix: ns.Prefix, VectorIdentity: fixtureTestVectorStore,
	})
	mux := http.NewServeMux()
	var firstStatus atomic.Bool
	mux.HandleFunc("GET /evaluation/fixtures/{doc_id}", func(w http.ResponseWriter, r *http.Request) {
		if !firstStatus.CompareAndSwap(false, true) {
			status(w, r)
			return
		}
		stale := httptest.NewRecorder()
		status(stale, r)
		body, err := json.Marshal(client.IngestRequest{DocumentID: id, Collection: ns.Collection(false), Content: doc.Text})
		if err != nil {
			t.Error(err)
			return
		}
		post := httptest.NewRequestWithContext(r.Context(), http.MethodPost, "/evaluation/fixtures", bytes.NewReader(body))
		post.Header = r.Header.Clone()
		post.Header.Set("Content-Type", "application/json")
		admitted := httptest.NewRecorder()
		ingest.ServeHTTP(admitted, post)
		if admitted.Code != http.StatusAccepted {
			t.Errorf("admission between status and reconciliation: %d %s", admitted.Code, admitted.Body.String())
		}
		for name, values := range stale.Header() {
			w.Header()[name] = values
		}
		w.WriteHeader(stale.Code)
		_, _ = w.Write(stale.Body.Bytes())
	})
	mux.HandleFunc("DELETE /evaluation/fixtures/{doc_id}", remove)
	l, c := fixtureTestClient(t, ns, mux, 4)
	require.NoError(t, l.set(doc.Key, ledgerEntry{State: "pending"}))
	require.Equal(t, "ownership_pending", cleanup(t.Context(), c, l)[0].Outcome)
	require.Equal(t, []string{id}, l.remainingIDs())
	entry := l.Entries[doc.Key]
	require.Equal(t, "accepted", entry.State)
	require.True(t, evaluation.ValidIncarnation(entry.Incarnation))
	fixture, err := db.ReadEvaluationFixture(t.Context(), domain.DocumentID(id), l.Receipt)
	require.NoError(t, err)
	require.Equal(t, domain.FixtureOwned, fixture.State)
	require.Equal(t, fixture.Incarnation, entry.Incarnation)
	require.Equal(t, "deleted", cleanup(t.Context(), c, l)[0].Outcome)
	require.Empty(t, l.remainingIDs())
}

type testRoundTrip func(*http.Request) (*http.Response, error)

func (f testRoundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestVisibilityUsesCurrentChunkWhenGoldSpansSplit(t *testing.T) {
	db := fixtureTestDatabase(t)
	ns := evaluation.Namespace{Prefix: "stage", ID: uuid.New()}
	doc := evaluation.Documents()[0]
	id := domain.DocumentID(ns.DocumentID(doc.Key))
	chunks := chunker.NewChunker(8, 0).Chunk(id, doc.Text)
	store := memory.NewFakeVectorStore()
	emb := embedder.NewFakeEmbedder(8)
	for i := range chunks {
		require.NotContains(t, chunks[i].Text, doc.Spans[0].Text)
		vector, err := emb.Embed(t.Context(), chunks[i].Text)
		require.NoError(t, err)
		chunks[i].Embedding = vector
		chunks[i].Metadata["collection"] = ns.Collection(false)
		chunks[i].Metadata["doc_type"] = domain.DefaultDocType
	}
	require.Greater(t, len(chunks), 1)
	require.NoError(t, store.Upsert(t.Context(), "", chunks))
	q := evaluation.Questions()[0]
	receipt := uuid.NewString()
	for _, spec := range evaluation.Documents() {
		if !slices.Contains(q.Allowed, spec.Key) {
			continue
		}
		selected := domain.DocumentID(ns.DocumentID(spec.Key))
		accepted, err := db.AcceptDocument(t.Context(), domain.AcceptRequest{
			Document:   domain.Document{ID: selected, Content: spec.Text},
			Collection: ns.Collection(false), CallerSupplied: true, ContentDedup: domain.ContentDedupOff,
			DocType: domain.DefaultDocType, Fingerprint: spec.Key, MaxPending: 10,
			EvaluationReceipt: receipt, EvaluationVectorIdentity: fixtureTestVectorStore,
		})
		require.NoError(t, err)
		require.Equal(t, domain.AcceptAccepted, accepted.Outcome)
		job, err := db.ClaimJobForFixtureStore(t.Context(), "fixture-query", time.Minute, 2, fixtureTestVectorStore)
		require.NoError(t, err)
		require.NotNil(t, job)
		var stored []domain.Chunk
		if selected == id {
			stored = chunks
		}
		done, err := db.CompleteJob(t.Context(), *job, stored, domain.IndexObservation{})
		require.NoError(t, err)
		require.True(t, done)
	}
	cfg := &config.Config{EvaluationCollectionPrefix: ns.Prefix, MaxChunksPerQuery: 3, BedrockFallbackModel: "fixture"}
	amb := llm.NewAmbassador(cfg)
	amb.SetClient(llm.NewFakeConverser())
	ret := retriever.NewRetriever(store, emb, 0.7, 3)
	w := httptest.NewServer(handler.QueryHandler(ret, amb, cfg, db.Pool))
	t.Cleanup(w.Close)
	c, err := newLiveClient(targetConfig{BaseURL: w.URL}, "", &requestBudget{limit: 2}, 5*time.Second)
	require.NoError(t, err)
	v := pollEvidence(t.Context(), c, ns, doc, visibility{Outcome: "failed"}, time.Now(), time.Millisecond)
	require.Equal(t, "observed", v.Outcome)
	s := checkQuery(t.Context(), c, ns, receipt, q, options{Instrument: true, K: 3})
	metrics := summarize([]sample{s})
	require.Equal(t, proportion(1, 1), metrics.Recall)
	require.Equal(t, proportion(0, 1), metrics.Coverage)
}

func TestLoopbackProviderCallsRemainUnverified(t *testing.T) {
	ns := evaluation.Namespace{Prefix: "stage", ID: uuid.New()}
	atlas := evaluation.Documents()[0]
	vector := memory.NewFakeVectorStore()
	require.NoError(t, vector.Upsert(t.Context(), "", []domain.Chunk{{
		ID: domain.ChunkID(uuid.NewString()), DocumentID: domain.DocumentID(ns.DocumentID(atlas.Key)),
		Text: atlas.Text, Embedding: []float32{1, 0},
		Metadata: map[string]any{"collection": ns.Collection(false), "doc_type": domain.DefaultDocType},
	}}))
	var embeds, generates atomic.Int32
	upstream := http.NewServeMux()
	upstream.HandleFunc("POST /embeddings", func(w http.ResponseWriter, _ *http.Request) {
		embeds.Add(1)
		_, _ = io.WriteString(w, `{"data":[{"index":0,"embedding":[1,0]}]}`)
	})
	upstream.HandleFunc("POST /chat/completions", func(w http.ResponseWriter, _ *http.Request) {
		generates.Add(1)
		_, _ = io.WriteString(w, `{"choices":[{"message":{"role":"assistant","content":"Exactly four violet lamps."},"finish_reason":"eos"}],"usage":{"prompt_tokens":9,"completion_tokens":4}}`)
	})
	provider := httptest.NewServer(upstream)
	t.Cleanup(provider.Close)
	cfg := &config.Config{
		EvaluationCollectionPrefix: ns.Prefix, VectorStore: "postgres", MaxChunksPerQuery: 3,
		LLMProvider: "openai", EmbeddingProvider: "openai", OpenAIBaseURL: provider.URL,
		OpenAIChatModel: "fixture-chat", OpenAIEmbeddingModel: "fixture-emb", EmbeddingDimensions: 2,
		EmbedConcurrency: 1, EmbedTimeout: 3 * time.Second,
	}
	emb, err := embedder.NewOpenAIEmbedder(cfg)
	require.NoError(t, err)
	gen, err := llm.NewOpenAIClient(cfg)
	require.NoError(t, err)
	amb := llm.NewAmbassador(cfg)
	amb.SetGenerator(gen)
	ret := retriever.NewRetriever(vector, emb, 0.7, 3)
	query := handler.QueryHandler(ret, amb, cfg, nil)
	tls := httptest.NewTLSServer(query)
	t.Cleanup(tls.Close)
	plain := httptest.NewServer(query)
	t.Cleanup(plain.Close)
	fakeCfg := *cfg
	fakeCfg.FakeMode = true
	fake := httptest.NewTLSServer(handler.QueryHandler(ret, amb, &fakeCfg, nil))
	t.Cleanup(fake.Close)
	deployed := deployment{Backend: "postgres", EmbeddingProvider: "openai", AnswerProvider: "openai"}
	for _, tc := range []struct {
		server   *httptest.Server
		question evaluation.Question
		source   string
	}{
		{tls, evaluation.Questions()[0], "live_unverified"},
		{plain, evaluation.Questions()[0], "live_unverified"},
		{tls, evaluation.Questions()[5], "live_unverified"},
		{fake, evaluation.Questions()[5], "offline"},
	} {
		hc := &http.Client{Transport: budgetTransport{base: tc.server.Client().Transport, budget: &requestBudget{limit: 1}}}
		c, err := client.New(tc.server.URL, client.WithHTTPClient(hc))
		require.NoError(t, err)
		s := checkQuery(t.Context(), c, ns, "", tc.question, options{K: 3})
		require.Empty(t, s.Error)
		r := report{Backend: deployed.Backend, Deployment: &deployed, Queries: []sample{s}}
		r.summarize(true)
		require.Equal(t, tc.source, r.Source)
		if tc.question.ID == "atlas-en" {
			require.Equal(t, "unknown", s.Finish)
			require.Equal(t, 1, s.RecallHits)
			require.Equal(t, &client.TokenUsage{InputTokens: 9, OutputTokens: 4, TotalTokens: 13}, s.SynthesisUsage)
		}
	}
	require.EqualValues(t, 2, embeds.Load())
	require.EqualValues(t, 2, generates.Load())
}

func TestCompletionEventTimingIncludesDelayedStatusObservation(t *testing.T) {
	ns := evaluation.Namespace{Prefix: "stage", ID: uuid.New()}
	doc := evaluation.Documents()[0]
	observed := time.Now()
	completed := observed.Add(-200 * time.Millisecond)
	revision := uuid.NewString()
	for _, censor := range []bool{false, true} {
		mux := http.NewServeMux()
		mux.HandleFunc("GET /evaluation/fixtures/{doc_id}", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set(evaluation.StateHeader, "owned")
			w.Header().Set(evaluation.DatabaseTimeHeader, observed.UTC().Format(time.RFC3339Nano))
			writeFixtureStatus(w, r, ns.Collection(false), revision, completed)
		})
		mux.HandleFunc("POST /query", func(w http.ResponseWriter, _ *http.Request) {
			_ = json.NewEncoder(w).Encode(client.QueryResponse{
				FinishReason: client.FinishReasonEvidenceOnly, ScoredChunks: []client.ScoredChunkResponse{{DocID: ns.DocumentID(doc.Key), Text: doc.Text, Excerpt: 1}},
			})
		})
		limit := 2
		if censor {
			limit = 1
		}
		l, c := fixtureTestClient(t, ns, mux, limit)
		require.NoError(t, l.set(doc.Key, ledgerEntry{State: "accepted", Incarnation: revision}))
		got := waitVisible(t.Context(), c, l, doc, time.Second, time.Millisecond)
		data, err := json.Marshal(got)
		require.NoError(t, err)
		var fields map[string]any
		require.NoError(t, json.Unmarshal(data, &fields))
		name := "completion_event_to_visible_ms"
		if censor {
			name = "completion_event_to_censor_ms"
		}
		bounds, ok := fields[name].(map[string]any)
		require.True(t, ok, "missing completion clock bounds: %s", data)
		require.GreaterOrEqual(t, bounds["lower"].(float64), 199.0)
		require.GreaterOrEqual(t, bounds["upper"].(float64), bounds["lower"].(float64))
	}
}

func fixtureTestDatabase(t *testing.T) *postgres.Client {
	t.Helper()
	db, err := postgres.NewClient(t.Context(), pgtest.Start(t))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close(context.Background())) })
	require.NoError(t, db.RunMigrations(t.Context()))
	return db
}

func admitOrdinary(ctx context.Context, db *postgres.Client, ns evaluation.Namespace, doc evaluation.Document, content string) (domain.AcceptResult, error) {
	collection := ns.Collection(false)
	return db.AcceptDocument(ctx, domain.AcceptRequest{
		Document:       domain.Document{ID: domain.DocumentID(ns.DocumentID(doc.Key)), Content: content},
		CallerSupplied: true, Fingerprint: chunker.ScopedFingerprint(collection, content),
		Collection: collection, ContentDedup: domain.ContentDedupCollection, MaxPending: 8,
	})
}

func fixtureTestClient(t *testing.T, ns evaluation.Namespace, h http.Handler, limit int) (*ledger, *client.Client) {
	t.Helper()
	server := httptest.NewServer(h)
	t.Cleanup(server.Close)
	target := targetConfig{Name: "test", BaseURL: server.URL}
	l, err := createLedger(filepath.Join(t.TempDir(), "ledger.json"), target, ns)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, l.Close()) })
	c, err := newLiveClient(target, "", &requestBudget{limit: limit}, 5*time.Second)
	require.NoError(t, err)
	return l, c
}

func writeFixtureStatus(w http.ResponseWriter, r *http.Request, collection, incarnation string, completed time.Time) {
	if incarnation != "" {
		w.Header().Set(evaluation.IncarnationHeader, incarnation)
	}
	_ = json.NewEncoder(w).Encode(client.DocumentIncarnationSummary{DocumentSummary: client.DocumentSummary{
		ID: r.PathValue("doc_id"), Collection: collection, Status: "completed", CompletedAt: &completed,
	}, Incarnation: incarnation})
}

func TestOfflineAndEmptyScopeResultsAreNotHosted(t *testing.T) {
	offline := deployment{EmbeddingProvider: "offline", AnswerProvider: "offline"}
	r := report{Target: "offline", Deployment: &offline, Queries: []sample{{ID: "atlas-en", Gold: 1}}}
	r.summarize(true)
	require.Equal(t, "offline", r.Source)
	r = report{Target: "empty", Queries: []sample{{ID: "empty-scope", NoEvidence: true}}}
	r.summarize(true)
	require.Equal(t, "live_unverified", r.Source)
}

func TestHostedClassificationRequiresAttestedUpstreamsOverTLS(t *testing.T) {
	ns := evaluation.Namespace{Prefix: "stage", ID: uuid.New()}
	answer := client.QueryResponse{
		Answer:       "Exactly four violet lamps.",
		ScoredChunks: []client.ScoredChunkResponse{{DocID: ns.DocumentID("atlas"), Score: .9}},
	}
	deployed := deployment{Backend: "postgres", EmbeddingProvider: "openai", AnswerProvider: "bedrock"}
	for _, tc := range []struct {
		name, origins, want string
		secure              bool
	}{
		{"verified service and providers", "api.openai.com,bedrock-runtime.us-east-1.amazonaws.com", "hosted", true},
		{"plain service", "api.openai.com,bedrock-runtime.us-east-1.amazonaws.com", "live_unverified", false},
		{"one provider unidentified", "api.openai.com,", "live_unverified", true},
		{"wrong provider", "api.openai.com,api.openai.com", "live_unverified", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			query := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set(evaluation.ExecutionHeader, "postgres,openai,bedrock")
				w.Header().Set(evaluation.UpstreamsHeader, tc.origins)
				_ = json.NewEncoder(w).Encode(answer)
			})
			server := httptest.NewUnstartedServer(query)
			if tc.secure {
				server.StartTLS()
			} else {
				server.Start()
			}
			t.Cleanup(server.Close)
			hc := &http.Client{Transport: budgetTransport{base: server.Client().Transport, budget: &requestBudget{limit: 1}}}
			c, err := client.New(server.URL, client.WithHTTPClient(hc))
			require.NoError(t, err)
			s := checkQuery(t.Context(), c, ns, "", evaluation.Questions()[0], options{K: 3})
			require.Empty(t, s.Error)
			r := report{Backend: deployed.Backend, Deployment: &deployed, Queries: []sample{s}}
			r.summarize(true)
			require.Equal(t, tc.want, r.Source)
		})
	}
}

func TestReconciliationFencesBorrowedDocumentWithoutReservingItsIncarnation(t *testing.T) {
	db := fixtureTestDatabase(t)
	ns := evaluation.Namespace{Prefix: "stage", ID: uuid.New()}
	doc := evaluation.Documents()[0]
	ordinary, err := admitOrdinary(t.Context(), db, ns, doc, doc.Text)
	require.NoError(t, err)
	require.Equal(t, domain.AcceptAccepted, ordinary.Outcome)
	lister := db.WithDocumentIncarnations()
	before, err := lister.GetDocumentSummary(t.Context(), ordinary.DocumentID)
	require.NoError(t, err)
	require.NotNil(t, before)
	require.True(t, evaluation.ValidIncarnation(before.Incarnation))
	ingest := handler.EvaluationFixtureIngestHandler(nil, db, handler.WithEvaluationFixtures(ns.Prefix, fixtureTestVectorStore))
	mux := http.NewServeMux()
	mux.HandleFunc("POST /evaluation/fixtures", ingest)
	mux.HandleFunc("GET /evaluation/fixtures/{doc_id}", handler.EvaluationFixtureStatusHandler(db, ns.Prefix))
	mux.HandleFunc("DELETE /evaluation/fixtures/{doc_id}", handler.EvaluationFixtureDeleteHandler(db, memory.NewFakeVectorStore(), handler.EvaluationFixtureOptions{Prefix: ns.Prefix, VectorIdentity: fixtureTestVectorStore}))
	mux.HandleFunc("DELETE /documents/{doc_id}", handler.DeleteHandler(db, nil, nil))
	l, c := fixtureTestClient(t, ns, mux, 4)
	require.NoError(t, l.set(doc.Key, ledgerEntry{State: "pending"}))
	assert.Equal(t, "retired", cleanup(t.Context(), c, l)[0].Outcome)
	assert.Empty(t, l.remainingIDs())
	row, err := lister.GetDocumentSummary(t.Context(), ordinary.DocumentID)
	require.NoError(t, err)
	require.NotNil(t, row)
	require.Equal(t, before.Incarnation, row.Incarnation)
	_, err = c.DeleteDocument(t.Context(), string(ordinary.DocumentID))
	require.NoError(t, err)

	ctx, _ := fixtureExchange(t.Context(), l, "")
	_, err = c.IngestEvaluationFixture(ctx, client.IngestRequest{DocumentID: string(ordinary.DocumentID), Collection: ns.Collection(false), Content: doc.Text})
	_, code := failure(err)
	assert.Equal(t, http.StatusConflict, code)
	replacement, err := admitOrdinary(t.Context(), db, ns, doc, doc.Text)
	require.NoError(t, err)
	require.Equal(t, domain.AcceptAccepted, replacement.Outcome)
	after, err := lister.GetDocumentSummary(t.Context(), replacement.DocumentID)
	require.NoError(t, err)
	require.NotNil(t, after)
	require.True(t, evaluation.ValidIncarnation(after.Incarnation))
	require.NotEqual(t, before.Incarnation, after.Incarnation)
}
