package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/client"
	"github.com/Victoroide/quarry-rag/internal/api/handler"
	"github.com/Victoroide/quarry-rag/internal/chunker"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/evaluation"
	"github.com/Victoroide/quarry-rag/internal/store/memory"
	"github.com/Victoroide/quarry-rag/internal/store/postgres"
	"github.com/Victoroide/quarry-rag/internal/testutil/pgtest"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestCanceledAcceptanceAndSeparatelyCappedCleanup(t *testing.T) {
	db, err := postgres.NewClient(t.Context(), pgtest.Start(t))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close(context.Background())) })
	require.NoError(t, db.RunMigrations(t.Context()))

	ns := evaluation.Namespace{Prefix: "integration", ID: uuid.New()}
	doc, borrowed := evaluation.Documents()[0], evaluation.Documents()[1]
	ownedID, borrowedID := ns.DocumentID(doc.Key), ns.DocumentID(borrowed.Key)
	collection := ns.Collection(false)
	_, err = db.AcceptDocument(t.Context(), domain.AcceptRequest{
		Document:    domain.Document{ID: domain.DocumentID(borrowedID), Content: borrowed.Text},
		Fingerprint: chunker.ScopedFingerprint(collection, borrowed.Text), CallerSupplied: true,
		Collection: collection, ContentDedup: domain.ContentDedupCollection, MaxPending: 8,
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	ingest := handler.EvaluationFixtureIngestHandler(nil, db, handler.WithEvaluationFixtures(ns.Prefix, fixtureTestVectorStore))
	mux := http.NewServeMux()
	mux.HandleFunc("POST /evaluation/fixtures", func(w http.ResponseWriter, r *http.Request) {
		rec := httptest.NewRecorder()
		ingest.ServeHTTP(rec, r)
		if rec.Code != http.StatusAccepted {
			t.Errorf("durable ingest returned %d", rec.Code)
		}
		cancel()
		<-r.Context().Done()
		w.WriteHeader(rec.Code)
	})
	mux.HandleFunc("GET /evaluation/fixtures/{doc_id}", handler.EvaluationFixtureStatusHandler(db, ns.Prefix))
	var deletes atomic.Int32
	deleteDoc := handler.EvaluationFixtureDeleteHandler(db, memory.NewFakeVectorStore(), handler.EvaluationFixtureOptions{Prefix: ns.Prefix, VectorIdentity: fixtureTestVectorStore})
	mux.HandleFunc("DELETE /evaluation/fixtures/{doc_id}", func(w http.ResponseWriter, r *http.Request) {
		deletes.Add(1)
		if got := r.PathValue("doc_id"); got != ownedID {
			t.Errorf("unexpected deletion of %s", got)
		}
		deleteDoc.ServeHTTP(w, r)
	})
	server := httptest.NewServer(mux)
	defer server.Close()
	target := targetConfig{Name: "pg", BaseURL: server.URL}
	path := filepath.Join(t.TempDir(), "ledger.json")
	l, err := createLedger(path, target, ns)
	require.NoError(t, err)

	budget := &requestBudget{limit: 3}
	c, err := newLiveClient(target, "", budget, time.Second)
	require.NoError(t, err)
	require.Equal(t, "preexisting", submit(ctx, c, l, borrowed).Outcome)
	require.Equal(t, 2, budget.limit-budget.used)
	require.Equal(t, "canceled", submit(ctx, c, l, doc).Outcome)
	require.Equal(t, 3, budget.used)
	require.Equal(t, map[string]ledgerEntry{doc.Key: {State: "pending"}}, l.Entries)
	accepted, err := db.GetDocumentSummary(t.Context(), domain.DocumentID(ownedID))
	require.NoError(t, err)
	require.NotNil(t, accepted)

	require.NoError(t, l.Close())
	loaded, err := openLedger(path, target)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, loaded.Close()) })
	require.Equal(t, []string{ownedID}, loaded.remainingIDs())
	require.Equal(t, "request_budget", cleanup(t.Context(), c, loaded)[0].Outcome)
	require.Zero(t, deletes.Load())

	fresh := &requestBudget{limit: 2}
	c, err = newLiveClient(target, "", fresh, time.Second)
	require.NoError(t, err)
	require.Equal(t, "deleted", cleanup(t.Context(), c, loaded)[0].Outcome)
	require.Equal(t, 2, fresh.used)
	require.EqualValues(t, 1, deletes.Load())
	require.Empty(t, loaded.remainingIDs())
	original, err := db.GetDocumentSummary(t.Context(), domain.DocumentID(borrowedID))
	require.NoError(t, err)
	require.NotNil(t, original)
}

func TestSynthesisFailuresInReport(t *testing.T) {
	ns := evaluation.Namespace{Prefix: "stage", ID: uuid.New()}
	q := evaluation.Questions()[0]
	var r report
	for _, message := range []string{
		"synthesis input counting failed",
		"answer synthesis returned no text",
		"unrecognized synthesis failure",
	} {
		t.Run(message, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusBadGateway)
				if err := json.NewEncoder(w).Encode(client.ErrorResponse{Error: "synthesis", Message: message}); err != nil {
					t.Error(err)
				}
			}))
			t.Cleanup(server.Close)
			c, err := client.New(server.URL)
			require.NoError(t, err)
			s := checkQuery(t.Context(), c, ns, uuid.NewString(), q, options{K: 3})
			require.Equal(t, http.StatusBadGateway, s.HTTPStatus)
			r.Queries = append(r.Queries, s)
		})
	}
	r.summarize(true)
	all := r.Slices["all"]
	require.Equal(t, 3, all.Selected)
	require.Zero(t, all.Results)
	require.Equal(t, map[string]int{"http_error": 2, "empty_synthesis": 1}, all.Errors)
	require.Equal(t, 1, all.EmptySynthesisErrors)
}

func TestVisibilityWaitIgnoresStaleAndReportsThrottle(t *testing.T) {
	ns := evaluation.Namespace{Prefix: "stage", ID: uuid.New()}
	doc := evaluation.Documents()[0]
	var calls atomic.Int32
	complete := time.Now().UTC()
	revision := uuid.NewString()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /evaluation/fixtures/{doc_id}", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(evaluation.StateHeader, "owned")
		writeFixtureStatus(w, r, ns.Collection(false), revision, complete)
	})
	mux.HandleFunc("POST /query", func(w http.ResponseWriter, r *http.Request) {
		var req client.QueryRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decode query: %v", err)
		}
		if req.AnswerMode != client.AnswerModeEvidence || len(req.AllowedDocumentIDs) != 1 ||
			req.AllowedDocumentIDs[0] != ns.DocumentID(doc.Key) || req.Collection != ns.Collection(false) {
			t.Errorf("unexpected visibility scope %+v", req)
		}
		switch calls.Add(1) {
		case 1:
			_ = json.NewEncoder(w).Encode(client.QueryResponse{FinishReason: "evidence_only", ScoredChunks: []client.ScoredChunkResponse{
				{DocID: ns.DocumentID(doc.Key), Text: "Stale body", Excerpt: 1},
			}})
		case 2:
			_ = json.NewEncoder(w).Encode(client.QueryResponse{FinishReason: "evidence_only", ScoredChunks: []client.ScoredChunkResponse{
				{DocID: ns.DocumentID(doc.Key), Text: doc.Text, Excerpt: 1},
			}})
		default:
			w.Header().Set("Retry-After", "10")
			w.WriteHeader(http.StatusTooManyRequests)
		}
	})
	server := httptest.NewServer(mux)
	defer server.Close()
	target := targetConfig{Name: "stub", BaseURL: server.URL}
	l, err := createLedger(filepath.Join(t.TempDir(), "ledger.json"), target, ns)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, l.Close()) })
	require.NoError(t, l.set(doc.Key, ledgerEntry{State: "accepted"}))
	budget := &requestBudget{limit: 10}
	c, err := newLiveClient(target, "", budget, time.Second)
	require.NoError(t, err)
	got := waitVisible(t.Context(), c, l, doc, time.Second, time.Millisecond)
	require.Equal(t, "observed", got.Outcome)
	require.True(t, got.Completed)
	require.Equal(t, "unmeasured", got.EventClock)
	require.Nil(t, got.EventDelayMS)
	require.NotNil(t, got.DelayMS)
	require.EqualValues(t, 2, calls.Load())
	got = waitVisible(t.Context(), c, l, doc, time.Second, time.Millisecond)
	require.Equal(t, "failed", got.Outcome)
	require.Equal(t, "throttled", got.Reason)
	require.Equal(t, http.StatusTooManyRequests, got.Status)
	require.EqualValues(t, 3, calls.Load())
}

func TestHardBudgetStopsBeforeAnyRequest(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		calls.Add(1)
	}))
	defer server.Close()
	target := targetConfig{Name: "budget", BaseURL: server.URL}
	for _, settings := range []options{{MaxTokens: 1}, {MaxSpend: 1}} {
		budget := &requestBudget{limit: 1, hardBudget: settings.hardBudget()}
		c, err := newLiveClient(target, "", budget, time.Second)
		require.NoError(t, err)
		_, err = c.GetDocument(t.Context(), uuid.NewString())
		require.ErrorIs(t, err, errHardBudget)
		require.Zero(t, budget.used)
	}
	require.Zero(t, calls.Load())
}

func TestCleanupRefusesReplacedIncarnationAndBorrowedCollection(t *testing.T) {
	ns := evaluation.Namespace{Prefix: "stage", ID: uuid.New()}
	doc := evaluation.Documents()[0]
	for _, tc := range []struct{ name, collection, previous string }{
		{"replaced", ns.Collection(false), uuid.NewString()},
		{"borrowed", "another-collection", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet {
					t.Error("unowned document reached delete")
				}
				revision := uuid.NewString()
				w.Header().Set(evaluation.StateHeader, "owned")
				w.Header().Set(evaluation.IncarnationHeader, revision)
				_ = json.NewEncoder(w).Encode(client.DocumentIncarnationSummary{DocumentSummary: client.DocumentSummary{
					ID: ns.DocumentID(doc.Key), Collection: tc.collection, Status: "processing",
				}, Incarnation: revision})
			}))
			defer server.Close()
			target := targetConfig{Name: "stub", BaseURL: server.URL}
			l, err := createLedger(filepath.Join(t.TempDir(), "ledger.json"), target, ns)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, l.Close()) })
			require.NoError(t, l.set(doc.Key, ledgerEntry{State: "accepted", Incarnation: tc.previous}))
			budget := &requestBudget{limit: 2}
			c, err := newLiveClient(target, "", budget, time.Second)
			require.NoError(t, err)
			require.Equal(t, "ownership_mismatch", cleanup(t.Context(), c, l)[0].Outcome)
			require.Equal(t, 1, budget.used)
			require.Equal(t, []string{ns.DocumentID(doc.Key)}, l.remainingIDs())
		})
	}
}

func TestBorrowedReceiptNeverReachesCleanup(t *testing.T) {
	ns := evaluation.Namespace{Prefix: "stage", ID: uuid.New()}
	doc := evaluation.Documents()[0]
	for _, ack := range []client.IngestResponse{
		{DocumentID: uuid.NewString(), Status: "processing"},
		{DocumentID: ns.DocumentID(doc.Key), Status: "already_ingested"},
	} {
		t.Run(ack.Status, func(t *testing.T) {
			mux := http.NewServeMux()
			mux.HandleFunc("GET /evaluation/fixtures/{doc_id}", func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set(evaluation.StateHeader, "missing")
				http.NotFound(w, r)
			})
			mux.HandleFunc("POST /evaluation/fixtures", func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set(evaluation.StateHeader, "borrowed")
				_ = json.NewEncoder(w).Encode(ack)
			})
			mux.HandleFunc("DELETE /evaluation/fixtures/{doc_id}", func(http.ResponseWriter, *http.Request) {
				t.Error("borrowed response reached cleanup")
			})
			server := httptest.NewServer(mux)
			defer server.Close()
			target := targetConfig{Name: "stub", BaseURL: server.URL}
			l, err := createLedger(filepath.Join(t.TempDir(), "ledger.json"), target, ns)
			require.NoError(t, err)
			budget := &requestBudget{limit: 3}
			c, err := newLiveClient(target, "", budget, time.Second)
			require.NoError(t, err)
			require.Equal(t, "borrowed_or_duplicate", submit(t.Context(), c, l, doc).Outcome)
			require.NoError(t, l.Close())
			loaded, err := openLedger(l.path, target)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, loaded.Close()) })
			require.Empty(t, loaded.remainingIDs())
			require.Empty(t, cleanup(t.Context(), c, loaded))
			require.Equal(t, 2, budget.used)
		})
	}
}

func TestCompletedVisibilityIsCensoredWhenBudgetEnds(t *testing.T) {
	ns := evaluation.Namespace{Prefix: "stage", ID: uuid.New()}
	doc := evaluation.Documents()[0]
	complete := time.Now().UTC()
	revision := uuid.NewString()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /evaluation/fixtures/{doc_id}", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(evaluation.StateHeader, "owned")
		writeFixtureStatus(w, r, ns.Collection(false), revision, complete)
	})
	mux.HandleFunc("POST /query", func(http.ResponseWriter, *http.Request) {
		t.Error("search exceeded the request budget")
	})
	server := httptest.NewServer(mux)
	defer server.Close()
	target := targetConfig{Name: "stub", BaseURL: server.URL}
	l, err := createLedger(filepath.Join(t.TempDir(), "ledger.json"), target, ns)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, l.Close()) })
	budget := &requestBudget{limit: 1}
	c, err := newLiveClient(target, "", budget, time.Second)
	require.NoError(t, err)
	got := waitVisible(t.Context(), c, l, doc, time.Second, time.Millisecond)
	require.Equal(t, "censored", got.Outcome)
	require.Equal(t, "request_budget", got.Reason)
	require.Equal(t, complete.Format(time.RFC3339Nano), got.CompletedAt)
	require.Nil(t, got.DelayMS)
	require.NotNil(t, got.CensoredMS)
	require.Equal(t, 1, budget.used)
}
