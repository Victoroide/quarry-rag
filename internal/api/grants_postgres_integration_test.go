package api_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/Victoroide/quarry-rag/client"
	"github.com/Victoroide/quarry-rag/internal/testutil/grantse2e"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestGrantsPostgresRouterIsolation(t *testing.T) {
	for _, backend := range grantse2e.Backends {
		t.Run(backend, func(t *testing.T) {
			f := grantse2e.New(t, backend)
			docs := f.Seed(t)
			owner := f.Client(t, 0, f.Owner)
			reader := f.Client(t, 1, f.Reader)

			t.Run("credentials", func(t *testing.T) {
				for _, tc := range []struct{ name, service, token string }{
					{"anonymous", "", ""},
					{"missing service", "", f.Owner.Credential.Token},
					{"missing subject", f.ServiceToken, ""},
					{"wrong audience", f.ServiceToken, f.Sign(t, f.Owner, "other-audience", grantse2e.TokenLifetime).Token},
					{"expired", f.ServiceToken, f.Sign(t, f.Owner, grantse2e.Audience, -grantse2e.TokenLifetime).Token},
				} {
					t.Run(tc.name, func(t *testing.T) {
						resp := f.Raw(t, 1, http.MethodGet, "/documents", tc.service, tc.token, nil)
						require.Equal(t, http.StatusUnauthorized, resp.Status)
						require.Equal(t, "private, no-store", resp.Header.Get("Cache-Control"))
						require.JSONEq(t, `{"error":"unauthorized","message":"invalid or missing credentials"}`, string(resp.Body))
					})
				}
			})

			t.Run("listing and status", func(t *testing.T) {
				requirePage(t, owner, client.ListOptions{}, docs.First.ID, docs.Second.ID, docs.Text.ID)
				requirePage(t, reader, client.ListOptions{Collection: grantse2e.Collection}, docs.First.ID)
				requirePage(t, reader, client.ListOptions{Collection: "wrong-collection"})
				requirePage(t, f.Client(t, 1, f.Other), client.ListOptions{})
				requirePage(t, f.Client(t, 1, f.Foreign), client.ListOptions{}, docs.Foreign.ID)
				status, err := reader.GetDocumentWithIncarnation(t.Context(), docs.First.ID)
				require.NoError(t, err)
				require.Equal(t, docs.First.ID, status.ID)
				require.Equal(t, docs.First.Incarnation, status.Incarnation)
				require.Equal(t, "completed", status.Status)
				for _, id := range []string{docs.Second.ID, docs.Foreign.ID, f.LegacyID, uuid.NewString()} {
					_, err := reader.GetDocument(t.Context(), id)
					requireAPIError(t, err, http.StatusNotFound, "not_found")
				}
				_, err = f.Client(t, 1, f.Foreign).GetDocument(t.Context(), docs.First.ID)
				requireAPIError(t, err, http.StatusNotFound, "not_found")
			})

			t.Run("file", func(t *testing.T) {
				for _, actor := range []grantse2e.Actor{f.Owner, f.Downloader} {
					file, err := f.Client(t, 1, actor).DocumentFileURL(t.Context(), docs.First.ID)
					require.NoError(t, err)
					require.Contains(t, file.URL, docs.First.ID)
					require.Positive(t, file.ExpiresIn)
					require.LessOrEqual(t, file.ExpiresIn, 900)
				}
				_, err := reader.DocumentFileURL(t.Context(), docs.First.ID)
				requireAPIError(t, err, http.StatusNotFound, "not_found")
				for _, actor := range []grantse2e.Actor{f.Other, f.Foreign} {
					_, err := f.Client(t, 1, actor).DocumentFileURL(t.Context(), docs.First.ID)
					requireAPIError(t, err, http.StatusNotFound, "not_found")
				}
			})

			t.Run("query", func(t *testing.T) {
				for _, mode := range []string{"local", "global"} {
					for _, answer := range []client.AnswerMode{client.AnswerModeEvidence, client.AnswerModeGenerated} {
						req := grantse2e.Query(mode, answer)
						t.Run(mode+"/"+string(answer)+"/json", func(t *testing.T) {
							before := len(f.Model.Inputs())
							resp, err := reader.Query(t.Context(), req)
							require.NoError(t, err)
							require.Equal(t, strings.ToUpper(mode), resp.Mode)
							requireEvidence(t, resp.ScoredChunks, docs.First)
							requireAnswer(t, answer, resp.Answer, resp.FinishReason, resp.Citations, docs.First.ID)
							requireModelScope(t, f.Model.Inputs()[before:], answer, docs)
						})
						t.Run(mode+"/"+string(answer)+"/sse", func(t *testing.T) {
							before := len(f.Model.Inputs())
							var names []string
							var output strings.Builder
							var done client.QueryStreamDone
							err := reader.QueryStream(t.Context(), req, func(event client.Event) error {
								names = append(names, event.Type)
								switch event.Type {
								case "context":
									var data client.QueryStreamContext
									if err := json.Unmarshal(event.Data, &data); err != nil {
										return err
									}
									require.Equal(t, strings.ToUpper(mode), data.Mode)
									requireEvidence(t, data.ScoredChunks, docs.First)
								case "delta":
									var data struct{ Text string }
									if err := json.Unmarshal(event.Data, &data); err != nil {
										return err
									}
									output.WriteString(data.Text)
								case "done":
									return json.Unmarshal(event.Data, &done)
								}
								return nil
							})
							require.NoError(t, err)
							want := []string{"context", "done"}
							if answer == client.AnswerModeGenerated {
								want = []string{"context", "delta", "done"}
							}
							require.Equal(t, want, names)
							requireAnswer(t, answer, output.String(), done.FinishReason, done.Citations, docs.First.ID)
							requireModelScope(t, f.Model.Inputs()[before:], answer, docs)
						})
					}
				}
				for _, tc := range []struct {
					name, collection, docType string
					ids                       []string
					actor                     grantse2e.Actor
					visible                   bool
				}{
					{"matching filters", grantse2e.Collection, grantse2e.FileType, nil, f.Reader, true},
					{"wrong collection", "wrong-collection", "", nil, f.Reader, false},
					{"wrong type", "", grantse2e.TextType, nil, f.Reader, false},
					{"empty allowlist", "", "", []string{}, f.Reader, false},
					{"foreign allowlist", "", "", []string{docs.Foreign.ID}, f.Reader, false},
					{"empty grants", "", "", nil, f.Other, false},
				} {
					t.Run(tc.name, func(t *testing.T) {
						req := grantse2e.Query("global", client.AnswerModeEvidence)
						req.Collection, req.FilterDocType, req.AllowedDocumentIDs = tc.collection, tc.docType, tc.ids
						resp, err := f.Client(t, 1, tc.actor).Query(t.Context(), req)
						require.NoError(t, err)
						if tc.visible {
							requireEvidence(t, resp.ScoredChunks, docs.First)
						} else {
							require.Empty(t, resp.ScoredChunks)
							require.Empty(t, resp.Citations)
							require.Zero(t, resp.TokenUsage)
						}
					})
				}
				resp := f.Raw(t, 1, http.MethodPost, "/query", f.ServiceToken, f.Reader.Credential.Token,
					[]byte(`{"query":"corpus question","mode":"global","answer_mode":"evidence","allowed_document_ids":null}`))
				require.Equal(t, http.StatusOK, resp.Status)
				var result client.QueryResponse
				require.NoError(t, json.Unmarshal(resp.Body, &result))
				requireEvidence(t, result.ScoredChunks, docs.First)
			})

			t.Run("deletion and reuse", func(t *testing.T) {
				_, err := reader.DeleteDocumentIncarnation(t.Context(), docs.First.ID, docs.First.Incarnation)
				requireAPIError(t, err, http.StatusNotFound, "not_found")
				_, err = owner.DeleteDocumentIncarnation(t.Context(), docs.First.ID, uuid.NewString())
				requireAPIError(t, err, http.StatusPreconditionFailed, "precondition")
				f.RequireStoredVectors(t, docs.First, true)
				for range 2 {
					result, err := owner.DeleteDocumentIncarnation(t.Context(), docs.First.ID, docs.First.Incarnation)
					require.NoError(t, err)
					require.True(t, result.Deleted)
					require.Equal(t, docs.First.ID, result.DocID)
				}
				f.RequireFinalized(t, docs.First)
				f.RequireStoredVectors(t, docs.First, false)
				f.RequireStoredVectors(t, docs.Second, true)
				f.RequireStoredVectors(t, docs.Foreign, true)
				_, err = reader.GetDocument(t.Context(), docs.First.ID)
				requireAPIError(t, err, http.StatusNotFound, "not_found")
				_, err = owner.Ingest(t.Context(), client.IngestRequest{
					DocumentID: docs.First.ID, Collection: grantse2e.Collection, Content: "Replacement for a retired identity.",
				})
				requireAPIError(t, err, http.StatusNotFound, "not_found")
				f.RequireFinalized(t, docs.First)
			})
		})
	}
}

func requireAPIError(t *testing.T, err error, status int, code string) {
	t.Helper()
	var apiErr *client.APIError
	require.ErrorAs(t, err, &apiErr)
	require.Equal(t, status, apiErr.StatusCode)
	require.Equal(t, code, apiErr.Code)
}

func requirePage(t *testing.T, c *client.Client, opts client.ListOptions, ids ...string) {
	t.Helper()
	page, err := c.ListDocumentsWithIncarnation(t.Context(), opts)
	require.NoError(t, err)
	got := make([]string, 0, len(page.Documents))
	for _, doc := range page.Documents {
		got = append(got, doc.ID)
		require.NotEmpty(t, doc.Incarnation)
	}
	require.ElementsMatch(t, ids, got)
	require.Equal(t, len(ids), page.Count)
	require.Nil(t, page.NextAfter)
}

func requireEvidence(t *testing.T, chunks []client.ScoredChunkResponse, doc grantse2e.Document) {
	t.Helper()
	require.Len(t, chunks, 1)
	require.Equal(t, doc.ID, chunks[0].DocID)
	require.Equal(t, doc.DocType, chunks[0].DocType)
	require.Equal(t, doc.Text, chunks[0].Text)
	require.Zero(t, chunks[0].ChunkIndex)
}

func requireAnswer(t *testing.T, mode client.AnswerMode, answer, reason string, citations []client.CitationResponse, id string) {
	t.Helper()
	if mode == client.AnswerModeEvidence {
		require.Empty(t, answer)
		require.Empty(t, citations)
		require.Equal(t, client.FinishReasonEvidenceOnly, reason)
		return
	}
	require.Contains(t, answer, grantse2e.Answer)
	require.Len(t, citations, 1)
	require.Equal(t, id, citations[0].DocID)
	require.Equal(t, 1, citations[0].Excerpt)
}

func requireModelScope(t *testing.T, inputs []string, mode client.AnswerMode, docs grantse2e.Documents) {
	t.Helper()
	if mode == client.AnswerModeEvidence {
		require.Empty(t, inputs)
		return
	}
	require.Len(t, inputs, 1)
	require.Contains(t, inputs[0], docs.First.Text)
	for _, doc := range []grantse2e.Document{docs.Second, docs.Text, docs.Foreign} {
		require.NotContains(t, inputs[0], doc.Text)
		require.NotContains(t, inputs[0], doc.ID)
	}
}
