package handler

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/llm"
	"github.com/Victoroide/quarry-rag/internal/retriever"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
	brtypes "github.com/aws/aws-sdk-go-v2/service/bedrockruntime/types"
	"github.com/stretchr/testify/require"
)

type lineBoundaryAuthority struct {
	*grantWorld
	batches [][]string
	failure string
}

func (a *lineBoundaryAuthority) Evidence(p domain.Principal, collection, docType string) domain.SubjectEvidence {
	checks := a.grantWorld.Evidence(p, collection, docType)
	checks.Visibility = lineBoundaryEvidence{EvidenceVisibility: checks.Visibility, authority: a}
	return checks
}

type lineBoundaryEvidence struct {
	domain.EvidenceVisibility
	authority *lineBoundaryAuthority
}

func (e lineBoundaryEvidence) ObserveEvidenceSnapshot(context.Context) (string, error) {
	return "presearch snapshot", nil
}

func (e lineBoundaryEvidence) VisibleEvidenceWithLineBoundaries(ctx context.Context, hits []domain.ScoredChunk) ([]domain.ScoredChunk, error) {
	var batch []string
	for _, hit := range hits {
		batch = append(batch, string(hit.Chunk.DocumentID))
	}
	e.authority.batches = append(e.authority.batches, batch)
	switch e.authority.failure {
	case "error":
		return nil, errors.New("optional lookup unavailable")
	case "timeout":
		<-ctx.Done()
		return nil, ctx.Err()
	}
	current, err := e.VisibleEvidence(ctx, hits)
	for i := range current {
		if offset := strings.IndexByte(current[i].Chunk.Text, ' '); offset > 0 {
			current[i].Chunk.SourceLineBoundaries = []domain.SourceLineBoundary{{Offset: offset, Kind: domain.SourceLineBreak}}
		}
	}
	return current, err
}

type lineBoundaryConverser struct {
	handlerBudgetConverser
	requests []lineBoundaryModelRequest
}

type lineBoundaryModelRequest struct {
	model, system string
	messages      []brtypes.Message
	temperature   float32
	maxTokens     int
}

func (c *lineBoundaryConverser) ConverseWithModelAndSystem(ctx context.Context, model, system string, messages []brtypes.Message, temperature float32, maxTokens int) (*bedrockruntime.ConverseOutput, error) {
	return c.ConverseStreamWithModelAndSystem(ctx, model, system, messages, temperature, maxTokens, func(string) error { return nil })
}

func (c *lineBoundaryConverser) ConverseStreamWithModelAndSystem(ctx context.Context, model, system string, messages []brtypes.Message, temperature float32, maxTokens int, delta func(string) error) (*bedrockruntime.ConverseOutput, error) {
	c.requests = append(c.requests, lineBoundaryModelRequest{model, system, slices.Clone(messages), temperature, maxTokens})
	return c.handlerBudgetConverser.ConverseStreamWithModelAndSystem(ctx, model, system, messages, temperature, maxTokens, delta)
}

func TestQueryLineBoundaryParity(t *testing.T) {
	type observation struct {
		body     map[string]json.RawMessage
		events   []recordedEvent
		model    []lineBoundaryModelRequest
		embedded []string
		reranked []string
	}
	for _, accept := range []string{acceptJSON, acceptSSE} {
		t.Run(accept, func(t *testing.T) {
			for _, mode := range []domain.AnswerMode{domain.AnswerModeGenerated, domain.AnswerModeEvidence} {
				t.Run(string(mode), func(t *testing.T) {
					run := func(capture bool, failure string) observation {
						t.Helper()
						world := newGrantWorld()
						authority := &lineBoundaryAuthority{grantWorld: world, failure: failure}
						store := &grantStore{w: world, kind: domain.GrantScopeRelation}
						embed := &grantEmbedder{w: world}
						cfg := handlerBedrockStrictConfig()
						cfg.PreserveSourceLineBoundaries = capture
						cfg.ServiceTokens = []string{"fixture-key"}
						cfg.SynthesisContextPolicy = config.SynthesisContextPolicyDiverse
						cfg.SynthesisTimeout = 500 * time.Millisecond
						conv := &lineBoundaryConverser{handlerBudgetConverser: handlerBudgetConverser{region: cfg.AWSRegion}}
						amb := llm.NewAmbassador(cfg)
						amb.SetClient(conv)
						var order []string
						rerank := queryReranker(func(_ context.Context, _ string, hits []domain.ScoredChunk) ([]domain.ScoredChunk, error) {
							hits = slices.Clone(hits)
							slices.Reverse(hits)
							for _, hit := range hits {
								order = append(order, string(hit.Chunk.DocumentID))
							}
							return hits, nil
						})
						handler := QueryHandler(retriever.NewRetriever(store, embed, .7, 10), amb, cfg, nil,
							WithReranker(rerank), WithAuthorizedRetrieval(authority))
						rig := &grantRig{w: world, handler: handler}
						body, err := json.Marshal(QueryRequest{
							Query: "What do these archives say?", Mode: "global", Collection: "archive",
							AnswerMode: requestedAnswerMode(mode), IncludeCitations: mode == domain.AnswerModeGenerated,
						})
						require.NoError(t, err)
						rec := rig.serve(t, userA1, string(body), accept)
						require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
						wantBatch := []string{docVisible, docShared}
						if mode == domain.AnswerModeGenerated {
							wantBatch = []string{docShared, docVisible}
							require.Len(t, conv.requests, 1)
							require.Equal(t, wantBatch, order)
						} else {
							require.Empty(t, conv.requests)
							require.Empty(t, order)
						}
						if capture {
							require.Equal(t, [][]string{wantBatch}, authority.batches)
						} else {
							require.Empty(t, authority.batches)
						}
						got := observation{model: conv.requests, embedded: embed.queries, reranked: order}
						if accept == acceptJSON {
							var decoded QueryResponse
							require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &decoded))
							if mode == domain.AnswerModeGenerated {
								require.NotEmpty(t, decoded.Citations)
							}
							require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got.body))
							zeroLineBoundaryStageTimes(t, got.body)
						} else {
							got.events = recordedEvents(t, rec.Body.String())
							require.Equal(t, "done", got.events[len(got.events)-1].name)
							zeroLineBoundaryStageTimes(t, got.events[len(got.events)-1].data)
						}
						return got
					}
					disabled := run(false, "")
					for _, failure := range []string{"", "error", "timeout"} {
						require.Equal(t, disabled, run(true, failure), "optional lookup %q", failure)
					}
				})
			}
		})
	}
}

// Elapsed time varies with the lookup while the timing field and stage keys stay the same.
func zeroLineBoundaryStageTimes(t *testing.T, data map[string]json.RawMessage) {
	t.Helper()
	var timings map[string]int64
	require.NoError(t, json.Unmarshal(data["stage_timings_ms"], &timings))
	require.Contains(t, timings, "synthesis")
	for stage := range timings {
		timings[stage] = 0
	}
	encoded, err := json.Marshal(timings)
	require.NoError(t, err)
	data["stage_timings_ms"] = encoded
}
