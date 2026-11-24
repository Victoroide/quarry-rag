package handler

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/Victoroide/quarry-rag/internal/citationsupport"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/embedder"
	"github.com/Victoroide/quarry-rag/internal/llm"
	"github.com/Victoroide/quarry-rag/internal/pipeline"
	"github.com/Victoroide/quarry-rag/internal/retriever"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
	brtypes "github.com/aws/aws-sdk-go-v2/service/bedrockruntime/types"
	"github.com/stretchr/testify/require"
)

type handlerVerifier func(context.Context, citationsupport.Batch) ([]citationsupport.Status, error)

type delayedCitationVisibility struct {
	domain.EvidenceVisibility
	duringDelta context.Context
	deltaErr    error
}

func (v *delayedCitationVisibility) VisibleEvidenceWithPreviews(ctx context.Context, hits []domain.ScoredChunk, _ []string) ([]domain.ScoredChunk, map[string]string, error) {
	if v.duringDelta == nil {
		return hits, nil, ctx.Err()
	}
	<-v.duringDelta.Done()
	v.deltaErr = ctx.Err()
	return hits, nil, nil
}

type timedCitationGenerator struct {
	llm.Generator
	visibility *delayedCitationVisibility
}

func (g timedCitationGenerator) Stream(ctx context.Context, _ llm.Request, delta func(string) error) (*llm.Response, error) {
	g.visibility.duringDelta = ctx
	if err := delta("Friday"); err != nil {
		return nil, err
	}
	return &llm.Response{Text: "Friday", StopReason: llm.StopEndTurn}, nil
}

func TestCitationStreamRejectsDeltaAfterSynthesisScopeDeadline(t *testing.T) {
	visibility := new(delayedCitationVisibility)
	cfg := streamQueryConfig()
	cfg.SynthesisTimeout = 80 * time.Millisecond
	amb := llm.NewAmbassador(cfg)
	amb.SetGenerator(timedCitationGenerator{visibility: visibility})
	qc := &pipeline.QueryContext{
		OriginalQuery: "On what day?", IncludeCitations: true, RequireCitationScope: true,
		EvidenceVisibility: visibility,
		RetrievedChunks:    []domain.ScoredChunk{{Chunk: domain.Chunk{ID: "chunk", DocumentID: "doc", Text: "Friday"}, Score: 1}},
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	r := streamQueryRequest(ctx, `{}`)
	rec := httptest.NewRecorder()
	stream := newQueryStream(ctx, rec, r, qc)
	err := pipeline.NewSynthesisStage(amb, cfg).Run(ctx, qc)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	stream.finish(ctx, err)
	require.NoError(t, ctx.Err())
	events := recordedEvents(t, rec.Body.String())
	require.Len(t, events, 2)
	require.Equal(t, "context", events[0].name)
	require.Equal(t, "error", events[1].name)
	require.JSONEq(t, `"timeout"`, string(events[1].data["error"]))
	require.ErrorIs(t, visibility.deltaErr, context.DeadlineExceeded)
}

func (v handlerVerifier) Version() string { return "fixture-revision" }

func (v handlerVerifier) Verify(ctx context.Context, batch citationsupport.Batch) ([]citationsupport.Status, error) {
	return v(ctx, batch)
}

func TestQueryCitationSupportJSONAndSSE(t *testing.T) {
	const answer = "Archive [99] evidence [1]. Unmarked statement."
	for _, tc := range []struct {
		name              string
		include, evidence bool
		failure           bool
	}{
		{"supported", true, true, false},
		{"provider outage", true, true, true},
		{"unrequested", false, true, false},
		{"no evidence", true, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			verifier := handlerVerifier(func(_ context.Context, b citationsupport.Batch) ([]citationsupport.Status, error) {
				calls++
				require.Equal(t, []citationsupport.Pair{{Claim: "Archive evidence", Body: "Archive evidence"}}, b.Pairs)
				if tc.failure {
					return nil, errors.New("private provider failure")
				}
				return []citationsupport.Status{citationsupport.Supported}, nil
			})
			var chunks []domain.ScoredChunk
			if tc.evidence {
				chunks = []domain.ScoredChunk{{Chunk: domain.Chunk{ID: "chunk", DocumentID: "document", Text: "Archive evidence"}, Score: 1}}
			}
			cfg := streamQueryConfig()
			amb := llm.NewAmbassador(cfg)
			amb.SetClient(&citationSupportConverser{answer: answer})
			ret := retriever.NewRetriever(citationQueryStore{chunks: chunks}, embedder.NewFakeEmbedder(8), .7, 10)
			h := QueryHandler(ret, amb, cfg, nil, WithCitationSupport(verifier))
			body := `{"query":"archive","mode":"global"}`
			if tc.include {
				body = `{"query":"archive","mode":"global","include_citations":true}`
			}
			for _, streaming := range []bool{false, true} {
				r := streamQueryRequest(t.Context(), body)
				if !streaming {
					r.Header.Del("Accept")
				}
				rec := httptest.NewRecorder()
				h(rec, r)
				require.Equal(t, http.StatusOK, rec.Code)
				require.NotContains(t, rec.Body.String(), "private provider failure")
				var resp map[string]json.RawMessage
				if streaming {
					events := recordedEvents(t, rec.Body.String())
					require.Equal(t, []string{"context", "delta", "done"}, []string{events[0].name, events[1].name, events[2].name})
					if tc.evidence {
						require.JSONEq(t, `"`+answer+`"`, string(events[1].data["text"]))
						require.NotContains(t, events[1].data, "citation_coverage")
					}
					resp = events[2].data
				} else {
					require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
					if tc.include && tc.evidence {
						require.JSONEq(t, `"Archive evidence [1]. Unmarked statement."`, string(resp["answer"]))
					}
				}
				switch {
				case !tc.include:
					require.NotContains(t, resp, "citation_coverage")
					require.NotContains(t, resp, "citations")
				case !tc.evidence:
					require.NotContains(t, resp, "citation_coverage")
					require.JSONEq(t, `[]`, string(resp["citations"]))
				default:
					require.JSONEq(t, `{"sentences":2,"cited_sentences":1}`, string(resp["citation_coverage"]))
					var citations []CitationResponse
					require.NoError(t, json.Unmarshal(resp["citations"], &citations))
					require.Len(t, citations, 1)
					got := citations[0]
					require.True(t, got.Verified)
					wantOffset := 17
					if streaming {
						wantOffset = 22
					}
					require.Equal(t, wantOffset, got.Offset)
					require.Equal(t, []int{1}, got.Support.Evidence)
					want := citationsupport.Supported
					if tc.failure {
						want = citationsupport.Unchecked
					}
					require.Equal(t, want, got.Support.Status)
				}
			}
			if tc.include && tc.evidence {
				require.Equal(t, 2, calls)
			} else {
				require.Zero(t, calls)
			}
		})
	}
}

func TestCitationSupportPreservesPatternWhitespace(t *testing.T) {
	const prefix = "The archive pattern is "
	const reference = prefix + "`^A B$`"
	for _, tc := range []struct {
		name, pattern string
		status        citationsupport.Status
	}{
		{"identical", "^A B$", citationsupport.Supported},
		{"extra space", "^A  B$", citationsupport.Contradicted},
		{"different character", "^AX B$", citationsupport.Contradicted},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.status == citationsupport.Supported, regexp.MustCompile(tc.pattern).MatchString("A B"))
			claim := prefix + "`" + tc.pattern + "`"
			checkCitationPattern(t, claim, claim, reference, tc.status)
		})
	}
}

func TestCitationSupportPreservesQuotedPatterns(t *testing.T) {
	const prefix = "The archive pattern is "
	for _, quote := range []struct{ name, open, close string }{
		{"straight", `"`, `"`},
		{"typographic", "“", "”"},
	} {
		for _, tc := range []struct {
			name, pattern, jsonPattern string
			status                     citationsupport.Status
		}{
			{"identical", "^A B$", "^A B$", citationsupport.Supported},
			{"extra space", "^A  B$", "^A  B$", citationsupport.Contradicted},
			{"bracketed number", "^A[1]B$", "^A[1]B$", citationsupport.Unchecked},
			{"different character", "^AX B$", "^AX B$", citationsupport.Contradicted},
			{"rewritten bracketed number", "^A[01]B$", "^A[1]B$", citationsupport.Unchecked},
		} {
			t.Run(quote.name+"/"+tc.name, func(t *testing.T) {
				require.Equal(t, tc.status == citationsupport.Supported, regexp.MustCompile(tc.pattern).MatchString("A B"))
				claim := prefix + quote.open + tc.pattern + quote.close
				jsonClaim := prefix + quote.open + tc.jsonPattern + quote.close
				reference := prefix + quote.open + "^A B$" + quote.close
				checkCitationPattern(t, claim, jsonClaim, reference, tc.status)
			})
		}
	}
}

func TestCitationSupportPreservesEscapedQuoteInJSONString(t *testing.T) {
	const prefix = "The archive tag is the JSON string "
	const reference = `"A\" X \"B"`
	var want string
	require.NoError(t, json.Unmarshal([]byte(reference), &want))
	for _, tc := range []struct {
		name, literal string
		status        citationsupport.Status
	}{
		{"identical", `"A\" X \"B"`, citationsupport.Supported},
		{"extra space", `"A\"  X \"B"`, citationsupport.Contradicted},
		{"bracketed number", `"A\"[1]X \"B"`, citationsupport.Unchecked},
		{"different character", `"A\" Z \"B"`, citationsupport.Contradicted},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var displayed string
			require.NoError(t, json.Unmarshal([]byte(tc.literal), &displayed))
			require.Equal(t, tc.status == citationsupport.Supported, displayed == want)
			checkCitationPattern(t, prefix+tc.literal, prefix+tc.literal, prefix+reference, tc.status)
		})
	}
}

func TestCitationSupportAbstainsOnEscapedSingleQuote(t *testing.T) {
	const prefix = "The archive tag is the JavaScript string "
	const reference = `'A\' X'`
	for _, tc := range []struct{ name, literal string }{
		{"identical", `'A\' X'`},
		{"extra space", `'A\'  X'`},
		{"bracketed number", `'A\'[1]X'`},
		{"different character", `'A\' Z'`},
		{"curly quote", `‘A\’  X’`},
		{"guillemet", `«A\»  X»`},
		{"nested single quote", `'say "x\'  y" now'`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			checkCitationPattern(t, prefix+tc.literal, prefix+tc.literal, prefix+reference, citationsupport.Unchecked)
		})
	}
}

func checkCitationPattern(t *testing.T, claim, jsonClaim, reference string, status citationsupport.Status) {
	t.Helper()
	const trailingCitation = " [1]."
	answer := claim + trailingCitation
	for _, mode := range []string{"JSON", "SSE"} {
		t.Run(mode, func(t *testing.T) {
			var pairs []citationsupport.Pair
			verifier := handlerVerifier(func(_ context.Context, batch citationsupport.Batch) ([]citationsupport.Status, error) {
				require.Len(t, batch.Pairs, 1)
				require.Equal(t, [][]int{{0}}, batch.Claims)
				pair := batch.Pairs[0]
				pairs = append(pairs, pair)
				if pair.Claim == pair.Body {
					return []citationsupport.Status{citationsupport.Supported}, nil
				}
				return []citationsupport.Status{citationsupport.Contradicted}, nil
			})
			chunks := []domain.ScoredChunk{{Chunk: domain.Chunk{ID: "chunk", DocumentID: "doc", Text: reference}, Score: 1}}
			cfg := streamQueryConfig()
			amb := llm.NewAmbassador(cfg)
			amb.SetClient(&citationSupportConverser{answer: answer})
			ret := retriever.NewRetriever(citationQueryStore{chunks: chunks}, embedder.NewFakeEmbedder(8), .7, 10)
			h := QueryHandler(ret, amb, cfg, nil, WithCitationSupport(verifier))
			r := streamQueryRequest(t.Context(), `{"query":"archive","mode":"global","include_citations":true}`)
			if mode == "JSON" {
				r.Header.Del("Accept")
			}
			rec := httptest.NewRecorder()
			h(rec, r)
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

			var fields map[string]json.RawMessage
			var displayed json.RawMessage
			if mode == "SSE" {
				events := recordedEvents(t, rec.Body.String())
				require.Len(t, events, 3)
				require.Equal(t, "delta", events[1].name)
				require.Equal(t, "done", events[2].name)
				displayed, fields = events[1].data["text"], events[2].data
			} else {
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &fields))
				displayed = fields["answer"]
			}
			var text string
			require.NoError(t, json.Unmarshal(displayed, &text))
			wantAnswer := answer
			if mode == "JSON" {
				wantAnswer = jsonClaim + trailingCitation
			}
			require.Equal(t, wantAnswer, text)
			require.JSONEq(t, `{"sentences":1,"cited_sentences":1}`, string(fields["citation_coverage"]))
			var citations []CitationResponse
			require.NoError(t, json.Unmarshal(fields["citations"], &citations))
			require.NotEmpty(t, citations)
			citation := citations[len(citations)-1]
			require.Equal(t, 1, citation.Excerpt)
			require.Equal(t, "doc", citation.DocID)
			require.Equal(t, utf8.RuneCountInString(wantAnswer[:strings.LastIndex(wantAnswer, "[1]")]), citation.Offset)
			require.True(t, citation.Verified)
			require.NotNil(t, citation.Support)
			require.Equal(t, []int{1}, citation.Support.Evidence)
			require.Equal(t, status, citation.Support.Status)
			if status == citationsupport.Unchecked {
				require.Empty(t, pairs)
			} else {
				require.Len(t, citations, 1)
				require.Equal(t, []citationsupport.Pair{{Claim: claim, Body: reference}}, pairs)
			}
		})
	}
}

type citationSupportConverser struct {
	llm.FakeConverser
	answer string
}

func (c *citationSupportConverser) ConverseWithModelAndSystem(context.Context, string, string, []brtypes.Message, float32, int) (*bedrockruntime.ConverseOutput, error) {
	return deadlineOutput(c.answer, brtypes.StopReasonEndTurn), nil
}

func (c *citationSupportConverser) ConverseStreamWithModelAndSystem(_ context.Context, _, _ string, _ []brtypes.Message, _ float32, _ int, delta func(string) error) (*bedrockruntime.ConverseOutput, error) {
	if err := delta(c.answer); err != nil {
		return nil, err
	}
	return deadlineOutput(c.answer, brtypes.StopReasonEndTurn), nil
}

func TestCitationStreamDisconnectDuringAssessment(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	chunks := []domain.ScoredChunk{{Chunk: domain.Chunk{ID: "chunk", DocumentID: "document", Text: "Archive evidence"}, Score: 1}}
	cfg := streamQueryConfig()
	amb := llm.NewAmbassador(cfg)
	amb.SetClient(&citationSupportConverser{answer: "Archive evidence [1]."})
	verifier := handlerVerifier(func(provider context.Context, _ citationsupport.Batch) ([]citationsupport.Status, error) {
		cancel()
		<-provider.Done()
		return []citationsupport.Status{citationsupport.Supported}, nil
	})
	h := QueryHandler(retriever.NewRetriever(citationQueryStore{chunks: chunks}, embedder.NewFakeEmbedder(8), .7, 10), amb, cfg, nil, WithCitationSupport(verifier))
	rec := httptest.NewRecorder()
	h(rec, streamQueryRequest(ctx, `{"query":"archive","mode":"global","include_citations":true}`))
	events := recordedEvents(t, rec.Body.String())
	require.Len(t, events, 2)
	require.Equal(t, "context", events[0].name)
	require.Equal(t, "delta", events[1].name)
}
