package pipeline

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/citationsupport"
	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/llm"
	"github.com/stretchr/testify/require"
)

type claimVerifier func(context.Context, citationsupport.Batch) ([]citationsupport.Status, error)

func (v claimVerifier) Version() string { return "pinned-test-model" }

func (v claimVerifier) Verify(ctx context.Context, batch citationsupport.Batch) ([]citationsupport.Status, error) {
	return v(ctx, batch)
}

func citationEvidence(bodies ...string) []domain.ScoredChunk {
	chunks := make([]domain.ScoredChunk, len(bodies))
	for i, body := range bodies {
		chunks[i] = domain.ScoredChunk{Chunk: domain.Chunk{ID: domain.ChunkID(fmt.Sprint(i)), DocumentID: "document", Index: i, Text: body}, Score: 1}
	}
	return chunks
}

func TestAssessCitationsGroupsFullClaimsAndPreservesOffsets(t *testing.T) {
	answer := "記録は3.5年 [99][1, 1][2]と別条件です。記録は3.5年 [1][2]と別条件です。別の事実。最後 [999]。 [1]"
	chunks := citationEvidence("Records last 3.5 years.", "The second condition applies.")
	for _, streaming := range []bool{false, true} {
		qc := &QueryContext{Excerpts: chunks, FinalAnswer: answer, IncludeCitations: true}
		if streaming {
			qc.Stream = &QueryStream{}
		}
		verifier := claimVerifier(func(_ context.Context, batch citationsupport.Batch) ([]citationsupport.Status, error) {
			require.Equal(t, citationsupport.Batch{
				Pairs:  []citationsupport.Pair{{Claim: "記録は3.5年 と別条件です", Body: chunks[0].Chunk.Text}, {Claim: "記録は3.5年 と別条件です", Body: chunks[1].Chunk.Text}},
				Claims: [][]int{{0, 1}},
			}, batch)
			return []citationsupport.Status{citationsupport.Supported}, nil
		})
		stage := NewSynthesisStage(nil, &config.Config{}).WithCitationSupport(verifier)
		require.NoError(t, stage.finalizeCitations(t.Context(), qc))
		expectedAnswer, expected := VerifyCitations(answer, chunks)
		if streaming {
			expectedAnswer, expected = answer, VerifyStreamCitations(answer, chunks)
		}
		require.Equal(t, expectedAnswer, qc.FinalAnswer)
		require.Equal(t, &citationsupport.Coverage{Sentences: 4, CitedSentences: 2}, qc.CitationCoverage)
		for i, actual := range qc.Citations {
			require.NotNil(t, actual.Support)
			if i < 4 {
				require.Equal(t, citationsupport.Supported, actual.Support.Status)
				require.Equal(t, []int{1, 2}, actual.Support.Evidence)
			} else {
				require.Equal(t, citationsupport.Unchecked, actual.Support.Status)
			}
			actual.Support = nil
			require.Equal(t, expected[i], actual)
		}
	}
}

func TestAssessCitationsPreservesClaimBoundariesAcrossResponses(t *testing.T) {
	for _, tc := range []struct {
		name, answer, claim string
		sentences, marked   int
	}{
		{"dotted abbreviation", "Trains do not stop in the U.S. on Fridays [1].", "Trains do not stop in the U.S. on Fridays", 1, 1},
		{"marker before abbreviation", "Trains do not stop [1] in the U.S. on Fridays.", "Trains do not stop in the U.S. on Fridays", 1, 1},
		{"initials and title", "Dr. J. R. Rao did not stop [1].", "Dr. J. R. Rao did not stop", 1, 1},
		{"danda", "पहली बात गलत है। रेल शुक्रवार को चलती है [1]।", "रेल शुक्रवार को चलती है", 2, 1},
		{"ordered list", "1. Trains do not stop [1].\n۲) Trains pass.", "Trains do not stop", 2, 1},
		{"unmatched bracket", "First line [unfinished\nTrains stop [1].", "Trains stop", 2, 1},
		{"unlisted title abbreviation", "Trains do not stop at Mt. Everest [1].", "", 1, 1},
		{"unlisted month abbreviation", "Trains do not stop on Jan. 5 [1].", "", 1, 1},
		{"short title before surname", "Trains do not stop at Gen. Smith station [1].", "", 1, 1},
		{"longer month abbreviation", "The museum does not open on Sept. 10 [1].", "", 1, 1},
		{"longer title before name", "The route does not end at Capt. James Square [1].", "", 1, 1},
		{"soft line wrap", "The library is not\nopen on Sundays [1].", "", 1, 1},
		{"month before a CRLF wrap", "The museum does not open on Sept.\r\n10 [1].", "", 1, 1},
		{"paragraph break", "Trains run daily\r\n \r\nDoors open Sunday [1].", "Doors open Sunday", 2, 1},
		{"ordered list after a heading", "Hours\n  1. Doors open Sunday [1].", "Doors open Sunday", 2, 1},
		{"unordered list after a heading", "Hours\n  - Doors open Sunday [1].", "Doors open Sunday", 2, 1},
		{"separate sentences", "Trains run daily. Doors open Sunday [1].", "Doors open Sunday", 2, 1},
		{"multiletter dotted abbreviation", "Trains do not stop for Ph.D. Smith [1].", "", 1, 1},
		{"Spanish country abbreviation", "Los trenes no paran en EE. UU. los viernes [1].", "", 1, 1},
		{"ellipsis", "Trains do not stop... on Fridays [1].", "", 1, 1},
		{"sentence after a long word", "Trains run daily. Dogs do not bark [1].", "Dogs do not bark", 2, 1},
		{"sentence after an uncased short word", "열차는 정차하지 않는다. 금요일에는 정차한다 [1].", "금요일에는 정차한다", 2, 1},
		{"ambiguous period", "Trains do not stop at the terminus. on Fridays [1].", "", 1, 1},
		{"ambiguous abbreviation", "Trains do not stop before approx. Noon [1].", "", 1, 1},
		{"unclosed clause", "Trains do not stop [1] (except", "", 1, 1},
		{"quoted exclamation", "The museum never advertised “Free entry!” and charged a fee [1].", "The museum never advertised “Free entry!” and charged a fee", 1, 1},
		{"quoted exclamation with a middle marker", "The museum never advertised “Free entry!” [1] and charged a fee.", "The museum never advertised “Free entry!” and charged a fee", 1, 1},
		{"quote ends the cited sentence", "The museum never advertised “Free entry!” [1]", "The museum never advertised “Free entry!”", 1, 1},
		{"quoted exclamation in guillemets", "Le musée n’a jamais annoncé « Entrée libre ! » et facture [1].", "Le musée n’a jamais annoncé « Entrée libre ! » et facture", 1, 1},
		{"quoted exclamation in low quotes", "Das Museum warb nie mit „Freier Eintritt!“ und verlangt Gebühren [1].", "Das Museum warb nie mit „Freier Eintritt!“ und verlangt Gebühren", 1, 1},
		{"quoted exclamation in reversed guillemets", "Museet annoncerede aldrig »Gratis adgang!« og tager betaling [1].", "Museet annoncerede aldrig »Gratis adgang!« og tager betaling", 1, 1},
		{"quoted exclamation in single quotes", "The museum never advertised ‘Free entry!’ and charged a fee [1].", "The museum never advertised ‘Free entry!’ and charged a fee", 1, 1},
		{"quoted exclamation in straight single quotes", "The museum never advertised 'Free entry!' and charged a fee [1].", "The museum never advertised 'Free entry!' and charged a fee", 1, 1},
		{"quoted exclamation in straight quotes", "The museum never advertised \"Free entry!\" and charged a fee [1].", "The museum never advertised \"Free entry!\" and charged a fee", 1, 1},
		{"quoted exclamation in corner brackets", "博物館は「入場無料！」[1]。", "博物館は「入場無料！」", 1, 1},
		{"nested quotation", "The museum never advertised “Free ‘entry!’ here” and charged a fee [1].", "The museum never advertised “Free ‘entry!’ here” and charged a fee", 1, 1},
		{"quoted whitespace and prose separators", "The  sign\tsays “ A\t\u00a0B ”[1]and lists hours.", "The sign says “ A\t\u00a0B ” and lists hours", 1, 1},
		{"line break inside a quotation in a parenthetical", "The sign says (for example “A\r\n  B”) at the entrance [1].", "The sign says (for example “A\r\n  B”) at the entrance", 1, 1},
		{"apostrophe inside a single quotation", "The sign said ‘Don’t enter!’ and nothing else [1].", "The sign said ‘Don’t enter!’ and nothing else", 1, 1},
		{"apostrophe inside a straight single quotation", "The sign said 'Don't enter!' and nothing else [1].", "The sign said 'Don't enter!' and nothing else", 1, 1},
		{"quote followed by a capital", "The museum advertised “free entry.” Doors open Sunday [1].", "", 1, 1},
		{"adjacent quotations", "The sign said “Free entry!” “Doors open!” [1].", "", 1, 1},
		{"quote followed by an uncased letter", "博物馆从未宣传“免费入场！”并收取费用 [1]。", "", 1, 1},
		{"soft wrap after a quotation", "The sign said “free entry”\nand nothing else [1].", "", 1, 1},
		{"unclosed quotation", "The museum never advertised “Free entry! and charged a fee [1].", "", 1, 1},
		{"quote closes before a line break", "The sign said “Free entry!”\nDoors open Sunday [1].", "Doors open Sunday", 2, 1},
		{"soft wrap after quoted punctuation", "The museum did not offer “Free entry!”\nor waive admission fees [1].", "The museum did not offer “Free entry!” or waive admission fees", 1, 1},
		{"soft wrap after quoted punctuation and a marker", "The museum did not offer “Free entry!” [1]\r\n  or waive admission fees.", "The museum did not offer “Free entry!” or waive admission fees", 1, 1},
		{"paragraph after quoted punctuation", "The sign said “Free entry!”\n \nvisitors paid [1].", "visitors paid", 2, 1},
		{"list after quoted punctuation", "The sign said “Free entry!”\n - visitors paid [1].", "visitors paid", 2, 1},
		{"terminal after the quotation", "The sign said “free entry”. Doors open Sunday [1].", "Doors open Sunday", 2, 1},
		{"possessive apostrophes", "The museum’s fee isn’t free. Students’ doors open Sunday [1].", "Students’ doors open Sunday", 2, 1},
		{"straight apostrophe", "The museum doesn't charge [1].", "The museum doesn't charge", 1, 1},
		{"straight possessive apostrophe", "The students' visit is free [1].", "The students' visit is free", 1, 1},
		{"leading dot in a name", "The library never used .NET to generate reports [1].", "The library never used .NET to generate reports", 1, 1},
		{"leading dot in a filename", "The service never loaded .env.local at startup [1].", "The service never loaded .env.local at startup", 1, 1},
		{"soft wrap in a parenthetical", "The museum does not (even on\nSundays) admit visitors for free [1].", "The museum does not (even on Sundays) admit visitors for free", 1, 1},
		{"unclosed parenthetical across a soft wrap", "The museum does not (even on\nSundays admit visitors for free [1].", "", 1, 1},
		{"inline code filename", "The service never reads `.env` at startup [1].", "The service never reads `.env` at startup", 1, 1},
		{"padded inline code filename", "The service never reads ` .env` at startup [1].", "The service never reads ` .env` at startup", 1, 1},
		{"longer inline code delimiter", "The service never prints `` `ready!` `` at startup [1].", "The service never prints `` `ready!` `` at startup", 1, 1},
		{"code whitespace and prose separators", "The  service\tcompares `A\t\u00a0B`[1]with `` `A  B` ``  at startup.", "The service compares `A\t\u00a0B` with `` `A  B` `` at startup", 1, 1},
		{"line break inside code in a parenthetical", "The service prints (for example `A\r\n  B`) at startup [1].", "The service prints (for example `A\r\n  B`) at startup", 1, 1},
		{"sentence ends after inline code", "The service takes `ready!`. Logs show failures [1].", "Logs show failures", 2, 1},
		{"escaped backtick before inline code", "The service never prints \\` or reads `.env` at startup [1].", "The service never prints \\` or reads `.env` at startup", 1, 1},
		{"escaped quote inside a JSON string", "The tag is the JSON string \"A\\\" X \\\"B\" [1].", "The tag is the JSON string \"A\\\" X \\\"B\"", 1, 1},
		{"escaped quote keeps significant spaces", "The tag is the JSON string \"A\\\"  X \\\"B\" [1].", "The tag is the JSON string \"A\\\"  X \\\"B\"", 1, 1},
		{"citation-shaped text after an escaped quote", "The tag is the JSON string \"A\\\"[1]X \\\"B\" [1].", "", 1, 1},
		{"escaped single quote keeps significant spaces", "The tag is the JavaScript string 'A\\'  X' [1].", "", 1, 1},
		{"citation-shaped text after an escaped single quote", "The tag is the JavaScript string 'A\\'[1]X' [1].", "", 1, 1},
		{"escaped single quote", "The tag is the JavaScript string 'A\\' X' [1].", "", 1, 1},
		{"escaped curly quote keeps significant spaces", "The tag is ‘A\\’  X’ [1].", "", 1, 1},
		{"escaped guillemet", "The tag is «A\\»  X» [1].", "", 1, 1},
		{"escaped double quote in prose", "The tag is \\\"A  X\\\" [1].", "", 1, 1},
		{"escaped single quote inside a double quotation", "The tag is 'say \"x\\'  y\" now' [1].", "", 1, 1},
		{"paired backslashes before a single quote", "The tag is 'A\\\\' then  X [1].", "The tag is 'A\\\\' then X", 1, 1},
		{"paired backslashes before the closing quote", "The tag is \"A\\\\\" then X [1].", "The tag is \"A\\\\\" then X", 1, 1},
		{"leading decimal after a currency symbol", "The fee was $.50 per visit [1].", "The fee was $.50 per visit", 1, 1},
		{"leading decimal after a sign", "The offset was -.25 degrees [1].", "The offset was -.25 degrees", 1, 1},
		{"leading decimal after a space", "The fee was .50 per visit [1].", "The fee was .50 per visit", 1, 1},
		{"leading decimal in parentheses", "The fee (.50 per visit) was fixed [1].", "The fee (.50 per visit) was fixed", 1, 1},
		{"leading decimal at the start", ".5 liter bottles cost less [1].", ".5 liter bottles cost less", 1, 1},
		{"leading decimal across a marker", "The fee was $.50 [1] per visit.", "The fee was $.50 per visit", 1, 1},
		{"decimal with a leading digit", "The fee was $0.50 per visit [1].", "The fee was $0.50 per visit", 1, 1},
		{"sentence ends before a number", "Trains run daily.5 stations closed [1].", "5 stations closed", 2, 1},
		{"sentence ends after a number", "The fee was 5. 50 visitors came [1].", "50 visitors came", 2, 1},
	} {
		for _, response := range []struct {
			name      string
			streaming bool
		}{{"JSON", false}, {"SSE", true}} {
			t.Run(tc.name+"/"+response.name, func(t *testing.T) {
				chunks := citationEvidence("Scheduled stops")
				qc := &QueryContext{Excerpts: chunks, FinalAnswer: tc.answer, IncludeCitations: true}
				wantAnswer, lexical := VerifyCitations(tc.answer, chunks)
				if response.streaming {
					qc.Stream = &QueryStream{}
					wantAnswer, lexical = tc.answer, VerifyStreamCitations(tc.answer, chunks)
				}
				var gotClaims []string
				v := claimVerifier(func(_ context.Context, batch citationsupport.Batch) ([]citationsupport.Status, error) {
					for _, pair := range batch.Pairs {
						gotClaims = append(gotClaims, pair.Claim)
					}
					if slices.Equal(gotClaims, []string{tc.claim}) {
						return []citationsupport.Status{citationsupport.Contradicted}, nil
					}
					return []citationsupport.Status{citationsupport.Supported}, nil
				})
				stage := NewSynthesisStage(nil, &config.Config{}).WithCitationSupport(v)
				require.NoError(t, stage.finalizeCitations(t.Context(), qc))
				require.Equal(t, wantAnswer, qc.FinalAnswer)
				require.Equal(t, &citationsupport.Coverage{Sentences: tc.sentences, CitedSentences: tc.marked}, qc.CitationCoverage)
				require.Len(t, qc.Citations, len(lexical))
				wantStatus := citationsupport.Unchecked
				if tc.claim == "" {
					require.Empty(t, gotClaims)
				} else {
					require.Equal(t, []string{tc.claim}, gotClaims)
					wantStatus = citationsupport.Contradicted
				}
				for i, c := range qc.Citations {
					require.Equal(t, wantStatus, c.Support.Status)
					c.Support = nil
					require.Equal(t, lexical[i], c)
				}
			})
		}
	}
}

func TestAssessCitationsAbstainsOnCleanedLiterals(t *testing.T) {
	for _, tc := range []struct {
		name, literal, cleaned string
	}{
		{"zero index", "stations[0]", "stations"},
		{"normalized index", "stations[01]", "stations[1]"},
		{"retained index", "stations[1]", "stations[1]"},
		{"no brackets", "stations.First()", "stations.First()"},
	} {
		for _, response := range []struct {
			name      string
			streaming bool
		}{{"JSON", false}, {"SSE", true}} {
			t.Run(tc.name+"/"+response.name, func(t *testing.T) {
				claim := "The first station is `" + tc.literal + "`"
				answer := claim + " [1]."
				chunks := citationEvidence(claim + ".")
				qc := &QueryContext{Excerpts: chunks, FinalAnswer: answer, IncludeCitations: true}
				wantAnswer := "The first station is `" + tc.cleaned + "` [1]."
				_, lexical := VerifyCitations(answer, chunks)
				if response.streaming {
					qc.Stream = &QueryStream{}
					wantAnswer = answer
					lexical = VerifyStreamCitations(answer, chunks)
				}
				var submitted []citationsupport.Batch
				verifier := claimVerifier(func(_ context.Context, batch citationsupport.Batch) ([]citationsupport.Status, error) {
					submitted = append(submitted, batch)
					status := citationsupport.Insufficient
					if len(batch.Pairs) == 1 && batch.Pairs[0].Claim+"." == batch.Pairs[0].Body {
						status = citationsupport.Supported
					}
					return []citationsupport.Status{status}, nil
				})
				stage := NewSynthesisStage(nil, &config.Config{}).WithCitationSupport(verifier)
				require.NoError(t, stage.finalizeCitations(t.Context(), qc))
				require.Equal(t, wantAnswer, qc.FinalAnswer)
				require.Equal(t, &citationsupport.Coverage{Sentences: 1, CitedSentences: 1}, qc.CitationCoverage)
				require.Len(t, qc.Citations, len(lexical))
				for i, citation := range qc.Citations {
					citation.Support = nil
					require.Equal(t, lexical[i], citation)
				}
				trailing := qc.Citations[len(qc.Citations)-1]
				require.Equal(t, len(wantAnswer)-len("[1]."), trailing.Offset)
				require.Equal(t, []int{1}, trailing.Support.Evidence)
				if !response.streaming && tc.literal != tc.cleaned {
					require.Equal(t, citationsupport.Unchecked, trailing.Support.Status)
					require.Empty(t, submitted)
					return
				}
				require.Equal(t, citationsupport.Supported, trailing.Support.Status)
				require.Equal(t, []citationsupport.Batch{{
					Pairs:  []citationsupport.Pair{{Claim: claim, Body: claim + "."}},
					Claims: [][]int{{0}},
				}}, submitted)
			})
		}
	}
}

func TestAssessCitationsRejectsMalformedAndEmptyClaims(t *testing.T) {
	for _, answer := range []string{"Archive [[99]1].", "Archive [[1]].", "Archive [].", "Archive [one].", "[1].", "Archive."} {
		for _, streaming := range []bool{false, true} {
			qc := &QueryContext{Excerpts: citationEvidence("Archive"), FinalAnswer: answer, IncludeCitations: true}
			if streaming {
				qc.Stream = &QueryStream{}
			}
			stage := NewSynthesisStage(nil, &config.Config{}).WithCitationSupport(claimVerifier(func(context.Context, citationsupport.Batch) ([]citationsupport.Status, error) {
				t.Fatal("verifier received a marker without a valid claim")
				return nil, nil
			}))
			require.NoError(t, stage.finalizeCitations(t.Context(), qc))
			require.Zero(t, qc.CitationCoverage.CitedSentences)
			for _, citation := range qc.Citations {
				require.Equal(t, citationsupport.Unchecked, citation.Support.Status)
			}
		}
	}
}

func TestAssessCitationsBoundsPairsBytesAndWholeSets(t *testing.T) {
	for _, tc := range []struct {
		name     string
		chunks   []domain.ScoredChunk
		answer   string
		maxPairs int
		want     int
	}{
		{"default pair cap", citationEvidence("body"), "One [1]. Two [1]. Three [1]. Four [1]. Five [1]. Six [1]. Seven [1]. Eight [1]. Nine [1].", 0, 8},
		{"duplicate bodies", citationEvidence("same", "same"), "Same [1][2].", 1, 1},
		{"whole set", citationEvidence("first", "second"), "Joint [1][2]. Only second [2].", 1, 1},
		{"json escaped bytes", citationEvidence(strings.Repeat("\\", citationsupport.MaxBatchBytes/2), "second"), "Large [1]. Small [2].", 8, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			qc := &QueryContext{Excerpts: tc.chunks, FinalAnswer: tc.answer, IncludeCitations: true}
			stage := NewSynthesisStage(nil, &config.Config{CitationSupportMaxPairs: tc.maxPairs}).WithCitationSupport(claimVerifier(func(_ context.Context, batch citationsupport.Batch) ([]citationsupport.Status, error) {
				require.Len(t, batch.Pairs, tc.want)
				for _, pair := range batch.Pairs {
					require.Less(t, len(pair.Body), citationsupport.MaxBatchBytes/2)
				}
				results := make([]citationsupport.Status, len(batch.Claims))
				for i := range results {
					results[i] = citationsupport.Insufficient
				}
				return results, nil
			}))
			require.NoError(t, stage.finalizeCitations(t.Context(), qc))
			switch tc.name {
			case "default pair cap":
				require.Equal(t, citationsupport.Unchecked, qc.Citations[8].Support.Status)
			case "duplicate bodies":
				require.Equal(t, []int{1, 2}, qc.Citations[0].Support.Evidence)
			default:
				require.Equal(t, citationsupport.Unchecked, qc.Citations[0].Support.Status)
				require.Equal(t, citationsupport.Insufficient, qc.Citations[len(qc.Citations)-1].Support.Status)
			}
		})
	}
}

func TestCitationSupportUsesOnlySelectedSynthesisBodies(t *testing.T) {
	cfg := &config.Config{BedrockFallbackModel: "fallback", SynthesisContextTokens: 40, SynthesisRelativeScoreFloor: .55}
	generator := &citationPromptRecorder{answer: "Generated claim [1][2]."}
	amb := llm.NewAmbassador(cfg)
	amb.SetClient(generator)
	qc := &QueryContext{OriginalQuery: "private question", IncludeCitations: true, RetrievedChunks: citationEvidence(
		"Archi\u200bves keep "+strings.Repeat("evidence ", 100)+"discarded tail", "discarded other body")}
	calls := 0
	stage := NewSynthesisStage(amb, cfg).WithCitationSupport(claimVerifier(func(_ context.Context, batch citationsupport.Batch) ([]citationsupport.Status, error) {
		calls++
		require.Len(t, batch.Pairs, 1)
		pair := batch.Pairs[0]
		require.Contains(t, generator.prompt, "id=\"1\">\n"+pair.Body+"\n")
		require.NotContains(t, pair.Body, "discarded")
		require.NotContains(t, pair.Body, "\u200b")
		require.Equal(t, "Generated claim", pair.Claim)
		return []citationsupport.Status{citationsupport.Insufficient}, nil
	}))
	require.NoError(t, stage.Run(t.Context(), qc))
	require.Equal(t, 1, calls)
	require.Equal(t, []bool{true}, qc.Truncated)
	require.Equal(t, citationsupport.Insufficient, qc.Citations[0].Support.Status)
	qc.IncludeCitations = false
	require.NoError(t, stage.Run(t.Context(), qc))
	require.Nil(t, qc.CitationCoverage)
	qc.IncludeCitations, qc.RetrievedChunks = true, nil
	require.NoError(t, stage.Run(t.Context(), qc))
	require.Nil(t, qc.CitationCoverage)
	require.Empty(t, qc.Citations)
	require.Equal(t, 1, calls)
}

func TestCitationSupportOutageTimeoutAndQueryCancellation(t *testing.T) {
	for _, tc := range []struct {
		name        string
		cancelQuery bool
		timeout     time.Duration
	}{
		{"outage", false, time.Second},
		{"verifier timeout", false, 10 * time.Millisecond},
		{"query cancellation", true, time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			qc := &QueryContext{Excerpts: citationEvidence("Archive"), FinalAnswer: "Archive [1].", IncludeCitations: true}
			stage := NewSynthesisStage(nil, &config.Config{CitationSupportTimeout: tc.timeout}).WithCitationSupport(claimVerifier(func(provider context.Context, _ citationsupport.Batch) ([]citationsupport.Status, error) {
				switch tc.name {
				case "outage":
					return nil, errors.New("unavailable")
				case "query cancellation":
					cancel()
				}
				<-provider.Done()
				return []citationsupport.Status{citationsupport.Supported}, nil
			}))
			err := stage.finalizeCitations(ctx, qc)
			if tc.cancelQuery {
				require.ErrorIs(t, err, context.Canceled)
			} else {
				require.NoError(t, err)
			}
			require.True(t, qc.Citations[0].Verified)
			require.Equal(t, citationsupport.Unchecked, qc.Citations[0].Support.Status)
		})
	}
}

func TestCitationSupportSharesTotalQueryDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	deadline, _ := ctx.Deadline()
	qc := &QueryContext{Excerpts: citationEvidence("Archive"), FinalAnswer: "Archive [1].", IncludeCitations: true}
	stage := NewSynthesisStage(nil, &config.Config{}).WithCitationSupport(claimVerifier(func(provider context.Context, _ citationsupport.Batch) ([]citationsupport.Status, error) {
		got, ok := provider.Deadline()
		require.True(t, ok)
		require.Equal(t, deadline, got)
		return []citationsupport.Status{citationsupport.Insufficient}, nil
	}))
	require.NoError(t, stage.finalizeCitations(ctx, qc))
	require.Equal(t, citationsupport.Insufficient, qc.Citations[0].Support.Status)
}

func FuzzCitationClaims(f *testing.F) {
	for _, seed := range []string{"記録は3.5年 [99][1, 1][2]です。", "Archive [[99]1].", "Text [1,\n2].", "Text [0, 1, 99].", "Archive [[1]].", "\xff [1].", "Never advertised “Free entry!” and charged [1].", "The fee was $.50 per visit [1].", "First line [unfinished\nTrains stop [1]."} {
		f.Add(seed)
	}
	f.Add("The first station is `stations[0]` [1].")
	f.Fuzz(func(t *testing.T, answer string) {
		chunks := citationEvidence("first", "second")
		cleaned, cleanup := cleanCitationMarkersForAssessment(answer, len(chunks))
		jsonCitations, streamCitations := verifyCitationMarkers(cleaned, chunks, nil), VerifyStreamCitations(answer, chunks)
		jsonClaims, streamClaims := citationClaims(answer, jsonCitations, cleanup), citationClaims(answer, streamCitations, nil)
		require.Len(t, jsonClaims, len(streamClaims))
		excerpts := func(claim citedSentence, citations []Citation) []int {
			var refs []int
			for _, i := range claim.citations {
				refs = append(refs, citations[i].Excerpt)
			}
			slices.Sort(refs)
			return slices.Compact(refs)
		}
		for i, claim := range jsonClaims {
			require.NotEmpty(t, claim.text)
			require.Equal(t, streamClaims[i].text, claim.text)
			if len(cleanup.editedMarkers) == 0 || streamClaims[i].ambiguous {
				require.Equal(t, streamClaims[i].ambiguous, claim.ambiguous)
			}
			require.Equal(t, excerpts(streamClaims[i], streamCitations), excerpts(claim, jsonCitations))
			for _, index := range claim.citations {
				require.Contains(t, cleanup.origins, jsonCitations[index].markerByte)
			}
		}
	})
}

func TestCitationSupportPreservingCodeAssessesDisplayedLiteral(t *testing.T) {
	for _, tc := range []struct {
		name, literal, cleaned string
	}{
		{"zero index", "stations[0]", "stations"},
		{"normalized index", "stations[01]", "stations[1]"},
		{"retained index", "stations[1]", "stations[1]"},
	} {
		for _, preserve := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/preserve=%t", tc.name, preserve), func(t *testing.T) {
				claim := "The first station is `" + tc.literal + "`"
				answer := claim + " [1]."
				chunks := citationEvidence(claim + ".")
				qc := &QueryContext{Excerpts: chunks, FinalAnswer: answer, IncludeCitations: true, PreserveCode: preserve}
				var submitted []citationsupport.Batch
				verifier := claimVerifier(func(_ context.Context, batch citationsupport.Batch) ([]citationsupport.Status, error) {
					submitted = append(submitted, batch)
					return []citationsupport.Status{citationsupport.Supported}, nil
				})
				stage := NewSynthesisStage(nil, &config.Config{}).WithCitationSupport(verifier)
				require.NoError(t, stage.finalizeCitations(t.Context(), qc))
				require.Equal(t, &citationsupport.Coverage{Sentences: 1, CitedSentences: 1}, qc.CitationCoverage)
				trailing := qc.Citations[len(qc.Citations)-1]
				if !preserve && tc.literal != tc.cleaned {
					// Legacy cleanup rewrote the literal, so no claim is attached to it.
					require.Equal(t, "The first station is `"+tc.cleaned+"` [1].", qc.FinalAnswer)
					require.Equal(t, citationsupport.Unchecked, trailing.Support.Status)
					require.Empty(t, submitted)
					return
				}
				require.Equal(t, answer, qc.FinalAnswer)
				require.Equal(t, citationsupport.Supported, trailing.Support.Status)
				require.Equal(t, []citationsupport.Batch{{
					Pairs:  []citationsupport.Pair{{Claim: claim, Body: claim + "."}},
					Claims: [][]int{{0}},
				}}, submitted)
				if preserve {
					// Only the marker outside code is a citation.
					require.Len(t, qc.Citations, 1)
				}
			})
		}
	}
}

func TestCitationSupportPreservingCodeCoverage(t *testing.T) {
	// The claim scanner reads the fenced line as ordinary text, so its marker
	// makes a cited sentence. With the option that marker is code: it is not a
	// citation and the sentence is no longer counted as cited.
	answer := "Run it:\n\n```\nb[1]\n```"
	for _, tc := range []struct {
		preserve  bool
		citations int
		cited     int
	}{{false, 1, 1}, {true, 0, 0}} {
		qc := &QueryContext{Excerpts: citationEvidence("b"), FinalAnswer: answer, IncludeCitations: true, PreserveCode: tc.preserve}
		stage := NewSynthesisStage(nil, &config.Config{}).WithCitationSupport(claimVerifier(func(context.Context, citationsupport.Batch) ([]citationsupport.Status, error) {
			return []citationsupport.Status{citationsupport.Supported}, nil
		}))
		require.NoError(t, stage.finalizeCitations(t.Context(), qc))
		require.Equal(t, answer, qc.FinalAnswer)
		require.Len(t, qc.Citations, tc.citations)
		require.Equal(t, tc.cited, qc.CitationCoverage.CitedSentences)
		require.Equal(t, 2, qc.CitationCoverage.Sentences)
	}
}

func TestCitationSupportPreservingCodeAbstainsWhenScannersDisagree(t *testing.T) {
	// The claim scanner keeps the code state across the soft-wrapped fence lines
	// and reads the final marker as code; the citation stays unchecked.
	answer := "```\nb[1]\n```\nDone [1]."
	qc := &QueryContext{Excerpts: citationEvidence("Done"), FinalAnswer: answer, IncludeCitations: true, PreserveCode: true}
	stage := NewSynthesisStage(nil, &config.Config{}).WithCitationSupport(claimVerifier(func(context.Context, citationsupport.Batch) ([]citationsupport.Status, error) {
		t.Fatal("no claim should be submitted")
		return nil, nil
	}))
	require.NoError(t, stage.finalizeCitations(t.Context(), qc))
	require.Len(t, qc.Citations, 1)
	require.Equal(t, citationsupport.Unchecked, qc.Citations[0].Support.Status)
}
