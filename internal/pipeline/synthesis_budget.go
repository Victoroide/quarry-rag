package pipeline

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/Victoroide/quarry-rag/internal/chunker"
	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/llm"
)

type synthesisPromptBuilder struct {
	question         string
	includeCitations bool
	boundary         string
	guardrail        bool
	modelID          string
	maxTokens        int
	evidenceCheck    func(context.Context, []domain.ScoredChunk) error
}

func (b synthesisPromptBuilder) build(chunks []domain.ScoredChunk) (req llm.Request, system, prompt string) {
	system, parts := buildSynthesisPrompt(b.question, chunks, b.boundary)
	if b.includeCitations {
		system += "\n" + synthesisCitationInstruction
	}
	prompt = strings.Join(parts[:], "")
	content := synthesisMessageContent(parts, prompt, b.guardrail)
	req = llm.Request{
		Model:       b.modelID,
		System:      system,
		Messages:    []llm.Message{{Role: "user", Content: content}},
		Temperature: 0.2,
		MaxTokens:   b.maxTokens,
	}
	return req, system, prompt
}

type strictBudgetOutcome struct {
	chunks    []domain.ScoredChunk
	truncated []bool
	request   llm.Request
	system    string
	prompt    string
	count     llm.CountResult
}

func translatePipelineCountError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, domain.ErrEvidenceCheck) {
		return err
	}
	switch {
	case errors.Is(err, llm.ErrUnsupportedProfile),
		errors.Is(err, llm.ErrUnsupportedRegion),
		errors.Is(err, llm.ErrUnsupportedInputShape):
		return fmt.Errorf("%w: %w", ErrSynthesisProfileUnsupported, err)
	case errors.Is(err, llm.ErrProviderCountFailed),
		errors.Is(err, llm.ErrInvalidTokenCount),
		errors.Is(err, llm.ErrCountAttemptsExceeded):
		return fmt.Errorf("%w: %w", ErrSynthesisCountFailed, err)
	default:
		return fmt.Errorf("%w: %w", ErrSynthesisCountFailed, err)
	}
}

func resolveReservedOutput(profile llm.ModelProfile, requested int) (int, error) {
	reserved := requested
	if reserved <= 0 {
		reserved = profile.OutputReserve
	}
	if profile.OutputReserve > reserved {
		reserved = profile.OutputReserve
	}
	if reserved <= 0 {
		reserved = llm.DefaultMaxTokens
	}
	if profile.MaxOutputTokens > 0 && reserved > profile.MaxOutputTokens {
		return 0, fmt.Errorf("%w: output reserve %d exceeds model max %d", ErrSynthesisProfileUnsupported, reserved, profile.MaxOutputTokens)
	}
	return reserved, nil
}

func budgetStrictSynthesisInput(
	ctx context.Context,
	amb *llm.Ambassador,
	cfg *config.Config,
	builder synthesisPromptBuilder,
	profile llm.ModelProfile,
	inputBudget int,
	chunks []domain.ScoredChunk,
	truncated []bool,
) (strictBudgetOutcome, error) {
	sanitized := slices.Clone(chunks)
	for i := range sanitized {
		sanitized[i].Chunk.Text = sanitizeExcerpt(sanitized[i].Chunk.Text)
	}
	if !profile.IsRemote() {
		return budgetLocalStrictInput(ctx, amb, cfg, builder, inputBudget, sanitized, truncated)
	}
	return budgetRemoteStrictInput(ctx, amb, cfg, builder, inputBudget, sanitized, truncated)
}

func (b synthesisPromptBuilder) disclosureContext(ctx context.Context, chunks []domain.ScoredChunk) context.Context {
	if b.evidenceCheck == nil {
		return ctx
	}
	return domain.WithEvidenceCheck(ctx, func(ctx context.Context) error { return b.evidenceCheck(ctx, chunks) })
}

func (b synthesisPromptBuilder) measure(
	ctx context.Context, amb *llm.Ambassador, cfg *config.Config, budget *llm.CountBudget,
	chunks []domain.ScoredChunk, truncated []bool,
) (strictBudgetOutcome, error) {
	if err := ctx.Err(); err != nil {
		return strictBudgetOutcome{}, err
	}
	req, system, prompt := b.build(chunks)
	if budget != nil && len(chunks) != 0 {
		ctx = b.disclosureContext(ctx, chunks)
		if err := domain.CheckEvidenceBeforeSend(ctx); err != nil {
			return strictBudgetOutcome{}, err
		}
	}
	count, err := amb.CountInput(ctx, cfg, req, budget)
	if err != nil {
		return strictBudgetOutcome{}, translatePipelineCountError(err)
	}
	return strictBudgetOutcome{chunks, truncated, req, system, prompt, count}, nil
}

func budgetLocalStrictInput(
	ctx context.Context,
	amb *llm.Ambassador,
	cfg *config.Config,
	builder synthesisPromptBuilder,
	inputBudget int,
	chunks []domain.ScoredChunk,
	truncated []bool,
) (strictBudgetOutcome, error) {
	measure := func(chunks []domain.ScoredChunk, cuts []bool) (strictBudgetOutcome, error) {
		return builder.measure(ctx, amb, cfg, nil, chunks, cuts)
	}
	full, err := measure(chunks, truncated)
	if err != nil || full.count.InputTokens <= inputBudget {
		return full, err
	}
	base, err := measure(nil, nil)
	if err != nil {
		return strictBudgetOutcome{}, err
	}
	if base.count.InputTokens > inputBudget {
		return strictBudgetOutcome{}, baseInputTooLarge(base.count.InputTokens, inputBudget)
	}

	best := base
	lo, hi := 0, len(chunks)-1
	for lo < hi {
		mid := (lo + hi + 1) / 2
		trial, err := measure(chunks[:mid], truncated[:mid])
		if err != nil {
			return strictBudgetOutcome{}, err
		}
		if trial.count.InputTokens <= inputBudget {
			lo, best = mid, trial
		} else {
			hi = mid - 1
		}
	}
	candidate := chunks[lo]
	runes := []rune(candidate.Chunk.Text)
	if len(runes) == 0 {
		return best, nil
	}
	trialChunks := slices.Concat(chunks[:lo], []domain.ScoredChunk{candidate})
	trialCuts := append(slices.Clone(truncated[:lo]), true)
	longest := 0
	tried := make(map[string]bool)
	testCut := func(n int) (bool, error) {
		body, ok := cleanExcerptCut(string(runes[:n]), lo > 0)
		if !ok {
			return false, nil
		}
		if fits, seen := tried[body]; seen {
			return fits, nil
		}
		trialChunks[lo].Chunk.Text = body
		trial, err := measure(trialChunks, trialCuts)
		if err != nil {
			return false, err
		}
		fits := trial.count.InputTokens <= inputBudget
		tried[body] = fits
		if fits && len(body) > longest {
			longest = len(body)
			trial.chunks = slices.Clone(trialChunks)
			best = trial
		}
		return fits, nil
	}

	// Bisection locates the approximate cut. Byte-pair merges make
	// adjacent UTF-8 prefixes nonmonotone, so check the neighborhood as
	// well as the shortest prefixes when no whole excerpt fits.
	left, right := 0, len(runes)-1
	for left < right {
		mid := (left + right + 1) / 2
		fits, err := testCut(mid)
		if err != nil {
			return strictBudgetOutcome{}, err
		}
		if fits {
			left = mid
		} else {
			right = mid - 1
		}
	}
	const mergeNeighborhood = 8
	for n := min(len(runes)-1, left+mergeNeighborhood); n > left; n-- {
		if fits, err := testCut(n); err != nil {
			return strictBudgetOutcome{}, err
		} else if fits {
			break
		}
	}
	if len(best.chunks) == 0 {
		// The verified vocabulary has no mergeable token longer than 128 bytes.
		// A short fit may lie on the far side of an overflowing rune prefix.
		for n := left + mergeNeighborhood + 1; n <= min(len(runes)-1, 128); n++ {
			if _, err := testCut(n); err != nil {
				return strictBudgetOutcome{}, err
			}
		}
	}
	if len(best.chunks) == 0 {
		return strictBudgetOutcome{}, fmt.Errorf("%w: no excerpt fits remaining budget", ErrSynthesisInputTooLarge)
	}
	return best, nil
}

func budgetRemoteStrictInput(
	ctx context.Context,
	amb *llm.Ambassador,
	cfg *config.Config,
	builder synthesisPromptBuilder,
	inputBudget int,
	chunks []domain.ScoredChunk,
	truncated []bool,
) (strictBudgetOutcome, error) {
	budget := llm.NewCountBudget(llm.MaxRemoteCountAttempts)
	measure := func(chunks []domain.ScoredChunk, cuts []bool) (strictBudgetOutcome, error) {
		return builder.measure(ctx, amb, cfg, budget, chunks, cuts)
	}

	full, err := measure(chunks, truncated)
	if err != nil || full.count.InputTokens <= inputBudget {
		return full, err
	}
	if len(chunks) == 0 {
		return strictBudgetOutcome{}, baseInputTooLarge(full.count.InputTokens, inputBudget)
	}
	minimal := minimumEvidence(chunks)
	if budget.Remaining() == 0 {
		return strictBudgetOutcome{}, verificationExhausted()
	}
	if sameExcerptBodies(minimal, chunks) {
		base, baseErr := measure(nil, nil)
		if baseErr != nil {
			return strictBudgetOutcome{}, baseErr
		}
		if base.count.InputTokens > inputBudget {
			return strictBudgetOutcome{}, baseInputTooLarge(base.count.InputTokens, inputBudget)
		}
		return strictBudgetOutcome{}, fmt.Errorf("%w: the smallest excerpt exceeds the remaining budget", ErrSynthesisInputTooLarge)
	}

	if budget.Remaining() == 1 {
		trial, trialErr := measure(minimal, []bool{true})
		if trialErr != nil || trial.count.InputTokens <= inputBudget {
			return trial, trialErr
		}
		return strictBudgetOutcome{}, verificationExhausted()
	}

	wrapperWeight, weights, fullWeight := measureExcerptWeights(builder.boundary, chunks)
	ratio := float64(inputBudget) / float64(full.count.InputTokens)
	// Aggregate density can underestimate the leading excerpts. Confirm a
	// conservative prefix first and keep it if an expansion fails to fit.
	target := int(float64(fullWeight) * min(0.5, ratio*0.75))
	candidate, cuts, usedWeight := selectExcerptsByWeight(chunks, truncated, wrapperWeight, weights, target)
	if len(candidate) == 0 {
		candidate, cuts = minimal, []bool{true}
	}
	fallback, err := measure(candidate, cuts)
	if err != nil {
		return strictBudgetOutcome{}, err
	}
	if fallback.count.InputTokens <= inputBudget {
		if budget.Remaining() == 0 || usedWeight == 0 {
			return fallback, nil
		}
		deltaWeight := fullWeight - usedWeight
		deltaTokens := full.count.InputTokens - fallback.count.InputTokens
		target = usedWeight + int(float64(deltaWeight)*float64(inputBudget-fallback.count.InputTokens)/float64(deltaTokens))
		expanded, expandedCuts, _ := selectExcerptsByWeight(chunks, truncated, wrapperWeight, weights, target)
		if sameExcerptBodies(candidate, expanded) || len(expanded) < len(candidate) {
			return fallback, nil
		}
		trial, trialErr := measure(expanded, expandedCuts)
		if trialErr != nil {
			return strictBudgetOutcome{}, trialErr
		}
		if trial.count.InputTokens <= inputBudget {
			return trial, nil
		}
		return fallback, nil
	}
	if budget.Remaining() == 0 {
		return strictBudgetOutcome{}, verificationExhausted()
	}
	if !sameExcerptBodies(candidate, minimal) {
		trial, trialErr := measure(minimal, []bool{true})
		if trialErr != nil || trial.count.InputTokens <= inputBudget {
			return trial, trialErr
		}
		return strictBudgetOutcome{}, verificationExhausted()
	}

	base, err := measure(nil, nil)
	if err != nil {
		return strictBudgetOutcome{}, err
	}
	if base.count.InputTokens > inputBudget {
		return strictBudgetOutcome{}, baseInputTooLarge(base.count.InputTokens, inputBudget)
	}
	return strictBudgetOutcome{}, verificationExhausted()
}

func minimumEvidence(chunks []domain.ScoredChunk) []domain.ScoredChunk {
	for _, chunk := range chunks {
		body := strings.TrimLeftFunc(chunk.Chunk.Text, unicode.IsSpace)
		if body != "" {
			_, width := utf8.DecodeRuneInString(body)
			chunk.Chunk.Text = body[:width]
			return []domain.ScoredChunk{chunk}
		}
	}
	return nil
}

func baseInputTooLarge(count, limit int) error {
	return fmt.Errorf("%w: base request requires %d tokens, budget is %d", ErrSynthesisInputTooLarge, count, limit)
}

func verificationExhausted() error {
	return fmt.Errorf("%w: %w", ErrSynthesisCountFailed, llm.ErrCountAttemptsExceeded)
}

func cleanExcerptCut(body string, hasPriorExcerpts bool) (string, bool) {
	if end := strings.LastIndexAny(body, ".\n"); end >= 0 && chunker.EstimateTokens(body[:end+1]) >= 64 {
		return body[:end+1], true
	}
	if ws := strings.LastIndexFunc(body, unicode.IsSpace); ws > 0 {
		if trimmed := strings.TrimRightFunc(body[:ws], unicode.IsSpace); chunker.EstimateTokens(trimmed) >= 64 {
			return trimmed, true
		}
	}
	if !hasPriorExcerpts && strings.TrimSpace(body) != "" {
		return body, true
	}
	return "", false
}

func measureFineWeight(text string) int {
	dense, words, other := 0, 0, 0
	inWord := false
	for _, r := range text {
		isDense := isDenseRune(r)
		isWord := !isDense && !unicode.IsSpace(r)
		if isDense {
			dense++
		} else if isWord {
			other++
			if !inWord {
				words++
			}
		}
		inWord = isWord
	}
	return 4*dense + other + words
}

func cutTextToWeight(text string, maxWeight int) string {
	if maxWeight <= 0 {
		return ""
	}
	dense, words, other := 0, 0, 0
	inWord := false
	endByte := 0
	for i, r := range text {
		isDense := isDenseRune(r)
		isWord := !isDense && !unicode.IsSpace(r)
		if isDense {
			dense++
		} else if isWord {
			other++
			if !inWord {
				words++
			}
		}
		inWord = isWord
		if 4*dense+other+words > maxWeight {
			break
		}
		endByte = i + utf8.RuneLen(r)
	}
	return text[:endByte]
}

func isDenseRune(r rune) bool {
	// The lowest block among Thai/Lao/Tibetan/Myanmar/Khmer/Hangul/Kana/Han starts at U+0E00.
	if r < 0x0E00 {
		return false
	}
	return unicode.In(r, unicode.Han, unicode.Hiragana, unicode.Katakana,
		unicode.Hangul, unicode.Thai, unicode.Lao, unicode.Khmer, unicode.Myanmar, unicode.Tibetan)
}

func measureExcerptWeights(boundary string, chunks []domain.ScoredChunk) (wrapperW int, elemW []int, totalExcerptsW int) {
	wrapperSample := fmt.Sprintf("\n<excerpt-%s id=\"%d\">\n\n</excerpt-%s>\n", boundary, len(chunks), boundary)
	wrapperW = max(1, measureFineWeight(wrapperSample))
	elemW = make([]int, len(chunks))
	for i, sc := range chunks {
		w := wrapperW + max(1, measureFineWeight(sc.Chunk.Text))
		elemW[i] = w
		totalExcerptsW += w
	}
	return wrapperW, elemW, totalExcerptsW
}

func selectExcerptsByWeight(chunks []domain.ScoredChunk, truncated []bool, wrapperW int, elemW []int, targetExcerptsW int) (out []domain.ScoredChunk, outTrunc []bool, usedW int) {
	if len(chunks) == 0 || targetExcerptsW <= 0 {
		return nil, nil, 0
	}
	for i, sc := range chunks {
		if usedW+elemW[i] <= targetExcerptsW {
			out = append(out, sc)
			outTrunc = append(outTrunc, truncated[i])
			usedW += elemW[i]
			continue
		}
		remW := targetExcerptsW - usedW
		if remW <= wrapperW {
			break
		}
		availBodyW := remW - wrapperW
		if len(out) > 0 && availBodyW < 64*4 {
			break
		}
		body := cutTextToWeight(sc.Chunk.Text, availBodyW)
		cleanBody, ok := cleanExcerptCut(body, len(out) > 0)
		if !ok {
			break
		}
		sc.Chunk.Text = cleanBody
		out = append(out, sc)
		outTrunc = append(outTrunc, true)
		usedW += wrapperW + max(1, measureFineWeight(cleanBody))
		break
	}
	return out, outTrunc, usedW
}

func sameExcerptBodies(a, b []domain.ScoredChunk) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Chunk.Text != b[i].Chunk.Text {
			return false
		}
	}
	return true
}
