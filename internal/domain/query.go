package domain

// AnswerMode selects what the query returns after retrieval.
type AnswerMode string

// Supported answer modes and evidence completion reason.
const (
	AnswerModeGenerated AnswerMode = "generated"
	AnswerModeEvidence  AnswerMode = "evidence"

	// FinishReasonEvidenceOnly identifies completion with selected evidence.
	FinishReasonEvidenceOnly = "evidence_only"
)
