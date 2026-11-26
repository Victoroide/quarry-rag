package handler

import (
	"fmt"

	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/google/uuid"
)

// maxAllowedDocumentIDs is the largest allowlist, and the largest hosted
// membership, a single query may carry.
func maxAllowedDocumentIDs(cfg *config.Config) int {
	if cfg.MaxAllowedDocumentIDs <= 0 {
		return 25000
	}
	return cfg.MaxAllowedDocumentIDs
}

func validateQueryAllowlist(req *QueryRequest, cfg *config.Config) *APIError {
	if cfg.RequireDocumentAllowlist && req.AllowedDocumentIDs == nil {
		return &APIError{Error: "validation", Message: "allowed_document_ids is required"}
	}
	limit := maxAllowedDocumentIDs(cfg)
	if len(req.AllowedDocumentIDs) > limit {
		return &APIError{Error: "validation", Message: fmt.Sprintf("allowed_document_ids exceeds maximum of %d entries", limit)}
	}
	if req.AllowedDocumentIDs == nil {
		return nil
	}
	canonical := make([]string, 0, len(req.AllowedDocumentIDs))
	seen := make(map[string]struct{}, len(req.AllowedDocumentIDs))
	for _, id := range req.AllowedDocumentIDs {
		parsed, err := uuid.Parse(id)
		if err != nil {
			return &APIError{Error: "validation", Message: "allowed_document_ids must contain UUIDs"}
		}
		id = parsed.String()
		if _, ok := seen[id]; !ok {
			canonical = append(canonical, id)
			seen[id] = struct{}{}
		}
	}
	req.AllowedDocumentIDs = canonical
	return nil
}
