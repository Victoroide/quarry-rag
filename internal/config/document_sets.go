package config

import "fmt"

// Limits of the document set settings.
const (
	// DocumentSetMembersLimit is the largest membership ceiling an operator may configure.
	DocumentSetMembersLimit = 100000
	// DocumentSetBatchesLimit is the largest hosted search budget one request may have.
	DocumentSetBatchesLimit = 10
)

// LoadDocumentSetMaxMembers reads and validates DOCUMENT_SET_MAX_MEMBERS. The
// service and the operator command that creates sets read the same value.
func LoadDocumentSetMaxMembers() (int, error) {
	value, err := envIntOrDefault("DOCUMENT_SET_MAX_MEMBERS", DocumentSetMembersLimit)
	if err != nil {
		return 0, err
	}
	return value, checkDocumentSetMaxMembers(value)
}

func checkDocumentSetMaxMembers(value int) error {
	if value < 1 || value > DocumentSetMembersLimit {
		return fmt.Errorf("config: DOCUMENT_SET_MAX_MEMBERS must be between 1 and %d", DocumentSetMembersLimit)
	}
	return nil
}

func (c *Config) validateDocumentSets() error {
	if err := checkDocumentSetMaxMembers(c.DocumentSetMaxMembers); err != nil {
		return err
	}
	if c.DocumentSetMaxBatches < 1 || c.DocumentSetMaxBatches > DocumentSetBatchesLimit {
		return fmt.Errorf("config: DOCUMENT_SET_MAX_BATCHES must be between 1 and %d", DocumentSetBatchesLimit)
	}
	if c.DocumentSetsEnabled && c.AuthzMode != AuthzModeGrants {
		return fmt.Errorf("config: DOCUMENT_SETS_ENABLED=true requires AUTHZ_MODE=%s", AuthzModeGrants)
	}
	return nil
}
