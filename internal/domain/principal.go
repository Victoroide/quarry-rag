package domain

// Principal identifies a provisioned subject and its immutable tenant.
type Principal struct {
	ID       string
	TenantID string
}
