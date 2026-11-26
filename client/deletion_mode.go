package client

import (
	"context"
	"net/http"
)

// DeletionModeResponse reports the deletion mode recorded in the service's own
// database.
type DeletionModeResponse struct {
	// DeletionMode is "inactive" before an operator activates checkpointed
	// deletion and "active" afterwards.
	DeletionMode string `json:"deletion_mode"`
}

// Active reports whether the database selected checkpointed deletion. Any
// other value, including one this client does not know, is not active.
func (r DeletionModeResponse) Active() bool { return r.DeletionMode == "active" }

// GetDeletionMode reads the deletion mode from the target service. The check
// only reads: it never activates anything. Tools that write documents call it
// before their first ingest, so that cleanup with a saved incarnation is
// available. The service answers 503 when it cannot read its database.
func (c *Client) GetDeletionMode(ctx context.Context, opts ...RequestOption) (*DeletionModeResponse, error) {
	var response DeletionModeResponse
	if err := c.jsonRequest(ctx, http.MethodGet, "/deletion-mode", nil, &response, opts...); err != nil {
		return nil, err
	}
	return &response, nil
}
