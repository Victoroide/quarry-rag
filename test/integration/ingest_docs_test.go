package integration

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/chunker"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestPrivateRollbackQueueCheck(t *testing.T) {
	doc, err := os.ReadFile("../../docs/deployment.md")
	require.NoError(t, err)
	_, downgrade, found := strings.Cut(string(doc), "### Downgrading\n")
	require.True(t, found, "the rollout guide must describe downgrading")
	downgrade, _, _ = strings.Cut(downgrade, "\n## ")
	_, block, found := strings.Cut(downgrade, "```sql\n")
	require.True(t, found, "the downgrade guide must provide a queue check")
	query, _, found := strings.Cut(block, "\n```")
	require.True(t, found, "the queue check must be a complete SQL example")

	c := durableDatabase(t)
	ctx := t.Context()
	assertDrained := func(want bool) {
		t.Helper()
		var drained bool
		require.NoError(t, c.Pool.QueryRow(ctx, query).Scan(&drained))
		require.Equal(t, want, drained)
	}
	const content = "pending private document"
	req := domain.AcceptRequest{
		Document:       domain.Document{ID: domain.DocumentID(uuid.NewString()), Content: content},
		CallerSupplied: true,
		Fingerprint:    chunker.Fingerprint(content),
		ContentDedup:   domain.ContentDedupOff,
		MaxPending:     2,
	}
	result, err := c.AcceptDocument(ctx, req)
	require.NoError(t, err)
	require.Equal(t, domain.AcceptAccepted, result.Outcome)
	assertDrained(false)
	job, err := c.ClaimJob(ctx, "private-downgrade", time.Minute, 5)
	require.NoError(t, err)
	require.NotNil(t, job)
	require.Equal(t, req.Document.ID, job.Document.ID)
	assertDrained(false)
	require.NoError(t, c.FailJob(ctx, *job, "terminal failure"))

	durableSubmitResponse := durableSubmit(t, nil, c, `{"content":"pending shared document"}`)
	require.Equal(t, 202, durableSubmitResponse.Code, durableSubmitResponse.Body.String())
	assertDrained(true)
	req.ContentDedup = domain.ContentDedupCollection
	result, err = c.AcceptDocument(ctx, req)
	require.NoError(t, err)
	require.Equal(t, domain.AcceptRequeued, result.Outcome)
	assertDrained(false)
}
