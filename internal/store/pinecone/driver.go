package pinecone

import (
	"context"

	"github.com/Victoroide/quarry-rag/internal/domain"
)

var (
	_ domain.VectorStore                   = (*Driver)(nil)
	_ domain.VectorDeleter                 = (*Driver)(nil)
	_ domain.RevisionVectorDeleter         = (*Driver)(nil)
	_ domain.ManifestRevisionVectorDeleter = (*Driver)(nil)
	_ domain.VectorPresenceChecker         = (*Driver)(nil)
)

// Driver implements the domain.VectorStore interface using Pinecone.
type Driver struct {
	client *Client
}

// NewDriver creates a VectorStore backed by Pinecone.
func NewDriver(client *Client) *Driver {
	return &Driver{client: client}
}

// Upsert inserts or updates vectors in the specified namespace.
func (d *Driver) Upsert(ctx context.Context, namespace string, chunks []domain.Chunk) error {
	return d.client.Upsert(ctx, namespace, chunks)
}

// UpsertWithVisibility returns the visibility probe for these writes when available.
func (d *Driver) UpsertWithVisibility(ctx context.Context, namespace string, chunks []domain.Chunk) (domain.VisibilityProbe, error) {
	return d.client.UpsertWithVisibility(ctx, namespace, chunks)
}

// Query searches for similar vectors in the specified namespace.
func (d *Driver) Query(ctx context.Context, namespace string, embedding []float32, topK int, docTypeFilter string) ([]domain.ScoredChunk, error) {
	return d.client.Query(ctx, namespace, embedding, topK, docTypeFilter)
}

// QueryAllNamespaces queries across all namespaces.
func (d *Driver) QueryAllNamespaces(ctx context.Context, embedding []float32, topK int, docTypeFilter string) ([]domain.ScoredChunk, error) {
	return d.client.QueryAllNamespaces(ctx, embedding, topK, docTypeFilter)
}

// DeleteAllVectorsInNamespace deletes all vectors from the specified namespace.
func (d *Driver) DeleteAllVectorsInNamespace(ctx context.Context, namespace string) error {
	return d.client.DeleteAllVectorsInNamespace(ctx, namespace)
}

// DeleteByDocID deletes vectors from the default namespace whose metadata doc_id matches.
func (d *Driver) DeleteByDocID(ctx context.Context, docID string) error {
	return d.client.DeleteByDocID(ctx, docID)
}

// DeleteByRevision removes only vectors for one document incarnation.
func (d *Driver) DeleteByRevision(ctx context.Context, docID, revision string) error {
	return d.client.DeleteByRevision(ctx, docID, revision)
}

// DeleteByManifestRevision removes vectors belonging only to one manifested revision.
func (d *Driver) DeleteByManifestRevision(ctx context.Context, docID, revisionID string, knownIDs []string) error {
	return d.client.DeleteByManifestRevision(ctx, docID, revisionID, knownIDs)
}

// DeleteVectorIDs removes exact external vector IDs from the target namespace.
func (d *Driver) DeleteVectorIDs(ctx context.Context, namespace string, ids []string) error {
	return d.client.DeleteVectorIDs(ctx, namespace, ids)
}

// FetchPresentVectorIDs returns the subset of external IDs still present in namespace.
func (d *Driver) FetchPresentVectorIDs(ctx context.Context, namespace string, ids []string) ([]string, error) {
	return d.client.FetchPresentVectorIDs(ctx, namespace, ids)
}

// ListPrefixVectorIDs lists one page of external IDs matching prefix when supported.
func (d *Driver) ListPrefixVectorIDs(ctx context.Context, namespace, prefix string, limit int, token string) (ids []string, nextToken string, supported bool, err error) {
	return d.client.ListPrefixVectorIDs(ctx, namespace, prefix, limit, token)
}

// Ping verifies connectivity to the Pinecone index.
func (d *Driver) Ping(ctx context.Context) error {
	return d.client.Ping(ctx)
}

// QueryFiltered applies document scope before selecting vector results.
func (d *Driver) QueryFiltered(ctx context.Context, embedding []float32, topK int, filter domain.VectorFilter) ([]domain.ScoredChunk, error) {
	return d.client.QueryFiltered(ctx, embedding, topK, filter)
}

// GrantScope reports that every search sends its tenant and allowed incarnations.
func (*Driver) GrantScope() domain.GrantScopeKind { return domain.GrantScopeMembership }

// SearchPartitions reports how many requests one search over the given number
// of members sends: one per ten thousand documents, and none for an empty scope.
func (*Driver) SearchPartitions(members int) (int, bool) {
	if members < 0 {
		return 0, false
	}
	return partitionCount(members), true
}

// VisibilityRefillLimit caps metadata-bearing refill responses with values omitted.
func (*Driver) VisibilityRefillLimit() int { return maxRefillTopK }

// QueryCandidates preserves all partition results for SQL visibility checking.
func (d *Driver) QueryCandidates(ctx context.Context, embedding []float32, topK int, filter domain.VectorFilter) ([]domain.ScoredChunk, error) {
	return d.client.queryCandidates(ctx, embedding, topK, filter, nil)
}

// QueryFilteredBudgeted applies all filter partitions within the attempt budget.
func (d *Driver) QueryFilteredBudgeted(ctx context.Context, embedding []float32, topK int, filter domain.VectorFilter, budget int) ([]domain.ScoredChunk, int, error) {
	remaining := budget
	chunks, err := d.client.queryCandidates(ctx, embedding, topK, filter, &remaining)
	return chunks, budget - remaining, err
}

// ManagesVectorDispatch reports that Pinecone claims every mutating RPC separately.
func (*Driver) ManagesVectorDispatch() bool { return true }

// DeleteByAttempt removes vectors written under one worker claim.
func (d *Driver) DeleteByAttempt(ctx context.Context, docID, revision, attempt string) error {
	return d.client.DeleteByAttempt(ctx, docID, revision, attempt)
}
