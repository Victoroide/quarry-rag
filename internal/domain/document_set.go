package domain

import (
	"context"
	"errors"
	"sync"
	"time"
)

// ErrDocumentSetNotFound reports a document set that cannot be used by the
// requesting principal. A missing, foreign, deleted or expired reference
// produces this same error.
var ErrDocumentSetNotFound = errors.New("document set not found")

// DocumentSetRef identifies the immutable membership a query is bound to. A
// reference only selects documents. It grants its holder nothing.
type DocumentSetRef struct {
	ID string
	// Revision is the server-generated identity of the immutable membership.
	Revision string
	// ExpiresAt is the instant after which the set no longer resolves.
	ExpiresAt time.Time
}

// DocumentSetAuthority resolves and rechecks sets bound to one principal. A
// ReadAuthority that also implements it can serve queries that carry a set.
type DocumentSetAuthority interface {
	// DocumentSetState reports the principal's standing and the policy revision
	// together with whether the set is still live under the same revision.
	DocumentSetState(ctx context.Context, principal Principal, set DocumentSetRef) (state ReadState, live bool, err error)
	// EvidenceInSet returns checks that also require every document to belong to
	// the set, so a candidate outside it can never be previewed or released.
	EvidenceInSet(principal Principal, set DocumentSetRef, collection, docType string) SubjectEvidence
}

// DocumentSetScopedVectorStore is a vector store that can confine a search to a
// set's membership. A relation store joins the membership inside its own
// ranking statement. A relation store that cannot must report false, because it
// receives no document list to enforce the membership with.
type DocumentSetScopedVectorStore interface {
	SupportsDocumentSets() bool
}

// PartitionedVectorStore is a hosted store that splits the members of a scoped
// search into bounded requests. It reports how many requests one search over
// the given number of members makes, so a caller can price the search before
// it spends anything. The second result is false when the store cannot say.
type PartitionedVectorStore interface {
	SearchPartitions(members int) (requests int, ok bool)
}

// SearchBudget bounds the hosted search attempts of one request. It counts
// every attempt, including retries, and a search must reserve its attempts
// before it dispatches. The request's route is planned so that its first
// attempts never exceed the planned count. Retries may use only the remainder,
// so a retry can never starve a search the route still needs.
type SearchBudget struct {
	mu      sync.Mutex
	limit   int
	retries int
	used    int
}

// NewSearchBudget allows limit attempts in total. planned is the most first
// attempts the route can make. The difference is the pool available for retries.
func NewSearchBudget(limit, planned int) *SearchBudget {
	return &SearchBudget{limit: limit, retries: max(0, limit-planned)}
}

// Acquire reserves between one and max attempts for a single search and
// returns how many it may make. It fails with ErrVectorQueryBudget when no
// attempt remains.
func (b *SearchBudget) Acquire(maxAttempts int) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if maxAttempts < 1 || b.used >= b.limit {
		return 0, ErrVectorQueryBudget
	}
	allowed := min(maxAttempts, 1+b.retries, b.limit-b.used)
	b.retries -= allowed - 1
	b.used += allowed
	return allowed, nil
}

// Release returns the attempts a search reserved but did not make.
func (b *SearchBudget) Release(allowed, made int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	made = max(0, min(made, allowed))
	b.used -= allowed - made
	b.retries += (allowed - 1) - max(0, made-1)
}

// Used reports the attempts made or reserved so far.
func (b *SearchBudget) Used() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.used
}
