package domain

import (
	"context"
	"fmt"
	"time"
)

// FormatRevisionVectorID builds the immutable external vector ID for a chunk
// belonging to a fenced document revision.
func FormatRevisionVectorID(docID DocumentID, revisionID string, index int) string {
	return fmt.Sprintf("%s#%s#%d", docID, revisionID, index)
}

// RevisionVectorPrefix returns the prefix shared by all external vectors
// belonging to a fenced document revision.
func RevisionVectorPrefix(docID DocumentID, revisionID string) string {
	return fmt.Sprintf("%s#%s#", docID, revisionID)
}

// VectorManifest records the intended external vector IDs for one ingestion attempt.
type VectorManifest struct {
	RevisionID      string
	DocumentID      DocumentID
	Incarnation     string
	Attempt         int
	LeaseToken      string
	Collection      string
	VectorNamespace string
	IDPrefix        string
	VectorIDs       []string
	ChunkIDs        []string
	State           string
}

// VectorCleanupTask tracks a retired revision whose external vectors must be removed.
type VectorCleanupTask struct {
	RevisionID        string
	DocumentID        DocumentID
	Incarnation       string
	Collection        string
	VectorNamespace   string
	IDPrefix          string
	VectorIDs         []string
	NextOffset        int
	State             string
	Attempts          int
	ConvergenceChecks int
	LastError         string
	LeaseOwner        string
	LeaseToken        *string
	NextAttemptAt     time.Time
	RetiredAt         time.Time
	Reason            string
}

// ManifestRevisionVectorDeleter removes only external vectors belonging to one manifested revision.
type ManifestRevisionVectorDeleter interface {
	DeleteByManifestRevision(ctx context.Context, docID, revisionID string, knownIDs []string) error
}

// VectorPresenceChecker deletes exact external IDs and checks for remaining residue.
type VectorPresenceChecker interface {
	DeleteVectorIDs(ctx context.Context, namespace string, ids []string) error
	FetchPresentVectorIDs(ctx context.Context, namespace string, ids []string) ([]string, error)
	ListPrefixVectorIDs(ctx context.Context, namespace, prefix string, limit int, token string) (ids []string, nextToken string, supported bool, err error)
}

// VectorManifestStore persists write-intent manifests and leased cleanup cursors.
type VectorManifestStore interface {
	PersistVectorManifest(ctx context.Context, job Job, manifest VectorManifest) error
	GetVectorManifest(ctx context.Context, revisionID string) (*VectorManifest, error)
	ListDocumentVectorManifests(ctx context.Context, docID DocumentID, incarnation string) ([]VectorManifest, error)
	ClaimVectorCleanup(ctx context.Context, owner string, lease, drain time.Duration) (*VectorCleanupTask, error)
	RenewVectorCleanup(ctx context.Context, task VectorCleanupTask, lease time.Duration) (bool, error)
	AdvanceVectorCleanup(ctx context.Context, task VectorCleanupTask, nextOffset int) (bool, error)
	DeferVectorCleanup(ctx context.Context, task VectorCleanupTask, state string, delay time.Duration, convergenceChecks int, lastError string) (bool, error)
	CompleteVectorCleanup(ctx context.Context, task VectorCleanupTask) (bool, error)
	ReleaseVectorCleanup(ctx context.Context, task VectorCleanupTask) error
}
