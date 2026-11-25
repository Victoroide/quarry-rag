// Package cleanup removes documents and their stored data.
package cleanup

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/Victoroide/quarry-rag/client"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	pcstore "github.com/Victoroide/quarry-rag/internal/store/pinecone"
	pgstore "github.com/Victoroide/quarry-rag/internal/store/postgres"
	s3store "github.com/Victoroide/quarry-rag/internal/store/s3"
)

// ObjectDeleter removes a stored source file.
type ObjectDeleter interface {
	Delete(context.Context, string) error
}

// Remove deletes external data before metadata so failed operations remain retryable.
func Remove(ctx context.Context, cs domain.ChunkStore, vectors domain.VectorDeleter, objects ObjectDeleter, doc *domain.Document) error {
	if guard, ok := cs.(domain.DocumentRemovalGuard); ok {
		return guard.WithOrdinaryRemoval(ctx, doc.ID, func(guardCtx context.Context) error {
			return remove(guardCtx, cs, vectors, objects, doc)
		})
	}
	return remove(ctx, cs, vectors, objects, doc)
}

func remove(ctx context.Context, cs domain.ChunkStore, vectors domain.VectorDeleter, objects ObjectDeleter, doc *domain.Document) error {
	if doc.S3Key != nil && objects == nil {
		return ErrFileStorageUnavailable
	}
	store, guarded := cs.(interface {
		CheckLiveVectorRemoval(context.Context, *domain.Document) error
		RemoveLiveDocument(context.Context, *domain.Document) error
	})
	if guarded {
		if err := store.CheckLiveVectorRemoval(ctx, doc); err != nil {
			return err
		}
	}
	if vectors != nil {
		if err := vectors.DeleteAllVectorsInNamespace(ctx, string(doc.ID)); err != nil && status.Code(err) != codes.NotFound {
			return fmt.Errorf("delete document namespace: %w", err)
		}
		if err := vectors.DeleteByDocID(ctx, string(doc.ID)); err != nil && status.Code(err) != codes.NotFound {
			return fmt.Errorf("delete global document vectors: %w", err)
		}
	}
	if objects != nil && doc.S3Key != nil {
		if err := objects.Delete(ctx, *doc.S3Key); err != nil {
			return fmt.Errorf("delete source file: %w", err)
		}
	}
	var removeErr error
	if guarded {
		removeErr = store.RemoveLiveDocument(ctx, doc)
	} else {
		removeErr = cs.DeleteDocument(ctx, doc.ID)
	}
	if removeErr != nil {
		return fmt.Errorf("delete document metadata: %w", removeErr)
	}
	if vectors != nil {
		if err := vectors.DeleteByDocID(ctx, string(doc.ID)); err != nil && status.Code(err) != codes.NotFound {
			return fmt.Errorf("delete document vectors after metadata: %w", err)
		}
	}
	return nil
}

// ErrDocumentNotFound indicates that metadata is not yet present or was removed.
var ErrDocumentNotFound = domain.ErrDocumentNotFound

// ErrFileStorageUnavailable indicates a stored file cannot be removed without object storage.
var ErrFileStorageUnavailable = domain.ErrFileStorageUnavailable

// ErrScopedDeletionCanceled reports that a scoped deletion stopped because the
// caller context ended before the deletion intent committed.
var ErrScopedDeletionCanceled = errors.New("scoped deletion canceled before commit")

// Targets names the nonsecret stores that a checkpointed deletion acts on. The
// coordinator records them with the cleanup target and refuses to resume
// against different stores.
type Targets struct {
	// SourceBucket is the object bucket that holds original files.
	SourceBucket string
	// VectorIndex is the hosted index that holds vectors.
	VectorIndex string
	// InMemory marks vectors that live in the serving process.
	InMemory bool
	// Memory identifies that in-memory index and serializes its deletions. An
	// ID-only cleanup of in-memory vectors needs it.
	Memory domain.InMemoryIndex
}

// operations adapts the configured stores to the coordinator's external steps.
// A transactional vector store has no external step: the metadata removal
// cascades to its rows.
func operations(vectors domain.VectorDeleter, objects ObjectDeleter, targets Targets) domain.DeleteOperations {
	ops := domain.DeleteOperations{InMemory: targets.InMemory, SourceBucket: targets.SourceBucket, VectorIndex: targets.VectorIndex}
	if targets.Memory != nil {
		ops.MemoryOwner = targets.Memory.MemoryOwner()
	}
	if _, transactional := vectors.(domain.TransactionalVectorStore); vectors != nil && !transactional {
		ops.LegacyNamespace = func(ctx context.Context, id string) error {
			return ignoreMissing(vectors.DeleteAllVectorsInNamespace(ctx, id))
		}
		ops.LegacyVectors = func(ctx context.Context, id string) error {
			return ignoreMissing(vectors.DeleteByDocID(ctx, id))
		}
		if revisions, ok := vectors.(domain.RevisionVectorDeleter); ok {
			ops.RevisionVectors = func(ctx context.Context, id, incarnation string) error {
				return ignoreMissing(revisions.DeleteByRevision(ctx, id, incarnation))
			}
		}
	}
	if objects != nil {
		ops.Source = objects.Delete
	}
	return ops
}

func ignoreMissing(err error) error {
	if status.Code(err) == codes.NotFound {
		return nil
	}
	return err
}

// RemoveIncarnation deletes the document lifetime the caller observed through
// the checkpointed coordinator. HTTP and administrative cleanup both use it.
// An empty incarnation is accepted only for an absent document.
func RemoveIncarnation(ctx context.Context, deleter domain.DocumentDeleter, vectors domain.VectorDeleter, objects ObjectDeleter, targets Targets, id domain.DocumentID, incarnation string) error {
	ops := operations(vectors, objects, targets)
	ops.ExpectedIncarnation = incarnation
	return deleter.DeleteWithCleanup(ctx, id, ops)
}

// RemoveScoped atomically authorizes owner-only deletion against the current
// incarnation under ctx, then runs detached cross-store cleanup for that target.
func RemoveScoped(ctx context.Context, deleter domain.ScopedDocumentDeleter, vectors domain.VectorDeleter, objects ObjectDeleter, targets Targets, principal domain.Principal, validUntil time.Time, id domain.DocumentID, incarnation string) error {
	ops := operations(vectors, objects, targets)
	ops.ExpectedIncarnation = incarnation
	resolvedIncarnation, complete, err := deleter.BeginScopedDeletion(ctx, principal, validUntil, id, ops)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return fmt.Errorf("%w: %w", ErrScopedDeletionCanceled, err)
		}
		return err
	}
	if complete {
		return nil
	}
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 60*time.Second)
	defer cancel()
	return deleter.ResumeScopedDeletion(cleanupCtx, id, resolvedIncarnation, ops)
}

// Cleaner reuses store connections for a sequence of document deletions.
type Cleaner struct {
	metadata *pgstore.Client
	vectors  *pcstore.Client
	objects  ObjectDeleter
	targets  Targets
	// offline forwards deletions to the service that holds an in-memory index.
	offline OfflineAPI
	// reserve routes ID-only cleanup through the activation guard and its reservation.
	reserve bool
}

// OfflineAPI is the service interface that holds in-memory vectors. Only that
// service can delete them, so offline cleanup is sent there.
type OfflineAPI interface {
	DeleteDocument(ctx context.Context, id string) (*client.DeleteResponse, error)
	DeleteDocumentIncarnation(ctx context.Context, id, incarnation string) (*client.DeleteResponse, error)
}

// Option configures a Cleaner.
type Option func(*Cleaner)

// WithLegacyReservations makes ID-only cleanup commit a durable reservation
// under the activation lock before it mutates any external store. It is meant
// for a deployment that can also quarantine, release and activate.
func WithLegacyReservations() Option {
	return func(c *Cleaner) { c.reserve = true }
}

// WithOfflineService sends every deletion to the service that owns the
// in-memory index instead of touching any store directly.
func WithOfflineService(api OfflineAPI) Option {
	return func(c *Cleaner) { c.offline = api }
}

// New connects the stores required for document deletion. With GO_RAG_FAKE_MODE
// the vectors live in the running service, so deletions go to that service.
func New(ctx context.Context, cfg *config.Config, opts ...Option) (*Cleaner, error) {
	cleaner := &Cleaner{}
	for _, opt := range opts {
		opt(cleaner)
	}
	if cfg.FakeMode {
		if cleaner.offline != nil {
			return cleaner, nil
		}
		api, err := client.New("http://127.0.0.1:"+cfg.ServerPort, client.WithServiceToken(serviceToken(cfg)))
		if err != nil {
			return nil, fmt.Errorf("offline service client: %w", err)
		}
		cleaner.offline = api
		return cleaner, nil
	}
	pg, err := pgstore.NewClient(ctx, cfg.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("postgres connect: %w", err)
	}
	pg.SetVectorGeneration(cfg.ActiveVectorGeneration())
	migrationCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	migrationErr := pg.RunMigrations(migrationCtx)
	cancel()
	if migrationErr != nil {
		return nil, errors.Join(fmt.Errorf("prepare cleanup schema: %w", migrationErr), pg.Close(ctx))
	}
	cleaner.metadata = pg
	cleaner.targets = Targets{SourceBucket: cfg.S3Bucket}
	var host string
	if cfg.VectorStore != "postgres" {
		cleaner.vectors, err = pcstore.NewPineconeClient(ctx, cfg)
		if err != nil {
			return nil, errors.Join(fmt.Errorf("pinecone connect: %w", err), pg.Close(ctx))
		}
		cleaner.targets.VectorIndex = cfg.PineconeIndexName
		host = cleaner.vectors.Host()
	}
	store, target := cfg.VectorTarget()
	if cfg.VectorStore == "postgres" {
		err = pg.PinCleanupVectorGeneration(ctx, cfg.ActiveVectorGeneration(), store, target, host, cfg.EmbeddingSettings())
	} else {
		err = pg.PinExistingVectorGeneration(ctx, cfg.ActiveVectorGeneration(), store, target, host, cfg.EmbeddingSettings())
	}
	if err != nil {
		return nil, errors.Join(fmt.Errorf("pin the cleanup target: %w", err), cleaner.Close(ctx))
	}
	if cfg.S3Bucket != "" {
		cleaner.objects, err = s3store.New(ctx, cfg)
		if err != nil {
			return nil, errors.Join(fmt.Errorf("s3 connect: %w", err), cleaner.Close(ctx))
		}
	}
	return cleaner, nil
}

// serviceToken returns the first configured service credential.
func serviceToken(cfg *config.Config) string {
	if cfg.ServiceToken != "" {
		return cfg.ServiceToken
	}
	for _, token := range cfg.ServiceTokens {
		if token != "" {
			return token
		}
	}
	return ""
}

// Delete removes one stored document and its external data. When the database
// selected checkpointed deletion, a live document needs DeleteIncarnation.
func (c *Cleaner) Delete(ctx context.Context, docID string) error {
	return c.delete(ctx, docID, "", false)
}

// DeleteIncarnation removes the document lifetime identified by incarnation
// through the checkpointed coordinator. It never falls back to an ID-only
// request: an empty incarnation is invalid, and a database that has not selected
// checkpointed deletion refuses the request.
func (c *Cleaner) DeleteIncarnation(ctx context.Context, docID, incarnation string) error {
	return c.delete(ctx, docID, incarnation, true)
}

func (c *Cleaner) delete(ctx context.Context, docID, incarnation string, qualified bool) error {
	if qualified && incarnation == "" {
		return domain.ErrDeletionPreconditionRequired
	}
	if c.offline != nil {
		return c.deleteOffline(ctx, docID, incarnation, qualified)
	}
	release, err := c.metadata.AcquireWriter(ctx)
	if err != nil {
		if errors.Is(err, domain.ErrWritersFrozen) || errors.Is(err, domain.ErrGenerationMismatch) {
			return fmt.Errorf("deletions are paused: %w", err)
		}
		return fmt.Errorf("check document writers: %w", err)
	}
	defer release()
	var vectors domain.VectorDeleter
	if c.vectors != nil {
		vectors = c.vectors
	}
	id := domain.DocumentID(docID)
	if !qualified && c.reserve {
		// The activation lock decides the contract, so an activation that wins the
		// race continues as an activated ID-only request.
		err = RemoveLegacy(ctx, c.metadata, vectors, c.objects, c.targets, id)
		if !errors.Is(err, domain.ErrDeletionActivated) {
			return err
		}
	}
	active, err := c.metadata.DeletionActive(ctx)
	if err != nil {
		return err
	}
	if active {
		return RemoveIncarnation(ctx, c.metadata, vectors, c.objects, c.targets, id, incarnation)
	}
	if qualified {
		return domain.ErrDeletionInactive
	}
	doc, err := c.metadata.GetDocument(ctx, id)
	if err != nil {
		return err
	}
	if doc == nil {
		return fmt.Errorf("%w: %s", ErrDocumentNotFound, docID)
	}
	return Remove(ctx, c.metadata, vectors, c.objects, doc)
}

// deleteOffline sends the request to the service that owns the vectors. It
// sends the saved incarnation once and never looks up a current one.
func (c *Cleaner) deleteOffline(ctx context.Context, docID, incarnation string, qualified bool) error {
	var err error
	if qualified {
		_, err = c.offline.DeleteDocumentIncarnation(ctx, docID, incarnation)
	} else {
		_, err = c.offline.DeleteDocument(ctx, docID)
	}
	var apiErr *client.APIError
	if errors.As(err, &apiErr) && apiErr.StatusCode == http.StatusNotFound {
		return fmt.Errorf("%w: %s", ErrDocumentNotFound, docID)
	}
	return err
}

// Close releases the shared store connections.
func (c *Cleaner) Close(ctx context.Context) error {
	if c.metadata == nil {
		return nil
	}
	var vectorErr error
	if c.vectors != nil {
		vectorErr = c.vectors.Close()
	}
	return errors.Join(vectorErr, c.metadata.Close(ctx))
}
