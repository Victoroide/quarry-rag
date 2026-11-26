// Package cleanup removes documents and their stored data.
package cleanup

import (
	"context"
	"errors"
	"fmt"
	"time"

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
}

// operations adapts the configured stores to the coordinator's external steps.
// A transactional vector store has no external step: the metadata removal
// cascades to its rows.
func operations(vectors domain.VectorDeleter, objects ObjectDeleter, targets Targets) domain.DeleteOperations {
	ops := domain.DeleteOperations{InMemory: targets.InMemory, SourceBucket: targets.SourceBucket, VectorIndex: targets.VectorIndex}
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

// Cleaner reuses store connections for a sequence of document deletions.
type Cleaner struct {
	metadata *pgstore.Client
	vectors  *pcstore.Client
	objects  ObjectDeleter
	targets  Targets
}

// New connects the stores required for document deletion.
func New(ctx context.Context, cfg *config.Config) (*Cleaner, error) {
	pg, err := pgstore.NewClient(ctx, cfg.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("postgres connect: %w", err)
	}
	migrationCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	migrationErr := pg.RunMigrations(migrationCtx)
	cancel()
	if migrationErr != nil {
		return nil, errors.Join(fmt.Errorf("prepare cleanup schema: %w", migrationErr), pg.Close(ctx))
	}
	cleaner := &Cleaner{metadata: pg, targets: Targets{SourceBucket: cfg.S3Bucket}}
	if cfg.VectorStore != "postgres" {
		cleaner.vectors, err = pcstore.NewPineconeClient(ctx, cfg)
		if err != nil {
			return nil, errors.Join(fmt.Errorf("pinecone connect: %w", err), pg.Close(ctx))
		}
		cleaner.targets.VectorIndex = cfg.PineconeIndexName
	}
	if cfg.S3Bucket != "" {
		cleaner.objects, err = s3store.New(ctx, cfg)
		if err != nil {
			return nil, errors.Join(fmt.Errorf("s3 connect: %w", err), cleaner.Close(ctx))
		}
	}
	return cleaner, nil
}

// Delete removes one stored document and its external data. When the database
// selected checkpointed deletion, a live document needs DeleteIncarnation.
func (c *Cleaner) Delete(ctx context.Context, docID string) error {
	return c.DeleteIncarnation(ctx, docID, "")
}

// DeleteIncarnation removes the document lifetime identified by incarnation
// through the checkpointed coordinator. An empty incarnation takes the
// compatibility path until the database selects checkpointed deletion.
func (c *Cleaner) DeleteIncarnation(ctx context.Context, docID, incarnation string) error {
	active, err := c.metadata.DeletionActive(ctx)
	if err != nil {
		return err
	}
	var vectors domain.VectorDeleter
	if c.vectors != nil {
		vectors = c.vectors
	}
	id := domain.DocumentID(docID)
	if active {
		return RemoveIncarnation(ctx, c.metadata, vectors, c.objects, c.targets, id, incarnation)
	}
	if incarnation != "" {
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

// Close releases the shared store connections.
func (c *Cleaner) Close(ctx context.Context) error {
	var vectorErr error
	if c.vectors != nil {
		vectorErr = c.vectors.Close()
	}
	return errors.Join(vectorErr, c.metadata.Close(ctx))
}
