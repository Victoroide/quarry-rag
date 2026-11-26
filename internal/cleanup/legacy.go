package cleanup

import (
	"context"
	"fmt"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/google/uuid"
)

// RemoveLegacy deletes the live document stored under id for an ID-only request
// while the database has not selected checkpointed deletion. The activation lock
// stays shared through the last external call. Before the first call the store
// commits a reservation that survives the loss of that lock, so a replacement
// cannot be admitted while any effect of this cleanup is outstanding. It
// returns domain.ErrDeletionActivated, without acting, once the database has
// selected checkpointed deletion.
func RemoveLegacy(ctx context.Context, guard domain.LegacyDeletionGuard, vectors domain.VectorDeleter, objects ObjectDeleter, targets Targets, id domain.DocumentID) error {
	// External stores and receipts key the document by its canonical form.
	parsed, err := uuid.Parse(string(id))
	if err != nil {
		return domain.ErrDocumentNotFound
	}
	id = domain.DocumentID(parsed.String())
	return guard.WithLegacyDeletion(ctx, id, func(store domain.LegacyDocumentStore) error {
		return removeLegacy(ctx, store, vectors, objects, targets, id)
	})
}

// legacyVectors names the vector operations of one legacy cleanup.
type legacyVectors struct {
	deleter   domain.VectorDeleter
	revisions domain.RevisionVectorDeleter
}

func removeLegacy(ctx context.Context, store domain.LegacyDocumentStore, vectors domain.VectorDeleter, objects ObjectDeleter, targets Targets, id domain.DocumentID) error {
	if _, transactional := vectors.(domain.TransactionalVectorStore); transactional {
		// The metadata removal cascades to the rows on the same connection.
		vectors = nil
	}
	spec := domain.LegacyDeletionSpec{SourceAvailable: objects != nil, SourceBucket: targets.SourceBucket}
	var ops legacyVectors
	if vectors != nil {
		ops.deleter = vectors
		ops.revisions, spec.RevisionVectors = vectors.(domain.RevisionVectorDeleter)
		if targets.InMemory {
			if targets.Memory == nil {
				return domain.ErrVectorStorageUnavailable
			}
			// Only the process that owns the index mutates it, one cleanup at a time.
			unlock, ok := targets.Memory.LockDocument(string(id))
			if !ok {
				return domain.ErrDeletionBusy
			}
			defer unlock()
			spec.Memory, spec.MemoryOwner = true, targets.Memory.MemoryOwner()
		} else {
			spec.Hosted, spec.VectorIndex = true, targets.VectorIndex
		}
	}
	target, err := store.PrepareLegacyDeletion(ctx, id, spec)
	if err != nil {
		return err
	}
	run := &legacyRun{store: store, id: id, target: target}
	done := false
	defer func() {
		if !done {
			store.AbandonLegacyDeletion(ctx, id, target)
		}
	}()
	if sweepErr := run.sweep(ctx, ops, "before"); sweepErr != nil {
		return sweepErr
	}
	if target.SourceKey != nil {
		if deleteErr := objects.Delete(ctx, *target.SourceKey); deleteErr != nil {
			return fmt.Errorf("delete source file: %w", deleteErr)
		}
	}
	if err = store.RetireLegacyDocument(ctx, id, target); err != nil {
		return fmt.Errorf("delete document metadata: %w", err)
	}
	// A write that was in flight when the row went away is removed by the last sweep.
	if sweepErr := run.sweep(ctx, ops, "after"); sweepErr != nil {
		return sweepErr
	}
	if completeErr := store.CompleteLegacyDeletion(ctx, id, target, run.uncertain); completeErr != nil {
		return completeErr
	}
	done = true
	return nil
}

// legacyRun carries what a cleanup has learned about its external effects.
type legacyRun struct {
	store     domain.LegacyDocumentStore
	id        domain.DocumentID
	target    domain.LegacyDeletionTarget
	uncertain bool
}

// sweep removes the vectors of the observed lifetime. Untagged vectors are
// matched by document and the absence of a revision; tagged ones by the exact
// incarnation. A provider's own retries stay enabled, and any attempt that may
// have reached it keeps the identity held although a later attempt succeeds.
func (r *legacyRun) sweep(ctx context.Context, ops legacyVectors, phase string) error {
	if ops.deleter == nil {
		return nil
	}
	docID := string(r.id)
	if r.target.ScopedVectors {
		return r.call(ctx, phase+" revision vectors", func(ctx context.Context) error {
			return ignoreMissing(ops.revisions.DeleteByRevision(ctx, docID, r.target.Incarnation))
		})
	}
	if phase == "before" {
		if err := r.call(ctx, "delete document namespace", func(ctx context.Context) error {
			return ignoreMissing(ops.deleter.DeleteAllVectorsInNamespace(ctx, docID))
		}); err != nil {
			return err
		}
	}
	return r.call(ctx, phase+" document vectors", func(ctx context.Context) error {
		return ignoreMissing(ops.deleter.DeleteByDocID(ctx, docID))
	})
}

func (r *legacyRun) call(ctx context.Context, operation string, fn func(context.Context) error) error {
	observed, outcome := domain.ObserveVectorOutcome(ctx)
	err := fn(observed)
	hidden := outcome.Uncertain() || (err != nil && domain.VectorMutationUncertain(err))
	if hidden {
		r.uncertain = true
		if noteErr := r.store.NoteLegacyUncertain(ctx, r.id, r.target); noteErr != nil && err == nil {
			return fmt.Errorf("%s: record uncertain attempt: %w", operation, noteErr)
		}
	}
	if err != nil {
		return fmt.Errorf("%s: %w", operation, err)
	}
	return nil
}
