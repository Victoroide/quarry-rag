package integration

import (
	"context"
	"errors"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/store/memory"
)

type delayedVectorOperation string

const (
	startupCleanup  delayedVectorOperation = "startup cleanup"
	upsert          delayedVectorOperation = "upsert"
	terminalCleanup delayedVectorOperation = "terminal cleanup"
)

type delayedVectorMode string

const (
	lateEffect     delayedVectorMode = "late effect"
	canceledEffect delayedVectorMode = "canceled effect"
)

type revisionVectorIdentity struct {
	documentID domain.DocumentID
	revision   string
	attempt    string
}

func chunkVectorIdentity(chunk domain.Chunk) revisionVectorIdentity {
	revision, _ := chunk.Metadata[domain.VectorRevisionKey].(string)
	attempt, _ := chunk.Metadata[domain.VectorAttemptKey].(string)
	return revisionVectorIdentity{documentID: chunk.DocumentID, revision: revision, attempt: attempt}
}

type delayedVectorCall struct {
	operation delayedVectorOperation
	identity  revisionVectorIdentity
}

type delayedVectorResult struct {
	call    delayedVectorCall
	invoked bool
	err     error
	cause   error
}

type delayedRevisionVectors struct {
	*memory.FakeVectorStore
	operation          delayedVectorOperation
	mode               delayedVectorMode
	entered            chan delayedVectorCall
	proceed            chan struct{}
	finished           chan delayedVectorResult
	dispatched         bool
	repeatedDispatch   bool
	deletes            int
	unqualifiedDeletes int
}

func (v *delayedRevisionVectors) delay(ctx context.Context, call delayedVectorCall, apply func() error) error {
	if v.dispatched {
		v.repeatedDispatch = true
		return errors.New("worker repeated a delayed vector dispatch")
	}
	v.dispatched = true
	v.entered <- call
	if v.mode == canceledEffect {
		<-ctx.Done()
		err := ctx.Err()
		v.finished <- delayedVectorResult{call: call, err: err, cause: context.Cause(ctx)}
		return err
	}
	// An accepted provider call can apply its effect after the lease is lost.
	<-v.proceed
	err := apply()
	v.finished <- delayedVectorResult{call: call, invoked: true, err: err}
	return err
}

func (v *delayedRevisionVectors) Upsert(ctx context.Context, ns string, chunks []domain.Chunk) error {
	if v.operation == upsert && len(chunks) > 0 {
		call := delayedVectorCall{operation: upsert, identity: chunkVectorIdentity(chunks[0])}
		return v.delay(ctx, call, func() error { return v.FakeVectorStore.Upsert(ctx, ns, chunks) })
	}
	return v.FakeVectorStore.Upsert(ctx, ns, chunks)
}

func (v *delayedRevisionVectors) DeleteByAttempt(ctx context.Context, id, revision, attempt string) error {
	v.deletes++
	if (v.operation == startupCleanup && v.deletes == 1) || (v.operation == terminalCleanup && v.deletes == 2) {
		call := delayedVectorCall{
			operation: v.operation,
			identity:  revisionVectorIdentity{documentID: domain.DocumentID(id), revision: revision, attempt: attempt},
		}
		return v.delay(ctx, call, func() error { return v.FakeVectorStore.DeleteByAttempt(ctx, id, revision, attempt) })
	}
	return v.FakeVectorStore.DeleteByAttempt(ctx, id, revision, attempt)
}

func (v *delayedRevisionVectors) DeleteByDocID(context.Context, string) error {
	v.unqualifiedDeletes++
	return errors.New("tagged worker attempted an unqualified cleanup")
}
