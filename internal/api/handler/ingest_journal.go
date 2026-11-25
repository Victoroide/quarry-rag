package handler

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/extractor"
	"github.com/google/uuid"
)

// SourceObjectStore writes originals under keys allocated before the write, so
// a journal can record the owner of an object before the object exists.
type SourceObjectStore interface {
	ObjectStore
	Bucket() string
	NewKey(documentID, uploadID, docType, fileName string) string
	PutSource(context.Context, string, extractor.FileType, io.Reader) (domain.SourceUploadResult, error)
}

// WithSourceUploadJournal records each original's owner in durable storage
// before the object write, so an upload that is never accepted can be reclaimed.
func WithSourceUploadJournal(journal domain.SourceUploadJournal) IngestOption {
	return func(opts *ingestOptions) { opts.journal = journal }
}

// sourceWrite describes a stored original that acceptance may adopt.
type sourceWrite struct {
	key  string
	link *domain.SourceUploadLink
}

type sourceWriter struct {
	objects ObjectStore
	journal domain.SourceUploadJournal
}

var errSourceJournalUnsupported = errors.New("object store cannot write journaled uploads")

// write stores one attempt's original under its own object key. With a journal
// the owner is durable before the object write; without one the write is a
// single unrecorded upload.
func (w sourceWriter) write(ctx context.Context, documentID, collection, docType, fileName string, ft extractor.FileType, data []byte) (sourceWrite, error) {
	if w.journal == nil {
		// Each attempt owns a distinct object so rollback cannot delete another acceptance's file.
		key, err := w.objects.Upload(ctx, documentID+"/"+uuid.NewString(), docType, fileName, ft, bytes.NewReader(data))
		return sourceWrite{key: key}, err
	}
	objects, ok := w.objects.(SourceObjectStore)
	if !ok {
		return sourceWrite{}, errSourceJournalUnsupported
	}
	uploadID := uuid.NewString()
	key := objects.NewKey(documentID, uploadID, docType, fileName)
	// Staging and all PUT attempts share the same deadline. A delayed staging
	// response must never start a new write after reclamation can settle the key.
	writeCtx, cancel := context.WithTimeout(ctx, domain.SourceUploadPutTimeout)
	defer cancel()
	if err := w.journal.StageSourceUpload(writeCtx, domain.SourceUpload{
		ID: uploadID, DocumentID: domain.DocumentID(documentID), Collection: collection, Bucket: objects.Bucket(), Key: key,
	}); err != nil {
		return sourceWrite{}, err
	}
	if err := writeCtx.Err(); err != nil {
		return sourceWrite{}, err
	}
	if err := w.journal.BeginSourceUpload(writeCtx, uploadID); err != nil {
		return sourceWrite{}, err
	}
	if err := writeCtx.Err(); err != nil {
		w.recordResult(ctx, uploadID, domain.SourceUploadResult{Settled: true})
		return sourceWrite{}, err
	}
	result, err := objects.PutSource(writeCtx, key, ft, bytes.NewReader(data))
	if result.Acknowledged || result.Settled {
		w.recordResult(ctx, uploadID, result)
	}
	if err != nil {
		return sourceWrite{}, err
	}
	return sourceWrite{key: key, link: &domain.SourceUploadLink{ID: uploadID, VersionID: result.VersionID}}, nil
}

func (w sourceWriter) recordResult(ctx context.Context, id string, result domain.SourceUploadResult) {
	resultCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	if err := w.journal.RecordSourceUploadResult(resultCtx, id, result); err != nil {
		slog.WarnContext(ctx, "record source upload result", "upload_id", id, "error", err)
	}
}
