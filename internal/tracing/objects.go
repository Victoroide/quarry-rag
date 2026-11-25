package tracing

import (
	"context"
	"io"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/extractor"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// ObjectStore stores and removes original files.
type ObjectStore interface {
	Upload(context.Context, string, string, string, extractor.FileType, io.Reader) (string, error)
	Delete(context.Context, string) error
}

// SourceObjectStore is an ObjectStore that writes under caller-allocated keys.
type SourceObjectStore interface {
	ObjectStore
	Bucket() string
	NewKey(documentID, uploadID, docType, fileName string) string
	PutSource(context.Context, string, extractor.FileType, io.Reader) (domain.SourceUploadResult, error)
}

type objectStore struct{ inner ObjectStore }

type sourceObjectStore struct {
	objectStore
	source SourceObjectStore
}

// WrapObjectStore traces uploads and deletions without recording file contents.
// The result also writes caller-allocated keys when the inner store does.
func WrapObjectStore(inner ObjectStore) ObjectStore {
	if source, ok := inner.(SourceObjectStore); ok {
		return &sourceObjectStore{objectStore{inner}, source}
	}
	return &objectStore{inner}
}

func (s *sourceObjectStore) Bucket() string { return s.source.Bucket() }

func (s *sourceObjectStore) NewKey(documentID, uploadID, docType, fileName string) string {
	return s.source.NewKey(documentID, uploadID, docType, fileName)
}

func (s *sourceObjectStore) PutSource(ctx context.Context, key string, fileType extractor.FileType, reader io.Reader) (domain.SourceUploadResult, error) {
	ctx, span := start(ctx, "storage.put", "tool", trace.SpanKindClient, attribute.String("quarry.storage.system", "s3"))
	defer span.End()
	result, err := s.source.PutSource(ctx, key, fileType, reader)
	RecordError(span, err)
	return result, err
}

func (s *objectStore) Upload(ctx context.Context, documentID, docType, name string, fileType extractor.FileType, reader io.Reader) (string, error) {
	ctx, span := start(ctx, "storage.upload", "tool", trace.SpanKindClient, attribute.String("quarry.storage.system", "s3"))
	defer span.End()
	key, err := s.inner.Upload(ctx, documentID, docType, name, fileType, reader)
	RecordError(span, err)
	return key, err
}

func (s *objectStore) Delete(ctx context.Context, key string) error {
	ctx, span := start(ctx, "storage.delete", "tool", trace.SpanKindClient, attribute.String("quarry.storage.system", "s3"))
	defer span.End()
	err := s.inner.Delete(ctx, key)
	RecordError(span, err)
	return err
}
