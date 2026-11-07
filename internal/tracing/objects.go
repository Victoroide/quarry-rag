package tracing

import (
	"context"
	"io"

	"github.com/Victoroide/quarry-rag/internal/extractor"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// ObjectStore stores and removes original files.
type ObjectStore interface {
	Upload(context.Context, string, string, string, extractor.FileType, io.Reader) (string, error)
	Delete(context.Context, string) error
}

type objectStore struct{ inner ObjectStore }

// WrapObjectStore traces uploads and deletions without recording file contents.
func WrapObjectStore(inner ObjectStore) ObjectStore { return &objectStore{inner} }

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
