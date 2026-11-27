package postgres

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
)

const (
	writerEvidenceTimeout  = time.Second
	writerEvidenceCapacity = 16
)

// All operations in the process share the same write lane, including records
// with different correlation fields. The service logger has its own level.
var writerRecoveryEvidence = newWriterRecoveryEvidence(retainedWriterStderr{})

var openWriterRecoveryStderr = sync.OnceValues(func() (*os.File, error) {
	stream, err := os.Stderr.Stat()
	if err != nil {
		return nil, err
	}
	if discard, statErr := os.Stat(os.DevNull); statErr == nil && os.SameFile(stream, discard) {
		return nil, domain.ErrWriterEvidenceUnavailable
	}
	return writerEvidenceOutput(os.Stderr)
})

type retainedWriterStderr struct{}

func (retainedWriterStderr) Write(data []byte) (int, error) {
	// Resolve the descriptor inside the bounded write lane as well.
	output, err := openWriterRecoveryStderr()
	if err != nil {
		return 0, err
	}
	return output.Write(data)
}

type writerEvidenceStream struct {
	handler slog.Handler
	waiters chan struct{}
	writing chan struct{}
}

func newWriterRecoveryEvidence(output io.Writer) *writerEvidenceStream {
	return &writerEvidenceStream{
		handler: slog.NewJSONHandler(&completeEvidenceWrite{output: output}, nil).
			WithAttrs([]slog.Attr{slog.String("stream", "writer_recovery")}),
		waiters: make(chan struct{}, writerEvidenceCapacity),
		writing: make(chan struct{}, 1),
	}
}

func (s *writerEvidenceStream) withAttrs(attrs []slog.Attr) *writerEvidenceStream {
	return &writerEvidenceStream{s.handler.WithAttrs(attrs), s.waiters, s.writing}
}

type completeEvidenceWrite struct {
	output        io.Writer
	needsBoundary bool
}

func (w *completeEvidenceWrite) Write(data []byte) (int, error) {
	if w.needsBoundary {
		// Keep the next complete JSON record separate from an interrupted line.
		n, err := io.WriteString(w.output, "\n")
		if err != nil {
			return 0, err
		}
		if n != 1 {
			return 0, io.ErrShortWrite
		}
		w.needsBoundary = false
	}
	n, err := w.output.Write(data)
	w.needsBoundary = n > 0 && n < len(data)
	if err == nil && n != len(data) {
		err = io.ErrShortWrite
	}
	return n, err
}

func writeWriterEvidence(ctx context.Context, sink *writerEvidenceStream, message string, fields ...any) error {
	if sink == nil {
		domain.NotifyWriterEvidenceFailure(ctx)
		return domain.ErrWriterEvidenceUnavailable
	}
	record := slog.NewRecord(time.Now(), slog.LevelInfo, message, 0)
	record.Add(fields...)
	if err := sink.write(ctx, record); err != nil {
		domain.NotifyWriterEvidenceFailure(ctx)
		return fmt.Errorf("%w: %w", domain.ErrWriterEvidenceUnavailable, err)
	}
	return nil
}

func (s *writerEvidenceStream) write(ctx context.Context, record slog.Record) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case s.waiters <- struct{}{}:
	default:
		return domain.ErrWriterEvidenceUnavailable
	}
	defer func() { <-s.waiters }()
	ctx, cancel := context.WithTimeout(ctx, writerEvidenceTimeout)
	defer cancel()
	select {
	case s.writing <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		<-s.writing
		return err
	}
	// A writer that ignores cancellation can outlive the caller. Keep it in
	// the lane until it returns so a stalled collector cannot accumulate writes.
	result := make(chan error, 1)
	go func() {
		err := s.handler.Handle(ctx, record)
		<-s.writing
		result <- err
	}()
	select {
	case err := <-result:
		if err != nil {
			return err
		}
		return ctx.Err()
	case <-ctx.Done():
		return ctx.Err()
	}
}
