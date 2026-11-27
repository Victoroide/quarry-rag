package postgres

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/stretchr/testify/require"
)

type recoveryPipe struct {
	pipe    *io.PipeWriter
	started chan struct{}
	skip    int
}

func (p *recoveryPipe) Write(data []byte) (int, error) {
	if p.skip > 0 {
		p.skip--
		return len(data), nil
	}
	select {
	case p.started <- struct{}{}:
	default:
	}
	return p.pipe.Write(data)
}

func newRecoveryPipe(t *testing.T, skip int) (*io.PipeReader, *recoveryPipe) {
	t.Helper()
	reader, writer := io.Pipe()
	t.Cleanup(func() {
		_ = reader.Close()
		_ = writer.Close()
	})
	return reader, &recoveryPipe{pipe: writer, started: make(chan struct{}, 1), skip: skip}
}

type shortRecoveryOutput struct{}

func (shortRecoveryOutput) Write(data []byte) (int, error) { return len(data) - 1, nil }

func TestWriterEvidenceRejectsIncompleteWrites(t *testing.T) {
	err := writeWriterEvidence(t.Context(), newWriterRecoveryEvidence(shortRecoveryOutput{}), "short write")
	require.ErrorIs(t, err, io.ErrShortWrite)
	require.ErrorIs(t, err, domain.ErrWriterEvidenceUnavailable)

	err = writeWriterEvidence(t.Context(), newWriterRecoveryEvidence(failedRecoveryOutput{}), "broken pipe")
	require.ErrorIs(t, err, io.ErrClosedPipe)
	require.ErrorIs(t, err, domain.ErrWriterEvidenceUnavailable)
}

type interruptedEvidenceOutput struct {
	bytes.Buffer
	short bool
}

func (w *interruptedEvidenceOutput) Write(data []byte) (int, error) {
	if w.short {
		w.short = false
		return w.Buffer.Write(data[:len(data)/2])
	}
	return w.Buffer.Write(data)
}

func TestWriterEvidenceSeparatesRecordsAfterAPartialWrite(t *testing.T) {
	output := &interruptedEvidenceOutput{short: true}
	sink := newWriterRecoveryEvidence(output)
	require.ErrorIs(t, writeWriterEvidence(t.Context(), sink, "interrupted record"), io.ErrShortWrite)
	require.NoError(t, writeWriterEvidence(t.Context(), sink, "complete record"))
	lines := bytes.Split(bytes.TrimSpace(output.Bytes()), []byte("\n"))
	require.Len(t, lines, 2)
	var recovered map[string]any
	require.NoError(t, json.Unmarshal(lines[1], &recovered))
	require.Equal(t, "complete record", recovered["msg"])
}

func TestWriterEvidenceBoundsAnUnreadRecoveryStream(t *testing.T) {
	reader, output := newRecoveryPipe(t, 0)
	sink := newWriterRecoveryEvidence(output)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	writeCtx, cancelWrite := context.WithCancel(ctx)
	defer cancelWrite()
	done := make(chan error, 1)
	go func() { done <- writeWriterEvidence(writeCtx, sink, "canceled record") }()
	select {
	case <-output.started:
	case <-ctx.Done():
		t.Fatal("the sink did not enter its write")
	}
	cancelWrite()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-ctx.Done():
		t.Fatal("cancellation could not release an unread stream")
	}
	require.Len(t, sink.writing, 1, "the timed-out write still owns the lane")

	// A second record shares the blocked lane even with different attributes.
	other := sink.withAttrs([]slog.Attr{slog.String("writer_operation", "second")})
	err := writeWriterEvidence(ctx, other, "concurrent record")
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.NoError(t, ctx.Err())
	require.Empty(t, output.started, "a second physical write started on the stalled stream")

	readDone := make(chan error, 1)
	go func() {
		_, err := io.Copy(io.Discard, reader)
		readDone <- err
	}()
	require.NoError(t, writeWriterEvidence(ctx, other, "recovered record"))
	require.NoError(t, output.pipe.Close())
	require.NoError(t, <-readDone)
}

func TestWriterEvidenceRejectsSaturation(t *testing.T) {
	_, output := newRecoveryPipe(t, 0)
	sink := newWriterRecoveryEvidence(output)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	// Occupy the write lane while the concurrent calls wait to enter it.
	sink.writing <- struct{}{}
	done := make(chan error, writerEvidenceCapacity)
	for range writerEvidenceCapacity {
		go func() { done <- writeWriterEvidence(ctx, sink, "waiting record") }()
	}
	require.Eventually(t, func() bool { return len(sink.waiters) == writerEvidenceCapacity },
		writerEvidenceTimeout/2, time.Millisecond)
	err := writeWriterEvidence(ctx, sink, "excess record")
	require.ErrorIs(t, err, domain.ErrWriterEvidenceUnavailable)
	require.NotErrorIs(t, err, context.DeadlineExceeded)
	cancel()
	for range writerEvidenceCapacity {
		require.ErrorIs(t, <-done, context.Canceled)
	}
	<-sink.writing
}

func TestWriterOperationBoundsRecoveryEvidence(t *testing.T) {
	c, _ := sessionTestClientWithPool(t, 1)
	grantExec(t, c, `UPDATE vector_state SET writer_operations_enabled=TRUE`)
	for _, test := range []struct {
		name   string
		stage  int
		cancel bool
	}{
		{name: "enrollment deadline"},
		{name: "enrollment cancel", cancel: true},
		{name: "provider start deadline", stage: 1},
		{name: "provider result deadline", stage: 2},
		{name: "provider start cancel", stage: 1, cancel: true},
		{name: "provider result cancel", stage: 2, cancel: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, output := newRecoveryPipe(t, test.stage)
			c.writerEvidence = newWriterRecoveryEvidence(output)
			watchdog, stop := context.WithTimeout(t.Context(), 10*time.Second)
			defer stop()
			ctx, cancel := context.WithCancel(watchdog)
			defer cancel()
			var callbacks, sends int
			done := make(chan error, 1)
			go func() {
				done <- c.RunWriterOperation(ctx, domain.WriterOperationReconcile, "scoped", func(ctx context.Context) error {
					callbacks++
					return domain.DispatchExternalWriter(ctx, func(ctx context.Context) (bool, error) {
						evidence := domain.ExternalWriterEvidence{Provider: "s3", Namespace: "source", Outcome: "started"}
						if err := domain.RecordExternalWriterEvidence(ctx, evidence); err != nil {
							return true, err
						}
						sends++
						evidence.Outcome = "acknowledged"
						err := domain.RecordExternalWriterEvidence(ctx, evidence)
						return err == nil, err
					})
				})
			}()
			select {
			case <-output.started:
			case err := <-done:
				t.Fatalf("operation returned before reaching the stream: %v", err)
			case <-watchdog.Done():
				t.Fatal(watchdog.Err())
			}
			if test.cancel {
				cancel()
			}
			select {
			case err := <-done:
				require.ErrorIs(t, err, domain.ErrWriterEvidenceUnavailable)
				if test.cancel {
					require.ErrorIs(t, err, context.Canceled)
				} else {
					require.ErrorIs(t, err, context.DeadlineExceeded)
				}
			case <-watchdog.Done():
				t.Fatal("operation held for the stalled recovery stream")
			}

			err := c.FreezeWriters(watchdog, "next")
			switch test.stage {
			case 0:
				require.Zero(t, callbacks)
				require.Zero(t, sends)
				require.NoError(t, err, "failed enrollment must release both row locks and leave no journal entry")
				require.NoError(t, c.ThawWriters(watchdog))
			case 1:
				require.Equal(t, 1, callbacks)
				require.Zero(t, sends)
				require.NoError(t, err, "an attempt with no send can settle")
				require.NoError(t, c.ThawWriters(watchdog))
			case 2:
				require.Equal(t, 1, callbacks)
				require.Equal(t, 1, sends)
				var pending *ErrWriterOperationsPending
				require.ErrorAs(t, err, &pending, "the reservation must survive a result record timeout")
				require.Len(t, pending.OperationIDs, 1)
				grantExec(t, c, `DELETE FROM vector_writer_operations`)
			}
		})
	}
}
