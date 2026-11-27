//go:build unix

package postgres

import (
	"context"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWriterEvidenceReportsABrokenStderrPipe(t *testing.T) {
	const child = "QUARRY_TEST_WRITER_EVIDENCE_BROKEN_STDERR"
	if os.Getenv(child) == "1" {
		err := writeWriterEvidence(t.Context(), writerRecoveryEvidence, "broken stderr")
		require.ErrorIs(t, err, syscall.EPIPE)
		return
	}
	reader, writer, err := os.Pipe()
	require.NoError(t, err)
	t.Cleanup(func() { _ = writer.Close() })
	require.NoError(t, reader.Close())
	executable, err := os.Executable()
	require.NoError(t, err)
	watchdog, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	childProcess, err := os.StartProcess(executable,
		[]string{executable, "-test.run=^TestWriterEvidenceReportsABrokenStderrPipe$"},
		&os.ProcAttr{Env: append(os.Environ(), child+"=1"), Files: []*os.File{os.Stdin, os.Stdout, writer}},
	)
	require.NoError(t, err)
	stopChild := context.AfterFunc(watchdog, func() { _ = childProcess.Kill() })
	defer stopChild()
	state, err := childProcess.Wait()
	require.NoError(t, err)
	require.NoError(t, watchdog.Err())
	require.True(t, state.Success(), "%s", state)
}
