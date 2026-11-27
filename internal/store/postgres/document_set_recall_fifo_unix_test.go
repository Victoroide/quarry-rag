//go:build unix

package postgres

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestRecallInputFIFO(t *testing.T) {
	const (
		childCase = "QUARRY_TEST_RECALL_FIFO_CASE"
		input     = "input.jsonl"
		pipe      = "pipe"
		pipeLink  = "pipe-link"
	)
	if scenario := os.Getenv(childCase); scenario != "" {
		_, err := readRecallFileWithOpener(t.Context(), input, 1,
			func(_ int, _ [32]byte, _ []float32) error {
				t.Fatal("FIFO reached the parser callback")
				return nil
			},
			func(root *os.Root, name string) (*os.File, error) {
				switch scenario {
				case "initial":
					t.Fatal("the opener was called for an initial FIFO")
				case "replacement":
					require.NoError(t, root.Rename(pipe, name))
				case "relative symlink replacement":
					require.NoError(t, root.Rename(pipeLink, name))
				default:
					t.Fatalf("unknown FIFO scenario %q", scenario)
				}
				return openRecallInput(root, name)
			},
		)
		require.EqualError(t, err, "recall input must be a readable regular file")
		return
	}

	executable, err := os.Executable()
	require.NoError(t, err)
	for _, scenario := range []string{"initial", "replacement", "relative symlink replacement"} {
		t.Run(scenario, func(t *testing.T) {
			dir := t.TempDir()
			require.NoError(t, unix.Mkfifo(filepath.Join(dir, pipe), 0o600))
			if scenario == "initial" {
				require.NoError(t, os.Rename(filepath.Join(dir, pipe), filepath.Join(dir, input)))
			} else {
				content := recallTestHeader() + "\n" + recallTestRecord("example")
				require.NoError(t, os.WriteFile(filepath.Join(dir, input), []byte(content), 0o600))
			}
			if scenario == "relative symlink replacement" {
				require.NoError(t, os.Symlink(pipe, filepath.Join(dir, pipeLink)))
			}

			watchdog, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			cmd := exec.CommandContext(watchdog, executable, "-test.run=^TestRecallInputFIFO$")
			cmd.Dir = dir
			cmd.Env = append(cmd.Environ(), childCase+"="+scenario)
			output, err := cmd.CombinedOutput()
			require.NoError(t, watchdog.Err(), "FIFO check exceeded its watchdog: %s", output)
			require.NoError(t, err, "%s", output)
		})
	}
}
