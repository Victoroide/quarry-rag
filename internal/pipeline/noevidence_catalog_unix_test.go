//go:build unix

package pipeline

import (
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLoadNoEvidenceCatalogRejectsFIFOWithoutBlocking(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.json")
	require.NoError(t, syscall.Mkfifo(path, 0o600))
	done := make(chan error, 1)
	go func() {
		_, err := LoadNoEvidenceCatalog(path)
		done <- err
	}()
	select {
	case err := <-done:
		require.ErrorContains(t, err, "regular file")
	case <-time.After(2 * time.Second):
		t.Fatal("opening a catalog FIFO blocked")
	}
}
