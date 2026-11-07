//go:build unix

package main

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestOpenUploadRejectsFIFOWithoutBlocking(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "pipe.txt")
	require.NoError(t, syscall.Mkfifo(path, 0o600))
	directory, err := os.OpenRoot(root)
	require.NoError(t, err)
	defer func() { require.NoError(t, directory.Close()) }()
	done := make(chan error, 1)
	go func() {
		f, err := openUpload(path, []string{root})
		if f != nil {
			_ = f.Close()
		}
		done <- err
	}()
	select {
	case err := <-done:
		require.Error(t, err)
	case <-time.After(200 * time.Millisecond):
		f, err := directory.OpenFile("pipe.txt", os.O_RDWR, 0o600)
		require.NoError(t, err)
		require.Error(t, <-done)
		require.NoError(t, f.Close())
		t.Fatal("opening a non-regular file blocked")
	}
}
