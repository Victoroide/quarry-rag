package main

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func syntheticFile(t *testing.T, path string) {
	t.Helper()
	require.NoError(t, os.WriteFile(path, []byte("synthetic file content"), 0o600))
}

func TestOpenUploadConfinement(t *testing.T) {
	parent := t.TempDir()
	root := filepath.Join(parent, "allowed")
	require.NoError(t, os.Mkdir(root, 0o700))
	file := filepath.Join(root, "sample.MD")
	syntheticFile(t, file)
	sibling := filepath.Join(parent, "allowed-other")
	require.NoError(t, os.Mkdir(sibling, 0o700))
	escape := filepath.Join(sibling, "sample.md")
	syntheticFile(t, escape)
	link := filepath.Join(root, "escape.md")
	require.NoError(t, os.Symlink(escape, link))
	unknown := filepath.Join(root, "sample.pem")
	syntheticFile(t, unknown)
	disguised := filepath.Join(root, "disguised.md")
	require.NoError(t, os.Symlink(unknown, disguised))
	for _, path := range []string{escape, root, unknown, disguised, link, filepath.Join(root, "..", "allowed-other", "sample.md"), filepath.Join(root, "missing.md")} {
		f, err := openUpload(path, []string{root})
		if f != nil {
			require.NoError(t, f.Close())
		}
		require.Error(t, err, "path %s", path)
	}
	f, err := openUpload(file, []string{root})
	require.NoError(t, err)
	data, err := io.ReadAll(f)
	require.NoError(t, err)
	require.NoError(t, f.Close())
	assert.Equal(t, "synthetic file content", string(data))
	_, err = openUpload(file, nil)
	require.Error(t, err)
	rootLink := filepath.Join(parent, "root-link")
	require.NoError(t, os.Symlink(root, rootLink))
	f, err = openUpload(filepath.Join(rootLink, "sample.MD"), []string{rootLink})
	require.NoError(t, err)
	require.NoError(t, f.Close())
}

func TestOpenUploadExtensions(t *testing.T) {
	root := t.TempDir()
	for _, extension := range []string{".pdf", ".docx", ".doc", ".txt", ".md", ".markdown"} {
		path := filepath.Join(root, "sample"+extension)
		syntheticFile(t, path)
		f, err := openUpload(path, []string{root})
		require.NoError(t, err)
		require.NoError(t, f.Close())
	}
}
