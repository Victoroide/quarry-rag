//go:build unix

package main

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/client"
	"github.com/stretchr/testify/require"
)

func TestSubjectFilePermissions(t *testing.T) {
	path := filepath.Join(t.TempDir(), "subject-token")
	valid := subjectFixture(time.Now().Add(time.Minute), 1)
	replaceSubjectFile(t, path, valid, 0o600)
	_, err := subjectTokenFile(path)(t.Context())
	require.NoError(t, err)

	replaceSubjectFile(t, path, valid, 0o644)
	_, err = subjectTokenFile(path)(t.Context())
	require.ErrorIs(t, err, client.ErrSubjectCredential)
	require.NotContains(t, err.Error(), path)
}
