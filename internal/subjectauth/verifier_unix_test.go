//go:build unix

package subjectauth

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func makeTrustFileGroupWritable(t *testing.T, path string) {
	t.Helper()
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.NoError(t, os.Chmod(path, info.Mode().Perm()|0o020))
}

func TestSubjectTrustFilePermissions(t *testing.T) {
	verifier, key, path := newFixtureVerifier(t)
	token := fixtureToken(t, key, fixtureHeader, fixtureClaims(t, nil))
	_, err := verifier.Verify(token)
	require.NoError(t, err)

	makeTrustFileGroupWritable(t, path)
	_, err = verifier.Verify(token)
	require.ErrorIs(t, err, ErrAuthorityUnavailable)
}

func TestTrustTransitionRejectsInsecureReread(t *testing.T) {
	testTrustTransitionInvalidReread(t, makeTrustFileGroupWritable)
}
