//go:build unix

package subjectauth

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/credential"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func makeTrustFileGroupWritable(t *testing.T, path string) {
	t.Helper()
	addTrustFilePermissions(t, path, 0o020)
}

func addTrustFilePermissions(t *testing.T, path string, mode os.FileMode) {
	t.Helper()
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.NoError(t, os.Chmod(path, info.Mode().Perm()|mode))
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

func TestTrustReadLimitsAndRecoveryUnix(t *testing.T) {
	verifier, _, path := newFixtureVerifier(t)
	original, err := credential.ReadFile(path, maxTrustBytes, 0o022)
	require.NoError(t, err)
	require.NoError(t, verifier.ValidateTrust())

	for _, tc := range []struct {
		name    string
		replace func(*testing.T)
	}{
		{"group writable", func(t *testing.T) { makeTrustFileGroupWritable(t, path) }},
		{"world writable", func(t *testing.T) { addTrustFilePermissions(t, path, 0o002) }},
		{"fifo", func(t *testing.T) {
			require.NoError(t, os.Remove(path))
			require.NoError(t, unix.Mkfifo(path, 0o600))
		}},
		{"outside symlink", func(t *testing.T) {
			target := filepath.Join(t.TempDir(), "outside.json")
			replaceTrust(t, target, original)
			relative, err := filepath.Rel(filepath.Dir(path), target)
			require.NoError(t, err)
			require.NoError(t, os.Remove(path))
			require.NoError(t, os.Symlink(relative, path))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.replace(t)
			require.ErrorIs(t, awaitTrustResult(t, startTrustCall(verifier.ValidateTrust)), ErrAuthorityUnavailable)
			require.NoError(t, os.Remove(path))
			replaceTrust(t, path, original)
			require.NoError(t, verifier.ValidateTrust())
		})
	}

	contained := filepath.Join(filepath.Dir(path), "contained.json")
	replaceTrust(t, contained, original)
	require.NoError(t, os.Remove(path))
	require.NoError(t, os.Symlink(filepath.Base(contained), path))
	require.NoError(t, verifier.ValidateTrust())
}
