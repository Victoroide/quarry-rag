package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEmbeddingInputProfileStartup(t *testing.T) {
	providerEnvironment(t)
	t.Setenv("EMBEDDING_INPUT_PROFILE", "")
	cfg, err := Load()
	require.NoError(t, err)
	require.Nil(t, cfg.EmbeddingInputProfile)

	path := filepath.Join(t.TempDir(), "embedding.json")
	require.NoError(t, os.WriteFile(path, []byte(`{"provider":"openai","model":"embed","revision":"r1","max_characters":6}`), 0o600))
	t.Setenv("EMBEDDING_INPUT_PROFILE", path)
	cfg, err = Load()
	require.NoError(t, err)
	require.NotNil(t, cfg.EmbeddingInputProfile)

	t.Setenv("OPENAI_EMBEDDING_MODEL", "other")
	_, err = Load()
	require.ErrorContains(t, err, "EMBEDDING_INPUT_PROFILE")
}
