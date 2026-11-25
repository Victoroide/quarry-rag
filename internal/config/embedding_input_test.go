package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEmbeddingInputProfileStartup(t *testing.T) {
	providerEnvironment(t)
	t.Setenv("EXTRACTION_CHUNK_VERSION", "release")
	t.Setenv("EMBEDDING_INPUT_PROFILE", "")
	cfg, err := Load()
	require.NoError(t, err)
	require.Nil(t, cfg.EmbeddingInputProfile)
	withoutInputLimits := cfg.TextProfile()

	path := filepath.Join(t.TempDir(), "embedding.json")
	require.NoError(t, os.WriteFile(path, []byte(`{"provider":"openai","model":"embed","revision":"r1","max_characters":6}`), 0o600))
	t.Setenv("EMBEDDING_INPUT_PROFILE", path)
	cfg, err = Load()
	require.NoError(t, err)
	require.NotNil(t, cfg.EmbeddingInputProfile)
	withInputLimits := cfg.TextProfile()
	require.Contains(t, withInputLimits, withoutInputLimits+"/input=")

	require.NoError(t, os.WriteFile(path, []byte(`{"provider":"openai","model":"other","revision":"r2","max_characters":6}`), 0o600))
	t.Setenv("OPENAI_EMBEDDING_MODEL", "other")
	cfg, err = Load()
	require.NoError(t, err)
	require.Equal(t, withInputLimits, cfg.TextProfile(), "changing the model alone preserves the chunk settings")
	require.NoError(t, os.WriteFile(path, []byte(`{"provider":"openai","model":"other","revision":"r2","max_characters":7}`), 0o600))
	cfg, err = Load()
	require.NoError(t, err)
	require.NotEqual(t, withInputLimits, cfg.TextProfile(), "changing input limits changes chunk identity")

	t.Setenv("OPENAI_EMBEDDING_MODEL", "embed")
	_, err = Load()
	require.ErrorContains(t, err, "EMBEDDING_INPUT_PROFILE")
}
