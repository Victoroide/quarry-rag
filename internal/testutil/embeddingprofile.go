// Package testutil provides fixtures for service tests.
package testutil

import (
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/embeddinginput"
	"github.com/stretchr/testify/require"
)

// OpenAIProfile loads an exact profile from the compressed provider vocabulary fixture.
func OpenAIProfile(t testing.TB, fixture string, limit int) *embeddinginput.Profile {
	t.Helper()
	in, err := os.Open(filepath.Clean(fixture))
	require.NoError(t, err)
	defer func() { require.NoError(t, in.Close()) }()
	z, err := gzip.NewReader(in)
	require.NoError(t, err)
	defer func() { require.NoError(t, z.Close()) }()
	const maxBytes = 2 << 20
	data, err := io.ReadAll(io.LimitReader(z, maxBytes+1))
	require.NoError(t, err)
	require.LessOrEqual(t, len(data), maxBytes)
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "ranks.tiktoken"), data, 0o600))
	const model = "text-embedding-3-small"
	path := filepath.Join(dir, "profile.json")
	definition := fmt.Sprintf(`{"provider":"openai","model":%q,"revision":%q,"max_tokens":%d,"tokenizer":{"type":"tiktoken","version":"cl100k_base","sha256":"223921b76ee99bde995b7ff738513eef100fb51d18c93597a113bcffe865b2a7","artifact":"ranks.tiktoken"}}`, model, model, limit)
	require.NoError(t, os.WriteFile(path, []byte(definition), 0o600))
	profile, err := embeddinginput.Load(path, embeddinginput.Target{Provider: "openai", Model: model, BaseURL: embeddinginput.OpenAIEndpoint})
	require.NoError(t, err)
	return profile
}
