package main

import (
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func env(values map[string]string) func(string) string {
	return func(key string) string { return values[key] }
}

func TestConfig(t *testing.T) {
	cfg, err := loadConfig(env(map[string]string{"QUARRY_URL": "127.0.0.1:8081"}))
	require.NoError(t, err)
	assert.Equal(t, (&url.URL{Scheme: "http", Host: "127.0.0.1:8081"}).String(), cfg.baseURL)
	assert.Equal(t, 90*time.Second, cfg.timeout)
	assert.False(t, cfg.readOnly)
	assert.Empty(t, cfg.fileRoots)
	base := (&url.URL{Scheme: "https", Host: "example.invalid", Path: "/prefix/"}).String()
	root := t.TempDir()
	cfg, err = loadConfig(env(map[string]string{"QUARRY_URL": base, "QUARRY_TIMEOUT": "10m", "QUARRY_SERVICE_TOKEN": " \tsynthetic-token\r\n", "QUARRY_MCP_READ_ONLY": "true", "QUARRY_MCP_FILE_ROOTS": root + string(os.PathListSeparator) + root}))
	require.NoError(t, err)
	assert.Equal(t, base, cfg.baseURL)
	assert.Equal(t, "synthetic-token", cfg.token)
	assert.Equal(t, 10*time.Minute, cfg.timeout)
	assert.True(t, cfg.readOnly)
	assert.Len(t, cfg.fileRoots, 2)
	canonical, err := filepath.EvalSymlinks(root)
	require.NoError(t, err)
	assert.Equal(t, canonical, cfg.fileRoots[0])
}

func TestConfigRejectsInvalidValues(t *testing.T) {
	for _, values := range []map[string]string{
		{},
		{"QUARRY_URL": (&url.URL{Scheme: "ftp", Host: "localhost"}).String()},
		{"QUARRY_URL": "localhost"},
		{"QUARRY_URL": "127.0.0.1:8081", "QUARRY_TIMEOUT": "oops"},
		{"QUARRY_URL": "127.0.0.1:8081", "QUARRY_TIMEOUT": "999ms"},
		{"QUARRY_URL": "127.0.0.1:8081", "QUARRY_TIMEOUT": "601s"},
		{"QUARRY_URL": "127.0.0.1:8081", "QUARRY_MCP_READ_ONLY": "maybe"},
		{"QUARRY_URL": "127.0.0.1:8081", "QUARRY_MCP_FILE_ROOTS": filepath.Join(t.TempDir(), "missing")},
		{"QUARRY_URL": "127.0.0.1:8081", "QUARRY_MCP_FILE_ROOTS": string(os.PathListSeparator)},
		{"QUARRY_URL": "127.0.0.1:8081", "QUARRY_SERVICE_TOKEN": "synthetic\ntoken"},
	} {
		_, err := loadConfig(env(values))
		require.Error(t, err)
	}
}

func TestConfigRejectsUnsafeToken(t *testing.T) {
	for _, token := range []string{"synthetic token", "synthetic\ttoken", "synthetic\rtoken", "synthetic\x00token", "synthetic\x7ftoken", "synthetic\u00a0token"} {
		_, err := loadConfig(env(map[string]string{"QUARRY_URL": "127.0.0.1:8081", "QUARRY_SERVICE_TOKEN": token}))
		require.EqualError(t, err, "QUARRY_SERVICE_TOKEN must not contain whitespace or control characters")
		assert.NotContains(t, err.Error(), token)
	}
}
