package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDocumentSetSettingsDefaultOff(t *testing.T) {
	t.Setenv("GO_RAG_FAKE_MODE", "true")
	t.Setenv("DATABASE_URL", "host=localhost dbname=rag")
	for _, key := range []string{"DOCUMENT_SETS_ENABLED", "DOCUMENT_SET_MAX_MEMBERS", "DOCUMENT_SET_MAX_BATCHES"} {
		t.Setenv(key, "")
	}
	cfg, err := Load()
	require.NoError(t, err)
	require.False(t, cfg.DocumentSetsEnabled)
	require.Equal(t, 100000, cfg.DocumentSetMaxMembers)
	require.Equal(t, 10, cfg.DocumentSetMaxBatches)
}

func TestDocumentSetLimitsAreBounded(t *testing.T) {
	t.Setenv("GO_RAG_FAKE_MODE", "true")
	t.Setenv("DATABASE_URL", "host=localhost dbname=rag")
	for _, tc := range []struct {
		key, value string
		valid      bool
	}{
		{"DOCUMENT_SET_MAX_MEMBERS", "1", true}, {"DOCUMENT_SET_MAX_MEMBERS", "100000", true},
		{"DOCUMENT_SET_MAX_MEMBERS", "0", false}, {"DOCUMENT_SET_MAX_MEMBERS", "100001", false}, {"DOCUMENT_SET_MAX_MEMBERS", "-1", false},
		{"DOCUMENT_SET_MAX_MEMBERS", "many", false},
		{"DOCUMENT_SET_MAX_BATCHES", "1", true}, {"DOCUMENT_SET_MAX_BATCHES", "10", true},
		{"DOCUMENT_SET_MAX_BATCHES", "0", false}, {"DOCUMENT_SET_MAX_BATCHES", "11", false}, {"DOCUMENT_SET_MAX_BATCHES", "ten", false},
		{"DOCUMENT_SETS_ENABLED", "maybe", false},
	} {
		t.Run(tc.key+"="+tc.value, func(t *testing.T) {
			t.Setenv(tc.key, tc.value)
			_, err := Load()
			if tc.valid {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tc.key)
		})
	}
}

func TestOperatorReadsTheServiceMembershipCeiling(t *testing.T) {
	t.Setenv("DOCUMENT_SET_MAX_MEMBERS", "")
	limit, err := LoadDocumentSetMaxMembers()
	require.NoError(t, err)
	require.Equal(t, DocumentSetMembersLimit, limit)
	t.Setenv("DOCUMENT_SET_MAX_MEMBERS", "250")
	limit, err = LoadDocumentSetMaxMembers()
	require.NoError(t, err)
	require.Equal(t, 250, limit)
	t.Setenv("DOCUMENT_SET_MAX_MEMBERS", "100001")
	_, err = LoadDocumentSetMaxMembers()
	require.ErrorContains(t, err, "DOCUMENT_SET_MAX_MEMBERS")
}

func TestDocumentSetsRequireGrantsMode(t *testing.T) {
	cfg := baseValidConfig()
	cfg.DocumentSetsEnabled = true
	require.ErrorContains(t, cfg.validate(), "DOCUMENT_SETS_ENABLED=true requires AUTHZ_MODE=grants")
	cfg.DocumentSetsEnabled = false
	require.NoError(t, cfg.validate())
	cfg.DocumentSetMaxBatches = 11
	require.ErrorContains(t, cfg.validate(), "DOCUMENT_SET_MAX_BATCHES")
}

func TestGrantsModeAcceptsDocumentSets(t *testing.T) {
	configureValidGrantsEnv(t, writeValidJWKSFixture(t, time.Now().UTC().Add(time.Hour)))
	t.Setenv("DOCUMENT_SETS_ENABLED", "true")
	cfg, err := Load()
	require.NoError(t, err)
	require.True(t, cfg.DocumentSetsEnabled)
	t.Setenv("VECTOR_STORE", "pinecone")
	cfg, err = Load()
	require.NoError(t, err)
	require.True(t, cfg.DocumentSetsEnabled)
	t.Setenv("VECTOR_STORE", "memory")
	_, err = Load()
	require.Error(t, err, "an unsupported store never serves sets")
}

func TestServiceModeRejectsDocumentSets(t *testing.T) {
	awsConfigEnvironment(t)
	t.Setenv("DOCUMENT_SETS_ENABLED", "true")
	t.Setenv("AUTHZ_MODE", "service")
	_, err := Load()
	require.ErrorContains(t, err, "DOCUMENT_SETS_ENABLED")
}
