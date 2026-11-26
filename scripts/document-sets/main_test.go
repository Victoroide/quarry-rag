package main

import (
	"bytes"
	"encoding/json"
	"io"
	"strings"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/store/postgres"
	"github.com/Victoroide/quarry-rag/internal/testutil/pgtest"
	"github.com/stretchr/testify/require"
)

const (
	tenantID = "00000000-0000-4000-8000-0000000000a0"
	ownerID  = "00000000-0000-4000-8000-0000000000a1"
	docA     = "00000000-0000-4000-8000-00000000000a"
)

func TestDocumentSetsRefuseBadInputBeforeConnecting(t *testing.T) {
	valid := []string{"-tenant", tenantID, "-owner", ownerID}
	for name, tc := range map[string]struct {
		args  []string
		input string
		url   string
		want  string
	}{
		"extra argument":         {[]string{"extra"}, "", "invalid connection string", "standard input"},
		"missing tenant":         {[]string{"-owner", ownerID}, "", "invalid connection string", "-tenant and -owner are required"},
		"missing owner":          {[]string{"-tenant", tenantID}, "", "invalid connection string", "-tenant and -owner are required"},
		"purge with other flags": {[]string{"-purge-expired", "-tenant", tenantID}, "", "invalid connection string", "without other flags"},
		"missing database":       {valid, "", "", "DATABASE_URL is required"},
		"unknown flag":           {[]string{"-nope"}, "", "invalid connection string", "invalid document-sets flags"},
		"bad tenant":             {[]string{"-tenant", "tenant", "-owner", ownerID}, "", "invalid connection string", "tenant and an owning principal"},
		"lifetime above a day":   {append([]string{"-ttl", "25h"}, valid...), "", "invalid connection string", "lifetime"},
		"zero lifetime floor":    {append([]string{"-ttl", "1ms"}, valid...), "", "invalid connection string", "lifetime"},
		"long import key":        {append([]string{"-import-key", strings.Repeat("k", 129)}, valid...), "", "invalid connection string", "import key"},
		"malformed manifest":     {valid, docA + "\nnot-a-uuid\n", "invalid connection string", "invalid document set manifest"},
		"blank manifest line":    {valid, docA + "\n\n", "invalid connection string", "invalid document set manifest"},
		"too many entries":       {valid, strings.Repeat(docA+"\n", 4), "invalid connection string", "more than 3 entries"},
	} {
		t.Run(name, func(t *testing.T) {
			err := run(t.Context(), tc.args, tc.url, 3, strings.NewReader(tc.input), io.Discard)
			require.ErrorContains(t, err, tc.want)
		})
	}
}

func TestDocumentSetsCommandCreatesReplaysAndPurges(t *testing.T) {
	if testing.Short() {
		t.Skip("requires a Docker daemon")
	}
	databaseURL := pgtest.Start(t)
	c, err := postgres.NewClient(t.Context(), databaseURL)
	require.NoError(t, err)
	t.Cleanup(c.Pool.Close)
	require.NoError(t, c.RunMigrations(t.Context()))
	_, err = c.Pool.Exec(t.Context(), `INSERT INTO grant_catalog_tenants(tenant_id) VALUES($1)`, tenantID)
	require.NoError(t, err)
	_, err = c.Pool.Exec(t.Context(), `INSERT INTO grant_catalog_principals(principal_id,tenant_id,issuer,subject,enabled) VALUES($1,$2,'issuer','owner',true)`, ownerID, tenantID)
	require.NoError(t, err)

	args := []string{"-tenant", tenantID, "-owner", ownerID, "-import-key", "nightly", "-ttl", "30m"}
	var first, second bytes.Buffer
	require.NoError(t, run(t.Context(), args, databaseURL, 10, strings.NewReader(""), &first))
	var created postgres.DocumentSet
	require.NoError(t, json.Unmarshal(first.Bytes(), &created))
	require.NotEmpty(t, created.ID)
	require.Zero(t, created.Members)
	require.False(t, created.Replayed)
	require.NoError(t, run(t.Context(), args, databaseURL, 10, strings.NewReader(""), &second))
	var replayed postgres.DocumentSet
	require.NoError(t, json.Unmarshal(second.Bytes(), &replayed))
	require.True(t, replayed.Replayed)
	require.Equal(t, created.ID, replayed.ID)

	err = run(t.Context(), args, databaseURL, 10, strings.NewReader(docA+"\n"), io.Discard)
	require.ErrorContains(t, err, "import key conflict", "changed contents under a used key are a conflict")
	err = run(t.Context(), []string{"-tenant", tenantID, "-owner", ownerID}, databaseURL, 10, strings.NewReader(docA+"\n"), io.Discard)
	require.ErrorContains(t, err, "unknown or unauthorized document set member")

	var purged bytes.Buffer
	require.NoError(t, run(t.Context(), []string{"-purge-expired"}, databaseURL, 10, strings.NewReader(""), &purged))
	require.JSONEq(t, `{"purged":0}`, purged.String())
}
