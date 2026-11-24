package main

import (
	"io"
	"strings"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/store/postgres"
	"github.com/Victoroide/quarry-rag/internal/testutil/pgtest"
	"github.com/stretchr/testify/require"
)

func TestGrantCatalogRejectsDuplicateInputBeforeConnecting(t *testing.T) {
	for _, raw := range []string{
		`{"expected_revision":1,"expected_revision":0}`,
		`{"batch_id":"10000000-0000-4000-8000-000000000001","expected_revision":0,"tenants":[{"tenant_id":null,"tenant_id":"10000000-0000-4000-8000-000000000002"}]}`,
	} {
		err := run(t.Context(), nil, "invalid connection string", strings.NewReader(raw), io.Discard)
		require.ErrorContains(t, err, "invalid grant batch JSON")
	}
}

func TestGrantCatalogInvalidDimensionsLeaveSchemaUntouched(t *testing.T) {
	if testing.Short() {
		t.Skip("requires a Docker daemon")
	}
	databaseURL := pgtest.Start(t)
	c, err := postgres.NewClient(t.Context(), databaseURL)
	require.NoError(t, err)
	t.Cleanup(c.Pool.Close)
	for _, dimension := range []string{"-1", "0", "2001"} {
		t.Run(dimension, func(t *testing.T) {
			runErr := run(t.Context(), []string{"-migrate", "-vector-dimensions", dimension}, databaseURL, strings.NewReader(""), io.Discard)
			require.ErrorContains(t, runErr, "vector dimensions must be between 1 and 2000")
			var untouched bool
			require.NoError(t, c.Pool.QueryRow(t.Context(), `SELECT to_regclass('public.schema_migrations') IS NULL
				AND to_regclass('public.documents') IS NULL AND NOT EXISTS (SELECT FROM pg_extension WHERE extname='vector')`).Scan(&untouched))
			require.True(t, untouched)
		})
	}
}
