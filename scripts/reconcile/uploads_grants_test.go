package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/domain"
	pgstore "github.com/Victoroide/quarry-rag/internal/store/postgres"
	"github.com/Victoroide/quarry-rag/internal/testutil/pgtest"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

var publishedUploadRoles = []string{
	"quarry_service",
	"quarry_source_maintenance",
	"quarry_operator",
	"quarry_auditor",
}

func newUploadGrantRoles(t *testing.T, fixtureURL string) (map[string]string, *strings.Replacer) {
	t.Helper()
	roles := make(map[string]string, len(publishedUploadRoles))
	pairs := make([]string, 0, 2*len(publishedUploadRoles))
	for _, base := range publishedUploadRoles {
		name := pgtest.Role(t, fixtureURL, base)
		roles[base] = name
		pairs = append(pairs, base, pgx.Identifier{name}.Sanitize())
	}
	return roles, strings.NewReplacer(pairs...)
}

func execPublishedGrantBlock(t *testing.T, owner *pgstore.Client, replace *strings.Replacer, docs []byte, firstLine string) {
	t.Helper()
	_, block, found := strings.Cut(string(docs), "```sql\n"+firstLine)
	require.True(t, found, "missing grant block starting with %s", firstLine)
	sql, _, found := strings.Cut(block, "\n```")
	require.True(t, found, "unterminated grant block starting with %s", firstLine)
	_, err := owner.Pool.Exec(t.Context(), replace.Replace(firstLine+sql))
	require.NoError(t, err)
}

func connectUploadRole(t *testing.T, owner *pgstore.Client, login string) *pgstore.Client {
	t.Helper()
	client, err := pgstore.NewClient(t.Context(), owner.Pool.Config().ConnString(), func(cfg *pgxpool.Config) {
		cfg.ConnConfig.User = login
		cfg.ConnConfig.Password = "fixture-password"
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close(context.WithoutCancel(t.Context()))) })
	return client
}

func TestPublishedSourceUploadGrants(t *testing.T) {
	owner := newDatabase(t)
	roles, replaceRoles := newUploadGrantRoles(t, owner.Pool.Config().ConnString())
	deployment, err := os.ReadFile("../../docs/deployment.md")
	require.NoError(t, err)
	catalog, err := os.ReadFile("../../docs/grant-catalog.md")
	require.NoError(t, err)
	ctx := t.Context()
	serviceIdent := pgx.Identifier{roles["quarry_service"]}.Sanitize()
	maintenanceIdent := pgx.Identifier{roles["quarry_source_maintenance"]}.Sanitize()
	operatorIdent := pgx.Identifier{roles["quarry_operator"]}.Sanitize()
	auditorIdent := pgx.Identifier{roles["quarry_auditor"]}.Sanitize()
	_, err = owner.Pool.Exec(ctx, fmt.Sprintf(`
		REVOKE ALL ON SCHEMA public FROM PUBLIC;
		CREATE ROLE %s LOGIN PASSWORD 'fixture-password';
		CREATE ROLE %s LOGIN PASSWORD 'fixture-password';
		CREATE ROLE %s;
		CREATE ROLE %s;
		ALTER ROLE %s SET search_path = public;
		ALTER ROLE %s SET search_path = public;
	`, serviceIdent, maintenanceIdent, operatorIdent, auditorIdent, serviceIdent, maintenanceIdent))
	require.NoError(t, err)
	execPublishedGrantBlock(t, owner, replaceRoles, catalog,
		"GRANT USAGE ON SCHEMA public TO quarry_service, quarry_operator, quarry_auditor;")

	serving := connectUploadRole(t, owner, roles["quarry_service"])
	maintenance := connectUploadRole(t, owner, roles["quarry_source_maintenance"])
	r := newRig(t, serving, "restricted-source-uploads", true)
	response := r.upload("", noteText)
	require.Equal(t, http.StatusServiceUnavailable, response.Code, response.Body.String())
	require.Empty(t, r.bucket.keys(), "the serving grants alone cannot stage a journaled PUT")

	execPublishedGrantBlock(t, owner, replaceRoles, deployment,
		"GRANT SELECT, INSERT, UPDATE ON public.source_uploads TO quarry_service;")
	response = r.upload("", noteText)
	require.Equal(t, http.StatusAccepted, response.Code, response.Body.String())
	keys := linkedKeys(t, r)
	require.Len(t, keys, 1)
	linkedKey := keys[0]

	// Acknowledgment must succeed even when the upload never reaches adoption.
	r.store.stopBefore = func(domain.AcceptRequest) bool { return true }
	response = r.upload("", noteText+" Unadopted original.")
	require.Equal(t, http.StatusServiceUnavailable, response.Code, response.Body.String())
	_, err = owner.Pool.Exec(ctx, `UPDATE source_uploads SET created_at=NOW()-interval '3 hours',
		expires_at=NOW()-interval '2 hours' WHERE bucket=$1 AND state='staged'`, r.bucket.name)
	require.NoError(t, err)

	execPublishedGrantBlock(t, owner, replaceRoles, deployment, "GRANT USAGE ON SCHEMA public TO quarry_source_maintenance;")
	before := r.bucket.mutations()
	inventory, err := reconcileSourceUploads(ctx, maintenance, r.bucket, r.bucket.name, inventoryOptions())
	require.NoError(t, err)
	require.Len(t, inventory.Uploads, 1)
	require.True(t, inventory.Uploads[0].Acknowledged)
	require.Equal(t, actionDelete, inventory.Uploads[0].Action)
	require.Equal(t, before, r.bucket.mutations())
	var linkedVersion string
	require.NoError(t, maintenance.Pool.QueryRow(ctx, `SELECT version_id FROM source_uploads
		WHERE state='linked' AND object_key=$1 AND linked_document_id=document_id`, linkedKey).Scan(&linkedVersion))
	require.NotEmpty(t, linkedVersion)
	references, err := maintenance.ReferencedSourceKeys(ctx, []string{linkedKey, inventory.Uploads[0].Key})
	require.NoError(t, err)
	require.Equal(t, map[string]bool{linkedKey: true}, references)

	execPublishedGrantBlock(t, owner, replaceRoles, deployment,
		"GRANT UPDATE, DELETE ON public.source_uploads TO quarry_source_maintenance;")
	applied, err := reconcileSourceUploads(ctx, maintenance, r.bucket, r.bucket.name, applyOptions())
	require.NoError(t, err)
	require.Equal(t, 1, applied.DeletedVersions)
	require.Equal(t, []string{linkedKey}, r.bucket.keys())
	settled, err := reconcileSourceUploads(ctx, maintenance, r.bucket, r.bucket.name, applyOptions())
	require.NoError(t, err)
	require.Equal(t, 1, settled.Settled)
	require.Len(t, settled.Journal, 1)
	require.Equal(t, "linked", settled.Journal[0].State)
}
