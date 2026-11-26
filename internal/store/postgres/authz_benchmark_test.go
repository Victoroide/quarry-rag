package postgres

import (
	"fmt"
	"net/url"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/testutil/pgtest"
	"github.com/google/uuid"
)

// BenchmarkPGVectorAuthzModes measures PostgreSQL pgvector query latency across
// scope sizes 0, 1, 10,000, and 25,000 comparing service-mode allowlists against
// grants-mode relation checks without materializing full allowlists.
func BenchmarkPGVectorAuthzModes(b *testing.B) {
	const maxScope = 25000

	c, err := NewClient(b.Context(), pgtest.StartVector(b))
	if err != nil {
		b.Fatalf("connect postgres: %v", err)
	}
	b.Cleanup(func() { _ = c.Close(b.Context()) })
	if migErr := c.RunMigrations(b.Context()); migErr != nil {
		b.Fatalf("run migrations: %v", migErr)
	}
	vs, vsErr := NewVectorStore(b.Context(), c, 3)
	if vsErr != nil {
		b.Fatalf("open pgvector store: %v", vsErr)
	}

	tenantID := uuid.NewString()
	ownerID := uuid.NewString()
	readerID := uuid.NewString()

	if _, tErr := c.Pool.Exec(b.Context(), `INSERT INTO grant_catalog_tenants (tenant_id) VALUES ($1)`, tenantID); tErr != nil {
		b.Fatalf("seed tenant: %v", tErr)
	}
	if _, pErr := c.Pool.Exec(b.Context(),
		`INSERT INTO grant_catalog_principals (principal_id, tenant_id, issuer, subject, enabled) VALUES ($1,$2,$4,'owner',true), ($3,$2,$4,'reader',true)`,
		ownerID, tenantID, readerID, (&url.URL{Scheme: "https", Host: "bench.local"}).String()); pErr != nil {
		b.Fatalf("seed principals: %v", pErr)
	}
	if _, colErr := c.Pool.Exec(b.Context(), `INSERT INTO grant_catalog_collections (tenant_id, collection) VALUES ($1, '')`, tenantID); colErr != nil {
		b.Fatalf("seed collection: %v", colErr)
	}

	docIDs := make([]string, maxScope)
	chunkIDs := make([]string, maxScope)
	for i := range maxScope {
		docIDs[i] = uuid.NewString()
		chunkIDs[i] = uuid.NewString()
	}

	if _, docErr := c.Pool.Exec(b.Context(), `
		INSERT INTO documents (id, incarnation, content, status, doc_type, collection, chunk_count, completed_at, grant_catalog_assigned)
		SELECT d, d, 'benchmark document content', 'completed', 'guide', '', 1, NOW(), TRUE
		FROM unnest($1::uuid[]) AS d`, docIDs); docErr != nil {
		b.Fatalf("seed documents: %v", docErr)
	}
	if _, chErr := c.Pool.Exec(b.Context(), `
		INSERT INTO chunks (id, document_id, chunk_index, text, token_count)
		SELECT c, d, 0, 'benchmark chunk text', 3
		FROM unnest($1::uuid[], $2::uuid[]) AS t(c, d)`, chunkIDs, docIDs); chErr != nil {
		b.Fatalf("seed chunks: %v", chErr)
	}
	if _, vecErr := c.Pool.Exec(b.Context(), fmt.Sprintf(`
		INSERT INTO %s (chunk_id, document_id, chunk_index, doc_type, collection, text, token_count, embedding)
		SELECT c, d, 0, 'guide', '', 'benchmark chunk text', 3, '[1,0,0]'::vector
		FROM unnest($1::uuid[], $2::uuid[]) AS t(c, d)`, vs.table), chunkIDs, docIDs); vecErr != nil {
		b.Fatalf("seed vector rows: %v", vecErr)
	}
	if _, incErr := c.Pool.Exec(b.Context(), `
		INSERT INTO grant_catalog_assigned_incarnations (document_id, incarnation)
		SELECT d, d FROM unnest($1::uuid[]) AS d`, docIDs); incErr != nil {
		b.Fatalf("seed incarnations: %v", incErr)
	}
	if _, ownErr := c.Pool.Exec(b.Context(), fmt.Sprintf(`
		INSERT INTO grant_catalog_document_ownership (document_id, incarnation, tenant_id, principal_id, collection, source_version, ready)
		SELECT d, d, $2::uuid, $3::uuid, '', %q, TRUE
		FROM unnest($1::uuid[]) AS d`, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
		docIDs, tenantID, ownerID); ownErr != nil {
		b.Fatalf("seed ownership: %v", ownErr)
	}

	reader := GrantReader{Pool: c.Pool}
	principal := domain.Principal{ID: readerID, TenantID: tenantID}
	queryVec := []float32{1, 0, 0}

	for _, size := range []int{0, 1, 10000, 25000} {
		if _, delErr := c.Pool.Exec(b.Context(), `DELETE FROM grant_catalog_document_grants WHERE tenant_id=$1 AND principal_id=$2`, tenantID, readerID); delErr != nil {
			b.Fatalf("reset grants: %v", delErr)
		}
		if size > 0 {
			if _, grErr := c.Pool.Exec(b.Context(), `
				INSERT INTO grant_catalog_document_grants (tenant_id, document_id, incarnation, principal_id, can_read, can_download)
				SELECT $1::uuid, d, d, $2::uuid, TRUE, FALSE
				FROM unnest($3::uuid[]) AS d`,
				tenantID, readerID, docIDs[:size]); grErr != nil {
				b.Fatalf("seed %d grants: %v", size, grErr)
			}
		}

		b.Run(fmt.Sprintf("scope=%d/service", size), func(b *testing.B) {
			filter := domain.VectorFilter{DocumentIDs: docIDs[:size]}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if _, qErr := vs.QueryFiltered(b.Context(), queryVec, 10, filter); qErr != nil {
					b.Fatal(qErr)
				}
			}
		})

		b.Run(fmt.Sprintf("scope=%d/grants", size), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				scope, dErr := reader.DeriveReadScope(b.Context(), principal, domain.ReadScopeRequest{})
				if dErr != nil {
					b.Fatal(dErr)
				}
				if scope.Empty {
					continue
				}
				filter := domain.VectorFilter{
					Authorized: &domain.AuthorizedScope{TenantID: tenantID, PrincipalID: readerID},
				}
				if _, qErr := vs.QueryFiltered(b.Context(), queryVec, 10, filter); qErr != nil {
					b.Fatal(qErr)
				}
			}
		})
	}
}
