package postgres

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/chunker"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/testutil/pgtest"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
)

const oldLayoutVectorDDL = `CREATE TABLE %s (
			chunk_id TEXT PRIMARY KEY,
			document_id UUID NOT NULL REFERENCES %s(id) ON DELETE CASCADE,
			chunk_index INTEGER NOT NULL,
			doc_type TEXT NOT NULL,
			collection TEXT NOT NULL DEFAULT '',
			text TEXT NOT NULL,
			kind TEXT,
			quality TEXT,
			token_count INTEGER NOT NULL,
			embedding vector(3) NOT NULL
		)`

func TestPostgresLayoutMigrationAndGrants(t *testing.T) {
	t.Run("no_extension_or_vector_tables", func(t *testing.T) {
		c := migrationTestClient(t, startTestDatabase(t))
		applyMigrationPrefix(t, c, 38)
		ctx := t.Context()
		require.NoError(t, c.RunMigrations(ctx))
		requireRecordedMigrations(t, c)
		var extCount int
		require.NoError(t, c.Pool.QueryRow(ctx, `SELECT count(*) FROM pg_extension WHERE extname='vector'`).Scan(&extCount))
		require.Zero(t, extCount)
		// Idempotent re-run.
		require.NoError(t, c.RunMigrations(ctx))
	})

	t.Run("legacy_named_unregistered_other_schema_and_irrelevant_tables", func(t *testing.T) {
		url := pgtest.StartVector(t)
		c := migrationTestClient(t, url)
		applyMigrationPrefix(t, c, 38)

		_, err := c.Pool.Exec(t.Context(), `CREATE EXTENSION IF NOT EXISTS vector`)
		require.NoError(t, err)

		for _, tbl := range []string{"chunk_vectors", "chunk_vectors_act", "chunk_vectors_ret", "chunk_vectors_unreg"} {
			_, err = c.Pool.Exec(t.Context(), fmt.Sprintf(oldLayoutVectorDDL, tbl, "documents"))
			require.NoError(t, err)
		}
		_, err = c.Pool.Exec(t.Context(), `INSERT INTO vector_generations(name,store,target,profile,profile_hash,profile_origin,state)
			VALUES('act','postgres','chunk_vectors_act','{}','h1','recorded','active'),
			      ('ret','postgres','chunk_vectors_ret','{}','h2','recorded','retired')`)
		require.NoError(t, err)
		_, err = c.Pool.Exec(t.Context(), `UPDATE vector_state SET legacy_store='postgres', legacy_target='chunk_vectors'`)
		require.NoError(t, err)

		// Other schema and irrelevant similarly named table must remain untouched.
		_, err = c.Pool.Exec(t.Context(), `CREATE SCHEMA other_corpus`)
		require.NoError(t, err)
		_, err = c.Pool.Exec(t.Context(), `CREATE TABLE other_corpus.documents(id UUID PRIMARY KEY)`)
		require.NoError(t, err)
		_, err = c.Pool.Exec(t.Context(), fmt.Sprintf(oldLayoutVectorDDL, "other_corpus.chunk_vectors", "other_corpus.documents"))
		require.NoError(t, err)
		_, err = c.Pool.Exec(t.Context(), `CREATE TABLE chunk_vectors_notes (id TEXT PRIMARY KEY, note TEXT)`)
		require.NoError(t, err)

		// Pre-migration validation fails closed without DDL on OpenGenerationVectorStore or NewVectorStore.
		_, err = OpenGenerationVectorStore(t.Context(), c, 3, LegacyGeneration)
		require.ErrorContains(t, err, "apply database migrations")
		_, err = NewVectorStore(t.Context(), c, 3)
		require.ErrorContains(t, err, "apply database migrations")
		var hasPages bool
		require.NoError(t, c.Pool.QueryRow(t.Context(), `SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='chunk_vectors' AND column_name='pages')`).Scan(&hasPages))
		require.False(t, hasPages, "read-only Open and validation must issue no DDL")

		// A partial install with broad or unvalidated checks must be normalized.
		grantExec(t, c, `ALTER TABLE chunk_vectors
			ADD COLUMN pages integer[] DEFAULT ARRAY[7] CHECK (cardinality(pages) >= 0),
			ADD COLUMN table_ids text[] DEFAULT ARRAY['p7-t1'],
			ADD COLUMN row_ids text[] DEFAULT ARRAY['p7-t1-r1'],
			ADD CONSTRAINT quarry_layout_table_ids_check CHECK (cardinality(table_ids) >= 0) NOT VALID,
			ADD CONSTRAINT partial_row_ids_check CHECK (cardinality(row_ids) >= 0) NOT VALID`)

		require.NoError(t, c.RunMigrations(t.Context()))
		requireRecordedMigrations(t, c)
		require.NoError(t, c.RunMigrations(t.Context()))

		for _, tbl := range []string{"chunk_vectors", "chunk_vectors_act", "chunk_vectors_ret", "chunk_vectors_unreg"} {
			require.NoError(t, validateVectorLayoutSchema(t.Context(), c.Pool, tbl), tbl)
		}

		// Other schema and irrelevant table were not altered.
		var untouchedCols int
		require.NoError(t, c.Pool.QueryRow(t.Context(), `SELECT count(*) FROM information_schema.columns
			WHERE ((table_schema='other_corpus' AND table_name='chunk_vectors') OR (table_schema='public' AND table_name='chunk_vectors_notes'))
			AND column_name IN ('pages','table_ids','row_ids')`).Scan(&untouchedCols))
		require.Zero(t, untouchedCols)

		// Old SQL with explicit columns works after migration and leaves layout fields NULL.
		docID := completedDocument(t, c)
		_, err = c.Pool.Exec(t.Context(), `INSERT INTO chunk_vectors(chunk_id,document_id,chunk_index,doc_type,text,token_count,embedding)
			VALUES('legacy-sql-1',$1,0,'report','legacy row',2,'[1,0,0]')`, string(docID))
		require.NoError(t, err)
		store, err := OpenGenerationVectorStore(t.Context(), c, 3, LegacyGeneration)
		require.NoError(t, err)
		fetched, err := store.Fetch(t.Context(), []string{"legacy-sql-1"})
		require.NoError(t, err)
		require.Contains(t, fetched, "legacy-sql-1")
		require.NotContains(t, fetched["legacy-sql-1"].Metadata, "pages")
		require.NotContains(t, fetched["legacy-sql-1"].Metadata, "table_ids")
		require.NotContains(t, fetched["legacy-sql-1"].Metadata, "row_ids")

		var pgErr *pgconn.PgError
		for _, field := range []struct {
			name, sqlType, element string
			limit                  int
		}{
			{"pages", "integer", "1", domain.MaxPDFPages},
			{"table_ids", "text", "'p1-t1'", domain.MaxLayoutProvenanceIDs},
			{"row_ids", "text", "'p1-t1-r1'", domain.MaxLayoutProvenanceIDs},
		} {
			for _, value := range []string{
				fmt.Sprintf("'{}'::%s[]", field.sqlType),
				fmt.Sprintf("array_fill(%s::%s, ARRAY[%d])", field.element, field.sqlType, field.limit+1),
				fmt.Sprintf("array_fill(%s::%s, ARRAY[2,2])", field.element, field.sqlType),
				fmt.Sprintf("array_fill(%s::%s, ARRAY[1], ARRAY[0])", field.element, field.sqlType),
			} {
				stmt := fmt.Sprintf("UPDATE chunk_vectors SET %s=%s WHERE chunk_id='legacy-sql-1'", field.name, value)
				_, err = c.Pool.Exec(t.Context(), stmt)
				require.ErrorAs(t, err, &pgErr)
				require.Equal(t, "23514", pgErr.Code, stmt)
			}
		}

		// Restricted serving and column-restricted grants work; schema-only fails with 42501.
		dsn := c.Pool.Config().ConnString()
		allRoles := append(append([]string(nil), servingRoles...), "quarry_col", "quarry_schema_only")
		roles := newGrantRoles(t, dsn, allRoles...)
		for _, purpose := range allRoles {
			grantExec(t, c, "CREATE ROLE "+pgx.Identifier{roles.name(purpose)}.Sanitize())
		}
		grantExec(t, c, publishedServingSQL(t, roles, "grant-catalog.md", "GRANT USAGE ON SCHEMA public"))
		grantExec(t, c, publishedServingSQL(t, roles, "grant-catalog.md", "ON public.chunk_vectors TO quarry_service"))
		srv := grantRoleClient(t, dsn, "SET SESSION AUTHORIZATION "+pgx.Identifier{roles.name("quarry_service")}.Sanitize())
		srvStore, err := OpenGenerationVectorStore(t.Context(), srv, 3, LegacyGeneration)
		require.NoError(t, err)
		require.NoError(t, srvStore.Upsert(t.Context(), "", []domain.Chunk{{
			ID: "srv-1", DocumentID: docID, Index: 1, Text: "served", TokenCount: 1, Embedding: []float32{0, 1, 0},
			Metadata: map[string]any{"doc_type": "report", "pages": []int{1}, "table_ids": []string{"p1-t1"}, "row_ids": []string{"p1-t1-r1"}},
		}}))
		got, err := srvStore.Fetch(t.Context(), []string{"srv-1"})
		require.NoError(t, err)
		require.Equal(t, []int{1}, got["srv-1"].Metadata["pages"])

		colIdent := pgx.Identifier{roles.name("quarry_col")}.Sanitize()
		colGrantSQL := strings.NewReplacer(
			"quarry_service", colIdent,
			"public.chunk_vectors", "public.chunk_vectors_act",
		).Replace(publishedSQL(t, "grant-catalog.md", "SELECT (chunk_id"))
		grantExec(t, c, fmt.Sprintf(`GRANT USAGE ON SCHEMA public TO %s`, colIdent))
		grantExec(t, c, fmt.Sprintf(`GRANT SELECT (id, status, incarnation, doc_type, collection, scoped_vectors, vector_attempt, vector_mutation_pending, vector_incarnation_required) ON public.documents TO %s`, colIdent))
		grantExec(t, c, colGrantSQL)
		colClient := grantRoleClient(t, dsn, "SET SESSION AUTHORIZATION "+colIdent)
		colStore, err := OpenGenerationVectorStore(t.Context(), colClient, 3, "act", WithExactScopeMax(10))
		require.NoError(t, err)
		require.NoError(t, colStore.Upsert(t.Context(), "", []domain.Chunk{{
			ID: "col-1", DocumentID: docID, Index: 2, Text: "col", TokenCount: 1, Embedding: []float32{0, 0, 1},
			Metadata: map[string]any{"doc_type": "report", "pages": []int{2}, "table_ids": []string{"p2-t1"}, "row_ids": []string{"p2-t1-r1"}},
		}}))
		colFetched, err := colStore.Fetch(t.Context(), []string{"col-1"})
		require.NoError(t, err)
		require.Equal(t, []int{2}, colFetched["col-1"].Metadata["pages"])
		colQueried, exact, err := colStore.QueryFilteredWithExactness(t.Context(), []float32{0, 0, 1}, 5, domain.VectorFilter{DocumentIDs: []string{string(docID)}})
		require.NoError(t, err)
		require.True(t, exact)
		require.Len(t, colQueried, 1)
		require.Equal(t, []int{2}, colQueried[0].Chunk.Metadata["pages"])

		schemaOnlyIdent := pgx.Identifier{roles.name("quarry_schema_only")}.Sanitize()
		grantExec(t, c, fmt.Sprintf(`GRANT USAGE ON SCHEMA public TO %s`, schemaOnlyIdent))
		schemaOnly := grantRoleClient(t, dsn, "SET SESSION AUTHORIZATION "+schemaOnlyIdent)
		schemaStore, err := OpenGenerationVectorStore(t.Context(), schemaOnly, 3, LegacyGeneration)
		require.NoError(t, err)
		_, err = schemaStore.Fetch(t.Context(), []string{"srv-1"})
		require.ErrorAs(t, err, &pgErr)
		require.Equal(t, "42501", pgErr.Code)
	})

	t.Run("incompatible_registered_target_fails_migration", func(t *testing.T) {
		c := migrationTestClient(t, pgtest.StartVector(t))
		applyMigrationPrefix(t, c, 38)
		ctx := t.Context()
		_, err := c.Pool.Exec(ctx, `CREATE EXTENSION IF NOT EXISTS vector`)
		require.NoError(t, err)
		_, err = c.Pool.Exec(ctx, `CREATE TABLE chunk_vectors_bad (chunk_id TEXT PRIMARY KEY, document_id UUID NOT NULL, embedding vector(3) NOT NULL)`)
		require.NoError(t, err)
		_, err = c.Pool.Exec(ctx, `INSERT INTO vector_generations(name,store,target,profile,profile_hash,profile_origin,state)
			VALUES('bad','postgres','chunk_vectors_bad','{}','h','recorded','staging')`)
		require.NoError(t, err)
		err = c.RunMigrations(ctx)
		require.ErrorContains(t, err, "incompatible registered pgvector target")
	})
}

func TestPublishedPrecreatedGenerationLayoutArrays(t *testing.T) {
	c, _ := vectorDatabase(t)
	ctx := t.Context()
	requireRecordedMigrations(t, c)

	ddl := strings.NewReplacer("<generation>", "g2", "<dimensions>", "3").
		Replace(publishedSQL(t, "deployment.md", "CREATE TABLE public.chunk_vectors_<generation>"))
	grantExec(t, c, ddl)
	v, err := OpenGenerationVectorStore(ctx, c, 3, "g2")
	require.NoError(t, err)

	doc := completedDocument(t, c)
	const key = "documented-layout"
	grantExec(t, c, `INSERT INTO chunk_vectors_g2(chunk_id,document_id,chunk_index,doc_type,text,token_count,embedding)
		VALUES($1,$2,0,'report','body',1,'[1,0,0]')`, key, string(doc))
	fetched, err := v.Fetch(ctx, []string{key})
	require.NoError(t, err)
	require.Contains(t, fetched, key)
	requireLayoutMetadata(t, domain.Chunk{}, fetched[key])

	for _, field := range []struct {
		name, sqlType, element string
	}{
		{"pages", "integer", "1"},
		{"table_ids", "text", "'p1-t1'"},
		{"row_ids", "text", "'p1-t1-r1'"},
	} {
		t.Run(field.name, func(t *testing.T) {
			for _, invalid := range []string{
				fmt.Sprintf("array_fill(%s::%s, ARRAY[2,2])", field.element, field.sqlType),
				fmt.Sprintf("array_fill(%s::%s, ARRAY[2], ARRAY[0])", field.element, field.sqlType),
			} {
				update := fmt.Sprintf("UPDATE chunk_vectors_g2 SET %s = %s WHERE chunk_id = $1", field.name, invalid)
				_, updateErr := c.Pool.Exec(t.Context(), update, key)
				var pgErr *pgconn.PgError
				require.ErrorAs(t, updateErr, &pgErr)
				require.Equal(t, "23514", pgErr.Code)
				require.Equal(t, "quarry_layout_"+field.name+"_check", pgErr.ConstraintName)
			}
		})
	}

	grantExec(t, c, `UPDATE chunk_vectors_g2 SET pages = ARRAY[2,1], table_ids = ARRAY['p1-t1'],
		row_ids = ARRAY['p1-t1-r2','p1-t1-r1'] WHERE chunk_id = $1`, key)
	fetched, err = v.Fetch(ctx, []string{key})
	require.NoError(t, err)
	require.Contains(t, fetched, key)
	requireLayoutMetadata(t, domain.Chunk{Metadata: map[string]any{
		domain.LayoutPagesKey:    []int{2, 1},
		domain.LayoutTableIDsKey: []string{"p1-t1"},
		domain.LayoutRowIDsKey:   []string{"p1-t1-r2", "p1-t1-r1"},
	}}, fetched[key])
}

func TestCompleteLayoutJobReturnsProvenance(t *testing.T) {
	f := newRetrievalFixture(t)
	f.setCreate(t, f.peer, "team", true)
	f.setCreate(t, f.outsider, "team", true)

	produced := layoutFixtureChunks(t)
	requireLayoutFixtureProvenance(t, produced)

	completeLayoutDoc := func(t *testing.T, targetStore *VectorStore, owner domain.Principal, docType, collection string, vec []float32, withLayout bool) (string, []domain.Chunk) {
		t.Helper()
		targetStore.RouteCompletion()
		content := "Region summary content " + uuid.NewString()
		req := ownedRequest(owner, collection, content)
		req.DocType = docType
		require.Equal(t, domain.AcceptAccepted, f.accept(t, req).Outcome)
		job := f.claim(t)
		var jobChunks []domain.Chunk
		if withLayout {
			jobChunks = make([]domain.Chunk, len(produced))
			for i, pc := range produced {
				pc.ID = domain.ChunkID(uuid.NewString())
				pc.DocumentID = req.Document.ID
				pc.Embedding = []float32{vec[0], vec[1] + float32(i)/10, vec[2]}
				jobChunks[i] = pc
			}
		} else {
			jobChunks = []domain.Chunk{{
				ID:         domain.ChunkID(uuid.NewString()),
				DocumentID: req.Document.ID,
				Index:      0,
				Text:       content,
				TokenCount: 3,
				Embedding:  vec,
			}}
		}
		ok, completeErr := f.c.CompleteJob(t.Context(), *job, jobChunks, domain.IndexObservation{})
		require.NoError(t, completeErr)
		require.True(t, ok)
		return string(req.Document.ID), jobChunks
	}

	for _, generation := range []string{LegacyGeneration, "g2"} {
		t.Run(generation, func(t *testing.T) {
			ctx := t.Context()
			if generation != LegacyGeneration {
				_, err := CreateGenerationVectorStore(ctx, f.c, 3, generation)
				require.NoError(t, err)
			}
			ann, err := OpenGenerationVectorStore(ctx, f.c, 3, generation, WithExactScopeMax(0))
			require.NoError(t, err)
			exact, err := OpenGenerationVectorStore(ctx, f.c, 3, generation, WithExactScopeMax(100))
			require.NoError(t, err)
			require.NoError(t, exact.EnsureExactScopeIndexes(ctx))

			layoutID, layout := completeLayoutDoc(t, ann, f.owner, "report", "team", []float32{1, 0, 0}, true)
			plainID, plain := completeLayoutDoc(t, ann, f.owner, "report", "team", []float32{.7, .7, 0}, false)
			wrongCollection, _ := completeLayoutDoc(t, ann, f.owner, "report", "", []float32{1, 0, 0}, false)
			wrongType, _ := completeLayoutDoc(t, ann, f.owner, "memo", "team", []float32{1, 0, 0}, false)
			peerID, _ := completeLayoutDoc(t, ann, f.peer, "report", "team", []float32{1, 0, 0}, false)
			otherTenant, _ := completeLayoutDoc(t, ann, f.outsider, "report", "team", []float32{1, 0, 0}, false)

			keys := make([]string, len(layout)+1)
			for i, chunk := range layout {
				keys[i] = string(chunk.ID)
			}
			keys[len(layout)] = string(plain[0].ID)
			fetched, err := ann.Fetch(ctx, keys)
			require.NoError(t, err)
			require.Len(t, fetched, len(keys))
			allowed := slices.Concat(layout, plain)
			for i, want := range allowed {
				requireLayoutMetadata(t, want, fetched[keys[i]])
			}

			scope := domain.AuthorizedScope{TenantID: f.owner.TenantID, PrincipalID: f.owner.ID}
			filter := domain.VectorFilter{DocType: "report", Collection: "team", Authorized: &scope}
			checkRanking := func(ctx context.Context, searchFilter domain.VectorFilter, want []domain.Chunk) {
				t.Helper()
				for i, store := range []*VectorStore{ann, exact} {
					hits, isExact, queryErr := store.QueryFilteredWithExactness(ctx, retrievalQuery, 20, searchFilter)
					require.NoError(t, queryErr)
					require.Equal(t, i == 1, isExact)
					require.Len(t, hits, len(want))
					for rank, hit := range hits {
						require.Equal(t, want[rank].ID, hit.Chunk.ID)
						require.Equal(t, want[rank].DocumentID, hit.Chunk.DocumentID)
						requireLayoutMetadata(t, want[rank], hit.Chunk)
					}
				}
			}
			// The hidden or mislabeled vectors are closer than several allowed hits.
			checkRanking(ctx, filter, allowed)
			explicit := filter
			explicit.DocumentIDs = []string{layoutID, wrongCollection, wrongType, peerID, otherTenant}
			checkRanking(ctx, explicit, layout)
			setScope := scope
			set, err := f.c.CreateDocumentSet(ctx, DocumentSetRequest{TenantID: f.tenant, OwnerID: f.owner.ID, DocumentIDs: []string{layoutID}, MaxMembers: 10})
			require.NoError(t, err)
			setScope.DocumentSet = f.setRef(set)
			inSet := filter
			inSet.Authorized = &setScope
			inSet.DocumentIDs = []string{layoutID, plainID}
			checkRanking(ctx, inSet, layout)

			breakLayoutArraysAndRequireReadFailure(ctx, t, f.c, ann, exact, layoutID, keys[0])
		})
	}
}

func layoutFixtureChunks(t *testing.T) []domain.Chunk {
	t.Helper()
	ch := chunker.NewChunker(9, 4)
	sections := []domain.LayoutSection{
		{Page: 1, TableID: "p1-t1", RowID: "p1-t1-r1", Text: "A a. B b. C c. D d."},
		{Page: 2, TableID: "p1-t1", RowID: "p1-t1-r1", Text: "E e. F f."},
		{Page: 2, TableID: "p1-t1", RowID: "p1-t1-r2", Text: "G g. H h."},
		{Page: 2, TableID: "p2-t1", RowID: "p2-t1-r1", Text: "I i. J j."},
	}
	chunks, err := ch.ChunkLayout(t.Context(), "fixture", sections, nil)
	require.NoError(t, err)
	return chunks
}

func TestLayoutFixtureProvenance(t *testing.T) {
	requireLayoutFixtureProvenance(t, layoutFixtureChunks(t))
}

func requireLayoutFixtureProvenance(t *testing.T, chunks []domain.Chunk) {
	t.Helper()
	expected := []struct {
		text         string
		pages        []int
		tables, rows []string
	}{
		{"A a. B b. C c.", []int{1}, []string{"p1-t1"}, []string{"p1-t1-r1"}},
		{"C c. D d.\n\nE e.", []int{1, 2}, []string{"p1-t1"}, []string{"p1-t1-r1"}},
		{"E e. F f.\n\nG g.", []int{2}, []string{"p1-t1"}, []string{"p1-t1-r1", "p1-t1-r2"}},
		{"G g. H h.\n\nI i.", []int{2}, []string{"p1-t1", "p2-t1"}, []string{"p1-t1-r2", "p2-t1-r1"}},
		{"I i. J j.", []int{2}, []string{"p2-t1"}, []string{"p2-t1-r1"}},
	}
	require.Len(t, chunks, len(expected))
	for i, want := range expected {
		require.Equal(t, want.text, chunks[i].Text)
		require.Equal(t, want.pages, chunks[i].Metadata[domain.LayoutPagesKey])
		require.Equal(t, want.tables, chunks[i].Metadata[domain.LayoutTableIDsKey])
		require.Equal(t, want.rows, chunks[i].Metadata[domain.LayoutRowIDsKey])
	}
}

func requireLayoutMetadata(t *testing.T, want, got domain.Chunk) {
	t.Helper()
	for _, field := range []string{domain.LayoutPagesKey, domain.LayoutTableIDsKey, domain.LayoutRowIDsKey} {
		expected, present := want.Metadata[field]
		actual, found := got.Metadata[field]
		require.Equal(t, present, found, "chunk %s field %s", want.ID, field)
		require.Equal(t, expected, actual, "chunk %s field %s", want.ID, field)
	}
}

func breakLayoutArraysAndRequireReadFailure(ctx context.Context, t *testing.T, c *Client, ann, exact *VectorStore, docID, key string) {
	t.Helper()
	table := pgx.Identifier{ann.table}.Sanitize()
	filter := domain.VectorFilter{DocumentIDs: []string{docID}}
	for _, field := range []struct {
		name, sqlType, element string
		limit                  int
	}{
		{"pages", "integer", "1", domain.MaxPDFPages},
		{"table_ids", "text", "'p1-t1'", domain.MaxLayoutProvenanceIDs},
		{"row_ids", "text", "'p1-t1-r1'", domain.MaxLayoutProvenanceIDs},
	} {
		// Current DDL rejects malformed shapes. Bypass it to exercise the read
		// contract for existing bad rows, and for NULL elements in valid shapes.
		stmt := fmt.Sprintf("ALTER TABLE %s DROP CONSTRAINT %s", table, pgx.Identifier{"quarry_layout_" + field.name + "_check"}.Sanitize())
		_, dropErr := c.Pool.Exec(ctx, stmt)
		require.NoError(t, dropErr)
		for _, malformed := range []string{
			fmt.Sprintf("array_fill(%s::%s, ARRAY[2,2])", field.element, field.sqlType),
			fmt.Sprintf("array_fill(%s::%s, ARRAY[1], ARRAY[0])", field.element, field.sqlType),
			fmt.Sprintf("array_fill(%s::%s, ARRAY[%d])", field.element, field.sqlType, field.limit+1),
			fmt.Sprintf("ARRAY[%s,NULL]::%s[]", field.element, field.sqlType),
		} {
			update := fmt.Sprintf("UPDATE %s SET %s=%s WHERE chunk_id=$1", table, field.name, malformed)
			_, err := c.Pool.Exec(ctx, update, key)
			require.NoError(t, err)
			got, err := ann.Fetch(ctx, []string{key})
			require.ErrorContains(t, err, field.name)
			require.Nil(t, got)
			for _, reader := range []*VectorStore{ann, exact} {
				hits, _, err := reader.QueryFilteredWithExactness(ctx, retrievalQuery, 20, filter)
				require.ErrorContains(t, err, field.name)
				require.Empty(t, hits)
			}
		}
		_, err := c.Pool.Exec(ctx, fmt.Sprintf("UPDATE %s SET %s=NULL WHERE chunk_id=$1", table, field.name), key)
		require.NoError(t, err)
	}
}

func TestVectorLayoutUpsertReplacementAndRollback(t *testing.T) {
	c, v := vectorDatabase(t)
	v.RouteCompletion()

	// Lost lease / canceled context during completion writes no partial vectors.
	accepted, err := c.AcceptDocument(t.Context(), jobsRequest("lease loss layout document"))
	require.NoError(t, err)
	require.Equal(t, domain.AcceptAccepted, accepted.Outcome)
	job, err := c.ClaimJob(t.Context(), "worker-a", time.Minute, 3)
	require.NoError(t, err)
	require.NotNil(t, job)

	lostLease := *job
	lostLease.LeaseToken = uuid.NewString()
	ok, err := c.CompleteJob(t.Context(), lostLease, []domain.Chunk{{
		ID: domain.ChunkID(uuid.NewString()), DocumentID: job.Document.ID, Index: 0, Text: "lost", TokenCount: 1, Embedding: []float32{1, 0, 0},
		Metadata: map[string]any{"pages": []int{1}, "table_ids": []string{"p1-t1"}, "row_ids": []string{"p1-t1-r1"}},
	}}, domain.IndexObservation{})
	require.NoError(t, err)
	require.False(t, ok)

	canceledCtx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = c.CompleteJob(canceledCtx, *job, []domain.Chunk{{
		ID: domain.ChunkID(uuid.NewString()), DocumentID: job.Document.ID, Index: 0, Text: "canceled", TokenCount: 1, Embedding: []float32{1, 0, 0},
		Metadata: map[string]any{"pages": []int{1}},
	}}, domain.IndexObservation{})
	require.ErrorIs(t, err, context.Canceled)

	var count int
	require.NoError(t, c.Pool.QueryRow(t.Context(), `SELECT count(*) FROM chunk_vectors WHERE document_id=$1`, string(accepted.DocumentID)).Scan(&count))
	require.Zero(t, count)

	// Complete the job legitimately, then delete and recreate the same document ID.
	originalID := uuid.NewString()
	original := domain.Chunk{
		ID: domain.ChunkID(originalID), DocumentID: job.Document.ID, Index: 0, Text: "v1 layout", TokenCount: 2, Embedding: []float32{1, 0, 0},
		Metadata: map[string]any{"pages": []int{1, 2}, "table_ids": []string{"p1-t1"}, "row_ids": []string{"p1-t1-r1"}},
	}
	ok, err = c.CompleteJob(t.Context(), *job, []domain.Chunk{original}, domain.IndexObservation{})
	require.NoError(t, err)
	require.True(t, ok)
	beforeDelete, err := v.Fetch(t.Context(), []string{originalID})
	require.NoError(t, err)
	require.Contains(t, beforeDelete, originalID)
	requireLayoutMetadata(t, original, beforeDelete[originalID])

	require.NoError(t, c.DeleteDocument(t.Context(), accepted.DocumentID))
	recreatedReq := domain.AcceptRequest{
		Document:       domain.Document{ID: accepted.DocumentID, Content: "v2 plain content"},
		Fingerprint:    chunker.Fingerprint("v2 plain content"),
		CallerSupplied: true,
		MaxPending:     1000,
	}
	recreated, err := c.AcceptDocument(t.Context(), recreatedReq)
	require.NoError(t, err)
	require.Equal(t, domain.AcceptAccepted, recreated.Outcome)
	job2, err := c.ClaimJob(t.Context(), "worker-b", time.Minute, 3)
	require.NoError(t, err)
	require.NotNil(t, job2)
	replacementID := uuid.NewString()
	ok, err = c.CompleteJob(t.Context(), *job2, []domain.Chunk{{
		ID: domain.ChunkID(replacementID), DocumentID: accepted.DocumentID, Index: 0, Text: "v2 plain", TokenCount: 2, Embedding: []float32{1, 0, 0},
	}}, domain.IndexObservation{})
	require.NoError(t, err)
	require.True(t, ok)

	fetched, err := v.Fetch(t.Context(), []string{originalID, replacementID})
	require.NoError(t, err)
	require.NotContains(t, fetched, originalID)
	require.Contains(t, fetched, replacementID)
	require.Equal(t, "v2 plain", fetched[replacementID].Text)
	requireLayoutMetadata(t, domain.Chunk{}, fetched[replacementID])

	// Upsert with a malformed batch fails before executing the batch, leaving existing rows intact; retry succeeds.
	doc := completedDocument(t, c)
	ctx := t.Context()
	require.NoError(t, v.Upsert(ctx, "", []domain.Chunk{
		{
			ID: "replace-key", DocumentID: doc, Index: 0, Text: "layout chunk", TokenCount: 2, Embedding: []float32{0.8, 0.6, 0},
			Metadata: map[string]any{"doc_type": "report", "pages": []int{3, 1, 3}, "table_ids": []string{"p1-t1"}, "row_ids": []string{"p1-t1-r2"}},
		},
	}))

	err = v.Upsert(ctx, "", []domain.Chunk{
		{ID: "good-in-bad-batch", DocumentID: doc, Index: 1, Text: "should rollback", TokenCount: 2, Embedding: []float32{0, 1, 0}},
		{ID: "bad-in-batch", DocumentID: doc, Index: 2, Text: "SECRET_BODY", TokenCount: 2, Embedding: []float32{0, 0, 1}, Metadata: map[string]any{"table_ids": []string{"invalid-token"}}},
	})
	require.Error(t, err)
	require.ErrorContains(t, err, "table_ids")
	require.NotContains(t, err.Error(), "SECRET_BODY")

	afterFailed, err := v.Fetch(ctx, []string{"good-in-bad-batch", "replace-key"})
	require.NoError(t, err)
	require.NotContains(t, afterFailed, "good-in-bad-batch")
	require.Equal(t, []int{3, 1}, afterFailed["replace-key"].Metadata["pages"])

	// Replace layout chunk with a plain chunk at the exact same chunk_id: ON CONFLICT clears stale arrays to NULL.
	require.NoError(t, v.Upsert(ctx, "", []domain.Chunk{
		{ID: "replace-key", DocumentID: doc, Index: 0, Text: "plain replacement", TokenCount: 2, Embedding: []float32{0.8, 0.6, 0}, Metadata: map[string]any{"doc_type": "report"}},
	}))
	replaced, err := v.Fetch(ctx, []string{"replace-key"})
	require.NoError(t, err)
	require.Contains(t, replaced, "replace-key")
	require.NotContains(t, replaced["replace-key"].Metadata, "pages")
	require.NotContains(t, replaced["replace-key"].Metadata, "table_ids")
	require.NotContains(t, replaced["replace-key"].Metadata, "row_ids")
	var rawNil bool
	require.NoError(t, c.Pool.QueryRow(ctx, `SELECT pages IS NULL AND table_ids IS NULL AND row_ids IS NULL FROM chunk_vectors WHERE chunk_id='replace-key'`).Scan(&rawNil))
	require.True(t, rawNil)

	// Retry valid second chunk.
	require.NoError(t, v.Upsert(ctx, "", []domain.Chunk{
		{ID: "good-in-bad-batch", DocumentID: doc, Index: 1, Text: "retried", TokenCount: 2, Embedding: []float32{0, 1, 0}, Metadata: map[string]any{"doc_type": "report", "pages": []int{4}, "table_ids": []string{"p1-t1"}}},
	}))
	retried, err := v.Fetch(ctx, []string{"good-in-bad-batch"})
	require.NoError(t, err)
	require.Equal(t, []int{4}, retried["good-in-bad-batch"].Metadata["pages"])
	require.Equal(t, []string{"p1-t1"}, retried["good-in-bad-batch"].Metadata["table_ids"])
	require.NotContains(t, retried["good-in-bad-batch"].Metadata, "row_ids")

	// Exact and ANN query return consistent sorting and no cross-document leakage.
	exactStore, err := OpenGenerationVectorStore(ctx, c, 3, LegacyGeneration, WithExactScopeMax(100))
	require.NoError(t, err)
	require.NoError(t, exactStore.EnsureExactScopeIndexes(ctx))
	hits, isExact, err := exactStore.QueryFilteredWithExactness(ctx, []float32{0.8, 0.6, 0}, 10, domain.VectorFilter{DocumentIDs: []string{string(doc)}})
	require.NoError(t, err)
	require.True(t, isExact)
	require.Len(t, hits, 2)
	require.Equal(t, domain.ChunkID("replace-key"), hits[0].Chunk.ID)
	require.NotContains(t, hits[0].Chunk.Metadata, "pages")
	require.Equal(t, domain.ChunkID("good-in-bad-batch"), hits[1].Chunk.ID)
	require.Equal(t, []int{4}, hits[1].Chunk.Metadata["pages"])
}

func TestPostgresLayoutMigrationRejectsInheritanceAndPartitions(t *testing.T) {
	for _, tc := range []struct {
		name, target, setup string
	}{
		{"local descendant", "chunk_vectors", "CREATE TABLE child (extra text) INHERITS (public.chunk_vectors)"},
		{"foreign-schema descendant", "chunk_vectors", "CREATE TABLE other_corpus.child (extra text) INHERITS (public.chunk_vectors)"},
		{"local child of foreign-schema parent", "chunk_vectors_child", "CREATE TABLE chunk_vectors_child (FOREIGN KEY (document_id) REFERENCES documents(id) ON DELETE CASCADE) INHERITS (other_corpus.parent)"},
		{"partitioned parent", "chunk_vectors_parts", "CREATE TABLE chunk_vectors_parts (document_id uuid REFERENCES documents(id) ON DELETE CASCADE, embedding vector(3)) PARTITION BY HASH (document_id)"},
		{"leaf partition", "chunk_vectors_leaf", "CREATE TABLE parts (document_id uuid REFERENCES documents(id) ON DELETE CASCADE, embedding vector(3)) PARTITION BY HASH (document_id); CREATE TABLE chunk_vectors_leaf PARTITION OF parts FOR VALUES WITH (MODULUS 1, REMAINDER 0)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := preLayoutVectorDatabase(t)
			grantExec(t, c, "CREATE SCHEMA other_corpus")
			grantExec(t, c, fmt.Sprintf(oldLayoutVectorDDL, "other_corpus.parent", "public.documents"))
			grantExec(t, c, fmt.Sprintf(oldLayoutVectorDDL, "chunk_vectors", "documents"))
			grantExec(t, c, tc.setup)
			grantExec(t, c, `INSERT INTO vector_generations(name,store,target,profile,profile_hash,profile_origin)
				VALUES('target','postgres',$1,'{}','h','recorded')`, tc.target)
			require.ErrorContains(t, c.RunMigrations(t.Context()), "uses inheritance or partitioning")
			requireNoLayoutMigration(t, c)
			grantExec(t, c, "DELETE FROM vector_generations WHERE name='target'")
			require.NoError(t, c.RunMigrations(t.Context()), "unregistered inheritance and partitions are skipped")
			var count int
			require.NoError(t, c.Pool.QueryRow(t.Context(), `SELECT count(*) FROM information_schema.columns
				WHERE column_name IN ('pages','table_ids','row_ids')
				AND (table_schema='other_corpus' OR table_name IN ('child','parts',$1))`, tc.target).Scan(&count))
			require.Zero(t, count)
		})
	}
}

func TestPostgresLayoutMigrationRejectsIncompatiblePartialColumns(t *testing.T) {
	for _, tc := range []struct {
		name, definition, badValue string
	}{
		{"pages", "integer[] DEFAULT ARRAY[7]", "'{}'::integer[]"},
		{"table_ids", "text[]", "ARRAY[['p1-t1','p1-t2']]"},
		{"row_ids", "text[]", "'[0:0]={p1-t1-r1}'::text[]"},
		{"pages", "integer[] GENERATED ALWAYS AS (ARRAY[chunk_index + 1]) STORED", ""},
		{"table_ids", "integer[]", ""},
		{"row_ids", "text[] NOT NULL DEFAULT ARRAY['p1-t1-r1']", ""},
	} {
		t.Run(tc.name+" "+tc.definition, func(t *testing.T) {
			c := preLayoutVectorDatabase(t)
			grantExec(t, c, fmt.Sprintf(oldLayoutVectorDDL, "chunk_vectors", "documents"))
			grantExec(t, c, fmt.Sprintf(oldLayoutVectorDDL, "chunk_vectors_partial", "documents"))
			grantExec(t, c, fmt.Sprintf("ALTER TABLE chunk_vectors_partial ADD COLUMN %s %s", tc.name, tc.definition))
			if tc.badValue != "" {
				doc := completedDocument(t, c)
				grantExec(t, c, fmt.Sprintf(`INSERT INTO chunk_vectors_partial(chunk_id,document_id,chunk_index,doc_type,text,token_count,embedding,%s)
					VALUES('bad',$1,0,'report','partial',1,'[1,0,0]',%s)`, tc.name, tc.badValue), string(doc))
			}
			require.ErrorContains(t, c.RunMigrations(t.Context()), tc.name)
			var cols, migrations int
			require.NoError(t, c.Pool.QueryRow(t.Context(), `SELECT
				(SELECT count(*) FROM pg_attribute WHERE attrelid='chunk_vectors'::regclass AND attname IN ('pages','table_ids','row_ids')),
				(SELECT count(*) FROM schema_migrations WHERE version=39)`).Scan(&cols, &migrations))
			require.Zero(t, cols, "other targets are rolled back when validation fails")
			require.Zero(t, migrations)
			if tc.badValue != "" {
				grantExec(t, c, fmt.Sprintf("UPDATE chunk_vectors_partial SET %s=NULL", tc.name))
				require.NoError(t, c.RunMigrations(t.Context()), "repairing invalid existing values permits retry")
			}
		})
	}
}

func TestPostgresLayoutMigrationPreservesUnrelatedConstraint(t *testing.T) {
	c := preLayoutVectorDatabase(t)
	grantExec(t, c, fmt.Sprintf(oldLayoutVectorDDL, "chunk_vectors", "documents"))
	grantExec(t, c, "ALTER TABLE chunk_vectors ADD CONSTRAINT quarry_layout_pages_check CHECK (chunk_index >= 0)")
	require.ErrorContains(t, c.RunMigrations(t.Context()), "rename that constraint")
	var present bool
	require.NoError(t, c.Pool.QueryRow(t.Context(), `SELECT EXISTS (
		SELECT 1 FROM pg_constraint WHERE conrelid='chunk_vectors'::regclass AND conname='quarry_layout_pages_check'
	)`).Scan(&present))
	require.True(t, present)
	requireNoLayoutMigration(t, c)
}

func preLayoutVectorDatabase(t *testing.T) *Client {
	t.Helper()
	c := migrationTestClient(t, pgtest.StartVector(t))
	applyMigrationPrefix(t, c, 38)
	grantExec(t, c, "CREATE EXTENSION IF NOT EXISTS vector")
	return c
}

func requireNoLayoutMigration(t *testing.T, c *Client) {
	t.Helper()
	var layoutCols, migrationCount int
	require.NoError(t, c.Pool.QueryRow(t.Context(), `SELECT
		(SELECT count(*) FROM information_schema.columns WHERE column_name IN ('pages','table_ids','row_ids')
		 AND (table_schema='other_corpus' OR table_name IN ('chunk_vectors','child','parts','chunk_vectors_parts','chunk_vectors_leaf','chunk_vectors_child'))),
		(SELECT count(*) FROM schema_migrations WHERE version=39)`).Scan(&layoutCols, &migrationCount))
	require.Zero(t, layoutCols)
	require.Zero(t, migrationCount)
}

func TestPostgresLayoutWriterCutoverAndLateGenerationRecovery(t *testing.T) {
	c := preLayoutVectorDatabase(t)
	ctx := t.Context()
	require.NoError(t, c.RunMigrations(ctx))
	legacy, err := NewVectorStore(ctx, c, 3)
	require.NoError(t, err)
	doc := completedDocument(t, c)
	chunk := domain.Chunk{ID: "layout", DocumentID: doc, Text: "layout", Embedding: []float32{1, 0, 0},
		Metadata: map[string]any{"pages": []int{2}, "table_ids": []string{"p1-t1"}, "row_ids": []string{"p1-t1-r1"}}}
	require.NoError(t, legacy.Upsert(ctx, "", []domain.Chunk{chunk}))

	oldUpsert := `INSERT INTO chunk_vectors(chunk_id,document_id,chunk_index,doc_type,text,token_count,embedding)
		VALUES('layout',$1,0,'report','old replacement',1,'[0,1,0]')
		ON CONFLICT(chunk_id) DO UPDATE SET text=EXCLUDED.text, embedding=EXCLUDED.embedding`
	// Reproduce the stale payload an old writer would leave if it retained access.
	tx, err := c.Pool.Begin(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback(context.Background()) })
	_, err = tx.Exec(ctx, oldUpsert, string(doc))
	require.NoError(t, err)
	var stale []int32
	require.NoError(t, tx.QueryRow(ctx, "SELECT pages FROM chunk_vectors WHERE chunk_id='layout' AND text='old replacement'").Scan(&stale))
	require.Equal(t, []int32{2}, stale)
	require.NoError(t, tx.Rollback(ctx))

	dsn := c.Pool.Config().ConnString()
	roles := newGrantRoles(t, dsn, "quarry_retired_writer")
	role := pgx.Identifier{roles.name("quarry_retired_writer")}.Sanitize()
	grantExec(t, c, "CREATE ROLE "+role)
	grantExec(t, c, "GRANT USAGE, CREATE ON SCHEMA public TO "+role)
	grantExec(t, c, "GRANT SELECT, INSERT, UPDATE, DELETE ON chunk_vectors TO "+role)
	// Retire writer privileges before the upgraded generation is reopened.
	grantExec(t, c, "REVOKE CREATE ON SCHEMA public FROM "+role)
	grantExec(t, c, "REVOKE INSERT, UPDATE, DELETE ON chunk_vectors FROM "+role)
	retiredReader := grantRoleClient(t, dsn, "SET SESSION AUTHORIZATION "+role)
	_, err = retiredReader.Pool.Exec(ctx, oldUpsert, string(doc))
	requireSQLState(t, err, "42501")
	lateDDL := fmt.Sprintf(oldLayoutVectorDDL, "chunk_vectors_late", "documents")
	_, err = retiredReader.Pool.Exec(ctx, lateDDL)
	requireSQLState(t, err, "42501")
	_, err = retiredReader.Pool.Exec(ctx, "SELECT text FROM chunk_vectors WHERE chunk_id='layout'")
	require.NoError(t, err)

	// A table left by a previous creator after the migration record needs owner repair.
	grantExec(t, c, lateDDL)
	grantExec(t, c, `INSERT INTO chunk_vectors_late(chunk_id,document_id,chunk_index,doc_type,text,token_count,embedding)
		VALUES('old',$1,0,'report','old row',1,'[1,0,0]')`, string(doc))
	require.NoError(t, c.RunMigrations(ctx))
	_, err = OpenGenerationVectorStore(ctx, c, 3, "late")
	require.ErrorContains(t, err, "follow migration 039 recovery in the deployment guide")
	migrationSQL, err := migrationFiles.ReadFile("migrations/039_vector_layout_provenance.sql")
	require.NoError(t, err)
	for range 2 {
		require.NoError(t, pgx.BeginFunc(ctx, c.Pool, func(tx pgx.Tx) error {
			if _, lockErr := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", migrationLockID); lockErr != nil {
				return lockErr
			}
			_, repairErr := tx.Exec(ctx, string(migrationSQL))
			return repairErr
		}))
	}
	requireRecordedMigrations(t, c)
	late, err := OpenGenerationVectorStore(ctx, c, 3, "late")
	require.NoError(t, err)
	repaired, err := late.Fetch(ctx, []string{"old"})
	require.NoError(t, err)
	require.Equal(t, "old row", repaired["old"].Text)
	requireLayoutMetadata(t, domain.Chunk{}, repaired["old"])
	retained, err := legacy.Fetch(ctx, []string{string(chunk.ID)})
	require.NoError(t, err)
	requireLayoutMetadata(t, chunk, retained[string(chunk.ID)])
}
