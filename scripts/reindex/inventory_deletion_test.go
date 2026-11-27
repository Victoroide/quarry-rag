package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	pgstore "github.com/Victoroide/quarry-rag/internal/store/postgres"
	"github.com/Victoroide/quarry-rag/internal/testutil/pgtest"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

func insertInventoryReceipt(t *testing.T, pg *pgstore.Client, id, incarnation string, held, finalized bool) {
	t.Helper()
	_, err := pg.Pool.Exec(t.Context(), `INSERT INTO document_deletions
 (document_id, incarnation, kind, vector_mode, held, finalized)
 VALUES ($1, $2, 'qualified_intent', 'local', $3, $4)`, id, incarnation, held, finalized)
	require.NoError(t, err)
}

func TestInventoryDeletionDocumentCount(t *testing.T) {
	tests := []struct {
		name           string
		status         string
		receipt        bool
		held           bool
		finalized      bool
		oldIncarnation bool
		unrelated      bool
		want           documentCounts
	}{
		{name: "empty"},
		{name: "deleting_without_receipt", status: "deleting", want: documentCounts{Deleting: 1}},
		{name: "deleting_released", status: "deleting", receipt: true, finalized: true, want: documentCounts{Deleting: 1}},
		{name: "completed_held", status: "completed", receipt: true, held: true, finalized: true, want: documentCounts{Completed: 1, Deleting: 1, WithoutOriginal: 1}},
		{name: "completed_open", status: "completed", receipt: true, want: documentCounts{Completed: 1, Deleting: 1, WithoutOriginal: 1}},
		{name: "processing_held", status: "processing", receipt: true, held: true, finalized: true, want: documentCounts{Processing: 1, Deleting: 1}},
		{name: "failed_open", status: "failed", receipt: true, want: documentCounts{Failed: 1, Deleting: 1}},
		{name: "completed_released", status: "completed", receipt: true, finalized: true, want: documentCounts{Completed: 1, WithoutOriginal: 1}},
		{name: "orphan_held", receipt: true, held: true, finalized: true},
		{name: "orphan_open", receipt: true},
		{name: "older_incarnation", status: "completed", receipt: true, held: true, finalized: true, oldIncarnation: true, want: documentCounts{Completed: 1, Deleting: 1, WithoutOriginal: 1}},
		{name: "unrelated_open", status: "completed", receipt: true, unrelated: true, want: documentCounts{Completed: 1, WithoutOriginal: 1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pg := vectorDB(t)
			doc := newDoc("manuals", "manual")
			doc.Incarnation = uuid.NewString()
			if tt.status != "" {
				doc.Status = tt.status
				insertDoc(t, pg, doc)
			}
			if tt.receipt {
				id, incarnation := doc.ID, doc.Incarnation
				if tt.unrelated {
					id = uuid.NewString()
				}
				if tt.oldIncarnation {
					incarnation = uuid.NewString()
				}
				insertInventoryReceipt(t, pg, id, incarnation, tt.held, tt.finalized)
			}
			var rep inventoryReport
			require.NoError(t, (deps{pg: pg}).countDocuments(t.Context(), &rep))
			require.Equal(t, tt.want, rep.Documents)
		})
	}

	t.Run("receipt_cap_and_stage_cursor", func(t *testing.T) {
		pg := vectorDB(t)
		h := newHarness(t, &fixture{pg: pg}, "postgres")
		const idCap = 20
		var receiptIDs []any
		for i := 1; i <= idCap+1; i++ {
			doc := newDoc("manuals", "manual", "blocked chunk")
			doc.ID = fmt.Sprintf("00000000-0000-4000-8000-%012d", i)
			doc.Status, doc.Incarnation = "deleting", uuid.NewString()
			insertDoc(t, pg, doc)
			insertInventoryReceipt(t, pg, doc.ID, doc.Incarnation, false, false)
			receiptIDs = append(receiptIDs, doc.ID)
		}
		insertInventoryReceipt(t, pg, "ffffffff-ffff-4fff-8fff-ffffffffffff", uuid.NewString(), false, false)
		cursor := "00000000-0000-4000-8000-000000000100"
		for _, id := range []string{cursor, "00000000-0000-4000-8000-000000000101", "00000000-0000-4000-8000-000000000102"} {
			doc := newDoc("manuals", "manual", "eligible chunk")
			doc.ID = id
			insertDoc(t, pg, doc)
		}

		opts := defaultOptions()
		opts.Generation, opts.Profile = "g2", h.profileFile("target.json", testProfile(sourceModel))
		opts.Page, opts.MaxDocuments, opts.After = 1, 1, cursor
		report, err := h.run(opts)
		require.NoError(t, err)
		docs := report["documents"].(map[string]any)
		require.EqualValues(t, idCap+1, docs["deleting_or_held"])
		require.EqualValues(t, 3, docs["completed"])
		require.EqualValues(t, 0, docs["processing"])
		require.EqualValues(t, 0, docs["failed"])
		require.EqualValues(t, idCap+2, report["open_deletions"])
		require.Equal(t, receiptIDs[:idCap], report["open_deletion_ids"])
		require.EqualValues(t, 1, report["plan"].(map[string]any)["documents_to_stage"])
		require.Equal(t, true, report["truncated"])

		opts.After = "00000000-0000-4000-8000-000000000102"
		report, err = h.run(opts)
		require.NoError(t, err)
		require.EqualValues(t, idCap+1, report["documents"].(map[string]any)["deleting_or_held"])
		require.EqualValues(t, idCap+2, report["open_deletions"])
		require.EqualValues(t, 0, report["plan"].(map[string]any)["documents_to_stage"])
		require.Equal(t, false, report["truncated"])
	})
}

func TestInventoryDeletionCountReadOnlyAndCancelable(t *testing.T) {
	owner := vectorDB(t)
	h := newHarness(t, &fixture{pg: owner}, "postgres")
	ctx := t.Context()
	doc := newDoc("manuals", "manual", "deleting chunk")
	doc.Status, doc.Incarnation = "deleting", uuid.NewString()
	insertDoc(t, owner, doc)
	insertInventoryReceipt(t, owner, doc.ID, doc.Incarnation, false, false)

	dsn := owner.Pool.Config().ConnString()
	role := pgtest.Role(t, dsn, "quarry_reindex_reader")
	roleIdent := pgx.Identifier{role}.Sanitize()
	_, err := owner.Pool.Exec(ctx, `CREATE ROLE `+roleIdent+` LOGIN PASSWORD 'fixture-password'`)
	require.NoError(t, err)
	grants := publishedDeploymentSQL(t, "GRANT USAGE ON SCHEMA public TO quarry_reindex")
	_, err = owner.Pool.Exec(ctx, replaceReindexRoleSQL(grants, role, ""))
	require.NoError(t, err)
	_, err = owner.Pool.Exec(ctx, `REVOKE INSERT, UPDATE, DELETE ON public.vector_generations,
 public.vector_generation_documents, public.vector_state FROM `+roleIdent)
	require.NoError(t, err)
	reader, err := pgstore.NewClient(ctx, dsn, func(cfg *pgxpool.Config) {
		cfg.ConnConfig.User = role
		cfg.ConnConfig.Password = "fixture-password"
		cfg.ConnConfig.RuntimeParams["default_transaction_read_only"] = "on"
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reader.Close(context.Background())) })
	h.deps.pg = reader

	opts := defaultOptions()
	opts.Generation, opts.Profile = "g2", h.profileFile("target.json", testProfile(sourceModel))
	requireInventory := func(count int, ids []string) {
		t.Helper()
		var out bytes.Buffer
		require.NoError(t, execute(ctx, h.deps, opts, &out, time.Millisecond))
		var rep inventoryReport
		require.NoError(t, json.Unmarshal(out.Bytes(), &rep))
		require.Equal(t, count, rep.Documents.Deleting)
		require.Equal(t, len(ids), rep.OpenDeletions)
		require.Equal(t, ids, rep.OpenDeletionIDs)
		require.Empty(t, rep.Blockers)
		require.Zero(t, rep.Writes)
	}
	receiptState := func() string {
		t.Helper()
		var state string
		require.NoError(t, owner.Pool.QueryRow(ctx, `SELECT to_jsonb(x)::text FROM document_deletions x WHERE document_id=$1`, doc.ID).Scan(&state))
		return state
	}

	beforeDocs, beforeReceipt := relationalState(t, owner), receiptState()
	requireInventory(1, []string{doc.ID})
	tx, err := owner.Pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(ctx) }()
	_, err = tx.Exec(ctx, `DELETE FROM documents WHERE id=$1`, doc.ID)
	require.NoError(t, err)
	_, err = tx.Exec(ctx, `UPDATE document_deletions SET finalized=TRUE, step=3 WHERE document_id=$1`, doc.ID)
	require.NoError(t, err)
	requireInventory(1, []string{doc.ID})
	require.Equal(t, beforeDocs, relationalState(t, owner))
	require.Equal(t, beforeReceipt, receiptState())
	require.NoError(t, tx.Commit(ctx))

	afterDocs, afterReceipt, afterCounts := relationalState(t, owner), receiptState(), writeTableCounts(t, owner)
	requireInventory(0, nil)

	var out bytes.Buffer
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	require.ErrorIs(t, execute(canceled, h.deps, opts, &out, time.Millisecond), context.Canceled)
	require.Empty(t, out.Bytes())

	_, err = owner.Pool.Exec(ctx, `REVOKE SELECT ON public.document_deletions FROM `+roleIdent)
	require.NoError(t, err)
	var denied *pgconn.PgError
	var report inventoryReport
	err = h.deps.countDocuments(ctx, &report)
	require.ErrorAs(t, err, &denied)
	require.Equal(t, "42501", denied.Code)
	require.Contains(t, denied.Message, "document_deletions")
	err = execute(ctx, h.deps, opts, &out, time.Millisecond)
	require.ErrorAs(t, err, &denied)
	require.Equal(t, "42501", denied.Code)
	require.Contains(t, denied.Message, "document_deletions")
	require.Empty(t, out.Bytes())
	require.Equal(t, afterDocs, relationalState(t, owner))
	require.Equal(t, afterReceipt, receiptState())
	require.Equal(t, afterCounts, writeTableCounts(t, owner))
	require.Zero(t, h.stores.opened)
	require.Nil(t, h.embed)
}
