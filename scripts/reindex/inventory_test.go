package main

import (
	"context"
	"errors"
	"math"
	"testing"

	pgstore "github.com/Victoroide/quarry-rag/internal/store/postgres"
	"github.com/stretchr/testify/require"
)

type inventoryPageRequest struct {
	after string
	limit int
}

type pagedSource struct {
	t             *testing.T
	ids           []string
	requests      []inventoryPageRequest
	probes        []string
	afterScan     string
	pageErr       error
	checkpointErr error
	probeErr      error
	unregistered  bool
}

func (s *pagedSource) DocumentRevisions(_ context.Context, after string, limit int) ([]pgstore.DocumentRevision, error) {
	s.requests = append(s.requests, inventoryPageRequest{after, limit})
	if len(s.requests) == 2 && s.pageErr != nil {
		return nil, s.pageErr
	}
	var page []pgstore.DocumentRevision
	for _, id := range s.ids {
		if id > after {
			page = append(page, pgstore.DocumentRevision{ID: id, Chunks: 1, DeclaredChunks: 1})
			if len(page) == limit {
				break
			}
		}
	}
	return page, nil
}

func (s *pagedSource) HasDocumentsAfter(_ context.Context, after string) (bool, error) {
	s.probes = append(s.probes, after)
	if s.probeErr != nil {
		return false, s.probeErr
	}
	if s.afterScan > after {
		return true, nil
	}
	for _, id := range s.ids {
		if id > after {
			return true, nil
		}
	}
	return false, nil
}

func (s *pagedSource) GenerationCheckpoints(_ context.Context, _ string, _ []string) (map[string]pgstore.Checkpoint, error) {
	s.t.Helper()
	require.False(s.t, s.unregistered, "unregistered inventory must skip checkpoint reads")
	if len(s.requests) == 2 {
		return nil, s.checkpointErr
	}
	return nil, nil
}

func (s *pagedSource) OrphanedGenerationDocuments(_ context.Context, _ string, _ int) ([]pgstore.Checkpoint, error) {
	s.t.Helper()
	require.False(s.t, s.unregistered, "unregistered inventory must skip orphan reads")
	return nil, nil
}

func TestScanPlanTruncation(t *testing.T) {
	t.Parallel()
	failure := errors.New("query failed")
	tests := []struct {
		name                     string
		opts                     options
		ids                      []string
		afterScan                string
		pageErr, cpErr, probeErr error
		unregistered, truncated  bool
		requests                 []inventoryPageRequest
		probes                   []string
		staged                   int
	}{
		{
			name: "empty", opts: options{MaxDocuments: 1, Page: 100},
			requests: []inventoryPageRequest{{"", 1}},
		},
		{
			name: "exact_limit_single_page", opts: options{MaxDocuments: 1, Page: 100},
			ids: []string{"a"}, requests: []inventoryPageRequest{{"", 1}},
			probes: []string{"a"}, staged: 1,
		},
		{
			name: "more_remain", opts: options{MaxDocuments: 1, Page: 100},
			ids: []string{"a", "b"}, requests: []inventoryPageRequest{{"", 1}},
			probes: []string{"a"}, staged: 1, truncated: true,
		},
		{
			name: "exact_limit_over_pages", opts: options{MaxDocuments: 4, Page: 2},
			ids: []string{"a", "b", "c", "d"}, requests: []inventoryPageRequest{{"", 2}, {"b", 2}},
			probes: []string{"d"}, staged: 4,
		},
		{
			name: "short_page_stops", opts: options{MaxDocuments: 4, Page: 2},
			ids: []string{"a", "b", "c"}, requests: []inventoryPageRequest{{"", 2}, {"b", 2}},
			staged: 3,
		},
		{
			name: "page_above_limit", opts: options{MaxDocuments: 3, Page: 1000},
			ids: []string{"a", "b", "c"}, requests: []inventoryPageRequest{{"", 3}},
			probes: []string{"c"}, staged: 3,
		},
		{
			name: "after_past_end", opts: options{MaxDocuments: 1, Page: 100, After: "z"},
			ids: []string{"a", "b"}, requests: []inventoryPageRequest{{"z", 1}},
		},
		{
			name: "after_offset", opts: options{MaxDocuments: 2, Page: 100, After: "a"},
			ids: []string{"a", "b", "c"}, requests: []inventoryPageRequest{{"a", 2}},
			probes: []string{"c"}, staged: 2,
		},
		{
			name: "probe_error", opts: options{MaxDocuments: 1, Page: 100},
			ids: []string{"a"}, requests: []inventoryPageRequest{{"", 1}},
			probes: []string{"a"}, probeErr: failure,
		},
		{
			name: "page_error", opts: options{MaxDocuments: 2, Page: 1},
			ids: []string{"a", "b"}, requests: []inventoryPageRequest{{"", 1}, {"a", 1}},
			pageErr: failure,
		},
		{
			name: "checkpoint_error", opts: options{MaxDocuments: 2, Page: 1},
			ids: []string{"a", "b"}, requests: []inventoryPageRequest{{"", 1}, {"a", 1}},
			cpErr: failure,
		},
		{
			name: "max_int_limit", opts: options{MaxDocuments: math.MaxInt, Page: 1000},
			ids: []string{"a", "b", "c"}, requests: []inventoryPageRequest{{"", 1000}},
			staged: 3,
		},
		{
			name: "unregistered", opts: options{MaxDocuments: 1, Page: 100},
			ids: []string{"a"}, requests: []inventoryPageRequest{{"", 1}},
			probes: []string{"a"}, staged: 1, unregistered: true,
		},
		{
			name: "appears_after_scan", opts: options{MaxDocuments: 1, Page: 100},
			ids: []string{"a"}, requests: []inventoryPageRequest{{"", 1}},
			afterScan: "b", probes: []string{"a"}, staged: 1, truncated: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			src := pagedSource{
				t: t, ids: tt.ids, afterScan: tt.afterScan, unregistered: tt.unregistered,
				pageErr: tt.pageErr, checkpointErr: tt.cpErr, probeErr: tt.probeErr,
			}
			var report inventoryReport
			err := scanPlan(t.Context(), &src, tt.opts, targetPlan{generation: "g2", store: "postgres"}, !tt.unregistered, &report)
			require.Equal(t, tt.requests, src.requests)
			require.Equal(t, tt.probes, src.probes)
			if tt.pageErr != nil || tt.cpErr != nil || tt.probeErr != nil {
				require.Equal(t, failure, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.truncated, report.Truncated)
			require.Equal(t, tt.staged, report.Plan.DocumentsToStage)
			require.Equal(t, ledgerCounts{Missing: tt.staged}, report.Ledger)
		})
	}
}
