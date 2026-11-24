package metrics

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestServiceIndexVisibilityExposition(t *testing.T) {
	service := NewService(nil)
	service.IndexVisibility.WithLabelValues("visible").Inc()
	service.IndexVisibility.WithLabelValues("timeout").Inc()
	service.IndexVisibility.WithLabelValues("visible").Inc()
	var output strings.Builder
	require.NoError(t, service.Registry.WriteText(&output))
	require.Contains(t, output.String(), "# TYPE quarry_index_visibility_waits_total counter\n")
	require.Contains(t, output.String(), "quarry_index_visibility_waits_total{result=\"visible\"} 2\n")
	require.Contains(t, output.String(), "quarry_index_visibility_waits_total{result=\"timeout\"} 1\n")
}

func TestServicePGVectorSearchExposition(t *testing.T) {
	service := NewService(nil)
	service.PGVectorSearch.WithLabelValues("exact", "ok").Inc()
	service.PGVectorSearch.WithLabelValues("hnsw", "partial").Inc()
	service.PGVectorSearch.WithLabelValues("probe", "timeout").Inc()
	var output strings.Builder
	require.NoError(t, service.Registry.WriteText(&output))
	require.Contains(t, output.String(), "# TYPE quarry_pgvector_search_total counter\n")
	require.Contains(t, output.String(), "quarry_pgvector_search_total{strategy=\"exact\",status=\"ok\"} 1\n")
	require.Contains(t, output.String(), "quarry_pgvector_search_total{strategy=\"hnsw\",status=\"partial\"} 1\n")
	require.Contains(t, output.String(), "quarry_pgvector_search_total{strategy=\"probe\",status=\"timeout\"} 1\n")
}
