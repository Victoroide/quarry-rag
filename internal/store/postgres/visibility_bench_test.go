package postgres

import (
	"context"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/testutil/pgtest"
	"github.com/stretchr/testify/require"
)

func BenchmarkEvidenceSnapshotObservation(b *testing.B) {
	for _, tc := range []struct {
		name   string
		rows   int
		tagged bool
		cases  []string
	}{
		{"historical_1000", 1_000, false, []string{"unrestricted"}},
		{"historical_10000", 10_000, false, []string{"unrestricted"}},
		{"historical_100000", 100_000, false, []string{"unrestricted"}},
		{"historical_1000000", 1_000_000, false, []string{"unrestricted", "allowlist_1", "allowlist_100"}},
		{"tagged_1000000", 1_000_000, true, []string{"unrestricted"}},
	} {
		b.Run(tc.name, func(b *testing.B) {
			c, err := NewClient(b.Context(), pgtest.Start(b))
			require.NoError(b, err)
			b.Cleanup(func() { require.NoError(b, c.Close(context.Background())) })
			require.NoError(b, c.RunMigrations(b.Context()))
			_, err = c.Pool.Exec(b.Context(), `
INSERT INTO documents(content, status, vector_incarnation_required)
SELECT 'benchmark content', 'completed', $2 FROM generate_series(1, $1::integer)`, tc.rows, tc.tagged)
			require.NoError(b, err)

			visibility := Visibility{Pool: c.Pool}
			// The query uses the same observation with or without an allowlist.
			// Allowlist filtering applies after observation to hits from search.
			for _, name := range tc.cases {
				b.Run(name, func(b *testing.B) {
					b.ReportAllocs()
					for b.Loop() {
						if _, err := visibility.ObserveEvidenceSnapshot(b.Context()); err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		})
	}
}
