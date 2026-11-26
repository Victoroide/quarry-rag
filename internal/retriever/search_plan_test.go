package retriever

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPlanHostedSearchPricesEveryRoute(t *testing.T) {
	for _, tc := range []struct {
		name               string
		partitions, limit  int
		mode               string
		planned            int
		forceGlobal, fails bool
	}{
		{"an empty scope needs no search", 0, 10, "", 0, false, false},
		{"one partition fits the full automatic route", 1, 10, "", 3, false, false},
		{"auto prices a probe, a local pass and a top-up", 4, 10, "auto", 9, false, false},
		{"auto at the largest full route", 4, 9, "AUTO", 9, false, false},
		{"auto falls back to one global pass", 5, 10, "", 5, true, false},
		{"auto at the default ceiling is one global pass", 10, 10, "", 10, true, false},
		{"auto cannot fit even a global pass", 11, 10, "", 0, false, true},
		{"explicit global fits one pass", 10, 10, "global", 10, false, false},
		{"explicit global does not fit", 11, 10, "Global", 0, false, true},
		{"explicit local needs the full route", 4, 9, "local", 9, false, false},
		{"explicit local does not fit", 5, 10, "local", 0, false, true},
		{"explicit local with a single partition", 1, 3, "LOCAL", 3, false, false},
		{"explicit local with a single partition and a tight budget", 1, 2, "local", 0, false, true},
		{"a budget of one covers one partition globally", 1, 1, "global", 1, false, false},
		{"a budget of one forces the global pass", 1, 1, "", 1, true, false},
		{"a budget of two forces the global pass", 1, 2, "", 1, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			plan, err := PlanHostedSearch(tc.partitions, tc.limit, tc.mode)
			if tc.fails {
				require.ErrorIs(t, err, ErrSearchBudget)
				require.Zero(t, plan)
				return
			}
			require.NoError(t, err)
			require.Equal(t, SearchPlan{Planned: tc.planned, ForceGlobal: tc.forceGlobal}, plan)
		})
	}
}

func TestPlanHostedSearchNeverPlansBeyondTheLimit(t *testing.T) {
	for partitions := 0; partitions <= 12; partitions++ {
		for limit := 1; limit <= 10; limit++ {
			for _, mode := range []string{"", "auto", "local", "global"} {
				plan, err := PlanHostedSearch(partitions, limit, mode)
				if err == nil {
					require.LessOrEqual(t, plan.Planned, limit, "%d partitions, limit %d, mode %q", partitions, limit, mode)
				}
			}
		}
	}
}
