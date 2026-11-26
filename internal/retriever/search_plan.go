package retriever

import (
	"errors"
	"strings"
)

// ErrSearchBudget reports a route that cannot fit the request's hosted search
// budget. It is returned before anything is embedded or searched.
var ErrSearchBudget = errors.New("authorized scope cannot be searched within the request budget")

// SearchPlan prices a request's hosted searches before it spends anything.
type SearchPlan struct {
	// Planned is the most first attempts the chosen route can make. Retries may
	// use only what remains of the request's budget.
	Planned int
	// ForceGlobal selects one global pass up front instead of the automatic
	// route, because the full route would not fit.
	ForceGlobal bool
}

// PlanHostedSearch prices a scope that a hosted store searches in partitions
// requests, against a total of limit attempts. A global pass costs one request
// per partition. An automatic route can also need a probe of the whole scope, a
// local pass over one document and a sparse top-up, which is 2*partitions+1.
// When the full route does not fit, an automatic query takes one global pass if
// that fits. An explicit route that does not fit fails with ErrSearchBudget.
// An empty scope needs no search at all.
func PlanHostedSearch(partitions, limit int, mode string) (SearchPlan, error) {
	if partitions <= 0 {
		return SearchPlan{}, nil
	}
	global, full := partitions, 2*partitions+1
	switch {
	case strings.EqualFold(mode, string(RetrievalModeGlobal)):
		if global > limit {
			return SearchPlan{}, ErrSearchBudget
		}
		return SearchPlan{Planned: global}, nil
	case strings.EqualFold(mode, string(RetrievalModeLocal)):
		if full > limit {
			return SearchPlan{}, ErrSearchBudget
		}
		return SearchPlan{Planned: full}, nil
	case full <= limit:
		return SearchPlan{Planned: full}, nil
	case global <= limit:
		return SearchPlan{Planned: global, ForceGlobal: true}, nil
	default:
		return SearchPlan{}, ErrSearchBudget
	}
}
