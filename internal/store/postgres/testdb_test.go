package postgres

import (
	"os"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/testutil/pgtest"
)

// parallelDatabaseTest overlaps isolated fixtures on shared services when enabled.
// Call it before creating the fixture or starting any test deadlines.
func parallelDatabaseTest(t *testing.T) {
	t.Helper()
	if sharedDatabaseParallelEnabled() {
		t.Parallel()
	}
}

func sharedDatabaseParallelEnabled() bool {
	return os.Getenv("QUARRY_TEST_SHARED_DB_PARALLEL") == "1" &&
		os.Getenv("QUARRY_TEST_POSTGRES_ADMIN_DSN") != "" &&
		os.Getenv("QUARRY_TEST_PGVECTOR_ADMIN_DSN") != ""
}

func TestSharedDatabaseParallelOptIn(t *testing.T) {
	const configured = "configured"
	tests := []struct {
		name, optIn, postgres, vector string
		want                          bool
	}{
		{name: "both services without opt-in", postgres: configured, vector: configured},
		{name: "disabled", optIn: "0", postgres: configured, vector: configured},
		{name: "unrecognized opt-in", optIn: "yes", postgres: configured, vector: configured},
		{name: "without shared services", optIn: "1"},
		{name: "only postgres", optIn: "1", postgres: configured},
		{name: "only pgvector", optIn: "1", vector: configured},
		{name: "opted in with both services", optIn: "1", postgres: configured, vector: configured, want: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv("QUARRY_TEST_SHARED_DB_PARALLEL", test.optIn)
			t.Setenv("QUARRY_TEST_POSTGRES_ADMIN_DSN", test.postgres)
			t.Setenv("QUARRY_TEST_PGVECTOR_ADMIN_DSN", test.vector)
			if got := sharedDatabaseParallelEnabled(); got != test.want {
				t.Errorf("shared database parallel scheduling = %t, want %t", got, test.want)
			}
		})
	}
}

func startTestDatabase(t *testing.T) string {
	t.Helper()
	return pgtest.Start(t)
}
