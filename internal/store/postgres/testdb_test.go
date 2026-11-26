package postgres

import (
	"testing"

	"github.com/Victoroide/quarry-rag/internal/testutil/pgtest"
)

func startTestDatabase(t *testing.T) string {
	t.Helper()
	return pgtest.Start(t)
}
