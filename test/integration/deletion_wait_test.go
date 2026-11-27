package integration

import "testing"

func testAbandonedQualifiedDeletionBatch(t *testing.T) {
	t.Helper()
	checkAbandonedDeletionBatch(t, duplicateDeletionRequests)
}
