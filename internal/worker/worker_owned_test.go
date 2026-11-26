package worker

import (
	"context"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/pipeline"
	"github.com/stretchr/testify/require"
)

type tenantStage struct{ got chan string }

func (tenantStage) ID() pipeline.StageID { return pipeline.StageChunk }
func (s tenantStage) Run(_ context.Context, ic *pipeline.IngestContext) error {
	s.got <- ic.Tenant
	return nil
}

func TestProcessorPassesTheOwningTenant(t *testing.T) {
	for _, tenant := range []string{"10000000-0000-4000-8000-000000000001", ""} {
		s := leaseStore(1)
		s.jobs[0].Tenant = tenant
		got := make(chan string, 1)
		w := leaseWorker(s, tenantStage{got: got}, 1)
		w.Start(t.Context())
		leaseEvent(t, s, "completed")
		w.Shutdown()
		require.Equal(t, tenant, <-got, "service-mode jobs carry no tenant")
	}
}
