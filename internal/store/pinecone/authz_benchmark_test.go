package pinecone

import (
	"fmt"
	"os"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/google/uuid"
)

// BenchmarkPineconeAuthzModes compares live Pinecone query latency between service-mode
// document_id allowlists and grants-mode tenant+incarnation filters across scope sizes.
// Set QUARRY_BENCHMARK_LIVE=1, PINECONE_API_KEY, and PINECONE_INDEX_NAME to run against a real index.
func BenchmarkPineconeAuthzModes(b *testing.B) {
	if os.Getenv("QUARRY_BENCHMARK_LIVE") != "1" {
		b.Skip("set QUARRY_BENCHMARK_LIVE=1 with PINECONE_API_KEY and PINECONE_INDEX_NAME to run live Pinecone benchmarks")
	}
	cfg, err := config.Load()
	if err != nil {
		b.Fatalf("load config: %v", err)
	}
	if cfg.PineconeAPIKey == "" || cfg.PineconeIndexName == "" {
		b.Skip("PINECONE_API_KEY and PINECONE_INDEX_NAME are required")
	}
	client, err := NewPineconeClient(b.Context(), cfg)
	if err != nil {
		b.Fatalf("connect pinecone: %v", err)
	}
	b.Cleanup(func() { _ = client.Close() })
	driver := NewDriver(client)

	dims := cfg.EmbeddingDimensions
	if dims <= 0 {
		dims = 1024
	}
	vec := make([]float32, dims)
	vec[0] = 1

	const maxAllowed = 25000
	ids := make([]string, maxAllowed)
	incarnations := make(map[string]string, maxAllowed)
	for i := range maxAllowed {
		id := uuid.NewString()
		ids[i] = id
		incarnations[id] = id
	}

	for _, size := range []int{0, 1, 10000, 25000} {
		b.Run(fmt.Sprintf("scope=%d/service", size), func(b *testing.B) {
			filter := domain.VectorFilter{DocumentIDs: ids[:size]}
			b.ResetTimer()
			for range b.N {
				if _, qErr := driver.QueryFiltered(b.Context(), vec, 10, filter); qErr != nil {
					b.Fatal(qErr)
				}
			}
		})
		b.Run(fmt.Sprintf("scope=%d/grants", size), func(b *testing.B) {
			scopedIncarnations := make(map[string]string, size)
			for _, id := range ids[:size] {
				scopedIncarnations[id] = incarnations[id]
			}
			filter := domain.VectorFilter{
				DocumentIDs: ids[:size],
				Authorized: &domain.AuthorizedScope{
					TenantID:     "00000000-0000-4000-8000-000000000001",
					PrincipalID:  "00000000-0000-4000-8000-000000000002",
					Incarnations: scopedIncarnations,
				},
			}
			b.ResetTimer()
			for range b.N {
				if _, qErr := driver.QueryFiltered(b.Context(), vec, 10, filter); qErr != nil {
					b.Fatal(qErr)
				}
			}
		})
	}
}
