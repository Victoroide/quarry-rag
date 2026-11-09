package config

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
)

func TestLoadChunkOverlapBoundary(t *testing.T) {
	for _, size := range []int{2, 3, 5, 512, 2048, 8191, 8192} {
		for _, overlap := range []int{-1, 0, size / 2, size/2 + 1, size - 1, size} {
			t.Run(fmt.Sprintf("%d/%d", size, overlap), func(t *testing.T) {
				t.Setenv("GO_RAG_FAKE_MODE", "true")
				t.Setenv("DATABASE_URL", "host=localhost dbname=rag")
				t.Setenv("CHUNK_TOKEN_SIZE", strconv.Itoa(size))
				t.Setenv("CHUNK_OVERLAP", strconv.Itoa(overlap))
				cfg, err := Load()
				if overlap < 0 || overlap > size/2 {
					if err == nil || !strings.Contains(err.Error(), "CHUNK_OVERLAP") ||
						!strings.Contains(err.Error(), "CHUNK_TOKEN_SIZE/2") || !strings.Contains(err.Error(), strconv.Itoa(size/2)) {
						t.Fatalf("expected overlap error stating half-size limit %d, got %v", size/2, err)
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				if cfg.ChunkOverlap != overlap {
					t.Fatalf("overlap = %d, want %d", cfg.ChunkOverlap, overlap)
				}
			})
		}
	}
}

func TestChunkTokenSizeMinimum(t *testing.T) {
	cfg := baseValidConfig()
	cfg.ChunkOverlap = 0
	cfg.ChunkTokenSize = 1
	if err := cfg.validate(); err == nil || !strings.Contains(err.Error(), "CHUNK_TOKEN_SIZE") {
		t.Fatalf("one-token budget must be rejected: %v", err)
	}
	cfg.ChunkTokenSize = 2
	cfg.ChunkOverlap = 1
	if err := cfg.validate(); err != nil {
		t.Fatalf("two-token budget must be accepted: %v", err)
	}
}
