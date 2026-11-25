package main

import (
	"context"
	"errors"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	pcstore "github.com/Victoroide/quarry-rag/internal/store/pinecone"
	"github.com/stretchr/testify/require"
)

type recordingVectorPin struct {
	target string
	host   string
}

func (p *recordingVectorPin) PinVectorGeneration(_ context.Context, _, _, target, host string, _ domain.EmbeddingProfile) error {
	p.target, p.host = target, host
	return nil
}

func TestFailedFirstPineconeOpenDoesNotBindLegacyTarget(t *testing.T) {
	cfg := &config.Config{VectorStore: "pinecone", PineconeIndexName: "typo"}
	pin := &recordingVectorPin{}
	missing := errors.New("index not found")
	open := func(_ context.Context, cfg *config.Config) (*pcstore.Client, error) {
		if cfg.PineconeIndexName == "typo" {
			return nil, missing
		}
		return pcstore.NewClientForIndex(pcstore.NewMemoryIndex(4), cfg, "quarry-host"), nil
	}

	_, err := openPineconeVectors(t.Context(), cfg, pin, open)
	require.ErrorIs(t, err, missing)
	require.Empty(t, pin.target)

	cfg.PineconeIndexName = "quarry"
	client, err := openPineconeVectors(t.Context(), cfg, pin, open)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	require.Equal(t, "quarry", pin.target)
	require.Equal(t, "quarry-host", pin.host)
}
