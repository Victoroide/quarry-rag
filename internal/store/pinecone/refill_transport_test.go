package pinecone

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	sdk "github.com/pinecone-io/go-pinecone/v5/pinecone"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/http2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func TestBudgetedQueryCountsOutboundStreamsThroughSDK(t *testing.T) {
	for _, budget := range []int{1, 2} {
		t.Run(strconv.Itoa(budget), func(t *testing.T) {
			var listen net.ListenConfig
			listener, err := listen.Listen(t.Context(), "tcp", "127.0.0.1:0")
			require.NoError(t, err)
			t.Cleanup(func() { _ = listener.Close() })
			control := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				encodeErr := json.NewEncoder(w).Encode(map[string]any{
					"name": "fixture", "dimension": 1, "metric": "cosine", "host": "https://" + listener.Addr().String(),
					"status": map[string]any{"ready": true, "state": "Ready"},
					"spec":   map[string]any{"serverless": map[string]string{"cloud": "aws", "region": "us-east-1"}},
				})
				if encodeErr != nil {
					t.Error(encodeErr)
				}
			}))
			t.Cleanup(control.Close)
			roots := x509.NewCertPool()
			roots.AddCert(control.Certificate())
			tlsListener := tls.NewListener(listener, &tls.Config{
				Certificates: control.TLS.Certificates, MinVersion: tls.VersionTLS12, NextProtos: []string{"h2"},
			})
			var streams atomic.Int32
			served := make(chan error, 1)
			go func() { served <- refuseQueryStreams(tlsListener, &streams) }()
			pc, err := sdk.NewClient(sdk.NewClientParams{ApiKey: "fixture", Host: control.URL, RestClient: control.Client()})
			require.NoError(t, err)
			cfg := &config.Config{
				PineconeIndexName: "fixture", PineconeTimeout: 5 * time.Second, PineconeMaxAttempts: 3,
				EmbeddingDimensions: 1, HostedVisibilityRefill: true,
			}
			client, err := newPineconeClient(t.Context(), cfg, pc, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12})))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, client.Close()) })
			_, used, err := NewDriver(client).QueryFilteredBudgeted(t.Context(), []float32{1}, 4, domain.VectorFilter{}, budget)
			require.ErrorIs(t, err, domain.ErrVectorQueryBudget)
			require.Equal(t, budget, used)
			require.Equal(t, budget, int(streams.Load()))
			require.NoError(t, client.Close())
			require.NoError(t, <-served)
		})
	}
}

func refuseQueryStreams(listener net.Listener, streams *atomic.Int32) error {
	conn, err := listener.Accept()
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	if err := conn.SetDeadline(time.Now().Add(15 * time.Second)); err != nil {
		return err
	}
	preface := make([]byte, len(http2.ClientPreface))
	if _, err := io.ReadFull(conn, preface); err != nil {
		return err
	}
	if string(preface) != http2.ClientPreface {
		return errors.New("unexpected HTTP/2 preface")
	}
	framer := http2.NewFramer(conn, conn)
	if err := framer.WriteSettings(); err != nil {
		return err
	}
	for {
		frame, err := framer.ReadFrame()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		switch f := frame.(type) {
		case *http2.SettingsFrame:
			if !f.IsAck() {
				if err := framer.WriteSettingsAck(); err != nil {
					return err
				}
			}
		case *http2.HeadersFrame:
			// Refused streams are eligible for a transparent gRPC retry.
			streams.Add(1)
			if err := framer.WriteRSTStream(f.StreamID, http2.ErrCodeRefusedStream); err != nil {
				return err
			}
		}
	}
}
