// Package api assembles the HTTP router with all handlers and middleware.
package api

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net/http"

	"github.com/Victoroide/quarry-rag/internal/api/handler"
	"github.com/Victoroide/quarry-rag/internal/api/middleware"
	"github.com/Victoroide/quarry-rag/internal/citationsupport"
	"github.com/Victoroide/quarry-rag/internal/cleanup"
	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/extractor"
	"github.com/Victoroide/quarry-rag/internal/llm"
	"github.com/Victoroide/quarry-rag/internal/metrics"
	"github.com/Victoroide/quarry-rag/internal/pipeline"
	"github.com/Victoroide/quarry-rag/internal/retriever"
	s3store "github.com/Victoroide/quarry-rag/internal/store/s3"
	"github.com/Victoroide/quarry-rag/internal/tracing"
	"github.com/Victoroide/quarry-rag/internal/worker"
	"github.com/jackc/pgx/v5/pgxpool"
)

// RouterDeps groups the dependencies required to construct the HTTP router.
type RouterDeps struct {
	// FixtureVectorIdentity names the durable vector store that evaluation
	// fixture receipts bind to. Empty disables fixture admission.
	FixtureVectorIdentity string
	VectorStore           domain.VectorStore
	ChunkStore            domain.ChunkStore
	Retriever             *retriever.Retriever
	Ambassador            *llm.Ambassador
	Config                *config.Config
	Worker                *worker.Worker
	S3Client              *s3store.Client
	PineconeDriver        domain.VectorDeleter
	PGPool                *pgxpool.Pool
	Probes                *handler.Probes
	Metrics               *metrics.Service
	Reranker              retriever.Reranker
	NoEvidenceCatalog     *pipeline.NoEvidenceCatalog
	CitationVerifier      *citationsupport.Verifier
}

// NewRouter wires all HTTP routes, handlers, and middleware.
func NewRouter(deps *RouterDeps) http.Handler {
	mux := http.NewServeMux()
	// Service-to-service auth: when service tokens are set, every route except the
	// public probes requires a matching X-Service-Token header. The presigned
	// file GET is included because it mints S3 download URLs for any document id
	// and must never be reachable without the trusted caller in front of it.
	tokens := deps.Config.ServiceTokens
	if len(tokens) == 0 && deps.Config.ServiceToken != "" {
		tokens = []string{deps.Config.ServiceToken}
	}
	middleware.LogServiceAuthState(tokens)
	ingestOpts := []handler.IngestOption{
		handler.WithMaxPending(deps.Config.WorkerQueueSize),
		handler.WithUploadLimiter(extractor.NewLimiter(deps.Config.ExtractConcurrency, deps.Config.ExtractQueueTimeout)),
		handler.WithContentDedup(deps.Config.IngestContentDedup),
		handler.WithOCR(deps.Config.OCRProvider == "textract" && !deps.Config.FakeMode),
	}
	if deps.Config.ExtractIsolation {
		sb, err := extractor.NewSandbox(extractor.SandboxConfig{
			Timeout: deps.Config.ExtractTimeout, MemoryLimitMB: deps.Config.ExtractMemoryLimitMB,
			DOCStories: deps.Config.DOCStories, DOCXStories: deps.Config.DOCXStories,
		})
		if err != nil {
			slog.Error("initialize document extraction", "error", err)
			ingestOpts = append(ingestOpts, handler.WithExtractor(func(context.Context, extractor.FileType, []byte) (string, error) {
				return "", extractor.ErrExtractionUnavailable
			}))
			if deps.Config.OCRPDFMode == config.OCRPDFMixed {
				ingestOpts = append(ingestOpts, handler.WithMixedPDFOCR(func(context.Context, []byte) (domain.PDFPages, error) {
					return nil, extractor.ErrExtractionUnavailable
				}))
			}
		} else {
			ingestOpts = append(ingestOpts, handler.WithExtractor(tracing.WrapExtract(sb.Extract)))
			if deps.Config.OCRPDFMode == config.OCRPDFMixed {
				ingestOpts = append(ingestOpts, handler.WithMixedPDFOCR(sb.ExtractPDFPages))
			}
		}
	} else {
		docStories, docxStories := deps.Config.DOCStories, deps.Config.DOCXStories
		ingestOpts = append(ingestOpts, handler.WithExtractor(tracing.WrapExtract(func(ctx context.Context, ft extractor.FileType, data []byte) (string, error) {
			return extractor.NewWithStories(ft, docStories, docxStories).Extract(ctx, bytes.NewReader(data))
		})))
	}
	if deps.Config.SourceUploadJournal {
		ingestOpts = append(ingestOpts, handler.WithSourceUploadJournal(sourceUploadJournal(deps.ChunkStore)))
	}
	probes := deps.Probes
	if probes == nil {
		probes = handler.NewProbes(deps.VectorStore, deps.ChunkStore, deps.S3Client, deps.Config.HealthCacheTTL)
	}
	mux.HandleFunc("GET /health", handler.HealthHandler(probes))
	mux.HandleFunc("GET /livez", handler.LivenessHandler())
	mux.HandleFunc("GET /readyz", handler.ReadinessHandler(probes))
	var objects handler.ObjectStore
	if deps.S3Client != nil {
		objects = tracing.WrapObjectStore(deps.S3Client)
	}
	mux.HandleFunc("POST /ingest", middleware.RequireServiceTokens(tokens, handler.IngestHandler(deps.Worker, deps.ChunkStore, objects, deps.Config.MaxUploadBytes, ingestOpts...)))
	queryOpts := []handler.QueryOption{handler.WithReranker(deps.Reranker), handler.WithNoEvidenceCatalog(deps.NoEvidenceCatalog)}
	if deps.CitationVerifier != nil {
		queryOpts = append(queryOpts, handler.WithCitationSupport(deps.CitationVerifier))
	}
	mux.HandleFunc("POST /query", middleware.RequireServiceTokens(tokens, handler.QueryHandler(deps.Retriever, deps.Ambassador, deps.Config, deps.PGPool, queryOpts...)))
	mux.HandleFunc("GET /documents", middleware.RequireServiceTokens(tokens, handler.ListDocumentsHandler(deps.ChunkStore)))
	mux.HandleFunc("GET /documents/{doc_id}", middleware.RequireServiceTokens(tokens, handler.DocumentStatusHandler(deps.ChunkStore)))
	var signer handler.FileSigner
	if deps.S3Client != nil {
		signer = deps.S3Client
	}
	mux.HandleFunc("GET /documents/{doc_id}/file", middleware.RequireServiceTokens(tokens, handler.FileHandler(deps.ChunkStore, signer)))
	mux.HandleFunc("DELETE /documents/{doc_id}", middleware.RequireServiceTokens(tokens, handler.DeleteHandler(deps.ChunkStore, deps.PineconeDriver, objects,
		handler.WithCleanupTargets(deletionTargets(deps.Config)))))
	// Offline fake mode keeps vectors in process memory. A receipt bound to a
	// store that disappears on restart could never be proved clean again.
	fixtureStore := deps.FixtureVectorIdentity
	if deps.Config.FakeMode {
		fixtureStore = ""
	}
	mux.HandleFunc("POST /evaluation/fixtures", middleware.RequireServiceTokens(tokens, handler.EvaluationFixtureIngestHandler(deps.Worker, deps.ChunkStore,
		handler.WithEvaluationFixtures(deps.Config.EvaluationCollectionPrefix, fixtureStore),
		handler.WithMaxPending(deps.Config.WorkerQueueSize), handler.WithContentDedup(deps.Config.IngestContentDedup))))
	mux.HandleFunc("GET /evaluation/fixtures/{doc_id}", middleware.RequireServiceTokens(tokens, handler.EvaluationFixtureStatusHandler(deps.ChunkStore, deps.Config.EvaluationCollectionPrefix)))
	mux.HandleFunc("DELETE /evaluation/fixtures/{doc_id}", middleware.RequireServiceTokens(tokens, handler.EvaluationFixtureDeleteHandler(deps.ChunkStore, deps.PineconeDriver,
		handler.EvaluationFixtureOptions{Prefix: deps.Config.EvaluationCollectionPrefix, VectorIdentity: fixtureStore, LocalVectors: deps.Config.VectorStore == "postgres" && !deps.Config.FakeMode})))
	return middleware.RequestID(middleware.Metrics(middleware.Tracing(middleware.Logging(middleware.Recovery(mux)), deps.Config.Tracing.CaptureContent), deps.Metrics))
}

// sourceUploadJournal returns the store's journal, or one that refuses every
// upload so a store without journaling never writes untracked originals.
func sourceUploadJournal(store domain.ChunkStore) domain.SourceUploadJournal {
	if journal, ok := store.(domain.SourceUploadJournal); ok {
		return journal
	}
	slog.Error("SOURCE_UPLOAD_JOURNAL is enabled but the document store cannot journal uploads")
	return unavailableJournal{}
}

type unavailableJournal struct{}

var errJournalUnavailable = errors.New("source upload journal is unavailable")

func (unavailableJournal) StageSourceUpload(context.Context, domain.SourceUpload) error {
	return errJournalUnavailable
}

func (unavailableJournal) BeginSourceUpload(context.Context, string) error {
	return errJournalUnavailable
}

func (unavailableJournal) RecordSourceUploadResult(context.Context, string, domain.SourceUploadResult) error {
	return errJournalUnavailable
}

// deletionTargets names the stores checkpointed deletion acts on.
func deletionTargets(cfg *config.Config) cleanup.Targets {
	targets := cleanup.Targets{SourceBucket: cfg.S3Bucket, InMemory: cfg.FakeMode}
	if !cfg.FakeMode && cfg.VectorStore != "postgres" {
		targets.VectorIndex = cfg.PineconeIndexName
	}
	return targets
}
