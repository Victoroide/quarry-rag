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
	"github.com/Victoroide/quarry-rag/internal/subjectauth"
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
	SubjectVerifier       *subjectauth.Verifier
	SubjectResolver       middleware.SubjectResolver
	ReadAuthority         domain.ReadAuthority
	AuditRecorder         domain.SecurityAuditRecorder
	// LegacyDeletionReservations makes ID-only deletion reserve its targets under
	// the activation lock. The server entry point leaves it off until the
	// deployment can also quarantine, release and activate.
	LegacyDeletionReservations bool
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
	needsPDFPages := deps.Config.OCRPDFMode == config.OCRPDFMixed || deps.Config.PDFLayoutMode == config.PDFLayoutTextract
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
			if needsPDFPages {
				ingestOpts = append(ingestOpts, handler.WithMixedPDFOCR(func(context.Context, []byte) (domain.PDFPages, error) {
					return nil, extractor.ErrExtractionUnavailable
				}))
			}
		} else {
			ingestOpts = append(ingestOpts, handler.WithExtractor(tracing.WrapExtract(sb.Extract)))
			if needsPDFPages {
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
	if deps.Config.PDFLayoutMode == config.PDFLayoutTextract {
		ingestOpts = append(ingestOpts, handler.WithPDFLayout(true))
	}
	probes := deps.Probes
	if probes == nil {
		probes = handler.NewProbes(deps.VectorStore, deps.ChunkStore, deps.S3Client, deps.Config.HealthCacheTTL)
	}
	grantsMode := deps.Config.AuthzMode == config.AuthzModeGrants
	var modeReader domain.CorpusModeReader
	if reader, ok := deps.ChunkStore.(domain.CorpusModeReader); ok {
		modeReader = reader
	}
	if grantsMode {
		var trustCheck func() error
		if deps.SubjectVerifier != nil {
			trustCheck = deps.SubjectVerifier.ValidateTrust
		}
		probes.SetAuthzChecks(config.AuthzModeGrants, trustCheck)
	} else if modeReader != nil {
		probes.SetAuthzChecks(config.AuthzModeService, nil)
	}
	mux.HandleFunc("GET /health", handler.HealthHandler(probes))
	mux.HandleFunc("GET /livez", handler.LivenessHandler())
	mux.HandleFunc("GET /readyz", handler.ReadinessHandler(probes))
	var objects handler.ObjectStore
	if deps.S3Client != nil {
		objects = tracing.WrapObjectStore(deps.S3Client)
	}
	protectData := func(next http.HandlerFunc) http.HandlerFunc {
		if grantsMode {
			return middleware.RequireSubjectWithAudit(tokens, deps.SubjectVerifier, deps.SubjectResolver, deps.AuditRecorder,
				middleware.RequireAuthzMode(config.AuthzModeGrants, modeReader, next))
		}
		return middleware.RequireServiceTokens(tokens, middleware.RequireAuthzMode(config.AuthzModeService, modeReader, next))
	}
	if grantsMode {
		ingestOpts = append(ingestOpts, handler.WithOwnedIngestion(), handler.WithIngestAudit(deps.AuditRecorder))
	}
	mux.HandleFunc("POST /ingest", protectData(handler.IngestHandler(deps.Worker, deps.ChunkStore, objects, deps.Config.MaxUploadBytes, ingestOpts...)))
	queryOpts := []handler.QueryOption{handler.WithReranker(deps.Reranker), handler.WithNoEvidenceCatalog(deps.NoEvidenceCatalog)}
	if deps.CitationVerifier != nil {
		queryOpts = append(queryOpts, handler.WithCitationSupport(deps.CitationVerifier))
	}
	if grantsMode {
		queryOpts = append(queryOpts, handler.WithAuthorizedRetrieval(deps.ReadAuthority), handler.WithQueryAudit(deps.AuditRecorder))
	}
	mux.HandleFunc("POST /query", protectData(handler.QueryHandler(deps.Retriever, deps.Ambassador, deps.Config, deps.PGPool, queryOpts...)))
	var listOpts []handler.DocumentListOption
	var statusOpts []handler.DocumentStatusOption
	var fileOpts []handler.FileOption
	if grantsMode {
		listOpts = append(listOpts, handler.WithScopedListing(), handler.WithListingAudit(deps.AuditRecorder))
		statusOpts = append(statusOpts, handler.WithScopedStatus(), handler.WithStatusAudit(deps.AuditRecorder))
		fileOpts = append(fileOpts, handler.WithScopedFile(), handler.WithFileAudit(deps.AuditRecorder))
	}
	mux.HandleFunc("GET /documents", protectData(handler.ListDocumentsHandler(deps.ChunkStore, listOpts...)))
	mux.HandleFunc("GET /documents/{doc_id}", protectData(handler.DocumentStatusHandler(deps.ChunkStore, statusOpts...)))
	var signer handler.FileSigner
	if deps.S3Client != nil {
		signer = deps.S3Client
	}
	mux.HandleFunc("GET /documents/{doc_id}/file", protectData(handler.FileHandler(deps.ChunkStore, signer, fileOpts...)))
	deleteOpts := []handler.DeleteOption{handler.WithCleanupTargets(deletionTargets(deps.Config, deps.VectorStore))}
	if deps.LegacyDeletionReservations {
		deleteOpts = append(deleteOpts, handler.WithLegacyReservations())
	}
	if grantsMode {
		deleteOpts = append(deleteOpts, handler.WithScopedDeletion(), handler.WithDeleteAudit(deps.AuditRecorder))
	}
	mux.HandleFunc("DELETE /documents/{doc_id}", protectData(handler.DeleteHandler(deps.ChunkStore, deps.PineconeDriver, objects, deleteOpts...)))
	// Offline fake mode keeps vectors in process memory. A receipt bound to a
	// store that disappears on restart could never be proved clean again.
	fixtureStore := deps.FixtureVectorIdentity
	if deps.Config.FakeMode {
		fixtureStore = ""
	}
	var postFixture, getFixture, deleteFixture http.HandlerFunc
	if grantsMode {
		fixturesDisabled := func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Cache-Control", "private, no-store")
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"error":"unavailable","message":"evaluation fixtures are disabled in grants mode"}` + "\n"))
		}
		postFixture, getFixture, deleteFixture = fixturesDisabled, fixturesDisabled, fixturesDisabled
	} else {
		postFixture = handler.EvaluationFixtureIngestHandler(deps.Worker, deps.ChunkStore,
			handler.WithEvaluationFixtures(deps.Config.EvaluationCollectionPrefix, fixtureStore),
			handler.WithMaxPending(deps.Config.WorkerQueueSize), handler.WithContentDedup(deps.Config.IngestContentDedup))
		getFixture = handler.EvaluationFixtureStatusHandler(deps.ChunkStore, deps.Config.EvaluationCollectionPrefix)
		deleteFixture = handler.EvaluationFixtureDeleteHandler(deps.ChunkStore, deps.PineconeDriver,
			handler.EvaluationFixtureOptions{Prefix: deps.Config.EvaluationCollectionPrefix, VectorIdentity: fixtureStore, LocalVectors: deps.Config.VectorStore == "postgres" && !deps.Config.FakeMode})
	}
	mux.HandleFunc("POST /evaluation/fixtures", middleware.RequireServiceTokens(tokens, postFixture))
	mux.HandleFunc("GET /evaluation/fixtures/{doc_id}", middleware.RequireServiceTokens(tokens, getFixture))
	mux.HandleFunc("DELETE /evaluation/fixtures/{doc_id}", middleware.RequireServiceTokens(tokens, deleteFixture))
	logged := middleware.Logging(middleware.Recovery(mux))
	if grantsMode {
		logged = middleware.LoggingWithRouteTemplates(middleware.Recovery(mux))
	}
	return middleware.RequestID(middleware.Metrics(middleware.TracingWithRedaction(logged, deps.Config.Tracing.CaptureContent, grantsMode), deps.Metrics))
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
func deletionTargets(cfg *config.Config, vectors domain.VectorStore) cleanup.Targets {
	targets := cleanup.Targets{SourceBucket: cfg.S3Bucket, InMemory: cfg.FakeMode}
	if index, ok := vectors.(domain.InMemoryIndex); ok && cfg.FakeMode {
		targets.Memory = index
	}
	if !cfg.FakeMode && cfg.VectorStore != "postgres" {
		targets.VectorIndex = cfg.PineconeIndexName
	}
	return targets
}
