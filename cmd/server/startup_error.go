package main

import (
	"context"
	"errors"

	"github.com/Victoroide/quarry-rag/internal/tracing"
)

type fatalStage uint8

const (
	fatalConfiguration fatalStage = iota + 1
	fatalNoEvidenceCatalog
	fatalCitationVerifier
	fatalTracing
	fatalPostgres
	fatalMigrations
	fatalSubjectVerifier
	fatalSubjectTrust
	fatalSynthesisProfile
	fatalPostgresVectors
	fatalVectorGeneration
	fatalPinecone
	fatalEmbedder
	fatalLLMClient
	fatalS3
	fatalQueryComponents
	fatalReranker
	fatalHostname
	fatalOCR
	fatalAPIListen
	fatalMetricsListen
	fatalServe
)

func (stage fatalStage) label() string {
	switch stage {
	case fatalConfiguration:
		return "startup: configuration"
	case fatalNoEvidenceCatalog:
		return "startup: no-evidence catalog"
	case fatalCitationVerifier:
		return "startup: citation verifier"
	case fatalTracing:
		return "startup: tracing"
	case fatalPostgres:
		return "startup: postgres"
	case fatalMigrations:
		return "startup: migrations"
	case fatalSubjectVerifier:
		return "startup: subject verifier"
	case fatalSubjectTrust:
		return "startup: subject trust validation"
	case fatalSynthesisProfile:
		return "startup: synthesis profile"
	case fatalPostgresVectors:
		return "startup: postgres vectors"
	case fatalVectorGeneration:
		return "startup: vector generation"
	case fatalPinecone:
		return "startup: pinecone"
	case fatalEmbedder:
		return "startup: embedder"
	case fatalLLMClient:
		return "startup: llm client"
	case fatalS3:
		return "startup: s3"
	case fatalQueryComponents:
		return "startup: query components"
	case fatalReranker:
		return "startup: reranker"
	case fatalHostname:
		return "startup: hostname"
	case fatalOCR:
		return "startup: ocr"
	case fatalAPIListen:
		return "server: listen"
	case fatalMetricsListen:
		return "metrics: listen"
	case fatalServe:
		return "serve"
	default:
		return ""
	}
}

func fatalErrorForTelemetry(ctx context.Context, stage fatalStage, err error) error {
	if err == nil {
		return nil
	}
	if !tracing.RedactIdentifiers(ctx) {
		return err
	}
	safe := tracing.ErrorForTelemetry(ctx, err)
	if label := stage.label(); label != "" {
		return errors.New(label + ": " + safe.Error())
	}
	return safe
}
