package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/tracing"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
)

func TestFatalErrorForTelemetryStages(t *testing.T) {
	grants := tracing.WithRedactIdentifiers(t.Context(), true)
	driver := &pgconn.PgError{
		Code:           "23514",
		Message:        "row-canary",
		Detail:         "detail-canary",
		ConstraintName: "constraint-canary",
	}
	provider := errors.New("/tmp/provider-canary\nobject-canary\n" + strings.Repeat("雪", 4096))
	imitator := errors.New("startup: migrations: stage-canary")
	cause := errors.Join(fmt.Errorf("driver: %w", driver), provider, imitator)
	for _, tc := range []struct {
		stage fatalStage
		label string
	}{
		{fatalConfiguration, "startup: configuration"},
		{fatalNoEvidenceCatalog, "startup: no-evidence catalog"},
		{fatalCitationVerifier, "startup: citation verifier"},
		{fatalTracing, "startup: tracing"},
		{fatalPostgres, "startup: postgres"},
		{fatalMigrations, "startup: migrations"},
		{fatalSubjectVerifier, "startup: subject verifier"},
		{fatalSubjectTrust, "startup: subject trust validation"},
		{fatalSynthesisProfile, "startup: synthesis profile"},
		{fatalPostgresVectors, "startup: postgres vectors"},
		{fatalVectorGeneration, "startup: vector generation"},
		{fatalPinecone, "startup: pinecone"},
		{fatalEmbedder, "startup: embedder"},
		{fatalLLMClient, "startup: llm client"},
		{fatalS3, "startup: s3"},
		{fatalQueryComponents, "startup: query components"},
		{fatalReranker, "startup: reranker"},
		{fatalHostname, "startup: hostname"},
		{fatalOCR, "startup: ocr"},
		{fatalAPIListen, "server: listen"},
		{fatalMetricsListen, "metrics: listen"},
		{fatalServe, "serve"},
	} {
		t.Run(tc.label, func(t *testing.T) {
			ctx := t.Context()
			safe := fatalErrorForTelemetry(tracing.WithRedactIdentifiers(ctx, true), tc.stage, cause)
			require.EqualError(t, safe, tc.label+": operation failed")
			for _, canary := range []string{driver.Message, driver.Detail, driver.ConstraintName, "provider-canary", "object-canary", "stage-canary", "雪", "\n"} {
				require.NotContains(t, safe.Error(), canary)
			}
			var pgErr *pgconn.PgError
			require.False(t, errors.As(safe, &pgErr))
			require.NotErrorIs(t, safe, provider)
			require.Same(t, cause, fatalErrorForTelemetry(ctx, tc.stage, cause))
		})
	}

	require.NoError(t, fatalErrorForTelemetry(grants, fatalServe, nil))
	require.NoError(t, fatalErrorForTelemetry(t.Context(), fatalServe, nil))
	require.Same(t, provider, fatalErrorForTelemetry(t.Context(), 0, provider))
	require.EqualError(t, fatalErrorForTelemetry(grants, 0, errors.New("")), "operation failed")
	require.EqualError(t, fatalErrorForTelemetry(grants, 0, imitator), "operation failed")
	for _, stage := range []fatalStage{0, 255} {
		require.EqualError(t, fatalErrorForTelemetry(grants, stage, cause), "operation failed")
	}
}

type stageImitatingError struct {
	error
	asCalled bool
}

func (err *stageImitatingError) As(any) bool {
	err.asCalled = true
	return true
}

func TestFatalErrorForTelemetryIgnoresProviderStage(t *testing.T) {
	ctx := t.Context()
	grants := tracing.WithRedactIdentifiers(ctx, true)
	imitator := &stageImitatingError{error: errors.New("startup: migrations: provider-canary")}
	nested := fmt.Errorf("outside: %w", errors.Join(imitator, context.DeadlineExceeded))

	for _, tc := range []struct {
		cause    error
		category string
	}{
		{imitator, "operation failed"},
		{nested, "operation timed out"},
	} {
		require.EqualError(t, fatalErrorForTelemetry(grants, 0, tc.cause), tc.category)
		require.EqualError(t, fatalErrorForTelemetry(grants, fatalPostgres, tc.cause), "startup: postgres: "+tc.category)
		require.Same(t, tc.cause, fatalErrorForTelemetry(ctx, fatalPostgres, tc.cause))
	}
	require.False(t, imitator.asCalled, "fatal rendering must not ask provider errors for a stage")
}

func TestFatalErrorForTelemetryCategories(t *testing.T) {
	grants := tracing.WithRedactIdentifiers(t.Context(), true)
	bind := errors.New("listen: socket-canary")
	cleanup := errors.New("close: cleanup-canary")
	wrappedCancel := fmt.Errorf("outer: %w", fmt.Errorf("inner: %w", context.Canceled))
	wrappedDeadline := fmt.Errorf("deadline-canary: %w", context.DeadlineExceeded)
	for _, tc := range []struct {
		name, category string
		cause          error
	}{
		{"ordinary", "operation failed", bind},
		{"canceled", "operation canceled", context.Canceled},
		{"deadline", "operation timed out", context.DeadlineExceeded},
		{"wrapped cancellation", "operation canceled", wrappedCancel},
		{"joined cleanup", "operation failed", errors.Join(bind, cleanup)},
		{"joined cancellation", "operation canceled", errors.Join(bind, cleanup, wrappedCancel)},
		{"joined deadline and cancellation", "operation timed out", errors.Join(bind, wrappedCancel, errors.Join(cleanup, wrappedDeadline))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			safe := fatalErrorForTelemetry(grants, fatalMetricsListen, tc.cause)
			require.EqualError(t, safe, "metrics: listen: "+tc.category)
			require.NotErrorIs(t, safe, tc.cause)
			require.EqualError(t, fatalErrorForTelemetry(grants, 0, tc.cause), tc.category)
		})
	}
}
