package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/chunker"
	"github.com/Victoroide/quarry-rag/internal/domain"
	pgstore "github.com/Victoroide/quarry-rag/internal/store/postgres"
	"github.com/Victoroide/quarry-rag/internal/testutil/pgtest"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

const (
	documentArg    = "00000000-0000-4000-8000-000000000001"
	incarnationArg = "2d43ca14-76bc-4f72-bf85-e7a607a1197d"
)

func TestParseTargetKeepsBareUUIDsBare(t *testing.T) {
	got, err := parseTarget(documentArg)
	require.NoError(t, err)
	require.Equal(t, target{id: documentArg}, got)
}

func TestParseTargetQualifiesWithOneCanonicalIncarnation(t *testing.T) {
	got, err := parseTarget(documentArg + "@" + strings.ToUpper(incarnationArg))
	require.NoError(t, err)
	require.Equal(t, target{id: documentArg, incarnation: incarnationArg, qualified: true}, got)
}

func TestParseTargetNeverDowngradesAQualifier(t *testing.T) {
	for name, arg := range map[string]string{
		"empty incarnation":     documentArg + "@",
		"only separator":        "@",
		"empty document":        "@" + incarnationArg,
		"not a UUID":            documentArg + "@latest",
		"unhyphenated":          documentArg + "@2d43ca1476bc4f72bf85e7a607a1197d",
		"braced":                documentArg + "@{" + incarnationArg + "}",
		"urn":                   documentArg + "@urn:uuid:" + incarnationArg,
		"repeated separator":    documentArg + "@" + incarnationArg + "@" + incarnationArg,
		"trailing space":        documentArg + "@" + incarnationArg + " ",
		"leading space":         documentArg + "@ " + incarnationArg,
		"comma":                 documentArg + "@" + incarnationArg + "," + incarnationArg,
		"document is not UUID":  "document@" + incarnationArg,
		"bare document invalid": "not-a-uuid",
		"empty argument":        "",
	} {
		t.Run(name, func(t *testing.T) {
			_, err := parseTarget(arg)
			require.Error(t, err)
		})
	}
}

func TestParseTargetsRejectsTheWholeCommandBeforeAnyDeletion(t *testing.T) {
	_, err := parseTargets(nil)
	require.Error(t, err, "at least one target is required")
	_, err = parseTargets([]string{documentArg, documentArg + "@"})
	require.Error(t, err, "one bad argument must stop the command before it deletes the first")
	got, err := parseTargets([]string{documentArg, documentArg + "@" + incarnationArg})
	require.NoError(t, err)
	require.Len(t, got, 2)
	require.False(t, got[0].qualified)
	require.True(t, got[1].qualified)
}

func FuzzParseTarget(f *testing.F) {
	for _, seed := range []string{documentArg, documentArg + "@" + incarnationArg, "@", documentArg + "@", "a@b@c", "", "\x00@\x00", documentArg + "@" + incarnationArg + "@"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, arg string) {
		got, err := parseTarget(arg)
		if err != nil {
			return
		}
		_, parseErr := uuid.Parse(got.id)
		require.NoError(t, parseErr)
		if got.qualified {
			require.Len(t, got.incarnation, 36, "a qualified target carries one canonical incarnation")
			require.Equal(t, strings.ToLower(got.incarnation), got.incarnation)
			require.Contains(t, arg, "@")
			return
		}
		require.Empty(t, got.incarnation)
		require.NotContains(t, arg, "@", "an argument with a separator is never an ID-only request")
	})
}

func TestParseCommandModesAndValidation(t *testing.T) {
	owner := uuid.NewString()

	cmd, err := parseCommand([]string{"--list-held"})
	require.NoError(t, err)
	require.Equal(t, actionListHeld, cmd.action)

	_, err = parseCommand([]string{"--list-held", documentArg})
	require.Error(t, err)

	cmd, err = parseCommand([]string{"--quarantine", strings.ToUpper(documentArg)})
	require.NoError(t, err)
	require.Equal(t, actionQuarantine, cmd.action)
	require.Equal(t, []target{{id: documentArg}}, cmd.targets)

	for _, bad := range [][]string{
		{"--quarantine"},
		{"--quarantine", documentArg + "@" + incarnationArg},
		{"--quarantine", "not-a-uuid"},
		{"--quarantine", "{" + documentArg + "}"},
		{"--release-held"},
		{"--release-held", documentArg},
		{"--release-held", documentArg + "@" + incarnationArg, incarnationArg},
		{"--release-held", documentArg, incarnationArg, "extra"},
		{"--release-held", documentArg, ""},
		{"--release-held", documentArg, "latest"},
		{"--release-held", "--offline-owner-terminated", documentArg, incarnationArg},
		{"--release-held", "--offline-owner-terminated=", documentArg, incarnationArg},
		{"--release-held", "--offline-owner-terminated=bad", documentArg, incarnationArg},
		{"--release-held", "--offline-owner-terminated=" + owner, "--offline-owner-terminated=unknown", documentArg, incarnationArg},
		{"--offline-owner-terminated=" + owner, documentArg},
		{"--unknown-flag", documentArg},
		{"--activate-deletion", documentArg},
		{documentArg, "--activate-deletion"},
		{"--activate-deletion", "--activate-deletion"},
		{"--activate-deletion", "--list-held"},
		{"--list-held", "--activate-deletion"},
		{"--quarantine", documentArg, "--activate-deletion"},
		{"--release-held", documentArg, incarnationArg, "--activate-deletion"},
		{"--activate-deletion", ""},
		{"--activate-deletion=true"},
		{"--activate-deletion="},
		{"--activate-deletion", "true"},
	} {
		_, err = parseCommand(bad)
		require.Error(t, err, bad)
	}

	cmd, err = parseCommand([]string{"--activate-deletion"})
	require.NoError(t, err)
	require.Equal(t, command{action: actionActivate}, cmd, "activation takes no target")

	cmd, err = parseCommand([]string{"--release-held", strings.ToUpper(documentArg), strings.ToUpper(incarnationArg)})
	require.NoError(t, err)
	require.Equal(t, actionRelease, cmd.action)
	require.False(t, cmd.terminatedSet)
	require.Equal(t, []target{{id: documentArg, incarnation: incarnationArg, qualified: true}}, cmd.targets)

	cmd, err = parseCommand([]string{"--release-held", "--offline-owner-terminated=" + strings.ToUpper(owner), documentArg, incarnationArg})
	require.NoError(t, err)
	require.Equal(t, actionRelease, cmd.action)
	require.True(t, cmd.terminatedSet)
	require.Equal(t, owner, cmd.terminatedOwner)

	cmd, err = parseCommand([]string{"--release-held", "--offline-owner-terminated=unknown", documentArg, incarnationArg})
	require.NoError(t, err)
	require.True(t, cmd.terminatedSet)
	require.Empty(t, cmd.terminatedOwner)
}

func FuzzParseCommand(f *testing.F) {
	f.Add("--list-held", "", "")
	f.Add("--quarantine", documentArg, "")
	f.Add("--release-held", documentArg, incarnationArg)
	f.Add("--release-held", "--offline-owner-terminated=unknown", documentArg)
	f.Add("--activate-deletion", "", "")
	f.Add("--activate-deletion", documentArg, "")
	f.Add(documentArg, "--activate-deletion", "")
	f.Add(documentArg+"@"+incarnationArg, "", "")
	f.Fuzz(func(t *testing.T, a, b, c string) {
		var args []string
		for _, s := range []string{a, b, c} {
			if s != "" {
				args = append(args, s)
			}
		}
		cmd, err := parseCommand(args)
		if err != nil {
			return
		}
		if slices.Contains(args, "--activate-deletion") {
			require.Equal(t, command{action: actionActivate}, cmd, "activation is exclusive")
			require.Len(t, args, 1)
			return
		}
		require.NotEqual(t, actionActivate, cmd.action, "only the exact flag selects activation")
		for _, tgt := range cmd.targets {
			_, parseErr := uuid.Parse(tgt.id)
			require.NoError(t, parseErr)
			if cmd.action == actionRelease {
				require.True(t, tgt.qualified)
				require.Len(t, tgt.incarnation, 36)
			}
		}
	})
}

func TestCleanupCLIQuarantineListAndReleaseAcrossActivation(t *testing.T) {
	db := pgtest.StartVector(t)
	t.Setenv("DATABASE_URL", db)
	t.Setenv("VECTOR_STORE", "postgres")
	t.Setenv("GO_RAG_FAKE_MODE", "false")
	t.Setenv("S3_BUCKET", "")
	t.Setenv("AWS_REGION", "us-east-1")
	t.Setenv("SERVICE_TOKEN", "cleanup-cli")

	c, err := pgstore.NewClient(t.Context(), db)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, c.Close(context.Background())) })
	require.NoError(t, c.RunMigrations(t.Context()))
	_, err = pgstore.NewVectorStore(t.Context(), c, 3)
	require.NoError(t, err)

	id := uuid.NewString()
	var buf bytes.Buffer
	require.NoError(t, runArgs([]string{"--quarantine", id}, &buf))
	var qOut struct {
		Quarantined []domain.HeldDeletion `json:"quarantined"`
	}
	require.NoError(t, json.Unmarshal(buf.Bytes(), &qOut))
	require.Len(t, qOut.Quarantined, 1)
	require.Equal(t, domain.DocumentID(id), qOut.Quarantined[0].DocumentID)
	inc := qOut.Quarantined[0].Incarnation
	require.NotEmpty(t, inc)

	// Repeating quarantine preserves the same recovery pair.
	buf.Reset()
	require.NoError(t, runArgs([]string{"--quarantine", id}, &buf))
	qOut.Quarantined = nil
	require.NoError(t, json.Unmarshal(buf.Bytes(), &qOut))
	require.Len(t, qOut.Quarantined, 1)
	require.Equal(t, inc, qOut.Quarantined[0].Incarnation)

	// Inspection reports the held identity and phase.
	buf.Reset()
	require.NoError(t, runArgs([]string{"--list-held"}, &buf))
	var lOut struct {
		Held []domain.HeldDeletion `json:"held"`
	}
	require.NoError(t, json.Unmarshal(buf.Bytes(), &lOut))
	require.Len(t, lOut.Held, 1)
	require.Equal(t, inc, lOut.Held[0].Incarnation)
	require.Equal(t, "quarantined", lOut.Held[0].Phase)

	// Wrong incarnation cannot release.
	require.Error(t, runArgs([]string{"--release-held", id, uuid.NewString()}, &buf))
	require.NoError(t, runArgs([]string{"--release-held", id, inc}, &buf))

	buf.Reset()
	require.NoError(t, runArgs([]string{"--list-held"}, &buf))
	lOut.Held = nil
	require.NoError(t, json.Unmarshal(buf.Bytes(), &lOut))
	require.Empty(t, lOut.Held)

	// Keep os.Args unused in test output.
	_ = os.Stdout
}

// cleanupDatabase prepares a disposable database for the command: schema,
// transactional vectors and the environment the command reads.
func cleanupDatabase(t *testing.T) *pgstore.Client {
	t.Helper()
	db := pgtest.StartVector(t)
	t.Setenv("DATABASE_URL", db)
	t.Setenv("VECTOR_STORE", "postgres")
	t.Setenv("GO_RAG_FAKE_MODE", "false")
	t.Setenv("S3_BUCKET", "")
	t.Setenv("AWS_REGION", "us-east-1")
	t.Setenv("SERVICE_TOKEN", "cleanup-cli")
	c, err := pgstore.NewClient(t.Context(), db)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, c.Close(context.Background())) })
	require.NoError(t, c.RunMigrations(t.Context()))
	_, err = pgstore.NewVectorStore(t.Context(), c, 3)
	require.NoError(t, err)
	return c
}

func liveDocument(t *testing.T, c *pgstore.Client, content string) (id, incarnation string) {
	t.Helper()
	id = uuid.NewString()
	result, err := c.AcceptDocument(t.Context(), domain.AcceptRequest{
		Document: domain.Document{ID: domain.DocumentID(id), Content: content}, CallerSupplied: true,
		Fingerprint: chunker.ScopedFingerprint("", content), MaxPending: 100,
	})
	require.NoError(t, err)
	require.Equal(t, domain.AcceptAccepted, result.Outcome)
	require.NoError(t, c.Pool.QueryRow(t.Context(), `SELECT COALESCE(incarnation,id)::text FROM documents WHERE id=$1`, id).Scan(&incarnation))
	return id, incarnation
}

func documentExists(t *testing.T, c *pgstore.Client, id string) bool {
	t.Helper()
	var exists bool
	require.NoError(t, c.Pool.QueryRow(t.Context(), `SELECT EXISTS(SELECT 1 FROM documents WHERE id=$1)`, id).Scan(&exists))
	return exists
}

func recordedActivation(t *testing.T, c *pgstore.Client) *time.Time {
	t.Helper()
	var marker *time.Time
	require.NoError(t, c.Pool.QueryRow(t.Context(), `SELECT activated_at FROM document_deletion_activation`).Scan(&marker))
	return marker
}

func TestActivationCommandIsTheOnlyWriterOfTheMarker(t *testing.T) {
	c := cleanupDatabase(t)
	id, incarnation := liveDocument(t, c, "live before activation")
	var buf bytes.Buffer

	// A local setting from before activation moved into the database authorizes
	// nothing, and ordinary invocations never set the marker.
	t.Setenv("RETRYABLE_DELETION", "true")
	require.NoError(t, runArgs([]string{"--list-held"}, &buf))
	require.ErrorIs(t, runArgs([]string{uuid.NewString()}, &buf), domain.ErrDocumentNotFound)
	require.Nil(t, recordedActivation(t, c))
	require.ErrorIs(t, runArgs([]string{id + "@" + incarnation}, &buf), domain.ErrDeletionInactive, "a qualified request cannot activate")
	require.Nil(t, recordedActivation(t, c))
	require.True(t, documentExists(t, c, id))

	buf.Reset()
	require.NoError(t, runArgs([]string{"--activate-deletion"}, &buf))
	var first domain.DeletionActivation
	require.NoError(t, json.Unmarshal(buf.Bytes(), &first), buf.String())
	require.False(t, first.AlreadyActive)
	marker := recordedActivation(t, c)
	require.NotNil(t, marker)
	require.True(t, marker.Equal(first.ActivatedAt))

	// A second explicit activation reports the same instant.
	time.Sleep(20 * time.Millisecond)
	buf.Reset()
	require.NoError(t, runArgs([]string{"--activate-deletion"}, &buf))
	var second domain.DeletionActivation
	require.NoError(t, json.Unmarshal(buf.Bytes(), &second), buf.String())
	require.True(t, second.AlreadyActive)
	require.True(t, second.ActivatedAt.Equal(first.ActivatedAt))

	// The activated contract: a bare request for a live UUID is refused, and the
	// request that names the observed lifetime deletes it.
	t.Setenv("RETRYABLE_DELETION", "false")
	require.ErrorIs(t, runArgs([]string{id}, &buf), domain.ErrDeletionPreconditionRequired)
	require.True(t, documentExists(t, c, id))
	require.NoError(t, runArgs([]string{id + "@" + incarnation}, &buf))
	require.False(t, documentExists(t, c, id))
	require.True(t, recordedActivation(t, c).Equal(first.ActivatedAt))
}

func TestBareCleanupWaitsForTheActivationLock(t *testing.T) {
	c := cleanupDatabase(t)
	id, _ := liveDocument(t, c, "removed once activation is out of the way")
	holder, err := c.Pool.Acquire(t.Context())
	require.NoError(t, err)
	_, err = holder.Exec(t.Context(), `SELECT pg_advisory_lock($1)`, pgstore.DeletionActivationLockID)
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() { done <- runArgs([]string{id}, &bytes.Buffer{}) }()
	select {
	case err = <-done:
		t.Fatalf("an ID-only cleanup proceeded while activation held its lock: %v", err)
	case <-time.After(750 * time.Millisecond):
	}
	require.True(t, documentExists(t, c, id))

	_, err = holder.Exec(t.Context(), `SELECT pg_advisory_unlock($1)`, pgstore.DeletionActivationLockID)
	require.NoError(t, err)
	holder.Release()
	require.NoError(t, <-done)
	require.False(t, documentExists(t, c, id))
	require.Nil(t, recordedActivation(t, c))
}
