package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"

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
	} {
		_, err = parseCommand(bad)
		require.Error(t, err, bad)
	}

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
	if testing.Short() {
		t.Skip("requires a Docker daemon")
	}
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
