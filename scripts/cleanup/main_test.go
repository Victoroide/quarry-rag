package main

import (
	"strings"
	"testing"

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
