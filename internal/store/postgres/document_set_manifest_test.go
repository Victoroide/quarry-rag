package postgres

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"testing"
	"testing/iotest"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func manifestOf(ids ...string) string { return strings.Join(ids, "\n") + "\n" }

func TestManifestDeduplicatesAndSortsCanonicalIdentifiers(t *testing.T) {
	a, b := "00000000-0000-4000-8000-00000000000a", "00000000-0000-4000-8000-00000000000b"
	got, err := ReadDocumentSetManifest(strings.NewReader(manifestOf(b, strings.ToUpper(a), a, b)), 10)
	require.NoError(t, err)
	require.Equal(t, []string{a, b}, got, "spelling and repetition collapse before the database sees them")
	got, err = ReadDocumentSetManifest(strings.NewReader(strings.ReplaceAll(manifestOf(b, a), "\n", "\r\n")), 10)
	require.NoError(t, err)
	require.Equal(t, []string{a, b}, got)
	got, err = ReadDocumentSetManifest(strings.NewReader(a), 10)
	require.NoError(t, err, "the last line needs no terminator")
	require.Equal(t, []string{a}, got)
}

func TestManifestMayBeEmpty(t *testing.T) {
	got, err := ReadDocumentSetManifest(strings.NewReader(""), 10)
	require.NoError(t, err)
	require.Empty(t, got, "an empty manifest names an empty set")
}

func TestManifestRejectsMalformedInput(t *testing.T) {
	valid := "00000000-0000-4000-8000-00000000000a"
	for name, input := range map[string]string{
		"blank line":      valid + "\n\n" + valid + "\n",
		"only a newline":  "\n",
		"not a UUID":      "document\n",
		"nil UUID":        uuid.Nil.String() + "\n",
		"braced":          "{" + valid + "}\n",
		"urn":             "urn:uuid:" + valid + "\n",
		"no hyphens":      strings.ReplaceAll(valid, "-", "") + "\n",
		"trailing space":  valid + " \n",
		"leading space":   " " + valid + "\n",
		"two per line":    valid + " " + valid + "\n",
		"long line":       strings.Repeat("a", 4096) + "\n",
		"NUL":             valid[:35] + "\x00\n",
		"JSON array":      `["` + valid + `"]` + "\n",
		"invalid UTF-8":   "\xff\xfe\n",
		"short":           valid[:35] + "\n",
		"extra character": valid + "0\n",
	} {
		t.Run(name, func(t *testing.T) {
			_, err := ReadDocumentSetManifest(strings.NewReader(input), 10)
			require.ErrorIs(t, err, ErrDocumentSetManifest)
		})
	}
}

func TestManifestRefusesTooManyRawEntriesBeforeCanonicalizing(t *testing.T) {
	id := "00000000-0000-4000-8000-00000000000a"
	_, err := ReadDocumentSetManifest(strings.NewReader(manifestOf(id, id, id)), 2)
	require.ErrorIs(t, err, ErrDocumentSetManifest, "duplicates count toward the ceiling")
	require.ErrorContains(t, err, "more than 2 entries")
	_, err = ReadDocumentSetManifest(strings.NewReader(manifestOf(id, id)), 2)
	require.NoError(t, err)
	// The ceiling decides before a later malformed line is even read.
	_, err = ReadDocumentSetManifest(strings.NewReader(manifestOf(id, id, "bad")), 2)
	require.ErrorContains(t, err, "more than 2 entries")
}

func TestManifestRefusesMoreThanTheByteCapWithoutReadingItAll(t *testing.T) {
	var counted countingReader
	counted.inner = strings.NewReader(strings.Repeat("00000000-0000-4000-8000-00000000000a\n", 300000))
	_, err := ReadDocumentSetManifest(&counted, MaxDocumentSetMembers)
	require.ErrorIs(t, err, ErrDocumentSetManifest)
	require.LessOrEqual(t, counted.n, int64(MaxDocumentSetManifestBytes+1), "reading stops at the cap")
}

func TestManifestByteCapAppliesToLongBlankStreams(t *testing.T) {
	_, err := ReadDocumentSetManifest(bytes.NewReader(bytes.Repeat([]byte("\n"), MaxDocumentSetManifestBytes+10)), MaxDocumentSetMembers)
	require.ErrorIs(t, err, ErrDocumentSetManifest)
}

func TestManifestAcceptsTheLargestSupportedSet(t *testing.T) {
	ids := make([]string, MaxDocumentSetMembers)
	for i := range ids {
		ids[i] = fmt.Sprintf("00000000-0000-4000-8000-%012x", i+1)
	}
	got, err := ReadDocumentSetManifest(strings.NewReader(manifestOf(ids...)), MaxDocumentSetMembers)
	require.NoError(t, err)
	require.Equal(t, ids, got)
	require.Less(t, len(manifestOf(ids...)), MaxDocumentSetManifestBytes, "a full set fits the byte cap")
}

func TestManifestSurfacesReadFailureWithoutDetail(t *testing.T) {
	_, err := ReadDocumentSetManifest(iotest.ErrReader(errors.New("secret detail")), 10)
	require.Error(t, err)
	require.NotContains(t, err.Error(), "secret")
}

func TestManifestRequiresAValidCeiling(t *testing.T) {
	for _, limit := range []int{0, -1, MaxDocumentSetMembers + 1} {
		_, err := ReadDocumentSetManifest(strings.NewReader(""), limit)
		require.ErrorIs(t, err, ErrDocumentSetManifest)
	}
}

type countingReader struct {
	inner interface{ Read([]byte) (int, error) }
	n     int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.inner.Read(p)
	c.n += int64(n)
	return n, err
}

func FuzzReadDocumentSetManifest(f *testing.F) {
	for _, seed := range []string{"", "\n", "00000000-0000-4000-8000-00000000000a\n", "00000000-0000-4000-8000-00000000000a\r\n00000000-0000-4000-8000-00000000000A", "x\n", "\x00"} {
		f.Add(seed, 3)
	}
	f.Fuzz(func(t *testing.T, input string, limit int) {
		got, err := ReadDocumentSetManifest(strings.NewReader(input), limit)
		if err != nil {
			require.Nil(t, got)
			return
		}
		require.LessOrEqual(t, len(got), limit)
		for i, id := range got {
			parsed, parseErr := uuid.Parse(id)
			require.NoError(t, parseErr)
			require.Equal(t, parsed.String(), id, "identifiers are canonical")
			require.NotEqual(t, uuid.Nil, parsed)
			if i > 0 {
				require.Less(t, got[i-1], id, "identifiers are distinct and ascending")
			}
		}
	})
}

func TestDocumentSetRequestValidation(t *testing.T) {
	tenant, owner := "00000000-0000-4000-8000-0000000000a0", "00000000-0000-4000-8000-0000000000a1"
	valid := DocumentSetRequest{TenantID: tenant, OwnerID: owner, MaxMembers: 5, DocumentIDs: []string{"00000000-0000-4000-8000-00000000000a"}}
	require.NoError(t, valid.Validate())
	for name, mutate := range map[string]func(*DocumentSetRequest){
		"missing tenant":    func(r *DocumentSetRequest) { r.TenantID = "" },
		"bad owner":         func(r *DocumentSetRequest) { r.OwnerID = "owner" },
		"nil tenant":        func(r *DocumentSetRequest) { r.TenantID = uuid.Nil.String() },
		"lifetime too long": func(r *DocumentSetRequest) { r.TTL = MaxDocumentSetTTL + time.Second },
		"negative lifetime": func(r *DocumentSetRequest) { r.TTL = -time.Second },
		"sub-second":        func(r *DocumentSetRequest) { r.TTL = 1500 * time.Millisecond },
		"long key":          func(r *DocumentSetRequest) { r.ImportKey = strings.Repeat("k", MaxDocumentSetImportKeyBytes+1) },
		"NUL key":           func(r *DocumentSetRequest) { r.ImportKey = "a\x00b" },
		"bad UTF-8 key":     func(r *DocumentSetRequest) { r.ImportKey = "\xff" },
		"zero ceiling":      func(r *DocumentSetRequest) { r.MaxMembers = 0 },
		"ceiling too large": func(r *DocumentSetRequest) { r.MaxMembers = MaxDocumentSetMembers + 1 },
		"over the ceiling":  func(r *DocumentSetRequest) { r.MaxMembers = 0; r.DocumentIDs = make([]string, 6); r.MaxMembers = 5 },
	} {
		t.Run(name, func(t *testing.T) {
			request := valid
			mutate(&request)
			require.Error(t, request.Validate())
		})
	}
	valid.TTL = MaxDocumentSetTTL
	valid.ImportKey = strings.Repeat("k", MaxDocumentSetImportKeyBytes)
	require.NoError(t, valid.Validate(), "the extremes are accepted")
}
