package client_test

import (
	"context"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/Victoroide/quarry-rag/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	deleteDocumentID = "00000000-0000-4000-8000-000000000001"
	deleteLifetime   = "2d43ca14-76bc-4f72-bf85-e7a607a1197d"
)

// legacyDocumentDeleter is the method set a caller compiled before qualified
// deletion existed. Adding a method to the client must not break it.
type legacyDocumentDeleter interface {
	DeleteDocument(ctx context.Context, id string) (*client.DeleteResponse, error)
}

var _ legacyDocumentDeleter = (*client.Client)(nil)

type observedDelete struct {
	requests atomic.Int32
	headers  []http.Header
}

func deleteServer(t *testing.T, status int) (*client.Client, *observedDelete) {
	t.Helper()
	observed := new(observedDelete)
	c := newClient(t, func(w http.ResponseWriter, r *http.Request) {
		observed.requests.Add(1)
		observed.headers = append(observed.headers, r.Header.Clone())
		assert.Equal(t, http.MethodDelete+" /api/documents/"+deleteDocumentID, r.Method+" "+r.URL.Path)
		assert.Empty(t, r.URL.RawQuery)
		if status != http.StatusOK {
			w.WriteHeader(status)
			return
		}
		writeJSON(t, w, client.DeleteResponse{Deleted: true, DocID: deleteDocumentID})
	})
	return c, observed
}

func TestBareDeleteSendsNoIncarnation(t *testing.T) {
	c, observed := deleteServer(t, http.StatusOK)
	response, err := c.DeleteDocument(t.Context(), deleteDocumentID)
	require.NoError(t, err)
	require.Equal(t, &client.DeleteResponse{Deleted: true, DocID: deleteDocumentID}, response)
	require.EqualValues(t, 1, observed.requests.Load())
	require.Empty(t, observed.headers[0].Values("X-Document-Incarnation"), "an older service sees the original request")
}

func TestQualifiedDeleteSendsTheSavedIncarnationOnce(t *testing.T) {
	c, observed := deleteServer(t, http.StatusOK)
	response, err := c.DeleteDocumentIncarnation(t.Context(), deleteDocumentID, strings.ToUpper(deleteLifetime))
	require.NoError(t, err)
	require.Equal(t, &client.DeleteResponse{Deleted: true, DocID: deleteDocumentID}, response)
	require.EqualValues(t, 1, observed.requests.Load())
	require.Equal(t, []string{deleteLifetime}, observed.headers[0].Values("X-Document-Incarnation"), "exactly one canonical value")
	require.Equal(t, "synthetic-token", observed.headers[0].Get("X-Service-Token"))
}

func TestQualifiedDeleteRejectsAnIncarnationThatCouldBecomeIDOnly(t *testing.T) {
	for name, value := range map[string]string{
		"empty":            "",
		"space":            " ",
		"unhyphenated":     "2d43ca1476bc4f72bf85e7a607a1197d",
		"braced":           "{" + deleteLifetime + "}",
		"urn":              "urn:uuid:" + deleteLifetime,
		"trailing space":   deleteLifetime + " ",
		"repeated":         deleteLifetime + "," + deleteLifetime,
		"short":            deleteLifetime[:35],
		"long":             deleteLifetime + "0",
		"not hexadecimal":  strings.Replace(deleteLifetime, "2", "g", 1),
		"misplaced hyphen": "2d43ca147-6bc-4f72-bf85-e7a607a1197d",
		"unicode":          strings.Replace(deleteLifetime, "a", "а", 1),
		"new line":         deleteLifetime[:35] + "\n",
	} {
		t.Run(name, func(t *testing.T) {
			c, observed := deleteServer(t, http.StatusOK)
			response, err := c.DeleteDocumentIncarnation(t.Context(), deleteDocumentID, value)
			require.ErrorIs(t, err, client.ErrInvalidIncarnation)
			require.Nil(t, response)
			require.Zero(t, observed.requests.Load(), "nothing is sent, so nothing can fall back to an ID-only deletion")
		})
	}
}

func TestQualifiedDeleteNeverRepeatsOrDowngrades(t *testing.T) {
	for _, status := range []int{http.StatusPreconditionFailed, http.StatusPreconditionRequired, http.StatusServiceUnavailable, http.StatusInternalServerError, http.StatusNotFound} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			c, observed := deleteServer(t, status)
			_, err := c.DeleteDocumentIncarnation(t.Context(), deleteDocumentID, deleteLifetime)
			var apiErr *client.APIError
			require.ErrorAs(t, err, &apiErr)
			require.Equal(t, status, apiErr.StatusCode)
			require.EqualValues(t, 1, observed.requests.Load(), "one attempt per call; the caller decides to retry with the same value")
			require.Equal(t, []string{deleteLifetime}, observed.headers[0].Values("X-Document-Incarnation"))
		})
	}
}

func TestQualifiedDeleteKeepsTheDocumentIDInOnePathSegment(t *testing.T) {
	c := newClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/api/documents/a%2Fb", r.URL.EscapedPath())
		w.WriteHeader(http.StatusNotFound)
	})
	_, err := c.DeleteDocumentIncarnation(t.Context(), "a/b", deleteLifetime)
	require.Error(t, err)
}

func FuzzDeleteDocumentIncarnation(f *testing.F) {
	for _, seed := range []string{deleteLifetime, "", " ", strings.ToUpper(deleteLifetime), deleteLifetime + deleteLifetime, "\x00", "{" + deleteLifetime + "}"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, value string) {
		var headers [][]string
		c := newClient(t, func(w http.ResponseWriter, r *http.Request) {
			headers = append(headers, r.Header.Values("X-Document-Incarnation"))
			writeJSON(t, w, client.DeleteResponse{Deleted: true, DocID: deleteDocumentID})
		})
		_, err := c.DeleteDocumentIncarnation(context.Background(), deleteDocumentID, value)
		if err != nil {
			require.ErrorIs(t, err, client.ErrInvalidIncarnation)
			require.Empty(t, headers, "an invalid value is never sent")
			return
		}
		require.Len(t, headers, 1)
		require.Len(t, headers[0], 1)
		require.Equal(t, strings.ToLower(value), headers[0][0])
		require.Len(t, headers[0][0], 36)
	})
}
