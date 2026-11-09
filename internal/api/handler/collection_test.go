package handler

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestQueryCollectionValidation(t *testing.T) {
	for _, tc := range []struct {
		value, want string
		invalid     bool
	}{
		{"  cafe\u0301 ", "café", false}, {"", "", false}, {strings.Repeat("x", 129), "", true}, {"a\x00b", "", true}, {"a\u200bb", "", true},
	} {
		raw, err := json.Marshal(map[string]string{"query": "archive", "collection": tc.value})
		require.NoError(t, err)
		var req QueryRequest
		require.NoError(t, json.Unmarshal(raw, &req))
		apiErr := validateQueryRequest(&req)
		if tc.invalid {
			require.NotNil(t, apiErr)
			require.Contains(t, apiErr.Message, "collection")
		} else {
			require.Nil(t, apiErr)
			require.Equal(t, tc.want, req.Collection)
		}
	}
}

func TestListingCollectionValidation(t *testing.T) {
	for _, query := range []string{"collection=a&collection=b", "collection=" + strings.Repeat("x", 129), "collection=a%00b", "collection=a%E2%80%8Bb", "collection=%FF"} {
		rec := httptest.NewRecorder()
		listDocumentSummaries(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/documents?"+query, http.NoBody), nil)
		require.Equal(t, 400, rec.Code, rec.Body.String())
		require.Contains(t, rec.Body.String(), "collection")
	}
}
