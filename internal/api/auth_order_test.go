package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/stretchr/testify/require"
)

// TestDataRoutesAuthenticateBeforeAnyDatabaseAccess proves that a request
// without a valid service token is refused before the corpus mode is read and
// before any audit row is written, in both authorization modes.
func TestDataRoutesAuthenticateBeforeAnyDatabaseAccess(t *testing.T) {
	const serviceToken = "order-service-token"
	routes := []struct{ method, path string }{
		{http.MethodPost, "/ingest"},
		{http.MethodPost, "/query"},
		{http.MethodGet, "/documents"},
		{http.MethodGet, "/documents/" + grantsQuarantineID},
		{http.MethodGet, "/documents/" + grantsQuarantineID + "/file"},
		{http.MethodDelete, "/documents/" + grantsQuarantineID},
	}
	credentials := []struct {
		name       string
		tokens     []string
		grantsOnly bool
	}{
		{name: "missing"},
		{name: "wrong", tokens: []string{"another-token"}},
		{name: "empty", tokens: []string{""}},
		{name: "duplicated", tokens: []string{serviceToken, serviceToken}, grantsOnly: true},
		{name: "comma joined", tokens: []string{serviceToken + "," + serviceToken}},
	}
	for _, mode := range []string{config.AuthzModeService, config.AuthzModeGrants} {
		t.Run(mode, func(t *testing.T) {
			store := newGrantsMemoryStore()
			if mode == config.AuthzModeService {
				store.corpusMode = config.AuthzModeService
			}
			router := NewRouter(&RouterDeps{
				Config:          &config.Config{AuthzMode: mode, ServiceTokens: []string{serviceToken}, HealthCacheTTL: 1},
				ChunkStore:      store,
				VectorStore:     newGrantsVectorStore(store),
				SubjectResolver: store,
				AuditRecorder:   store,
			})
			for _, route := range routes {
				for _, credential := range credentials {
					if credential.grantsOnly && mode != config.AuthzModeGrants {
						continue
					}
					req := httptest.NewRequestWithContext(t.Context(), route.method, route.path, strings.NewReader("{}"))
					for _, token := range credential.tokens {
						req.Header.Add("X-Service-Token", token)
					}
					req.Header.Set("Authorization", "Bearer not-a-real-credential")
					rec := httptest.NewRecorder()
					router.ServeHTTP(rec, req)
					require.Equal(t, http.StatusUnauthorized, rec.Code, "%s %s %s", route.method, route.path, credential.name)
				}
			}
			require.Zero(t, store.modeReads.Load(), "corpus mode read before authentication")
			require.Empty(t, store.auditEvents, "audit written before authentication")
		})
	}
}
