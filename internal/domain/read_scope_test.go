package domain

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAuthorizedScopeFindsIncarnationsByCanonicalID(t *testing.T) {
	const id = "00000000-0000-4000-8000-0000000000ab"
	scope := &AuthorizedScope{Incarnations: map[string]string{id: "incarnation"}}
	for _, spelling := range []string{id, strings.ToUpper(id), "{" + id + "}", strings.ReplaceAll(id, "-", ""), "urn:uuid:" + id} {
		got, ok := scope.IncarnationOf(spelling)
		require.True(t, ok, spelling)
		require.Equal(t, "incarnation", got)
	}
	for _, spelling := range []string{"", "not-a-uuid", "00000000-0000-4000-8000-0000000000ac"} {
		_, ok := scope.IncarnationOf(spelling)
		require.False(t, ok, spelling)
	}
	_, ok := (*AuthorizedScope)(nil).IncarnationOf(id)
	require.False(t, ok)
	_, ok = (&AuthorizedScope{}).IncarnationOf(id)
	require.False(t, ok, "a scope without members lists nothing")
}

func TestAuthorizedScopeRequiresItsTenantLabel(t *testing.T) {
	scope := &AuthorizedScope{TenantID: "00000000-0000-4000-8000-0000000000a0"}
	require.True(t, scope.SameTenant(map[string]any{VectorTenantKey: scope.TenantID}))
	require.True(t, scope.SameTenant(map[string]any{VectorTenantKey: strings.ToUpper(scope.TenantID)}))
	require.False(t, scope.SameTenant(map[string]any{VectorTenantKey: "00000000-0000-4000-8000-0000000000b0"}))
	require.False(t, scope.SameTenant(map[string]any{VectorTenantKey: ""}))
	require.False(t, scope.SameTenant(map[string]any{VectorTenantKey: 7}))
	require.False(t, scope.SameTenant(nil), "absent labels never match")
	require.False(t, (*AuthorizedScope)(nil).SameTenant(map[string]any{VectorTenantKey: scope.TenantID}))
}

func TestVectorFilterWithAScopeStillMatchesWhatItsIDsMatch(t *testing.T) {
	scope := &AuthorizedScope{TenantID: "tenant"}
	require.False(t, VectorFilter{Authorized: scope}.MatchesNothing(), "an omitted list leaves the scope to the store")
	require.True(t, VectorFilter{Authorized: scope, DocumentIDs: []string{}}.MatchesNothing())
}
