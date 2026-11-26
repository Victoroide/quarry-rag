package pgtest

import (
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

var sqlBlock = regexp.MustCompile("(?s)```sql\n(.*?)```")

// PublishedSQL reads the first SQL block containing marker from a repository guide.
func PublishedSQL(t testing.TB, docsDir, guide, marker string) string {
	t.Helper()
	docs, err := os.OpenRoot(docsDir)
	require.NoError(t, err)
	defer func() { require.NoError(t, docs.Close()) }()
	body, err := docs.ReadFile(guide)
	require.NoError(t, err)
	for _, block := range sqlBlock.FindAllStringSubmatch(string(body), -1) {
		if strings.Contains(block[1], marker) {
			return block[1]
		}
	}
	require.FailNow(t, "published SQL block not found", marker)
	return ""
}

// ReplaceRoles substitutes documented role names with quoted fixture identifiers.
func ReplaceRoles(statement string, roles map[string]string) string {
	pairs := make([]string, 0, 2*len(roles))
	for role, name := range roles {
		pairs = append(pairs, role, pgx.Identifier{name}.Sanitize())
	}
	return strings.NewReplacer(pairs...).Replace(statement)
}
