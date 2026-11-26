package postgres

import (
	"context"
	"fmt"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Previews loads document previews for authorized routing candidates.
type Previews struct {
	Pool       *pgxpool.Pool
	Collection string
	DocType    string
	// Subject, when set, limits previews to documents it may read now.
	Subject *domain.Principal
}

// DocumentPreviews returns previews for completed candidates in its scope.
func (p Previews) DocumentPreviews(ctx context.Context, ids []string) (map[string]string, error) {
	previews := make(map[string]string)
	if len(ids) == 0 {
		return previews, nil
	}
	join, args := readableJoin(p.Subject, []any{ids, p.Collection, p.DocType})
	rows, err := p.Pool.Query(ctx, `
SELECT d.id::text, d.content_preview FROM documents d`+join+`
WHERE d.id = ANY($1::uuid[]) AND d.status='completed' AND d.content_preview IS NOT NULL
  AND ($2='' OR d.collection=$2) AND ($3='' OR d.doc_type=$3)`, args...)
	if err != nil {
		return nil, fmt.Errorf("postgres: document previews: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id, preview string
		if err := rows.Scan(&id, &preview); err != nil {
			return nil, fmt.Errorf("postgres: scan preview: %w", err)
		}
		previews[id] = preview
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: document previews: %w", err)
	}
	return previews, nil
}
