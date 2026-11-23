package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Previews loads document previews for authorized routing candidates.
type Previews struct {
	Pool       *pgxpool.Pool
	Collection string
	DocType    string
}

// DocumentPreviews returns previews for completed candidates in its scope.
func (p Previews) DocumentPreviews(ctx context.Context, ids []string) (map[string]string, error) {
	previews := make(map[string]string)
	if len(ids) == 0 {
		return previews, nil
	}
	rows, err := p.Pool.Query(ctx, `
SELECT id::text, content_preview FROM documents
WHERE id = ANY($1::uuid[]) AND status='completed' AND content_preview IS NOT NULL
  AND ($2='' OR collection=$2) AND ($3='' OR doc_type=$3)`, ids, p.Collection, p.DocType)
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
