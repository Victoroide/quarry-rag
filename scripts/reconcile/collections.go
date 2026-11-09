package main

import (
	"context"
	"fmt"

	pgstore "github.com/Victoroide/quarry-rag/internal/store/postgres"
	"github.com/pinecone-io/go-pinecone/v5/pinecone"
	"google.golang.org/protobuf/types/known/structpb"
)

type collectionDocument struct {
	ID         string `json:"document_id"`
	Collection string `json:"collection"`
}

type collectionIndex interface {
	UpdateVectorsByMetadata(context.Context, *pinecone.UpdateVectorsByMetadataRequest) (*pinecone.UpdateVectorsByMetadataResponse, error)
}

type collectionReport struct {
	Documents []collectionDocument `json:"documents"`
	Updated   int                  `json:"updated_documents"`
}

func listCollectionDocuments(ctx context.Context, pg *pgstore.Client, after string, limit int) ([]collectionDocument, error) {
	rows, err := pg.Pool.Query(ctx, `SELECT id::text,collection FROM documents
		WHERE collection <> '' AND ($1::text = '' OR id > NULLIF($1::text, '')::uuid) ORDER BY documents.id LIMIT $2`, after, limit)
	if err != nil {
		return nil, fmt.Errorf("list document collections: %w", err)
	}
	defer rows.Close()
	docs := make([]collectionDocument, 0)
	for rows.Next() {
		var doc collectionDocument
		if err := rows.Scan(&doc.ID, &doc.Collection); err != nil {
			return nil, fmt.Errorf("read document collection: %w", err)
		}
		docs = append(docs, doc)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list document collections: %w", err)
	}
	return docs, nil
}

func backfillCollections(ctx context.Context, idx collectionIndex, docs []collectionDocument, opts options) (collectionReport, error) {
	result := collectionReport{Documents: []collectionDocument{}}
	if opts.Limit < 1 {
		return result, fmt.Errorf("backfill limit must be greater than zero")
	}
	last := opts.After
	for _, doc := range docs[:min(len(docs), opts.Limit)] {
		if err := ctx.Err(); err != nil {
			return result, fmt.Errorf("backfill stopped before document %s: %w; resume with -after=%q", doc.ID, err, last)
		}
		result.Documents = append(result.Documents, doc)
		if !opts.Apply {
			continue
		}
		filter := &structpb.Struct{Fields: map[string]*structpb.Value{"doc_id": structpb.NewStructValue(&structpb.Struct{Fields: map[string]*structpb.Value{"$eq": structpb.NewStringValue(doc.ID)}})}}
		metadata := &structpb.Struct{Fields: map[string]*structpb.Value{"collection": structpb.NewStringValue(doc.Collection)}}
		if _, err := idx.UpdateVectorsByMetadata(ctx, &pinecone.UpdateVectorsByMetadataRequest{Filter: filter, Metadata: metadata}); err != nil {
			return result, fmt.Errorf("backfill document %s: %w; resume with -after=%q", doc.ID, err, last)
		}
		result.Updated++
		last = doc.ID
	}
	return result, nil
}
