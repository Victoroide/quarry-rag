package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/domain"
	pgstore "github.com/Victoroide/quarry-rag/internal/store/postgres"
	"github.com/pinecone-io/go-pinecone/v5/pinecone"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"
)

const scopedTenant = "00000000-0000-4000-8000-0000000000a1"

func scopedChunkText(docID string, index int) string {
	return fmt.Sprintf("chunk %s %d", docID, index)
}

func scopedDocument(n, chunks int) pgstore.BackfillDocument {
	doc := pgstore.BackfillDocument{
		DocumentID:    fmt.Sprintf("00000000-0000-4000-8000-%012d", n),
		Incarnation:   fmt.Sprintf("00000000-0000-4000-9000-%012d", n),
		TenantID:      scopedTenant,
		SourceVersion: "version",
		ChunkDigest:   "digest",
	}
	for i := range chunks {
		doc.ChunkIDs = append(doc.ChunkIDs, fmt.Sprintf("%032x", n*1000+i))
		sum := sha256.Sum256([]byte(scopedChunkText(doc.DocumentID, i)))
		doc.ChunkTextDigests = append(doc.ChunkTextDigests, hex.EncodeToString(sum[:]))
		doc.ChunkTokenCounts = append(doc.ChunkTokenCounts, int32(i+1))
	}
	return doc
}

func mustPhysicalID(t *testing.T, doc pgstore.BackfillDocument, chunkID string) string {
	t.Helper()
	id, err := physicalID(doc, chunkID)
	require.NoError(t, err)
	return id
}

type scopedFakeSource struct {
	documents []pgstore.BackfillDocument
	changed   map[string]bool
	published []string
	listErr   error
	listed    int
}

func (s *scopedFakeSource) BackfillCandidates(_ context.Context, after string, limit int) ([]pgstore.BackfillDocument, error) {
	s.listed++
	if s.listErr != nil {
		return nil, s.listErr
	}
	var batch []pgstore.BackfillDocument
	for _, doc := range s.documents {
		if after == "" || doc.DocumentID > after {
			batch = append(batch, doc)
		}
	}
	sort.Slice(batch, func(i, j int) bool { return batch[i].DocumentID < batch[j].DocumentID })
	return batch[:min(len(batch), limit)], nil
}

func (s *scopedFakeSource) PublishVerifiedDocument(_ context.Context, doc pgstore.BackfillDocument) (bool, error) {
	if s.changed[doc.DocumentID] {
		return false, nil
	}
	s.published = append(s.published, doc.DocumentID)
	return true, nil
}

type scopedFakeIndex struct {
	vectors   map[string]map[string]any
	updates   []string
	fetches   [][]string
	updateErr map[string]error
	dropAfter bool
}

func (i *scopedFakeIndex) add(doc pgstore.BackfillDocument) {
	if i.vectors == nil {
		i.vectors = map[string]map[string]any{}
	}
	for index, id := range doc.ChunkIDs {
		var tokenCount float64
		if index < len(doc.ChunkTokenCounts) {
			tokenCount = float64(doc.ChunkTokenCounts[index])
		}
		fields := map[string]any{
			"doc_id":      doc.DocumentID,
			"chunk_index": float64(index),
			"text":        scopedChunkText(doc.DocumentID, index),
			"token_count": tokenCount,
		}
		if doc.Scoped {
			fields[domain.VectorRevisionKey], fields[domain.VectorAttemptKey] = doc.Incarnation, doc.Attempt
		}
		key, err := physicalID(doc, id)
		if err == nil {
			i.vectors[key] = fields
		}
	}
}

func (i *scopedFakeIndex) FetchVectors(_ context.Context, ids []string) (*pinecone.FetchVectorsResponse, error) {
	i.fetches = append(i.fetches, ids)
	res := &pinecone.FetchVectorsResponse{Vectors: map[string]*pinecone.Vector{}}
	for _, id := range ids {
		fields, ok := i.vectors[id]
		if !ok {
			continue
		}
		metadata, err := structpb.NewStruct(fields)
		if err != nil {
			return nil, err
		}
		res.Vectors[id] = &pinecone.Vector{Id: id, Metadata: metadata}
	}
	return res, nil
}

func (i *scopedFakeIndex) UpdateVector(_ context.Context, req *pinecone.UpdateVectorRequest) error {
	if err := i.updateErr[req.Id]; err != nil {
		return err
	}
	i.updates = append(i.updates, req.Id)
	if i.dropAfter {
		return nil
	}
	for key, value := range req.Metadata.AsMap() {
		i.vectors[req.Id][key] = value
	}
	return nil
}

func TestScopedBackfillLabelsOnlyCurrentVerifiedChunks(t *testing.T) {
	doc := scopedDocument(1, 3)
	idx := &scopedFakeIndex{}
	idx.add(doc)
	stale := fmt.Sprintf("%032x", 99999)
	idx.vectors[stale] = map[string]any{"doc_id": doc.DocumentID, "chunk_index": float64(7)}
	source := &scopedFakeSource{documents: []pgstore.BackfillDocument{doc}}
	report, err := backfillScoped(t.Context(), source, hostedVectors{idx}, options{Apply: true, Limit: 10})
	require.NoError(t, err)
	require.Equal(t, scopedReport{Examined: 1, Verified: 1, ResumeAfter: doc.DocumentID}, report)
	require.Equal(t, doc.ChunkIDs, idx.updates, "exactly the current chunks are labeled")
	for _, id := range doc.ChunkIDs {
		require.Equal(t, scopedTenant, idx.vectors[id][domain.VectorTenantKey])
		require.Equal(t, doc.Incarnation, idx.vectors[id][domain.VectorIncarnationKey])
	}
	require.NotContains(t, idx.vectors[stale], domain.VectorTenantKey, "a stale vector never becomes eligible")
	require.Equal(t, []string{doc.DocumentID}, source.published)
}

func TestScopedBackfillQuarantinesUnverifiableDocuments(t *testing.T) {
	missing, wrongDoc, wrongIndex, empty := scopedDocument(1, 2), scopedDocument(2, 2), scopedDocument(3, 2), scopedDocument(4, 0)
	idx := &scopedFakeIndex{}
	for _, doc := range []pgstore.BackfillDocument{missing, wrongDoc, wrongIndex} {
		idx.add(doc)
	}
	delete(idx.vectors, missing.ChunkIDs[1])
	idx.vectors[wrongDoc.ChunkIDs[0]]["doc_id"] = missing.DocumentID
	idx.vectors[wrongIndex.ChunkIDs[0]]["chunk_index"] = float64(5)
	source := &scopedFakeSource{documents: []pgstore.BackfillDocument{missing, wrongDoc, wrongIndex, empty}}
	report, err := backfillScoped(t.Context(), source, hostedVectors{idx}, options{Apply: true, Limit: 10})
	require.NoError(t, err)
	require.Equal(t, scopedReport{Examined: 4, Quarantined: 4, ResumeAfter: empty.DocumentID}, report)
	require.Empty(t, idx.updates, "nothing is labeled until the whole document is verified")
	require.Empty(t, source.published)
}

func TestScopedBackfillRequiresLabelsToBeReadBack(t *testing.T) {
	doc := scopedDocument(1, 2)
	idx := &scopedFakeIndex{dropAfter: true}
	idx.add(doc)
	source := &scopedFakeSource{documents: []pgstore.BackfillDocument{doc}}
	report, err := backfillScoped(t.Context(), source, hostedVectors{idx}, options{Apply: true, Limit: 10})
	require.NoError(t, err)
	require.Equal(t, 1, report.Quarantined)
	require.Empty(t, source.published, "an unconfirmed label never publishes readiness")
}

func TestScopedBackfillLeavesChangedDocumentsQuarantined(t *testing.T) {
	doc := scopedDocument(1, 1)
	idx := &scopedFakeIndex{}
	idx.add(doc)
	source := &scopedFakeSource{documents: []pgstore.BackfillDocument{doc}, changed: map[string]bool{doc.DocumentID: true}}
	report, err := backfillScoped(t.Context(), source, hostedVectors{idx}, options{Apply: true, Limit: 10})
	require.NoError(t, err)
	require.Equal(t, scopedReport{Examined: 1, Changed: 1, ResumeAfter: doc.DocumentID}, report)
	require.Empty(t, source.published)
}

func TestScopedBackfillResumesAfterAnInterruption(t *testing.T) {
	docs := []pgstore.BackfillDocument{scopedDocument(1, 2), scopedDocument(2, 2), scopedDocument(3, 2)}
	idx := &scopedFakeIndex{updateErr: map[string]error{docs[1].ChunkIDs[1]: errors.New("index unavailable")}}
	for _, doc := range docs {
		idx.add(doc)
	}
	source := &scopedFakeSource{documents: docs}
	report, err := backfillScoped(t.Context(), source, hostedVectors{idx}, options{Apply: true, Limit: 10})
	require.ErrorContains(t, err, "index unavailable")
	require.ErrorContains(t, err, fmt.Sprintf("-after=%q", docs[0].DocumentID))
	require.Equal(t, 1, report.Verified)
	require.Equal(t, docs[0].DocumentID, report.ResumeAfter)
	require.Equal(t, []string{docs[0].DocumentID}, source.published)

	// The interrupted document was partly labeled; it never published, and a rerun repeats it safely.
	require.Equal(t, scopedTenant, idx.vectors[docs[1].ChunkIDs[0]][domain.VectorTenantKey])
	idx.updateErr = nil
	source.documents = docs[1:]
	resumed, err := backfillScoped(t.Context(), source, hostedVectors{idx}, options{Apply: true, Limit: 10, After: report.ResumeAfter})
	require.NoError(t, err)
	require.Equal(t, 2, resumed.Verified)
	require.Equal(t, []string{docs[0].DocumentID, docs[1].DocumentID, docs[2].DocumentID}, source.published)
}

func TestScopedBackfillStopsWhenCanceled(t *testing.T) {
	docs := []pgstore.BackfillDocument{scopedDocument(1, 1), scopedDocument(2, 1)}
	idx := &scopedFakeIndex{}
	for _, doc := range docs {
		idx.add(doc)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	report, err := backfillScoped(ctx, &scopedFakeSource{documents: docs}, hostedVectors{idx}, options{Apply: true, Limit: 10})
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, report.Examined)
	require.Empty(t, idx.updates)
}

func TestScopedBackfillDryRunTouchesNothing(t *testing.T) {
	docs := []pgstore.BackfillDocument{scopedDocument(1, 2), scopedDocument(2, 2)}
	source := &scopedFakeSource{documents: docs}
	report, err := backfillScoped(t.Context(), source, nil, options{Limit: 10})
	require.NoError(t, err)
	require.Equal(t, scopedReport{Examined: 2, ResumeAfter: docs[1].DocumentID}, report)
	require.Empty(t, source.published)
}

func TestScopedBackfillHonorsTheLimitAndSourceErrors(t *testing.T) {
	var docs []pgstore.BackfillDocument
	for n := 1; n <= 5; n++ {
		docs = append(docs, scopedDocument(n, 1))
	}
	report, err := backfillScoped(t.Context(), &scopedFakeSource{documents: docs}, nil, options{Limit: 3})
	require.NoError(t, err)
	require.Equal(t, 3, report.Examined)
	require.Equal(t, docs[2].DocumentID, report.ResumeAfter)
	_, err = backfillScoped(t.Context(), &scopedFakeSource{listErr: errors.New("database unavailable")}, nil, options{Limit: 3, After: docs[0].DocumentID})
	require.ErrorContains(t, err, fmt.Sprintf("-after=%q", docs[0].DocumentID))
	_, err = backfillScoped(t.Context(), &scopedFakeSource{}, nil, options{})
	require.Error(t, err)
}

func TestHostedVerificationFetchesInBoundedBatches(t *testing.T) {
	doc := scopedDocument(1, hostedFetchBatch*2+5)
	idx := &scopedFakeIndex{}
	idx.add(doc)
	verified, err := hostedVectors{idx}.Verify(t.Context(), doc)
	require.NoError(t, err)
	require.True(t, verified)
	for _, batch := range idx.fetches {
		require.LessOrEqual(t, len(batch), hostedFetchBatch)
	}
	require.Len(t, idx.updates, len(doc.ChunkIDs))
}

func TestScopedBackfillFlags(t *testing.T) {
	opts, err := parseOptions([]string{"-backfill-scoped", "-apply", "-after=" + storedID}, &bytes.Buffer{})
	require.NoError(t, err)
	require.True(t, opts.BackfillScoped)
	require.Equal(t, storedID, opts.After)
	for _, args := range [][]string{
		{"-backfill-scoped", "-backfill-collections"},
		{"-after=" + storedID},
		{"-backfill-scoped", "-after=not-a-uuid"},
	} {
		_, err = parseOptions(args, &bytes.Buffer{})
		require.Error(t, err, args)
	}
}

func TestScopedBackfillLabelsTaggedDocumentsByTheirQualifiedIdentifiers(t *testing.T) {
	doc := scopedDocument(1, 2)
	doc.Scoped, doc.Attempt = true, "00000000-0000-4000-a000-000000000001"
	idx := &scopedFakeIndex{}
	idx.add(doc)
	source := &scopedFakeSource{documents: []pgstore.BackfillDocument{doc}}
	report, err := backfillScoped(t.Context(), source, hostedVectors{idx}, options{Apply: true, Limit: 10})
	require.NoError(t, err)
	require.Equal(t, 1, report.Verified)
	var qualified []string
	for _, id := range doc.ChunkIDs {
		qualified = append(qualified, doc.DocumentID+"/"+doc.Incarnation+"/"+doc.Attempt+"/"+id)
	}
	require.Equal(t, qualified, idx.updates)
	for _, id := range qualified {
		require.Equal(t, scopedTenant, idx.vectors[id][domain.VectorTenantKey])
		require.NotContains(t, idx.vectors[id], domain.VectorIncarnationKey, "the revision tag already names the incarnation")
	}
}

func TestScopedBackfillQuarantinesFormatMismatches(t *testing.T) {
	tagged := scopedDocument(1, 1)
	tagged.Scoped, tagged.Attempt = true, "00000000-0000-4000-a000-000000000001"
	noAttempt := scopedDocument(2, 1)
	noAttempt.Scoped = true
	retagged := scopedDocument(3, 1)
	idx := &scopedFakeIndex{}
	idx.add(tagged)
	idx.add(retagged)
	idx.vectors[mustPhysicalID(t, tagged, tagged.ChunkIDs[0])][domain.VectorRevisionKey] = "another-revision"
	idx.vectors[retagged.ChunkIDs[0]][domain.VectorRevisionKey] = retagged.Incarnation
	source := &scopedFakeSource{documents: []pgstore.BackfillDocument{tagged, noAttempt, retagged}}
	report, err := backfillScoped(t.Context(), source, hostedVectors{idx}, options{Apply: true, Limit: 10})
	require.NoError(t, err)
	require.Equal(t, 3, report.Quarantined, "a wrong revision, a missing attempt and an unexpected tag all stay quarantined")
	require.Empty(t, idx.updates)
}

func TestScopedBackfillRejectsRetiredIncarnationAndStaleContent(t *testing.T) {
	retiredIncarnation := "00000000-0000-4000-9000-000000000099"

	t.Run("retired incarnation with stale text", func(t *testing.T) {
		doc := scopedDocument(7, 2)
		idx := &scopedFakeIndex{}
		idx.add(doc)
		for _, id := range doc.ChunkIDs {
			idx.vectors[id][domain.VectorIncarnationKey] = retiredIncarnation
			idx.vectors[id]["text"] = "stale content"
		}
		source := &scopedFakeSource{documents: []pgstore.BackfillDocument{doc}}
		report, err := backfillScoped(t.Context(), source, hostedVectors{idx}, options{Apply: true, Limit: 10})
		require.NoError(t, err)
		require.Equal(t, scopedReport{Examined: 1, Quarantined: 1, ResumeAfter: doc.DocumentID}, report)
		require.Empty(t, idx.updates)
		require.Empty(t, source.published)
		for _, id := range doc.ChunkIDs {
			require.Equal(t, retiredIncarnation, idx.vectors[id][domain.VectorIncarnationKey])
			require.NotContains(t, idx.vectors[id], domain.VectorTenantKey)
		}
	})

	t.Run("retired incarnation with matching text", func(t *testing.T) {
		doc := scopedDocument(8, 1)
		idx := &scopedFakeIndex{}
		idx.add(doc)
		idx.vectors[doc.ChunkIDs[0]][domain.VectorIncarnationKey] = retiredIncarnation
		source := &scopedFakeSource{documents: []pgstore.BackfillDocument{doc}}
		report, err := backfillScoped(t.Context(), source, hostedVectors{idx}, options{Apply: true, Limit: 10})
		require.NoError(t, err)
		require.Equal(t, 1, report.Quarantined)
		require.Empty(t, idx.updates)
		require.Equal(t, retiredIncarnation, idx.vectors[doc.ChunkIDs[0]][domain.VectorIncarnationKey])
	})

	t.Run("untagged vector with stale text or token count or missing fields", func(t *testing.T) {
		staleText := scopedDocument(9, 1)
		staleTokens := scopedDocument(10, 1)
		missingText := scopedDocument(11, 1)
		missingTokens := scopedDocument(12, 1)
		missingIndex := scopedDocument(13, 1)
		idx := &scopedFakeIndex{}
		for _, doc := range []pgstore.BackfillDocument{staleText, staleTokens, missingText, missingTokens, missingIndex} {
			idx.add(doc)
		}
		idx.vectors[staleText.ChunkIDs[0]]["text"] = "other text"
		idx.vectors[staleTokens.ChunkIDs[0]]["token_count"] = float64(999)
		delete(idx.vectors[missingText.ChunkIDs[0]], "text")
		delete(idx.vectors[missingTokens.ChunkIDs[0]], "token_count")
		delete(idx.vectors[missingIndex.ChunkIDs[0]], "chunk_index")
		docs := []pgstore.BackfillDocument{staleText, staleTokens, missingText, missingTokens, missingIndex}
		source := &scopedFakeSource{documents: docs}
		report, err := backfillScoped(t.Context(), source, hostedVectors{idx}, options{Apply: true, Limit: 10})
		require.NoError(t, err)
		require.Equal(t, len(docs), report.Quarantined)
		require.Empty(t, idx.updates)
		require.Empty(t, source.published)
	})

	t.Run("tagged vector with stale text or token count", func(t *testing.T) {
		staleText := scopedDocument(14, 1)
		staleText.Scoped, staleText.Attempt = true, "00000000-0000-4000-a000-000000000001"
		staleTokens := scopedDocument(15, 1)
		staleTokens.Scoped, staleTokens.Attempt = true, "00000000-0000-4000-a000-000000000002"
		idx := &scopedFakeIndex{}
		idx.add(staleText)
		idx.add(staleTokens)
		idx.vectors[mustPhysicalID(t, staleText, staleText.ChunkIDs[0])]["text"] = "wrong tagged text"
		idx.vectors[mustPhysicalID(t, staleTokens, staleTokens.ChunkIDs[0])]["token_count"] = float64(42)
		source := &scopedFakeSource{documents: []pgstore.BackfillDocument{staleText, staleTokens}}
		report, err := backfillScoped(t.Context(), source, hostedVectors{idx}, options{Apply: true, Limit: 10})
		require.NoError(t, err)
		require.Equal(t, 2, report.Quarantined)
		require.Empty(t, idx.updates)
		require.Empty(t, source.published)
	})
}
