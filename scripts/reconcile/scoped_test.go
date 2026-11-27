package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/domain"
	pgstore "github.com/Victoroide/quarry-rag/internal/store/postgres"
	"github.com/google/uuid"
	"github.com/pinecone-io/go-pinecone/v5/pinecone"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
		Hosted:        &pgstore.HostedBackfillSnapshot{Valid: true, SourceCurrent: true, Digest: "legacy digest", DeclaredChunks: chunks},
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
	documents  []pgstore.BackfillDocument
	changed    map[string]bool
	published  []string
	publishErr error
	listErr    error
	listed     int
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
	if s.publishErr != nil {
		return false, s.publishErr
	}
	if s.changed[doc.DocumentID] {
		return false, nil
	}
	s.published = append(s.published, doc.DocumentID)
	return true, nil
}

const scopedFixtureDimensions = 3

type scopedFakeIndex struct {
	vectors     map[string]map[string]any
	updates     []string
	fetches     [][]string
	updateErr   map[string]error
	dropAfter   bool
	namespace   string
	afterFetch  func(*pinecone.FetchVectorsResponse) (*pinecone.FetchVectorsResponse, error)
	afterUpdate func()
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
		if doc.Hosted != nil && doc.Hosted.Present {
			fields["revision_id"] = doc.Hosted.RevisionID
			fields["chunk_id"] = id
			fields["collection"] = doc.Hosted.Collection
			i.vectors[doc.Hosted.VectorIDs[index]] = fields
			continue
		}
		key, err := physicalID(doc, id)
		if err == nil {
			i.vectors[key] = fields
		}
	}
}

func (i *scopedFakeIndex) FetchVectors(_ context.Context, ids []string) (*pinecone.FetchVectorsResponse, error) {
	i.fetches = append(i.fetches, ids)
	res := &pinecone.FetchVectorsResponse{Namespace: i.namespace, Vectors: map[string]*pinecone.Vector{}}
	for _, id := range ids {
		fields, ok := i.vectors[id]
		if !ok {
			continue
		}
		metadata, err := structpb.NewStruct(fields)
		if err != nil {
			return nil, err
		}
		values := []float32{1, 0, 0}
		res.Vectors[id] = &pinecone.Vector{Id: id, Metadata: metadata, Values: &values}
	}
	if i.afterFetch != nil {
		return i.afterFetch(res)
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
	if i.afterUpdate != nil {
		i.afterUpdate()
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
	report, err := backfillScoped(t.Context(), source, hostedVectors{idx: idx, dimensions: scopedFixtureDimensions}, options{Apply: true, Limit: 10})
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
	report, err := backfillScoped(t.Context(), source, hostedVectors{idx: idx, dimensions: scopedFixtureDimensions}, options{Apply: true, Limit: 10})
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
	report, err := backfillScoped(t.Context(), source, hostedVectors{idx: idx, dimensions: scopedFixtureDimensions}, options{Apply: true, Limit: 10})
	require.NoError(t, err)
	require.Equal(t, 1, report.Quarantined)
	require.Empty(t, source.published, "an unconfirmed label never publishes readiness")
}

func TestScopedBackfillLeavesChangedDocumentsQuarantined(t *testing.T) {
	doc := scopedDocument(1, 1)
	idx := &scopedFakeIndex{}
	idx.add(doc)
	source := &scopedFakeSource{documents: []pgstore.BackfillDocument{doc}, changed: map[string]bool{doc.DocumentID: true}}
	report, err := backfillScoped(t.Context(), source, hostedVectors{idx: idx, dimensions: scopedFixtureDimensions}, options{Apply: true, Limit: 10})
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
	report, err := backfillScoped(t.Context(), source, hostedVectors{idx: idx, dimensions: scopedFixtureDimensions}, options{Apply: true, Limit: 10})
	require.ErrorContains(t, err, "index unavailable")
	require.ErrorContains(t, err, fmt.Sprintf("-after=%q", docs[0].DocumentID))
	require.Equal(t, 1, report.Verified)
	require.Equal(t, docs[0].DocumentID, report.ResumeAfter)
	require.Equal(t, []string{docs[0].DocumentID}, source.published)

	// The interrupted document was partly labeled; it never published, and a rerun repeats it safely.
	require.Equal(t, scopedTenant, idx.vectors[docs[1].ChunkIDs[0]][domain.VectorTenantKey])
	idx.updateErr = nil
	source.documents = docs[1:]
	resumed, err := backfillScoped(t.Context(), source, hostedVectors{idx: idx, dimensions: scopedFixtureDimensions}, options{Apply: true, Limit: 10, After: report.ResumeAfter})
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
	report, err := backfillScoped(ctx, &scopedFakeSource{documents: docs}, hostedVectors{idx: idx, dimensions: scopedFixtureDimensions}, options{Apply: true, Limit: 10})
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
	verified, err := hostedVectors{idx: idx, dimensions: scopedFixtureDimensions}.Verify(t.Context(), doc)
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
	report, err := backfillScoped(t.Context(), source, hostedVectors{idx: idx, dimensions: scopedFixtureDimensions}, options{Apply: true, Limit: 10})
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
	report, err := backfillScoped(t.Context(), source, hostedVectors{idx: idx, dimensions: scopedFixtureDimensions}, options{Apply: true, Limit: 10})
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
		report, err := backfillScoped(t.Context(), source, hostedVectors{idx: idx, dimensions: scopedFixtureDimensions}, options{Apply: true, Limit: 10})
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
		report, err := backfillScoped(t.Context(), source, hostedVectors{idx: idx, dimensions: scopedFixtureDimensions}, options{Apply: true, Limit: 10})
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
		report, err := backfillScoped(t.Context(), source, hostedVectors{idx: idx, dimensions: scopedFixtureDimensions}, options{Apply: true, Limit: 10})
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
		report, err := backfillScoped(t.Context(), source, hostedVectors{idx: idx, dimensions: scopedFixtureDimensions}, options{Apply: true, Limit: 10})
		require.NoError(t, err)
		require.Equal(t, 2, report.Quarantined)
		require.Empty(t, idx.updates)
		require.Empty(t, source.published)
	})
}

func scopedManifest(n, chunks int) pgstore.BackfillDocument {
	doc := scopedDocument(n, chunks)
	doc.Hosted.Present = true
	doc.Hosted.RevisionID = uuid.NewString()
	doc.Hosted.Digest = "manifest digest"
	doc.Hosted.Collection = "archive"
	for i, chunkID := range doc.ChunkIDs {
		doc.Hosted.VectorIDs = append(doc.Hosted.VectorIDs, domain.FormatRevisionVectorID(domain.DocumentID(doc.DocumentID), doc.Hosted.RevisionID, i))
		doc.Hosted.ChunkIDs = append(doc.Hosted.ChunkIDs, chunkID)
		doc.Hosted.ChunkIndexes = append(doc.Hosted.ChunkIndexes, int32(i))
	}
	return doc
}

func TestScopedBackfillManifestWireBinding(t *testing.T) {
	for _, n := range []int{1, 2, hostedFetchBatch + 1} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			doc := scopedManifest(1, n)
			idx := &scopedFakeIndex{}
			if n > 1 {
				idx.namespace = defaultVectorNamespace
			} else {
				doc.Hosted.Collection = ""
			}
			idx.add(doc)
			first := idx.vectors[doc.Hosted.VectorIDs[0]]
			first["chunk_id"] = uuid.MustParse(doc.ChunkIDs[0]).String()
			delete(first, "collection")
			if n > 1 {
				first["collection"] = doc.Hosted.Collection
				first[domain.VectorIncarnationKey] = doc.Incarnation
				first[domain.VectorTenantKey] = doc.TenantID
			}
			idx.vectors[doc.ChunkIDs[0]] = map[string]any{"doc_id": doc.DocumentID}
			source := &scopedFakeSource{documents: []pgstore.BackfillDocument{doc}}
			report, err := backfillScoped(t.Context(), source, hostedVectors{idx: idx, dimensions: scopedFixtureDimensions}, options{Apply: true, Limit: 1})
			require.NoError(t, err)
			require.Equal(t, scopedReport{Examined: 1, Verified: 1, ResumeAfter: doc.DocumentID}, report)
			require.Equal(t, []string{doc.DocumentID}, source.published)
			require.Equal(t, doc.Hosted.VectorIDs, idx.updates)
			batches := (n + hostedFetchBatch - 1) / hostedFetchBatch
			require.Len(t, idx.fetches, 2*batches)
			require.Equal(t, idx.fetches[:batches], idx.fetches[batches:])
			require.NotContains(t, idx.vectors[doc.ChunkIDs[0]], domain.VectorTenantKey)
			for _, id := range doc.Hosted.VectorIDs {
				require.Equal(t, doc.TenantID, idx.vectors[id][domain.VectorTenantKey])
				require.Equal(t, doc.Incarnation, idx.vectors[id][domain.VectorIncarnationKey])
			}
		})
	}
	t.Run("incomplete second batch", func(t *testing.T) {
		doc := scopedManifest(1, hostedFetchBatch+1)
		idx := &scopedFakeIndex{}
		idx.add(doc)
		delete(idx.vectors, doc.Hosted.VectorIDs[hostedFetchBatch])
		source := &scopedFakeSource{documents: []pgstore.BackfillDocument{doc}}
		report, err := backfillScoped(t.Context(), source, hostedVectors{idx: idx, dimensions: scopedFixtureDimensions}, options{Apply: true, Limit: 1})
		require.NoError(t, err)
		require.Equal(t, 1, report.Quarantined)
		require.Len(t, idx.fetches, 2)
		require.Empty(t, idx.updates)
		require.Empty(t, source.published)
	})

	for name, mutate := range map[string]func(*pgstore.BackfillDocument){
		"swapped vectors": func(d *pgstore.BackfillDocument) {
			d.Hosted.VectorIDs[0], d.Hosted.VectorIDs[1] = d.Hosted.VectorIDs[1], d.Hosted.VectorIDs[0]
		},
		"swapped chunks": func(d *pgstore.BackfillDocument) {
			d.Hosted.ChunkIDs[0], d.Hosted.ChunkIDs[1] = d.Hosted.ChunkIDs[1], d.Hosted.ChunkIDs[0]
		},
		"reused vector": func(d *pgstore.BackfillDocument) { d.Hosted.VectorIDs[1] = d.Hosted.VectorIDs[0] },
		"reused chunk": func(d *pgstore.BackfillDocument) {
			d.ChunkIDs[1], d.Hosted.ChunkIDs[1] = d.ChunkIDs[0], d.Hosted.ChunkIDs[0]
		},
		"sparse indexes":    func(d *pgstore.BackfillDocument) { d.Hosted.ChunkIndexes[1] = 2 },
		"duplicate indexes": func(d *pgstore.BackfillDocument) { d.Hosted.ChunkIndexes[1] = 0 },
		"negative index":    func(d *pgstore.BackfillDocument) { d.Hosted.ChunkIndexes[0] = -1 },
		"wrong key":         func(d *pgstore.BackfillDocument) { d.Hosted.VectorIDs[0] = d.ChunkIDs[0] },
		"wrong count":       func(d *pgstore.BackfillDocument) { d.Hosted.DeclaredChunks++ },
		"incomplete arrays": func(d *pgstore.BackfillDocument) { d.Hosted.VectorIDs = d.Hosted.VectorIDs[:1] },
		"invalid manifest":  func(d *pgstore.BackfillDocument) { d.Hosted.Valid = false },
		"deletion reserved": func(d *pgstore.BackfillDocument) { d.Hosted.DeletionReserved = true },
	} {
		t.Run(name, func(t *testing.T) {
			doc := scopedManifest(1, 2)
			idx := &scopedFakeIndex{}
			idx.add(doc)
			mutate(&doc)
			source := &scopedFakeSource{documents: []pgstore.BackfillDocument{doc}}
			report, err := backfillScoped(t.Context(), source, hostedVectors{idx: idx, dimensions: scopedFixtureDimensions}, options{Apply: true, Limit: 1})
			require.NoError(t, err)
			require.Equal(t, 1, report.Quarantined)
			require.Empty(t, idx.fetches)
			require.Empty(t, idx.updates)
			require.Empty(t, source.published)
		})
	}
	for key, wrong := range map[string]any{
		"doc_id": uuid.NewString(), "chunk_index": 4., "revision_id": uuid.NewString(),
		"chunk_id": uuid.NewString(), "text": "stale", "token_count": 9., "collection": "foreign",
		domain.VectorTenantKey: uuid.NewString(), domain.VectorIncarnationKey: uuid.NewString(),
		domain.VectorRevisionKey: "", domain.VectorAttemptKey: "",
	} {
		modes := []string{"wrong", "type"}
		switch key {
		case "doc_id", "chunk_index", "revision_id", "chunk_id", "text", "token_count", "collection":
			modes = append(modes, "missing")
		}
		for _, mode := range modes {
			t.Run(key+"/"+mode, func(t *testing.T) {
				doc := scopedManifest(1, 2)
				idx := &scopedFakeIndex{}
				idx.add(doc)
				fields := idx.vectors[doc.Hosted.VectorIDs[1]]
				switch mode {
				case "wrong":
					fields[key] = wrong
				case "type":
					fields[key] = true
				case "missing":
					delete(fields, key)
				}
				source := &scopedFakeSource{documents: []pgstore.BackfillDocument{doc}}
				report, err := backfillScoped(t.Context(), source, hostedVectors{idx: idx, dimensions: scopedFixtureDimensions}, options{Apply: true, Limit: 1})
				require.NoError(t, err)
				require.Equal(t, 1, report.Quarantined)
				require.Empty(t, idx.updates)
				require.Empty(t, source.published)
			})
		}
	}
	t.Run("stale assignment and missing snapshot", func(t *testing.T) {
		doc := scopedManifest(1, 1)
		idx := &scopedFakeIndex{}
		doc.Hosted.SourceCurrent = false
		source := &scopedFakeSource{documents: []pgstore.BackfillDocument{doc}}
		vectors := hostedVectors{idx: idx, dimensions: scopedFixtureDimensions}
		report, err := backfillScoped(t.Context(), source, vectors, options{Apply: true, Limit: 1})
		require.NoError(t, err)
		require.Equal(t, 1, report.Changed)
		source.documents[0].Hosted = nil
		_, err = backfillScoped(t.Context(), source, vectors, options{Apply: true, Limit: 1})
		require.ErrorContains(t, err, "snapshot")
		require.Empty(t, idx.fetches)
		require.Empty(t, source.published)
	})
	t.Run("legacy rejects manifested provenance", func(t *testing.T) {
		doc := scopedDocument(1, 1)
		idx := &scopedFakeIndex{}
		idx.add(doc)
		idx.vectors[doc.ChunkIDs[0]]["revision_id"] = ""
		ok, err := (hostedVectors{idx: idx, dimensions: scopedFixtureDimensions}).Verify(t.Context(), doc)
		require.NoError(t, err)
		require.False(t, ok)
		require.Empty(t, idx.updates)
	})
}

func TestScopedBackfillManifestFailuresAndResume(t *testing.T) {
	for _, afterWrite := range []bool{false, true} {
		for _, failure := range []string{"unavailable", "permission", "nil", "extra", "wrong id", "namespace", "missing", "stale", "dimension", "missing values", "canceled"} {
			t.Run(fmt.Sprintf("%s/readback=%t", failure, afterWrite), func(t *testing.T) {
				first, doc := scopedManifest(1, 1), scopedManifest(2, 2)
				idx := &scopedFakeIndex{}
				idx.add(first)
				idx.add(doc)
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				terminal := false
				idx.afterFetch = func(res *pinecone.FetchVectorsResponse) (*pinecone.FetchVectorsResponse, error) {
					id := doc.Hosted.VectorIDs[1]
					if res.Vectors[id] == nil || afterWrite != (len(idx.updates) == 3) {
						return res, nil
					}
					terminal = true
					switch failure {
					case "unavailable":
						return nil, status.Error(codes.Unavailable, "provider down")
					case "permission":
						return nil, status.Error(codes.PermissionDenied, "denied")
					case "nil":
						return nil, nil
					case "extra":
						res.Vectors["extra"] = res.Vectors[id]
					case "wrong id":
						res.Vectors[id].Id = "wrong"
					case "namespace":
						res.Namespace = "foreign"
					case "missing":
						delete(res.Vectors, id)
						terminal = false
					case "stale":
						res.Vectors[id].Metadata.Fields["text"] = structpb.NewStringValue("older content")
						terminal = false
					case "dimension":
						res.Vectors[id].Values = &[]float32{1, 0}
						terminal = false
					case "missing values":
						res.Vectors[id].Values = nil
						terminal = false
					case "canceled":
						cancel()
					}
					return res, nil
				}
				source := &scopedFakeSource{documents: []pgstore.BackfillDocument{first, doc}}
				vectors := hostedVectors{idx: idx, dimensions: scopedFixtureDimensions}
				report, err := backfillScoped(ctx, source, vectors, options{Apply: true, Limit: 2})
				require.Equal(t, []string{first.DocumentID}, source.published)
				if terminal {
					require.ErrorContains(t, err, fmt.Sprintf("-after=%q", first.DocumentID))
					require.Equal(t, first.DocumentID, report.ResumeAfter)
				} else {
					require.NoError(t, err)
					require.Equal(t, 1, report.Quarantined)
				}
				if !afterWrite {
					require.Equal(t, first.Hosted.VectorIDs, idx.updates)
				}
				idx.afterFetch = nil
				source.documents = source.documents[1:]
				report, err = backfillScoped(t.Context(), source, vectors, options{Apply: true, Limit: 2, After: first.DocumentID})
				require.NoError(t, err)
				require.Equal(t, 1, report.Verified)
				require.Equal(t, []string{first.DocumentID, doc.DocumentID}, source.published)
			})
		}
	}
	for _, cancelUpdate := range []bool{false, true} {
		t.Run(fmt.Sprintf("partial/cancel=%t", cancelUpdate), func(t *testing.T) {
			doc := scopedManifest(1, 2)
			idx := &scopedFakeIndex{}
			idx.add(doc)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if cancelUpdate {
				idx.afterUpdate = cancel
			} else {
				idx.updateErr = map[string]error{doc.Hosted.VectorIDs[1]: status.Error(codes.PermissionDenied, "denied")}
			}
			source := &scopedFakeSource{documents: []pgstore.BackfillDocument{doc}}
			vectors := hostedVectors{idx: idx, dimensions: scopedFixtureDimensions}
			report, err := backfillScoped(ctx, source, vectors, options{Apply: true, Limit: 1})
			require.Error(t, err)
			require.Empty(t, report.ResumeAfter)
			require.Equal(t, doc.Hosted.VectorIDs[:1], idx.updates)
			require.Empty(t, source.published)
			idx.afterUpdate, idx.updateErr = nil, nil
			report, err = backfillScoped(t.Context(), source, vectors, options{Apply: true, Limit: 1, After: report.ResumeAfter})
			require.NoError(t, err)
			require.Equal(t, 1, report.Verified)
			require.Equal(t, doc.Hosted.VectorIDs, idx.updates[1:])
			require.Equal(t, []string{doc.DocumentID}, source.published)
		})
	}
	t.Run("metadata has not converged", func(t *testing.T) {
		doc := scopedManifest(1, 2)
		idx := &scopedFakeIndex{dropAfter: true}
		idx.add(doc)
		source := &scopedFakeSource{documents: []pgstore.BackfillDocument{doc}}
		vectors := hostedVectors{idx: idx, dimensions: scopedFixtureDimensions}
		report, err := backfillScoped(t.Context(), source, vectors, options{Apply: true, Limit: 1})
		require.NoError(t, err)
		require.Equal(t, 1, report.Quarantined)
		require.Empty(t, source.published)
		require.Equal(t, doc.Hosted.VectorIDs, idx.updates)
		idx.dropAfter = false
		report, err = backfillScoped(t.Context(), source, vectors, options{Apply: true, Limit: 1})
		require.NoError(t, err)
		require.Equal(t, 1, report.Verified)
	})

	t.Run("publication failure with accepted labels", func(t *testing.T) {
		doc := scopedManifest(1, 2)
		idx := &scopedFakeIndex{}
		idx.add(doc)
		source := &scopedFakeSource{documents: []pgstore.BackfillDocument{doc}, publishErr: errors.New("database unavailable")}
		vectors := hostedVectors{idx: idx, dimensions: scopedFixtureDimensions}
		report, err := backfillScoped(t.Context(), source, vectors, options{Apply: true, Limit: 1})
		require.ErrorIs(t, err, source.publishErr)
		require.Empty(t, report.ResumeAfter)
		require.Empty(t, source.published)
		require.Equal(t, doc.Hosted.VectorIDs, idx.updates)
		source.publishErr = nil
		report, err = backfillScoped(t.Context(), source, vectors, options{Apply: true, Limit: 1, After: report.ResumeAfter})
		require.NoError(t, err)
		require.Equal(t, 1, report.Verified)
		require.Equal(t, []string{doc.DocumentID}, source.published)
	})
}

func FuzzHostedChunkBinding(f *testing.F) {
	id := uuid.New()
	for _, s := range []string{id.String(), strings.ReplaceAll(id.String(), "-", ""), "{" + id.String() + "}", "", "urn:uuid:" + id.String()} {
		f.Add(s)
	}
	compact := strings.ReplaceAll(id.String(), "-", "")
	f.Fuzz(func(t *testing.T, candidate string) {
		parsed, err := uuid.Parse(candidate)
		want := (len(candidate) == 32 || len(candidate) == 36) && err == nil && parsed == id
		require.Equal(t, want, matchesHostedChunkID(candidate, compact))
	})
}
