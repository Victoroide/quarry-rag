// Package memory provides an in-memory domain.VectorStore implementation for
// GO_RAG_FAKE_MODE (offline QA). It does cosine-similarity search over stored
// vectors with namespace scoping and doc_type metadata filtering that mirrors
// the Pinecone driver, so the full ingest/query/delete HTTP API can be
// exercised locally without Pinecone. Not for production use.
package memory

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"maps"
	"math"
	"slices"
	"sort"
	"strings"
	"sync"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/google/uuid"
)

// FakeVectorStore is a thread-safe in-memory vector store. It implements
// domain.VectorStore (Upsert, Query, QueryAllNamespaces, Ping) plus
// the domain.VectorDeleter methods (DeleteAllVectorsInNamespace, DeleteByDocID)
// used by the delete handler.
type FakeVectorStore struct {
	mu sync.RWMutex
	// namespace -> vector key -> stored chunk (with embedding + metadata).
	ns         map[string]map[vectorKey]domain.Chunk
	dimensions int
	owner      string
	// deleting holds the documents whose local deletion is in progress.
	deleting map[string]struct{}
}

var _ domain.InMemoryIndex = (*FakeVectorStore)(nil)

type vectorKey struct {
	chunkID          domain.ChunkID
	documentID       domain.DocumentID
	revision         string
	attempt          string
	manifestRevision string
}

// NewFakeVectorStore creates an empty in-memory store.
func NewFakeVectorStore() *FakeVectorStore {
	return &FakeVectorStore{ns: make(map[string]map[vectorKey]domain.Chunk), owner: uuid.NewString(), deleting: make(map[string]struct{})}
}

// MemoryOwner identifies this index instance. A restarted process, or another
// replica, owns a different and empty index.
func (s *FakeVectorStore) MemoryOwner() string { return s.owner }

// LockDocument takes the single local deletion slot of a document. A caller
// that does not get it must not start a second mutation of the same index.
func (s *FakeVectorStore) LockDocument(documentID string) (func(), bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, busy := s.deleting[documentID]; busy {
		return nil, false
	}
	if s.deleting == nil {
		s.deleting = make(map[string]struct{})
	}
	s.deleting[documentID] = struct{}{}
	var once sync.Once
	return func() {
		once.Do(func() {
			s.mu.Lock()
			delete(s.deleting, documentID)
			s.mu.Unlock()
		})
	}, true
}

// Upsert inserts or replaces chunk vectors in the namespace.
func (s *FakeVectorStore) Upsert(ctx context.Context, namespace string, chunks []domain.Chunk) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	dimensions := s.dimensions
	for _, c := range chunks {
		if len(c.Embedding) == 0 {
			continue
		}
		vectorID := string(c.ID)
		if c.VectorID != "" {
			vectorID = c.VectorID
		}
		if len(vectorID) > 512 {
			return domain.RejectedVectorMutation(fmt.Errorf("memory: chunk ID exceeds 512 bytes"))
		}
		if err := validateMetadata(c); err != nil {
			return domain.RejectedVectorMutation(err)
		}
		if dimensions == 0 {
			dimensions = len(c.Embedding)
		}
		if len(c.Embedding) != dimensions {
			return domain.RejectedVectorMutation(fmt.Errorf("memory: embedding dimension %d does not match index dimension %d", len(c.Embedding), dimensions))
		}
	}
	s.dimensions = dimensions
	m, ok := s.ns[namespace]
	if !ok {
		m = make(map[vectorKey]domain.Chunk)
		s.ns[namespace] = m
	}
	stored := 0
	for _, c := range chunks {
		if len(c.Embedding) == 0 {
			continue
		}
		emb := make([]float32, len(c.Embedding))
		copy(emb, c.Embedding)
		cp := c
		cp.Embedding = emb
		cp.Metadata = cloneMetadata(c.Metadata)
		cp.SourceLineBoundaries = nil
		key := vectorKey{chunkID: c.ID, manifestRevision: c.RevisionID}
		if revision, _ := c.Metadata[domain.VectorRevisionKey].(string); revision != "" {
			key.documentID = c.DocumentID
			key.revision = revision
			key.attempt, _ = c.Metadata[domain.VectorAttemptKey].(string)
		} else if c.RevisionID != "" {
			key.documentID = c.DocumentID
		}
		m[key] = cp
		stored++
	}
	slog.DebugContext(ctx, "offline vectors upserted", "vectors", stored, "namespace", namespace, "namespaces", len(s.ns))
	return nil
}

func validateMetadata(c domain.Chunk) error {
	revision, tagged := c.Metadata[domain.VectorRevisionKey]
	if value, _ := revision.(string); tagged && value == "" {
		return fmt.Errorf("memory: invalid vector revision")
	}
	if attempt, present := c.Metadata[domain.VectorAttemptKey]; present {
		if value, _ := attempt.(string); !tagged || value == "" {
			return fmt.Errorf("memory: vector attempt requires a valid revision and attempt")
		}
	}

	for key, value := range c.Metadata {
		valid := true
		switch v := value.(type) {
		case string, bool, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64, json.Number:
		case []int:
			valid = v != nil
		case []string:
			valid = v != nil
		case []any:
			valid = v != nil
			for _, item := range v {
				if _, ok := item.(string); !ok {
					valid = false
					break
				}
			}
		default:
			valid = false
		}
		if !valid {
			return fmt.Errorf("memory: metadata %q must be a string, number, bool or list of strings", key)
		}
	}
	if _, err := json.Marshal(c.Metadata); err != nil {
		return fmt.Errorf("memory: serialize metadata: %w", err)
	}
	// Count the same required fields and optional labels as the hosted adapter.
	metadata := map[string]any{
		"doc_id": string(c.DocumentID), "chunk_index": float64(c.Index),
		"text": c.Text, "token_count": float64(c.TokenCount),
	}
	if c.VectorIncarnation != "" {
		metadata[domain.VectorIncarnationKey] = c.VectorIncarnation
	}
	if c.RevisionID != "" {
		metadata["revision_id"] = c.RevisionID
		metadata["chunk_id"] = string(c.ID)
	}
	for _, key := range []string{"doc_type", "kind", "quality", "collection", domain.VectorTenantKey, domain.VectorRevisionKey, domain.VectorAttemptKey, "pages", "table_ids", "row_ids"} {
		if value, ok := c.Metadata[key]; ok {
			metadata[key] = value
		}
	}
	encoded, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("memory: serialize vector metadata: %w", err)
	}
	if len(encoded) > 40960 {
		return fmt.Errorf("memory: vector metadata exceeds 40960 bytes (%d bytes)", len(encoded))
	}
	return nil
}

func cloneMetadata(metadata map[string]any) map[string]any {
	cloned := maps.Clone(metadata)
	for key, value := range cloned {
		switch list := value.(type) {
		case []int:
			cloned[key] = slices.Clone(list)
		case []string:
			cloned[key] = slices.Clone(list)
		case []any:
			cloned[key] = slices.Clone(list)
		}
	}
	return cloned
}

// Query searches a single namespace by cosine similarity, honoring docTypeFilter.
func (s *FakeVectorStore) Query(_ context.Context, namespace string, embedding []float32, topK int, docTypeFilter string) ([]domain.ScoredChunk, error) {
	return s.query(namespace, embedding, topK, domain.VectorFilter{DocType: docTypeFilter}), nil
}

// QueryAllNamespaces searches the default ("") namespace, mirroring the
// Pinecone driver's dual-write semantics where every vector also lives in "".
func (s *FakeVectorStore) QueryAllNamespaces(_ context.Context, embedding []float32, topK int, docTypeFilter string) ([]domain.ScoredChunk, error) {
	return s.query("", embedding, topK, domain.VectorFilter{DocType: docTypeFilter}), nil
}

// QueryFiltered applies document scope before selecting the highest scores.
func (s *FakeVectorStore) QueryFiltered(_ context.Context, embedding []float32, topK int, filter domain.VectorFilter) ([]domain.ScoredChunk, error) {
	if topK > 10000 {
		return nil, fmt.Errorf("memory: topK must not exceed 10000")
	}
	return s.query("", embedding, topK, filter), nil
}

func (s *FakeVectorStore) query(namespace string, embedding []float32, topK int, filter domain.VectorFilter) []domain.ScoredChunk {
	if filter.MatchesNothing() {
		return []domain.ScoredChunk{}
	}
	allowed := make(map[string]struct{}, len(filter.DocumentIDs))
	for _, id := range filter.DocumentIDs {
		allowed[id] = struct{}{}
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	m, ok := s.ns[namespace]
	if !ok {
		return []domain.ScoredChunk{}
	}
	scored := make([]domain.ScoredChunk, 0, len(m))
	for _, c := range m {
		if filter.DocumentIDs != nil {
			if _, ok := allowed[string(c.DocumentID)]; !ok {
				continue
			}
		}
		if filter.DocType != "" {
			if dt, _ := c.Metadata["doc_type"].(string); dt != filter.DocType {
				continue
			}
		}
		if filter.Collection != "" {
			if collection, _ := c.Metadata["collection"].(string); collection != filter.Collection {
				continue
			}
		}
		out := domain.Chunk{
			ID:                c.ID,
			DocumentID:        c.DocumentID,
			Index:             c.Index,
			Text:              c.Text,
			TokenCount:        c.TokenCount,
			Metadata:          cloneMetadata(c.Metadata),
			VectorIncarnation: c.VectorIncarnation,
			RevisionID:        c.RevisionID,
			VectorID:          c.VectorID,
		}
		scored = append(scored, domain.ScoredChunk{
			Chunk:     out,
			Score:     cosine(embedding, c.Embedding),
			Namespace: namespace,
		})
	}
	sort.Slice(scored, func(i, j int) bool {
		if scored[i].Score == scored[j].Score {
			return scored[i].Chunk.ID < scored[j].Chunk.ID
		}
		return scored[i].Score > scored[j].Score
	})
	if topK > 0 && len(scored) > topK {
		scored = scored[:topK]
	}
	return scored
}

// DeleteAllVectorsInNamespace drops an entire namespace (per-doc UUID namespace).
func (s *FakeVectorStore) DeleteAllVectorsInNamespace(_ context.Context, namespace string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.ns, namespace)
	return nil
}

// DeleteByDocID removes untagged vectors for docID across namespaces.
func (s *FakeVectorStore) DeleteByDocID(_ context.Context, docID string) error {
	return s.deleteDocumentVectors(docID, "", "")
}

// DeleteByRevision removes vectors for one document incarnation.
func (s *FakeVectorStore) DeleteByRevision(_ context.Context, docID, revision string) error {
	if revision == "" {
		return fmt.Errorf("memory: document revision is required")
	}
	return s.deleteDocumentVectors(docID, revision, "")
}

// DeleteByAttempt removes vectors belonging to one worker claim.
func (s *FakeVectorStore) DeleteByAttempt(_ context.Context, docID, revision, attempt string) error {
	if revision == "" || attempt == "" {
		return fmt.Errorf("memory: revision and attempt are required")
	}
	return s.deleteDocumentVectors(docID, revision, attempt)
}

func (s *FakeVectorStore) deleteDocumentVectors(docID, revision, attempt string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, m := range s.ns {
		for id, c := range m {
			if string(c.DocumentID) == docID && id.revision == revision && (attempt == "" || id.attempt == attempt) {
				delete(m, id)
			}
		}
	}
	return nil
}

func storedExternalID(key vectorKey, ch domain.Chunk) string {
	if ch.VectorID != "" {
		return ch.VectorID
	}
	return string(key.chunkID)
}

// DeleteByManifestRevision removes only vectors belonging to (docID, revisionID).
func (s *FakeVectorStore) DeleteByManifestRevision(_ context.Context, docID, revisionID string, knownIDs []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(knownIDs) > 0 {
		drop := make(map[string]struct{}, len(knownIDs))
		for _, id := range knownIDs {
			drop[id] = struct{}{}
		}
		if m := s.ns[""]; m != nil {
			for k, c := range m {
				if _, ok := drop[storedExternalID(k, c)]; ok {
					delete(m, k)
				}
			}
		}
		return nil
	}
	for _, m := range s.ns {
		for k, c := range m {
			if string(c.DocumentID) == docID && c.RevisionID == revisionID {
				delete(m, k)
			}
		}
	}
	return nil
}

// DeleteVectorIDs removes exact vector IDs from namespace.
func (s *FakeVectorStore) DeleteVectorIDs(_ context.Context, namespace string, ids []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	m := s.ns[namespace]
	if m == nil {
		return nil
	}
	drop := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		drop[id] = struct{}{}
	}
	for k, c := range m {
		if _, ok := drop[storedExternalID(k, c)]; ok {
			delete(m, k)
		}
	}
	return nil
}

// FetchPresentVectorIDs returns the subset of ids currently stored in namespace.
func (s *FakeVectorStore) FetchPresentVectorIDs(_ context.Context, namespace string, ids []string) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	m := s.ns[namespace]
	if m == nil {
		return nil, nil
	}
	presentSet := make(map[string]struct{}, len(m))
	for k, c := range m {
		presentSet[storedExternalID(k, c)] = struct{}{}
	}
	var present []string
	for _, id := range ids {
		if _, ok := presentSet[id]; ok {
			present = append(present, id)
		}
	}
	return present, nil
}

// ListPrefixVectorIDs lists stored vector IDs matching prefix in namespace.
func (s *FakeVectorStore) ListPrefixVectorIDs(_ context.Context, namespace, prefix string, limit int, _ string) (ids []string, nextToken string, supported bool, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	m := s.ns[namespace]
	if m == nil {
		return nil, "", true, nil
	}
	var out []string
	for k, c := range m {
		id := storedExternalID(k, c)
		if strings.HasPrefix(id, prefix) {
			out = append(out, id)
		}
	}
	sort.Strings(out)
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, "", true, nil
}

// Ping always succeeds for the in-memory store.
func (s *FakeVectorStore) Ping(_ context.Context) error { return nil }

func cosine(a, b []float32) float64 {
	if len(a) == 0 || len(b) == 0 || len(a) != len(b) {
		return 0
	}
	var dot, na, nb float64
	for i := range a {
		dot += float64(a[i]) * float64(b[i])
		na += float64(a[i]) * float64(a[i])
		nb += float64(b[i]) * float64(b[i])
	}
	if na == 0 || nb == 0 {
		return 0
	}
	return dot / (math.Sqrt(na) * math.Sqrt(nb))
}
