package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type queryVectors struct {
	TextHash string    `json:"text_sha256"`
	Source   []float32 `json:"source"`
	Target   []float32 `json:"target"`
}

type queryFixtures struct {
	SourceProfile string                  `json:"source_profile_hash"`
	TargetProfile string                  `json:"target_profile_hash"`
	Queries       map[string]queryVectors `json:"queries"`
}

func (v *verifier) queryVectors(ctx context.Context, sample []sampleChunk, rep *verifyReport) (map[string]queryVectors, error) {
	ids := make([]string, len(sample))
	sourceKeys := make([]string, len(sample))
	for i, c := range sample {
		ids[i] = c.id
		sourceKeys[i] = c.sourceKey
		if sourceKeys[i] == "" {
			sourceKeys[i] = c.id
		}
	}
	if !v.sourceQueryOnly && v.sourceProfile.SameEmbeddingSpace(v.profile) {
		if v.opts.QueryFixtures != "" {
			return nil, errors.New("-query-fixtures is only needed for different embedding spaces")
		}
		source, err := fetchAll(ctx, v.source, sourceKeys)
		if err != nil {
			return nil, err
		}
		queries := make(map[string]queryVectors, len(source))
		for i, c := range sample {
			chunk, ok := source[sourceKeys[i]]
			if !ok || chunk.Index < 0 || chunk.Index != c.index || string(chunk.DocumentID) != c.doc || sha256.Sum256([]byte(chunk.Text)) != c.textHash {
				continue
			}
			queries[c.id] = queryVectors{TextHash: fmt.Sprintf("%x", c.textHash), Source: chunk.Embedding, Target: chunk.Embedding}
		}
		return queries, nil
	}

	rep.Sample.SourceProfile = v.sourceProfile.Hash()
	rep.Sample.ChunkIDs = ids
	if v.opts.QueryFixtures == "" {
		v.fail(rep, "independent -query-fixtures are required for the sampled chunks")
		return nil, nil
	}
	fixtures, err := readQueryFixtures(v.opts.QueryFixtures, v.opts.MaxBytes)
	if err != nil {
		return nil, fmt.Errorf("read query fixtures: %w", err)
	}
	if fixtures.SourceProfile != rep.Sample.SourceProfile || fixtures.TargetProfile != v.profile.Hash() {
		return nil, errors.New("the query fixture profiles differ from the source query and recorded target profiles")
	}
	return fixtures.Queries, nil
}

func readQueryFixtures(path string, maxBytes int64) (queryFixtures, error) {
	var fixtures queryFixtures
	cleanPath := filepath.Clean(path)
	info, err := os.Stat(cleanPath)
	if err != nil {
		return fixtures, err
	}
	if !info.Mode().IsRegular() || info.Size() > maxBytes {
		return fixtures, errors.New("query fixtures must be a regular file within -max-bytes")
	}
	file, err := os.Open(cleanPath)
	if err != nil {
		return fixtures, err
	}
	defer func() { _ = file.Close() }()
	limited := &io.LimitedReader{R: file, N: maxBytes}
	decoder := json.NewDecoder(limited)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&fixtures); err != nil {
		return fixtures, err
	}
	var extra json.RawMessage
	if err := decoder.Decode(&extra); err != io.EOF {
		return fixtures, errors.New("expected a single query fixture object")
	}
	if limited.N == 0 {
		var extraByte [1]byte
		if _, err := file.Read(extraByte[:]); err != io.EOF {
			return fixtures, errors.New("query fixtures exceed -max-bytes")
		}
	}
	return fixtures, nil
}
