package postgres

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"unicode/utf8"
)

const (
	recallDimensions = 768
	recallLineBytes  = 128 << 10
	recallCorpusRows = 200_000
	recallQueryRows  = 32
)

type recallProfile struct {
	Version    int    `json:"version"`
	Source     string `json:"source"`
	Revision   string `json:"source_revision"`
	Encoder    string `json:"encoder"`
	Dimensions int    `json:"dimensions"`
}

type recallInput struct {
	Profile recallProfile `json:"profile"`
	Digest  string        `json:"sha256"`
	keys    map[[32]byte]struct{}
}

type recallInputRow func(ordinal int, key [32]byte, embedding []float32) error

func readRecallFile(ctx context.Context, path string, count int, accept recallInputRow) (recallInput, error) {
	if err := ctx.Err(); err != nil {
		return recallInput{}, err
	}
	dir, name := filepath.Split(path)
	if !filepath.IsLocal(name) || name == "." {
		return recallInput{}, errors.New("invalid recall input filename")
	}
	root, err := os.OpenRoot(filepath.Clean(dir))
	if err != nil {
		return recallInput{}, errors.New("cannot open recall input directory")
	}
	defer func() { _ = root.Close() }()
	info, err := root.Stat(name)
	if err != nil || !info.Mode().IsRegular() {
		return recallInput{}, errors.New("recall input must be a readable regular file")
	}
	file, err := root.Open(name)
	if err != nil {
		return recallInput{}, errors.New("cannot open recall input")
	}
	defer func() { _ = file.Close() }()
	info, err = file.Stat()
	if err != nil || !info.Mode().IsRegular() {
		return recallInput{}, errors.New("recall input must be a readable regular file")
	}
	return readRecallInput(ctx, file, count, accept)
}

func readRecallInput(ctx context.Context, input io.Reader, count int, accept recallInputRow) (recallInput, error) {
	if count < 1 || count > recallCorpusRows {
		return recallInput{}, errors.New("invalid recall input row count")
	}
	digest := sha256.New()
	reader := bufio.NewReaderSize(io.TeeReader(input, digest), recallLineBytes+2)
	read := func() ([]byte, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		line, err := reader.ReadSlice('\n')
		if errors.Is(err, bufio.ErrBufferFull) {
			return nil, errors.New("recall input line exceeds 128 KiB")
		}
		if err != nil && (!errors.Is(err, io.EOF) || len(line) == 0) {
			return nil, err
		}
		if len(line) > 0 && line[len(line)-1] == '\n' {
			line = bytes.TrimSuffix(line[:len(line)-1], []byte{'\r'})
		}
		if len(line) > recallLineBytes {
			return nil, errors.New("recall input line exceeds 128 KiB")
		}
		return line, nil
	}
	header, err := read()
	if err != nil {
		return recallInput{}, fmt.Errorf("recall input header: %w", err)
	}
	fields, err := recallObject(header, "version", "source", "source_revision", "encoder", "dimensions")
	if err != nil {
		return recallInput{}, fmt.Errorf("recall input header: %w", err)
	}
	var profile recallProfile
	if err := json.Unmarshal(header, &profile); err != nil || len(fields) != 5 ||
		profile.Version != 1 || profile.Dimensions != recallDimensions ||
		profile.Source == "" || profile.Revision == "" || profile.Encoder == "" {
		return recallInput{}, errors.New("invalid recall input profile")
	}
	seen := make(map[[32]byte]struct{}, count)
	for ordinal := 0; ordinal < count; ordinal++ {
		line, err := read()
		if err != nil {
			return recallInput{}, fmt.Errorf("recall input row %d: %w", ordinal+1, err)
		}
		key, vec, err := recallRecord(line)
		if err != nil {
			return recallInput{}, fmt.Errorf("recall input row %d: %w", ordinal+1, err)
		}
		keyDigest := sha256.Sum256([]byte(key))
		if _, duplicate := seen[keyDigest]; duplicate {
			return recallInput{}, fmt.Errorf("recall input row %d has a duplicate key", ordinal+1)
		}
		seen[keyDigest] = struct{}{}
		if accept != nil {
			if err := accept(ordinal, keyDigest, vec); err != nil {
				return recallInput{}, fmt.Errorf("accept recall input row %d: %w", ordinal+1, err)
			}
		}
	}
	if _, err := read(); !errors.Is(err, io.EOF) {
		if err == nil {
			err = errors.New("too many recall input rows")
		}
		return recallInput{}, err
	}
	return recallInput{Profile: profile, Digest: hex.EncodeToString(digest.Sum(nil)), keys: seen}, nil
}

func matchRecallInputs(corpus, queries recallInput) error {
	if corpus.Profile != queries.Profile {
		return errors.New("recall input profiles differ")
	}
	for key := range queries.keys {
		if _, duplicate := corpus.keys[key]; duplicate {
			return errors.New("recall corpus and held-out query keys overlap")
		}
	}
	return nil
}

func recallRecord(line []byte) (key string, embedding []float32, err error) {
	fields, err := recallObject(line, "key", "embedding")
	if err != nil {
		return "", nil, err
	}
	if decodeErr := json.Unmarshal(fields["key"], &key); decodeErr != nil || key == "" || len(key) > 256 {
		return "", nil, errors.New("invalid recall input key")
	}
	var values []json.RawMessage
	if decodeErr := json.Unmarshal(fields["embedding"], &values); decodeErr != nil || len(values) != recallDimensions {
		return "", nil, errors.New("invalid recall embedding dimensions or components")
	}
	embedding = make([]float32, recallDimensions)
	var squaredNorm float64
	for i, value := range values {
		n, parseErr := strconv.ParseFloat(string(value), 32)
		if parseErr != nil || math.IsNaN(n) || math.IsInf(n, 0) {
			return "", nil, errors.New("invalid recall embedding component")
		}
		embedding[i] = float32(n)
		squaredNorm += n * n
	}
	// pgvector sums squares and products in float32. Require at least one
	// normal square and leave rounding headroom for norms and dot products.
	const (
		minSquaredNorm = recallDimensions * 0x1p-126
		maxSquaredNorm = math.MaxFloat32 / 4
	)
	if squaredNorm < minSquaredNorm || squaredNorm > maxSquaredNorm {
		return "", nil, errors.New("recall embedding squared norm is outside the safe pgvector cosine range")
	}
	return key, embedding, nil
}

func recallObject(line []byte, keys ...string) (map[string]json.RawMessage, error) {
	if !utf8.Valid(line) || !recallUnicodeEscapes(line) {
		return nil, errors.New("invalid recall input Unicode")
	}
	decoder := json.NewDecoder(bytes.NewReader(line))
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return nil, errors.New("recall input line must be a JSON object")
	}
	fields := make(map[string]json.RawMessage, len(keys))
	for decoder.More() {
		field, tokenErr := decoder.Token()
		key, ok := field.(string)
		if tokenErr != nil || !ok || !recallField(keys, key) || fields[key] != nil {
			return nil, errors.New("unknown or repeated recall input field")
		}
		var raw json.RawMessage
		if decodeErr := decoder.Decode(&raw); decodeErr != nil {
			return nil, errors.New("malformed recall input field")
		}
		fields[key] = raw
	}
	token, err = decoder.Token()
	if err != nil || token != json.Delim('}') || len(fields) != len(keys) {
		return nil, errors.New("incomplete recall input object")
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return nil, errors.New("extra data after recall input object")
	}
	return fields, nil
}

func recallField(keys []string, key string) bool {
	for _, expected := range keys {
		if key == expected {
			return true
		}
	}
	return false
}

// encoding/json substitutes U+FFFD for lone surrogates. Reject those spellings
// before decoding so different source IDs cannot silently become the same key.
func recallUnicodeEscapes(line []byte) bool {
	for i := 0; i+1 < len(line); i++ {
		if line[i] != '\\' {
			continue
		}
		i++
		if line[i] != 'u' || i+4 >= len(line) {
			continue // The JSON decoder diagnoses malformed escapes.
		}
		value, _ := strconv.ParseUint(string(line[i+1:i+5]), 16, 16)
		i += 4
		switch {
		case value >= 0xdc00 && value <= 0xdfff:
			return false
		case value >= 0xd800 && value <= 0xdbff:
			if i+6 >= len(line) || line[i+1] != '\\' || line[i+2] != 'u' {
				return false
			}
			next, err := strconv.ParseUint(string(line[i+3:i+7]), 16, 16)
			if err != nil || next < 0xdc00 || next > 0xdfff {
				return false
			}
			i += 6
		}
	}
	return true
}
