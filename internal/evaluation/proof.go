package evaluation

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/Victoroide/quarry-rag/internal/domain"
)

// FixtureHeader and MatchesHeader carry the request namespace and response observation.
const (
	FixtureHeader   = "X-Quarry-Evaluation-Fixture"
	MatchesHeader   = "X-Quarry-Evaluation-Evidence"
	ObserveHeader   = "X-Quarry-Evaluation-Observe"
	ExecutionHeader = "X-Quarry-Evaluation-Execution"
)

// ProviderExecution identifies the configured real query provider combination.
func ProviderExecution(backend, embedding, answer string) string {
	if (backend != "postgres" && backend != "pinecone") ||
		(embedding != "bedrock" && embedding != "openai") ||
		(answer != "bedrock" && answer != "openai") {
		return ""
	}
	return backend + "," + embedding + "," + answer
}

// Match identifies the first retained excerpt containing a labeled span.
type Match struct {
	Span    string `json:"span"`
	Excerpt int    `json:"excerpt"`
	Runes   int    `json:"runes"`
}

// OwnsScope accepts only an explicit allowlist of fixture UUIDs in the primary collection.
func OwnsScope(ns Namespace, collection string, ids []string) bool {
	if ids == nil || collection != ns.Collection(false) {
		return false
	}
	docs := Documents()
	for _, id := range ids {
		if !slices.ContainsFunc(docs, func(d Document) bool { return !d.Distractor && ns.DocumentID(d.Key) == id }) {
			return false
		}
	}
	return true
}

// MatchExcerpts finds labeled spans in the final retained bodies. It records the
// first matching excerpt for each span and returns no content.
func MatchExcerpts(ns Namespace, ids []string, excerpts []domain.ScoredChunk) []Match {
	matches := make([]Match, 0)
	for _, doc := range Documents() {
		if doc.Distractor {
			continue
		}
		id := ns.DocumentID(doc.Key)
		if !slices.Contains(ids, id) {
			continue
		}
		for _, span := range doc.Spans {
			for i, chunk := range excerpts {
				if string(chunk.Chunk.DocumentID) == id && strings.Contains(chunk.Chunk.Text, span.Text) {
					matches = append(matches, Match{Span: span.ID, Excerpt: i + 1, Runes: utf8.RuneCountInString(chunk.Chunk.Text)})
					break
				}
			}
		}
	}
	return matches
}

// EncodeMatches encodes a JSON observation as a URL-safe header value.
func EncodeMatches(matches []Match) (string, error) {
	data, err := json.Marshal(matches)
	return base64.RawURLEncoding.EncodeToString(data), err
}

// DecodeMatches checks the size, shape and span IDs of a received observation.
func DecodeMatches(value string) ([]Match, error) {
	if value == "" || len(value) > 4096 {
		return nil, errors.New("missing or oversized excerpt observation")
	}
	data, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return nil, err
	}
	var matches []Match
	if !json.Valid(data) {
		return nil, errors.New("invalid excerpt observation JSON")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&matches); err != nil {
		return nil, err
	}
	if matches == nil {
		return nil, errors.New("null excerpt observation")
	}
	docs := Documents()
	seen := make(map[string]bool)
	for _, match := range matches {
		known := slices.ContainsFunc(docs, func(doc Document) bool {
			return slices.ContainsFunc(doc.Spans, func(span Span) bool {
				return span.ID == match.Span && match.Runes >= utf8.RuneCountInString(span.Text)
			})
		})
		if !known || seen[match.Span] || match.Excerpt < 1 || match.Excerpt > 100 {
			return nil, errors.New("invalid excerpt observation")
		}
		seen[match.Span] = true
	}
	return matches, nil
}
