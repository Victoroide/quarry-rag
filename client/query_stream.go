package client

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strings"
)

// Event contains an SSE event name and its JSON payload.
// Type is context, delta, done or error. Data remains valid after the callback.
type Event struct {
	Type string
	Data json.RawMessage
}

// QueryStreamContext is the payload of a context event.
type QueryStreamContext struct {
	Mode          string                `json:"mode"`
	ChunksUsed    int                   `json:"chunks_used"`
	ContextChunks int                   `json:"context_chunks"`
	ScoredChunks  []ScoredChunkResponse `json:"scored_chunks"`
}

// QueryStreamDone is the payload of a done event.
type QueryStreamDone struct {
	FinishReason     string             `json:"finish_reason"`
	TokenUsage       TokenUsage         `json:"token_usage"`
	ContextTokens    int                `json:"context_tokens"`
	CountMethod      string             `json:"count_method,omitempty"`
	ModelInputTokens int                `json:"model_input_tokens,omitempty"`
	StageTimings     map[string]int64   `json:"stage_timings_ms"`
	EarlyExit        bool               `json:"early_exit,omitempty"`
	Citations        []CitationResponse `json:"citations,omitzero"`
	CitationCoverage *CitationCoverage  `json:"citation_coverage,omitempty"`
}

// StreamError reports a query failure after the response stream has started.
type StreamError struct {
	Code    string
	Message string

	// RequestID is the X-Request-ID response header, when present.
	RequestID string
}

// Error returns the service error category and message.
func (e *StreamError) Error() string { return e.Code + ": " + e.Message }

// QueryStream delivers query events to fn. A valid done event completes the
// stream if fn returns nil. A non-2xx HTTP response returns an [*APIError].
// A terminal error event returns a [*StreamError] if fn returns nil.
// EOF before either terminal event returns an error wrapping [io.ErrUnexpectedEOF].
// QueryStream returns any callback error, cancels the HTTP request before
// returning, and makes a single attempt.
func (c *Client) QueryStream(ctx context.Context, input QueryRequest, fn func(Event) error, opts ...RequestOption) error {
	if fn == nil {
		return errors.New("stream callback is required")
	}
	data, err := json.Marshal(input)
	if err != nil {
		return fmt.Errorf("encode request: %w", err)
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	req, credentials, err := c.newRequest(ctx, http.MethodPost, "/query", bytes.NewReader(data), "application/json", "text/event-stream", opts...)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "text/event-stream")
	resp, err := c.perform(req, credentials)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return decodeAPIError(resp, credentials)
	}
	contentType, _, err := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if err != nil || contentType != "text/event-stream" {
		return errors.New("unexpected stream Content-Type")
	}
	err = readQueryEvents(ctx, resp.Body, fn, credentials)
	var streamErr *StreamError
	if errors.As(err, &streamErr) {
		streamErr.RequestID = credentials.redact(resp.Header.Get("X-Request-ID"))
	}
	return err
}

const maxQueryEventBytes = 8 << 20

func readQueryEvents(ctx context.Context, body io.Reader, fn func(Event) error, credentials requestCredentials) error {
	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 64<<10), maxQueryEventBytes)
	name := ""
	var data strings.Builder
	contextSeen := false
	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return err
		}
		line := scanner.Text()
		if line == "" {
			if data.Len() > 0 {
				event := Event{Type: name, Data: json.RawMessage(strings.TrimSuffix(data.String(), "\n"))}
				done, err := dispatchQueryEvent(event, &contextSeen, fn, credentials)
				if err != nil || done {
					return err
				}
			}
			name = ""
			data.Reset()
			continue
		}
		if strings.HasPrefix(line, ":") {
			continue
		}
		field, value, _ := strings.Cut(line, ":")
		value = strings.TrimPrefix(value, " ")
		switch field {
		case "event":
			name = value
		case "data":
			if data.Len()+len(value)+1 > maxQueryEventBytes {
				return errors.New("query event exceeds size limit")
			}
			data.WriteString(value)
			data.WriteByte('\n')
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("read query stream: %w", credentials.safeError(err))
	}
	return fmt.Errorf("read query stream: %w", io.ErrUnexpectedEOF)
}

func dispatchQueryEvent(event Event, contextSeen *bool, fn func(Event) error, credentials requestCredentials) (bool, error) {
	switch event.Type {
	case "context", "delta", "done", "error":
	default:
		return false, nil
	}
	var payload map[string]json.RawMessage
	if err := json.Unmarshal(event.Data, &payload); err != nil || payload == nil {
		return false, errors.New("invalid query event JSON")
	}
	if event.Type == "context" {
		if *contextSeen {
			return false, errors.New("duplicate query context event")
		}
		*contextSeen = true
	} else if !*contextSeen {
		return false, errors.New("query context event is missing")
	}
	if event.Type == "delta" {
		var text string
		if raw, ok := payload["text"]; !ok || string(raw) == "null" || json.Unmarshal(raw, &text) != nil {
			return false, errors.New("invalid query delta event")
		}
	}
	var streamErr *StreamError
	if event.Type == "error" {
		var body ErrorResponse
		if err := json.Unmarshal(event.Data, &body); err != nil || body.Error == "" || body.Message == "" {
			return false, errors.New("invalid query error event")
		}
		streamErr = &StreamError{Code: credentials.redact(body.Error), Message: credentials.redact(body.Message)}
		if streamErr.Code != body.Error || streamErr.Message != body.Message || credentials.redact(string(event.Data)) != string(event.Data) {
			data, err := json.Marshal(ErrorResponse{Error: streamErr.Code, Message: streamErr.Message})
			if err != nil {
				return false, errors.New("invalid query error event")
			}
			event.Data = data
		}
	}
	if err := fn(event); err != nil {
		return false, err
	}
	if streamErr != nil {
		return false, streamErr
	}
	return event.Type == "done", nil
}
