package llm

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"strings"

	"github.com/Victoroide/quarry-rag/internal/config"
)

// OpenAIClient synthesizes text using an OpenAI-compatible chat endpoint.
type OpenAIClient struct {
	http  *OpenAIHTTPClient
	model string
}

// NewOpenAIClient creates a chat client from explicit provider configuration.
func NewOpenAIClient(cfg *config.Config) (*OpenAIClient, error) {
	transport, err := NewOpenAIHTTPClient(cfg.OpenAIBaseURL, cfg.OpenAIAPIKey)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(cfg.OpenAIChatModel) == "" {
		return nil, errors.New("openai: chat model is required")
	}
	return &OpenAIClient{transport, cfg.OpenAIChatModel}, nil
}

type openAIMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}
type openAIRequest struct {
	Model         string               `json:"model"`
	Messages      []openAIMessage      `json:"messages"`
	Temperature   float32              `json:"temperature"`
	MaxTokens     int                  `json:"max_tokens"`
	Stream        bool                 `json:"stream,omitempty"`
	StreamOptions *openAIStreamOptions `json:"stream_options,omitempty"`
}
type openAIStreamOptions struct {
	IncludeUsage bool `json:"include_usage"`
}
type openAIResponse struct {
	Choices []struct {
		Index        int           `json:"index"`
		Message      openAIMessage `json:"message"`
		Delta        openAIMessage `json:"delta"`
		FinishReason string        `json:"finish_reason"`
	} `json:"choices"`
	Usage *struct {
		InputTokens  int32 `json:"prompt_tokens"`
		OutputTokens int32 `json:"completion_tokens"`
	} `json:"usage"`
	Error json.RawMessage `json:"error"`
}

func (c *OpenAIClient) request(r Request, stream bool) (openAIRequest, error) {
	request := openAIRequest{Model: r.Model, Temperature: r.Temperature, MaxTokens: r.MaxTokens, Stream: stream}
	if request.Model == "" {
		request.Model = c.model
	}
	if request.MaxTokens <= 0 {
		request.MaxTokens = DefaultMaxTokens
	}
	if stream {
		request.StreamOptions = &openAIStreamOptions{IncludeUsage: true}
	}
	if r.System != "" {
		request.Messages = append(request.Messages, openAIMessage{Role: "system", Content: r.System})
	}
	for _, message := range r.Messages {
		var text strings.Builder
		for _, block := range message.Content {
			if block.Qualifier != "" {
				return openAIRequest{}, errors.New("openai: Bedrock guardrail content is unsupported")
			}
			text.WriteString(block.Text)
		}
		request.Messages = append(request.Messages, openAIMessage{Role: message.Role, Content: text.String()})
	}
	return request, nil
}

func openAIStopReason(reason string) StopReason {
	switch reason {
	case "stop":
		return StopEndTurn
	case "length":
		return StopMaxTokens
	case "content_filter":
		return StopFiltered
	default:
		return StopReason(reason)
	}
}

func (r openAIResponse) usage() *Usage {
	if r.Usage == nil {
		return nil
	}
	return &Usage{InputTokens: r.Usage.InputTokens, OutputTokens: r.Usage.OutputTokens}
}

func (r openAIResponse) hasError() bool { return len(r.Error) > 0 && string(r.Error) != "null" }

// Generate returns one completion and its usage, without transport retries.
func (c *OpenAIClient) Generate(ctx context.Context, r Request) (*Response, error) {
	request, err := c.request(r, false)
	if err != nil {
		return nil, err
	}
	var response openAIResponse
	if err := c.http.Decode(ctx, "/chat/completions", request, &response); err != nil {
		return nil, err
	}
	if response.hasError() || len(response.Choices) != 1 || response.Choices[0].Index != 0 || response.Choices[0].FinishReason == "" {
		return nil, errors.New("openai: invalid completion response")
	}
	choice := response.Choices[0]
	return &Response{Text: choice.Message.Content, StopReason: openAIStopReason(choice.FinishReason), Usage: response.usage()}, nil
}

// Stream delivers SSE deltas synchronously until a finish reason and DONE marker.
// Usage is optional because compatible servers may omit it; absent usage stays nil.
func (c *OpenAIClient) Stream(ctx context.Context, r Request, delta func(string) error) (*Response, error) {
	if delta == nil {
		return nil, errors.New("openai: stream callback is required")
	}
	request, err := c.request(r, true)
	if err != nil {
		return nil, err
	}
	response, err := c.http.Post(ctx, "/chat/completions", request)
	if err != nil {
		return nil, err
	}
	defer func() { _ = response.Body.Close() }()
	media, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if err != nil || media != "text/event-stream" {
		return nil, errors.New("openai: expected event stream")
	}
	scanner := bufio.NewScanner(io.LimitReader(response.Body, maxOpenAIResponseBytes+1))
	scanner.Buffer(make([]byte, 4096), 1<<20)
	var text, event strings.Builder
	out := &Response{}
	done := false
	dispatch := func() error {
		if event.Len() == 0 {
			return nil
		}
		data := strings.TrimSuffix(event.String(), "\n")
		event.Reset()
		if data == "[DONE]" {
			if out.StopReason == "" {
				return errors.New("openai: incomplete response stream")
			}
			done = true
			return nil
		}
		var chunk openAIResponse
		if json.Unmarshal([]byte(data), &chunk) != nil {
			return errors.New("openai: invalid stream event")
		}
		if chunk.hasError() {
			return errors.New("openai: upstream stream error")
		}
		if chunk.Usage != nil {
			out.Usage = chunk.usage()
		}
		if len(chunk.Choices) > 1 {
			return errors.New("openai: invalid stream choices")
		}
		for _, choice := range chunk.Choices {
			if choice.Index != 0 || (out.StopReason != "" && (choice.Delta.Content != "" || choice.FinishReason != "")) {
				return errors.New("openai: invalid stream sequence")
			}
			if choice.Delta.Content != "" {
				if err := ctx.Err(); err != nil {
					return err
				}
				if err := delta(choice.Delta.Content); err != nil {
					return err
				}
				text.WriteString(choice.Delta.Content)
			}
			if choice.FinishReason != "" {
				out.StopReason = openAIStopReason(choice.FinishReason)
			}
		}
		return nil
	}
	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		line := scanner.Text()
		if line == "" {
			if err := dispatch(); err != nil {
				return nil, err
			}
			if done {
				out.Text = text.String()
				return out, nil
			}
		} else if value, ok := strings.CutPrefix(line, "data:"); ok {
			value = strings.TrimPrefix(value, " ")
			if event.Len()+len(value) > 1<<20 {
				return nil, errors.New("openai: stream event exceeds size limit")
			}
			event.WriteString(value)
			event.WriteByte('\n')
		}
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if scanner.Err() != nil {
		return nil, errors.New("openai: stream read failed")
	}
	return nil, errors.New("openai: incomplete response stream")
}
