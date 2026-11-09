package llm

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
)

func openAITestClient(t *testing.T, handler http.HandlerFunc, key string) *OpenAIClient {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	client, err := NewOpenAIClient(&config.Config{OpenAIBaseURL: server.URL + "/v1/", OpenAIAPIKey: key, OpenAIChatModel: "chat"})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func chatRequest() Request {
	return Request{Model: "chat", System: "instructions", Messages: []Message{{Role: "user", Content: []Content{{Text: "question"}}}}, Temperature: .2, MaxTokens: 100}
}

func TestOpenAIChat(t *testing.T) {
	for _, key := range []string{"", "synthetic-key"} {
		t.Run(fmt.Sprint(key != ""), func(t *testing.T) {
			client := openAITestClient(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPost || r.URL.Path != "/v1/chat/completions" || r.Header.Get("Content-Type") != "application/json" {
					t.Error("incorrect request")
				}
				wantAuth := ""
				if key != "" {
					wantAuth = "Bearer " + key
				}
				if r.Header.Get("Authorization") != wantAuth {
					t.Error("incorrect authorization")
				}
				var body struct {
					Model       string
					Messages    []struct{ Role, Content string }
					Temperature float32
					MaxTokens   int `json:"max_tokens"`
					Stream      bool
				}
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Error(err)
				}
				if body.Model != "chat" || len(body.Messages) != 2 || body.Messages[0].Role != "system" || body.Messages[0].Content != "instructions" || body.Messages[1].Content != "question" || body.MaxTokens != 100 || body.Temperature != .2 || body.Stream {
					t.Errorf("incorrect body: %+v", body)
				}
				_, _ = fmt.Fprint(w, `{"choices":[{"index":0,"message":{"content":"answer"},"finish_reason":"stop"}],"usage":{"prompt_tokens":9,"completion_tokens":2}}`)
			}, key)
			out, err := client.Generate(context.Background(), chatRequest())
			if err != nil {
				t.Fatal(err)
			}
			if out.Text != "answer" || out.StopReason != StopEndTurn || out.Usage == nil || out.Usage.InputTokens != 9 || out.Usage.OutputTokens != 2 {
				t.Fatalf("unexpected result: %+v", out)
			}
		})
	}
}

func TestOpenAIStream(t *testing.T) {
	client := openAITestClient(t, func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Stream  bool
			Options struct {
				IncludeUsage bool `json:"include_usage"`
			} `json:"stream_options"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		if !body.Stream || !body.Options.IncludeUsage {
			t.Error("streaming and usage must be requested")
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = fmt.Fprint(w, ": heartbeat\r\ndata: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"ans\"}}]}\r\n\r\n")
		w.(http.Flusher).Flush()
		_, _ = fmt.Fprint(w, "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"wer\"},\"finish_reason\":\"length\"}]}\n\ndata: {\"choices\":[],\"usage\":{\"prompt_tokens\":9,\"completion_tokens\":2}}\n\ndata: [DONE]\n\n")
	}, "")
	var deltas []string
	out, err := client.Stream(context.Background(), chatRequest(), func(s string) error { deltas = append(deltas, s); return nil })
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(deltas, []string{"ans", "wer"}) || out.Text != "answer" || out.StopReason != StopMaxTokens || out.Usage == nil || out.Usage.OutputTokens != 2 {
		t.Fatalf("unexpected stream: %+v, %+v", out, deltas)
	}
}

func TestOpenAIStreamFailures(t *testing.T) {
	for _, tc := range []struct {
		name, events string
		callback     bool
		want         string
	}{
		{"truncated", "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"partial\"}}]}\n\n", false, "incomplete"},
		{"no finish", "data: [DONE]\n\n", false, "incomplete"},
		{"malformed", "data: private-invalid-json\n\n", false, "invalid"},
		{"upstream error", "data: {\"error\":{\"message\":\"private\"}}\n\n", false, "error"},
		{"callback", "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"partial\"}}]}\n\n", true, "callback"},
		{"content after finish", "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"answer\"},\"finish_reason\":\"stop\"}]}\n\ndata: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"extra\"}}]}\n\ndata: [DONE]\n\n", false, "invalid stream sequence"},
		{"second finish", "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"answer\"},\"finish_reason\":\"stop\"}]}\n\ndata: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n", false, "invalid stream sequence"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := openAITestClient(t, func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = fmt.Fprint(w, tc.events)
			}, "")
			callbackErr := errors.New("callback")
			_, err := client.Stream(context.Background(), chatRequest(), func(string) error {
				if tc.callback {
					return callbackErr
				}
				return nil
			})
			if err == nil || !strings.Contains(err.Error(), tc.want) || strings.Contains(err.Error(), "private") {
				t.Fatalf("unexpected error: %v", err)
			}
			if tc.callback && !errors.Is(err, callbackErr) {
				t.Fatal("callback identity lost")
			}
		})
	}
}

func TestOpenAIStreamAnnotationAfterFinish(t *testing.T) {
	client := openAITestClient(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = fmt.Fprint(w, "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"answer\"},\"finish_reason\":\"stop\"}]}\n\ndata: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":null}]}\n\ndata: [DONE]\n\n")
	}, "")
	var deltas []string
	out, err := client.Stream(t.Context(), chatRequest(), func(s string) error { deltas = append(deltas, s); return nil })
	if err != nil {
		t.Fatal(err)
	}
	if out.Text != "answer" || out.StopReason != StopEndTurn || !reflect.DeepEqual(deltas, []string{"answer"}) {
		t.Fatalf("unexpected annotated stream: %+v, %+v", out, deltas)
	}
}

func TestOpenAIHTTPFailures(t *testing.T) {
	for _, code := range []int{429, 503, 401, 500} {
		t.Run(fmt.Sprint(code), func(t *testing.T) {
			client := openAITestClient(t, func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(code)
				_, _ = fmt.Fprint(w, "private response with credentials")
			}, "synthetic-key")
			for _, stream := range []bool{false, true} {
				var err error
				if stream {
					_, err = client.Stream(context.Background(), chatRequest(), func(string) error { return nil })
				} else {
					_, err = client.Generate(context.Background(), chatRequest())
				}
				if err == nil || strings.Contains(err.Error(), "private") || strings.Contains(err.Error(), "synthetic-key") {
					t.Fatalf("unsafe error: %v", err)
				}
				if errors.Is(err, domain.ErrUpstreamThrottled) != (code == 429 || code == 503) {
					t.Fatalf("incorrect throttle mapping: %v", err)
				}
			}
		})
	}
}

func TestOpenAIStreamCancellation(t *testing.T) {
	client := openAITestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}, "")
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err := client.Stream(ctx, chatRequest(), func(string) error { return nil })
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline, got %v", err)
	}
}

func TestOpenAIModelRouting(t *testing.T) {
	cfg := &config.Config{LLMProvider: "openai", OpenAIChatModel: "chat", SynthesisModel: "hybrid", BedrockPrimaryModel: "primary", BedrockFallbackModel: "fallback"}
	for _, tokens := range []int{10, NovaMicroSafeContextLimit} {
		model, _ := Route(cfg, RoutingRequest{ContextTokens: tokens, MultiNeed: true})
		if model != "chat" {
			t.Fatalf("wrong provider model: %s", model)
		}
	}
}

func TestOpenAICompatibleStreamWithoutUsage(t *testing.T) {
	client := openAITestClient(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
		_, _ = fmt.Fprint(w, "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"answer\"},\"finish_reason\":\"content_filter\"}]}\n\ndata: [DONE]\n\n")
	}, "")
	out, err := client.Stream(t.Context(), chatRequest(), func(string) error { return nil })
	if err != nil || out.Usage != nil || out.StopReason != StopFiltered {
		t.Fatalf("unexpected optional usage result: %+v %v", out, err)
	}
}

func TestOpenAIDoesNotFollowRedirects(t *testing.T) {
	received := false
	destination := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { received = true }))
	defer destination.Close()
	client := openAITestClient(t, func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, destination.URL, http.StatusTemporaryRedirect)
	}, "synthetic-key")
	_, err := client.Generate(t.Context(), chatRequest())
	if err == nil || received {
		t.Fatalf("redirect forwarded credentials: received=%v error=%v", received, err)
	}
}

func TestOpenAIMalformedCompletions(t *testing.T) {
	for _, body := range []string{`private-invalid-json`, `{"error":{"message":"private"}}`, `{"choices":[]}`, `{"choices":[{"index":0,"message":{"content":"private"}}]}`, `{"choices":[{"index":1,"message":{"content":"private"},"finish_reason":"stop"}]}`} {
		client := openAITestClient(t, func(w http.ResponseWriter, _ *http.Request) { _, _ = fmt.Fprint(w, body) }, "")
		_, err := client.Generate(t.Context(), chatRequest())
		if err == nil || strings.Contains(err.Error(), "private") {
			t.Fatalf("invalid or unsafe error: %v", err)
		}
	}
}

func TestOpenAIStreamDeliversBeforeCompletion(t *testing.T) {
	delivered := make(chan struct{})
	client := openAITestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = fmt.Fprint(w, "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"first\"}}]}\n\n")
		w.(http.Flusher).Flush()
		select {
		case <-delivered:
		case <-r.Context().Done():
			return
		}
		_, _ = fmt.Fprint(w, "data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n")
	}, "")
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	out, err := client.Stream(ctx, chatRequest(), func(string) error { close(delivered); return nil })
	if err != nil || out.Text != "first" {
		t.Fatalf("stream did not deliver incrementally: %+v %v", out, err)
	}
}

func TestOpenAIStreamCallbackClosesBody(t *testing.T) {
	closed := make(chan struct{})
	client := openAITestClient(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = fmt.Fprint(w, "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"first\"}}]}\n\n")
		w.(http.Flusher).Flush()
		<-r.Context().Done()
		close(closed)
	}, "")
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	callbackErr := errors.New("stop consuming")
	_, err := client.Stream(ctx, chatRequest(), func(string) error { return callbackErr })
	if !errors.Is(err, callbackErr) {
		t.Fatalf("callback error lost: %v", err)
	}
	select {
	case <-closed:
	case <-ctx.Done():
		t.Fatal("upstream body was not closed after callback error")
	}
}
