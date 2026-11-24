package llm

import "testing"

func FuzzCountOpenAIChatTokens(f *testing.F) {
	f.Add("gpt-4o-2024-08-06", "Answer using only excerpts.", "<question>Hello?</question>\n<excerpt-0123456789abcdef id=\"1\">\nTest\n</excerpt-0123456789abcdef>")
	f.Add("gpt-4-0613", "", "")
	f.Add("gpt-4o-2024-08-06", "日本語システム", "Café\t\n<|endoftext|> 日本語テスト 🚀")
	f.Add("gpt-4-0613", "Cite [1].", string([]byte{0xff, 0xfe, 'a', 'b'}))

	f.Fuzz(func(t *testing.T, model, system, user string) {
		if len(system)+len(user) > 16<<10 {
			t.Skip()
		}
		if _, ok := LookupModelProfile("openai", model); !ok {
			model = "gpt-4o-2024-08-06"
		}
		req := Request{
			Model:    model,
			System:   system,
			Messages: []Message{{Role: "user", Content: []Content{{Text: user}}}},
		}
		first, method, err := CountOpenAIChatTokens(model, req)
		if err != nil {
			t.Fatalf("unexpected count error: %v", err)
		}
		if first <= 0 {
			t.Fatalf("non-positive chat token count: %d", first)
		}
		second, secondMethod, err := CountOpenAIChatTokens(model, req)
		if err != nil {
			t.Fatalf("unexpected repeat count error: %v", err)
		}
		if first != second || method != secondMethod {
			t.Fatalf("non-deterministic count: (%d,%s) vs (%d,%s)", first, method, second, secondMethod)
		}
	})
}
