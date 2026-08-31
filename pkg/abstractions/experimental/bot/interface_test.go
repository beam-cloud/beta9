package bot

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	openai "github.com/sashabaranov/go-openai"
)

func TestNewBotLLMClientOpenAIProvider(t *testing.T) {
	client := newBotLLMClient(BotConfig{
		Provider: "openai",
		ApiKey:   "sk-test",
		Model:    "gpt-4o",
	})

	if client == nil {
		t.Fatal("expected non-nil client for openai provider")
	}
}

func TestNewBotLLMClientOrcaRouterProvider(t *testing.T) {
	var gotPath, gotAuth, gotReferer, gotTitle string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		gotReferer = r.Header.Get(httpRefererHeader)
		gotTitle = r.Header.Get(xTitleHeader)

		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"id":"chatcmpl-1","object":"chat.completion","created":0,"model":"openai/gpt-4o-mini","choices":[{"index":0,"message":{"role":"assistant","content":"hello"},"finish_reason":"stop"}]}`))
	}))
	defer server.Close()

	oldBaseURL := orcarouterBaseURL
	orcarouterBaseURL = server.URL
	defer func() { orcarouterBaseURL = oldBaseURL }()

	client := newBotLLMClient(BotConfig{
		Provider: "orcarouter",
		ApiKey:   "sk-orca-test",
		Model:    "openai/gpt-4o-mini",
	})

	resp, err := client.CreateChatCompletion(context.Background(), openai.ChatCompletionRequest{
		Model: "openai/gpt-4o-mini",
		Messages: []openai.ChatCompletionMessage{
			{Role: openai.ChatMessageRoleUser, Content: "hello"},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if gotPath != "/chat/completions" {
		t.Errorf("expected path /chat/completions, got %s", gotPath)
	}
	if gotAuth != "Bearer sk-orca-test" {
		t.Errorf("expected Bearer sk-orca-test, got %s", gotAuth)
	}
	if gotReferer == "" {
		t.Errorf("expected HTTP-Referer header to be set")
	}
	if gotTitle == "" {
		t.Errorf("expected X-Title header to be set")
	}
	if resp.Choices[0].Message.Content != "hello" {
		t.Errorf("expected response content hello, got %s", resp.Choices[0].Message.Content)
	}
}

func TestBotConfigProviderJSONRoundTrip(t *testing.T) {
	cfg := BotConfig{
		Provider:       "orcarouter",
		Model:          "orcarouter/auto",
		ApiKey:         "sk-orca-test",
		Authorized:     false,
		WelcomeMessage: "hi",
	}

	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var decoded BotConfig
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if decoded.Provider != "orcarouter" {
		t.Errorf("expected provider orcarouter, got %s", decoded.Provider)
	}
	if decoded.Model != "orcarouter/auto" {
		t.Errorf("expected model orcarouter/auto, got %s", decoded.Model)
	}
	if decoded.ApiKey != "sk-orca-test" {
		t.Errorf("expected api key to round trip")
	}
	if !strings.Contains(string(data), "\"provider\"") {
		t.Errorf("expected serialized JSON to contain provider field")
	}
}
