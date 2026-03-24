package bot

import (
	"testing"
)

func TestBotConfigBaseUrl(t *testing.T) {
	t.Run("default base url is empty", func(t *testing.T) {
		config := BotConfig{
			Model:  "gpt-4o",
			ApiKey: "test-key",
		}
		if config.BaseUrl != "" {
			t.Errorf("expected empty BaseUrl, got %q", config.BaseUrl)
		}
	})

	t.Run("base url can be set for MiniMax", func(t *testing.T) {
		config := BotConfig{
			Model:   "MiniMax-M2.7",
			ApiKey:  "test-key",
			BaseUrl: "https://api.minimax.io/v1",
		}
		if config.BaseUrl != "https://api.minimax.io/v1" {
			t.Errorf("expected MiniMax base URL, got %q", config.BaseUrl)
		}
	})

	t.Run("base url is optional for OpenAI models", func(t *testing.T) {
		config := BotConfig{
			Model:  "gpt-4o",
			ApiKey: "test-key",
		}
		if config.BaseUrl != "" {
			t.Errorf("expected empty BaseUrl for OpenAI model, got %q", config.BaseUrl)
		}
	})
}

func TestBotConfigJSON(t *testing.T) {
	t.Run("base url is omitted when empty", func(t *testing.T) {
		config := BotConfig{
			Model:  "gpt-4o",
			ApiKey: "test-key",
		}
		// BaseUrl should be zero-value and omitted in JSON
		if config.BaseUrl != "" {
			t.Errorf("expected empty BaseUrl, got %q", config.BaseUrl)
		}
	})

	t.Run("base url is included when set", func(t *testing.T) {
		config := BotConfig{
			Model:   "MiniMax-M2.5",
			ApiKey:  "test-key",
			BaseUrl: "https://api.minimax.io/v1",
		}
		if config.BaseUrl == "" {
			t.Error("expected non-empty BaseUrl")
		}
	})
}

func TestBotConfigMiniMaxModels(t *testing.T) {
	miniMaxModels := []string{
		"MiniMax-M2.7",
		"MiniMax-M2.5",
		"MiniMax-M2.5-highspeed",
	}

	for _, model := range miniMaxModels {
		t.Run("config accepts "+model, func(t *testing.T) {
			config := BotConfig{
				Model:   model,
				ApiKey:  "test-minimax-key",
				BaseUrl: "https://api.minimax.io/v1",
			}
			if config.Model != model {
				t.Errorf("expected model %q, got %q", model, config.Model)
			}
			if config.BaseUrl != "https://api.minimax.io/v1" {
				t.Errorf("expected MiniMax base URL, got %q", config.BaseUrl)
			}
		})
	}
}

func TestBotConfigFormatLocations(t *testing.T) {
	config := BotConfig{
		Locations: map[string]BotLocationConfig{},
	}
	result := config.FormatLocations()
	if result != "There are no known locations." {
		t.Errorf("expected no locations message, got %q", result)
	}
}

func TestBotConfigFormatTransitions(t *testing.T) {
	config := BotConfig{
		Transitions: map[string]BotTransitionConfig{},
	}
	result := config.FormatTransitions()
	if result != "There are no known transitions that can be performed." {
		t.Errorf("expected no transitions message, got %q", result)
	}
}

func TestParseContainerId(t *testing.T) {
	t.Run("valid container id", func(t *testing.T) {
		containerId := "bot-transition-ef3f780c-6fe1-4f38-a201-96d32e825bb3-5886f9-41f80f43"
		container, err := parseContainerId(containerId)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if container.StubId != "ef3f780c-6fe1-4f38-a201-96d32e825bb3" {
			t.Errorf("unexpected stub id: %s", container.StubId)
		}
		if container.SessionId != "5886f9" {
			t.Errorf("unexpected session id: %s", container.SessionId)
		}
	})

	t.Run("invalid container id", func(t *testing.T) {
		_, err := parseContainerId("invalid-id")
		if err == nil {
			t.Error("expected error for invalid container id")
		}
	})
}
