package bot

import (
	"fmt"

	"github.com/beam-cloud/beta9/pkg/types"
	openai "github.com/sashabaranov/go-openai"
)

var ErrBotSessionNotFound = fmt.Errorf("bot session not found")

type BotSession struct {
	Messages []BotChatCompletionMessage `json:"messages" redis:"messages"`
}

func (s *BotSession) GetMessagesInOpenAIFormat() []openai.ChatCompletionMessage {
	messages := []openai.ChatCompletionMessage{}

	for _, message := range s.Messages {
		messages = append(messages, openai.ChatCompletionMessage{
			Role:    message.Role,
			Content: message.Content,
			Name:    message.Name,
		})
	}

	return messages
}

type BotChatCompletionMessage struct {
	Role    string `json:"role" redis:"role"`
	Content string `json:"content" redis:"content"`

	// This property isn't in the official documentation, but it's in
	// the documentation for the official library for python:
	// - https://github.com/openai/openai-python/blob/main/chatml.md
	// - https://github.com/openai/openai-cookbook/blob/main/examples/How_to_count_tokens_with_tiktoken.ipynb
	Name string `json:"name,omitempty" redis:"name,omitempty"`
}

const botSchemaName = "beam_bot"

type UserRequest struct {
	Msg       string `json:"msg" redis:"msg"`
	SessionId string `json:"session_id" redis:"session_id"`
}

type BotResponse struct {
	UserMessage    string `json:"user_message" redis:"user_message"`
	MarkerData     Marker `json:"marker_data" redis:"marker_data"`
	CompleteMarker bool   `json:"complete_marker" redis:"complete_marker"`
}

type Marker struct {
	LocationName string        `json:"location_name" redis:"location_name"`
	Fields       []MarkerField `json:"marker_data" redis:"marker_data"`
}

type MarkerField struct {
	FieldName  string `json:"field_name" redis:"field_name"`
	FieldValue string `json:"field_value" redis:"field_value"`
}

// BotConfig holds the overall config for the bot
type BotConfig struct {
	Model       string                         `json:"model" redis:"model"`
	Locations   map[string]BotLocationConfig   `json:"locations" redis:"locations"`
	Transitions map[string]BotTransitionConfig `json:"transitions" redis:"transitions"`
}

// BotLocationConfig holds the config for a location in the bot network
type BotLocationConfig struct {
	Name   string            `json:"name" redis:"name"`
	Marker map[string]string `json:"marker" redis:"marker"`
}

// BotTransitionConfig holds the config for a transition
type BotTransitionConfig struct {
	Cpu         int64          `json:"cpu" redis:"cpu"`
	Gpu         types.GpuType  `json:"gpu" redis:"gpu"`
	Memory      int64          `json:"memory" redis:"memory"`
	ImageId     string         `json:"image_id" redis:"image_id"`
	Timeout     int            `json:"timeout" redis:"timeout"`
	KeepWarm    int            `json:"keep_warm" redis:"keep_warm"`
	MaxPending  int            `json:"max_pending" redis:"max_pending"`
	Volumes     []string       `json:"volumes" redis:"volumes"`
	Secrets     []string       `json:"secrets" redis:"secrets"`
	Handler     string         `json:"handler" redis:"handler"`
	OnStart     string         `json:"on_start" redis:"on_start"`
	CallbackUrl string         `json:"callback_url" redis:"callback_url"`
	TaskPolicy  string         `json:"task_policy" redis:"task_policy"`
	Name        string         `json:"name" redis:"name"`
	Inputs      map[string]int `json:"inputs" redis:"inputs"`
	Outputs     map[string]int `json:"outputs" redis:"outputs"`
	Description string         `json:"description" redis:"description"`
}
