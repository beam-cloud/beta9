package bot

import (
	"errors"
	"fmt"
	"strings"

	"github.com/beam-cloud/beta9/pkg/types"
	openai "github.com/sashabaranov/go-openai"
)

var ErrBotSessionNotFound = fmt.Errorf("bot session not found")

type BotSession struct {
	Id            string                     `json:"id" redis:"id"`
	Messages      []BotChatCompletionMessage `json:"messages" redis:"messages"`
	LastUpdatedAt int64                      `redis:"last_updated_at" json:"last_updated_at"`
}

type botContainer struct {
	StubId      string
	SessionId   string
	ContainerId string
}

func parseContainerId(containerId string) (*botContainer, error) {
	// Bot container IDs look like this: "bot-transition-ef3f780c-6fe1-4f38-a201-96d32e825bb3-5886f9-41f80f43"
	// This part: "ef3f780c-6fe1-4f38-a201-96d32e825bb3" is the stub ID
	// Session ID is: "5886f9"
	parts := strings.Split(containerId, "-")
	if len(parts) < 8 {
		return nil, errors.New("invalid container id")
	}

	stubId := strings.Join(parts[2:7], "-")
	sessionId := parts[7]

	return &botContainer{
		StubId:      stubId,
		SessionId:   sessionId,
		ContainerId: containerId,
	}, nil
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
	Msg string `json:"msg" redis:"msg"`
}

type BotEventType string

const (
	BotEventTypeMessage         BotEventType = "msg"
	BotEventTypeSessionCreated  BotEventType = "session_created"
	BotEventTypeTransitionFired BotEventType = "transition_fired"
	BotEventTypeTaskStarted     BotEventType = "task_started"
	BotEventTypeTaskCompleted   BotEventType = "task_completed"
)

type BotEvent struct {
	Type  BotEventType `json:"type" redis:"type"`
	Value string       `json:"value" redis:"value"`
}

type BotResponse struct {
	Msg            string `json:"msg" redis:"msg"`
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

func (b *BotConfig) FormatTransitions() string {
	transitions := []string{}
	for _, transition := range b.Transitions {
		transitions = append(transitions, transition.FormatTransition())
	}
	return strings.Join(transitions, "\n")
}

// BotLocationConfig holds the config for a location in the bot network
type BotLocationConfig struct {
	Name   string            `json:"name" redis:"name"`
	Marker map[string]string `json:"marker" redis:"marker"`
}

// BotTransitionConfig holds the config for a transition
type BotTransitionConfig struct {
	Cpu           int64          `json:"cpu" redis:"cpu"`
	Gpu           types.GpuType  `json:"gpu" redis:"gpu"`
	Memory        int64          `json:"memory" redis:"memory"`
	PythonVersion string         `json:"python_version" redis:"python_version"`
	ImageId       string         `json:"image_id" redis:"image_id"`
	Timeout       int            `json:"timeout" redis:"timeout"`
	KeepWarm      int            `json:"keep_warm" redis:"keep_warm"`
	MaxPending    int            `json:"max_pending" redis:"max_pending"`
	Volumes       []string       `json:"volumes" redis:"volumes"`
	Secrets       []string       `json:"secrets" redis:"secrets"`
	Handler       string         `json:"handler" redis:"handler"`
	OnStart       string         `json:"on_start" redis:"on_start"`
	CallbackUrl   string         `json:"callback_url" redis:"callback_url"`
	TaskPolicy    string         `json:"task_policy" redis:"task_policy"`
	Name          string         `json:"name" redis:"name"`
	Inputs        map[string]int `json:"inputs" redis:"inputs"`
	Outputs       map[string]int `json:"outputs" redis:"outputs"`
	Description   string         `json:"description" redis:"description"`
}

func (t *BotTransitionConfig) FormatTransition() string {
	inputs := []string{}
	for marker, count := range t.Inputs {
		inputs = append(inputs, fmt.Sprintf("requires %d %s inputs", count, marker))
	}
	inputsSection := strings.Join(inputs, ", ")

	outputs := []string{}
	for marker, count := range t.Outputs {
		outputs = append(outputs, fmt.Sprintf("produces %d %s outputs", count, marker))
	}
	outputsSection := strings.Join(outputs, ", ")

	return fmt.Sprintf(
		"Transition: %s %s %s\nDescription: %s",
		t.Name,
		inputsSection,
		outputsSection,
		t.Description,
	)
}
