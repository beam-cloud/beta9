package bot

import (
	"errors"
	"fmt"
	"strings"

	"github.com/beam-cloud/beta9/pkg/types"
	openai "github.com/sashabaranov/go-openai"
)

const (
	botVolumeName      = "beta9-bot-inputs"
	botVolumeMountPath = "./bot-input-files"
)

var ErrBotSessionNotFound = fmt.Errorf("bot session not found")

type BotSession struct {
	Id           string                     `json:"id" redis:"id"`
	Messages     []BotChatCompletionMessage `json:"messages" redis:"messages"`
	CreatedAt    int64                      `json:"created_at" redis:"created_at"`
	EventHistory []*BotEvent                `json:"event_history" redis:"event_history"`
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

type BotEventType string

const (
	BotEventTypeError               BotEventType = "error"
	BotEventTypeAgentMessage        BotEventType = "agent_message"
	BotEventTypeUserMessage         BotEventType = "user_message"
	BotEventTypeTransitionMessage   BotEventType = "transition_message"
	BotEventTypeMemoryMessage       BotEventType = "memory_message"
	BotEventTypeMemoryUpdated       BotEventType = "memory_updated"
	BotEventTypeSessionCreated      BotEventType = "session_created"
	BotEventTypeTransitionFired     BotEventType = "transition_fired"
	BotEventTypeTransitionFailed    BotEventType = "transition_failed"
	BotEventTypeTransitionStarted   BotEventType = "transition_started"
	BotEventTypeTransitionCompleted BotEventType = "transition_completed"
	BotEventTypeNetworkState        BotEventType = "network_state"
	BotEventTypeConfirmTransition   BotEventType = "confirm_transition"
	BotEventTypeAcceptTransition    BotEventType = "accept_transition"
	BotEventTypeRejectTransition    BotEventType = "reject_transition"
	BotEventTypeInputFileRequest    BotEventType = "input_file_request"
	BotEventTypeInputFileResponse   BotEventType = "input_file_response"
	BotEventTypeOutputFile          BotEventType = "output_file"
	BotEventTypeConfirmRequest      BotEventType = "confirm_request"
	BotEventTypeConfirmResponse     BotEventType = "confirm_response"
)

const PromptTypeUser = "user_message"
const PromptTypeTransition = "transition_message"
const PromptTypeMemory = "memory_message"

type BotEvent struct {
	Type     BotEventType      `json:"type" redis:"type"`
	Value    string            `json:"value" redis:"value"`
	Metadata map[string]string `json:"metadata" redis:"metadata"`
	PairId   string            `json:"pair_id" redis:"pair_id"`
}

type BotEventPair struct {
	Request  *BotEvent `json:"request" redis:"request"`
	Response *BotEvent `json:"response" redis:"response"`
}

type MetadataKey string

const (
	MetadataRequestId      MetadataKey = "request_id"
	MetadataSessionId      MetadataKey = "session_id"
	MetadataTransitionName MetadataKey = "transition_name"
	MetadataTaskId         MetadataKey = "task_id"
	MetadataErrorMsg       MetadataKey = "error_msg"
)

type BotUserResponse struct {
	Msg            string `json:"msg" redis:"msg"`
	MarkerData     Marker `json:"marker_data" redis:"marker_data"`
	CompleteMarker bool   `json:"complete_marker" redis:"complete_marker"`
}

type BotTransitionResponse struct {
	Msg string `json:"msg" redis:"msg"`
}

type BotMemoryResponse struct {
	Msg string `json:"msg" redis:"msg"`
}

type Marker struct {
	LocationName string        `json:"location_name" mapstructure:"location_name"`
	Fields       []MarkerField `json:"marker_data" mapstructure:"marker_data"`
	SourceTaskId string        `json:"source_task_id" mapstructure:"source_task_id"`
}

type MarkerField struct {
	FieldName  string `json:"field_name" mapstructure:"field_name"`
	FieldValue string `json:"field_value" mapstructure:"field_value"`
}

// BotConfig holds the overall config for the bot
type BotConfig struct {
	Model          string                         `json:"model" redis:"model"`
	Locations      map[string]BotLocationConfig   `json:"locations" redis:"locations"`
	Transitions    map[string]BotTransitionConfig `json:"transitions" redis:"transitions"`
	ApiKey         string                         `json:"api_key" redis:"api_key"`
	Authorized     bool                           `json:"authorized" redis:"authorized"`
	WelcomeMessage string                         `json:"welcome_message" redis:"welcome_message"`
}

func (b *BotConfig) FormatLocations() string {
	locations := []string{}
	for _, location := range b.Locations {
		if !location.Expose {
			continue
		}

		locations = append(locations, location.FormatLocation())
	}

	if len(locations) == 0 {
		return "There are no known locations."
	}

	return strings.Join(locations, "\n")
}

func (b *BotConfig) FormatTransitions() string {
	transitions := []string{}
	for _, transition := range b.Transitions {
		if !transition.Expose {
			continue
		}

		transitions = append(transitions, transition.FormatTransition())
	}

	if len(transitions) == 0 {
		return "There are no known transitions that can be performed."
	}

	return strings.Join(transitions, "\n")
}

// BotLocationConfig holds the config for a location in the bot network
type BotLocationConfig struct {
	Name   string            `json:"name" redis:"name"`
	Marker map[string]string `json:"marker" redis:"marker"`
	Expose bool              `json:"expose" redis:"expose"`
}

func (l *BotLocationConfig) FormatLocation() string {
	return fmt.Sprintf("Location: %s\nMarker: %v", l.Name, l.Marker)
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
	Outputs       []string       `json:"outputs" redis:"outputs"`
	Description   string         `json:"description" redis:"description"`
	Expose        bool           `json:"expose" redis:"expose"`
	Confirm       bool           `json:"confirm" redis:"confirm"`
}

func (t *BotTransitionConfig) FormatTransition() string {
	inputs := []string{}
	for marker, count := range t.Inputs {
		inputs = append(inputs, fmt.Sprintf("requires %d %s inputs", count, marker))
	}
	inputsSection := strings.Join(inputs, ", ")

	outputs := []string{}
	for _, marker := range t.Outputs {
		outputs = append(outputs, fmt.Sprintf("can produce outputs of type %s", marker))
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

type BotNetworkSnapshot struct {
	SessionId            string           `json:"session_id" redis:"session_id"`
	LocationMarkerCounts map[string]int64 `json:"location_marker_counts" redis:"location_marker_counts"`
	Config               BotConfig        `json:"config" redis:"config"`
}
