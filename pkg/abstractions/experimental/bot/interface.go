package bot

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/beam-cloud/beta9/pkg/types"
	openai "github.com/sashabaranov/go-openai"
	"github.com/sashabaranov/go-openai/jsonschema"
)

type BotInterface struct {
	client           *openai.Client
	botConfig        BotConfig
	model            string
	systemPrompt     string
	stateManager     *botStateManager
	workspace        *types.Workspace
	stub             *types.StubWithRelated
	userSchema       *jsonschema.Definition
	transitionSchema *jsonschema.Definition
}

type botInterfaceOpts struct {
	AppConfig    types.AppConfig
	BotConfig    BotConfig
	StateManager *botStateManager
	Workspace    *types.Workspace
	Stub         *types.StubWithRelated
}

func NewBotInterface(opts botInterfaceOpts) (*BotInterface, error) {
	bi := &BotInterface{
		client:       openai.NewClient(opts.AppConfig.Abstractions.Bot.OpenAIKey),
		botConfig:    opts.BotConfig,
		model:        opts.BotConfig.Model,
		systemPrompt: opts.AppConfig.Abstractions.Bot.SystemPrompt,
		stateManager: opts.StateManager,
		workspace:    opts.Workspace,
		stub:         opts.Stub,
	}

	// Generate the schema for each response
	var userResponse BotUserResponse
	schema, err := jsonschema.GenerateSchemaForType(userResponse)
	if err != nil {
		return nil, err
	}
	bi.userSchema = schema

	var transitionResponse BotTransitionResponse
	schema, err = jsonschema.GenerateSchemaForType(transitionResponse)
	if err != nil {
		return nil, err
	}
	bi.transitionSchema = schema

	return bi, nil
}

func (bi *BotInterface) initSession(sessionId string) error {
	var state *BotSession
	var err error

	state, err = bi.stateManager.loadSession(bi.workspace.Name, bi.stub.ExternalId, sessionId)

	// New session, create a state object to store message history and other session metadata
	if err != nil && err == ErrBotSessionNotFound {
		state = &BotSession{
			Id: sessionId,
		}

		setupPrompt := BotChatCompletionMessage{
			Role:    openai.ChatMessageRoleSystem,
			Content: bi.systemPrompt,
		}

		networkStructurePrompt := BotChatCompletionMessage{
			Role:    openai.ChatMessageRoleSystem,
			Content: fmt.Sprintf("Locations and marker types you can convert user data to: %v", bi.botConfig.FormatLocations()),
		}

		networkLayoutPrompt := BotChatCompletionMessage{
			Role:    openai.ChatMessageRoleSystem,
			Content: fmt.Sprintf("Transitions that can be performed, never mention the input names in your response: %v", bi.botConfig.FormatTransitions()),
		}

		state.Messages = []BotChatCompletionMessage{setupPrompt, networkStructurePrompt, networkLayoutPrompt}
	} else if err != nil {
		return err
	}

	resp, err := bi.client.CreateChatCompletion(
		context.Background(),
		openai.ChatCompletionRequest{
			Model:    bi.model,
			Messages: state.GetMessagesInOpenAIFormat(),
		},
	)
	if err != nil {
		return err
	}

	responseMessage := resp.Choices[0].Message

	state.Messages = append(state.Messages, BotChatCompletionMessage{
		Role:    responseMessage.Role,
		Content: responseMessage.Content,
	})

	err = bi.stateManager.updateSession(bi.workspace.Name, bi.stub.ExternalId, sessionId, state)
	if err != nil {
		return err
	}

	return nil
}

func (bi *BotInterface) addMessagesToSessionHistory(sessionId string, messages []BotChatCompletionMessage) error {
	state, err := bi.stateManager.loadSession(bi.workspace.Name, bi.stub.ExternalId, sessionId)
	if err != nil {
		return err
	}

	state.Messages = append(state.Messages, messages...)

	err = bi.stateManager.updateSession(bi.workspace.Name, bi.stub.ExternalId, sessionId, state)
	if err != nil {
		return err
	}

	return nil
}

func wrapPrompt(tag, prompt string) string {
	return fmt.Sprintf("<%s> %s </%s>", tag, prompt, tag)
}

func (bi *BotInterface) getSessionHistory(sessionId string) ([]openai.ChatCompletionMessage, error) {
	state, err := bi.stateManager.loadSession(bi.workspace.Name, bi.stub.ExternalId, sessionId)
	if err != nil {
		return nil, err
	}

	return state.GetMessagesInOpenAIFormat(), nil
}

func (bi *BotInterface) SendPrompt(sessionId, messageType, prompt string) error {
	messages, err := bi.getSessionHistory(sessionId)
	if err != nil {
		return err
	}

	role := openai.ChatMessageRoleUser
	promptMessage := openai.ChatCompletionMessage{
		Content: prompt,
	}

	var schema *jsonschema.Definition = bi.userSchema

	switch messageType {
	case PromptTypeUser:
		role = openai.ChatMessageRoleUser
		prompt = wrapPrompt(PromptTypeUser, prompt)
	case PromptTypeTransition:
		role = openai.ChatMessageRoleUser
		prompt = wrapPrompt(PromptTypeTransition, prompt)
		schema = bi.transitionSchema
	default:
		return fmt.Errorf("invalid message type: %s", messageType)
	}

	promptMessage.Content = prompt
	promptMessage.Role = role
	messages = append(messages, promptMessage)

	resp, err := bi.client.CreateChatCompletion(
		context.Background(),
		openai.ChatCompletionRequest{
			Model:    bi.model,
			Messages: messages,
			ResponseFormat: &openai.ChatCompletionResponseFormat{
				Type: openai.ChatCompletionResponseFormatTypeJSONSchema,
				JSONSchema: &openai.ChatCompletionResponseFormatJSONSchema{
					Name:   botSchemaName,
					Schema: schema,
					Strict: true,
				},
			},
		},
	)
	if err != nil {
		return err
	}

	responseMessage := resp.Choices[0].Message
	err = bi.addMessagesToSessionHistory(sessionId, []BotChatCompletionMessage{
		{
			Role:    role,
			Content: prompt,
		},
		{
			Role:    responseMessage.Role,
			Content: responseMessage.Content,
		},
	})
	if err != nil {
		return err
	}

	msg := ""
	if messageType == PromptTypeUser {
		formattedResponse := BotUserResponse{}
		err = json.Unmarshal([]byte(responseMessage.Content), &formattedResponse)
		if err != nil {
			return err
		}

		// If we have a complete marker, push it to session state
		if formattedResponse.CompleteMarker {
			err = bi.stateManager.pushMarker(bi.workspace.Name, bi.stub.ExternalId, sessionId, formattedResponse.MarkerData.LocationName, formattedResponse.MarkerData)
			if err != nil {
				return err
			}
		}

		msg = formattedResponse.Msg
	} else if messageType == PromptTypeTransition {
		formattedResponse := BotTransitionResponse{}
		err = json.Unmarshal([]byte(responseMessage.Content), &formattedResponse)
		if err != nil {
			return err
		}

		msg = formattedResponse.Msg
	}

	event := &BotEvent{
		Type:  BotEventTypeAgentMessage,
		Value: msg,
	}

	return bi.stateManager.pushEvent(bi.workspace.Name, bi.stub.ExternalId, sessionId, event)
}
