package bot

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"

	"github.com/beam-cloud/beta9/pkg/types"
	openai "github.com/sashabaranov/go-openai"
)

type BotInterface struct {
	client       *openai.Client
	botConfig    BotConfig
	model        string
	inputBuffer  *messageBuffer
	outputBuffer *messageBuffer
	systemPrompt string
	stateManager *botStateManager
	workspace    *types.Workspace
	stub         *types.StubWithRelated
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
		inputBuffer:  &messageBuffer{Messages: []string{}, MaxLength: 100},
		outputBuffer: &messageBuffer{Messages: []string{}, MaxLength: 100},
		systemPrompt: opts.AppConfig.Abstractions.Bot.SystemPrompt,
		stateManager: opts.StateManager,
		workspace:    opts.Workspace,
		stub:         opts.Stub,
	}

	bi.outputBuffer.Push(fmt.Sprintf("Starting bot, using model<%s>\n", bi.model))

	err := bi.initSession("testsession")
	if err != nil {
		return nil, err
	}

	return bi, nil
}

func (bi *BotInterface) initSession(sessionId string) error {
	var state *BotSession
	var err error

	state, err = bi.stateManager.loadSession(bi.workspace.Name, bi.stub.ExternalId, sessionId)

	// New session, create a state object to store message history and other session metadata
	if err != nil && err == ErrBotSessionNotFound {
		state = &BotSession{}

		setupPrompt := BotChatCompletionMessage{
			Role:    openai.ChatMessageRoleSystem,
			Content: bi.systemPrompt,
		}

		networkStructurePrompt := BotChatCompletionMessage{
			Role:    openai.ChatMessageRoleSystem,
			Content: fmt.Sprintf("Locations and marker types you can convert user data to: %v", bi.botConfig.Locations),
		}

		networkLayoutPrompt := BotChatCompletionMessage{
			Role:    openai.ChatMessageRoleSystem,
			Content: fmt.Sprintf("Transitions you can perform: %v", bi.botConfig.Transitions),
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

func (bi *BotInterface) addMessageToSessionHistory(sessionId string, message openai.ChatCompletionMessage) error {
	state, err := bi.stateManager.loadSession(bi.workspace.Name, bi.stub.ExternalId, sessionId)
	if err != nil {
		return err
	}

	state.Messages = append(state.Messages, BotChatCompletionMessage{
		Role:    message.Role,
		Content: message.Content,
	})

	err = bi.stateManager.updateSession(bi.workspace.Name, bi.stub.ExternalId, sessionId, state)
	if err != nil {
		return err
	}

	return nil
}

func (bi *BotInterface) getSessionHistory(sessionId string) ([]openai.ChatCompletionMessage, error) {
	state, err := bi.stateManager.loadSession(bi.workspace.Name, bi.stub.ExternalId, sessionId)
	if err != nil {
		return nil, err
	}

	log.Printf("loaded session state! ====> %v\n", state)

	return state.GetMessagesInOpenAIFormat(), nil
}

func (bi *BotInterface) SendPrompt(sessionId, prompt string) error {
	userMessage := openai.ChatCompletionMessage{
		Role:    openai.ChatMessageRoleUser,
		Content: prompt,
	}

	messages, err := bi.getSessionHistory(sessionId)
	if err != nil {
		return err
	}
	messages = append(messages, userMessage)

	resp, err := bi.client.CreateChatCompletion(
		context.Background(),
		openai.ChatCompletionRequest{
			Model:    bi.model,
			Messages: messages,
		},
	)
	if err != nil {
		return err
	}

	responseMessage := resp.Choices[0].Message
	err = bi.addMessageToSessionHistory(sessionId, responseMessage)
	if err != nil {
		return err
	}

	responseMsgContent := responseMessage.Content
	if !strings.HasSuffix(responseMsgContent, "\n") {
		responseMsgContent += "\n"
	}

	return bi.outputBuffer.Push(responseMsgContent)
}

func (bi *BotInterface) pushInput(msg string) error {
	return bi.inputBuffer.Push(msg)
}

type messageBuffer struct {
	Messages  []string
	MaxLength uint
}

func (b *messageBuffer) Push(msg string) error {
	if len(b.Messages) >= int(b.MaxLength) {
		return errors.New("buffer full")
	}

	b.Messages = append(b.Messages, msg)
	return nil
}

func (b *messageBuffer) Pop() (string, error) {
	if len(b.Messages) == 0 {
		return "", errors.New("buffer empty")
	}

	msg := b.Messages[len(b.Messages)-1]
	b.Messages = b.Messages[:len(b.Messages)-1]
	return msg, nil
}
