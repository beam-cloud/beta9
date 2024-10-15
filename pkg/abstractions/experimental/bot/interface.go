package bot

import (
	"context"
	"errors"
	"fmt"
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
	history      map[string][]openai.ChatCompletionMessage
	systemPrompt string
}

func NewBotInterface(appConfig types.AppConfig, botConfig BotConfig) (*BotInterface, error) {
	bi := &BotInterface{
		client:       openai.NewClient(appConfig.Abstractions.Bot.OpenAIKey),
		botConfig:    botConfig,
		model:        botConfig.Model,
		inputBuffer:  &messageBuffer{Messages: []string{}, MaxLength: 100},
		outputBuffer: &messageBuffer{Messages: []string{}, MaxLength: 100},
		history:      make(map[string][]openai.ChatCompletionMessage),
		systemPrompt: appConfig.Abstractions.Bot.SystemPrompt,
	}

	bi.outputBuffer.Push(fmt.Sprintf("Starting bot, using model<%s>\n", bi.model))

	err := bi.initSession("testsession")
	if err != nil {
		return nil, err
	}

	return bi, nil
}

func (bi *BotInterface) initSession(sessionId string) error {
	if _, exists := bi.history[sessionId]; !exists {
		bi.history[sessionId] = []openai.ChatCompletionMessage{}
	}

	setupPrompt := openai.ChatCompletionMessage{
		Role:    openai.ChatMessageRoleSystem,
		Content: bi.systemPrompt,
	}

	networkStructurePrompt := openai.ChatCompletionMessage{
		Role:    openai.ChatMessageRoleSystem,
		Content: fmt.Sprintf("Here are the locations and marker types you can convert user data to: %v", bi.botConfig.Locations),
	}

	networkLayoutPrompt := openai.ChatCompletionMessage{
		Role:    openai.ChatMessageRoleSystem,
		Content: fmt.Sprintf("Here are the transitions you can perform: %v", bi.botConfig.Transitions),
	}

	bi.addMessageToHistory(sessionId, setupPrompt)
	bi.addMessageToHistory(sessionId, networkStructurePrompt)
	bi.addMessageToHistory(sessionId, networkLayoutPrompt)

	resp, err := bi.client.CreateChatCompletion(
		context.Background(),
		openai.ChatCompletionRequest{
			Model:    bi.model,
			Messages: bi.getSessionHistory(sessionId),
		},
	)
	if err != nil {
		return err
	}

	responseMessage := resp.Choices[0].Message
	bi.addMessageToHistory(sessionId, responseMessage)

	return nil
}

func (bi *BotInterface) addMessageToHistory(sessionId string, message openai.ChatCompletionMessage) {
	bi.history[sessionId] = append(bi.history[sessionId], message)
}

func (bi *BotInterface) getSessionHistory(sessionId string) []openai.ChatCompletionMessage {
	return bi.history[sessionId]
}

func (bi *BotInterface) SendPrompt(sessionId, prompt string) error {
	userMessage := openai.ChatCompletionMessage{
		Role:    openai.ChatMessageRoleUser,
		Content: prompt,
	}

	bi.addMessageToHistory(sessionId, userMessage)

	resp, err := bi.client.CreateChatCompletion(
		context.Background(),
		openai.ChatCompletionRequest{
			Model:    bi.model,
			Messages: bi.getSessionHistory(sessionId),
		},
	)

	if err != nil {
		return fmt.Errorf("failed to create chat completion: %w", err)
	}

	responseMessage := resp.Choices[0].Message
	bi.addMessageToHistory(sessionId, responseMessage)

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
