package bot

import (
	"context"
	"errors"
	"fmt"
	"strings"

	openai "github.com/sashabaranov/go-openai"
)

type BotInterface struct {
	client       *openai.Client
	model        string
	inputBuffer  *messageBuffer
	outputBuffer *messageBuffer
	history      map[string][]openai.ChatCompletionMessage
}

func NewBotInterface(key, model string) (*BotInterface, error) {
	bi := &BotInterface{
		client:       openai.NewClient(key),
		model:        model,
		inputBuffer:  &messageBuffer{Messages: []string{}, MaxLength: 100},
		outputBuffer: &messageBuffer{Messages: []string{}, MaxLength: 100},
		history:      make(map[string][]openai.ChatCompletionMessage),
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

	userMessage := openai.ChatCompletionMessage{
		Role:    openai.ChatMessageRoleSystem,
		Content: "You can only speak italian.",
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
