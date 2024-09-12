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
	history      []openai.ChatCompletionMessage
}

func NewBotInterface(key, model string) (*BotInterface, error) {
	bi := &BotInterface{
		client:       openai.NewClient(key),
		model:        model,
		inputBuffer:  &messageBuffer{Messages: []string{}, MaxLength: 100},
		outputBuffer: &messageBuffer{Messages: []string{}, MaxLength: 100},
		history:      []openai.ChatCompletionMessage{},
	}

	bi.outputBuffer.Push(fmt.Sprintf("Starting bot, using model<%s>\n", bi.model))
	return bi, nil
}

func (bi *BotInterface) SendPrompt(prompt string) error {
	messages := []openai.ChatCompletionMessage{
		{
			Role:    openai.ChatMessageRoleUser,
			Content: prompt,
		},
	}

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

	responseMsg := resp.Choices[0].Message.Content
	if !strings.HasSuffix(responseMsg, "\n") {
		responseMsg += "\n"
	}

	return bi.outputBuffer.Push(responseMsg)
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
