package bot

import (
	"context"
	"errors"
	"log"

	openai "github.com/sashabaranov/go-openai"
)

type BotInterface struct {
	client       *openai.Client
	model        string
	inputBuffer  *Buffer
	outputBuffer *Buffer
}

type Buffer struct {
	Messages  []string
	MaxLength uint
}

func (b *Buffer) Push(msg string) error {
	if len(b.Messages) >= int(b.MaxLength) {
		return errors.New("buffer full")
	}

	b.Messages = append(b.Messages, msg)
	return nil
}

func NewBotInterface(key, model string) (*BotInterface, error) {
	return &BotInterface{
		client:       openai.NewClient(key),
		model:        model,
		inputBuffer:  &Buffer{Messages: []string{}, MaxLength: 100},
		outputBuffer: &Buffer{Messages: []string{}, MaxLength: 100},
	}, nil
}

func (bi *BotInterface) pushInput(msg string) error {
	return bi.inputBuffer.Push(msg)
}

func (bi *BotInterface) pushOutput(msg string) error {
	return bi.outputBuffer.Push(msg)
}

func (bi *BotInterface) SendPrompt(prompt string) error {
	resp, err := bi.client.CreateChatCompletion(
		context.Background(),
		openai.ChatCompletionRequest{
			Model: bi.model,
			Messages: []openai.ChatCompletionMessage{
				{
					Role:    openai.ChatMessageRoleUser,
					Content: prompt,
				},
			},
		},
	)
	if err != nil {
		return err
	}

	log.Println("resp: ", resp)

	return nil
}

type Prompt struct {
}

type Model struct {
}
