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
		log.Println(err)
		return err
	}

	responseMsg := resp.Choices[0].Message.Content
	return bi.outputBuffer.Push(responseMsg)
}

type Prompt struct {
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

func (b *Buffer) Pop() (string, error) {
	if len(b.Messages) == 0 {
		return "", errors.New("buffer empty")
	}

	msg := b.Messages[len(b.Messages)-1]
	b.Messages = b.Messages[:len(b.Messages)-1]
	return msg, nil
}