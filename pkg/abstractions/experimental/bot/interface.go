package bot

import (
	"context"
	"log"

	openai "github.com/sashabaranov/go-openai"
)

type BotInterface struct {
	client *openai.Client
}

func NewBotInterface(key string) (*BotInterface, error) {
	return &BotInterface{
		client: openai.NewClient(key),
	}, nil
}

func (bi *BotInterface) SendPrompt(prompt string) error {
	resp, err := bi.client.CreateChatCompletion(
		context.Background(),
		openai.ChatCompletionRequest{
			Model: openai.GPT3Dot5Turbo,
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
