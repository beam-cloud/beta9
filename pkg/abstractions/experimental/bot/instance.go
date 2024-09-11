package bot

import (
	"context"
	"log"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
)

type botInstance struct {
	ctx        context.Context
	token      *types.Token
	stubConfig *types.StubConfigV1
	botConfig  BotConfig
}

func newBotInstance(ctx context.Context, token *types.Token, stubConfig *types.StubConfigV1, botConfig BotConfig) (*botInstance, error) {
	return &botInstance{
		ctx:        ctx,
		token:      token,
		stubConfig: stubConfig,
		botConfig:  botConfig,
	}, nil
}

func (i *botInstance) Start() error {
	log.Printf("starting bot with config: %+v\n", i.botConfig)
	for {
		select {
		case <-i.ctx.Done():
			return nil
		default:
			time.Sleep(time.Second)
		}

	}
}
