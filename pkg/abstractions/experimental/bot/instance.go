package bot

import (
	"context"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
)

type botInstance struct {
	ctx        context.Context
	token      *types.Token
	stubConfig *types.StubConfigV1
	botConfig  BotConfig
	cancelFunc context.CancelFunc
}

func newBotInstance(ctx context.Context, token *types.Token, stubConfig *types.StubConfigV1, botConfig BotConfig) (*botInstance, error) {
	ctx, cancelFunc := context.WithCancel(ctx)
	return &botInstance{
		ctx:        ctx,
		token:      token,
		stubConfig: stubConfig,
		botConfig:  botConfig,
		cancelFunc: cancelFunc,
	}, nil
}

func (i *botInstance) Start() error {
	// TODO: Instantiate a prompt builder

	for {
		select {
		case <-i.ctx.Done():
			return nil
		default:
			time.Sleep(time.Second)
		}

	}
}

func (i *botInstance) step() {

}
