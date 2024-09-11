package bot

import (
	"context"
	"fmt"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/google/uuid"
)

type botInstance struct {
	stub            *types.StubWithRelated
	ctx             context.Context
	token           *types.Token
	stubConfig      *types.StubConfigV1
	botConfig       BotConfig
	cancelFunc      context.CancelFunc
	botStateManager *botStateManager
}

func newBotInstance(ctx context.Context, token *types.Token, stub *types.StubWithRelated, stubConfig *types.StubConfigV1, botConfig BotConfig, botStateManager *botStateManager) (*botInstance, error) {
	ctx, cancelFunc := context.WithCancel(ctx)
	return &botInstance{
		ctx:             ctx,
		token:           token,
		stub:            stub,
		stubConfig:      stubConfig,
		botConfig:       botConfig,
		cancelFunc:      cancelFunc,
		botStateManager: botStateManager,
	}, nil
}

func (i *botInstance) Start() error {
	// TODO: Instantiate a prompt builder

	for {
		select {
		case <-i.ctx.Done():
			return nil
		default:
			i.step()
			time.Sleep(time.Second)
		}

	}
}

func (i *botInstance) step() {
	i.botStateManager.addMarkerToLocation()
}

func (i *botInstance) genContainerId() string {
	return fmt.Sprintf("%s-%s-%s", botContainerPrefix, i.stub.ExternalId, uuid.New().String()[:8])
}
