package bot

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/beam-cloud/beta9/pkg/scheduler"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/google/uuid"
)

const (
	botContainerTypeModel      string = "model"
	botContainerTypeTransition string = "transition"
)

type botInstance struct {
	ctx             context.Context
	scheduler       *scheduler.Scheduler
	token           *types.Token
	stub            *types.StubWithRelated
	stubConfig      *types.StubConfigV1
	botConfig       BotConfig
	cancelFunc      context.CancelFunc
	botStateManager *botStateManager
}

func newBotInstance(ctx context.Context, scheduler *scheduler.Scheduler, token *types.Token, stub *types.StubWithRelated, stubConfig *types.StubConfigV1, botConfig BotConfig, botStateManager *botStateManager) (*botInstance, error) {
	ctx, cancelFunc := context.WithCancel(ctx)
	return &botInstance{
		ctx:             ctx,
		token:           token,
		scheduler:       scheduler,
		stub:            stub,
		stubConfig:      stubConfig,
		botConfig:       botConfig,
		cancelFunc:      cancelFunc,
		botStateManager: botStateManager,
	}, nil
}

func (i *botInstance) Start() error {
	/*
		TODO:
			- Get a handle to a model (this could be a VLLM container)
			- Setup some sort of system prompt to establish the rules of the game
				- this should include information about the types of markers...?
			- Send system prompt / intro
			- Create some sort of interface that can accept input from the user
			- Determine how transition containers are managed
				- We can use a similar thing to a taskqueue
				- We need to be able to...?
	*/

	err := i.scheduler.Run(&types.ContainerRequest{
		ContainerId: i.genContainerId(botContainerTypeModel),
		Env:         []string{},
		Cpu:         i.stubConfig.Runtime.Cpu,
		Memory:      i.stubConfig.Runtime.Memory,
		Gpu:         string(i.stubConfig.Runtime.Gpu),
		GpuCount:    0, //uint32(i.stubConfig.Runtime.Gp),
		ImageId:     i.stubConfig.Runtime.ImageId,
		StubId:      i.stub.ExternalId,
		WorkspaceId: i.stub.Workspace.ExternalId,
		EntryPoint:  []string{i.stubConfig.PythonVersion, "-m", "beta9.runner.bot.model"},
		// Mounts:      [],
	})
	if err != nil {
		log.Printf("err: %+v\n", err)
		return err
	}

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

func (i *botInstance) genContainerId(containerType string) string {
	return fmt.Sprintf("%s-%s-%s-%s", botContainerPrefix, containerType, i.stub.ExternalId, uuid.New().String()[:8])
}
