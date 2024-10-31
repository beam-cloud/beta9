package bot

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/beam-cloud/beta9/pkg/scheduler"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/google/uuid"
)

const (
	botContainerPrefix         string = "bot"
	botContainerTypeModel      string = "model" // TODO: only need this in the case where we host the model on beta9 using vllm (or similar)
	botContainerTypeTransition string = "transition"
)

type botInstance struct {
	ctx             context.Context
	appConfig       types.AppConfig
	scheduler       *scheduler.Scheduler
	token           *types.Token
	stub            *types.StubWithRelated
	workspace       *types.Workspace
	stubConfig      *types.StubConfigV1
	botConfig       BotConfig
	cancelFunc      context.CancelFunc
	botStateManager *botStateManager
	botInterface    *BotInterface
}

func newBotInstance(ctx context.Context, appConfig types.AppConfig, scheduler *scheduler.Scheduler, token *types.Token, stub *types.StubWithRelated, stubConfig *types.StubConfigV1, botConfig BotConfig, botStateManager *botStateManager) (*botInstance, error) {
	ctx, cancelFunc := context.WithCancel(ctx)

	botInterface, err := NewBotInterface(botInterfaceOpts{
		AppConfig:    appConfig,
		BotConfig:    botConfig,
		StateManager: botStateManager,
		Workspace:    &stub.Workspace,
		Stub:         stub,
	})
	if err != nil {
		cancelFunc()
		return nil, err
	}

	return &botInstance{
		ctx:             ctx,
		appConfig:       appConfig,
		token:           token,
		scheduler:       scheduler,
		stub:            stub,
		workspace:       &stub.Workspace,
		stubConfig:      stubConfig,
		botConfig:       botConfig,
		cancelFunc:      cancelFunc,
		botStateManager: botStateManager,
		botInterface:    botInterface,
	}, nil
}

func (i *botInstance) Start() error {
	for {
		select {
		case <-i.ctx.Done():
			return nil
		default:
			prompt, err := i.botInterface.inputBuffer.Pop()
			if err == nil {
				err = i.botInterface.SendPrompt("testsession", prompt)
				if err != nil {
					continue
				}
			}

			i.step()
			time.Sleep(time.Duration(i.appConfig.Abstractions.Bot.StepIntervalS) * time.Second)
		}
	}
}

func (i *botInstance) runTransition(transitionName string) error {
	transitionConfig, ok := i.botConfig.Transitions[transitionName]
	if !ok {
		return errors.New("transition not found")
	}

	env := []string{
		fmt.Sprintf("BETA9_TOKEN=%s", i.token.Key),
		fmt.Sprintf("HANDLER=%s", transitionConfig.Handler),
		fmt.Sprintf("ON_START=%s", transitionConfig.Handler),
		fmt.Sprintf("STUB_ID=%s", i.stub.ExternalId),
		fmt.Sprintf("STUB_TYPE=%s", i.stub.Type),
		fmt.Sprintf("KEEP_WARM_SECONDS=%d", transitionConfig.KeepWarm),
		fmt.Sprintf("PYTHON_VERSION=%s", i.stubConfig.PythonVersion),
		fmt.Sprintf("CALLBACK_URL=%s", transitionConfig.CallbackUrl),
		fmt.Sprintf("TIMEOUT=%d", i.stubConfig.TaskPolicy.Timeout), // TODO: add real timeout
	}

	err := i.scheduler.Run(&types.ContainerRequest{
		ContainerId: i.genContainerId(botContainerTypeTransition),
		Env:         env,
		Cpu:         transitionConfig.Cpu,
		Memory:      transitionConfig.Memory,
		Gpu:         string(transitionConfig.Gpu),
		GpuCount:    0,
		ImageId:     transitionConfig.ImageId,
		StubId:      i.stub.ExternalId,
		WorkspaceId: i.stub.Workspace.ExternalId,
		EntryPoint:  []string{i.stubConfig.PythonVersion, "-m", "beta9.runner.bot.transition"},
		// Mounts:      [], // TODO: properly configure mounts
	})
	if err != nil {
		return err
	}

	return nil
}

func (i *botInstance) step() {
	// Count the markers
	for _, location := range i.botConfig.Locations {
		log.Println("location: ", location.Name)
		markers, err := i.botStateManager.countMarkers(i.workspace.Name, i.stub.ExternalId, "testsession", location.Name)
		if err != nil {
			continue
		}

		log.Println("MARKERS: ", markers)
	}

	// Determine which transitions can fire (this should be locked)
}

func (i *botInstance) genContainerId(botContainerType string) string {
	return fmt.Sprintf("%s-%s-%s-%s", botContainerPrefix, botContainerType, i.stub.ExternalId, uuid.New().String()[:8])
}