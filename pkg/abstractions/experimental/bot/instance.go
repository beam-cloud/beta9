package bot

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/scheduler"
	"github.com/beam-cloud/beta9/pkg/task"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/google/uuid"
)

const (
	botContainerPrefix         string = "bot"
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
	taskDispatcher  *task.Dispatcher
	authInfo        *auth.AuthInfo
}

type botInstanceOpts struct {
	AppConfig      types.AppConfig
	Scheduler      *scheduler.Scheduler
	Token          *types.Token
	Stub           *types.StubWithRelated
	StubConfig     *types.StubConfigV1
	BotConfig      BotConfig
	StateManager   *botStateManager
	TaskDispatcher *task.Dispatcher
}

func newBotInstance(ctx context.Context, opts botInstanceOpts) (*botInstance, error) {
	ctx, cancelFunc := context.WithCancel(ctx)

	botInterface, err := NewBotInterface(botInterfaceOpts{
		AppConfig:    opts.AppConfig,
		BotConfig:    opts.BotConfig,
		StateManager: opts.StateManager,
		Workspace:    &opts.Stub.Workspace,
		Stub:         opts.Stub,
	})
	if err != nil {
		cancelFunc()
		return nil, err
	}

	return &botInstance{
		ctx:             ctx,
		appConfig:       opts.AppConfig,
		token:           opts.Token,
		scheduler:       opts.Scheduler,
		stub:            opts.Stub,
		workspace:       &opts.Stub.Workspace,
		stubConfig:      opts.StubConfig,
		botConfig:       opts.BotConfig,
		cancelFunc:      cancelFunc,
		botStateManager: opts.StateManager,
		botInterface:    botInterface,
		taskDispatcher:  opts.TaskDispatcher,
		authInfo: &auth.AuthInfo{
			Workspace: &opts.Stub.Workspace,
			Token:     opts.Token,
		},
	}, nil
}

func (i *botInstance) Start() error {
	stepInterval := time.Duration(i.appConfig.Abstractions.Bot.StepIntervalS) * time.Second

	for {
		select {
		case <-i.ctx.Done():
			return nil
		default:
			activeSessions, err := i.botStateManager.getActiveSessions(i.workspace.Name, i.stub.ExternalId)
			if err != nil || len(activeSessions) == 0 {
				select {
				case <-i.ctx.Done():
					return nil
				case <-time.After(stepInterval):
					continue
				}
			}

			for _, session := range activeSessions {
				if msg, err := i.botStateManager.popInputMessage(i.workspace.Name, i.stub.ExternalId, session.Id); err == nil {
					if err := i.botInterface.SendPrompt(session.Id, msg); err != nil {
						continue
					}
				}

				i.step(session.Id)
			}

			select {
			case <-i.ctx.Done():
				return nil
			case <-time.After(stepInterval):
			}
		}
	}
}

func (i *botInstance) step(sessionId string) {
	err := i.botStateManager.acquireLock(i.workspace.Name, i.stub.ExternalId, sessionId)
	if err != nil {
		return
	}

	func() {
		defer i.botStateManager.releaseLock(i.workspace.Name, i.stub.ExternalId, sessionId)

		for _, transition := range i.botConfig.Transitions {
			currentMarkerCounts := make(map[string]int64)

			markersToPop := make(map[string]int64)
			canFire := true

			for locationName, requiredCount := range transition.Inputs {
				count, err := i.botStateManager.countMarkers(i.workspace.Name, i.stub.ExternalId, sessionId, locationName)
				if err != nil {
					continue
				}

				log.Printf("locationName: %s, currentCount: %d, requiredCount: %d", locationName, count, requiredCount)
				currentMarkerCounts[locationName] = count
				if count < int64(requiredCount) {
					canFire = false
					break
				}

				markersToPop[locationName] = int64(requiredCount)
			}

			// If this transition can fire, we need to pop the required markers and dispatch a task
			if canFire {
				markers := []Marker{}

				for locationName, requiredCount := range markersToPop {
					for idx := 0; idx < int(requiredCount); idx++ {
						marker, err := i.botStateManager.popMarker(i.workspace.Name, i.stub.ExternalId, sessionId, locationName)
						if err != nil {
							continue
						}

						markers = append(markers, *marker)
					}
				}

				taskPayload := &types.TaskPayload{
					Kwargs: map[string]interface{}{
						"markers":         markers,
						"session_id":      sessionId,
						"transition_name": transition.Name,
					},
				}

				_, err := i.taskDispatcher.SendAndExecute(i.ctx, string(types.ExecutorBot), i.authInfo, i.stub.ExternalId, taskPayload, types.TaskPolicy{
					MaxRetries: 0,
					Timeout:    3600,
					TTL:        3600,
					Expires:    time.Now().Add(time.Duration(3600) * time.Second),
				})
				if err != nil {
					log.Printf("error sending and executing task: %v", err)
				}
			}
		}
	}()
}

func (i *botInstance) run(transitionName, sessionId, taskId string) error {
	transitionConfig, ok := i.botConfig.Transitions[transitionName]
	if !ok {
		return errors.New("transition not found")
	}

	log.Printf("running transition: %s, sessionId: %s, taskId: %s", transitionName, sessionId, taskId)

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
		ContainerId: i.genContainerId(botContainerTypeTransition, sessionId),
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

func (i *botInstance) genContainerId(botContainerType, sessionId string) string {
	return fmt.Sprintf("%s-%s-%s-%s-%s", botContainerPrefix, botContainerType, i.stub.ExternalId, sessionId, uuid.New().String()[:8])
}
