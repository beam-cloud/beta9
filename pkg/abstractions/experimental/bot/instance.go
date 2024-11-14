package bot

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	abstractions "github.com/beam-cloud/beta9/pkg/abstractions/common"
	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/repository"
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
	containerRepo   repository.ContainerRepository
	eventChan       chan *BotEvent
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
	ContainerRepo  repository.ContainerRepository
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

	instance := &botInstance{
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
		containerRepo: opts.ContainerRepo,
		eventChan:     make(chan *BotEvent),
	}

	go instance.monitorEvents()
	return instance, nil
}

func (i *botInstance) containersBySessionId() (map[string][]string, error) {
	containersBySessionId := make(map[string][]string)
	containers, err := i.containerRepo.GetActiveContainersByStubId(i.stub.ExternalId)
	if err != nil {
		return nil, err
	}

	for _, container := range containers {
		container, err := parseContainerId(container.ContainerId)
		if err != nil {
			continue
		}

		containersBySessionId[container.SessionId] = append(containersBySessionId[container.SessionId], container.ContainerId)
	}

	return containersBySessionId, nil
}

func (i *botInstance) Start() error {
	stepInterval := time.Duration(i.appConfig.Abstractions.Bot.StepIntervalS) * time.Second
	lastActiveSessionAt := time.Now().Unix()

	for {
		select {
		case <-i.ctx.Done():
			return nil
		default:
			containersBySessionId, err := i.containersBySessionId()
			if err != nil {
				continue
			}

			if len(containersBySessionId) > 0 {
				lastActiveSessionAt = time.Now().Unix()
			}

			activeSessions, err := i.botStateManager.getActiveSessions(i.workspace.Name, i.stub.ExternalId)
			if err != nil || len(activeSessions) == 0 {
				select {
				case <-i.ctx.Done():
					return nil
				case <-time.After(stepInterval):
					if time.Now().Unix()-lastActiveSessionAt > int64(i.appConfig.Abstractions.Bot.SessionInactivityTimeoutS) {
						log.Printf("<bot %s> No active sessions found, shutting down instance", i.stub.ExternalId)
						i.cancelFunc()
						return nil
					}

					continue
				}
			}

			lastActiveSessionAt = time.Now().Unix()
			for _, session := range activeSessions {
				if req, err := i.botStateManager.popInputMessage(i.workspace.Name, i.stub.ExternalId, session.Id); err == nil {
					if err := i.botInterface.SendPrompt(session.Id, PromptTypeUser, req); err != nil {
						continue
					}
				}

				// Run any network transitions that can run
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

			if len(transition.Inputs) == 0 {
				canFire = false
				continue
			}

			for locationName, requiredCount := range transition.Inputs {
				count, err := i.botStateManager.countMarkers(i.workspace.Name, i.stub.ExternalId, sessionId, locationName)
				if err != nil {
					continue
				}

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

				// If this transition requires confirmation, we need to send a confirmation request before creating and invoking the task
				if transition.Confirm {
					t, err := i.taskDispatcher.Send(i.ctx, string(types.ExecutorBot), i.authInfo, i.stub.ExternalId, taskPayload, getDefaultTaskPolicy())
					if err != nil {
						i.handleTransitionFailed(sessionId, transition.Name, err)
						continue
					}

					i.botStateManager.pushEvent(i.workspace.Name, i.stub.ExternalId, sessionId, &BotEvent{
						Type:  BotEventTypeConfirmRequest,
						Value: transition.Name,
						Metadata: map[string]string{
							string(MetadataSessionId):      sessionId,
							string(MetadataTransitionName): transition.Name,
							string(MetadataTaskId):         t.Metadata().TaskId,
						},
					})

					continue
				}

				t, err := i.taskDispatcher.SendAndExecute(i.ctx, string(types.ExecutorBot), i.authInfo, i.stub.ExternalId, taskPayload, getDefaultTaskPolicy())
				if err != nil {
					i.handleTransitionFailed(sessionId, transition.Name, err)
					continue
				}

				i.botStateManager.pushEvent(i.workspace.Name, i.stub.ExternalId, sessionId, &BotEvent{
					Type:  BotEventTypeTransitionFired,
					Value: transition.Name,
					Metadata: map[string]string{
						string(MetadataSessionId):      sessionId,
						string(MetadataTransitionName): transition.Name,
						string(MetadataTaskId):         t.Metadata().TaskId,
					},
				})
			}
		}
	}()
}

func (i *botInstance) handleTransitionFailed(sessionId, transitionName string, err error) {
	i.botStateManager.pushEvent(i.workspace.Name, i.stub.ExternalId, sessionId, &BotEvent{
		Type:  BotEventTypeTransitionFailed,
		Value: transitionName,
		Metadata: map[string]string{
			string(MetadataSessionId):      sessionId,
			string(MetadataTransitionName): transitionName,
			string(MetadataErrorMsg):       err.Error(),
		},
	})
}

func getDefaultTaskPolicy() types.TaskPolicy {
	return types.TaskPolicy{
		MaxRetries: 0,
		Timeout:    3600,
		TTL:        3600,
		Expires:    time.Now().Add(time.Duration(3600) * time.Second),
	}
}

func (i *botInstance) monitorEvents() error {
	for {
		select {
		case <-i.ctx.Done():
			return nil
		case event := <-i.eventChan:
			sessionId := event.Metadata[string(MetadataSessionId)]

			switch event.Type {
			case BotEventTypeUserMessage:
				i.botInterface.SendPrompt(sessionId, PromptTypeUser, &PromptRequest{Msg: event.Value})
			case BotEventTypeTransitionMessage:
				i.botInterface.SendPrompt(sessionId, PromptTypeTransition, &PromptRequest{Msg: event.Value})
			case BotEventTypeMemoryMessage:
				i.botInterface.SendPrompt(sessionId, PromptTypeMemory, &PromptRequest{Msg: event.Value})
			case BotEventTypeConfirmResponse:
				taskId := event.Metadata[string(MetadataTaskId)]
				accepts := event.Metadata[string(MetadataAccept)] == "true"

				task, err := i.taskDispatcher.Retrieve(i.ctx, i.workspace.Name, i.stub.ExternalId, taskId)
				if err != nil {
					continue
				}

				if accepts {
					err = task.Execute(i.ctx)
					if err != nil {
						i.handleTransitionFailed(sessionId, event.Metadata["transition_name"], err)
					}
				} else {
					task.Cancel(i.ctx, types.TaskRequestCancelled)
				}
			}
		}
	}
}

func (i *botInstance) run(transitionName, sessionId, taskId string) error {
	transitionConfig, ok := i.botConfig.Transitions[transitionName]
	if !ok {
		return errors.New("transition not found")
	}

	env := []string{
		fmt.Sprintf("BETA9_TOKEN=%s", i.token.Key),
		fmt.Sprintf("HANDLER=%s", transitionConfig.Handler),
		fmt.Sprintf("STUB_ID=%s", i.stub.ExternalId),
		fmt.Sprintf("STUB_TYPE=%s", i.stub.Type),
		fmt.Sprintf("PYTHON_VERSION=%s", transitionConfig.PythonVersion),
		fmt.Sprintf("CALLBACK_URL=%s", transitionConfig.CallbackUrl),
		fmt.Sprintf("TRANSITION_NAME=%s", transitionName),
		fmt.Sprintf("SESSION_ID=%s", sessionId),
		fmt.Sprintf("TASK_ID=%s", taskId),
	}

	mounts, err := abstractions.ConfigureContainerRequestMounts(
		i.stub.Object.ExternalId,
		i.authInfo.Workspace,
		*i.stubConfig,
		i.stub.ExternalId,
	)
	if err != nil {
		return err
	}

	gpuRequest := types.GpuTypesToStrings([]types.GpuType{})
	if transitionConfig.Gpu != "" {
		gpuRequest = append(gpuRequest, transitionConfig.Gpu.String())
	}

	gpuCount := uint32(0)
	if len(gpuRequest) > 0 {
		gpuCount = 1
	}

	log.Printf("<bot %s> Running transition %s", i.stub.ExternalId, transitionName)
	err = i.scheduler.Run(&types.ContainerRequest{
		ContainerId: i.genContainerId(botContainerTypeTransition, sessionId),
		Env:         env,
		Cpu:         transitionConfig.Cpu,
		Memory:      transitionConfig.Memory,
		Gpu:         string(transitionConfig.Gpu),
		GpuRequest:  gpuRequest,
		GpuCount:    gpuCount,
		ImageId:     transitionConfig.ImageId,
		StubId:      i.stub.ExternalId,
		WorkspaceId: i.workspace.ExternalId,
		Workspace:   *i.workspace,
		EntryPoint:  []string{transitionConfig.PythonVersion, "-m", "beta9.runner.bot.transition"},
		Mounts:      mounts,
		Stub:        *i.stub,
	})
	if err != nil {
		return err
	}

	return nil
}

func (i *botInstance) genContainerId(botContainerType, sessionId string) string {
	return fmt.Sprintf("%s-%s-%s-%s-%s", botContainerPrefix, botContainerType, i.stub.ExternalId, sessionId, uuid.New().String()[:8])
}
