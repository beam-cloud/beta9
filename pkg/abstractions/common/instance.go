package abstractions

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/scheduler"
	"github.com/beam-cloud/beta9/pkg/types"
	"go.opentelemetry.io/otel/attribute"
)

const IgnoreScalingEventInterval = 10 * time.Second

type IAutoscaledInstance interface {
	ConsumeScaleResult(*AutoscalerResult)
	ConsumeContainerEvent(types.ContainerEvent)
}

type AutoscaledInstanceState struct {
	RunningContainers  int
	PendingContainers  int
	StoppingContainers int
	FailedContainers   int
}

type AutoscaledInstanceConfig struct {
	Name                string
	AppConfig           types.AppConfig
	Workspace           *types.Workspace
	Stub                *types.StubWithRelated
	StubConfig          *types.StubConfigV1
	Object              *types.Object
	Token               *types.Token
	Scheduler           *scheduler.Scheduler
	Rdb                 *common.RedisClient
	InstanceLockKey     string
	ContainerRepo       repository.ContainerRepository
	BackendRepo         repository.BackendRepository
	TaskRepo            repository.TaskRepository
	StartContainersFunc func(containersToRun int) error
	StopContainersFunc  func(containersToStop int) error
}

type AutoscaledInstance struct {
	Ctx                      context.Context
	AppConfig                types.AppConfig
	CancelFunc               context.CancelFunc
	Name                     string
	Rdb                      *common.RedisClient
	Lock                     *common.RedisLock
	IsActive                 bool
	FailedContainerThreshold int

	// DB objects
	Workspace  *types.Workspace
	Stub       *types.StubWithRelated
	StubConfig *types.StubConfigV1
	Object     *types.Object
	Token      *types.Token

	// Scheduling
	Scheduler          *scheduler.Scheduler
	ContainerEventChan chan types.ContainerEvent
	Containers         map[string]bool
	ScaleEventChan     chan int
	EntryPoint         []string
	Autoscaler         IAutoscaler

	// Repositories
	ContainerRepo repository.ContainerRepository
	BackendRepo   repository.BackendRepository
	TaskRepo      repository.TaskRepository

	// Keys
	InstanceLockKey string

	// Callbacks
	StartContainersFunc func(containersToRun int) error
	StopContainersFunc  func(containersToStop int) error
}

func NewAutoscaledInstance(ctx context.Context, cfg *AutoscaledInstanceConfig) (*AutoscaledInstance, error) {
	ctx, cancelFunc := context.WithCancel(ctx)
	lock := common.NewRedisLock(cfg.Rdb)

	failedContainerThreshold := types.FailedContainerThreshold
	if cfg.Stub.Type.IsDeployment() {
		failedContainerThreshold = types.FailedDeploymentContainerThreshold
	}

	instance := &AutoscaledInstance{
		Lock:                     lock,
		Ctx:                      ctx,
		CancelFunc:               cancelFunc,
		IsActive:                 true,
		AppConfig:                cfg.AppConfig,
		Name:                     cfg.Name,
		Workspace:                cfg.Workspace,
		Stub:                     cfg.Stub,
		StubConfig:               cfg.StubConfig,
		Object:                   cfg.Object,
		Token:                    cfg.Token,
		Scheduler:                cfg.Scheduler,
		Rdb:                      cfg.Rdb,
		ContainerRepo:            cfg.ContainerRepo,
		BackendRepo:              cfg.BackendRepo,
		TaskRepo:                 cfg.TaskRepo,
		Containers:               make(map[string]bool),
		ContainerEventChan:       make(chan types.ContainerEvent, 1),
		ScaleEventChan:           make(chan int, 1),
		StartContainersFunc:      cfg.StartContainersFunc,
		StopContainersFunc:       cfg.StopContainersFunc,
		FailedContainerThreshold: failedContainerThreshold,
	}

	if instance.StubConfig.Autoscaler == nil {
		instance.StubConfig.Autoscaler = &types.Autoscaler{}
		instance.StubConfig.Autoscaler.Type = types.QueueDepthAutoscaler
		instance.StubConfig.Autoscaler.MaxContainers = 1
		instance.StubConfig.Autoscaler.TasksPerContainer = 1
	}

	return instance, nil
}

// Reload updates state that should be changed on the instance.
// If a stub has a deployment associated with it, we update the IsActive field.
func (i *AutoscaledInstance) Reload() error {
	deployments, err := i.BackendRepo.ListDeploymentsWithRelated(i.Ctx, types.DeploymentFilter{
		StubIds:     []string{i.Stub.ExternalId},
		WorkspaceID: i.Stub.Workspace.Id,
	})
	if err != nil || len(deployments) == 0 {
		return err
	}

	if len(deployments) == 1 && !deployments[0].Active {
		i.IsActive = false
	}

	return nil
}

func (i *AutoscaledInstance) WaitForContainer(ctx context.Context, duration time.Duration) (*types.ContainerState, error) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	timeout := time.After(duration)
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timeout:
			return nil, errors.New("timed out waiting for a container")
		case <-ticker.C:
			containers, err := i.ContainerRepo.GetActiveContainersByStubId(i.Stub.ExternalId)
			if err != nil {
				return nil, err
			}

			if len(containers) > 0 {
				return &containers[0], nil
			}
		}
	}
}

func (i *AutoscaledInstance) ConsumeScaleResult(result *AutoscalerResult) {
	i.ScaleEventChan <- result.DesiredContainers
}

func (i *AutoscaledInstance) ConsumeContainerEvent(event types.ContainerEvent) {
	i.ContainerEventChan <- event
}

func (i *AutoscaledInstance) Monitor() error {

	go i.Autoscaler.Start(i.Ctx) // Start the autoscaler
	ignoreScalingEventWindow := time.Now().Add(-IgnoreScalingEventInterval)

	for {
		select {

		case <-i.Ctx.Done():
			return nil

		case containerEvent := <-i.ContainerEventChan:
			initialContainerCount := len(i.Containers)

			_, exists := i.Containers[containerEvent.ContainerId]
			switch {
			case !exists && containerEvent.Change == 1: // Container created and doesn't exist in map
				i.Containers[containerEvent.ContainerId] = true
			case exists && containerEvent.Change == -1: // Container removed and exists in map
				delete(i.Containers, containerEvent.ContainerId)
			}

			if initialContainerCount != len(i.Containers) {
				log.Printf("<%s> scaled from %d->%d", i.Name, initialContainerCount, len(i.Containers))
			}

		case desiredContainers := <-i.ScaleEventChan:
			// Ignore scaling events if we're in the ignore window
			if time.Now().Before(ignoreScalingEventWindow) {
				continue
			}

			if err := i.HandleScalingEvent(desiredContainers); err != nil {
				if _, ok := err.(*types.ThrottledByConcurrencyLimitError); ok {
					if time.Now().After(ignoreScalingEventWindow) {
						log.Printf("<%s> throttled by concurrency limit", i.Name)
						ignoreScalingEventWindow = time.Now().Add(IgnoreScalingEventInterval)
					}
				}
				continue
			}
		}
	}
}

func (i *AutoscaledInstance) HandleScalingEvent(desiredContainers int) error {
	trace := common.TraceFunc(i.Ctx, "pkg/abstractions/common", "AutoscaledInstance.HandleScalingEvent",
		attribute.String("stub.id", i.Stub.ExternalId))
	defer trace.End()

	err := i.Lock.Acquire(i.Ctx, i.InstanceLockKey, common.RedisLockOptions{TtlS: 10, Retries: 0})
	if err != nil {
		return err
	}
	defer i.Lock.Release(i.InstanceLockKey)

	state, err := i.State()
	if err != nil {
		return err
	}

	if state.FailedContainers >= i.FailedContainerThreshold {
		log.Printf("<%s> reached failed container threshold, scaling to zero.\n", i.Name)
		desiredContainers = 0
	}

	if !i.IsActive {
		desiredContainers = 0
	}

	noContainersRunning := (state.PendingContainers == 0) && (state.RunningContainers == 0) && (state.StoppingContainers == 0)
	if desiredContainers == 0 && noContainersRunning {
		i.CancelFunc()
		return nil
	}

	containerDelta := desiredContainers - (state.RunningContainers + state.PendingContainers)
	if containerDelta > 0 {
		err = i.StartContainersFunc(containerDelta)
	} else if containerDelta < 0 {
		err = i.StopContainersFunc(-containerDelta)
	}

	return err
}

func (i *AutoscaledInstance) State() (*AutoscaledInstanceState, error) {
	trace := common.TraceFunc(i.Ctx, "pkg/abstractions/common", "AutoscaledInstance.State",
		attribute.String("stub.id", i.Stub.ExternalId))
	defer trace.End()

	containers, err := i.ContainerRepo.GetActiveContainersByStubId(i.Stub.ExternalId)
	if err != nil {
		return nil, err
	}

	failedContainers, err := i.ContainerRepo.GetFailedContainerCountByStubId(i.Stub.ExternalId)
	if err != nil {
		return nil, err
	}

	state := AutoscaledInstanceState{}
	for _, container := range containers {
		switch container.Status {
		case types.ContainerStatusRunning:
			state.RunningContainers++
		case types.ContainerStatusPending:
			state.PendingContainers++
		case types.ContainerStatusStopping:
			state.StoppingContainers++
		}
	}

	state.FailedContainers = failedContainers
	return &state, nil
}
