package abstractions

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/scheduler"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog/log"
	"k8s.io/utils/ptr"
)

const (
	IgnoreScalingEventInterval      = 10 * time.Second
	containerEventChannelBufferSize = 1024
	scaleEventChannelBufferSize     = 1
)

type IAutoscaledInstance interface {
	ConsumeScaleResult(*AutoscalerResult)
	ConsumeContainerEvent(types.ContainerEvent)
	HandleScalingEvent(int) error
	Sync() error
}

type AutoscaledInstanceState struct {
	RunningContainers  int
	PendingContainers  int
	StoppingContainers int
	FailedContainers   []string
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
	EventRepo           repository.EventRepository
	TaskRepo            repository.TaskRepository
	UsageMetricsRepo    repository.UsageMetricsRepository
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
	ContainerRepo    repository.ContainerRepository
	BackendRepo      repository.BackendRepository
	TaskRepo         repository.TaskRepository
	EventRepo        repository.EventRepository
	UsageMetricsRepo repository.UsageMetricsRepository

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
	if cfg.Stub.Type == types.StubType(types.StubTypeSandbox) {
		failedContainerThreshold = 0
	}

	instance := &AutoscaledInstance{
		Lock:                     lock,
		InstanceLockKey:          cfg.InstanceLockKey,
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
		EventRepo:                cfg.EventRepo,
		UsageMetricsRepo:         cfg.UsageMetricsRepo,
		Containers:               make(map[string]bool),
		ContainerEventChan:       make(chan types.ContainerEvent, containerEventChannelBufferSize),
		ScaleEventChan:           make(chan int, scaleEventChannelBufferSize),
		StartContainersFunc:      cfg.StartContainersFunc,
		StopContainersFunc:       cfg.StopContainersFunc,
		FailedContainerThreshold: failedContainerThreshold,
	}

	instance.Sync()
	return instance, nil
}

func (i *AutoscaledInstance) WaitForContainer(ctx context.Context, duration time.Duration) (*types.ContainerState, error) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	timeout := time.After(duration)
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-i.Ctx.Done():
			return nil, errors.New("instance context done")
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
	minContainers := int(i.StubConfig.Autoscaler.MinContainers)
	if i.Stub.Type.IsServe() {
		minContainers = 0
	}

	if string(i.Stub.Type) == types.StubTypeTaskQueue {
		minContainers = 0
	}

	i.sendLatestScaleEvent(max(result.DesiredContainers, minContainers))
}

func (i *AutoscaledInstance) ConsumeContainerEvent(event types.ContainerEvent) {
	select {
	case i.ContainerEventChan <- event:
	case <-i.Ctx.Done():
	default:
		go i.sendContainerEvent(event)
	}
}

func (i *AutoscaledInstance) sendLatestScaleEvent(desiredContainers int) {
	select {
	case i.ScaleEventChan <- desiredContainers:
		return
	case <-i.Ctx.Done():
		return
	default:
	}

	select {
	case <-i.ScaleEventChan:
	default:
	}

	select {
	case i.ScaleEventChan <- desiredContainers:
	case <-i.Ctx.Done():
	default:
	}
}

func (i *AutoscaledInstance) sendContainerEvent(event types.ContainerEvent) {
	select {
	case i.ContainerEventChan <- event:
	case <-i.Ctx.Done():
	}
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
				i.logScaleEvent(initialContainerCount, len(i.Containers))
			}

		case desiredContainers := <-i.ScaleEventChan:
			// Ignore scaling events if we're in the ignore window
			if time.Now().Before(ignoreScalingEventWindow) {
				continue
			}

			if err := i.HandleScalingEvent(desiredContainers); err != nil {
				if _, ok := err.(*types.ThrottledByConcurrencyLimitError); ok {
					if time.Now().After(ignoreScalingEventWindow) {
						log.Info().Str("instance_name", i.Name).Msg("throttled by concurrency limit")
						ignoreScalingEventWindow = time.Now().Add(IgnoreScalingEventInterval)
					}
				}
				continue
			}

		}
	}
}

func (i *AutoscaledInstance) logScaleEvent(initialTrackedCount, currentTrackedCount int) {
	event := log.Info().
		Str("instance_name", i.Name).
		Int("initial_tracked_count", initialTrackedCount).
		Int("current_tracked_count", currentTrackedCount)

	state, err := i.State()
	if err != nil {
		event.Err(err).Msg("scaled")
		return
	}

	event.
		Int("running_count", state.RunningContainers).
		Int("pending_count", state.PendingContainers).
		Int("stopping_count", state.StoppingContainers).
		Int("failed_count", len(state.FailedContainers)).
		Int("active_count", state.RunningContainers+state.PendingContainers).
		Msg("scaled")
}

func (i *AutoscaledInstance) HandleScalingEvent(desiredContainers int) error {
	err := i.Lock.Acquire(context.Background(), i.InstanceLockKey, common.RedisLockOptions{TtlS: 10, Retries: 0})
	if err != nil {
		return err
	}
	defer i.Lock.Release(i.InstanceLockKey)

	state, err := i.State()
	if err != nil {
		return err
	}

	if i.FailedContainerThreshold > 0 && len(state.FailedContainers) >= i.FailedContainerThreshold {
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

	if len(state.FailedContainers) > 0 {
		go i.handleStubEvents(state.FailedContainers)
	}

	return err
}

func (i *AutoscaledInstance) CheckConcurrencyLimit() error {
	if i.Scheduler == nil || i.StubConfig == nil || i.Workspace == nil {
		return nil
	}

	gpuCount := i.StubConfig.Runtime.GpuCount
	if i.StubConfig.RequiresGPU() && gpuCount == 0 {
		gpuCount = 1
	}

	request := &types.ContainerRequest{
		Cpu:              i.StubConfig.Runtime.Cpu,
		GpuCount:         uint32(gpuCount),
		WorkspaceId:      i.Workspace.ExternalId,
		Workspace:        *i.Workspace,
		AllowMarketplace: i.StubConfig.AllowMarketplace,
	}
	if i.Stub != nil {
		request.StubId = i.Stub.ExternalId
		request.Stub = *i.Stub
	}

	return i.Scheduler.CheckConcurrencyLimit(request)
}

// Sync updates any persistent state that can be changed on the instance.
// If a stub has a deployment associated with it, we update the IsActive field.
func (i *AutoscaledInstance) Sync() error {
	if i.Stub.Type.IsDeployment() {
		deployments, err := i.BackendRepo.ListDeploymentsWithRelated(i.Ctx, types.DeploymentFilter{
			StubIds:     []string{i.Stub.ExternalId},
			WorkspaceID: i.Stub.Workspace.Id,
			ShowDeleted: true,
		})
		if err != nil || len(deployments) == 0 {
			return err
		}

		if len(deployments) == 1 {
			i.IsActive = deployments[0].Active
		}

		stubConfigRaw := deployments[0].Stub.Config
		stubConfig := &types.StubConfigV1{}
		if err := json.Unmarshal([]byte(stubConfigRaw), stubConfig); err != nil {
			return err
		}

		i.StubConfig = stubConfig
	}

	// We must make sure that the autoscaler is initialized at all costs
	if i.StubConfig.Autoscaler == nil {
		i.StubConfig.Autoscaler = &types.Autoscaler{}
		i.StubConfig.Autoscaler.Type = types.QueueDepthAutoscaler
		i.StubConfig.Autoscaler.MaxContainers = 1
		i.StubConfig.Autoscaler.TasksPerContainer = 1
	}

	return nil
}

func (i *AutoscaledInstance) State() (*AutoscaledInstanceState, error) {
	containers, err := i.ContainerRepo.GetActiveContainersByStubId(i.Stub.ExternalId)
	if err != nil {
		return nil, err
	}

	failedContainers, err := i.ContainerRepo.GetFailedContainersByStubId(i.Stub.ExternalId)
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

func (i *AutoscaledInstance) handleStubEvents(failedContainers []string) {
	if i.FailedContainerThreshold > 0 && len(failedContainers) >= i.FailedContainerThreshold {
		i.emitUnhealthyEvent(i.Stub.ExternalId, types.StubStateDegraded, "reached max failed container threshold", failedContainers)
	} else if len(failedContainers) > 0 {
		i.emitUnhealthyEvent(i.Stub.ExternalId, types.StubStateWarning, "one or more containers failed", failedContainers)
	}
}

func (i *AutoscaledInstance) emitUnhealthyEvent(stubId, currentState, reason string, containers []string) {
	var state string
	state, err := i.ContainerRepo.GetStubState(stubId)
	if err != nil {
		return
	}

	if state == currentState {
		return
	}

	err = i.ContainerRepo.SetStubState(stubId, currentState)
	if err != nil {
		return
	}

	log.Info().Str("instance_name", i.Name).Msgf("%s\n", reason)
	go i.EventRepo.PushStubStateUnhealthy(i.Workspace.ExternalId, stubId, currentState, state, reason, containers)
}

type InstanceController struct {
	ctx                 context.Context
	getOrCreateInstance func(ctx context.Context, stubId string, options ...func(IAutoscaledInstance)) (IAutoscaledInstance, error)
	getInstance         func(stubId string) (IAutoscaledInstance, bool)
	stubTypes           []string
	backendRepo         repository.BackendRepository
	containerRepo       repository.ContainerRepository
	redisClient         *common.RedisClient
	eventBus            *common.EventBus
}

func NewInstanceController(
	ctx context.Context,
	getOrCreateInstance func(ctx context.Context, stubId string, options ...func(IAutoscaledInstance)) (IAutoscaledInstance, error),
	getInstance func(stubId string) (IAutoscaledInstance, bool),
	stubTypes []string,
	backendRepo repository.BackendRepository,
	containerRepo repository.ContainerRepository,
	redisClient *common.RedisClient,
) *InstanceController {
	return &InstanceController{
		ctx:                 ctx,
		getOrCreateInstance: getOrCreateInstance,
		getInstance:         getInstance,
		stubTypes:           stubTypes,
		backendRepo:         backendRepo,
		containerRepo:       containerRepo,
		redisClient:         redisClient,
		eventBus:            common.NewEventBus(redisClient),
	}
}

func (c *InstanceController) Init() error {
	eventBus := common.NewEventBus(
		c.redisClient,
		common.EventBusSubscriber{Type: common.EventTypeReloadInstance, Callback: func(e *common.Event) bool {
			stubId := e.Args["stub_id"].(string)
			stubType := e.Args["stub_type"].(string)

			correctStub := false
			for _, t := range c.stubTypes {
				if t == stubType {
					correctStub = true
					break
				}
			}

			if !correctStub {
				return true
			}

			if err := c.Load(&types.DeploymentFilter{
				StubIds:     []string{stubId},
				StubType:    c.stubTypes,
				ShowDeleted: true,
			}); err != nil {
				return false
			}

			return true
		}},
	)
	c.eventBus = eventBus

	go c.eventBus.ReceiveEvents(c.ctx)

	// Load all instances matching the defined stub types
	if err := c.Load(nil); err != nil {
		return err
	}

	return nil
}

func (c *InstanceController) Warmup(
	ctx context.Context,
	stubId string,
) error {
	instance, err := c.getOrCreateInstance(ctx, stubId)
	if err != nil {
		return err
	}

	return instance.HandleScalingEvent(1)
}

func (c *InstanceController) Load(filter *types.DeploymentFilter) error {
	if filter == nil {
		filter = &types.DeploymentFilter{
			StubType:         c.stubTypes,
			MinContainersGTE: 1,
			Active:           ptr.To(true),
		}
	}

	stubs, err := c.backendRepo.ListDeploymentsWithRelated(
		c.ctx,
		*filter,
	)
	if err != nil {
		return err
	}

	for _, stub := range stubs {
		if !stub.Active {
			if err := c.deactivateInactiveDeployment(stub); err != nil {
				log.Error().Str("instance_name", stub.Stub.ExternalId).Err(err).Msg("unable to deactivate inactive deployment")
			}
			continue
		}

		instance, err := c.getOrCreateInstance(c.ctx, stub.Stub.ExternalId)
		if err != nil {
			log.Error().Str("instance_name", stub.Stub.ExternalId).Err(err).Msg("unable to get or create instance")
			continue
		}
		if err := instance.Sync(); err != nil {
			log.Error().Str("instance_name", stub.Stub.ExternalId).Err(err).Msg("unable to sync instance")
			continue
		}
		c.queuePostSyncScale(stub, instance)
	}

	return nil
}

func (c *InstanceController) queuePostSyncScale(stub types.DeploymentWithRelated, instance IAutoscaledInstance) {
	// Manual scale changes update autoscaler bounds; queue a scale tick now so
	// fixed replica counts converge without waiting for the next autoscaler sample.
	instance.ConsumeScaleResult(&AutoscalerResult{DesiredContainers: c.postSyncDesiredContainers(stub.Stub), ResultValid: true})
}

func (c *InstanceController) postSyncDesiredContainers(stub types.Stub) int {
	config, err := stub.UnmarshalConfig()
	if err != nil || config == nil || config.Autoscaler == nil || config.Autoscaler.MinContainers > 0 {
		return 0
	}
	if c.containerRepo == nil {
		return 0
	}

	containers, err := c.containerRepo.GetActiveContainersByStubId(stub.ExternalId)
	if err != nil {
		log.Error().Str("stub_id", stub.ExternalId).Err(err).Msg("unable to count active containers")
		return 0
	}

	count := 0
	for _, container := range containers {
		if container.Status == types.ContainerStatusRunning || container.Status == types.ContainerStatusPending {
			count++
		}
	}
	return count
}

func (c *InstanceController) deactivateInactiveDeployment(stub types.DeploymentWithRelated) error {
	instance, exists := c.getExistingInstance(stub.Stub.ExternalId)
	if !exists {
		hasContainers, err := c.hasActiveContainers(stub.Stub.ExternalId)
		if err != nil {
			return err
		}
		if !hasContainers {
			return nil
		}

		createdInstance, err := c.getOrCreateInstance(c.ctx, stub.Stub.ExternalId)
		if err != nil {
			return err
		}
		instance = createdInstance
	}

	if err := instance.Sync(); err != nil {
		return err
	}

	return instance.HandleScalingEvent(0)
}

func (c *InstanceController) getExistingInstance(stubId string) (IAutoscaledInstance, bool) {
	if c.getInstance == nil {
		return nil, false
	}

	return c.getInstance(stubId)
}

func (c *InstanceController) hasActiveContainers(stubId string) (bool, error) {
	if c.containerRepo == nil {
		return true, nil
	}

	containers, err := c.containerRepo.GetActiveContainersByStubId(stubId)
	if err != nil {
		return false, err
	}

	return len(containers) > 0, nil
}
