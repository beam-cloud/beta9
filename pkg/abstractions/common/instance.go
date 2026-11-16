package abstractions

import (
	"context"
	"encoding/json"
	"errors"
	"math/rand"
	"sync"
	"sync/atomic"
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

	// Optional: see AutoscaledInstance.StoppableContainersFunc.
	StoppableContainersFunc func() ([]string, error)
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

	// StoppableContainersFunc lists the containers StopContainersFunc would
	// choose from right now. It is optional. With it, a scaling event that
	// asks for fewer containers while none may be stopped ends without the
	// instance lock; without it, such an event takes the lock to find out.
	StoppableContainersFunc func() ([]string, error)

	// Only Monitor reads and writes this.
	scaleStateLoggedAt time.Time

	// What State last read, and when the autoscaler last sampled.
	containersMu       sync.Mutex
	containers         stubContainers
	autoscalerTickedAt time.Time

	// How many of gatewayContainers are this instance's, and whether it has
	// arranged to take them back when it is cancelled.
	containersCounted     int
	uncountsWhenCancelled bool

	// settled is set by the scaling event of a sampled tick that changed
	// nothing, and cleared by the next sampled tick, by a scaling event that
	// may act, and by a container that comes or goes.
	settled bool

	// The part of the gateway's interval, from 0 to 1, by which this
	// instance takes its next sample early. Drawn at each sampled tick.
	autoscalerEarly float64
}

// stubContainers is one read of a stub's containers.
type stubContainers struct {
	active []types.ContainerState
	failed []string
	state  AutoscaledInstanceState
	readAt time.Time
}

// gatewayContainers is how many containers the instances of this process
// hold between them: for each one that has not been cancelled, the number
// its last read found. It paces the autoscalers of stubs that are holding
// still; see AutoscalerTickDue.
var gatewayContainers atomic.Int64

const (
	// containersJustReadFor is how long a read of the stub's containers
	// serves the step that follows it.
	containersJustReadFor = 250 * time.Millisecond

	// autoscalerContainersPerSecond is how many containers of one stub an
	// autoscaler samples a second before it samples less often, and how
	// many the autoscalers of the gateway's sandbox stubs that are holding
	// still sample a second between them.
	autoscalerContainersPerSecond = 2000

	// maxAutoscalerInterval bounds how seldom a stub is sampled, however
	// large it is and however much the gateway holds.
	maxAutoscalerInterval = 30 * time.Second
)

func NewAutoscaledInstance(ctx context.Context, cfg *AutoscaledInstanceConfig) (*AutoscaledInstance, error) {
	ctx, cancelFunc := context.WithCancel(ctx)
	lock := common.NewRedisLock(cfg.Rdb)

	failedContainerThreshold := types.FailedContainerThreshold
	if cfg.Stub.Type.IsDeployment() {
		failedContainerThreshold = types.FailedDeploymentContainerThreshold
	}
	if cfg.Stub.Type.IsSandbox() {
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
		StoppableContainersFunc:  cfg.StoppableContainersFunc,
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
	i.sendLatestScaleEvent(max(result.DesiredContainers, i.minContainers()))
}

// minContainers is how many containers the instance keeps running without traffic.
func (i *AutoscaledInstance) minContainers() int {
	if i.Stub.Type.IsServe() || string(i.Stub.Type) == types.StubTypeTaskQueue {
		return 0
	}
	if i.StubConfig == nil || i.StubConfig.Autoscaler == nil {
		return 0
	}
	return int(i.StubConfig.Autoscaler.MinContainers)
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
	persistentVM := i.StubConfig.IsPersistentVM()
	if !persistentVM {
		go i.Autoscaler.Start(i.Ctx)
	}

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

			// VM lifecycle owns scheduling; no scale-to-zero result will retire
			// its routing controller after the last runtime exits.
			if persistentVM && containerEvent.Change == -1 && len(i.Containers) == 0 {
				i.CancelFunc()
				return nil
			}

			if initialContainerCount != len(i.Containers) {
				// A container came or went, so the stub is sampled at its
				// own rate again until a tick finds nothing to do.
				i.markSettled(false)
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
				} else if _, ok := err.(*types.InsufficientCreditsError); ok {
					if time.Now().After(ignoreScalingEventWindow) {
						log.Info().Str("instance_name", i.Name).Str("reason", err.Error()).Msg("scale up blocked: insufficient credits")
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

	// The state is every container of the stub, read again. A stub that
	// gains or loses N containers together gets N of these lines, so reading
	// it for each one costs N²/2 container reads and holds up the events
	// behind it. The lines in between carry the tracked counts only, and
	// the read is as seldom as the autoscaler's sample.
	now := time.Now()
	if now.Sub(i.scaleStateLoggedAt) < autoscalerInterval(currentTrackedCount) {
		event.Msg("scaled")
		return
	}
	i.scaleStateLoggedAt = now

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
	// An event that starts nothing, stops nothing and keeps the instance
	// needs neither the lock nor a second read of the stub. One that may
	// act takes the lock and decides from the read it takes under it.
	failedContainers, unchanged := i.nothingToScale(desiredContainers)
	i.markSettled(unchanged)
	if unchanged {
		if len(failedContainers) > 0 {
			go i.handleStubEvents(failedContainers)
		}
		return nil
	}

	err := i.Lock.Acquire(context.Background(), i.InstanceLockKey, common.RedisLockOptions{TtlS: 10, Retries: 0})
	if err != nil {
		return err
	}
	defer i.Lock.Release(i.InstanceLockKey)

	state, err := i.State()
	if err != nil {
		return err
	}

	waitingForWork := desiredContainers > 0 && i.Stub != nil && i.Stub.Type.Kind() == types.StubTypeTaskQueue
	failing := i.FailedContainerThreshold > 0 && len(state.FailedContainers) >= i.FailedContainerThreshold
	if failing {
		desiredContainers = 0
		if err := i.ContainerRepo.SetContainerFailureCooldown(state.FailedContainers); err != nil {
			log.Warn().Err(err).Str("stub_id", i.Stub.ExternalId).Msg("failed to set container failure cooldown")
		}
	}

	if !i.IsActive {
		desiredContainers = 0
	}

	// Keep controllers with queued work or a minimum alive through cooldown;
	// neither receives fresh traffic to recreate a cancelled instance.
	retryAfterCooldown := failing && i.IsActive && (waitingForWork || i.minContainers() > 0)
	noContainersRunning := (state.PendingContainers == 0) && (state.RunningContainers == 0) && (state.StoppingContainers == 0)
	if desiredContainers == 0 && noContainersRunning && !retryAfterCooldown {
		i.CancelFunc()
		return nil
	}

	containerDelta := desiredContainers - (state.RunningContainers + state.PendingContainers)
	if containerDelta > 0 {
		// A writable durable disk has one writer, and a stopping container
		// stays that writer until its final snapshot is published. A
		// replacement started sooner restores the snapshot before that one.
		if i.StubConfig != nil && !durableDisksReadOnly(i.StubConfig.Disks) {
			containerDelta -= state.StoppingContainers
		}
		if containerDelta > 0 {
			err = i.StartContainersFunc(containerDelta)
		}
	} else if containerDelta < 0 {
		err = i.StopContainersFunc(-containerDelta)
	}

	if len(state.FailedContainers) > 0 {
		go i.handleStubEvents(state.FailedContainers)
	}

	return err
}

// nothingToScale reports whether a scaling event for desiredContainers would
// start no container, stop none and keep the instance, going by the read of
// the stub's containers taken a moment ago: the autoscaler's sample, when
// the event comes from its tick. It also returns the failed containers of
// that read, which such an event still reports.
//
// Nearly every tick of a stub that is holding still leads to such an event.
// A sandbox stub is always asked for fewer containers than it has, and all
// the event finds is that each sandbox is still within its keep-warm time.
// Taking the instance lock and reading every container a second time to
// find that cost more Redis commands than the sample did.
//
// The answer is no whenever the event may act or this cannot tell: without
// a read from a moment ago, when the instance is inactive, when it has no
// containers left, when it is asked for more than it has, and when it is
// asked for fewer and one may be stopped. It is also no when a container
// has failed and failures count towards stopping the stub. They do not for
// sandboxes, whose threshold is zero: there a failed container is only
// reported, and it stays among the stub's failed containers for an hour.
func (i *AutoscaledInstance) nothingToScale(desiredContainers int) ([]string, bool) {
	i.containersMu.Lock()
	read := i.containers
	i.containersMu.Unlock()
	if read.readAt.IsZero() || time.Since(read.readAt) > containersJustReadFor {
		return nil, false
	}

	state := read.state
	if !i.IsActive || (i.FailedContainerThreshold > 0 && len(state.FailedContainers) > 0) {
		return nil, false
	}
	if state.RunningContainers+state.PendingContainers+state.StoppingContainers == 0 {
		return nil, false
	}

	containerDelta := desiredContainers - (state.RunningContainers + state.PendingContainers)
	if containerDelta > 0 {
		return nil, false
	}
	if containerDelta < 0 {
		if i.StoppableContainersFunc == nil {
			return nil, false
		}
		stoppable, err := i.StoppableContainersFunc()
		if err != nil || len(stoppable) > 0 {
			return nil, false
		}
	}
	return state.FailedContainers, true
}

// markSettled records whether the stub is holding still: see settled.
func (i *AutoscaledInstance) markSettled(settled bool) {
	i.containersMu.Lock()
	i.settled = settled
	i.containersMu.Unlock()
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
		Cpu:         i.StubConfig.Runtime.Cpu,
		GpuCount:    uint32(gpuCount),
		WorkspaceId: i.Workspace.ExternalId,
		Workspace:   *i.Workspace,
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

		i.IsActive = false
		for _, deployment := range deployments {
			i.IsActive = i.IsActive || deployment.Active
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
	containers, failedContainers, err := i.ContainerRepo.GetContainersByStubId(i.Stub.ExternalId)
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

	i.containersMu.Lock()
	i.containers = stubContainers{active: containers, failed: failedContainers, state: state, readAt: time.Now()}
	i.countContainers(len(containers))
	i.containersMu.Unlock()

	return &state, nil
}

// countContainers keeps gatewayContainers in step with what this instance
// last read. An instance that is cancelled holds no containers, and the
// first count arranges for it to give them back when that happens. The
// caller holds containersMu.
func (i *AutoscaledInstance) countContainers(containers int) {
	if i.Ctx == nil || i.Ctx.Err() != nil {
		containers = 0
	}
	gatewayContainers.Add(int64(containers - i.containersCounted))
	i.containersCounted = containers

	if containers > 0 && !i.uncountsWhenCancelled {
		i.uncountsWhenCancelled = true
		context.AfterFunc(i.Ctx, func() {
			i.containersMu.Lock()
			defer i.containersMu.Unlock()
			i.countContainers(0)
		})
	}
}

// ContainersJustRead returns the stub's containers and its failed ones as
// State read them a moment ago, if it did. A scaling event reads the state
// and then picks the containers to stop from the same list, and reading it
// a second time costs a command for every container of the stub. The caller
// must not change what it is given.
func (i *AutoscaledInstance) ContainersJustRead() ([]types.ContainerState, []string, bool) {
	i.containersMu.Lock()
	defer i.containersMu.Unlock()

	if i.containers.readAt.IsZero() || time.Since(i.containers.readAt) > containersJustReadFor {
		return nil, nil, false
	}
	return i.containers.active, i.containers.failed, true
}

// AutoscalerTickDue reports whether the autoscaler should sample on this
// tick. A sample, and the scaling event it leads to, read every container of
// the stub, so sampling a large stub on every tick costs Redis several
// commands for each of its containers every second. A stub of up to
// autoscalerContainersPerSecond containers is sampled on every tick, as
// before; a larger one in proportion less often. A trigger is never held
// back, so a request still scales its stub at once.
//
// Many small stubs cost the same as one large one. So a sandbox stub that
// is holding still is sampled at the rate of a stub as large as everything
// this gateway holds: the same interval, with the same bound, from the
// containers of all its instances together. See holdingStill.
func (i *AutoscaledInstance) AutoscalerTickDue() bool {
	return i.autoscalerTickDue(time.Now())
}

func (i *AutoscaledInstance) autoscalerTickDue(now time.Time) bool {
	i.containersMu.Lock()
	defer i.containersMu.Unlock()

	interval := autoscalerInterval(len(i.containers.active))
	holdingStill := i.holdingStill()
	if holdingStill {
		shared := autoscalerInterval(int(gatewayContainers.Load()))
		shared -= time.Duration(i.autoscalerEarly * float64(shared))
		interval = max(interval, shared)
	}
	// Half a tick of slack, so the sample lands on the tick nearest the interval.
	if interval > sampleRate && now.Sub(i.autoscalerTickedAt) < interval-sampleRate/2 {
		return false
	}
	i.autoscalerTickedAt = now
	// The scaling event this sample leads to says whether the stub still
	// holds still. A sample that fails is tried again on the next tick.
	i.settled = false
	// Stubs that came due together would sample together from then on, and
	// they do come due together: all of them when the gateway starts, and
	// all those waiting when half its sandboxes are deleted. So a stub
	// that starts to hold still takes its first turn anywhere in the
	// gateway's interval, and one that already does takes each turn up to
	// a fifth early. Never late: the interval stays the longest wait.
	i.autoscalerEarly = rand.Float64()
	if holdingStill {
		i.autoscalerEarly /= 5
	}
	return true
}

// holdingStill reports whether nothing waits on this instance's next sample
// but a sandbox that outlives its keep-warm time, so that the sample can
// wait for its turn among all the stubs of the gateway. The caller holds
// containersMu.
//
// That is so for a stub of sandboxes or pod runs, whose containers their
// users start and the autoscaler only ever stops (desiredPodContainers
// never asks for more than there are), once a sampled tick has found
// nothing to do and while no container of it is starting or stopping. An
// endpoint, a task queue and a pod deployment start containers when a
// sample finds requests waiting, so they are sampled as before. So is a
// stub with failed containers that count towards stopping it: each of its
// scaling events goes to the lock, and none finds nothing to do.
func (i *AutoscaledInstance) holdingStill() bool {
	if !i.settled || i.Stub == nil {
		return false
	}
	if !i.Stub.Type.IsSandbox() && i.Stub.Type != types.StubType(types.StubTypePodRun) {
		return false
	}
	return i.containers.state.PendingContainers == 0 && i.containers.state.StoppingContainers == 0
}

// autoscalerInterval is the time between two samples of a stub with this
// many containers.
func autoscalerInterval(containers int) time.Duration {
	if containers <= autoscalerContainersPerSecond {
		return sampleRate
	}
	interval := time.Duration(float64(sampleRate) * float64(containers) / autoscalerContainersPerSecond)
	return min(interval, maxAutoscalerInterval)
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

	activeStubs := make(map[string]bool, len(stubs))
	for _, stub := range stubs {
		activeStubs[stub.Stub.ExternalId] = activeStubs[stub.Stub.ExternalId] || stub.Active
	}

	for _, stub := range stubs {
		if !stub.Active {
			// Rollbacks can leave inactive revisions sharing an active stub.
			if activeStubs[stub.Stub.ExternalId] {
				continue
			}
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
