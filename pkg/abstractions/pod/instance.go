package pod

import (
	"context"
	"fmt"

	"math/rand"
	"sync"
	"time"

	abstractions "github.com/beam-cloud/beta9/pkg/abstractions/common"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/singleflight"
)

// errUnavailable is a user-facing reason no container can start yet: the stub's
// GPU has no supporting pool, or its failed containers hold it in cooldown.
type errUnavailable string

func (e errUnavailable) Error() string { return string(e) }

type podInstance struct {
	*abstractions.AutoscaledInstance
	buffer                    *PodProxyBuffer
	durableDiskPlacementRepos abstractions.DurableDiskPlacementRepos
	readyChecks               singleflight.Group
	statusChecks              containerStatusChecks
}

const (
	// containerStatusCheckWindow is how long the containers that had an
	// event are collected before their statuses are read together.
	containerStatusCheckWindow = 50 * time.Millisecond

	// containerStatusesPerRead bounds one read of those statuses.
	containerStatusesPerRead = 1000
)

// containerStatusChecks is the set of containers whose status is about to be
// read because their state changed.
type containerStatusChecks struct {
	mu      sync.Mutex
	pending map[string]struct{}
	running bool
}

// ConsumeContainerEvent hears of every write to the state of one of the
// stub's containers, and takes a container that is stopping out of the
// proxy's rotation. A container the proxy routes to is checked at once. The
// others only have to be kept from being discovered while they stop, and
// they are checked together a moment later: every sandbox being created or
// renewing its lease writes its state several times, and reading the state
// back once for each write, one round trip at a time, was most of what the
// gateway asked of Redis while a burst was being placed.
func (i *podInstance) ConsumeContainerEvent(event types.ContainerEvent) {
	i.AutoscaledInstance.ConsumeContainerEvent(event)
	// The container's state was deleted or expired, so its lock has no use.
	if event.Change < 0 {
		i.deleteKeepWarmLock(event.ContainerId)
	}
	if i.buffer == nil || i.ContainerRepo == nil || event.ContainerId == "" {
		return
	}
	if event.Change < 0 {
		i.buffer.retireAvailableContainer(event.ContainerId)
		return
	}

	if i.buffer.routesTo(event.ContainerId) || i.Ctx == nil {
		i.retireContainerIfStopping(event.ContainerId)
		return
	}
	i.checkContainerStatusSoon(event.ContainerId)
}

func (i *podInstance) retireContainerIfStopping(containerId string) {
	state, err := i.ContainerRepo.GetContainerState(containerId)
	if err != nil {
		i.buffer.signalDiscovery()
		return
	}
	if state.Status == types.ContainerStatusStopping {
		i.buffer.retireAvailableContainer(containerId)
	}
}

func (i *podInstance) checkContainerStatusSoon(containerId string) {
	checks := &i.statusChecks
	checks.mu.Lock()
	if checks.pending == nil {
		checks.pending = make(map[string]struct{})
	}
	checks.pending[containerId] = struct{}{}
	start := !checks.running
	checks.running = true
	checks.mu.Unlock()

	if start {
		go i.checkContainerStatuses()
	}
}

// checkContainerStatuses reads the statuses of the containers that had an
// event, a window at a time, until a window passes with none.
func (i *podInstance) checkContainerStatuses() {
	checks := &i.statusChecks
	for {
		select {
		case <-i.Ctx.Done():
			checks.mu.Lock()
			checks.pending = nil
			checks.running = false
			checks.mu.Unlock()
			return
		case <-time.After(containerStatusCheckWindow):
		}

		checks.mu.Lock()
		if len(checks.pending) == 0 {
			checks.running = false
			checks.mu.Unlock()
			return
		}
		containerIds := make([]string, 0, len(checks.pending))
		for containerId := range checks.pending {
			containerIds = append(containerIds, containerId)
		}
		checks.pending = make(map[string]struct{})
		checks.mu.Unlock()

		for len(containerIds) > 0 {
			read := containerIds[:min(len(containerIds), containerStatusesPerRead)]
			containerIds = containerIds[len(read):]
			i.retireStoppingContainers(read)
		}
	}
}

// retireStoppingContainers is retireContainerIfStopping for many containers
// in one round trip.
func (i *podInstance) retireStoppingContainers(containerIds []string) {
	reader, ok := i.ContainerRepo.(interface {
		GetContainerStatuses(containerIds []string) (map[string]types.ContainerStatus, error)
	})
	if !ok {
		for _, containerId := range containerIds {
			i.retireContainerIfStopping(containerId)
		}
		return
	}

	statuses, err := reader.GetContainerStatuses(containerIds)
	if err != nil {
		i.buffer.signalDiscovery()
		return
	}
	gone := false
	for _, containerId := range containerIds {
		status, found := statuses[containerId]
		switch {
		case !found:
			gone = true
		case status == types.ContainerStatusStopping:
			i.buffer.retireAvailableContainer(containerId)
		}
	}
	if gone {
		i.buffer.signalDiscovery()
	}
}

// ensureReadyForRequest starts a container when none is running or pending.
// Requests that arrive together share one check, so a cold burst costs one.
func (i *podInstance) ensureReadyForRequest() error {
	_, err, _ := i.readyChecks.Do("", func() (any, error) {
		return nil, i.ensureReady()
	})
	return err
}

func (i *podInstance) ensureReady() error {
	if reason := i.UnschedulableReason(); reason != "" {
		return errUnavailable(reason)
	}
	if i.StubConfig.IsPersistentVM() {
		return nil
	}

	state, err := i.State()
	if err != nil {
		return err
	}
	if state.RunningContainers+state.PendingContainers > 0 {
		return nil
	}
	// Scaling would only cancel this instance, and the next request would rebuild it.
	if i.FailedContainerThreshold > 0 && len(state.FailedContainers) >= i.FailedContainerThreshold {
		return errUnavailable("containers failed repeatedly; a new one starts after a cooldown")
	}
	if err := i.Sync(); err != nil {
		return err
	}

	desiredContainers := 1
	if i.Stub.Type == types.StubType(types.StubTypePodDeployment) {
		desiredContainers = desiredPodDeploymentContainers(
			i.StubConfig,
			1,
			i.AppConfig.GatewayService.StubLimits.MaxReplicas,
		)
	}
	if desiredContainers <= 0 {
		return nil
	}

	err = i.HandleScalingEvent(desiredContainers)
	if common.IsRedisLockNotObtained(err) {
		if i.Autoscaler != nil {
			i.Autoscaler.Trigger()
		}
		return nil
	}
	return err
}

func (i *podInstance) startContainers(containersToRun int) error {
	if i.StubConfig.IsPersistentVM() {
		return fmt.Errorf("persistent VM runtimes must be started by the VM service")
	}
	poolSelector := i.StubConfig.PoolSelector()
	if err := abstractions.ConfigureDurableDiskPlacement(i.Ctx, i.durableDiskPlacementRepos, i.Workspace, i.StubConfig); err != nil {
		return err
	}
	if poolSelector != i.StubConfig.PoolSelector() && i.Stub != nil && i.BackendRepo != nil {
		if err := i.BackendRepo.UpdateStubConfig(i.Ctx, i.Stub.Id, i.StubConfig); err != nil {
			return err
		}
	}

	secrets, err := abstractions.ConfigureContainerRequestSecrets(i.Ctx, i.BackendRepo, i.Workspace, *i.StubConfig, i.Stub.Type.IsSandbox())
	if err != nil {
		return err
	}

	env := []string{}
	env = append(i.StubConfig.Env, env...)
	env = append(secrets.Env, env...)
	if !i.Stub.Type.IsSandbox() {
		env = append(env, fmt.Sprintf("BETA9_TOKEN=%s", i.Token.Key))
	}
	env = append(env, []string{
		fmt.Sprintf("STUB_ID=%s", i.Stub.ExternalId),
		fmt.Sprintf("STUB_TYPE=%s", i.Stub.Type),
		fmt.Sprintf("KEEP_WARM_SECONDS=%d", i.StubConfig.KeepWarmSeconds),
	}...)

	gpuRequest := types.GpuTypesToStrings(i.StubConfig.Runtime.Gpus)
	if i.StubConfig.Runtime.Gpu != "" {
		gpuRequest = append(gpuRequest, i.StubConfig.Runtime.Gpu.String())
	}

	gpuCount := i.StubConfig.Runtime.GpuCount
	if i.StubConfig.RequiresGPU() && gpuCount == 0 {
		gpuCount = 1
	}

	checkpointEnabled := i.StubConfig.CheckpointEnabled
	if gpuCount > 1 {
		checkpointEnabled = false
	}

	ports := []uint32{}
	if len(i.StubConfig.Ports) > 0 {
		ports = i.StubConfig.Ports
	}

	for c := 0; c < containersToRun; c++ {
		if err := i.CheckConcurrencyLimit(); err != nil {
			return err
		}

		containerId := i.genContainerId()
		mounts, err := abstractions.ConfigureContainerRequestMounts(
			containerId,
			i.Stub,
			i.Workspace,
			*i.StubConfig,
		)
		if err != nil {
			return err
		}

		runRequest := &types.ContainerRequest{
			ContainerId:       containerId,
			Env:               env,
			SecretBindings:    secrets.Bindings,
			Cpu:               i.StubConfig.Runtime.Cpu,
			Memory:            i.StubConfig.Runtime.Memory,
			GpuRequest:        gpuRequest,
			GpuCount:          uint32(gpuCount),
			ImageId:           i.StubConfig.Runtime.ImageId,
			StubId:            i.Stub.ExternalId,
			AppId:             i.Stub.App.ExternalId,
			WorkspaceId:       i.Workspace.ExternalId,
			Workspace:         *i.Workspace,
			EntryPoint:        i.EntryPoint,
			Mounts:            mounts,
			Stub:              *i.Stub,
			CheckpointEnabled: checkpointEnabled,
			CheckpointTrigger: i.StubConfig.CheckpointTrigger,
			Ports:             ports,
			PoolSelector:      i.StubConfig.PoolSelector(),
			Hostname:          i.StubConfig.Hostname,
		}
		if err := abstractions.ConfigureContainerRequestNetwork(runRequest, *i.StubConfig); err != nil {
			return err
		}

		if i.StubConfig.KeepWarmSeconds != 0 {
			setPodKeepWarmLock(
				context.Background(),
				i.ContainerRepo,
				i.Workspace.Name,
				i.Stub.ExternalId,
				runRequest.ContainerId,
				i.StubConfig.KeepWarmSeconds,
			)
		}

		err = i.Scheduler.Run(runRequest)
		if err != nil {
			if i.StubConfig.KeepWarmSeconds != 0 {
				setPodKeepWarmLock(
					context.Background(),
					i.ContainerRepo,
					i.Workspace.Name,
					i.Stub.ExternalId,
					runRequest.ContainerId,
					0,
				)
			}
			log.Error().Str("instance_name", i.Name).Err(err).Msg("unable to run container")
			return err
		}

		continue
	}

	return nil
}

func (i *podInstance) stopContainers(containersToStop int) error {
	if i.StubConfig.IsPersistentVM() {
		return fmt.Errorf("persistent VM runtimes must be stopped by the VM service")
	}
	src := rand.NewSource(time.Now().UnixNano())
	rnd := rand.New(src)

	containerIds, err := i.stoppableContainers()
	if err != nil {
		return err
	}

	stopped := 0
	for stopped < containersToStop && len(containerIds) > 0 {
		idx := rnd.Intn(len(containerIds))
		containerId := containerIds[idx]
		containerIds = append(containerIds[:idx], containerIds[idx+1:]...)

		if !i.retireContainerForStop(containerId) {
			continue
		}

		err := i.Scheduler.Stop(&types.StopContainerArgs{ContainerId: containerId, Force: true, Reason: types.StopContainerReasonScheduler})
		if err != nil {
			if i.buffer != nil {
				i.buffer.cancelContainerRetirement(containerId)
			}
			log.Error().Str("instance_name", i.Name).Err(err).Msg("unable to stop container")
			return err
		}
		stopped++
	}

	return nil
}

func (i *podInstance) retireContainerForStop(containerId string) bool {
	if i.buffer == nil {
		return true
	}

	// Retire the proxy route before stopping the backend so a new request is
	// queued for a replacement instead of reusing an idle connection.
	i.buffer.retireAvailableContainer(containerId)
	if !i.IsActive {
		return true
	}
	if i.buffer.containerConnectionCount(containerId) > 0 {
		i.buffer.cancelContainerRetirement(containerId)
		return false
	}

	connections, err := i.buffer.sharedContainerConnectionCount(containerId)
	if err != nil || connections > 0 {
		i.buffer.cancelContainerRetirement(containerId)
		return false
	}

	return true
}

// stoppableNow lists the containers stopContainers would choose from, for a
// scaling event to learn that there are none before it takes the instance
// lock. A sandbox stub is asked every tick to stop its sandboxes, and until
// one outlives its keep-warm time there is nothing to stop.
func (i *podInstance) stoppableNow() ([]string, error) {
	if i.StubConfig.IsPersistentVM() {
		return nil, fmt.Errorf("persistent VM runtimes must be stopped by the VM service")
	}
	stoppable, read, err := i.readStoppableContainers()
	if err == nil && !read {
		err = fmt.Errorf("unable to read the keep warm locks or connections of stub %s", i.Stub.ExternalId)
	}
	return stoppable, err
}

func (i *podInstance) stoppableContainers() ([]string, error) {
	stoppable, _, err := i.readStoppableContainers()
	return stoppable, err
}

// readStoppableContainers also reports whether it read all it chooses by. A
// keep-warm lock or a connection count that cannot be read stops nothing,
// which is not the same as finding nothing to stop.
func (i *podInstance) readStoppableContainers() ([]string, bool, error) {
	// The scaling event that asks for this has just read the stub's
	// containers, and that read serves here too.
	containers, failedContainers, justRead := i.ContainersJustRead()
	if !justRead {
		var err error
		containers, err = i.ContainerRepo.GetActiveContainersByStubId(i.Stub.ExternalId)
		if err != nil {
			return nil, false, err
		}
	}
	stopAll := !i.IsActive
	if !stopAll && i.FailedContainerThreshold > 0 {
		if !justRead {
			var err error
			failedContainers, err = i.ContainerRepo.GetFailedContainersByStubId(i.Stub.ExternalId)
			if err != nil {
				return nil, false, err
			}
		}
		stopAll = len(failedContainers) >= i.FailedContainerThreshold
	}

	keys := make([]string, 0, len(containers))
	candidates := make([]string, 0, len(containers))
	for _, container := range containers {
		if container.Status == types.ContainerStatusStopping {
			continue
		}

		// Circuit breaking must cancel pending replacements and ignore keep-warm.
		if stopAll {
			keys = append(keys, container.ContainerId)
			continue
		}
		if container.Status == types.ContainerStatusPending {
			continue
		}

		if i.buffer != nil && i.buffer.pendingKeepWarmLockExists(container.ContainerId) {
			continue
		}

		if i.buffer != nil && i.buffer.containerConnectionCount(container.ContainerId) > 0 {
			continue
		}

		candidates = append(candidates, container.ContainerId)
	}
	if len(candidates) == 0 {
		return keys, true, nil
	}

	// The autoscaler asks this every second for a stub it wants smaller, which
	// a sandbox stub always is, so the locks and the connection counts of all
	// its containers are read together instead of one container at a time.
	// A read that fails stops nothing, as it did for the one container.
	keepWarm, err := i.ContainerRepo.PodKeepWarmLocksExist(context.TODO(), i.Workspace.Name, i.Stub.ExternalId, candidates)
	if err != nil {
		log.Error().Str("instance_name", i.Name).Err(err).Msg("error getting keep warm locks for containers")
		return keys, false, nil
	}
	unlocked := candidates[:0]
	for _, containerId := range candidates {
		// Skip containers with keep warm locks
		if !keepWarm[containerId] {
			unlocked = append(unlocked, containerId)
		}
	}

	connections, err := sharedPodContainersConnections(context.TODO(), i.Rdb, i.Workspace.Name, i.Stub.ExternalId, unlocked)
	if err != nil {
		log.Error().Str("instance_name", i.Name).Err(err).Msg("error getting connections for containers")
		return keys, false, nil
	}
	for _, containerId := range unlocked {
		if connections[containerId] > 0 {
			continue
		}
		keys = append(keys, containerId)
	}

	return keys, true, nil
}

func (i *podInstance) genContainerId() string {
	return fmt.Sprintf("%s-%s-%s", podContainerPrefix, i.Stub.ExternalId, uuid.New().String()[:8])
}
