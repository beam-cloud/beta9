package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	common "github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/metrics"
	"github.com/beam-cloud/beta9/pkg/runtime"
	"github.com/beam-cloud/beta9/pkg/storage"
	types "github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	clipCommon "github.com/beam-cloud/clip/pkg/common"
	"tags.cncf.io/container-device-interface/pkg/cdi"

	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"
)

const (
	baseConfigPath                    string = types.AgentTmpPath
	defaultContainerDirectory         string = types.WorkerUserCodeVolume
	specBaseName                      string = "config.json"
	initialSpecBaseName               string = "initial_config.json"
	containerInnerPort                int    = 8001 // Use a fixed port inside the container
	maxHostnameLength                 int    = 63   // RFC 1123 label limit
	markRunningRetryTimeout                  = 15 * time.Second
	markRunningRetryInterval                 = 100 * time.Millisecond
	runtimeDeleteTimeout                     = 30 * time.Second
	containerRepositoryRetryTimeout          = 5 * time.Second
	containerRepositoryAttemptTimeout        = time.Second
	containerRepositoryRetryInterval         = 100 * time.Millisecond
	containerRuntimeStateTimeout             = 2 * time.Second
	observedStoppingSignalTimeout            = 5 * time.Second
	cpuQuotaApplyTimeout                     = 2 * time.Second
	runnerReadyTimeout                       = 30 * time.Second
	runnerReadyPollInterval                  = 10 * time.Millisecond
	restoredContainerPollInterval            = 500 * time.Millisecond
)

const (
	hostResolvConfPath   = "/etc/resolv.conf"
	workerResolvConfPath = "/workspace/etc/resolv.conf"
)

type containerResourceUpdater interface {
	UpdateResources(ctx context.Context, containerID string, resources *specs.LinuxResources) error
}

func containerResolvConfSource(useHostResolvConf bool, hostPath string) string {
	if useHostResolvConf && resolvConfHasUsableNameserver(hostPath) {
		return hostPath
	}
	return workerResolvConfPath
}

func resolvConfHasUsableNameserver(path string) bool {
	data, err := os.ReadFile(path)
	if err != nil {
		return false
	}
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 || fields[0] != "nameserver" {
			continue
		}
		ip := net.ParseIP(fields[1])
		if ip != nil && !ip.IsLoopback() && !ip.IsUnspecified() {
			return true
		}
	}
	return false
}

// handleStopContainerArgs queues a stop for a locally owned container.
func (s *Worker) handleStopContainerArgs(stopArgs types.StopContainerArgs, source types.EventSource) bool {
	reason := types.StopContainerReason(types.NormalizeEventReason(string(stopArgs.Reason)))
	if stopArgs.Reason == "" {
		stopArgs.Reason = reason
	}

	if containerInstance, exists := s.containerInstances.Get(stopArgs.ContainerId); exists {
		log.Info().Str("container_id", stopArgs.ContainerId).Msg("received stop container event")
		containerInstance.setStopReason(reason)
		s.containerInstances.Set(stopArgs.ContainerId, containerInstance)
		s.recordContainerEvent(context.Background(), containerInstance.Request, types.EventContainerEventSchema{
			ID:          types.ContainerEventWorkerStopEventReceived,
			ContainerID: stopArgs.ContainerId,
			Reason:      string(reason),
			Source:      source.String(),
			Message:     types.EventMessageWorkerStopEventReceived.String(),
			Attrs: map[string]string{
				types.EventAttrForce: fmt.Sprintf("%t", stopArgs.Force),
			},
		})
		s.cancelContainer(stopArgs.ContainerId)
		s.stopContainerChan <- stopContainerEvent{ContainerId: stopArgs.ContainerId, Kill: stopArgs.Force}
	}

	return true
}

// stopContainer stops a container. When force is true, a SIGKILL signal is sent to the container.
func (s *Worker) stopContainer(containerId string, kill bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), observedStoppingSignalTimeout)
	defer cancel()
	return s.stopContainerWithContext(ctx, containerId, kill)
}

func (s *Worker) stopContainerWithContext(ctx context.Context, containerId string, kill bool) error {
	return s.stopContainerWithCheckpointDeferral(ctx, containerId, kill, true)
}

func (s *Worker) stopContainerWithoutCheckpointDeferral(ctx context.Context, containerId string, kill bool) error {
	return s.stopContainerWithCheckpointDeferral(ctx, containerId, kill, false)
}

func (s *Worker) stopContainerWithCheckpointDeferral(ctx context.Context, containerId string, kill, allowCheckpointDeferral bool) error {
	instance, exists := s.containerInstances.Get(containerId)
	if !exists {
		log.Info().Str("container_id", containerId).Msg("container not found")
		return nil
	}
	if allowCheckpointDeferral && s.deferStopForCheckpoint(instance, kill) {
		return nil
	}

	log.Info().Str("container_id", containerId).Msg("stopping container")

	signal := syscall.SIGTERM
	if kill {
		signal = syscall.SIGKILL
	}

	s.stopDockerSandbox(containerId, instance, kill)

	// Use the runtime that was selected for this container
	rt := instance.Runtime
	if rt == nil {
		rt = s.runtime
	}
	if rt == nil {
		return errors.New("container runtime is unavailable")
	}

	// Kill the container - this will cause the runtime.Run() call to exit
	// which triggers all the defers (overlay cleanup, mount cleanup, etc.)
	err := rt.Kill(ctx, instance.Id, signal, &runtime.KillOpts{All: true})
	if err != nil {
		log.Debug().Str("container_id", containerId).Err(err).Msg("error killing container (may already be stopped)")
		return err
	}

	log.Info().Str("container_id", containerId).Msg("container stop signal sent")
	return nil
}

// handleObservedStoppingContainer turns an authoritative persisted STOPPING
// state into one graceful stop attempt followed by one bounded force escalation.
// Heartbeat and event reconciliation share this path so neither can cancel the
// status heartbeat without also stopping the runtime.
func (s *Worker) handleObservedStoppingContainer(containerID string, source types.EventSource) {
	s.handleObservedContainerStop(containerID, source, false)
}

func (s *Worker) handleObservedOrphanedContainer(containerID string, source types.EventSource) {
	s.handleObservedContainerStop(containerID, source, true)
}

func (s *Worker) handleObservedContainerStop(containerID string, source types.EventSource, forceImmediately bool) {
	if s.containerInstances == nil {
		return
	}
	instance, exists := s.containerInstances.Get(containerID)
	if !exists || instance == nil {
		return
	}
	exitCode, stopReason := instance.lifecycleState()
	if exitCode >= 0 {
		return
	}
	if stopReason == "" {
		instance.setStopReason(types.StopContainerReasonUnknown)
		s.containerInstances.Set(containerID, instance)
	}
	s.cancelContainer(containerID)
	if instance.Runtime == nil && s.runtime == nil {
		return
	}
	if !instance.StopEscalationStarted.CompareAndSwap(false, true) {
		return
	}

	go s.stopObservedContainer(containerID, instance.Request, source, forceImmediately)
}

func (s *Worker) stopObservedContainer(containerID string, request *types.ContainerRequest, source types.EventSource, forceImmediately bool) {
	needsStop := forceImmediately
	var stopSignalErr error
	if !forceImmediately {
		graceStarted := time.Now()
		stopCtx, cancelStop := context.WithTimeout(context.Background(), observedStoppingSignalTimeout)
		stopSignalErr = s.stopContainerWithContext(stopCtx, containerID, false)
		cancelStop()
		if stopSignalErr != nil && !runtimeContainerNotFound(stopSignalErr) {
			log.Warn().Str("container_id", containerID).Err(stopSignalErr).Msg("failed to send graceful stop for persisted STOPPING container")
		}

		grace := time.Duration(s.config.Worker.TerminationGracePeriod) * time.Second
		if remaining := grace - time.Since(graceStarted); remaining > 0 {
			timer := time.NewTimer(remaining)
			defer timer.Stop()
			workerCtx := s.ctx
			if workerCtx == nil {
				workerCtx = context.Background()
			}
			select {
			case <-timer.C:
			case <-workerCtx.Done():
				return
			}
		}

		instance, rt, exists := s.observedContainerRuntime(containerID)
		if !exists {
			return
		}
		stateCtx, cancelState := context.WithTimeout(context.Background(), containerRuntimeStateTimeout)
		var runtimeAbsent bool
		needsStop, runtimeAbsent = runtimeNeedsStop(stateCtx, rt, containerID)
		cancelState()
		if runtimeAbsent && stopSignalErr != nil {
			// A runtime can report its PID before `state`/`kill` can find the
			// sandbox. Keep this STOPPING transition retryable even when startup
			// was already signalled.
			instance.StopEscalationStarted.Store(false)
			runtimeStarted, _ := instance.runtimeStartState()
			if !runtimeStarted {
				s.scheduleStuckWorkspaceMountRecovery(instance, request, stuckContainerAbortDelay)
			}
			return
		}
		if needsStop {
			log.Info().Str("container_id", containerID).Int64("grace_period_seconds", s.config.Worker.TerminationGracePeriod).Msg("container still running after stop event")
			_, stopReason := instance.lifecycleState()
			s.recordContainerEvent(context.Background(), request, types.EventContainerEventSchema{
				ID:          types.ContainerEventWorkerStoppingGraceKill,
				ContainerID: containerID,
				Reason:      string(stopReason),
				Source:      source.String(),
				Message:     types.EventMessageStoppingGraceKill.String(),
				Attrs: map[string]string{
					types.EventAttrGracePeriodSeconds: fmt.Sprintf("%d", s.config.Worker.TerminationGracePeriod),
				},
			})
		}
	}

	if needsStop {
		killCtx, cancelKill := context.WithTimeout(context.Background(), observedStoppingSignalTimeout)
		stopSignalErr = s.stopContainerWithContext(killCtx, containerID, true)
		cancelKill()
		if stopSignalErr != nil && !runtimeContainerNotFound(stopSignalErr) {
			log.Warn().Str("container_id", containerID).Err(stopSignalErr).Msg("failed to force-stop persisted STOPPING container")
		}

		instance, rt, exists := s.observedContainerRuntime(containerID)
		if !exists {
			return
		}
		verifyCtx, cancelVerify := context.WithTimeout(context.Background(), containerRuntimeStateTimeout)
		stillRunning, runtimeAbsent := runtimeNeedsStop(verifyCtx, rt, containerID)
		cancelVerify()
		if stillRunning || (runtimeAbsent && stopSignalErr != nil) {
			instance.StopEscalationStarted.Store(false)
			log.Warn().Str("container_id", containerID).Msg("container termination is not confirmed; allowing the next status heartbeat to retry")
			runtimeStarted, _ := instance.runtimeStartState()
			if !runtimeStarted {
				s.scheduleStuckWorkspaceMountRecovery(instance, request, stuckContainerAbortDelay)
			}
			return
		}
	}

	// A stop can be observed after spawn is handed off but before the runtime
	// exists. A stopped probe before the start signal is not terminal proof in
	// that window; leave the heartbeat free to retry after startup.
	if instance, _, exists := s.observedContainerRuntime(containerID); exists {
		runtimeStarted, _ := instance.runtimeStartState()
		if !runtimeStarted {
			instance.StopEscalationStarted.Store(false)
			s.scheduleStuckWorkspaceMountRecovery(instance, request, stuckContainerAbortDelay)
			return
		}
	}

	if _, exists := s.containerInstances.Get(containerID); !exists {
		return
	}
	workerCtx := s.ctx
	if workerCtx == nil {
		workerCtx = context.Background()
	}
	select {
	case <-time.After(stuckContainerAbortDelay):
	case <-workerCtx.Done():
		return
	}
	s.abortStuckWorkspaceMount(request)
}

func (s *Worker) scheduleStuckWorkspaceMountRecovery(instance *ContainerInstance, request *types.ContainerRequest, delay time.Duration) {
	if instance == nil || request == nil || s.storageManager == nil || s.storageManager.poolConfig.StorageMode != storage.StorageModeGeese {
		return
	}
	if !instance.StuckMountRecoveryStarted.CompareAndSwap(false, true) {
		return
	}

	go func() {
		defer instance.StuckMountRecoveryStarted.Store(false)
		timer := time.NewTimer(delay)
		defer timer.Stop()
		workerCtx := s.ctx
		if workerCtx == nil {
			workerCtx = context.Background()
		}
		select {
		case <-timer.C:
			s.abortStuckWorkspaceMount(request)
		case <-workerCtx.Done():
		}
	}()
}

func (s *Worker) observedContainerRuntime(containerID string) (*ContainerInstance, runtime.Runtime, bool) {
	instance, exists := s.containerInstances.Get(containerID)
	if !exists || instance == nil {
		return nil, nil, false
	}
	rt := instance.Runtime
	if rt == nil {
		rt = s.runtime
	}
	return instance, rt, true
}

func (s *Worker) finalizeContainer(containerId string, request *types.ContainerRequest, exitCode int, exitReported bool) {
	if exitCode < 0 {
		exitCode = 1
	}

	s.clearContainer(containerId, request, exitCode, exitReported)
}

func (s *Worker) clearContainer(containerId string, request *types.ContainerRequest, exitCode int, exitReported bool) {
	hasDurableDisk := request != nil && request.HasDurableDiskMount()
	instance, exists := s.containerInstances.Get(containerId)
	if exists {
		if request != nil && request.Stub.Type.Kind() == types.StubTypeSandbox {
			instance.signalProcessManagerReadiness(false)
		}
		s.containerInstances.Set(containerId, instance)
		instance.statusHeartbeatMu.Lock()
	}

	if hasDurableDisk {
		// Publish the long lease while the heartbeat fence prevents a stale overwrite.
		s.markContainerStopping(containerId, types.ContainerStateTtlSWhileStopping)
	}
	s.setLocalContainerExitCode(containerId, exitCode)
	if exists {
		instance.statusHeartbeatMu.Unlock()
	}

	if hasDurableDisk {
		exitCode, exitReported = s.finalizeDurableDiskMounts(containerId, request, exitCode, exitReported)
	}
	if containerId != "" {
		_ = os.RemoveAll(filepath.Join(baseConfigPath, containerId))
	}
	if !exitReported {
		s.setContainerExitCode(containerId, exitCode)
	}

	s.containerLock.Lock()
	if instance, exists := s.containerInstances.Get(containerId); exists {
		instance.CPUSet = ""
		instance.RestoreCPUAffinityDeferred = false
		s.containerInstances.Set(containerId, instance)
	}

	// De-allocate GPU devices so they are available for new containers
	if request.Gpu != "" {
		s.containerGPUManager.UnassignGPUDevices(containerId)
	}

	// Tear down container network components - best effort
	if err := s.containerNetworkManager.TearDown(request.ContainerId); err != nil {
		log.Warn().Str("container_id", request.ContainerId).Err(err).Msg("failed to clean up container network")
	}

	// Clean up upload directory
	os.RemoveAll(filepath.Join(types.WorkerContainerUploadsHostPath, containerId))

	s.containerLock.Unlock()

	s.markContainerStopping(containerId, types.ContainerStateTtlS)

	s.finishContainerShutdown(containerId, request)
}

func (s *Worker) finishContainerShutdown(containerId string, request *types.ContainerRequest) {
	instance, exists := s.containerInstances.Get(containerId)
	if exists {
		rt := instance.Runtime
		if rt == nil {
			rt = s.runtime
		}
		stateCtx, cancel := context.WithTimeout(context.Background(), containerRuntimeStateTimeout)
		needsGrace := runtimeNeedsGraceKill(stateCtx, rt, containerId)
		cancel()
		if needsGrace {
			var workerDone <-chan struct{}
			if s.ctx != nil {
				workerDone = s.ctx.Done()
			}
			timer := time.NewTimer(time.Duration(s.config.Worker.TerminationGracePeriod) * time.Second)
			select {
			case <-timer.C:
			case <-workerDone:
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
			}

			stateCtx, cancel = context.WithTimeout(context.Background(), containerRuntimeStateTimeout)
			needsStop := runtimeNeedsGraceKill(stateCtx, rt, containerId)
			cancel()
			if needsStop {
				if err := s.stopContainer(containerId, true); err != nil {
					log.Error().Str("container_id", containerId).Err(err).Msg("failed to stop container")
				}
			}
		}
		instance.stopOOMWatcher()
	}

	s.deleteContainer(containerId)
	if request != nil && s.completedRequests != nil {
		var workerDone <-chan struct{}
		if s.ctx != nil {
			workerDone = s.ctx.Done()
		}
		select {
		case s.completedRequests <- request:
		case <-workerDone:
		}
	}
	log.Info().Str("container_id", containerId).Msg("finalized container shutdown")
}

func (s *Worker) setLocalContainerExitCode(containerId string, exitCode int) {
	instance, exists := s.containerInstances.Get(containerId)
	if !exists {
		return
	}
	instance.setExitCode(exitCode)
	s.containerInstances.Set(containerId, instance)
}

func (s *Worker) setContainerExitCode(containerId string, exitCode int) bool {
	if s.containerRepoClient == nil {
		return false
	}

	err := retryContainerRepositoryCall(func(ctx context.Context) error {
		_, err := handleGRPCResponse(s.containerRepoClient.SetContainerExitCode(ctx, &pb.SetContainerExitCodeRequest{
			ContainerId: containerId,
			ExitCode:    int32(exitCode),
		}))
		if (&types.ErrContainerStateNotFound{}).From(err) {
			return nil
		}
		return err
	})
	if err != nil {
		log.Error().Str("container_id", containerId).Err(err).Msg("failed to set exit code")
		return false
	}
	return true
}

func (s *Worker) markContainerStopping(containerId string, expirySeconds int64) bool {
	if s.containerRepoClient == nil {
		return true
	}

	err := retryContainerRepositoryCall(func(ctx context.Context) error {
		_, err := handleGRPCResponse(s.containerRepoClient.UpdateContainerStatus(ctx, &pb.UpdateContainerStatusRequest{
			ContainerId:   containerId,
			Status:        string(types.ContainerStatusStopping),
			ExpirySeconds: expirySeconds,
		}))
		if (&types.ErrContainerStateNotFound{}).From(err) {
			return nil
		}
		return err
	})
	if err != nil {
		log.Debug().Str("container_id", containerId).Err(err).Msg("failed to mark container stopping during shutdown")
		return false
	}
	return true
}

func (s *Worker) deleteContainer(containerId string) {
	if instance, exists := s.containerInstances.Get(containerId); exists && instance.SandboxProcessManager != nil {
		if err := instance.SandboxProcessManager.Cleanup(); err != nil {
			log.Debug().Str("container_id", containerId).Err(err).Msg("failed to cleanup sandbox process manager client")
		}
	}

	if err := s.deleteContainerState(containerId); err != nil {
		log.Debug().Str("container_id", containerId).Err(err).Msg("failed to remove remote container state")
	}
	s.containerInstances.Delete(containerId)
}

func (s *Worker) deleteContainerState(containerId string) error {
	if s.containerRepoClient == nil {
		return nil
	}

	return retryContainerRepositoryCall(func(ctx context.Context) error {
		_, err := handleGRPCResponse(s.containerRepoClient.DeleteContainerState(ctx, &pb.DeleteContainerStateRequest{ContainerId: containerId}))
		if (&types.ErrContainerStateNotFound{}).From(err) {
			return nil
		}
		return err
	})
}

func retryContainerRepositoryCall(call func(context.Context) error) error {
	retryCtx, cancel := context.WithTimeout(context.Background(), containerRepositoryRetryTimeout)
	defer cancel()
	var lastErr error
	for {
		attemptCtx, attemptCancel := context.WithTimeout(retryCtx, containerRepositoryAttemptTimeout)
		err := call(attemptCtx)
		attemptCancel()
		if err == nil {
			return nil
		}
		lastErr = err
		if errors.Is(err, context.Canceled) {
			return err
		}
		if err := waitForRetry(retryCtx, containerRepositoryRetryInterval); err != nil {
			return lastErr
		}
	}
}

// Spawn a single container and stream output to stdout/stderr
func (s *Worker) RunContainer(ctx context.Context, request *types.ContainerRequest) error {
	containerId := request.ContainerId
	startupStartedAt := time.Now()

	caps := s.runtime.Capabilities()

	if request.RequiresGPU() && !caps.GPU {
		return fmt.Errorf("runtime %s does not support GPU workloads", s.runtime.Name())
	}
	if request.DockerEnabled && forcedRuncCheckpointProfileRequired(request, s.runtime) {
		return errors.New("forced runc containers do not support Docker-enabled mode")
	}
	if err := validateCheckpointRestoreRuntime(request, s.runtime); err != nil {
		return err
	}

	instance, exists := s.containerInstances.Get(containerId)
	if !exists {
		instance = &ContainerInstance{
			Id:        containerId,
			StubId:    request.StubId,
			LogBuffer: common.NewLogBuffer(),
			Request:   request,
			Runtime:   s.runtime,
		}
		if request.Stub.Type.Kind() == types.StubTypeSandbox {
			instance.initializeProcessManagerReadiness()
		}
	}
	s.containerInstances.Set(containerId, instance)

	bundlePath := filepath.Join(s.imageMountPath, request.ImageId)

	startup, startupCtx := errgroup.WithContext(ctx)
	addressRequest := request.Clone()
	startup.Go(func() error { return s.setWorkerAddress(startupCtx, addressRequest) })

	logChan := make(chan common.LogRecord, 1000)
	outputLogger := slog.New(common.NewChannelHandler(logChan))
	logCaptureClosed := false
	defer func() {
		if !logCaptureClosed {
			outputLogger.Info("", "done", true, "success", false)
		}
	}()

	// Handle stdout/stderr
	go s.containerLogger.CaptureLogs(request, logChan)

	// Gate checkpointing based on runtime capabilities, and let the user know if it was disabled
	if request.CheckpointEnabled && !caps.CheckpointRestore {
		log.Info().Str("container_id", containerId).
			Str("runtime", s.runtime.Name()).
			Msg("disabling checkpoint for runtime without CRIU support")
		outputLogger.Info("Checkpointing is enabled for this container, but the runtime on this worker does not support checkpoint/restore - disabling checkpointing\n")

		request.CheckpointEnabled = false
		if request.Checkpoint == nil || !request.Checkpoint.IsFilesystemOnly() {
			request.Checkpoint = nil
		}
	}

	var filesystemRestore *checkpointFilesystemRestore
	if s.canRestoreCheckpoint(request, s.runtime) {
		filesystemRestore = s.startCheckpointFilesystemRestore(request, outputLogger)
	}
	filesystemRestoreHandedOff := false
	defer func() {
		if !filesystemRestoreHandedOff {
			filesystemRestore.cleanup()
		}
	}()

	var imageLoaded bool
	startup.Go(func() error {
		var err error
		_, imageLoaded, err = s.loadContainerImage(startupCtx, request, outputLogger)
		return err
	})
	if s.containerMountManager.RequiresWorkspaceStorageMount(request) {
		startup.Go(func() error { return s.mountWorkspaceStorage(startupCtx, request) })
	}
	if err := startup.Wait(); err != nil {
		return err
	}
	if !imageLoaded {
		return nil
	}

	// Clip v2 build short-circuit: For v2 builds, the image was already built via buildah
	// (see buildOrPullBaseImage) and indexed as a .clip archive. We don't need to run a
	// runc container or execute any commands inside it. Mark the build as successful and exit.
	if request.IsBuildRequest() && s.config.ImageService.ClipVersion == uint32(types.ClipVersion2) {
		exitCode := 0
		exitReported := false
		outputLogger.Info("", "done", true, "success", true)
		s.containerWg.Add(1)
		go func() {
			defer s.containerWg.Done()
			s.finalizeContainer(containerId, request, exitCode, exitReported)
		}()
		logCaptureClosed = true
		metrics.RecordWorkerStartupLatency(time.Since(startupStartedAt), request)
		return nil
	}

	phaseStart := time.Now()
	requestedPorts := append([]uint32(nil), request.Ports...)
	request.Ports = portsForRequest(request)
	bindPorts, err := s.containerNetworkManager.ReservePorts(containerId, len(request.Ports))
	if err != nil {
		return err
	}
	portsHandedOff := false
	defer func() {
		if !portsHandedOff {
			s.containerNetworkManager.ReleasePortReservations(containerId)
		}
	}()

	log.Info().Str("container_id", containerId).Msgf("acquired ports: %v", bindPorts)
	metrics.RecordWorkerStartupPhase("port_allocation", time.Since(phaseStart), request, map[string]string{"port_count": fmt.Sprintf("%d", len(bindPorts))})
	s.recordStartupLifecycle(ctx, request, types.ContainerLifecyclePortAllocation, phaseStart, err == nil, map[string]string{"port_count": fmt.Sprintf("%d", len(bindPorts))})

	// Read spec from bundle; guard against empty image IDs
	if request.ImageId == "" {
		return fmt.Errorf("empty image id in request")
	}
	phaseStart = time.Now()
	initialBundleSpec, _ := s.readBundleConfig(request)
	metrics.RecordWorkerStartupPhase("read_bundle_config", time.Since(phaseStart), request, map[string]string{"derived": fmt.Sprintf("%t", initialBundleSpec == nil)})
	s.recordStartupLifecycle(ctx, request, types.ContainerLifecycleReadBundleConfig, phaseStart, true, map[string]string{"derived": fmt.Sprintf("%t", initialBundleSpec == nil)})

	startupPortBindings := startupPortBindingsForRequest(request, requestedPorts, bindPorts)
	opts := &ContainerOptions{
		BundlePath:                  bundlePath,
		HostBindPort:                bindPorts[0],
		BindPorts:                   bindPorts,
		StartupPortBindings:         startupPortBindings,
		InitialSpec:                 initialBundleSpec,
		StartupStartedAt:            startupStartedAt,
		CheckpointFilesystemRestore: filesystemRestore,
	}

	phaseStart = time.Now()
	err = s.containerMountManager.SetupContainerMounts(ctx, request, outputLogger)
	metrics.RecordWorkerStartupPhase("setup_mounts", time.Since(phaseStart), request, map[string]string{
		"mount_count": fmt.Sprintf("%d", len(request.Mounts)),
		"success":     fmt.Sprintf("%t", err == nil),
	})
	s.recordStartupLifecycle(ctx, request, types.ContainerLifecycleSetupMounts, phaseStart, err == nil, map[string]string{"mount_count": fmt.Sprintf("%d", len(request.Mounts))})
	if err != nil {
		outputLogger.Info(fmt.Sprintf("failed to setup container mounts: %v", err))
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	var mountSetup errgroup.Group
	mountSetup.Go(func() error { return s.containerMountManager.ensureBindMountSourceDirs(ctx, request.Mounts) })

	// Generate dynamic runc spec for this container
	phaseStart = time.Now()
	spec, err := s.specFromRequest(request, opts)
	metrics.RecordWorkerStartupPhase("spec_from_request", time.Since(phaseStart), request, map[string]string{"success": fmt.Sprintf("%t", err == nil)})
	s.recordStartupLifecycle(ctx, request, types.ContainerLifecycleSpecFromRequest, phaseStart, err == nil, nil)
	if err != nil {
		return err
	}
	log.Info().Str("container_id", containerId).Msg("successfully created spec from request")

	if err := mountSetup.Wait(); err != nil {
		return err
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		// Start the container
		phaseStart = time.Now()
		portsHandedOff = true
		filesystemRestoreHandedOff = true
		s.containerWg.Add(1)
		go s.spawn(request, spec, outputLogger, opts)
		metrics.RecordWorkerStartupPhase("spawn_enqueue", time.Since(phaseStart), request, nil)
	}

	log.Info().Str("container_id", containerId).Msg("spawned successfully")
	logCaptureClosed = true
	return nil
}

func validateCheckpointRestoreRuntime(request *types.ContainerRequest, rt runtime.Runtime) error {
	if !hasAvailableCheckpoint(request) {
		return nil
	}
	if request.Checkpoint.IsFilesystemOnly() {
		return nil
	}
	if rt == nil {
		return fmt.Errorf("cannot restore checkpoint %q: container runtime is unavailable", request.Checkpoint.CheckpointId)
	}
	runtimeName := rt.Name()
	if !rt.Capabilities().CheckpointRestore {
		return fmt.Errorf(
			"cannot restore checkpoint %q with runtime %q: checkpoint restore is unsupported",
			request.Checkpoint.CheckpointId,
			runtimeName,
		)
	}
	required := strings.TrimSpace(request.Checkpoint.Runtime)
	if required == "" || required == runtimeName {
		return nil
	}
	return fmt.Errorf(
		"cannot restore %s checkpoint %q with runtime %q",
		required,
		request.Checkpoint.CheckpointId,
		runtimeName,
	)
}

func (s *Worker) mountWorkspaceStorage(ctx context.Context, request *types.ContainerRequest) error {
	log.Info().Str("container_id", request.ContainerId).Msg("mounting workspace storage")
	startedAt := time.Now()
	mount, err := s.storageManager.Mount(request.Workspace.Name, request.Workspace.Storage)
	metrics.RecordWorkerStartupPhase("workspace_mount", time.Since(startedAt), request, map[string]string{"success": fmt.Sprintf("%t", err == nil)})
	s.recordStartupLifecycle(ctx, request, types.ContainerLifecycleWorkspaceMount, startedAt, err == nil, nil)
	if err != nil {
		return fmt.Errorf("mount workspace storage: %w", err)
	}
	if aware, ok := mount.(storage.VolumeContentReporterAware); ok {
		var reporter storage.VolumeContentReporter
		if s.cacheManager != nil {
			reporter = s.cacheManager.ContentReporter()
		}
		// The workspace id also scopes the durable volume object hash registry.
		// Set it even when proactive required-content reporting is disabled.
		aware.SetVolumeContentReporter(request.WorkspaceId, reporter)
	}
	return nil
}

func (s *Worker) setWorkerAddress(ctx context.Context, request *types.ContainerRequest) error {
	hostname := fmt.Sprintf("%s:%d", s.podAddr, s.containerServer.port)
	startedAt := time.Now()
	_, err := handleGRPCResponse(s.containerRepoClient.SetWorkerAddress(ctx, &pb.SetWorkerAddressRequest{
		ContainerId: request.ContainerId,
		Address:     hostname,
		Route:       s.backendRouteFor(request, types.BackendRouteKindWorker, 0, hostname),
	}))
	metrics.RecordWorkerStartupPhase("set_worker_address", time.Since(startedAt), request, nil)
	s.recordStartupLifecycle(ctx, request, types.ContainerLifecycleSetWorkerAddress, startedAt, err == nil, nil)
	return err
}

func (s *Worker) loadContainerImage(ctx context.Context, request *types.ContainerRequest, outputLogger *slog.Logger) (time.Duration, bool, error) {
	outputLogger.Info(fmt.Sprintf("Loading image <%s>...\n", request.ImageId))

	elapsed, err := s.pullLazyWithMetrics(ctx, request, "pull_lazy", outputLogger)
	if err == nil {
		outputLogger.Info(fmt.Sprintf("Loaded image <%s>, took: %s\n", request.ImageId, elapsed))
		return elapsed, true, nil
	}

	if !request.IsBuildRequest() {
		log.Error().Str("container_id", request.ContainerId).Msgf("failed to pull image: %v", err)
		return elapsed, false, err
	}

	select {
	case <-ctx.Done():
		return elapsed, false, nil
	default:
	}

	if err := s.buildOrPullBaseImageWithMetrics(ctx, request, outputLogger); err != nil {
		return elapsed, false, err
	}
	if !requiresPostBuildImageMaterialization(request, s.config.ImageService.ClipVersion) {
		outputLogger.Info(fmt.Sprintf("Image <%s> is ready\n", request.ImageId))
		return 0, true, nil
	}

	elapsed, err = s.pullLazyWithMetrics(ctx, request, "pull_lazy_after_build", outputLogger)
	if err != nil {
		return elapsed, false, err
	}

	outputLogger.Info(fmt.Sprintf("Loaded image <%s>, took: %s\n", request.ImageId, elapsed))
	return elapsed, true, nil
}

func requiresPostBuildImageMaterialization(request *types.ContainerRequest, clipVersion uint32) bool {
	return clipVersion != uint32(types.ClipVersion2) || !request.IsBuildRequest()
}

func (s *Worker) pullLazyWithMetrics(ctx context.Context, request *types.ContainerRequest, phase string, outputLogger *slog.Logger) (time.Duration, error) {
	phaseStart := time.Now()
	// A deployment must not eagerly inflate every OCI layer before its runtime
	// can start. Large model images can spend tens of minutes on one compressed
	// layer even when the entrypoint only needs a small working set. Checkpoint
	// creation still requires a fully materialized root filesystem; ordinary
	// deployments use CLIP's lazy, content-addressed read path.
	elapsed, err := s.imageClient.PullLazy(ctx, request, outputLogger, request.CheckpointEnabled)
	metrics.RecordWorkerStartupPhase(phase, time.Since(phaseStart), request, map[string]string{"success": fmt.Sprintf("%t", err == nil)})
	spanID := types.ContainerLifecycleImageLoad
	if phase != "pull_lazy" && phase != "pull_lazy_after_build" {
		spanID = types.ContainerLifecycleID("image." + phase)
	}
	s.recordContainerLifecycle(ctx, request, containerLifecycleFromDuration(spanID, request, phaseStart, time.Since(phaseStart), err == nil, map[string]string{
		types.EventAttrLifecycle: phase,
		"image_id":               request.ImageId,
		"elapsed_raw":            elapsed.String(),
	}))
	return elapsed, err
}

func (s *Worker) buildOrPullBaseImageWithMetrics(ctx context.Context, request *types.ContainerRequest, outputLogger *slog.Logger) error {
	phaseStart := time.Now()
	err := s.buildOrPullBaseImage(ctx, request, request.ContainerId, outputLogger)
	metrics.RecordWorkerStartupPhase("build_or_pull_base_image", time.Since(phaseStart), request, map[string]string{"success": fmt.Sprintf("%t", err == nil)})
	return err
}

func portsForRequest(request *types.ContainerRequest) []uint32 {
	if request.Checkpoint != nil {
		return request.Checkpoint.ExposedPorts
	}

	ports := request.Ports
	if len(ports) == 0 {
		ports = []uint32{uint32(containerInnerPort)}
	}

	ports = append(ports, uint32(types.WorkerShellPort))
	if request.Stub.Type.Kind() == types.StubTypeSandbox {
		ports = append(ports, uint32(types.WorkerSandboxProcessManagerPort))
	}

	return ports
}

func startupPortBindingsForRequest(request *types.ContainerRequest, requestedPorts []uint32, bindPorts []int) []PortBinding {
	if len(request.Ports) == 0 || len(bindPorts) == 0 {
		return nil
	}

	exposePorts := make(map[uint32]struct{}, len(request.Ports))
	if request.Checkpoint != nil {
		for _, port := range request.Ports {
			exposePorts[port] = struct{}{}
		}
	} else if request.Stub.Type.Kind() == types.StubTypeSandbox {
		for _, port := range requestedPorts {
			exposePorts[port] = struct{}{}
		}
	} else {
		for _, port := range request.Ports {
			exposePorts[port] = struct{}{}
		}
	}

	if len(exposePorts) == 0 {
		return nil
	}

	bindings := make([]PortBinding, 0, len(exposePorts))
	for idx, containerPort := range request.Ports {
		if idx >= len(bindPorts) {
			break
		}
		if _, ok := exposePorts[containerPort]; !ok {
			continue
		}
		bindings = append(bindings, PortBinding{
			HostPort:      bindPorts[idx],
			ContainerPort: int(containerPort),
		})
	}

	return bindings
}

func (s *Worker) publishContainerAddresses(ctx context.Context, request *types.ContainerRequest, bindings []PortBinding) error {
	if s.agentWorker() {
		return nil
	}

	containerId := request.ContainerId
	addressMap := make(map[int32]string, len(bindings))
	for _, binding := range bindings {
		addressMap[int32(binding.ContainerPort)] = joinHostPort(s.podAddr, binding.HostPort)
	}
	s.cacheContainerAddressMap(containerId, addressMap)
	var primaryPort int32
	if len(bindings) > 0 {
		primaryPort = int32(bindings[0].ContainerPort)
	}

	phaseStart := time.Now()
	_, err := handleGRPCResponse(s.containerRepoClient.SetContainerAddressMap(context.Background(), &pb.SetContainerAddressMapRequest{
		ContainerId: containerId,
		AddressMap:  addressMap,
		PrimaryPort: primaryPort,
	}))
	metrics.RecordWorkerStartupPhase("set_container_address_map", time.Since(phaseStart), request, map[string]string{"port_count": fmt.Sprintf("%d", len(addressMap))})
	s.recordStartupLifecycle(ctx, request, types.ContainerLifecycleSetAddressMap, phaseStart, err == nil, map[string]string{"port_count": fmt.Sprintf("%d", len(addressMap))})
	if err != nil {
		return err
	}

	log.Info().
		Str("container_id", containerId).
		Str("stub_type", request.Stub.Type.Kind()).
		Int("port_count", len(addressMap)).
		Interface("address_map", addressMap).
		Msg("set container address map")
	return nil
}

func (s *Worker) buildOrPullBaseImage(ctx context.Context, request *types.ContainerRequest, containerId string, outputLogger *slog.Logger) error {
	// For Clip v2 builds, the Dockerfile is rendered by the builder with all build steps.
	// Build via buildah if a non-empty Dockerfile is present (contains RUN commands for actual builds).
	if request.BuildOptions.Dockerfile != nil && *request.BuildOptions.Dockerfile != "" {
		log.Info().Str("container_id", containerId).Msg("building image from Dockerfile")
		return s.imageClient.BuildAndArchiveImage(ctx, outputLogger, request)
	}

	// Fallback: pull the source image and archive it
	if request.BuildOptions.SourceImage != nil {
		log.Info().Str("container_id", containerId).Msgf("pulling source image: %s", *request.BuildOptions.SourceImage)
		return s.imageClient.PullAndArchiveImage(ctx, outputLogger, request)
	}

	return nil
}

func (s *Worker) readBundleConfig(request *types.ContainerRequest) (*specs.Spec, error) {
	imageConfigPath := filepath.Join(s.imageMountPath, request.ImageId, initialSpecBaseName)
	if request.IsBuildRequest() {
		imageConfigPath = filepath.Join(s.imageMountPath, request.ImageId, specBaseName)
	}

	data, err := os.ReadFile(imageConfigPath)
	if err != nil {
		// For v2 images, there's no pre-baked config.json in the mounted root.
		// Derive the spec from CLIP metadata embedded in the archive.
		if os.IsNotExist(err) {
			log.Info().Str("image_id", request.ImageId).Msg("no bundle config found, deriving from v2 image metadata")
			return s.deriveSpecFromV2Image(request)
		}
		log.Error().Str("image_id", request.ImageId).Str("image_config_path", imageConfigPath).Err(err).Msg("failed to read bundle config")
		return nil, err
	}

	specTemplate := strings.TrimSpace(string(data))
	var spec specs.Spec

	err = json.Unmarshal([]byte(specTemplate), &spec)
	if err != nil {
		log.Error().Str("image_id", request.ImageId).Str("image_config_path", imageConfigPath).Err(err).Msg("failed to unmarshal bundle config")
		return nil, err
	}

	return &spec, nil
}

// deriveSpecFromV2Image creates an OCI spec from v2 image metadata.
// This is ONLY used for v2 images where we don't have an unpacked bundle with config.json.
// V1 images always have a config.json, so if we're here, it's a v2 image.
func (s *Worker) deriveSpecFromV2Image(request *types.ContainerRequest) (*specs.Spec, error) {
	clipMeta, ok := s.imageClient.GetCLIPImageMetadata(request.ImageId)
	if !ok {
		log.Warn().
			Str("image_id", request.ImageId).
			Msg("no metadata found in v2 image archive, using base spec")
		return nil, nil
	}

	log.Info().
		Str("image_id", request.ImageId).
		Msg("using metadata from v2 clip archive")

	return s.buildSpecFromCLIPMetadata(clipMeta), nil
}

// buildSpecFromCLIPMetadata constructs an OCI spec from CLIP image metadata
// This is the primary path for v2 images with embedded metadata
func (s *Worker) buildSpecFromCLIPMetadata(clipMeta *clipCommon.ImageMetadata) *specs.Spec {
	spec := specs.Spec{
		Process: &specs.Process{
			Env: []string{},
			Cwd: "/",
		},
	}

	// CLIP metadata has a flat structure with all fields at the top level
	if len(clipMeta.Env) > 0 {
		spec.Process.Env = clipMeta.Env
	}
	if clipMeta.WorkingDir != "" {
		spec.Process.Cwd = clipMeta.WorkingDir
	}
	if clipMeta.User != "" {
		spec.Process.User.Username = clipMeta.User
	}
	// Combine Entrypoint and Cmd, or use Cmd alone
	if len(clipMeta.Entrypoint) > 0 {
		spec.Process.Args = append(clipMeta.Entrypoint, clipMeta.Cmd...)
	} else if len(clipMeta.Cmd) > 0 {
		spec.Process.Args = clipMeta.Cmd
	}
	if spec.Process.Cwd == "" {
		spec.Process.Cwd = "/"
	}

	return &spec
}

// Generate a runc spec from a given request
func (s *Worker) specFromRequest(request *types.ContainerRequest, options *ContainerOptions) (*specs.Spec, error) {
	if request == nil {
		return nil, errors.New("cannot build a spec for a nil container request")
	}
	if options == nil || len(options.BindPorts) == 0 {
		return nil, fmt.Errorf("container <%s> has no reserved bind port", request.ContainerId)
	}
	if err := os.MkdirAll(filepath.Join(baseConfigPath, request.ContainerId), os.ModePerm); err != nil {
		return nil, fmt.Errorf("create container config directory: %w", err)
	}

	spec, err := s.newSpecTemplate()
	if err != nil {
		return nil, err
	}
	// Preserve the runtime default unless a hostname is requested.
	if hostname := sanitizeHostname(request.Hostname); hostname != "" {
		spec.Hostname = hostname
	}

	spec.Process.Cwd = defaultContainerDirectory
	spec.Process.Args = append([]string(nil), request.EntryPoint...)
	spec.Process.Terminal = false

	if request.Stub.Type.Kind() == types.StubTypePod && options.InitialSpec != nil && options.InitialSpec.Process != nil {
		if len(request.EntryPoint) == 0 {
			log.Info().
				Str("container_id", request.ContainerId).
				Str("stub_id", request.StubId).
				Str("entrypoint", strings.Join(request.EntryPoint, " ")).
				Msg("no entrypoint provided, using initial spec entrypoint")

			spec.Process.Args = append([]string(nil), options.InitialSpec.Process.Args...)
		}

		if options.InitialSpec.Process.Cwd != "" {
			spec.Process.Cwd = options.InitialSpec.Process.Cwd
		}
		spec.Process.User.UID = options.InitialSpec.Process.User.UID
		spec.Process.User.GID = options.InitialSpec.Process.User.GID
	}

	if len(spec.Process.Args) == 0 {
		if args := fallbackEntrypoint(request); len(args) > 0 {
			log.Warn().
				Str("container_id", request.ContainerId).
				Str("stub_id", request.StubId).
				Str("stub_type", string(request.Stub.Type)).
				Strs("entrypoint", args).
				Msg("container request had empty entrypoint, using stub default")
			spec.Process.Args = args
		}
	}

	if len(spec.Process.Args) == 0 {
		return nil, fmt.Errorf("container <%s> has empty process args for stub <%s> type <%s>", request.ContainerId, request.StubId, request.Stub.Type)
	}

	cpuAffinity := ""
	if s.containerInstances != nil {
		if instance, exists := s.containerInstances.Get(request.ContainerId); exists {
			cpuAffinity = instance.CPUSet
		}
	}
	if cpuAffinity != "" {
		if spec.Linux.Resources.CPU == nil {
			spec.Linux.Resources.CPU = &specs.LinuxCPU{}
		}
		spec.Linux.Resources.CPU.Cpus = cpuAffinity
	}

	cpuEnforced := s.cpuLimitsEnforced(request)
	memoryEnforced := s.memoryLimitsEnforced(request)
	if cpuEnforced || memoryEnforced {
		resources, err := s.getContainerResources(request)
		if err != nil {
			return nil, err
		}
		if cpuEnforced && resources.CPU != nil {
			resources.CPU.Cpus = cpuAffinity
			if s.deferCPUThrottle(request, resources.CPU) {
				startupCPU := *resources.CPU
				startupCPU.Quota = nil
				startupCPU.Burst = nil
				startupCPU.Period = nil
				// gVisor fixes the guest CPU topology when the sandbox starts;
				// a later resource update cannot change what nproc reports.
				if !requestForcesResourceLimits(request) {
					startupCPU.Cpus = ""
				}
				spec.Linux.Resources.CPU = &startupCPU
			} else {
				spec.Linux.Resources.CPU = resources.CPU
			}
		}
		if memoryEnforced && resources.Memory != nil {
			spec.Linux.Resources.Unified = cgroupV2Parameters()
			spec.Linux.Resources.Memory = resources.Memory
		}
	}

	deferredCPU := s.hasDeferredCPUThrottle(request.ContainerId)
	runnerReadySignal := deferredCPU && request.Stub.Type.Kind() == types.StubTypeFunction
	if runnerReadySignal {
		if err := os.MkdirAll(runnerSignalDir(request.ContainerId), 0755); err != nil {
			return nil, err
		}
		spec.Mounts = append(spec.Mounts, specs.Mount{
			Type:        "bind",
			Source:      runnerSignalDir(request.ContainerId),
			Destination: filepath.Dir(types.ContainerRunnerReadyPath),
			Options:     []string{"rbind", "rprivate", "nosuid", "nodev"},
		})
	}

	env := s.getContainerEnvironment(request, options)
	if runnerReadySignal {
		env = append(env, types.ContainerRunnerReadyPathEnv+"="+types.ContainerRunnerReadyPath)
	}
	if request.Gpu != "" {
		env = s.containerGPUManager.InjectEnvVars(env)
	}
	env = s.applyRuntimeEnvironmentOverrides(env, request, spec.Process.Args)

	// Environment is already assembled in getContainerEnvironment (includes InitialSpec.Env if present)
	spec.Process.Env = env

	// We need to include the checkpoint signal files in the container spec
	if s.IsCRIUAvailable(request.GpuCount) && request.CheckpointEnabled {
		disableIOUringForCheckpoint(spec)

		err = os.MkdirAll(checkpointSignalDir(request.ContainerId), os.ModePerm) // Add a mount point for the checkpoint signal file
		if err != nil {
			return nil, err
		}

		spec.Mounts = append(spec.Mounts, specs.Mount{
			Type:        "bind",
			Source:      checkpointSignalDir(request.ContainerId),
			Destination: "/criu",
			Options: []string{
				"rbind",
				"rprivate",
				"nosuid",
				"nodev",
			},
		})

		containerIdPath := filepath.Join(checkpointSignalDir(request.ContainerId), checkpointContainerIdFileName)
		err := os.WriteFile(containerIdPath, []byte(request.ContainerId), 0644)
		if err != nil {
			return nil, err
		}

		containerHostname := fmt.Sprintf("%s:%d", s.podAddr, options.HostBindPort)
		containerHostnamePath := filepath.Join(checkpointSignalDir(request.ContainerId), checkpointContainerHostnameFileName)
		err = os.WriteFile(containerHostnamePath, []byte(containerHostname), 0644)
		if err != nil {
			return nil, err
		}
	}

	volumeCacheMap, err := s.addRequestMounts(request, spec)
	if err != nil {
		return nil, err
	}
	s.enableVolumeCaching(request, volumeCacheMap, spec)

	// Configure resolv.conf
	resolvMount := specs.Mount{
		Type:        "none",
		Source:      containerResolvConfSource(s.config.Worker.UseHostResolvConf, hostResolvConfPath),
		Destination: "/etc/resolv.conf",
		Options: []string{
			"ro",
			"rbind",
			"rprivate",
			"nosuid",
			"noexec",
			"nodev",
		},
	}

	spec.Mounts = append(spec.Mounts, resolvMount)

	// External mount for gVisor file uploads (external mounts bypass directory caching)
	uploadsPath := filepath.Join(types.WorkerContainerUploadsHostPath, request.ContainerId)
	if err := os.MkdirAll(uploadsPath, 0755); err == nil {
		spec.Mounts = append(spec.Mounts, specs.Mount{
			Type:        "none",
			Source:      uploadsPath,
			Destination: types.WorkerContainerUploadsMountPath,
			Options:     []string{"rbind", "rw"},
		})
	}

	// Add back tmpfs pod/sandbox mounts from initial spec if they exist
	if (request.Stub.Type.Kind() == types.StubTypePod || request.Stub.Type.Kind() == types.StubTypeSandbox) && options.InitialSpec != nil {
		for _, m := range options.InitialSpec.Mounts {
			if m.Source == "none" && m.Type == "tmpfs" {
				m.Options = append([]string(nil), m.Options...)
				spec.Mounts = append(spec.Mounts, m)
			}
		}
	}

	if err := validateContainerSpec(spec); err != nil {
		return nil, fmt.Errorf("invalid spec for container <%s>: %w", request.ContainerId, err)
	}
	return spec, nil
}

func fallbackEntrypoint(request *types.ContainerRequest) []string {
	stubConfig := requestStubConfig(request)
	if stubConfig != nil && len(stubConfig.EntryPoint) > 0 {
		return stubConfig.EntryPoint
	}

	pythonVersion := types.Python3.String()
	if stubConfig != nil && stubConfig.PythonVersion != "" {
		pythonVersion = stubConfig.PythonVersion
	}

	switch request.Stub.Type.Kind() {
	case types.StubTypeEndpoint, types.StubTypeASGI:
		if !envHas(request.Env, "HANDLER=") {
			return nil
		}
		return []string{pythonVersion, "-m", "beta9.runner.endpoint"}
	case types.StubTypeFunction:
		if !envHas(request.Env, "HANDLER=") {
			return nil
		}
		return []string{pythonVersion, "-m", "beta9.runner.function"}
	case types.StubTypeTaskQueue:
		if !envHas(request.Env, "HANDLER=") {
			return nil
		}
		return []string{pythonVersion, "-m", "beta9.runner.taskqueue"}
	default:
		return nil
	}
}

func envHas(env []string, prefix string) bool {
	for _, item := range env {
		if strings.HasPrefix(item, prefix) {
			return true
		}
	}
	return false
}

func requestStubConfig(request *types.ContainerRequest) *types.StubConfigV1 {
	if strings.TrimSpace(request.Stub.Config) == "" {
		return nil
	}

	stubConfig, err := request.Stub.UnmarshalConfig()
	if err != nil {
		log.Debug().
			Str("container_id", request.ContainerId).
			Str("stub_id", request.StubId).
			Err(err).
			Msg("failed to parse stub config for entrypoint fallback")
		return nil
	}

	return stubConfig
}

func (s *Worker) addRequestMounts(request *types.ContainerRequest, spec *specs.Spec) (map[string]string, error) {
	volumeCacheMap := make(map[string]string)

	for i := range request.Mounts {
		mount := &request.Mounts[i]
		ok, err := s.prepareRequestMount(request, mount, volumeCacheMap)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}

		if mount.LinkPath != "" {
			if err := forceSymlink(mount.MountPath, mount.LinkPath); err != nil {
				log.Error().Str("container_id", request.ContainerId).Msgf("unable to symlink volume: %v", err)
			}
		}

		spec.Mounts = append(spec.Mounts, specs.Mount{
			Type:        "none",
			Source:      mount.LocalPath,
			Destination: mount.MountPath,
			Options:     []string{"rbind", bindMountMode(*mount)},
		})
	}

	return volumeCacheMap, nil
}

func (s *Worker) prepareRequestMount(request *types.ContainerRequest, mount *types.Mount, volumeCacheMap map[string]string) (bool, error) {
	if mount == nil {
		return false, nil
	}

	if mount.MountType == storage.StorageModeMountPoint {
		if _, err := os.Stat(mount.LocalPath); os.IsNotExist(err) {
			return false, nil
		}
		return true, nil
	}

	if mount.MountType == types.StorageModeDurableDisk {
		if err := s.prepareDurableDiskMount(request, mount); err != nil {
			return false, fmt.Errorf("failed to prepare durable disk mount: %w", err)
		}
		return true, nil
	}

	if strings.HasPrefix(mount.MountPath, types.WorkerContainerVolumePath) && !checkpointModelCacheMount(mount.MountPath) {
		volumeCacheMap[filepath.Base(mount.MountPath)] = mount.LocalPath
	}

	return true, nil
}

func checkpointModelCacheMount(mountPath string) bool {
	return strings.HasPrefix(filepath.Base(mountPath), types.CheckpointModelCacheVolumePrefix)
}

func bindMountMode(mount types.Mount) string {
	if mount.ReadOnly {
		return "ro"
	}
	return "rw"
}

func (s *Worker) enableVolumeCaching(request *types.ContainerRequest, volumeCacheMap map[string]string, spec *specs.Spec) {
	if s.fileCacheManager == nil || !request.VolumeCacheCompatible() || !s.fileCacheManager.CacheAvailable() {
		return
	}

	if err := s.fileCacheManager.EnableVolumeCaching(request.Workspace.Name, volumeCacheMap, spec); err != nil {
		log.Error().Str("container_id", request.ContainerId).Msgf("failed to setup volume caching: %v", err)
	}
}

func (s *Worker) newSpecTemplate() (*specs.Spec, error) {
	if s == nil || s.runtime == nil {
		return nil, errors.New("container runtime is unavailable")
	}
	var newSpec specs.Spec
	// Get the appropriate base config for the runtime
	baseConfig := runtime.GetBaseConfig(s.runtime.Name())
	specTemplate := strings.TrimSpace(baseConfig)
	err := json.Unmarshal([]byte(specTemplate), &newSpec)
	if err != nil {
		return nil, err
	}
	if newSpec.Process == nil || newSpec.Root == nil || newSpec.Linux == nil || newSpec.Linux.Resources == nil {
		return nil, fmt.Errorf("runtime <%s> has an incomplete base OCI spec", s.runtime.Name())
	}
	return &newSpec, nil
}

// sanitizeHostname returns an RFC 1123 label.
func sanitizeHostname(name string) string {
	label := make([]byte, 0, maxHostnameLength)
	separatorPending := false
	for index := 0; index < len(name) && len(label) < maxHostnameLength; index++ {
		character := name[index]
		if character >= 'A' && character <= 'Z' {
			character += 'a' - 'A'
		}

		if (character >= 'a' && character <= 'z') || (character >= '0' && character <= '9') {
			if separatorPending && len(label)+1 < maxHostnameLength {
				label = append(label, '-')
			}
			label = append(label, character)
			separatorPending = false
		} else if len(label) > 0 {
			// Defer separators to avoid trailing or overlong labels.
			separatorPending = true
		}
	}
	return string(label)
}

func validateContainerSpec(spec *specs.Spec) error {
	if spec == nil || spec.Process == nil || spec.Root == nil || spec.Linux == nil || spec.Linux.Resources == nil {
		return errors.New("required OCI sections are missing")
	}
	if spec.Root.Path == "" {
		return errors.New("root path is empty")
	}
	if len(spec.Process.Args) == 0 || strings.TrimSpace(spec.Process.Args[0]) == "" {
		return errors.New("process executable is empty")
	}
	if !filepath.IsAbs(spec.Process.Cwd) {
		return errors.New("process working directory must be absolute")
	}
	for _, env := range spec.Process.Env {
		name, _, ok := strings.Cut(env, "=")
		if !ok || name == "" {
			return fmt.Errorf("invalid environment entry %q", env)
		}
	}
	for _, mount := range spec.Mounts {
		if !filepath.IsAbs(mount.Destination) {
			return fmt.Errorf("mount destination %q must be absolute", mount.Destination)
		}
	}
	return nil
}

// spawn a container using runc binary
func (s *Worker) spawn(request *types.ContainerRequest, spec *specs.Spec, outputLogger *slog.Logger, opts *ContainerOptions) {
	defer s.containerWg.Done()

	ctx, cancel := context.WithCancel(s.ctx)
	defer s.containerNetworkManager.ReleasePortReservations(request.ContainerId)

	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()

		s.workerRepoClient.RemoveContainerFromWorker(cleanupCtx, &pb.RemoveContainerFromWorkerRequest{
			WorkerId:    s.workerId,
			ContainerId: request.ContainerId,
		})
	}()

	defer cancel()

	exitCode := -1
	exitReported := false
	containerId := request.ContainerId

	// Unmount external s3 buckets
	defer s.containerMountManager.RemoveContainerMounts(containerId)

	// Cleanup container state and resources
	defer func() {
		s.finalizeContainer(containerId, request, exitCode, exitReported)
	}()

	// Create overlayfs for container
	overlay := s.createOverlay(request, opts.BundlePath)
	restoreFilesystemCleanupNeeded := opts.CheckpointFilesystemRestore != nil
	defer func() {
		if restoreFilesystemCleanupNeeded {
			opts.CheckpointFilesystemRestore.cleanup()
		}
	}()

	containerInstance, exists := s.containerInstances.Get(containerId)
	if !exists {
		return
	}
	containerInstance.BundlePath = opts.BundlePath
	containerInstance.Overlay = overlay
	containerInstance.Spec = spec
	containerInstance.setExitCode(-1)
	containerInstance.OutputWriter = common.NewOutputWriter(func(s string) {
		outputLogger.Info(s, "done", false, "success", false)
	})
	s.containerInstances.Set(containerId, containerInstance)

	// Setup container overlay filesystem
	var err error
	if opts.CheckpointFilesystemRestore != nil {
		if restoreErr := opts.CheckpointFilesystemRestore.wait(); restoreErr != nil {
			log.Debug().Err(restoreErr).Str("container_id", containerId).Msg("checkpoint filesystem preparation did not complete")
			if err := opts.CheckpointFilesystemRestore.discard(); err != nil {
				log.Error().Err(err).Str("container_id", containerId).Msg("failed to discard partial checkpoint filesystem")
				return
			}
			restoreFilesystemCleanupNeeded = false
		}
	}
	phaseStart := time.Now()
	err = containerInstance.Overlay.Setup()
	metrics.RecordWorkerStartupPhase("overlay_setup", time.Since(phaseStart), request, map[string]string{"success": fmt.Sprintf("%t", err == nil)})
	s.recordStartupLifecycle(ctx, request, types.ContainerLifecycleOverlaySetup, phaseStart, err == nil, nil)
	if err != nil {
		log.Error().Str("container_id", containerId).Msgf("failed to setup overlay: %v", err)
		return
	}
	defer containerInstance.Overlay.Cleanup()
	restoreFilesystemCleanupNeeded = false

	spec.Root.Readonly = false
	spec.Root.Path = containerInstance.Overlay.TopLayerPath()

	// Setup container network namespace / devices
	phaseStart = time.Now()
	err = s.containerNetworkManager.Setup(containerId, spec, request)
	metrics.RecordWorkerStartupPhase("network_setup", time.Since(phaseStart), request, map[string]string{"success": fmt.Sprintf("%t", err == nil)})
	s.recordStartupLifecycle(ctx, request, types.ContainerLifecycleNetworkSetup, phaseStart, err == nil, nil)
	if err != nil {
		log.Error().Str("container_id", containerId).Msgf("failed to setup container network: %v", err)
		return
	}

	// Only inject GPU devices if runtime supports GPU
	if request.RequiresGPU() && s.runtime.Capabilities().GPU {
		// Assign n-number of GPUs to a container
		phaseStart = time.Now()
		assignedDevices, err := s.containerGPUManager.AssignGPUDevices(request.ContainerId, request.GpuCount)
		metrics.RecordWorkerStartupPhase("gpu_assignment", time.Since(phaseStart), request, map[string]string{"success": fmt.Sprintf("%t", err == nil)})
		s.recordStartupLifecycle(ctx, request, types.ContainerLifecycleGPUAssignment, phaseStart, err == nil, map[string]string{"gpu_count": fmt.Sprintf("%d", request.GpuCount)})
		if err != nil {
			log.Error().Str("container_id", request.ContainerId).Msgf("failed to assign GPUs: %v", err)
			return
		}

		// Only use CDI if runtime supports it
		if s.runtime.Capabilities().CDI {
			cdiCache := cdi.GetDefaultCache()
			devicesToInject := s.containerGPUManager.CDIDevices(assignedDevices)

			unresolvable, err := cdiCache.InjectDevices(spec, devicesToInject...)
			if err != nil {
				log.Error().Str("container_id", request.ContainerId).Msgf("failed to inject devices: %v", err)
				return
			}
			if len(unresolvable) > 0 {
				log.Error().Str("container_id", request.ContainerId).Msgf("unresolvable devices: %v", unresolvable)
				return
			}
		}

		// Pin env vars to the assigned devices regardless of CDI support; for
		// non-CDI runtimes this is the only thing scoping the container's GPUs.
		spec.Process.Env = s.containerGPUManager.InjectAssignedEnvVars(spec.Process.Env, assignedDevices)
	}

	// Expose the bind ports
	phaseStart = time.Now()
	err = s.containerNetworkManager.ExposePorts(containerId, opts.StartupPortBindings)
	if err != nil {
		metrics.RecordWorkerStartupPhase("network_expose_ports", time.Since(phaseStart), request, map[string]string{
			"port_count": fmt.Sprintf("%d", len(opts.StartupPortBindings)),
			"success":    "false",
		})
		s.recordStartupLifecycle(ctx, request, types.ContainerLifecycleNetworkExpose, phaseStart, false, map[string]string{"port_count": fmt.Sprintf("%d", len(opts.StartupPortBindings))})
		log.Error().Str("container_id", containerId).Msgf("failed to expose container bind port: %v", err)
		return
	}
	metrics.RecordWorkerStartupPhase("network_expose_ports", time.Since(phaseStart), request, map[string]string{
		"port_count": fmt.Sprintf("%d", len(opts.StartupPortBindings)),
		"success":    "true",
	})
	s.recordStartupLifecycle(ctx, request, types.ContainerLifecycleNetworkExpose, phaseStart, true, map[string]string{"port_count": fmt.Sprintf("%d", len(opts.StartupPortBindings))})

	if err := s.registerContainerPorts(ctx, request, opts.StartupPortBindings); err != nil {
		log.Error().Str("container_id", containerId).Err(err).Msg("failed to register container network addresses")
		return
	}

	// Modify sandbox entry point to point to process manager binary
	if request.Stub.Type.Kind() == types.StubTypeSandbox {
		instance, exists := s.containerInstances.Get(containerId)
		if !exists {
			log.Error().Str("container_id", containerId).Msg("instance not found")
			return
		}

		instance.SandboxProcessManager = nil
		s.containerInstances.Set(containerId, instance)

		spec.Process.Args = []string{types.WorkerSandboxProcessManagerContainerPath}
		spec.Mounts = append(spec.Mounts, specs.Mount{
			Type:        "bind",
			Source:      types.WorkerSandboxProcessManagerWorkerPath,
			Destination: types.WorkerSandboxProcessManagerContainerPath,
			Options: []string{
				"ro",
				"rbind",
				"rprivate",
				"nosuid",
				"nodev",
			},
		})
	}

	// Add Docker capabilities if enabled for sandbox containers.
	if request.DockerEnabled && request.Stub.Type.Kind() == types.StubTypeSandbox {
		runtime.AddDockerInDockerCapabilities(spec)
		log.Info().Str("container_id", containerId).Str("runtime", s.runtime.Name()).Msg("added docker capabilities for sandbox container")
	}

	// Prepare spec for the selected runtime
	phaseStart = time.Now()
	if err := s.runtime.Prepare(ctx, spec); err != nil {
		metrics.RecordWorkerStartupPhase("runtime_prepare", time.Since(phaseStart), request, map[string]string{
			"runtime": s.runtime.Name(),
			"success": "false",
		})
		s.recordStartupLifecycle(ctx, request, types.ContainerLifecycleRuntimePrepare, phaseStart, false, map[string]string{"runtime": s.runtime.Name()})
		log.Error().Str("container_id", containerId).Msgf("failed to prepare spec for runtime: %v", err)
		return
	}
	metrics.RecordWorkerStartupPhase("runtime_prepare", time.Since(phaseStart), request, map[string]string{
		"runtime": s.runtime.Name(),
		"success": "true",
	})
	s.recordStartupLifecycle(ctx, request, types.ContainerLifecycleRuntimePrepare, phaseStart, true, map[string]string{"runtime": s.runtime.Name()})

	// Write container config spec to disk
	configContents, err := json.Marshal(spec)
	if err != nil {
		return
	}

	configPath := filepath.Join(spec.Root.Path, specBaseName)
	phaseStart = time.Now()
	err = os.WriteFile(configPath, configContents, 0644)
	metrics.RecordWorkerStartupPhase("config_write", time.Since(phaseStart), request, map[string]string{"success": fmt.Sprintf("%t", err == nil)})
	s.recordStartupLifecycle(ctx, request, types.ContainerLifecycleConfigWrite, phaseStart, err == nil, nil)
	if err != nil {
		log.Error().Str("container_id", containerId).Msgf("failed to write container config: %v", err)
		return
	}
	request.ConfigPath = configPath

	outputWriter := containerInstance.OutputWriter
	restoringRuntimeCheckpoint := s.supportsCheckpointRestore(request, containerInstance.Runtime) && hasAvailableCheckpoint(request)
	if restoringRuntimeCheckpoint && request.Checkpoint.IsFilesystemOnly() {
		restoringRuntimeCheckpoint = false
	}

	// Log metrics
	go s.workerUsageMetrics.EmitContainerUsage(ctx, request)

	startedChan := make(chan int, 1)
	checkpointPIDChan := make(chan int, 1)
	monitorPIDChan := make(chan int, 1)

	defer func() {
		// Close in reverse order of dependency
		close(checkpointPIDChan)
		close(monitorPIDChan)
		close(startedChan)
	}()

	go func() {
		// When the process starts monitor it and potentially checkpoint it
		pid, ok := <-startedChan
		if !ok {
			return
		}

		monitorPIDChan <- pid
		checkpointPIDChan <- pid

		if request.Stub.Type.Kind() == types.StubTypeSandbox {
			instance, exists := s.containerInstances.Get(containerId)
			if !exists {
				return
			}
			if restoringRuntimeCheckpoint {
				s.signalRestoredSandboxProcessManager(ctx, request, instance.Runtime)
			}

			phaseStart := time.Now()
			processManagerClient, processManagerReady, processManagerStats := s.waitForProcessManager(ctx, containerId, instance)
			metrics.RecordWorkerStartupPhase("sandbox_process_manager_ready", time.Since(phaseStart), request, map[string]string{"success": fmt.Sprintf("%t", processManagerReady)})
			s.recordStartupLifecycle(ctx, request, types.ContainerLifecycleSandboxProcessManagerReady, phaseStart, processManagerReady, processManagerStats.attrs())

			if fresh, exists := s.containerInstances.Get(containerId); exists {
				instance = fresh
			}
			if processManagerReady {
				phaseStart = time.Now()
				err := s.applyDeferredCPUThrottle(request, instance)
				metrics.RecordWorkerStartupPhase("sandbox_apply_cpu_quota", time.Since(phaseStart), request, map[string]string{
					"success": fmt.Sprintf("%t", err == nil),
				})
				s.recordStartupLifecycle(ctx, request, types.ContainerLifecycleSandboxApplyCPUQuota, phaseStart, err == nil, nil)
				if err != nil {
					log.Error().Err(err).Str("container_id", containerId).Msg("failed to apply sandbox CPU quota")
					processManagerReady = false
				} else if fresh, exists := s.containerInstances.Get(containerId); exists {
					instance = fresh
				}
			}
			instance.SandboxProcessManager = processManagerClient
			instance.signalProcessManagerReadiness(processManagerReady)
			s.containerInstances.Set(containerId, instance)

			if !processManagerReady {
				log.Error().Str("container_id", containerId).Msg("failed to initialize process manager - sandbox may not be functional")
				return
			}

			if request.DockerEnabled {
				go s.startDockerDaemon(ctx, containerId, instance)
			}
		} else if s.hasDeferredCPUThrottle(containerId) {
			phaseStart := time.Now()
			err := s.applyDeferredCPUThrottleAfterRunnerReady(ctx, request)
			metrics.RecordWorkerStartupPhase("runner_apply_cpu_quota", time.Since(phaseStart), request, map[string]string{
				"success": fmt.Sprintf("%t", err == nil),
			})
			s.recordStartupLifecycle(ctx, request, types.ContainerLifecycleRunnerApplyCPUQuota, phaseStart, err == nil, nil)
			if err != nil && !errors.Is(err, context.Canceled) {
				log.Error().Err(err).Str("container_id", containerId).Msg("failed to apply runner CPU quota")
			}
		}
	}()

	isOOMKilled := atomic.Bool{}
	go func() {
		pid := <-monitorPIDChan
		go s.collectAndSendContainerMetrics(ctx, request, spec, pid)
		s.setupOOMWatcher(ctx, containerId, pid, spec, request, outputLogger, &isOOMKilled)
	}()

	exitCode, _ = s.runContainer(ctx, request, outputLogger, outputWriter, startedChan, checkpointPIDChan, opts.StartupStartedAt, opts.StartupPortBindings, opts.CheckpointFilesystemRestore)

	stopReason := types.StopContainerReasonUnknown
	containerInstance, exists = s.containerInstances.Get(containerId)
	if exists {
		_, instanceStopReason := containerInstance.lifecycleState()
		if instanceStopReason != "" {
			stopReason = instanceStopReason
		}
	}
	rawExitCode := exitCode

	exitCode = normalizeContainerExitCode(exitCode, stopReason, isOOMKilled.Load())
	exitReason := containerExitReason(exitCode, stopReason, isOOMKilled.Load())

	logEvent := log.Info().
		Str("container_id", containerId).
		Int("exit_code", exitCode).
		Str("exit_reason", exitReason)
	if stopReason != types.StopContainerReasonUnknown {
		logEvent = logEvent.Str("stop_reason", string(stopReason))
	}
	logEvent.Msg("container process exited")
	s.recordContainerEvent(context.Background(), request, types.EventContainerEventSchema{
		ID:          types.ContainerEventRuntimeExited,
		ContainerID: containerId,
		Reason:      eventStopReason(stopReason),
		Source:      types.EventSourceWorkerRuntime.String(),
		Message:     types.EventMessageRuntimeExited.String(),
		Attrs: map[string]string{
			types.EventAttrRawExitCode:    fmt.Sprintf("%d", rawExitCode),
			types.EventAttrMappedExitCode: fmt.Sprintf("%d", exitCode),
			types.EventAttrOOMKilled:      fmt.Sprintf("%t", isOOMKilled.Load()),
			types.EventAttrExitReason:     exitReason,
			types.EventAttrReason:         string(stopReason),
		},
	})
	outputLogger.Info("", "done", true, "success", exitCode == 0)
	// Runtime deletion can be slow under burst load. Report completion before
	// cleanup so clients are never blocked on resource reclamation. Durable-disk
	// containers are reported by finalizeContainer after their final sync.
	if !request.HasDurableDiskMount() {
		exitReported = s.setContainerExitCode(containerId, exitCode)
	}
	if err := s.deleteRuntimeContainer(containerId); err != nil {
		log.Error().Str("container_id", containerId).Msgf("failed to delete container: %v", err)
	}
}

func (s *Worker) deleteRuntimeContainer(containerId string) error {
	if containerId == "" {
		return nil
	}

	instance, exists := s.containerInstances.Get(containerId)
	rt := s.runtime
	if exists && instance.Runtime != nil {
		rt = instance.Runtime
	}
	if rt == nil {
		return fmt.Errorf("runtime unavailable")
	}

	ctx, cancel := context.WithTimeout(context.Background(), runtimeDeleteTimeout)
	defer cancel()

	return rt.Delete(ctx, containerId, &runtime.DeleteOpts{Force: true})
}

func normalizeContainerExitCode(exitCode int, stopReason types.StopContainerReason, oomKilled bool) int {
	switch stopReason {
	case types.StopContainerReasonScheduler:
		return int(types.ContainerExitCodeScheduler)
	case types.StopContainerReasonTtl:
		return int(types.ContainerExitCodeTtl)
	case types.StopContainerReasonUser:
		return int(types.ContainerExitCodeUser)
	case types.StopContainerReasonAdmin:
		return int(types.ContainerExitCodeAdmin)
	}

	if oomKilled {
		return int(types.ContainerExitCodeOomKill)
	}
	if exitCode == int(types.ContainerExitCodeSigterm) {
		return int(types.ContainerExitCodeSuccess)
	}
	if exitCode < 0 {
		return int(types.ContainerExitCodeUnknownError)
	}
	return exitCode
}

func containerExitReason(exitCode int, stopReason types.StopContainerReason, oomKilled bool) string {
	if stopReason != types.StopContainerReasonUnknown {
		return string(stopReason)
	}
	if oomKilled {
		return "OOM"
	}
	switch exitCode {
	case int(types.ContainerExitCodeSuccess):
		return "COMPLETED"
	case int(types.ContainerExitCodeOomKill):
		return "SIGKILL"
	default:
		return "ERROR"
	}
}

func eventStopReason(stopReason types.StopContainerReason) string {
	if stopReason == types.StopContainerReasonUnknown {
		return ""
	}
	return string(stopReason)
}

func (s *Worker) runContainer(ctx context.Context, request *types.ContainerRequest, outputLogger *slog.Logger, outputWriter *common.OutputWriter, startedChan chan int, checkpointPIDChan chan int, startupStartedAt time.Time, startupPortBindings []PortBinding, filesystemRestore *checkpointFilesystemRestore) (int, error) {
	phaseStart := time.Now()
	releaseStartupSlot := func() {}
	if s.containerStartSem != nil {
		select {
		case s.containerStartSem <- struct{}{}:
		case <-ctx.Done():
			return -1, ctx.Err()
		}
		releaseStartupSlot = sync.OnceFunc(func() { <-s.containerStartSem })
		defer releaseStartupSlot()
	}
	metrics.RecordWorkerStartupPhase("worker_start_queue_wait", time.Since(phaseStart), request, map[string]string{
		"limit": fmt.Sprintf("%d", s.containerStartLimit),
	})
	s.recordStartupLifecycle(ctx, request, types.ContainerLifecycleStartQueueWait, phaseStart, true, map[string]string{"limit": fmt.Sprintf("%d", s.containerStartLimit)})

	instance, exists := s.containerInstances.Get(request.ContainerId)
	if !exists {
		return -1, fmt.Errorf("container instance not found")
	}
	if s.imageClient != nil {
		defer s.imageClient.untrackContainer(request.ContainerId)
	}

	supportsCheckpoint := s.supportsCheckpointRestore(request, instance.Runtime)
	restoringCheckpoint := s.canRestoreCheckpoint(request, instance.Runtime)
	checkpointRestoreStarted := time.Now()
	if filesystemRestore != nil {
		checkpointRestoreStarted = filesystemRestore.startedAt
	}
	bundlePath := filepath.Dir(request.ConfigPath)
	originalConfig, originalConfigErr := os.ReadFile(request.ConfigPath)
	if originalConfigErr != nil {
		log.Warn().Err(originalConfigErr).Str("container_id", request.ContainerId).Msg("failed to snapshot container config before checkpoint restore")
	}
	var checkpointRestoredOnce sync.Once
	markCheckpointRestored := func() {
		if !restoringCheckpoint {
			return
		}
		checkpointRestoredOnce.Do(func() {
			if err := s.updateCheckpointRestored(request.Checkpoint.CheckpointId); err != nil {
				log.Warn().Err(err).Str("checkpoint_id", request.Checkpoint.CheckpointId).Msg("failed to update checkpoint restore timestamp")
			}
			duration := time.Since(checkpointRestoreStarted)
			metrics.RecordWorkerStartupPhase("checkpoint_restore", duration, request, map[string]string{"success": "true"})
			log.Info().Str("container_id", request.ContainerId).Str("checkpoint_id", request.Checkpoint.CheckpointId).Dur("duration", duration).Msg("checkpoint restore completed")
			outputLogger.Info(fmt.Sprintf("Checkpoint found and restored in %s\n", duration.Round(time.Millisecond)))
		})
	}

	startAutoCheckpoint := func() {
		if supportsCheckpoint && request.CheckpointEnabled {
			s.startAutoCheckpoint(ctx, request, outputLogger, checkpointPIDChan)
		}
	}
	fallbackFromCheckpoint := func(reseedCheckpointFilesystem bool) error {
		restoringCheckpoint = false
		if reseedCheckpointFilesystem && originalConfigErr != nil {
			return fmt.Errorf("checkpoint mount validation fallback requires the original container config: %w", originalConfigErr)
		}
		if !reseedCheckpointFilesystem {
			request.Checkpoint = nil
		}
		var seedUpper func(string) error
		if reseedCheckpointFilesystem {
			seedUpper = func(upperPath string) error {
				return s.restoreCheckpointFilesystem(ctx, request, outputLogger, upperPath)
			}
		}
		if err := s.prepareRestoreFallback(request, originalConfig, seedUpper); err != nil {
			return fmt.Errorf("prepare checkpoint restore fallback: %w", err)
		}
		if reseedCheckpointFilesystem {
			request.Checkpoint = nil
		}
		startAutoCheckpoint()
		return nil
	}

	if !restoringCheckpoint {
		startAutoCheckpoint()
	}

	runtimeStartedChan := make(chan int, 1)
	runtimeStart := time.Now()
	var runtimeStartedPublished sync.Once
	var runtimeStartedPID atomic.Int64
	runtimeStartedPID.Store(-1)

	publishRuntimeStarted := func(pid int) {
		if err := s.publishContainerAddresses(ctx, request, startupPortBindings); err != nil {
			log.Error().Err(err).Str("container_id", request.ContainerId).Msg("failed to publish container address")
			s.handleObservedContainerStop(request.ContainerId, types.EventSourceWorkerRuntime, false)
			return
		}
		runtimeStartedPublished.Do(func() {
			select {
			case startedChan <- pid:
			case <-ctx.Done():
			}
		})
		s.markContainerRunning(ctx, request, startupStartedAt)
	}

	handleRuntimeStarted := func(pid int) {
		releaseStartupSlot()
		runtimeStartedPID.Store(int64(pid))
		if instance, exists := s.containerInstances.Get(request.ContainerId); exists {
			instance.markRuntimeStarted(pid)
			s.containerInstances.Set(request.ContainerId, instance)
		}
		if s.imageClient != nil {
			s.imageClient.trackContainerRuntimePID(request, pid)
		}

		metrics.RecordWorkerStartupPhase("runtime_start_to_pid", time.Since(runtimeStart), request, map[string]string{
			"runtime": instance.Runtime.Name(),
		})
		log.Info().Str("container_id", request.ContainerId).Dur("duration", time.Since(runtimeStart)).Msg("container runtime process started")
		s.recordContainerLifecycle(ctx, request, containerLifecycleFromDuration(types.ContainerLifecycleRuntimeStartToPID, request, runtimeStart, time.Since(runtimeStart), true, map[string]string{
			"runtime": instance.Runtime.Name(),
			"pid":     fmt.Sprintf("%d", pid),
		}))

		// A restore can emit a PID and still fail. Do not consume the one-shot
		// startup notification until the restore is known-good; a cold fallback
		// must publish its own PID to monitoring and checkpoint consumers.
		if !restoringCheckpoint {
			publishRuntimeStarted(pid)
		}
	}

	startRuntimeStartedHandler := func() func() {
		runtimeStartedDone := make(chan struct{})
		runtimeStartedHandled := make(chan struct{})
		var runtimeStartedOnce sync.Once
		go func() {
			defer close(runtimeStartedHandled)
			defer releaseStartupSlot()
			waitForRuntimeStarted(ctx, runtimeStartedChan, runtimeStartedDone, handleRuntimeStarted)
		}()
		return func() {
			runtimeStartedOnce.Do(func() {
				close(runtimeStartedDone)
				<-runtimeStartedHandled
			})
		}
	}
	finishRuntimeStarted := startRuntimeStartedHandler()

	// Handle restore from checkpoint if available
	if restoringCheckpoint {
		var restoreErr error
		if filesystemRestore != nil {
			restoreErr = filesystemRestore.wait()
		} else {
			if originalConfigErr != nil {
				restoreErr = fmt.Errorf("checkpoint filesystem restore requires the original container config: %w", originalConfigErr)
			} else {
				restoreErr = s.prepareRestoreFallback(request, originalConfig, func(upperPath string) error {
					return s.restoreCheckpointFilesystem(ctx, request, outputLogger, upperPath)
				})
			}
		}
		if restoreErr != nil {
			// A local fetch failure does not prove the checkpoint itself is invalid.
			if !request.Stub.Type.IsDeployment() {
				finishRuntimeStarted()
				return -1, restoreErr
			}
			if err := fallbackFromCheckpoint(false); err != nil {
				finishRuntimeStarted()
				return -1, err
			}
		} else if request.Checkpoint.IsFilesystemOnly() {
			markCheckpointRestored()
			request.Checkpoint = nil
			restoringCheckpoint = false
			startAutoCheckpoint()
			log.Info().Str("container_id", request.ContainerId).Msg("checkpoint filesystem restored; starting container cold")
			outputLogger.Info("Checkpoint filesystem restored; starting container normally\n")
		} else if forcedRuncCheckpointNeedsColdMigration(s.checkpointPath(request.Checkpoint.CheckpointId), request, instance.Runtime) {
			checkpointID := request.Checkpoint.CheckpointId
			if err := fallbackFromCheckpoint(true); err != nil {
				finishRuntimeStarted()
				return -1, fmt.Errorf("migrate legacy forced runc checkpoint %q: %w", checkpointID, err)
			}
			log.Info().Str("container_id", request.ContainerId).Str("checkpoint_id", checkpointID).Msg("legacy forced runc checkpoint filesystem restored; starting container cold")
			outputLogger.Info("Checkpoint security profile changed; starting from the saved filesystem\n")
		} else {
			if originalConfigErr == nil {
				if err := s.deferCheckpointRestoreCPUAffinity(request, originalConfig); err != nil {
					log.Warn().Err(err).Str("container_id", request.ContainerId).Msg("failed to defer CPU affinity for checkpoint restore")
					if restoreErr := os.WriteFile(request.ConfigPath, originalConfig, 0644); restoreErr != nil {
						finishRuntimeStarted()
						return -1, errors.Join(err, fmt.Errorf("restore constrained container config: %w", restoreErr))
					}
				}
			}
			runtimeStart = time.Now()
			phaseStarted := time.Now()
			exitCode, restored, restoreStarted, err := s.attemptRestoreCheckpoint(ctx, request, outputLogger, outputWriter, runtimeStartedChan, checkpointPIDChan)
			runtimeRestoreDuration := time.Since(phaseStarted)
			metrics.RecordWorkerStartupPhase("checkpoint_runtime_restore", runtimeRestoreDuration, request, map[string]string{"success": fmt.Sprintf("%t", restored && err == nil)})
			log.Info().Str("container_id", request.ContainerId).Str("checkpoint_id", request.Checkpoint.CheckpointId).Dur("duration", runtimeRestoreDuration).Bool("restored", restored).Err(err).Msg("checkpoint runtime restore finished")
			if restored {
				finishRuntimeStarted()
				if restoredPID := int(runtimeStartedPID.Load()); restoredPID > 0 {
					publishRuntimeStarted(restoredPID)
				} else {
					return exitCode, fmt.Errorf("checkpoint restore completed without a valid runtime pid")
				}
				markCheckpointRestored()
				if runtimeRestoreWaitsForExit(instance.Runtime) {
					return exitCode, err
				}
				if err != nil {
					return exitCode, err
				}
				exitCode, err = s.waitForRestoredContainerExit(ctx, instance.Runtime, request.ContainerId, exitCode)
				terminalCheckpoint := s.waitForTerminalAutoCheckpoint(ctx, request)
				if err != nil && terminalCheckpoint {
					log.Info().Str("container_id", request.ContainerId).Int("exit_code", exitCode).Msg("restored container exited after terminal checkpoint")
					err = nil
				}
				return exitCode, err
			}

			var durableMountValidationErr *checkpointDurableMountValidationError
			durableMountValidationFailed := errors.As(err, &durableMountValidationErr)
			runscVersionFallback := forcedRunscVersionFallback(request, err)
			var restoreCleanupErr *checkpointRestoreCleanupError
			restoreCleanupFailed := errors.As(err, &restoreCleanupErr)
			if (durableMountValidationFailed || runscVersionFallback) && restoreCleanupFailed {
				finishRuntimeStarted()
				return exitCode, err
			}
			// Non-deployment fallbacks are limited to forced requests whose
			// independently captured root filesystem can be safely reseeded.
			if !restored && !request.Stub.Type.IsDeployment() &&
				(!durableMountValidationFailed || !requestForcesResourceLimits(request)) && !runscVersionFallback {
				finishRuntimeStarted()
				return exitCode, err
			}
			if restoreStarted {
				finishRuntimeStarted()
				finishRuntimeStarted = startRuntimeStartedHandler()
			}
			if fallbackErr := fallbackFromCheckpoint(durableMountValidationFailed || runscVersionFallback); fallbackErr != nil {
				if !restoreStarted {
					finishRuntimeStarted()
				}
				return exitCode, errors.Join(err, fallbackErr)
			}
		}
	}

	if request.CheckpointEnabled {
		err := addEnvToSpec(request.ConfigPath, []string{fmt.Sprintf("CHECKPOINT_ENABLED=%t", request.CheckpointEnabled && s.IsCRIUAvailable(request.GpuCount))})
		if err != nil {
			log.Warn().Str("container_id", request.ContainerId).Msgf("failed to add checkpoint env var to spec: %v", err)
		}

	}

	select {
	case <-ctx.Done():
		return -1, ctx.Err()
	default:
	}
	if _, stopReason := instance.lifecycleState(); stopReason != "" {
		finishRuntimeStarted()
		return int(types.ContainerExitCodeSigterm), nil
	}

	runtimeStart = time.Now()
	exitCode, err := instance.Runtime.Run(context.WithoutCancel(ctx), request.ContainerId, bundlePath, &runtime.RunOpts{
		OutputWriter:  outputWriter,
		Started:       runtimeStartedChan,
		DockerEnabled: request.DockerEnabled,
	})
	terminalCheckpoint := s.waitForTerminalAutoCheckpoint(ctx, request)
	finishRuntimeStarted()
	if err != nil {
		if terminalCheckpoint {
			log.Info().Str("container_id", request.ContainerId).Int("exit_code", exitCode).Msg("container runtime exited after terminal checkpoint")
			err = nil
		} else {
			log.Warn().Str("container_id", request.ContainerId).Err(err).Msgf("error running container from bundle, exit code %d", exitCode)
		}
	}

	return exitCode, err
}

func (s *Worker) waitForRestoredContainerExit(ctx context.Context, rt runtime.Runtime, containerId string, fallbackExitCode int) (int, error) {
	ticker := time.NewTicker(restoredContainerPollInterval)
	defer ticker.Stop()
	observedRunning := false

	for {
		state, err := rt.State(ctx, containerId)
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return fallbackExitCode, ctxErr
			}
			if !observedRunning {
				return -1, fmt.Errorf("restored container state unavailable: %w", err)
			}
			return -1, nil
		}
		// A checkpoint temporarily reports the container as paused.
		if state.Status != types.RuncContainerStatusRunning && state.Status != types.RuncContainerStatusPaused {
			return -1, nil
		}
		observedRunning = true

		select {
		case <-ctx.Done():
			return fallbackExitCode, ctx.Err()
		case <-ticker.C:
		}
	}
}

type restoreWaitsForExitRuntime interface {
	RestoreWaitsForExit() bool
}

func runtimeRestoreWaitsForExit(rt runtime.Runtime) bool {
	restoreRuntime, ok := rt.(restoreWaitsForExitRuntime)
	return ok && restoreRuntime.RestoreWaitsForExit()
}

// CRIU moves CUDA restore helpers into the container cgroup. Let those helpers
// use the worker CPU set, then restore the workload affinity before publication.
func (s *Worker) deferCheckpointRestoreCPUAffinity(request *types.ContainerRequest, config []byte) error {
	if request == nil || request.ConfigPath == "" || len(config) == 0 {
		return nil
	}
	instance, exists := s.containerInstances.Get(request.ContainerId)
	if !exists || instance.Runtime == nil || instance.CPUSet == "" || runtimeRestoreWaitsForExit(instance.Runtime) {
		return nil
	}
	if _, ok := instance.Runtime.(containerResourceUpdater); !ok {
		return nil
	}

	var spec specs.Spec
	if err := json.Unmarshal(config, &spec); err != nil {
		return fmt.Errorf("decode container config: %w", err)
	}
	if spec.Linux == nil || spec.Linux.Resources == nil || spec.Linux.Resources.CPU == nil || spec.Linux.Resources.CPU.Cpus == "" {
		return nil
	}

	spec.Linux.Resources.CPU.Cpus = ""
	updated, err := json.Marshal(&spec)
	if err != nil {
		return fmt.Errorf("encode container config: %w", err)
	}
	if err := os.WriteFile(request.ConfigPath, updated, 0644); err != nil {
		return fmt.Errorf("write restore container config: %w", err)
	}

	instance.RestoreCPUAffinityDeferred = true
	s.containerInstances.Set(request.ContainerId, instance)
	return nil
}

func (s *Worker) applyDeferredCheckpointRestoreCPUAffinity(ctx context.Context, request *types.ContainerRequest) error {
	if request == nil {
		return nil
	}
	instance, exists := s.containerInstances.Get(request.ContainerId)
	if !exists || !instance.RestoreCPUAffinityDeferred {
		return nil
	}
	updater, ok := instance.Runtime.(containerResourceUpdater)
	if !ok {
		return fmt.Errorf("runtime %s does not support resource updates", instance.Runtime.Name())
	}

	updateCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cpuQuotaApplyTimeout)
	defer cancel()
	startedAt := time.Now()
	err := updater.UpdateResources(updateCtx, request.ContainerId, &specs.LinuxResources{
		CPU: &specs.LinuxCPU{Cpus: instance.CPUSet},
	})
	metrics.RecordWorkerStartupPhase("checkpoint_restore_cpu_affinity", time.Since(startedAt), request, map[string]string{"success": fmt.Sprintf("%t", err == nil)})
	if err != nil {
		return err
	}

	instance.RestoreCPUAffinityDeferred = false
	s.containerInstances.Set(request.ContainerId, instance)
	return nil
}

func (s *Worker) markContainerRunning(ctx context.Context, request *types.ContainerRequest, startupStartedAt time.Time) {
	containerId := request.ContainerId
	resp, err := s.getContainerStateForRunning(ctx, containerId)
	if err != nil {
		notFoundErr := &types.ErrContainerStateNotFound{}
		if notFoundErr.From(err) {
			log.Warn().Str("container_id", containerId).Msg("container state not found, returning")
			return
		}

		log.Error().Str("container_id", containerId).Err(err).Msg("failed to get container state before marking running")
		return
	}

	if resp.State.Status == string(types.ContainerStatusStopping) {
		log.Info().Str("container_id", containerId).Msg("container started after stop request, sending stop signal")
		s.handleObservedStoppingContainer(containerId, types.EventSourceWorkerStatusHeartbeat)
		return
	}

	phaseStart := time.Now()
	err = s.updateContainerStatusRunning(ctx, containerId)
	metrics.RecordWorkerStartupPhase("set_running_status", time.Since(phaseStart), request, map[string]string{"success": fmt.Sprintf("%t", err == nil)})
	if err != nil {
		log.Error().Str("container_id", containerId).Err(err).Msg("failed to update container status to running")
		return
	}
	if !startupStartedAt.IsZero() {
		startupLatency := time.Since(startupStartedAt)
		metrics.RecordWorkerStartupLatency(startupLatency, request)
		s.recordContainerLifecycle(ctx, request, containerLifecycleFromDuration(types.ContainerLifecycleStartup, request, startupStartedAt, startupLatency, true, map[string]string{
			"status": string(types.ContainerStatusRunning),
		}))
	}
}

func (s *Worker) getContainerStateForRunning(ctx context.Context, containerId string) (*pb.GetContainerStateResponse, error) {
	retryCtx, cancel := context.WithTimeout(ctx, markRunningRetryTimeout)
	defer cancel()

	var lastErr error
	for {
		resp, err := handleGRPCResponse(s.containerRepoClient.GetContainerState(retryCtx, &pb.GetContainerStateRequest{ContainerId: containerId}))
		if err == nil {
			return resp, nil
		}
		lastErr = err
		if err := waitForMarkRunningRetry(retryCtx, lastErr); err != nil {
			return resp, err
		}
	}
}

func (s *Worker) updateContainerStatusRunning(ctx context.Context, containerId string) error {
	retryCtx, cancel := context.WithTimeout(ctx, markRunningRetryTimeout)
	defer cancel()

	var lastErr error
	for {
		_, err := handleGRPCResponse(s.containerRepoClient.UpdateContainerStatus(retryCtx, &pb.UpdateContainerStatusRequest{
			ContainerId:   containerId,
			Status:        string(types.ContainerStatusRunning),
			ExpirySeconds: int64(types.ContainerStateTtlS),
		}))
		if err == nil {
			return nil
		}
		lastErr = err
		if err := waitForMarkRunningRetry(retryCtx, lastErr); err != nil {
			return err
		}
	}
}

func waitForMarkRunningRetry(ctx context.Context, lastErr error) error {
	timer := time.NewTimer(markRunningRetryInterval)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		if ctx.Err() == context.DeadlineExceeded && lastErr != nil {
			return lastErr
		}
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func waitForRuntimeStarted(ctx context.Context, runtimeStarted <-chan int, runtimeDone <-chan struct{}, handle func(int)) {
	select {
	case pid := <-runtimeStarted:
		handle(pid)
	case <-ctx.Done():
	case <-runtimeDone:
		select {
		case pid := <-runtimeStarted:
			handle(pid)
		default:
		}
	}
}

func (s *Worker) createOverlay(request *types.ContainerRequest, bundlePath string) *common.ContainerOverlay {
	// For images that have a rootfs path, set that as the root path
	// otherwise, assume OCI spec files are in the root
	rootPath := filepath.Join(bundlePath, "rootfs")
	if _, err := os.Stat(rootPath); os.IsNotExist(err) {
		rootPath = bundlePath
	}

	return common.NewContainerOverlay(request, rootPath, s.containerOverlayBasePath(request))
}

func (s *Worker) containerOverlayBasePath(request *types.ContainerRequest) string {
	if s.useMemoryOverlay(request) {
		return "/dev/shm"
	}
	return baseConfigPath
}

func (s *Worker) getContainerResources(request *types.ContainerRequest) (*specs.LinuxResources, error) {
	var resources ContainerResources

	instance, exists := s.containerInstances.Get(request.ContainerId)
	if exists && instance.Runtime != nil && instance.Runtime.Name() == types.ContainerRuntimeGvisor.String() {
		resources = NewGvisorResources()
	} else {
		resources = NewRuncResources()
	}

	return &specs.LinuxResources{
		CPU:    resources.GetCPU(request),
		Memory: resources.GetMemory(request),
	}, nil
}

func (s *Worker) deferCPUThrottle(request *types.ContainerRequest, cpu *specs.LinuxCPU) bool {
	if cpu == nil {
		return false
	}
	kind := request.Stub.Type.Kind()
	if kind != types.StubTypeSandbox && kind != types.StubTypeFunction {
		return false
	}
	if kind == types.StubTypeFunction && !s.agentWorker() {
		return false
	}

	instance, exists := s.containerInstances.Get(request.ContainerId)
	if !exists || instance.Runtime == nil {
		return false
	}
	if _, ok := instance.Runtime.(containerResourceUpdater); !ok {
		return false
	}

	instance.DeferredCPUQuota = cpu
	s.containerInstances.Set(request.ContainerId, instance)
	return true
}

func (s *Worker) applyDeferredCPUThrottle(request *types.ContainerRequest, instance *ContainerInstance) error {
	if instance == nil || instance.DeferredCPUQuota == nil {
		return nil
	}

	updater, ok := instance.Runtime.(containerResourceUpdater)
	if !ok {
		if instance.Runtime == nil {
			return fmt.Errorf("runtime is nil")
		}
		return fmt.Errorf("runtime %s does not support resource updates", instance.Runtime.Name())
	}

	ctx, cancel := context.WithTimeout(context.Background(), cpuQuotaApplyTimeout)
	defer cancel()

	resources := &specs.LinuxResources{CPU: instance.DeferredCPUQuota}
	if err := updater.UpdateResources(ctx, request.ContainerId, resources); err != nil {
		return err
	}

	instance.DeferredCPUQuota = nil
	s.containerInstances.Set(request.ContainerId, instance)
	return nil
}

func (s *Worker) hasDeferredCPUThrottle(containerID string) bool {
	if s == nil || s.containerInstances == nil {
		return false
	}
	instance, exists := s.containerInstances.Get(containerID)
	return exists && instance.DeferredCPUQuota != nil
}

func (s *Worker) applyDeferredCPUThrottleAfterRunnerReady(ctx context.Context, request *types.ContainerRequest) error {
	parentCtx := ctx
	ctx, cancel := context.WithTimeout(ctx, runnerReadyTimeout)
	defer cancel()

	stopOnFailure := func(err error) error {
		if err == nil || errors.Is(err, context.Canceled) {
			return err
		}
		if stopErr := s.stopContainer(request.ContainerId, true); stopErr != nil {
			return errors.Join(err, fmt.Errorf("stop unthrottled container: %w", stopErr))
		}
		return err
	}

	readyPath := filepath.Join(runnerSignalDir(request.ContainerId), filepath.Base(types.ContainerRunnerReadyPath))
	ticker := time.NewTicker(runnerReadyPollInterval)
	defer ticker.Stop()

waitForReady:
	for {
		if _, statErr := os.Stat(readyPath); statErr == nil {
			break
		} else if !os.IsNotExist(statErr) {
			return stopOnFailure(statErr)
		}

		select {
		case <-ctx.Done():
			if err := parentCtx.Err(); err != nil {
				return err
			}
			log.Debug().Str("container_id", request.ContainerId).Msg("runner readiness timed out; applying CPU quota")
			break waitForReady
		case <-ticker.C:
		}
	}

	instance, exists := s.containerInstances.Get(request.ContainerId)
	if !exists {
		return stopOnFailure(fmt.Errorf("container instance not found"))
	}
	return stopOnFailure(s.applyDeferredCPUThrottle(request, instance))
}
