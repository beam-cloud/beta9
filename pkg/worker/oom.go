package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"time"

	runtime "github.com/beam-cloud/beta9/pkg/runtime"
	types "github.com/beam-cloud/beta9/pkg/types"
	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/rs/zerolog/log"
)

const (
	gvisorOOMKillTimeout   = 5 * time.Second
	gvisorOOMDeleteTimeout = 5 * time.Second
)

// setupOOMWatcher starts OOM monitoring for the container.
func (s *Worker) setupOOMWatcher(
	ctx context.Context,
	containerId string,
	pid int,
	spec *specs.Spec,
	request *types.ContainerRequest,
	outputLogger *slog.Logger,
	isOOMKilled *atomic.Bool,
) {
	var newOOMWatcher func() runtime.OOMWatcher
	containerInstance, exists := s.containerInstances.Get(containerId)
	if !exists {
		return
	}
	containerRuntime := containerInstance.Runtime
	if containerRuntime == nil {
		containerRuntime = s.runtime
	}
	if containerRuntime == nil {
		return
	}

	if containerRuntime.Name() == types.ContainerRuntimeGvisor.String() {
		if !s.memoryLimitsEnforced(request) {
			return
		}
		// runsc places the sandbox and gofer in a cgroup named after the
		// container. memory.events accounts shared mappings once and only
		// reports an OOM after the kernel actually enforces the hard limit.
		cgroupPath := containerId
		if spec != nil && spec.Linux != nil && spec.Linux.CgroupsPath != "" {
			cgroupPath = strings.TrimPrefix(spec.Linux.CgroupsPath, "/")
		}
		newOOMWatcher = func() runtime.OOMWatcher {
			return runtime.NewCgroupOOMWatcher(ctx, cgroupPath)
		}
	} else {
		cgroupPath, err := runtime.GetCgroupPathFromPID(pid)
		if err != nil {
			log.Warn().Str("container_id", containerId).Err(err).Msg("failed to get cgroup path, OOM detection disabled")
			return
		}
		newOOMWatcher = func() runtime.OOMWatcher {
			return runtime.NewCgroupOOMWatcher(ctx, cgroupPath)
		}
	}

	containerInstance.installOOMWatcher(func(onOOM func()) runtime.OOMWatcher {
		oomWatcher := newOOMWatcher()
		err := oomWatcher.Watch(onOOM)
		if err != nil {
			oomWatcher.Stop()
			log.Warn().Str("container_id", containerId).Err(err).Msg("OOM watcher failed to start")
			return nil
		}
		return oomWatcher
	}, func() error {
		return s.handleOOMKill(ctx, containerId, request, outputLogger, isOOMKilled)
	})
}

// handleOOMKill handles the OOM kill event
func (s *Worker) handleOOMKill(
	ctx context.Context,
	containerId string,
	request *types.ContainerRequest,
	outputLogger *slog.Logger,
	isOOMKilled *atomic.Bool,
) error {
	log.Warn().Str("container_id", containerId).Msg("OOM kill detected")
	isOOMKilled.Store(true)
	outputLogger.Info(types.WorkerContainerExitCodeOomKillMessage)

	go s.recordContainerEvent(ctx, request, types.EventContainerEventSchema{
		ID:        types.ContainerEventRuntimeOOMKilled,
		Domain:    types.EventDomainRuntime,
		Timestamp: time.Now().UTC(),
		Reason:    "OOM",
		Source:    types.EventSourceWorkerRuntime.String(),
		Message:   types.EventMessageRuntimeOOMKilled.String(),
		Attrs: map[string]string{
			types.EventAttrOOMKilled: "true",
		},
	})

	// For gVisor, manually stop the container (kernel won't do it automatically).
	// A restored container may use a different runtime than the worker default.
	containerRuntime := s.runtime
	if instance, exists := s.containerInstances.Get(containerId); exists {
		if _, stopReason := instance.lifecycleState(); stopReason == "" {
			instance.setStopReason(types.StopContainerReasonUnknown)
			s.containerInstances.Set(containerId, instance)
		}
		if instance.Runtime != nil {
			containerRuntime = instance.Runtime
		}
	}
	if containerRuntime != nil && containerRuntime.Name() == types.ContainerRuntimeGvisor.String() {
		log.Info().Str("container_id", containerId).Msg("stopping container due to OOM (gVisor)")
		// Fence local lifecycle work immediately. The bounded runtime commands stay
		// synchronous with watcher arbitration, so no kill escapes into CRIU.
		s.cancelContainer(containerId)
		if err := s.terminateGvisorAfterOOM(ctx, containerId, containerRuntime); err != nil {
			log.Error().Str("container_id", containerId).Err(err).Msg("failed to stop OOM container")
			return err
		}
	}

	return nil
}

func (s *Worker) terminateGvisorAfterOOM(ctx context.Context, containerId string, containerRuntime runtime.Runtime) error {
	killCtx, cancelKill := oomOperationContext(ctx, gvisorOOMKillTimeout)
	// OOM termination is not an automatic scheduler/TTL stop. It must not be
	// deferred behind an in-flight checkpoint because the runtime is already in
	// an unsafe terminal condition.
	killErr := s.stopContainerWithoutCheckpointDeferral(killCtx, containerId, true)
	cancelKill()
	if killErr == nil || runtimeContainerNotFound(killErr) {
		return nil
	}

	deleteCtx, cancelDelete := context.WithTimeout(context.Background(), gvisorOOMDeleteTimeout)
	deleteErr := containerRuntime.Delete(deleteCtx, containerId, &runtime.DeleteOpts{Force: true})
	cancelDelete()
	if deleteErr == nil || runtimeContainerNotFound(deleteErr) {
		return nil
	}

	return fmt.Errorf("gVisor OOM termination failed: %w", errors.Join(
		fmt.Errorf("kill: %w", killErr),
		fmt.Errorf("force delete: %w", deleteErr),
	))
}

func oomOperationContext(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	deadline := time.Now().Add(timeout)
	if parentDeadline, ok := parent.Deadline(); ok && parentDeadline.Before(deadline) {
		deadline = parentDeadline
	}
	return context.WithDeadline(context.WithoutCancel(parent), deadline)
}
