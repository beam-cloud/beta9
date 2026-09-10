package managedendpoint

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	abstractions "github.com/beam-cloud/beta9/pkg/abstractions/common"
	"github.com/beam-cloud/beta9/pkg/abstractions/common/llmroute"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

const probeTimeout = 3 * time.Second

// observeReplicas syncs each replica with its container and returns the ones still alive.
func (c *controller) observeReplicas(ctx context.Context, replicas []*types.EndpointReplica) []*types.EndpointReplica {
	live := make([]*types.EndpointReplica, 0, len(replicas))
	for _, replica := range replicas {
		if replica.Status.Terminal() {
			if !replica.EndedAt.IsZero() && time.Since(replica.EndedAt) > terminalRetention {
				_ = c.s.repo.DeleteReplica(ctx, replica.ID)
			}
			continue
		}
		updated, err := c.observeReplica(ctx, replica)
		if err != nil {
			replicaLog(replica).Warn().Err(err).Msg("managed endpoints: observe replica failed")
			live = append(live, replica)
			continue
		}
		if updated != nil && !updated.Status.Terminal() {
			live = append(live, updated)
		}
	}
	return live
}

func (c *controller) observeReplica(ctx context.Context, replica *types.EndpointReplica) (*types.EndpointReplica, error) {
	var out *types.EndpointReplica
	err := c.s.repo.WithReplicaLock(ctx, replica.ID, func(ctx context.Context) error {
		current, err := c.s.repo.GetReplica(ctx, replica.ID)
		if err != nil || current == nil || current.Status.Terminal() {
			out = current
			return err
		}
		before := mustJSON(current)
		if err := c.syncReplica(ctx, current); err != nil {
			return err
		}
		out = current
		if mustJSON(current) == before {
			return nil
		}
		return c.s.repo.SaveReplica(ctx, current)
	})
	return out, err
}

// syncReplica folds the container's state into the replica.
func (c *controller) syncReplica(ctx context.Context, replica *types.EndpointReplica) error {
	now := time.Now()
	state, err := c.s.containers.GetContainerState(replica.ContainerID)
	if err != nil && !containerStateNotFound(err) {
		return err
	}
	if err != nil || state == nil {
		// The container is gone: exited, evicted, or never scheduled.
		if now.Sub(replica.StartedAt) < containerLostGrace {
			return nil
		}
		if replica.Status == types.ReplicaStatusScheduling {
			if status, err := c.s.containers.GetContainerRequestStatus(replica.ContainerID); err == nil && status == types.ContainerRequestStatusFailed {
				return c.finishReplica(ctx, replica, types.ReplicaStatusFailed, "scheduler failed the request")
			}
		}
		return c.finishReplica(ctx, replica, c.exitStatus(replica), "container exited")
	}
	if state.WorkerId != "" && replica.WorkerID == "" {
		replica.WorkerID, replica.MachineID = state.WorkerId, state.MachineId
		if cfg, ok := c.poolConfig(replica.PoolName); ok && cfg.Mode == types.PoolModeProvider {
			if worker, err := c.s.workers.GetWorkerById(state.WorkerId); err == nil && worker != nil {
				replica.ProviderWorkspaceID = worker.WorkspaceId
			}
		}
	}

	switch state.Status {
	case types.ContainerStatusPending:
		if now.Sub(replica.StartedAt) > schedulingGrace {
			return c.stopAndFinish(ctx, replica, types.ReplicaStatusFailed, "not scheduled within grace period")
		}
		return nil
	case types.ContainerStatusStopping:
		if replica.Alive() {
			if state.Evicting {
				// Picked as an eviction victim; the worker gives it DrainSeconds.
				replica.Status = types.ReplicaStatusEvicting
				replica.StatusReason = "evicted for higher priority workload"
				replica.DrainDeadline = now.Add(time.Duration(state.DrainSeconds) * time.Second)
				if replica.HarnessEnabled {
					if err := c.s.repo.RequestDrain(ctx, replica.ID, state.DrainSeconds); err != nil {
						replicaLog(replica).Warn().Err(err).Msg("managed endpoints: request drain for evicted replica failed")
					}
				}
				c.s.replicaEvent(replica, "replica.evicting", replica.StatusReason, nil)
				replicaLog(replica).Info().Msg("managed endpoints: replica evicted by scheduler")
			} else {
				replica.Status = types.ReplicaStatusDraining
				replica.StatusReason = "container stopping"
				if replica.DrainDeadline.IsZero() {
					replica.DrainDeadline = now
				}
			}
		}
		return nil
	case types.ContainerStatusRunning:
	default:
		return nil
	}

	if replica.Address == "" {
		if addresses, err := c.s.containers.GetContainerAddressMap(replica.ContainerID); err == nil {
			if addr, ok := addresses[int32(replica.Probe.Port)]; ok && strings.TrimSpace(addr) != "" {
				replica.Address = addr
			}
		}
	}

	switch replica.Status {
	case types.ReplicaStatusDraining, types.ReplicaStatusEvicting:
		if !replica.DrainDeadline.IsZero() && now.After(replica.DrainDeadline) {
			final := types.ReplicaStatusStopped
			if replica.Status == types.ReplicaStatusEvicting {
				final = types.ReplicaStatusEvicted
			}
			return c.stopAndFinish(ctx, replica, final, "drain complete")
		}
		return nil
	case types.ReplicaStatusScheduling:
		replica.EnterLoading(now, "")
	}

	if replica.HarnessEnabled {
		if !replica.LastHeartbeat.IsZero() && c.silentFor(replica.LastHeartbeat, now) > c.s.config.ReplicaStaleAfter {
			return c.stopAndFinish(ctx, replica, types.ReplicaStatusFailed, "harness heartbeat stale")
		}
	}
	if replica.Address != "" && (!replica.HarnessEnabled || replica.EngineReady) {
		c.probeReplica(ctx, replica)
	}
	if replica.Status == types.ReplicaStatusLoading && replica.LoadingFor(now) > loadingGrace {
		return c.stopAndFinish(ctx, replica, types.ReplicaStatusFailed, "did not become ready within grace period")
	}
	return nil
}

// containerStateNotFound matches the typed sentinel or its string form after crossing a wrapper.
func containerStateNotFound(err error) bool {
	var notFound *types.ErrContainerStateNotFound
	return errors.As(err, &notFound) || (&types.ErrContainerStateNotFound{}).From(err)
}

// exitStatus decides what a vanished container means for its replica.
func (c *controller) exitStatus(replica *types.EndpointReplica) types.ReplicaStatus {
	if replica.Status == types.ReplicaStatusEvicting {
		return types.ReplicaStatusEvicted
	}
	exitCode, err := c.s.containers.GetContainerExitCode(replica.ContainerID)
	// The worker's exit code is authoritative for evictions.
	if err == nil && exitCode == int(types.ContainerExitCodeEvicted) {
		return types.ReplicaStatusEvicted
	}
	if replica.Status == types.ReplicaStatusDraining || (err == nil && exitCode == 0) {
		return types.ReplicaStatusStopped
	}
	return types.ReplicaStatusFailed
}

// probeReplica verifies the serving path even when the engine has a harness.
// Harness heartbeats remain authoritative for engine liveness and capacity.
func (c *controller) probeReplica(ctx context.Context, replica *types.EndpointReplica) {
	client := c.s.probeClient(replica.Address)
	baseURL := "http://replica"
	paths := llmroute.ReadinessPaths("")
	if strings.TrimSpace(replica.Probe.Health) != "" {
		paths = []string{replica.Probe.Health}
	}
	ready := llmroute.CheckReady(ctx, client, baseURL, paths, probeTimeout)
	now := time.Now()
	if !replica.HarnessEnabled {
		replica.LastHeartbeat = now
	}
	switch {
	case ready && replica.Status != types.ReplicaStatusReady:
		replica.Status = types.ReplicaStatusReady
		replica.ReadyAt = now
		replica.StatusReason = ""
		c.s.replicaEvent(replica, "replica.ready", "", nil)
	case !ready && replica.Status == types.ReplicaStatusReady:
		replica.EnterLoading(now, "health check failing")
	}
	if !ready || replica.HarnessEnabled || replica.Probe.Metrics == "" {
		return
	}
	// Rates derive from counter deltas, so the previous scrape is kept per replica.
	c.metricsMu.Lock()
	previous := c.lastMetrics[replica.ID]
	c.metricsMu.Unlock()
	metrics, ok, err := llmroute.FetchEngineMetrics(ctx, client, baseURL+replica.Probe.Metrics, previous)
	if err != nil || !ok {
		return
	}
	c.metricsMu.Lock()
	c.lastMetrics[replica.ID] = metrics
	c.metricsMu.Unlock()
	replica.Capacity.Running = metrics.RunningRequests
	replica.Capacity.Waiting = metrics.WaitingRequests
	replica.Capacity.TTFTMs = metrics.TTFTMs
	replica.Capacity.TPOTMs = metrics.TPOTMs
	replica.Capacity.DecodeTokensPerSec = metrics.DecodeTokensPerSecond
	replica.Capacity.PromptTokensPerSec = metrics.PromptTokensPerSecond
	replica.Capacity.KVCacheFreeMilli = 1000 - metrics.GPUCacheUsageMilli
	replica.Capacity.PrefixCacheHitMilli = metrics.PrefixCacheHitMilli
}

// probeFor snapshots the probe contract of the deployment a replica is started from.
func probeFor(spec *types.ManagedEndpointSpec) types.ReplicaProbe {
	probe := types.ReplicaProbe{Port: spec.Port, Health: spec.Health}
	if spec.Kind == types.EndpointKindLLM {
		probe.Metrics = llmroute.NormalizeMetricsPath(spec.Metrics)
	}
	return probe
}

// finishReplica records a terminal status.
func (c *controller) finishReplica(ctx context.Context, replica *types.EndpointReplica, status types.ReplicaStatus, reason string) error {
	replica.Status = status
	replica.StatusReason = reason
	replica.EndedAt = time.Now()
	c.s.forgetTransport(replica.Address)
	c.metricsMu.Lock()
	delete(c.lastMetrics, replica.ID)
	c.metricsMu.Unlock()
	if status == types.ReplicaStatusFailed {
		_ = c.s.repo.SetScheduleBackoff(ctx, replica.EndpointID, replica.GPU, c.s.config.Reconcile.FailureBackoff)
	}
	c.s.replicaEvent(replica, "replica."+string(status), reason, nil)
	replicaLog(replica).Info().Str("status", string(status)).Str("reason", reason).Msg("managed endpoints: replica finished")
	return nil
}

// stopAndFinish stops the container and records the terminal status. A failed
// stop leaves the replica live so the next pass retries it.
func (c *controller) stopAndFinish(ctx context.Context, replica *types.EndpointReplica, status types.ReplicaStatus, reason string) error {
	if c.s.scheduler != nil {
		if err := c.s.scheduler.Stop(&types.StopContainerArgs{ContainerId: replica.ContainerID, Force: true, Reason: types.StopContainerReasonScheduler}); err != nil {
			replica.StatusReason = "stop failed: " + err.Error()
			return fmt.Errorf("stop container %s: %w", replica.ContainerID, err)
		}
	}
	return c.finishReplica(ctx, replica, status, reason)
}

// drainReplica takes a replica out of rotation and lets in-flight work finish before the stop.
func (c *controller) drainReplica(ctx context.Context, replica *types.EndpointReplica, drainSeconds uint32, evict bool, reason string) error {
	return c.s.repo.WithReplicaLock(ctx, replica.ID, func(ctx context.Context) error {
		current, err := c.s.repo.GetReplica(ctx, replica.ID)
		if err != nil || current == nil || !current.Alive() {
			return err
		}
		defer func() { *replica = *current }()
		if drainSeconds == 0 || current.Status != types.ReplicaStatusReady {
			final := types.ReplicaStatusStopped
			if evict {
				final = types.ReplicaStatusEvicted
			}
			if err := c.stopAndFinish(ctx, current, final, reason); err != nil {
				return err
			}
			return c.s.repo.SaveReplica(ctx, current)
		}
		current.Status = types.ReplicaStatusDraining
		if evict {
			current.Status = types.ReplicaStatusEvicting
		}
		current.StatusReason = reason
		current.DrainDeadline = time.Now().Add(time.Duration(drainSeconds) * time.Second)
		if err := c.s.repo.RequestDrain(ctx, current.ID, drainSeconds); err != nil {
			return err
		}
		c.s.replicaEvent(current, "replica."+string(current.Status), reason, nil)
		return c.s.repo.SaveReplica(ctx, current)
	})
}

// startReplica submits a container request for one replica and records it.
func (c *controller) startReplica(ctx context.Context, endpoint *types.ManagedEndpoint, gpu string, pool eligiblePool) (*types.EndpointReplica, error) {
	stub, err := c.s.backend.GetStubByExternalId(ctx, endpoint.StubID)
	if err != nil {
		return nil, err
	}
	if stub == nil || stub.ExternalId == "" {
		return nil, fmt.Errorf("stub %q: %w", endpoint.StubID, errNotFound)
	}
	stubConfig, err := stub.UnmarshalConfig()
	if err != nil {
		return nil, err
	}
	workspace, err := c.s.AdminWorkspace(ctx)
	if err != nil {
		return nil, err
	}
	replicaID := fmt.Sprintf("%s-%s", strings.ReplaceAll(endpoint.Spec.ID, "/", "-"), uuid.New().String()[:8])
	containerID := fmt.Sprintf("%s-%s-%s", containerPrefix, stub.ExternalId, uuid.New().String()[:8])
	replicaSecret, secretHash := newReplicaSecret()

	mounts, err := abstractions.ConfigureContainerRequestMounts(containerID, stub, workspace, *stubConfig)
	if err != nil {
		return nil, err
	}
	secrets, err := abstractions.ConfigureContainerRequestSecrets(workspace, *stubConfig)
	if err != nil {
		return nil, err
	}

	gpuSpec := endpoint.Spec.Gpu[gpu]
	drainSeconds := endpoint.Spec.DrainSeconds
	env := append(append([]string{}, stubConfig.Env...), secrets...)
	env = append(env,
		EnvReplicaSecret+"="+replicaSecret,
		"STUB_ID="+stub.ExternalId,
		"STUB_TYPE="+string(stub.Type),
		EnvEndpointID+"="+endpoint.Spec.ID,
		EnvReplicaID+"="+replicaID,
		EnvGpu+"="+gpu,
		EnvLocality+"="+pool.Locality,
		fmt.Sprintf("%s=%d", EnvEndpointPort, endpoint.Spec.Port),
		fmt.Sprintf("%s=%t", EnvHarnessEnabled, endpoint.Spec.Harness),
		fmt.Sprintf("%s=%d", EnvDrainSeconds, drainSeconds),
	)
	if len(gpuSpec.Harness) > 0 {
		env = append(env, EnvHarnessConfig+"="+mustJSON(gpuSpec.Harness))
	}

	entrypoint := endpoint.Spec.Entrypoint
	if len(entrypoint) == 0 {
		entrypoint = stubConfig.EntryPoint
	}
	var requestGpu string
	var gpuRequest []string
	var gpuCount uint32
	if gpu != types.CPUInventoryKey {
		requestGpu, gpuRequest, gpuCount = gpu, []string{gpu}, max(gpuSpec.Count, 1)
	}
	request := &types.ContainerRequest{
		ContainerId:       containerID,
		EntryPoint:        appendEngineArgs(entrypoint, gpuSpec.EngineArgs),
		Env:               env,
		Cpu:               stubConfig.Runtime.Cpu,
		Memory:            stubConfig.Runtime.Memory,
		Gpu:               requestGpu,
		GpuRequest:        gpuRequest,
		GpuCount:          gpuCount,
		ImageId:           stubConfig.Runtime.ImageId,
		StubId:            stub.ExternalId,
		AppId:             stub.App.ExternalId,
		WorkspaceId:       workspace.ExternalId,
		Workspace:         *workspace,
		Stub:              *stub,
		Mounts:            mounts,
		Ports:             []uint32{endpoint.Spec.Port},
		PoolSelector:      pool.Name,
		OpportunisticOnly: true,
		Evictable:         c.s.config.Preemption.Enabled && !endpoint.Spec.Protected,
		DrainSeconds:      drainSeconds,
		Timestamp:         time.Now(),
	}
	if err := abstractions.ConfigureContainerRequestNetwork(request, *stubConfig); err != nil {
		return nil, err
	}

	replica := &types.EndpointReplica{
		ID:             replicaID,
		EndpointID:     endpoint.Spec.ID,
		Version:        endpoint.Version,
		GPU:            gpu,
		GPUCount:       gpuCount,
		Locality:       pool.Locality,
		PoolName:       pool.Name,
		ContainerID:    containerID,
		Status:         types.ReplicaStatusScheduling,
		SecretHash:     secretHash,
		HarnessEnabled: endpoint.Spec.Harness,
		Probe:          probeFor(&endpoint.Spec),
		StartedAt:      time.Now(),
	}
	if err := c.s.repo.SaveReplica(ctx, replica); err != nil {
		return nil, err
	}
	if err := c.s.scheduler.Run(request); err != nil {
		_ = c.finishReplica(ctx, replica, types.ReplicaStatusFailed, "scheduler rejected request: "+err.Error())
		_ = c.s.repo.SaveReplica(ctx, replica)
		return nil, err
	}
	c.s.replicaEvent(replica, "replica.scheduled", "", map[string]any{"git_sha": endpoint.GitSHA})
	log.Info().Str("endpoint_id", replica.EndpointID).Str("replica_id", replica.ID).Str("gpu", replica.GPU).
		Str("pool", replica.PoolName).Msg("managed endpoints: replica scheduled")
	return replica, nil
}

// appendEngineArgs appends engine args to a `sh -c` script or a plain argv entrypoint.
func appendEngineArgs(entrypoint []string, args []string) []string {
	if len(args) == 0 {
		return entrypoint
	}
	out := append([]string{}, entrypoint...)
	if len(out) >= 3 && (out[0] == "sh" || out[0] == "/bin/sh" || out[0] == "bash") && out[1] == "-c" {
		quoted := make([]string, 0, len(args))
		for _, a := range args {
			quoted = append(quoted, shellQuote(a))
		}
		out[2] = strings.TrimSpace(out[2]) + " " + strings.Join(quoted, " ")
		return out
	}
	return append(out, args...)
}

func shellQuote(s string) string {
	if s == "" {
		return "''"
	}
	if !strings.ContainsAny(s, " \t\n'\"\\$`!*?[]{}()<>|&;#~") {
		return s
	}
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}
