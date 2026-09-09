package managedendpoint

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	abstractions "github.com/beam-cloud/beta9/pkg/abstractions/common"
	"github.com/beam-cloud/beta9/pkg/abstractions/common/llmroute"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

// Replica lifecycle: observe container state, probe health when no harness
// is present, and start / drain / stop containers.

var probeClient = &http.Client{Timeout: 3 * time.Second}

// observeReplicas syncs each replica record with its container and returns
// the replicas that are still alive.
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
		before := *current
		if err := c.syncReplica(ctx, current); err != nil {
			return err
		}
		out = current
		if before.Status != current.Status || before.Address != current.Address || before.WorkerID != current.WorkerID ||
			before.Capacity != current.Capacity || before.StatusReason != current.StatusReason || !before.ReadyAt.Equal(current.ReadyAt) ||
			!before.LastHeartbeat.Equal(current.LastHeartbeat) || !before.EndedAt.Equal(current.EndedAt) {
			return c.s.repo.SaveReplica(ctx, current)
		}
		return nil
	})
	return out, err
}

// syncReplica folds the container's state into the replica. Terminal
// transitions stop the container when it is still around.
func (c *controller) syncReplica(ctx context.Context, replica *types.EndpointReplica) error {
	now := time.Now()
	state, err := c.s.containers.GetContainerState(replica.ContainerID)
	if err != nil || state == nil {
		// The container record is gone: it exited, was evicted, or was never
		// scheduled. Give the scheduler a moment after Run before concluding.
		if now.Sub(replica.StartedAt) < containerLostGrace {
			return nil
		}
		if replica.Status == types.ReplicaStatusScheduling {
			// Opportunistic requests fail fast when no worker has idle
			// capacity; the failure backoff keeps the fill loop from
			// re-submitting every tick.
			if status, err := c.s.containers.GetContainerRequestStatus(replica.ContainerID); err == nil && status == types.ContainerRequestStatusFailed {
				return c.finishReplica(ctx, replica, types.ReplicaStatusFailed, "not scheduled: no idle capacity")
			}
		}
		return c.finishReplica(ctx, replica, c.exitStatus(replica), "container exited")
	}
	if state.WorkerId != "" && replica.WorkerID == "" {
		replica.WorkerID = state.WorkerId
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
				// The scheduler picked this replica as a victim for a
				// serverless workload. Pull it from rotation now; the worker
				// gives it DrainSeconds to finish in-flight requests.
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

	port, spec := c.replicaSpec(ctx, replica)
	if replica.Address == "" {
		if addresses, err := c.s.containers.GetContainerAddressMap(replica.ContainerID); err == nil {
			if addr, ok := addresses[int32(port)]; ok && strings.TrimSpace(addr) != "" {
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
		replica.Status = types.ReplicaStatusLoading
		replica.StatusReason = ""
	}

	if replica.HarnessEnabled {
		if !replica.LastHeartbeat.IsZero() && c.silentFor(replica.LastHeartbeat, now) > c.s.config.ReplicaStaleAfterOrDefault() {
			return c.stopAndFinish(ctx, replica, types.ReplicaStatusFailed, "harness heartbeat stale")
		}
	} else if replica.Address != "" {
		c.probeReplica(ctx, replica, spec)
	}
	if replica.Status == types.ReplicaStatusLoading && now.Sub(replica.StartedAt) > loadingGrace {
		return c.stopAndFinish(ctx, replica, types.ReplicaStatusFailed, "did not become ready within grace period")
	}
	return nil
}

// exitStatus decides what a vanished container means for its replica.
func (c *controller) exitStatus(replica *types.EndpointReplica) types.ReplicaStatus {
	if replica.Status == types.ReplicaStatusEvicting {
		return types.ReplicaStatusEvicted
	}
	exitCode, err := c.s.containers.GetContainerExitCode(replica.ContainerID)
	// The worker's exit code is authoritative for evictions: an engine that
	// drains fast on SIGTERM reports "draining" through the harness before
	// the controller ever sees the container marked as a victim.
	if err == nil && exitCode == int(types.ContainerExitCodeEvicted) {
		return types.ReplicaStatusEvicted
	}
	if replica.Status == types.ReplicaStatusDraining || (err == nil && exitCode == 0) {
		return types.ReplicaStatusStopped
	}
	if !replica.Protected && replica.Status == types.ReplicaStatusReady {
		// Unprotected replicas that were serving are most often preempted.
		return types.ReplicaStatusEvicted
	}
	return types.ReplicaStatusFailed
}

// probeReplica drives status for endpoints without a harness: readiness from
// the health path and, for LLM engines, capacity from Prometheus metrics.
func (c *controller) probeReplica(ctx context.Context, replica *types.EndpointReplica, spec *types.ManagedEndpointSpec) {
	baseURL := "http://" + replica.Address
	paths := llmroute.ReadinessPaths("")
	if spec != nil && strings.TrimSpace(spec.Health) != "" {
		paths = []string{spec.Health}
	}
	ready := llmroute.CheckReady(ctx, probeClient, baseURL, paths, probeClient.Timeout)
	now := time.Now()
	replica.LastHeartbeat = now
	switch {
	case ready && replica.Status != types.ReplicaStatusReady:
		replica.Status = types.ReplicaStatusReady
		replica.ReadyAt = now
		replica.StatusReason = ""
		c.s.replicaEvent(replica, "replica.ready", "", nil)
	case !ready && replica.Status == types.ReplicaStatusReady:
		replica.Status = types.ReplicaStatusLoading
		replica.StatusReason = "health check failing"
	}
	if !ready || spec == nil || spec.Kind != types.EndpointKindLLM {
		return
	}
	metrics, ok, err := llmroute.FetchEngineMetrics(ctx, probeClient, baseURL+llmroute.NormalizeMetricsPath(spec.Metrics), llmroute.EngineMetrics{})
	if err != nil || !ok {
		return
	}
	replica.Capacity.Running = metrics.RunningRequests
	replica.Capacity.Waiting = metrics.WaitingRequests
	replica.Capacity.TTFTMs = metrics.TTFTMs
	replica.Capacity.TPOTMs = metrics.TPOTMs
	replica.Capacity.DecodeTokensPerSec = metrics.DecodeTokensPerSecond
	replica.Capacity.PromptTokensPerSec = metrics.PromptTokensPerSecond
	replica.Capacity.KVCacheFreeMilli = 1000 - metrics.GPUCacheUsageMilli
	replica.Capacity.PrefixCacheHitMilli = metrics.PrefixCacheHitMilli
}

// replicaSpec returns the port a replica serves on and, for endpoint
// replicas, the endpoint spec (nil for service replicas).
func (c *controller) replicaSpec(ctx context.Context, replica *types.EndpointReplica) (uint32, *types.ManagedEndpointSpec) {
	if strings.HasPrefix(replica.EndpointID, serviceReplicaPrefix) {
		if service, err := c.s.repo.GetService(ctx, strings.TrimPrefix(replica.EndpointID, serviceReplicaPrefix)); err == nil && service != nil {
			return service.Spec.Port, nil
		}
		return 0, nil
	}
	if endpoint, err := c.s.repo.GetEndpoint(ctx, replica.EndpointID); err == nil && endpoint != nil {
		return endpoint.Spec.Port, &endpoint.Spec
	}
	return 0, nil
}

// finishReplica records a terminal status.
func (c *controller) finishReplica(ctx context.Context, replica *types.EndpointReplica, status types.ReplicaStatus, reason string) error {
	replica.Status = status
	replica.StatusReason = reason
	replica.EndedAt = time.Now()
	if status == types.ReplicaStatusFailed {
		_ = c.s.repo.SetScheduleBackoff(ctx, replica.EndpointID, targetKey(replica.Role, replica.GPU), c.s.config.Fill.FailureBackoffOrDefault())
	}
	c.s.replicaEvent(replica, "replica."+string(status), reason, nil)
	replicaLog(replica).Info().Str("status", string(status)).Str("reason", reason).Msg("managed endpoints: replica finished")
	return nil
}

// stopAndFinish stops the container and records the terminal status.
func (c *controller) stopAndFinish(ctx context.Context, replica *types.EndpointReplica, status types.ReplicaStatus, reason string) error {
	if c.s.scheduler != nil {
		if err := c.s.scheduler.Stop(&types.StopContainerArgs{ContainerId: replica.ContainerID, Force: true, Reason: types.StopContainerReasonScheduler}); err != nil {
			replicaLog(replica).Warn().Err(err).Msg("managed endpoints: stop container failed")
		}
	}
	return c.finishReplica(ctx, replica, status, reason)
}

// drainReplica takes a replica out of rotation and asks the harness (or the
// deadline) to finish in-flight work before the container is stopped.
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

// startSpec is everything needed to launch one replica container.
type startSpec struct {
	EndpointID string
	Version    uint
	StubID     string
	Role       string
	Target     types.GpuTarget
	Port       uint32
	Locality   string
	PoolName   string
	Protected  bool
	Tuning     bool
	Harness    bool
	Entrypoint []string
	// Services maps a service name to its address for BEAM_SERVICE_* env.
	Services map[string]string
	// KVCache is the endpoint's kv_cache spec, injected as JSON for the harness.
	KVCache *types.KVCacheSpec
	// Evictable replicas may be preempted by serverless workloads.
	Evictable bool
	// DrainSeconds is the grace an evicted replica gets to finish in-flight
	// requests before the worker kills it.
	DrainSeconds uint32
	GitSHA       string
}

// evictOrder ranks roles for preemption: prefill replicas go first because
// decode replicas hold the KV cache for in-flight generations.
func evictOrder(role string) int32 {
	switch role {
	case types.ReplicaRolePrefill:
		return 0
	case types.ReplicaRoleDecode:
		return 2
	default:
		return 1
	}
}

// startReplica submits a container request for one replica and records it.
func (c *controller) startReplica(ctx context.Context, spec startSpec) (*types.EndpointReplica, error) {
	stub, stubConfig, err := c.stub(ctx, spec.StubID)
	if err != nil {
		return nil, err
	}
	workspace, err := c.s.AdminWorkspace(ctx)
	if err != nil {
		return nil, err
	}
	tokenKey, err := c.s.runtimeTokenKey(ctx)
	if err != nil {
		return nil, err
	}
	replicaID := fmt.Sprintf("%s-%s", strings.ReplaceAll(spec.EndpointID, "/", "-"), uuid.New().String()[:8])
	containerID := fmt.Sprintf("%s-%s-%s", containerPrefix, stub.ExternalId, uuid.New().String()[:8])

	mounts, err := abstractions.ConfigureContainerRequestMounts(containerID, stub, workspace, *stubConfig)
	if err != nil {
		return nil, err
	}
	secrets, err := abstractions.ConfigureContainerRequestSecrets(workspace, *stubConfig)
	if err != nil {
		return nil, err
	}

	env := append(append([]string{}, stubConfig.Env...), secrets...)
	env = append(env,
		"BETA9_TOKEN="+tokenKey,
		"STUB_ID="+stub.ExternalId,
		"STUB_TYPE="+string(stub.Type),
		EnvEndpointID+"="+spec.EndpointID,
		EnvReplicaID+"="+replicaID,
		EnvReplicaRole+"="+spec.Role,
		EnvGpuTarget+"="+spec.Target.Key(),
		EnvLocality+"="+spec.Locality,
		fmt.Sprintf("%s=%d", EnvEndpointPort, spec.Port),
		fmt.Sprintf("%s=%t", EnvHarnessEnabled, spec.Harness),
		fmt.Sprintf("%s=%d", EnvDrainSeconds, spec.DrainSeconds),
	)
	if len(spec.Target.Harness) > 0 {
		env = append(env, EnvHarnessConfig+"="+mustJSON(spec.Target.Harness))
	}
	if spec.KVCache != nil {
		env = append(env, EnvKVCache+"="+mustJSON(spec.KVCache))
	}
	for name, addr := range spec.Services {
		env = append(env, EnvServicePrefix+strings.ToUpper(strings.ReplaceAll(name, "-", "_"))+"="+addr)
	}

	entrypoint := spec.Entrypoint
	if len(entrypoint) == 0 {
		entrypoint = stubConfig.EntryPoint
	}
	var gpu string
	var gpuRequest []string
	var gpuCount uint32
	if !spec.Target.IsCPU() {
		gpu = string(types.NormalizeGPUType(spec.Target.Type))
		gpuRequest = []string{gpu}
		gpuCount = spec.Target.Count
	}
	request := &types.ContainerRequest{
		ContainerId:       containerID,
		EntryPoint:        appendEngineArgs(entrypoint, spec.Target.EngineArgs),
		Env:               env,
		Cpu:               stubConfig.Runtime.Cpu,
		Memory:            stubConfig.Runtime.Memory,
		Gpu:               gpu,
		GpuRequest:        gpuRequest,
		GpuCount:          gpuCount,
		ImageId:           stubConfig.Runtime.ImageId,
		StubId:            stub.ExternalId,
		AppId:             stub.App.ExternalId,
		WorkspaceId:       workspace.ExternalId,
		Workspace:         *workspace,
		Stub:              *stub,
		Mounts:            mounts,
		Ports:             []uint32{spec.Port},
		PoolSelector:      spec.PoolName,
		Evictable:         spec.Evictable && !spec.Protected,
		OpportunisticOnly: !spec.Protected,
		DrainSeconds:      spec.DrainSeconds,
		EvictOrder:        evictOrder(spec.Role),
		Timestamp:         time.Now(),
	}
	if err := abstractions.ConfigureContainerRequestNetwork(request, *stubConfig); err != nil {
		return nil, err
	}

	replica := &types.EndpointReplica{
		ID:             replicaID,
		EndpointID:     spec.EndpointID,
		Version:        spec.Version,
		Role:           spec.Role,
		GPU:            spec.Target.Key(),
		GPUCount:       gpuCount,
		Locality:       spec.Locality,
		PoolName:       spec.PoolName,
		ContainerID:    containerID,
		Status:         types.ReplicaStatusScheduling,
		Protected:      spec.Protected,
		Tuning:         spec.Tuning,
		HarnessEnabled: spec.Harness,
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
	c.s.replicaEvent(replica, "replica.scheduled", "", map[string]any{"protected": spec.Protected, "tuning": spec.Tuning, "git_sha": spec.GitSHA})
	log.Info().Str("endpoint_id", replica.EndpointID).Str("replica_id", replica.ID).Str("gpu", replica.GPU).
		Str("pool", replica.PoolName).Bool("protected", replica.Protected).Msg("managed endpoints: replica scheduled")
	return replica, nil
}

// appendEngineArgs adds target-specific engine args to the stub's entrypoint.
// SDK entrypoints are `sh -c "<script>"`, so args are appended to the script;
// plain argv entrypoints get them appended as arguments.
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
