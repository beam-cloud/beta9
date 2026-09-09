package managedendpoint

import (
	"encoding/json"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
)

func unixMs(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixMilli()
}

func mustJSON(v any) string {
	if v == nil {
		return ""
	}
	data, err := json.Marshal(v)
	if err != nil {
		return ""
	}
	return string(data)
}

func capacityToProto(c types.ReplicaCapacity) *pb.ReplicaCapacity {
	out := &pb.ReplicaCapacity{
		InFlight:            c.InFlight,
		MaxConcurrency:      c.MaxConcurrency,
		Running:             c.Running,
		Waiting:             c.Waiting,
		KvCacheFreeMilli:    c.KVCacheFreeMilli,
		DecodeTokensPerSec:  c.DecodeTokensPerSec,
		PromptTokensPerSec:  c.PromptTokensPerSec,
		TtftMs:              c.TTFTMs,
		TpotMs:              c.TPOTMs,
		PrefixCacheHitMilli: c.PrefixCacheHitMilli,
	}
	if c.KVTransfer != nil {
		out.KvTransferJson = mustJSON(c.KVTransfer)
	}
	return out
}

func capacityFromProto(c *pb.ReplicaCapacity) types.ReplicaCapacity {
	if c == nil {
		return types.ReplicaCapacity{}
	}
	out := types.ReplicaCapacity{
		InFlight:            c.InFlight,
		MaxConcurrency:      c.MaxConcurrency,
		Running:             c.Running,
		Waiting:             c.Waiting,
		KVCacheFreeMilli:    c.KvCacheFreeMilli,
		DecodeTokensPerSec:  c.DecodeTokensPerSec,
		PromptTokensPerSec:  c.PromptTokensPerSec,
		TTFTMs:              c.TtftMs,
		TPOTMs:              c.TpotMs,
		PrefixCacheHitMilli: c.PrefixCacheHitMilli,
	}
	if c.KvTransferJson != "" {
		var kv types.KVTransferStats
		if err := json.Unmarshal([]byte(c.KvTransferJson), &kv); err == nil {
			out.KVTransfer = &kv
		}
	}
	return out
}

func revisionToProto(r *types.EndpointConfigRevision) *pb.ConfigRevision {
	if r == nil {
		return nil
	}
	return &pb.ConfigRevision{
		Revision:        r.Revision,
		EndpointId:      r.EndpointID,
		Scope:           string(r.Scope),
		ScopeKey:        r.ScopeKey,
		ConfigJson:      mustJSON(r.Config),
		Author:          r.Author,
		Source:          string(r.Source),
		CreatedAtUnixMs: unixMs(r.CreatedAt),
	}
}

func replicaToProto(r *types.EndpointReplica) *pb.EndpointReplica {
	if r == nil {
		return nil
	}
	return &pb.EndpointReplica{
		Id:                  r.ID,
		EndpointId:          r.EndpointID,
		Version:             uint32(r.Version),
		Role:                r.Role,
		Gpu:                 r.GPU,
		GpuCount:            r.GPUCount,
		Locality:            r.Locality,
		PoolName:            r.PoolName,
		ContainerId:         r.ContainerID,
		WorkerId:            r.WorkerID,
		Address:             r.Address,
		Status:              string(r.Status),
		Protected:           r.Protected,
		Candidate:           r.Candidate,
		Tuning:              r.Tuning,
		HarnessEnabled:      r.HarnessEnabled,
		ConfigRevision:      r.ConfigRevision,
		Capacity:            capacityToProto(r.Capacity),
		CapabilitiesJson:    string(r.Capabilities),
		StartedAtUnixMs:     unixMs(r.StartedAt),
		ReadyAtUnixMs:       unixMs(r.ReadyAt),
		LastHeartbeatUnixMs: unixMs(r.LastHeartbeat),
		StatusReason:        r.StatusReason,
	}
}

func endpointToProto(e *types.ManagedEndpoint) *pb.ManagedEndpoint {
	if e == nil {
		return nil
	}
	return &pb.ManagedEndpoint{
		Id:              e.Spec.ID,
		SpecJson:        mustJSON(e.Spec),
		StubId:          e.StubID,
		Version:         uint32(e.Version),
		GitSha:          e.GitSHA,
		Enabled:         e.Enabled,
		Status:          string(e.Status),
		CreatedAtUnixMs: unixMs(e.CreatedAt),
		UpdatedAtUnixMs: unixMs(e.UpdatedAt),
	}
}

func serviceToProto(s *types.ManagedService) *pb.ManagedService {
	if s == nil {
		return nil
	}
	return &pb.ManagedService{
		Name:     s.Spec.Name,
		SpecJson: mustJSON(s.Spec),
		StubId:   s.StubID,
		Version:  uint32(s.Version),
		GitSha:   s.GitSHA,
		Enabled:  s.Enabled,
		Status:   string(s.Status),
	}
}

func versionToProto(v *types.EndpointVersion) *pb.EndpointVersion {
	return &pb.EndpointVersion{
		EndpointId:      v.EndpointID,
		Version:         uint32(v.Version),
		StubId:          v.StubID,
		GitSha:          v.GitSHA,
		State:           string(v.State),
		CreatedAtUnixMs: unixMs(v.CreatedAt),
	}
}

func rolloutToProto(r *types.RolloutState, versions []*types.EndpointVersion) *pb.RolloutState {
	if r == nil {
		return nil
	}
	out := &pb.RolloutState{
		EndpointId:           r.EndpointID,
		ActiveVersion:        uint32(r.ActiveVersion),
		CanaryVersion:        uint32(r.CanaryVersion),
		PinnedVersion:        uint32(r.PinnedVersion),
		Phase:                r.Phase,
		BakeStartedAtUnixMs:  unixMs(r.BakeStartedAt),
		LastDecision:         r.LastDecision,
		LastDecisionAtUnixMs: unixMs(r.LastDecisionAt),
	}
	for _, v := range versions {
		out.Versions = append(out.Versions, versionToProto(v))
	}
	return out
}

func gitopsToProto(state *types.GitOpsState) *pb.GitOpsState {
	if state == nil {
		return nil
	}
	out := &pb.GitOpsState{
		RepoUrl:         state.RepoURL,
		Ref:             state.Ref,
		LastSha:         state.LastSHA,
		TargetSha:       state.TargetSHA,
		LastRunAtUnixMs: unixMs(state.LastRunAt),
		LastError:       state.LastError,
		Running:         state.Running,
	}
	for _, e := range state.PerEndpoint {
		out.Endpoints = append(out.Endpoints, &pb.GitOpsEndpointState{
			Path:            e.Path,
			Id:              e.ID,
			Kind:            e.Kind,
			AppliedSha:      e.AppliedSHA,
			Status:          e.Status,
			Error:           e.Error,
			StubId:          e.StubID,
			Version:         uint32(e.Version),
			UpdatedAtUnixMs: unixMs(e.UpdatedAt),
		})
	}
	return out
}

func experimentToProto(e *types.Experiment, replica *types.EndpointReplica) *pb.Experiment {
	if e == nil {
		return nil
	}
	out := &pb.Experiment{
		Id:               e.ID,
		EndpointId:       e.EndpointID,
		Gpu:              e.GPU,
		Role:             e.Role,
		ReplicaId:        e.ReplicaID,
		BaselineRevision: e.BaselineRevision,
		CurrentRevision:  e.CurrentRevision,
		Outcome:          string(e.Outcome),
		Notes:            e.Notes,
		Author:           e.Author,
		Budget:           e.Budget,
		StartedAtUnixMs:  unixMs(e.StartedAt),
		EndedAtUnixMs:    unixMs(e.EndedAt),
		Replica:          replicaToProto(replica),
	}
	for _, step := range e.Steps {
		out.Steps = append(out.Steps, &pb.ExperimentStep{
			Revision:      step.Revision,
			ConfigJson:    mustJSON(step.Config),
			Applied:       step.Applied,
			Error:         step.Error,
			BenchJson:     string(step.Bench),
			EngineMetrics: capacityToProto(step.EngineMetrics),
			AtUnixMs:      unixMs(step.At),
		})
	}
	return out
}

func routeMetricsToProto(m *types.RouteMetrics, replicas []*types.EndpointReplica) *pb.EndpointMetrics {
	if m == nil {
		return nil
	}
	out := &pb.EndpointMetrics{
		EndpointId:       m.EndpointID,
		Gpu:              m.GPU,
		WindowSeconds:    uint32(m.Window.Seconds()),
		Requests:         m.Requests,
		Errors:           m.Errors,
		PromptTokens:     m.PromptTokens,
		CompletionTokens: m.CompletionTokens,
		TtftMsP50:        m.MeanTTFTMs(),
		TpotMsP50:        m.MeanTPOTMs(),
		CostMicroUsd:     m.CostMicroUSD,
	}
	if m.Requests > 0 {
		out.QueueWaitMsP95 = m.QueueWaitSumMs / m.Requests
	}
	aggregate := types.ReplicaCapacity{}
	for _, replica := range replicas {
		if replica.Status != types.ReplicaStatusReady {
			continue
		}
		out.ReadyReplicas++
		aggregate.InFlight += replica.Capacity.InFlight
		aggregate.MaxConcurrency += replica.Capacity.MaxConcurrency
		aggregate.Running += replica.Capacity.Running
		aggregate.Waiting += replica.Capacity.Waiting
		aggregate.DecodeTokensPerSec += replica.Capacity.DecodeTokensPerSec
		aggregate.PromptTokensPerSec += replica.Capacity.PromptTokensPerSec
	}
	out.DecodeTokensPerSec = aggregate.DecodeTokensPerSec
	out.AggregateCapacity = capacityToProto(aggregate)
	return out
}
