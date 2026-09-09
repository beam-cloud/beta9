package managedendpoint

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// EndpointHarnessService: called from inside replica containers.

const (
	maxEventsPerPublish = 256
	watchKeepAlive      = 30 * time.Second
)

func (s *Service) Register(ctx context.Context, in *pb.HarnessRegisterRequest) (*pb.HarnessRegisterResponse, error) {
	if err := s.authorizeHarness(ctx); err != nil {
		return nil, err
	}
	containerID := strings.TrimSpace(in.ContainerId)
	if containerID == "" {
		return &pb.HarnessRegisterResponse{Ok: false, ErrMsg: "container_id is required"}, nil
	}

	replica, err := s.repo.GetReplicaByContainer(ctx, containerID)
	if err != nil {
		return nil, rpcError(err)
	}
	if replica == nil {
		return &pb.HarnessRegisterResponse{Ok: false, ErrMsg: "container is not a managed endpoint replica"}, nil
	}
	if replica.Status.Terminal() {
		return &pb.HarnessRegisterResponse{Ok: false, ErrMsg: "replica is " + string(replica.Status)}, nil
	}

	err = s.repo.WithReplicaLock(ctx, replica.ID, func(ctx context.Context) error {
		current, err := s.repo.GetReplica(ctx, replica.ID)
		if err != nil {
			return err
		}
		if current == nil {
			return notFound("replica", replica.ID)
		}
		current.HarnessEnabled = true
		current.LastHeartbeat = time.Now()
		if in.CapabilitiesJson != "" && json.Valid([]byte(in.CapabilitiesJson)) {
			current.Capabilities = json.RawMessage(in.CapabilitiesJson)
		}
		if current.Status == types.ReplicaStatusScheduling {
			current.Status = types.ReplicaStatusLoading
		}
		replica = current
		return s.repo.SaveReplica(ctx, current)
	})
	if err != nil {
		return nil, rpcError(err)
	}

	revision, err := s.effectiveRevision(ctx, replica)
	if err != nil {
		return nil, rpcError(err)
	}

	s.emit(types.EventEndpointHarness, types.EventEndpointSchema{
		EndpointID:  replica.EndpointID,
		Action:      "harness.registered",
		ReplicaID:   replica.ID,
		ContainerID: replica.ContainerID,
		GPU:         replica.GPU,
		Role:        replica.Role,
		Version:     replica.Version,
		Data: map[string]any{
			"engine":         in.Engine,
			"engine_version": in.EngineVersion,
			"limits":         json.RawMessage(nonEmptyJSON(in.LimitsJson)),
		},
	})

	return &pb.HarnessRegisterResponse{
		Ok:                       true,
		ReplicaId:                replica.ID,
		EndpointId:               replica.EndpointID,
		Role:                     replica.Role,
		Gpu:                      replica.GPU,
		Locality:                 replica.Locality,
		HeartbeatIntervalSeconds: uint32(s.config.HeartbeatIntervalOrDefault().Seconds()),
		Current:                  revisionToProto(revision),
	}, nil
}

func nonEmptyJSON(raw string) string {
	if strings.TrimSpace(raw) == "" || !json.Valid([]byte(raw)) {
		return "null"
	}
	return raw
}

func (s *Service) WatchConfig(in *pb.HarnessWatchConfigRequest, stream pb.EndpointHarnessService_WatchConfigServer) error {
	ctx := stream.Context()
	if err := s.authorizeHarness(ctx); err != nil {
		return err
	}
	replica, err := s.repo.GetReplica(ctx, in.ReplicaId)
	if err != nil {
		return rpcError(err)
	}
	if replica == nil {
		return status.Error(codes.NotFound, "replica not found")
	}

	updates, err := s.repo.SubscribeConfigRevisions(ctx, replica.EndpointID)
	if err != nil {
		return rpcError(err)
	}

	sent := in.AfterRevision
	send := func(revision *types.EndpointConfigRevision) error {
		if revision == nil || revision.Revision <= sent {
			return nil
		}
		if err := stream.Send(revisionToProto(revision)); err != nil {
			return err
		}
		sent = revision.Revision
		return nil
	}

	current, err := s.effectiveRevision(ctx, replica)
	if err != nil {
		return rpcError(err)
	}
	if err := send(current); err != nil {
		return err
	}

	keepAlive := time.NewTicker(watchKeepAlive)
	defer keepAlive.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-s.ctx.Done():
			return nil
		case <-keepAlive.C:
			// Re-resolve so a missed pub/sub message (or a replica flipping in
			// or out of tuning) converges on the next tick.
			fresh, err := s.repo.GetReplica(ctx, replica.ID)
			if err != nil || fresh == nil || fresh.Status.Terminal() {
				return nil
			}
			replica = fresh
			current, err := s.effectiveRevision(ctx, replica)
			if err != nil {
				continue
			}
			if err := send(current); err != nil {
				return err
			}
		case revision, ok := <-updates:
			if !ok {
				return nil
			}
			if !revisionTargets(revision, replica) {
				continue
			}
			// A replica in tuning ignores fleet revisions while it has its own.
			if replica.Tuning && revision.Scope == types.ConfigScopeTarget {
				own, err := s.repo.LatestConfigRevision(ctx, replica.EndpointID, types.ConfigScopeReplica, replica.ID)
				if err == nil && own != nil {
					continue
				}
			}
			if err := send(revision); err != nil {
				return err
			}
		}
	}
}

func revisionTargets(revision *types.EndpointConfigRevision, replica *types.EndpointReplica) bool {
	if revision == nil || replica == nil || revision.EndpointID != replica.EndpointID {
		return false
	}
	switch revision.Scope {
	case types.ConfigScopeReplica:
		return revision.ScopeKey == replica.ID
	case types.ConfigScopeTarget:
		return revision.ScopeKey == fleetKey(replica.Role, replica.GPU, replica.Version)
	}
	return false
}

func (s *Service) AckConfig(ctx context.Context, in *pb.HarnessAckConfigRequest) (*pb.HarnessAckConfigResponse, error) {
	if err := s.authorizeHarness(ctx); err != nil {
		return nil, err
	}
	replica, err := s.repo.GetReplica(ctx, in.ReplicaId)
	if err != nil {
		return nil, rpcError(err)
	}
	if replica == nil {
		return &pb.HarnessAckConfigResponse{Ok: false, ErrMsg: "replica not found"}, nil
	}

	ack := &types.ConfigAck{ReplicaID: replica.ID, Revision: in.Revision, Applied: in.Applied, Error: in.Error, At: time.Now()}
	if in.EffectiveJson != "" && json.Valid([]byte(in.EffectiveJson)) {
		ack.Effective = json.RawMessage(in.EffectiveJson)
	}
	if err := s.repo.SaveConfigAck(ctx, ack); err != nil {
		return nil, rpcError(err)
	}

	if in.Applied {
		err = s.repo.WithReplicaLock(ctx, replica.ID, func(ctx context.Context) error {
			current, err := s.repo.GetReplica(ctx, replica.ID)
			if err != nil || current == nil {
				return err
			}
			if in.Revision > current.ConfigRevision {
				current.ConfigRevision = in.Revision
			}
			return s.repo.SaveReplica(ctx, current)
		})
		if err != nil {
			return nil, rpcError(err)
		}
	}

	action := "config.applied"
	if !in.Applied {
		action = "config.rejected"
	}
	s.emit(types.EventEndpointConfig, types.EventEndpointSchema{
		EndpointID: replica.EndpointID,
		Action:     action,
		ReplicaID:  replica.ID,
		GPU:        replica.GPU,
		Role:       replica.Role,
		Revision:   in.Revision,
		Message:    in.Error,
	})
	return &pb.HarnessAckConfigResponse{Ok: true}, nil
}

func (s *Service) Heartbeat(ctx context.Context, in *pb.HarnessHeartbeatRequest) (*pb.HarnessHeartbeatResponse, error) {
	if err := s.authorizeHarness(ctx); err != nil {
		return nil, err
	}
	var replica *types.EndpointReplica
	err := s.repo.WithReplicaLock(ctx, in.ReplicaId, func(ctx context.Context) error {
		current, err := s.repo.GetReplica(ctx, in.ReplicaId)
		if err != nil {
			return err
		}
		if current == nil {
			return notFound("replica", in.ReplicaId)
		}
		s.applyHeartbeat(current, in)
		replica = current
		return s.repo.SaveReplica(ctx, current)
	})
	if err != nil {
		return nil, rpcError(err)
	}

	drain, drainSeconds, err := s.repo.DrainRequested(ctx, replica.ID)
	if err != nil {
		return nil, rpcError(err)
	}
	return &pb.HarnessHeartbeatResponse{
		Ok:                       true,
		Drain:                    drain || replica.Status == types.ReplicaStatusDraining || replica.Status == types.ReplicaStatusEvicting,
		DrainSeconds:             drainSeconds,
		HeartbeatIntervalSeconds: uint32(s.config.HeartbeatIntervalOrDefault().Seconds()),
	}, nil
}

// applyHeartbeat folds a heartbeat into the replica record. Status
// transitions reported by the harness are only accepted while the replica is
// alive; control-plane states (draining/evicting/terminal) are sticky.
func (s *Service) applyHeartbeat(replica *types.EndpointReplica, in *pb.HarnessHeartbeatRequest) {
	now := time.Now()
	replica.LastHeartbeat = now
	replica.HarnessEnabled = true
	if in.Capacity != nil {
		replica.Capacity = capacityFromProto(in.Capacity)
	}
	if in.AppliedRevision > replica.ConfigRevision {
		replica.ConfigRevision = in.AppliedRevision
	}
	switch replica.Status {
	case types.ReplicaStatusDraining, types.ReplicaStatusEvicting, types.ReplicaStatusEvicted, types.ReplicaStatusFailed, types.ReplicaStatusStopped:
		return
	}
	switch types.ReplicaStatus(strings.ToLower(strings.TrimSpace(in.Status))) {
	case types.ReplicaStatusReady:
		if replica.Status != types.ReplicaStatusReady {
			replica.ReadyAt = now
			replica.StatusReason = ""
			s.emit(types.EventEndpointReplica, types.EventEndpointSchema{
				EndpointID: replica.EndpointID, Action: "replica.ready", ReplicaID: replica.ID,
				ContainerID: replica.ContainerID, GPU: replica.GPU, Role: replica.Role, Version: replica.Version,
				WorkerID: replica.WorkerID, PoolName: replica.PoolName, Locality: replica.Locality,
			})
		}
		replica.Status = types.ReplicaStatusReady
	case types.ReplicaStatusLoading:
		if replica.Status == types.ReplicaStatusScheduling {
			replica.Status = types.ReplicaStatusLoading
		}
	case types.ReplicaStatusDraining:
		replica.Status = types.ReplicaStatusDraining
	}
}

func (s *Service) PublishEvents(ctx context.Context, in *pb.HarnessPublishEventsRequest) (*pb.HarnessPublishEventsResponse, error) {
	if err := s.authorizeHarness(ctx); err != nil {
		return nil, err
	}
	replica, err := s.repo.GetReplica(ctx, in.ReplicaId)
	if err != nil {
		return nil, rpcError(err)
	}
	if replica == nil {
		return &pb.HarnessPublishEventsResponse{Ok: false, ErrMsg: "replica not found"}, nil
	}

	var accepted uint32
	for i, event := range in.Events {
		if i >= maxEventsPerPublish || event == nil || strings.TrimSpace(event.Name) == "" {
			continue
		}
		var data map[string]any
		if event.PayloadJson != "" {
			_ = json.Unmarshal([]byte(event.PayloadJson), &data)
		}
		at := time.Now()
		if event.AtUnixMs > 0 {
			at = time.UnixMilli(event.AtUnixMs)
		}
		s.emit(types.EventEndpointHarness, types.EventEndpointSchema{
			EndpointID:  replica.EndpointID,
			Action:      "harness." + strings.TrimPrefix(event.Name, "harness."),
			ReplicaID:   replica.ID,
			ContainerID: replica.ContainerID,
			GPU:         replica.GPU,
			Role:        replica.Role,
			Version:     replica.Version,
			Revision:    replica.ConfigRevision,
			Data:        data,
			Timestamp:   at,
		})
		accepted++
	}
	return &pb.HarnessPublishEventsResponse{Ok: true, Accepted: accepted}, nil
}
