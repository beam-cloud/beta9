package managedendpoint

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
)

// EndpointHarnessService: called from inside replica containers.

const (
	maxEventsPerPublish = 256
	watchKeepAlive      = 30 * time.Second
)

// updateReplica applies fn to the stored replica under its lock and saves it.
// It returns the saved record.
func (s *Service) updateReplica(ctx context.Context, replicaID string, fn func(*types.EndpointReplica)) (*types.EndpointReplica, error) {
	var out *types.EndpointReplica
	err := s.repo.WithReplicaLock(ctx, replicaID, func(ctx context.Context) error {
		current, err := s.repo.GetReplica(ctx, replicaID)
		if err != nil {
			return err
		}
		if current == nil {
			return fmt.Errorf("replica %q: %w", replicaID, errNotFound)
		}
		fn(current)
		out = current
		return s.repo.SaveReplica(ctx, current)
	})
	return out, err
}

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
	if err := s.harnessReplica(ctx, replica); err != nil {
		return nil, err
	}
	if replica.Status.Terminal() {
		return &pb.HarnessRegisterResponse{Ok: false, ErrMsg: "replica is " + string(replica.Status)}, nil
	}

	replica, err = s.updateReplica(ctx, replica.ID, func(r *types.EndpointReplica) {
		r.HarnessEnabled = true
		r.LastHeartbeat = time.Now()
		if json.Valid([]byte(in.CapabilitiesJson)) {
			r.Capabilities = json.RawMessage(in.CapabilitiesJson)
		}
		if r.Status == types.ReplicaStatusScheduling {
			r.Status = types.ReplicaStatusLoading
		}
	})
	if err != nil {
		return nil, rpcError(err)
	}
	revision, err := s.effectiveRevision(ctx, replica)
	if err != nil {
		return nil, rpcError(err)
	}

	limits := json.RawMessage("null")
	if json.Valid([]byte(in.LimitsJson)) {
		limits = json.RawMessage(in.LimitsJson)
	}
	s.emit(types.EventEndpointHarness, types.EventEndpointSchema{
		EndpointID: replica.EndpointID, Action: "harness.registered", ReplicaID: replica.ID, ContainerID: replica.ContainerID,
		GPU: replica.GPU, Role: replica.Role, Version: replica.Version,
		Data: map[string]any{"engine": in.Engine, "engine_version": in.EngineVersion, "limits": limits},
	})
	return &pb.HarnessRegisterResponse{
		Ok:                       true,
		ReplicaId:                replica.ID,
		EndpointId:               replica.EndpointID,
		Role:                     replica.Role,
		Gpu:                      replica.GPU,
		Locality:                 replica.Locality,
		HeartbeatIntervalSeconds: uint32(s.config.HeartbeatInterval.Seconds()),
		Current:                  revisionToProto(revision),
	}, nil
}

// WatchConfig streams config revisions that apply to the replica: its own
// (while tuning) or its fleet's. A keep-alive re-resolve covers missed
// pub/sub messages and replicas flipping in or out of tuning.
func (s *Service) WatchConfig(in *pb.HarnessWatchConfigRequest, stream pb.EndpointHarnessService_WatchConfigServer) error {
	ctx := stream.Context()
	if err := s.authorizeHarness(ctx); err != nil {
		return err
	}
	replica, err := s.repo.GetReplica(ctx, in.ReplicaId)
	if err != nil {
		return rpcError(err)
	}
	if err := s.harnessReplica(ctx, replica); err != nil {
		return err
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
			fresh, err := s.repo.GetReplica(ctx, replica.ID)
			if err != nil || fresh == nil || fresh.Status.Terminal() {
				return nil
			}
			replica = fresh
			if current, err := s.effectiveRevision(ctx, replica); err == nil {
				if err := send(current); err != nil {
					return err
				}
			}
		case revision, ok := <-updates:
			if !ok {
				return nil
			}
			if !revisionTargets(revision, replica) {
				continue
			}
			if replica.Tuning && revision.Scope == types.ConfigScopeTarget {
				// A replica in tuning ignores fleet revisions while it has its own.
				if own, err := s.repo.LatestConfigRevision(ctx, replica.EndpointID, types.ConfigScopeReplica, replica.ID); err == nil && own != nil {
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
	if revision == nil || revision.EndpointID != replica.EndpointID {
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
	if err := s.harnessReplica(ctx, replica); err != nil {
		return nil, err
	}
	ack := &types.ConfigAck{ReplicaID: replica.ID, Revision: in.Revision, Applied: in.Applied, Error: in.Error, At: time.Now()}
	if json.Valid([]byte(in.EffectiveJson)) {
		ack.Effective = json.RawMessage(in.EffectiveJson)
	}
	if err := s.repo.SaveConfigAck(ctx, ack); err != nil {
		return nil, rpcError(err)
	}
	action := "config.rejected"
	if in.Applied {
		action = "config.applied"
		if _, err := s.updateReplica(ctx, replica.ID, func(r *types.EndpointReplica) {
			r.ConfigRevision = max(r.ConfigRevision, in.Revision)
		}); err != nil {
			return nil, rpcError(err)
		}
	}
	s.emit(types.EventEndpointConfig, types.EventEndpointSchema{
		EndpointID: replica.EndpointID, Action: action, ReplicaID: replica.ID, GPU: replica.GPU, Role: replica.Role,
		Revision: in.Revision, Message: in.Error,
	})
	return &pb.HarnessAckConfigResponse{Ok: true}, nil
}

func (s *Service) Heartbeat(ctx context.Context, in *pb.HarnessHeartbeatRequest) (*pb.HarnessHeartbeatResponse, error) {
	if err := s.authorizeHarness(ctx); err != nil {
		return nil, err
	}
	current, err := s.repo.GetReplica(ctx, in.ReplicaId)
	if err != nil {
		return nil, rpcError(err)
	}
	if err := s.harnessReplica(ctx, current); err != nil {
		return nil, err
	}
	replica, err := s.updateReplica(ctx, in.ReplicaId, func(r *types.EndpointReplica) { s.applyHeartbeat(r, in) })
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
		HeartbeatIntervalSeconds: uint32(s.config.HeartbeatInterval.Seconds()),
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
	replica.ConfigRevision = max(replica.ConfigRevision, in.AppliedRevision)
	if !replica.Alive() {
		return
	}
	switch types.ReplicaStatus(strings.ToLower(strings.TrimSpace(in.Status))) {
	case types.ReplicaStatusReady:
		if replica.Status != types.ReplicaStatusReady {
			replica.ReadyAt = now
			replica.StatusReason = ""
			s.replicaEvent(replica, "replica.ready", "", nil)
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
	if err := s.harnessReplica(ctx, replica); err != nil {
		return nil, err
	}
	var accepted uint32
	for i, event := range in.Events {
		if i >= maxEventsPerPublish || event == nil || strings.TrimSpace(event.Name) == "" {
			continue
		}
		var data map[string]any
		_ = json.Unmarshal([]byte(event.PayloadJson), &data)
		at := time.Now()
		if event.AtUnixMs > 0 {
			at = time.UnixMilli(event.AtUnixMs)
		}
		s.emit(types.EventEndpointHarness, types.EventEndpointSchema{
			EndpointID: replica.EndpointID, Action: "harness." + strings.TrimPrefix(event.Name, "harness."),
			ReplicaID: replica.ID, ContainerID: replica.ContainerID, GPU: replica.GPU, Role: replica.Role,
			Version: replica.Version, Revision: replica.ConfigRevision, Data: data, Timestamp: at,
		})
		accepted++
	}
	return &pb.HarnessPublishEventsResponse{Ok: true, Accepted: accepted}, nil
}
