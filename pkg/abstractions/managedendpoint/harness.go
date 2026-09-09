package managedendpoint

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
)

// EndpointHarnessService: called from inside replica containers.

const (
	maxEventsPerPublish = 256
	// maxEngineMetricsBytes bounds the engine status kept from a heartbeat.
	maxEngineMetricsBytes = 64 << 10
	watchKeepAlive        = 30 * time.Second
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
			r.EnterLoading(time.Now(), "")
		}
	})
	if err != nil {
		return nil, rpcError(err)
	}
	limits := json.RawMessage("null")
	if json.Valid([]byte(in.LimitsJson)) {
		limits = json.RawMessage(in.LimitsJson)
	}
	s.emit(types.EventEndpointHarness, types.EventEndpointSchema{
		EndpointID: replica.EndpointID, Action: "harness.registered", ReplicaID: replica.ID, ContainerID: replica.ContainerID,
		GPU: replica.GPU, Version: replica.Version,
		Data: map[string]any{"engine": in.Engine, "engine_version": in.EngineVersion, "limits": limits},
	})
	return &pb.HarnessRegisterResponse{
		Ok:                       true,
		ReplicaId:                replica.ID,
		EndpointId:               replica.EndpointID,
		Gpu:                      replica.GPU,
		HeartbeatIntervalSeconds: uint32(s.config.HeartbeatInterval.Seconds()),
		Current:                  configToProto(replica.Config),
	}, nil
}

// WatchConfig streams the replica's live config whenever an admin sets a new
// revision. A keep-alive re-read covers missed pub/sub messages.
func (s *Service) WatchConfig(in *pb.HarnessWatchConfigRequest, stream pb.EndpointHarnessService_WatchConfigServer) error {
	ctx := stream.Context()
	replica, err := s.repo.GetReplica(ctx, in.ReplicaId)
	if err != nil {
		return rpcError(err)
	}
	if err := s.harnessReplica(ctx, replica); err != nil {
		return err
	}
	updates, err := s.repo.SubscribeReplicaConfig(ctx, replica.ID)
	if err != nil {
		return rpcError(err)
	}

	sent := in.AfterRevision
	send := func() error {
		fresh, err := s.repo.GetReplica(ctx, replica.ID)
		if err != nil || fresh == nil || fresh.Status.Terminal() {
			return errWatchDone
		}
		if fresh.Config.Revision <= sent {
			return nil
		}
		if err := stream.Send(configToProto(fresh.Config)); err != nil {
			return err
		}
		sent = fresh.Config.Revision
		return nil
	}
	if err := send(); err != nil {
		return ignoreWatchDone(err)
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
		case _, ok := <-updates:
			if !ok {
				return nil
			}
		}
		if err := send(); err != nil {
			return ignoreWatchDone(err)
		}
	}
}

var errWatchDone = errors.New("watch done")

func ignoreWatchDone(err error) error {
	if errors.Is(err, errWatchDone) {
		return nil
	}
	return err
}

func (s *Service) AckConfig(ctx context.Context, in *pb.HarnessAckConfigRequest) (*pb.HarnessAckConfigResponse, error) {
	replica, err := s.repo.GetReplica(ctx, in.ReplicaId)
	if err != nil {
		return nil, rpcError(err)
	}
	if err := s.harnessReplica(ctx, replica); err != nil {
		return nil, err
	}
	accepted := false
	replica, err = s.updateReplica(ctx, replica.ID, func(r *types.EndpointReplica) {
		accepted = r.Config.Ack(in.Revision, in.Applied, in.Error, json.RawMessage(in.EffectiveJson), time.Now())
	})
	if err != nil {
		return nil, rpcError(err)
	}
	if !accepted {
		return &pb.HarnessAckConfigResponse{Ok: false, ErrMsg: fmt.Sprintf("revision %d was not issued (current %d, acked %d)", in.Revision, replica.Config.Revision, replica.Config.AckedRevision)}, nil
	}
	action := "config.rejected"
	if in.Applied {
		action = "config.applied"
	}
	// The outcome is recorded with the requested and effective config so the
	// history stands on its own once the replica record is gone.
	data := map[string]any{"requested": rawJSON(replica.Config.Config), "effective": rawJSON(replica.Config.Effective), "author": replica.Config.Author, "actor": replica.Config.Actor}
	s.emit(types.EventEndpointConfig, types.EventEndpointSchema{
		EndpointID: replica.EndpointID, Action: action, ReplicaID: replica.ID, ContainerID: replica.ContainerID, GPU: replica.GPU, Version: replica.Version,
		Revision: in.Revision, Message: in.Error, Data: data,
	})
	return &pb.HarnessAckConfigResponse{Ok: true}, nil
}

func (s *Service) Heartbeat(ctx context.Context, in *pb.HarnessHeartbeatRequest) (*pb.HarnessHeartbeatResponse, error) {
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
	// A heartbeat may recover a missed ack for an issued revision, never
	// advance past what was issued.
	if in.AppliedRevision > replica.Config.AckedRevision && in.AppliedRevision <= replica.Config.Revision {
		replica.Config.AckedRevision, replica.Config.Applied, replica.Config.Error = in.AppliedRevision, true, ""
	}
	if len(in.MetricsJson) > 0 && len(in.MetricsJson) <= maxEngineMetricsBytes && json.Valid([]byte(in.MetricsJson)) {
		replica.EngineMetrics = json.RawMessage(in.MetricsJson)
	}
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
		// A ready engine that reports loading (model/config reload) leaves the
		// serving set until it reports ready again.
		if replica.Status == types.ReplicaStatusScheduling || replica.Status == types.ReplicaStatusReady {
			replica.EnterLoading(now, "engine reported loading")
		}
	case types.ReplicaStatusDraining:
		replica.Status = types.ReplicaStatusDraining
	}
}

func (s *Service) PublishEvents(ctx context.Context, in *pb.HarnessPublishEventsRequest) (*pb.HarnessPublishEventsResponse, error) {
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
			ReplicaID: replica.ID, ContainerID: replica.ContainerID, GPU: replica.GPU,
			Version: replica.Version, Revision: replica.Config.AckedRevision, Data: data, Timestamp: at,
		})
		accepted++
	}
	return &pb.HarnessPublishEventsResponse{Ok: true, Accepted: accepted}, nil
}

// rawJSON keeps a stored JSON document as-is inside an event payload.
func rawJSON(raw json.RawMessage) any {
	if len(raw) == 0 {
		return nil
	}
	return raw
}
