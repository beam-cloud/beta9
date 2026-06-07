package compute

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	model "github.com/beam-cloud/beta9/pkg/compute"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
)

func (s *Service) StreamAgentTelemetry(stream pb.GatewayService_StreamAgentTelemetryServer) error {
	ctx := stream.Context()
	var agentState *model.AgentTokenState
	var agentToken string

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			if agentState != nil {
				s.recordAgentDisconnect(ctx, agentState)
			}
			return stream.SendAndClose(&pb.AgentTelemetryResponse{Ok: true})
		}
		if err != nil {
			if agentState != nil && ctx.Err() != nil {
				s.recordAgentDisconnect(context.Background(), agentState)
			}
			return err
		}

		if agentState == nil {
			agentToken = strings.TrimSpace(req.AgentToken)
			state, err := s.getCurrentComputeAgentTokenState(ctx, agentToken)
			if err != nil {
				return stream.SendAndClose(&pb.AgentTelemetryResponse{Ok: false, ErrMsg: err.Error()})
			}
			if state == nil {
				return stream.SendAndClose(&pb.AgentTelemetryResponse{Ok: false, ErrMsg: "invalid agent token"})
			}
			agentState = state
		} else if req.AgentToken != "" && req.AgentToken != agentToken {
			return stream.SendAndClose(&pb.AgentTelemetryResponse{Ok: false, ErrMsg: "agent token changed on telemetry stream"})
		}

		s.recordAgentLogs(agentState, req.Logs)
		s.recordAgentEvents(agentState, req.Events)
		if req.Metrics != nil {
			if err := s.recordAgentMetrics(ctx, agentState, req.Metrics); err != nil {
				return stream.SendAndClose(&pb.AgentTelemetryResponse{Ok: false, ErrMsg: err.Error()})
			}
		}
	}
}

func (s *Service) recordAgentLogs(agentState *model.AgentTokenState, logs []*pb.AgentLogRecord) {
	if s.eventRepo == nil || agentState == nil {
		return
	}
	for _, record := range logs {
		if record == nil || strings.TrimSpace(record.Line) == "" {
			continue
		}
		source := firstNonEmpty(record.Source, types.AgentTelemetrySourceAgent)
		instanceID := agentState.MachineID
		if source == types.AgentTelemetrySourceWorker && record.WorkerId != "" {
			instanceID = record.WorkerId
		}
		s.eventRepo.PushPlatformLogEvent(types.EventPlatformLogSchema{
			Timestamp:   timeFromUnixNano(record.TimestampUnixNano),
			WorkspaceID: agentState.WorkspaceID,
			PoolName:    agentState.PoolName,
			MachineID:   agentState.MachineID,
			Service:     source,
			InstanceID:  instanceID,
			WorkerID:    record.WorkerId,
			Level:       firstNonEmpty(record.Level, "info"),
			Stream:      record.Stream,
			Line:        record.Line,
		})
	}
}

func (s *Service) recordAgentEvents(agentState *model.AgentTokenState, events []*pb.AgentEventRecord) {
	if agentState == nil {
		return
	}
	for _, record := range events {
		if record == nil || record.Action == "" {
			continue
		}
		s.emitComputeEvent(types.EventComputeMachine, types.EventComputeSchema{
			Timestamp:   timeFromUnixNano(record.TimestampUnixNano),
			WorkspaceID: agentState.WorkspaceID,
			PoolName:    agentState.PoolName,
			MachineID:   agentState.MachineID,
			Action:      record.Action,
			Status:      record.Status,
			Message:     record.Message,
			Attrs:       record.Attrs,
		})
	}
}

func (s *Service) recordAgentMetrics(ctx context.Context, agentState *model.AgentTokenState, snapshot *pb.AgentMetricSnapshot) error {
	if agentState == nil || snapshot == nil {
		return nil
	}
	current, err := s.currentComputeAgentState(ctx, agentState)
	if err != nil || current == nil {
		return err
	}
	agentState = current

	metrics := model.AgentMachineMetrics{
		Timestamp:            timeFromUnixNano(snapshot.TimestampUnixNano),
		CPUUtilizationPct:    snapshot.CpuUtilizationPct,
		MemoryUsedMB:         snapshot.MemoryUsedMb,
		MemoryTotalMB:        snapshot.MemoryTotalMb,
		MemoryUtilizationPct: snapshot.MemoryUtilizationPct,
		DiskUsedMB:           snapshot.DiskUsedMb,
		DiskTotalMB:          snapshot.DiskTotalMb,
		DiskUsagePct:         snapshot.DiskUsagePct,
		DiskPath:             snapshot.DiskPath,
		WorkerCount:          snapshot.WorkerCount,
		ContainerCount:       snapshot.ContainerCount,
		FreeGPUCount:         snapshot.FreeGpuCount,
	}
	agentState.Metrics = metrics
	agentState.LastHeartbeatAt = metrics.Timestamp
	agentState.LastDisconnectAt = time.Time{}
	if err := s.saveComputeAgentTokenState(ctx, agentState); err != nil {
		return err
	}

	s.emitComputeEvent(types.EventComputeMachine, types.EventComputeSchema{
		Timestamp:   metrics.Timestamp,
		WorkspaceID: agentState.WorkspaceID,
		PoolName:    agentState.PoolName,
		MachineID:   agentState.MachineID,
		Action:      types.EventComputeActionMachineHeartbeat,
		Status:      computeMachineStatus(agentState),
		CPUCount:    agentState.CPUCount,
		MemoryMB:    firstNonZeroUint64(metrics.MemoryTotalMB, agentState.MemoryMB),
		GPUCount:    agentState.GPUCount,
		GPUs:        agentState.GPUs,
		Attrs: map[string]string{
			"cpu_utilization_pct":    fmt.Sprintf("%.2f", metrics.CPUUtilizationPct),
			"memory_used_mb":         fmt.Sprintf("%d", metrics.MemoryUsedMB),
			"memory_utilization_pct": fmt.Sprintf("%.2f", metrics.MemoryUtilizationPct),
			"disk_used_mb":           fmt.Sprintf("%d", metrics.DiskUsedMB),
			"disk_total_mb":          fmt.Sprintf("%d", metrics.DiskTotalMB),
			"disk_usage_pct":         fmt.Sprintf("%.2f", metrics.DiskUsagePct),
			"disk_path":              metrics.DiskPath,
			"worker_count":           fmt.Sprintf("%d", metrics.WorkerCount),
			"container_count":        fmt.Sprintf("%d", metrics.ContainerCount),
			"free_gpu_count":         fmt.Sprintf("%d", metrics.FreeGPUCount),
		},
	})
	return nil
}

func (s *Service) recordAgentDisconnect(ctx context.Context, agentState *model.AgentTokenState) {
	if agentState == nil {
		return
	}
	current, err := s.currentComputeAgentState(ctx, agentState)
	if err != nil || current == nil {
		return
	}
	agentState = current
	agentState.LastDisconnectAt = time.Now().UTC()
	if err := s.saveComputeAgentTokenState(ctx, agentState); err != nil {
		return
	}
	s.emitComputeEvent(types.EventComputeMachine, types.EventComputeSchema{
		Timestamp:   agentState.LastDisconnectAt,
		WorkspaceID: agentState.WorkspaceID,
		PoolName:    agentState.PoolName,
		MachineID:   agentState.MachineID,
		Action:      types.EventComputeActionMachineDisconnected,
		Status:      model.AgentMachineStatus(agentState, agentState.LastDisconnectAt),
		Message:     "agent telemetry stream disconnected",
	})
}

func timeFromUnixNano(value int64) time.Time {
	if value <= 0 {
		return time.Now().UTC()
	}
	return time.Unix(0, value).UTC()
}
