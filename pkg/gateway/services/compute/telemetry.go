package compute

import (
	"context"
	"fmt"
	"io"
	"regexp"
	"strings"
	"time"

	model "github.com/beam-cloud/beta9/pkg/compute"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/cockroachdb/redact"
	"github.com/rs/zerolog/log"
)

const (
	telemetrySensitiveLogKeyPattern = `[A-Za-z0-9_.-]*(?:access[_-]?key|accesskey|api[_-]?key|apikey|secret|token|password|credentials?)[A-Za-z0-9_.-]*`
	telemetryMaxPathMetricAttrs     = 8
)

var (
	telemetryAuthorizationPattern     = regexp.MustCompile(`(?i)(["']?authorization["']?\s*[:=]\s*)("?)(bearer|basic)(\s+)([A-Za-z0-9._~+/=-]+)("?)`)
	telemetryBearerTokenPattern       = regexp.MustCompile(`(?i)\b(bearer)(\s+)([A-Za-z0-9._~+/=-]+)`)
	telemetrySensitivePhrasePattern   = regexp.MustCompile(`(?i)\b(api\s*key|auth\s*key|token)(\s+)([A-Za-z0-9._~+/=-]+)`)
	telemetrySensitiveKeyValuePattern = regexp.MustCompile(`(?i)(["']?` + telemetrySensitiveLogKeyPattern + `["']?\s*[:=]\s*)("[^"]*"|'[^']*'|[^\s,}\]]+)`)
	telemetryRedactedValue            = redact.Sprintf("%s", redact.Unsafe("redacted")).Redact().StripMarkers()
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
			// Treat any stream failure as a potential disconnect; this no-ops
			// while the heartbeat is still fresh.
			if agentState != nil {
				s.recordAgentDisconnect(context.Background(), agentState)
			}
			return err
		}

		if agentState == nil {
			agentToken = strings.TrimSpace(req.AgentToken)
			state, errMsg := s.requireAgentState(ctx, agentToken)
			if errMsg != "" {
				return stream.SendAndClose(&pb.AgentTelemetryResponse{Ok: false, ErrMsg: errMsg})
			}
			state, err := s.touchAgentHeartbeat(ctx, state)
			if err != nil {
				return stream.SendAndClose(&pb.AgentTelemetryResponse{Ok: false, ErrMsg: err.Error()})
			}
			if err := s.ensureAgentMachine(ctx, state, nil); err != nil {
				return stream.SendAndClose(&pb.AgentTelemetryResponse{Ok: false, ErrMsg: err.Error()})
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
			Line:        redactTelemetryLogLine(record.Line),
		})
	}
}

func redactTelemetryLogLine(line string) string {
	line = telemetryAuthorizationPattern.ReplaceAllString(line, "${1}${2}${3}${4}"+telemetryRedactedValue+"${6}")
	line = telemetryBearerTokenPattern.ReplaceAllString(line, "${1}${2}"+telemetryRedactedValue)
	line = telemetrySensitivePhrasePattern.ReplaceAllString(line, "${1}${2}"+telemetryRedactedValue)
	return telemetrySensitiveKeyValuePattern.ReplaceAllString(line, "${1}"+telemetryRedactedValue)
}

func (s *Service) recordAgentEvents(agentState *model.AgentTokenState, events []*pb.AgentEventRecord) {
	if agentState == nil {
		return
	}
	for _, record := range events {
		if record == nil || record.Action == "" {
			continue
		}
		eventType := firstNonEmpty(record.EventType, types.EventComputeMachine)
		s.emitComputeEvent(eventType, types.EventComputeSchema{
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
	if err != nil {
		return err
	}
	if current == nil {
		return fmt.Errorf("agent token is no longer current")
	}
	agentState = current
	previousSeen := model.AgentMachineLastSeen(agentState)
	now := time.Now().UTC()

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
		PathMetrics:          agentPathMetricsFromProto(snapshot.PathMetrics),
	}
	var poolState *model.PoolState
	if s.computeRepo != nil {
		poolState, _ = s.getAgentPoolState(ctx, agentState)
	}
	agentState.Metrics = metrics

	// Liveness is always tracked on the gateway clock: agent-supplied
	// timestamps can be skewed or stale (buffered telemetry after a
	// reconnect), which would oscillate the machine across the heartbeat
	// timeout and flicker its status. Never move the heartbeat backwards.
	if now.After(agentState.LastHeartbeatAt) {
		agentState.LastHeartbeatAt = now
	}
	agentState.LastDisconnectAt = time.Time{}
	if err := s.saveComputeAgentTokenState(ctx, agentState); err != nil {
		return err
	}
	worker := s.agentMachineStatusWorker(agentState)
	capacityMetrics := agentMachineMetrics(agentState, worker)

	// The scheduler's container index is authoritative for per-machine
	// container counts, overriding the agent snapshot and worker state.
	s.applyAgentContainerCount(agentState, capacityMetrics)
	if err := s.recordAgentNodeUsage(agentState, poolState, capacityMetrics, previousSeen, now); err != nil {
		log.Warn().
			Err(err).
			Str("workspace_id", agentState.WorkspaceID).
			Str("pool_name", agentState.PoolName).
			Str("machine_id", agentState.MachineID).
			Msg("failed to record node usage telemetry")
	}

	attrs := map[string]string{
		types.EventComputeAttrCPUUtilizationPct:        fmt.Sprintf("%.2f", capacityMetrics.CpuUtilizationPct),
		types.EventComputeAttrMemoryUsedMB:             fmt.Sprintf("%d", capacityMetrics.MemoryUsedMb),
		types.EventComputeAttrMemoryUtilizationPct:     fmt.Sprintf("%.2f", capacityMetrics.MemoryUtilizationPct),
		types.EventComputeAttrHostCPUUtilizationPct:    fmt.Sprintf("%.2f", metrics.CPUUtilizationPct),
		types.EventComputeAttrHostMemoryUsedMB:         fmt.Sprintf("%d", metrics.MemoryUsedMB),
		types.EventComputeAttrHostMemoryUtilizationPct: fmt.Sprintf("%.2f", metrics.MemoryUtilizationPct),
		types.EventComputeAttrDiskUsedMB:               fmt.Sprintf("%d", metrics.DiskUsedMB),
		types.EventComputeAttrDiskTotalMB:              fmt.Sprintf("%d", metrics.DiskTotalMB),
		types.EventComputeAttrDiskUsagePct:             fmt.Sprintf("%.2f", metrics.DiskUsagePct),
		types.EventComputeAttrDiskPath:                 metrics.DiskPath,
		types.EventComputeAttrWorkerCount:              fmt.Sprintf("%d", capacityMetrics.WorkerCount),
		types.EventComputeAttrContainerCount:           fmt.Sprintf("%d", capacityMetrics.ContainerCount),
		types.EventComputeAttrFreeGPUCount:             fmt.Sprintf("%d", capacityMetrics.FreeGpuCount),
	}
	appendPathMetricAttrs(attrs, metrics.PathMetrics)
	if poolState != nil {
		attrs[types.EventComputeAttrPoolMode] = string(poolState.Mode)
		attrs[types.EventComputeAttrTransport] = poolState.Transport
		hourlyCostMicros := int64(0)
		for _, reservation := range poolState.Reservations {
			if reservation.MachineID == agentState.MachineID {
				hourlyCostMicros = s.billableMicros(reservation.HourlyCostMicros)
				break
			}
		}
		if hourlyCostMicros == 0 && poolState.WorkerConfig != nil {
			// DefaultMachineCost is the existing per-machine, per-second usage rate.
			hourlyCostMicros = model.DollarsToMicros(poolState.WorkerConfig.DefaultMachineCost * 3600)
		}
		if hourlyCostMicros > 0 {
			attrs[types.EventComputeAttrHourlyCostMicros] = fmt.Sprintf("%d", hourlyCostMicros)
		}
	}

	s.emitComputeEvent(types.EventComputeMachine, types.EventComputeSchema{
		Timestamp:   now,
		WorkspaceID: agentState.WorkspaceID,
		PoolName:    agentState.PoolName,
		MachineID:   agentState.MachineID,
		Action:      types.EventComputeActionMachineHeartbeat,
		Status:      string(agentMachineStatus(agentState, worker, now)),
		CPUCount:    agentState.CPUCount,
		MemoryMB:    firstNonZeroUint64(metrics.MemoryTotalMB, agentState.MemoryMB),
		GPUCount:    agentState.GPUCount,
		GPUs:        agentState.GPUs,
		Attrs:       attrs,
	})
	return nil
}

func appendPathMetricAttrs(attrs map[string]string, pathMetrics []model.AgentPathMetric) {
	count := 0
	for _, pathMetric := range pathMetrics {
		if pathMetric.Label == "" {
			continue
		}
		if count >= telemetryMaxPathMetricAttrs {
			break
		}
		prefix := fmt.Sprintf("path_metric_%d", count)
		attrs[prefix+"_label"] = pathMetric.Label
		attrs[prefix+"_path"] = pathMetric.Path
		attrs[prefix+"_used_mb"] = fmt.Sprintf("%d", pathMetric.UsedMB)
		attrs[prefix+"_total_mb"] = fmt.Sprintf("%d", pathMetric.TotalMB)
		attrs[prefix+"_available_mb"] = fmt.Sprintf("%d", pathMetric.AvailableMB)
		attrs[prefix+"_usage_pct"] = fmt.Sprintf("%.2f", pathMetric.UsagePct)
		count++
	}
	attrs["path_metric_count"] = fmt.Sprintf("%d", count)
}

func agentPathMetricsFromProto(items []*pb.MachinePathMetrics) []model.AgentPathMetric {
	if len(items) == 0 {
		return nil
	}
	out := make([]model.AgentPathMetric, 0, len(items))
	for _, item := range items {
		if item == nil {
			continue
		}
		out = append(out, model.AgentPathMetric{
			Label:       item.Label,
			Path:        item.Path,
			UsedMB:      item.UsedMb,
			TotalMB:     item.TotalMb,
			AvailableMB: item.AvailableMb,
			UsagePct:    item.UsagePct,
		})
	}
	return out
}

func (s *Service) recordAgentNodeUsage(agentState *model.AgentTokenState, poolState *model.PoolState, metrics *pb.MachineMetrics, previousSeen, currentSeen time.Time) error {
	if s == nil || s.usageMetricsRepo == nil || agentState == nil {
		return nil
	}
	seconds := nodeUsageSeconds(previousSeen, currentSeen)
	if seconds <= 0 {
		return nil
	}
	if err := s.usageMetricsRepo.IncrementCounter(
		types.UsageMetricsNodeUsage,
		agentNodeUsageMetadata(agentState, poolState, metrics, seconds),
		seconds,
	); err != nil {
		return fmt.Errorf("record node usage: %w", err)
	}
	return nil
}

func nodeUsageSeconds(previousSeen, currentSeen time.Time) float64 {
	if previousSeen.IsZero() || currentSeen.IsZero() || !currentSeen.After(previousSeen) {
		return 0
	}
	duration := currentSeen.Sub(previousSeen)
	if duration > model.AgentHeartbeatTimeout {
		// Cap stale gaps at the provable uptime instead of dropping them.
		duration = model.AgentHeartbeatTimeout
	}
	return duration.Seconds()
}

func agentNodeUsageMetadata(agentState *model.AgentTokenState, poolState *model.PoolState, metrics *pb.MachineMetrics, seconds float64) map[string]interface{} {
	source := types.ComputeSourceAttached
	poolMode := string(types.PoolModePrivate)
	transport := ""
	if poolState != nil {
		if poolState.Source != "" {
			source = poolState.Source.Canonical()
		}
		if poolState.Mode != "" {
			poolMode = poolState.Mode
		}
		transport = poolState.Transport
	}

	nodeType := "managed"
	if source.IsAttached() {
		nodeType = "byo"
	}

	metadata := map[string]interface{}{
		"workspace_id":        agentState.WorkspaceID,
		"pool_name":           agentState.PoolName,
		"machine_id":          agentState.MachineID,
		"node_type":           nodeType,
		"capacity_source":     string(source),
		"pool_mode":           poolMode,
		"transport":           transport,
		"executor":            agentState.Executor,
		"os":                  agentState.OS,
		"arch":                agentState.Arch,
		"hostname":            agentState.Hostname,
		"cpu_count":           agentState.CPUCount,
		"cpu_millicores":      agentState.CPUMillicores,
		"memory_mb":           agentState.MemoryMB,
		"gpu":                 strings.Join(agentState.GPUs, ","),
		"gpu_ids":             strings.Join(agentState.GPUIDs, ","),
		"gpu_count":           agentState.GPUCount,
		"usage_seconds":       seconds,
		"worker_count":        0,
		"container_count":     0,
		"free_gpu_count":      agentState.Metrics.FreeGPUCount,
		"cpu_used_pct":        agentState.Metrics.CPUUtilizationPct,
		"memory_used_pct":     agentState.Metrics.MemoryUtilizationPct,
		"cache_used_pct":      agentState.Metrics.DiskUsagePct,
		"cache_total_mb":      agentState.Metrics.DiskTotalMB,
		"cache_used_mb":       agentState.Metrics.DiskUsedMB,
		"host_memory_used_mb": agentState.Metrics.MemoryUsedMB,
	}
	if metrics != nil {
		metadata["worker_count"] = metrics.WorkerCount
		metadata["container_count"] = metrics.ContainerCount
		metadata["free_gpu_count"] = metrics.FreeGpuCount
		metadata["cpu_used_pct"] = metrics.CpuUtilizationPct
		metadata["memory_used_pct"] = metrics.MemoryUtilizationPct
	}
	return metadata
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
	lastSeen := model.AgentMachineLastSeen(agentState)
	now := time.Now().UTC()
	if lastSeen.IsZero() || !lastSeen.Before(now.Add(-model.AgentHeartbeatTimeout)) {
		return
	}
	if !agentState.LastDisconnectAt.IsZero() && !agentState.LastDisconnectAt.Before(lastSeen) {
		s.disableMachineWorker(ctx, agentState, reconcileReasonAgentDisconnected)
		return
	}
	agentState.LastDisconnectAt = now
	if err := s.saveComputeAgentTokenState(ctx, agentState); err != nil {
		return
	}
	s.disableMachineWorker(ctx, agentState, reconcileReasonAgentDisconnected)
	s.emitComputeEvent(types.EventComputeMachine, types.EventComputeSchema{
		Timestamp:   agentState.LastDisconnectAt,
		WorkspaceID: agentState.WorkspaceID,
		PoolName:    agentState.PoolName,
		MachineID:   agentState.MachineID,
		Action:      types.EventComputeActionMachineDisconnected,
		Status:      string(agentMachineStatus(agentState, nil, agentState.LastDisconnectAt)),
		Message:     "agent telemetry stream disconnected",
	})
}

// timeFromUnixNano converts an agent-supplied timestamp, clamping future
// values to the gateway clock so skewed agent clocks cannot produce events
// "from the future".
func timeFromUnixNano(value int64) time.Time {
	now := time.Now().UTC()
	if value <= 0 {
		return now
	}
	timestamp := time.Unix(0, value).UTC()
	if timestamp.After(now) {
		return now
	}
	return timestamp
}
