package compute

import (
	"fmt"
	"strings"
	"time"

	model "github.com/beam-cloud/beta9/pkg/compute"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func normalizePoolConfig(in *pb.PoolConfig) *pb.PoolConfig {
	if in == nil {
		return nil
	}
	out := *in
	out.Gpu = append([]string(nil), in.Gpu...)
	out.Providers = append([]string(nil), in.Providers...)
	out.Regions = append([]string(nil), in.Regions...)
	if out.Selector == "" {
		out.Selector = out.Name
	}
	if out.Mode == "" {
		out.Mode = string(types.PoolModePrivate)
	}
	if out.Transport == "" {
		out.Transport = defaultPrivateTransport
	}
	out.Transport = strings.ReplaceAll(out.Transport, "-", "_")
	if out.Fallback == "" {
		out.Fallback = defaultPrivateFallback
	}
	if out.Priority == 0 {
		out.Priority = defaultPrivatePriority
	}
	return &out
}

func computePoolFromProto(in *pb.PoolConfig, requireReservation bool) (model.Pool, error) {
	if in == nil {
		return model.Pool{}, fmt.Errorf("pool config is required")
	}
	if in.Mode != "" && in.Mode != string(types.PoolModePrivate) {
		return model.Pool{}, fmt.Errorf("private pool mode must be %q", types.PoolModePrivate)
	}
	switch in.Transport {
	case "", defaultPrivateTransport:
	default:
		return model.Pool{}, fmt.Errorf("unsupported agent transport %q", in.Transport)
	}
	switch in.Fallback {
	case "", types.PrivatePoolFallbackInternal, types.PrivatePoolFallbackWait, types.PrivatePoolFallbackFail:
	default:
		return model.Pool{}, fmt.Errorf("unsupported private pool fallback %q", in.Fallback)
	}
	ttl, err := model.ParseTTL(in.Ttl)
	if err != nil {
		return model.Pool{}, err
	}
	gpus, err := poolGPUConfig(in.Gpu)
	if err != nil {
		return model.Pool{}, err
	}

	pool := model.Pool{
		Name:           in.Name,
		Selector:       in.Selector,
		GPUs:           gpus,
		TotalGPUs:      in.Gpus,
		OfferID:        in.OfferId,
		TTL:            ttl,
		MaxSpendMicros: model.DollarsToMicros(in.MaxSpend),
		Providers:      in.Providers,
		Regions:        in.Regions,
		MinReliability: in.MinReliability,
	}
	if requireReservation {
		if err := pool.Validate(); err != nil {
			return model.Pool{}, err
		}
	} else if pool.MinReliability < 0 || pool.MinReliability > 1 {
		return model.Pool{}, fmt.Errorf("min_reliability must be between 0 and 1")
	}
	return pool, nil
}

func poolOfferToProto(offer model.Offer) *pb.PoolOffer {
	return &pb.PoolOffer{
		Id:               offer.ID,
		Provider:         offer.Provider,
		Cloud:            offer.Cloud,
		InstanceType:     offer.InstanceType,
		Region:           offer.Region,
		Gpu:              offer.GPU,
		GpuCount:         offer.GPUCount,
		CpuMillicores:    offer.CPUMillicores,
		MemoryMb:         offer.MemoryMB,
		StorageMb:        offer.StorageMB,
		HourlyCostMicros: offer.HourlyCostMicros,
		Reliability:      offer.Reliability,
		Available:        offer.Available,
	}
}

type computeCostProjector func(int64) int64

func providerInstanceToProto(reservation model.Reservation, projectCost computeCostProjector) *pb.ProviderInstance {
	if projectCost == nil {
		projectCost = identityCost
	}
	return &pb.ProviderInstance{
		Id:                reservation.ID,
		PoolName:          reservation.PoolName,
		Provider:          reservation.Provider,
		Cloud:             reservation.Cloud,
		OfferId:           reservation.OfferID,
		Status:            string(reservation.Status),
		GpuCount:          reservation.GPUCount,
		HourlyCostMicros:  projectCost(reservation.HourlyCostMicros),
		Source:            string(reservation.Source),
		CreatedAt:         timestampOrNil(reservation.CreatedAt),
		ExpiresAt:         timestampOrNil(reservation.ExpiresAt),
		BillingRenewalAt:  timestampOrNil(reservation.BillingRenewalAt),
		StatusMessage:     reservation.LastStatusMessage,
		TerminatingReason: reservation.TerminatingReason,
		MachineId:         reservation.MachineID,
	}
}

func identityCost(value int64) int64 {
	return value
}

func agentRouteToProto(route types.BackendRoute) *pb.AgentRoute {
	return &pb.AgentRoute{
		RouteId:     route.RouteID,
		WorkspaceId: route.WorkspaceID,
		PoolName:    route.PoolName,
		MachineId:   route.MachineID,
		WorkerId:    route.WorkerID,
		ContainerId: route.ContainerID,
		Kind:        route.Kind,
		Port:        route.Port,
		Protocol:    route.Protocol,
		Transport:   route.Transport,
		LocalTarget: route.LocalTarget,
		ProxyTarget: route.ProxyTarget,
		State:       route.State,
		Error:       route.Error,
		UpdatedAt:   route.UpdatedAt,
	}
}

func (s *Service) privatePoolStateToProto(state *model.PoolState) *pb.PrivatePool {
	return s.privatePoolStateToProtoWithMachines(state, nil)
}

func (s *Service) privatePoolStateToProtoWithMachines(state *model.PoolState, machines []*model.AgentTokenState) *pb.PrivatePool {
	return privatePoolStateToProtoWithMachines(state, machines, s.billableMicros)
}

func privatePoolStateToProtoWithMachines(state *model.PoolState, machines []*model.AgentTokenState, projectCost computeCostProjector) *pb.PrivatePool {
	if state == nil {
		return nil
	}
	reservations := make([]*pb.ProviderInstance, 0, len(state.Reservations))
	for _, reservation := range state.Reservations {
		reservations = append(reservations, providerInstanceToProto(reservation, projectCost))
	}
	readyMachineCount := uint32(0)
	now := time.Now()
	for _, machine := range machines {
		if model.AgentMachineConnected(machine, now) {
			readyMachineCount++
		}
	}
	config := normalizePoolConfig(state.Config)
	return &pb.PrivatePool{
		Name:                 state.Name,
		Selector:             state.Selector,
		Config:               config,
		Reservations:         reservations,
		ReservedGpus:         state.ReservedGPUs,
		CommittedSpendMicros: state.CommittedSpendMicros,
		Status:               state.Status,
		Source:               string(state.Source.Canonical()),
		CreatedAt:            timestampOrNil(state.CreatedAt),
		ExpiresAt:            timestampOrNil(state.ExpiresAt),
		MachineCount:         uint32(len(machines)),
		ReadyMachineCount:    readyMachineCount,
	}
}

func timestampOrNil(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return timestamppb.New(t)
}

func (s *Service) agentMachineToProto(state *model.AgentTokenState) *pb.Machine {
	return agentMachineToProto(state, s.agentMachineStatusWorker(state))
}

func (s *Service) agentMachineStatusWorker(state *model.AgentTokenState) *types.Worker {
	if s == nil || s.workerRepo == nil || state == nil {
		return nil
	}
	worker, err := s.workerRepo.GetWorkerById(model.AgentMachineWorkerID(state.MachineID))
	if err != nil || worker == nil {
		return nil
	}
	if worker.MachineId != state.MachineID || worker.PoolName != state.PoolName {
		return nil
	}
	return worker
}

func agentMachineToProto(state *model.AgentTokenState, worker *types.Worker) *pb.Machine {
	if state == nil {
		return &pb.Machine{}
	}
	gpu := ""
	if len(state.GPUs) > 0 {
		gpu = strings.Join(state.GPUs, ",")
	}
	lastKeepalive := model.AgentMachineLastSeen(state)
	metrics := agentMachineMetrics(state, worker)
	return &pb.Machine{
		Id:             state.MachineID,
		Cpu:            state.CPUMillicores,
		Memory:         int64(state.MemoryMB),
		Gpu:            gpu,
		GpuCount:       state.GPUCount,
		Status:         string(agentMachineStatus(state, worker, time.Now())),
		PoolName:       state.PoolName,
		ProviderName:   types.DefaultAgentName,
		Created:        formatComputeTime(state.CreatedAt),
		LastKeepalive:  formatComputeTime(lastKeepalive),
		MachineMetrics: metrics,
	}
}

func agentMachineMetrics(state *model.AgentTokenState, worker *types.Worker) *pb.MachineMetrics {
	totalCPU := state.CPUMillicores
	totalMemory := int64(firstNonZeroUint64(state.Metrics.MemoryTotalMB, state.MemoryMB))
	freeGPU := state.Metrics.FreeGPUCount
	containerCount := state.Metrics.ContainerCount
	workerCount := state.Metrics.WorkerCount
	var cpuPct, memoryPct float32
	var usedMemory int64

	if worker != nil {
		totalCPU = firstNonZeroInt64(worker.TotalCpu, totalCPU)
		totalMemory = firstNonZeroInt64(worker.TotalMemory, totalMemory)
		cpuPct = capacityUtilizationPct(worker.TotalCpu, worker.FreeCpu)
		memoryPct = capacityUtilizationPct(worker.TotalMemory, worker.FreeMemory)
		usedMemory = maxInt64(worker.TotalMemory-worker.FreeMemory, 0)
		freeGPU = worker.FreeGpuCount
		containerCount = uint32(len(worker.ActiveContainers))
		workerCount = 1
	} else {
		cpuPct = state.Metrics.CPUUtilizationPct
		memoryPct = state.Metrics.MemoryUtilizationPct
		usedMemory = int64(state.Metrics.MemoryUsedMB)
	}

	return &pb.MachineMetrics{
		TotalCpuAvailable:    int32(totalCPU),
		TotalMemoryAvailable: int32(totalMemory),
		CpuUtilizationPct:    cpuPct,
		MemoryUtilizationPct: memoryPct,
		WorkerCount:          int32(workerCount),
		ContainerCount:       int32(containerCount),
		FreeGpuCount:         int32(freeGPU),
		CacheUsagePct:        state.Metrics.DiskUsagePct,
		CacheCapacity:        int32(state.Metrics.DiskTotalMB),
		CacheMemoryUsage:     int32(state.Metrics.MemoryUsedMB),
		MemoryUsedMb:         usedMemory,
		MemoryTotalMb:        totalMemory,
		DiskUsedMb:           int64(state.Metrics.DiskUsedMB),
		DiskTotalMb:          int64(state.Metrics.DiskTotalMB),
		DiskUsagePct:         state.Metrics.DiskUsagePct,
	}
}

func capacityUtilizationPct(total, free int64) float32 {
	if total <= 0 {
		return 0
	}
	used := maxInt64(total-free, 0)
	return float32(used) * 100 / float32(total)
}

func agentMachineStatus(state *model.AgentTokenState, worker *types.Worker, now time.Time) types.MachineStatus {
	if state == nil {
		return ""
	}
	if model.AgentMachineStatus(state, now) == types.AgentMachineStatusPreflightFail {
		return types.MachineStatusDisabled
	}
	if !model.AgentMachineConnected(state, now) {
		return types.MachineStatusRegistered
	}
	if worker == nil {
		return types.MachineStatusRegistered
	}
	switch worker.Status {
	case types.WorkerStatusAvailable:
		return types.MachineStatusAvailable
	case types.WorkerStatusPending:
		return types.MachineStatusPending
	case types.WorkerStatusDisabled:
		return types.MachineStatusDisabled
	default:
		return types.MachineStatusPending
	}
}

func formatComputeTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.Format(time.RFC3339)
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func firstNonZeroInt64(values ...int64) int64 {
	for _, value := range values {
		if value != 0 {
			return value
		}
	}
	return 0
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func firstNonZeroUint64(values ...uint64) uint64 {
	for _, value := range values {
		if value != 0 {
			return value
		}
	}
	return 0
}
