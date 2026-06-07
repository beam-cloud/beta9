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
	pool := model.Pool{
		Name:           in.Name,
		Selector:       in.Selector,
		GPUs:           in.Gpu,
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

func providerInstanceToProto(reservation model.Reservation) *pb.ProviderInstance {
	return &pb.ProviderInstance{
		Id:               reservation.ID,
		PoolName:         reservation.PoolName,
		Provider:         reservation.Provider,
		OfferId:          reservation.OfferID,
		Status:           string(reservation.Status),
		GpuCount:         reservation.GPUCount,
		HourlyCostMicros: reservation.HourlyCostMicros,
		Source:           string(reservation.Source),
		CreatedAt:        timestampOrNil(reservation.CreatedAt),
		ExpiresAt:        timestampOrNil(reservation.ExpiresAt),
		BillingRenewalAt: timestampOrNil(reservation.BillingRenewalAt),
	}
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

func privatePoolStateToProto(state *model.PoolState) *pb.PrivatePool {
	return privatePoolStateToProtoWithMachines(state, nil)
}

func privatePoolStateToProtoWithMachines(state *model.PoolState, machines []*model.AgentTokenState) *pb.PrivatePool {
	if state == nil {
		return nil
	}
	reservations := make([]*pb.ProviderInstance, 0, len(state.Reservations))
	for _, reservation := range state.Reservations {
		reservations = append(reservations, providerInstanceToProto(reservation))
	}
	readyMachineCount := uint32(0)
	for _, machine := range machines {
		if machine != nil && machine.Schedulable {
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
		Source:               string(state.Source),
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

func agentMachineToProto(state *model.AgentTokenState) *pb.Machine {
	if state == nil {
		return &pb.Machine{}
	}
	gpu := ""
	if len(state.GPUs) > 0 {
		gpu = strings.Join(state.GPUs, ",")
	}
	lastKeepalive := state.LastJoinAt
	if !state.LastHeartbeatAt.IsZero() {
		lastKeepalive = state.LastHeartbeatAt
	}
	return &pb.Machine{
		Id:            state.MachineID,
		Cpu:           state.CPUMillicores,
		Memory:        int64(state.MemoryMB),
		Gpu:           gpu,
		GpuCount:      state.GPUCount,
		Status:        computeMachineStatus(state),
		PoolName:      state.PoolName,
		ProviderName:  types.DefaultAgentName,
		Created:       formatComputeTime(state.CreatedAt),
		LastKeepalive: formatComputeTime(lastKeepalive),
		MachineMetrics: &pb.MachineMetrics{
			TotalCpuAvailable:    int32(state.CPUMillicores),
			TotalMemoryAvailable: int32(firstNonZeroUint64(state.Metrics.MemoryTotalMB, state.MemoryMB)),
			CpuUtilizationPct:    state.Metrics.CPUUtilizationPct,
			MemoryUtilizationPct: state.Metrics.MemoryUtilizationPct,
			WorkerCount:          int32(state.Metrics.WorkerCount),
			ContainerCount:       int32(state.Metrics.ContainerCount),
			FreeGpuCount:         int32(state.Metrics.FreeGPUCount),
			CacheUsagePct:        state.Metrics.DiskUsagePct,
			CacheCapacity:        int32(state.Metrics.DiskTotalMB),
			CacheMemoryUsage:     int32(state.Metrics.MemoryUsedMB),
		},
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

func firstNonZeroUint64(values ...uint64) uint64 {
	for _, value := range values {
		if value != 0 {
			return value
		}
	}
	return 0
}
