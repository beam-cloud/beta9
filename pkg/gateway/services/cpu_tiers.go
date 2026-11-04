package gatewayservices

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
)

type cpuTierPool struct {
	CPUClass      types.CPUTier   `json:"cpu_class"`
	Valid         bool            `json:"valid"`
	Ready         bool            `json:"ready"`
	ReadyRuntimes map[string]bool `json:"ready_runtimes"`
}

// GetCPUTierCatalog returns the workspace's billing catalog with the gateway's
// tiered pools, their readiness and its CPU tier capabilities.
func (gws *GatewayService) GetCPUTierCatalog(ctx context.Context, _ *pb.GetCPUTierCatalogRequest) (*pb.GetCPUTierCatalogResponse, error) {
	info, _ := auth.AuthInfoFromContext(ctx)
	if info == nil || info.Workspace == nil {
		return nil, fmt.Errorf("workspace authentication required")
	}
	if gws.cpuPricing == nil {
		return &pb.GetCPUTierCatalogResponse{Catalog: `{"enabled":false,"default_cpu_tier":"","cpu_tiers":{}}`}, nil
	}
	raw, err := gws.cpuPricing.CPUTierCatalog(ctx, info.Workspace.ExternalId)
	if err != nil {
		return nil, err
	}
	var catalog map[string]any
	if err := json.Unmarshal(raw, &catalog); err != nil {
		return nil, err
	}
	workers, err := gws.workerRepo.GetAllWorkers()
	if err != nil {
		return nil, err
	}
	pools := map[string]*cpuTierPool{}
	for name, p := range gws.scheduler.PoolConfigs() {
		if p.CPUClass != "" {
			pools[name] = &cpuTierPool{CPUClass: p.CPUClass, Valid: p.CPUClass.MatchesPool(p), ReadyRuntimes: map[string]bool{}}
		}
	}
	for _, w := range workers {
		if p := pools[w.PoolName]; p != nil && w.CPUTierProtocol == types.CPUTierProtocolVersion && w.Status == types.WorkerStatusAvailable && w.TotalCpu > 0 && w.TotalMemory > 0 {
			p.Ready, p.ReadyRuntimes[w.Runtime] = true, true
		}
	}
	catalog["pools"], catalog["capabilities"] = pools, []string{"cpu_tiers", "immutable_cpu_prices", "durable_spot_stop"}
	data, err := json.Marshal(catalog)
	return &pb.GetCPUTierCatalogResponse{Catalog: string(data)}, err
}

func (gws *GatewayService) IsHostedCPUPool(name string) bool {
	p, ok := gws.scheduler.PoolConfig(name)
	return ok && p.HostedCPU()
}
