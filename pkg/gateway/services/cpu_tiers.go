package gatewayservices

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/clients"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
)

func (gws *GatewayService) GetCPUTierCatalog(ctx context.Context, _ *pb.GetCPUTierCatalogRequest) (*pb.GetCPUTierCatalogResponse, error) {
	info, _ := auth.AuthInfoFromContext(ctx)
	if info == nil || info.Workspace == nil {
		return nil, fmt.Errorf("workspace authentication required")
	}
	if gws.cpuPricing == nil {
		return &pb.GetCPUTierCatalogResponse{Catalog: `{"enabled":false,"default_cpu_tier":"","cpu_tiers":{}}`}, nil
	}
	quote, err := gws.cpuPricing.ResolveCPU(ctx, clients.ContainerCostRequest{WorkspaceId: info.Workspace.ExternalId, CatalogOnly: true})
	if err != nil {
		return nil, err
	}
	var catalog map[string]any
	if err := json.Unmarshal(quote.CPUTierCatalog, &catalog); err != nil {
		return nil, err
	}
	workers, err := gws.workerRepo.GetAllWorkers()
	if err != nil {
		return nil, err
	}
	ready := map[string]map[string]bool{}
	for _, worker := range workers {
		if worker.CPUTierProtocol == types.CPUTierProtocolVersion && worker.Status == types.WorkerStatusAvailable && worker.TotalCpu > 0 && worker.TotalMemory > 0 {
			if ready[worker.PoolName] == nil {
				ready[worker.PoolName] = map[string]bool{}
			}
			ready[worker.PoolName][worker.Runtime] = true
		}
	}
	pools := map[string]any{}
	for name, p := range gws.scheduler.PoolConfigs() {
		if p.CPUClass != "" {
			pools[name] = map[string]any{"cpu_class": p.CPUClass, "multiplier": p.CPUShareMultiplier, "valid": p.CPUClass.MatchesPool(p), "runtime": p.ContainerRuntime, "ready": len(ready[name]) > 0, "ready_runtimes": ready[name]}
		}
	}
	catalog["pools"] = pools
	catalog["capabilities"] = []string{"cpu_tiers", "immutable_cpu_prices", "durable_spot_stop"}
	data, err := json.Marshal(catalog)
	return &pb.GetCPUTierCatalogResponse{Catalog: string(data)}, err
}

func (gws *GatewayService) IsHostedCPUPool(name string) bool {
	p, ok := gws.scheduler.PoolConfig(name)
	return ok && p.GPUType == "" && !p.Mode.AgentHosted()
}
