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

// GetCPUTierCatalog returns the workspace's CPU tier policy with the gateway's
// tiered pools, their readiness and its CPU tier capabilities.
func (gws *GatewayService) GetCPUTierCatalog(ctx context.Context, _ *pb.GetCPUTierCatalogRequest) (*pb.GetCPUTierCatalogResponse, error) {
	info, _ := auth.AuthInfoFromContext(ctx)
	if info == nil || info.Workspace == nil {
		return nil, fmt.Errorf("workspace authentication required")
	}
	policy, err := gws.billing.CPUTierPolicy(ctx, info.Workspace.ExternalId)
	if err != nil {
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
	data, err := json.Marshal(struct {
		types.CPUTierPolicy
		Pools        map[string]*cpuTierPool `json:"pools"`
		Capabilities []string                `json:"capabilities"`
	}{policy, pools, []string{"cpu_tiers", "durable_spot_stop"}})
	return &pb.GetCPUTierCatalogResponse{Catalog: string(data)}, err
}

// ResolveCPUTier returns the tier a new workload runs on: the requested one, or
// the workspace's default when the workload may use a tier.
func (gws *GatewayService) ResolveCPUTier(ctx context.Context, workspace *types.Workspace, requested types.CPUTier, eligible bool) (types.CPUTier, error) {
	if !eligible {
		if requested != "" {
			return "", types.ErrCPUTierIneligible
		}
		return "", nil
	}
	policy, err := gws.billing.CPUTierPolicy(ctx, workspace.ExternalId)
	if err != nil {
		return "", err
	}
	return policy.Resolve(requested)
}

// resolveStubCPUTier sets a new stub's CPU tier. Only hosted CPU stubs may use
// one; eligible is false for stubs placed on private capacity.
func (gws *GatewayService) resolveStubCPUTier(ctx context.Context, workspace *types.Workspace, kind types.StubType, config *types.StubConfigV1, eligible bool) error {
	selector := config.PoolSelector()
	pool, known := gws.scheduler.PoolConfig(selector)
	eligible = eligible && !kind.IsPlatformWorkload() && !config.RequiresGPU() && (selector == "" || known && pool.HostedCPU())
	tier, err := gws.ResolveCPUTier(ctx, workspace, config.CPUTier, eligible)
	if err != nil {
		return err
	}
	if tier != "" && selector != "" && !tier.MatchesPool(pool) {
		return fmt.Errorf("pool does not support the selected CPU tier")
	}
	config.CPUTier = tier
	return config.ValidateCPUTier()
}

func (gws *GatewayService) IsHostedCPUPool(name string) bool {
	p, ok := gws.scheduler.PoolConfig(name)
	return ok && p.HostedCPU()
}
