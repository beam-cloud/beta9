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
	Ready         bool            `json:"ready"`
	ReadyRuntimes map[string]bool `json:"ready_runtimes"`
}

// GetCPUTierCatalog returns the workspace's CPU tier policy with the gateway's
// hosted CPU pools, the tier each sells and its readiness, and the gateway's
// CPU tier capabilities.
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
		if class := p.CPUClass(); class != "" {
			pools[name] = &cpuTierPool{CPUClass: class, ReadyRuntimes: map[string]bool{}}
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

// ResolveCPUTier returns the tier a new CPU workload runs on: the requested one,
// else the workspace default. Ineligible work (GPU, platform and private
// workloads) and work that targets a pool other than a hosted CPU pool runs
// untiered. The pool never sets the tier, and untiered work can't target a
// shared pool, which it never opted into.
func (gws *GatewayService) ResolveCPUTier(ctx context.Context, workspace *types.Workspace, requested types.CPUTier, poolName string, eligible bool) (types.CPUTier, error) {
	pool, known := gws.scheduler.PoolConfig(poolName)
	if !eligible || poolName != "" && !(known && pool.HostedCPU()) {
		if requested != "" {
			return "", types.ErrCPUTierIneligible
		}
		return "", nil
	}
	policy, err := gws.billing.CPUTierPolicy(ctx, workspace.ExternalId)
	if err != nil {
		return "", err
	}
	tier, err := policy.Resolve(requested)
	if err == nil && tier == "" && poolName != "" && !tier.MatchesPool(pool) {
		return "", fmt.Errorf("pool %q runs shared CPU; set cpu_tier to %q to use it", poolName, types.CPUTierShared)
	}
	return tier, err
}

// resolveStubCPUTier sets a new stub's CPU tier, which must suit the pool the
// stub targets. eligible is false for stubs placed on private capacity.
func (gws *GatewayService) resolveStubCPUTier(ctx context.Context, workspace *types.Workspace, kind types.StubType, config *types.StubConfigV1, eligible bool) error {
	selector := config.PoolSelector()
	tier, err := gws.ResolveCPUTier(ctx, workspace, config.CPUTier, selector, eligible && !kind.IsPlatformWorkload() && !config.RequiresGPU())
	if err != nil {
		return err
	}
	if pool, _ := gws.scheduler.PoolConfig(selector); tier != "" && selector != "" && !tier.MatchesPool(pool) {
		return fmt.Errorf("pool %q does not run %s CPU work", selector, tier)
	}
	config.CPUTier = tier
	return config.ValidateCPUTier()
}

// IsHostedCPUPool reports whether the named pool is Beam-hosted CPU capacity,
// the only kind a CPU tier can run on.
func (gws *GatewayService) IsHostedCPUPool(name string) bool {
	p, ok := gws.scheduler.PoolConfig(name)
	return ok && p.HostedCPU()
}
