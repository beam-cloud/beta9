package clients

import (
	"context"
	"fmt"
	"math"

	"github.com/beam-cloud/beta9/pkg/types"
)

// BindStubCPU is shared by creation, cloning and redeployment. A new stub gets
// current server prices; inherited/client-supplied pricing is never authority.
func (c *ContainerCostClient) BindStubCPU(ctx context.Context, workspaceID string, kind types.StubType, config *types.StubConfigV1, poolConfig func(string) (types.WorkerPoolConfig, bool), eligible bool) error {
	selector := config.PoolSelector()
	var pool types.WorkerPoolConfig
	if selector != "" {
		var known bool
		pool, known = poolConfig(selector)
		eligible = eligible && known && !pool.Mode.AgentHosted() && pool.GPUType == ""
	}
	eligible = eligible && !kind.IsPlatformWorkload() && !config.RequiresGPU()
	if !eligible {
		if config.CPUTier != "" {
			return fmt.Errorf("CPU tiers apply only to hosted CPU workloads")
		}
		config.CPUPricing = nil
		return nil
	}
	if c == nil {
		if config.CPUTier != "" {
			return fmt.Errorf("CPU tier billing is not configured")
		}
		config.CPUPricing = nil
		return nil
	}
	quote, err := c.ResolveCPU(ctx, ContainerCostRequest{
		WorkspaceId: workspaceID, Cpu: config.Runtime.Cpu, Memory: config.Runtime.Memory,
		CPUTier: config.CPUTier, ResolveCPUTier: true, CPUTierEligible: true,
		NativeVM: config.UseVM, VM: kind.Kind() == types.StubTypeVM,
		Sandbox: kind.Kind() == types.StubTypeSandbox,
	})
	if err != nil {
		return err
	}
	if config.CPUTier != "" && (quote.CPUPricing == nil || quote.CPUPricing.Tier != config.CPUTier) {
		return fmt.Errorf("billing did not resolve the requested CPU tier")
	}
	if quote.CPUPricing != nil {
		if err := quote.CPUPricing.Tier.Validate(config.Runtime.Cpu, config.Runtime.Memory, config.UseVM, false); err != nil {
			return err
		}
		rate, err := quote.CPUPricing.CostPerMs(config.Runtime.Cpu, config.Runtime.Memory)
		if err != nil {
			return err
		}
		if quote.PricingVersion != quote.CPUPricing.Version || math.Abs(quote.CostPerMs-rate) > math.Max(1e-18, rate*1e-12) {
			return fmt.Errorf("CPU quote does not match its stored prices")
		}
		if selector != "" && !quote.CPUPricing.Tier.MatchesPool(pool) {
			return fmt.Errorf("pool does not support the selected CPU tier")
		}
		config.CPUTier = quote.CPUPricing.Tier
	}
	config.CPUPricing = quote.CPUPricing
	return nil
}
