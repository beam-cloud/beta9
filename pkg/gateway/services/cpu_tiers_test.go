package gatewayservices

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/beam-cloud/beta9/pkg/clients"
	"github.com/beam-cloud/beta9/pkg/scheduler"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestResolveStubCPUTier(t *testing.T) {
	billing := func(enabled bool) *clients.ContainerCostClient {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = fmt.Fprintf(w, `{"cost_per_ms":"0","cpu_tier_policy":{"enabled":%t,"default_cpu_tier":"performance"}}`, enabled)
		}))
		t.Cleanup(server.Close)
		return clients.NewContainerCostClient(types.ContainerCostHookConfig{Endpoint: server.URL, Token: "test"})
	}
	pools := scheduler.NewWorkerPoolManager()
	pools.SetPool("private", types.WorkerPoolConfig{Mode: types.PoolModePrivate}, nil)
	pools.SetPool("performance", types.WorkerPoolConfig{}, nil)
	pools.SetPool("shared", types.WorkerPoolConfig{CPUShareMultiplier: 2.5}, nil)
	pools.SetPool("managed", types.WorkerPoolConfig{Mode: types.PoolModeExternal, CPUShareMultiplier: 3}, nil)
	gws := &GatewayService{scheduler: scheduler.NewSchedulerForCapacityChecks(nil, nil, pools)}
	resolve := func(config types.StubConfigV1) (types.CPUTier, error) {
		err := gws.resolveStubCPUTier(context.Background(), &types.Workspace{ExternalId: "workspace"}, types.StubType("function"), &config, true)
		return config.CPUTier, err
	}
	onPool := func(name string, config types.StubConfigV1) types.StubConfigV1 {
		config.Pool = &types.PoolConfig{Name: name}
		return config
	}
	gpu := types.Runtime{Gpus: []types.GpuType{types.GPU_T4}, GpuCount: 1}

	// Without a billing service no workspace may choose a tier, and untiered
	// stubs may target any hosted pool.
	tier, err := resolve(types.StubConfigV1{})
	require.NoError(t, err)
	require.Empty(t, tier)
	_, err = resolve(types.StubConfigV1{CPUTier: types.CPUTierSpot})
	require.ErrorContains(t, err, "CPU tiers are not enabled")
	for _, pool := range []string{"performance", "shared"} {
		tier, err = resolve(onPool(pool, types.StubConfigV1{}))
		require.NoError(t, err)
		require.Empty(t, tier)
	}

	// An enrolled workspace gets Performance, which may target a shared pool
	// (where it doesn't share CPU), but may not choose another tier.
	gws.billing = billing(false)
	large := types.StubConfigV1{Runtime: types.Runtime{Cpu: 16000, Memory: 1024}}
	for _, config := range []types.StubConfigV1{large, onPool("performance", large), onPool("shared", large)} {
		tier, err = resolve(config)
		require.NoError(t, err)
		require.Equal(t, types.CPUTierPerformance, tier)
	}
	_, err = resolve(types.StubConfigV1{CPUTier: types.CPUTierShared})
	require.ErrorContains(t, err, "CPU tiers are not enabled")

	// A workspace with tiers enabled may choose Shared, but only within its
	// limits and on a pool that runs it.
	gws.billing = billing(true)
	shared := types.StubConfigV1{CPUTier: types.CPUTierShared, Runtime: types.Runtime{Cpu: 1000, Memory: 1024}}
	for _, pool := range []string{"shared", "managed"} {
		tier, err = resolve(onPool(pool, shared))
		require.NoError(t, err)
		require.Equal(t, types.CPUTierShared, tier)
	}
	_, err = resolve(onPool("performance", shared))
	require.ErrorContains(t, err, `pool "performance" does not run shared CPU work`)
	_, err = resolve(types.StubConfigV1{CPUTier: types.CPUTierShared, Runtime: large.Runtime})
	require.ErrorContains(t, err, "the shared tier supports at most 8 vCPU")

	// Private and GPU stubs keep their placement and get no tier.
	for _, config := range []types.StubConfigV1{onPool("private", types.StubConfigV1{}), {Runtime: gpu}} {
		tier, err = resolve(config)
		require.NoError(t, err)
		require.Empty(t, tier)
		config.CPUTier = types.CPUTierShared
		_, err = resolve(config)
		require.ErrorIs(t, err, types.ErrCPUTierIneligible)
	}
}
