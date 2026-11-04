package gatewayservices

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/beam-cloud/beta9/pkg/clients"
	"github.com/beam-cloud/beta9/pkg/scheduler"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestResolveStubCPUTier(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"cost_per_ms":"0","cpu_tier_policy":{"enabled":false,"default_cpu_tier":"shared"}}`))
	}))
	defer server.Close()
	pools := scheduler.NewWorkerPoolManager()
	pools.SetPool("private", types.WorkerPoolConfig{Mode: types.PoolModePrivate}, nil)
	pools.SetPool("performance", types.WorkerPoolConfig{}, nil)
	pools.SetPool("shared", types.WorkerPoolConfig{CPUShareMultiplier: 2.5}, nil)
	gws := &GatewayService{scheduler: scheduler.NewSchedulerForCapacityChecks(nil, nil, pools)}
	resolve := func(config types.StubConfigV1) (types.CPUTier, error) {
		err := gws.resolveStubCPUTier(context.Background(), &types.Workspace{ExternalId: "workspace"}, types.StubType("function"), &config, true)
		return config.CPUTier, err
	}
	gpu := types.Runtime{Gpus: []types.GpuType{types.GPU_T4}, GpuCount: 1}

	// Without a billing service no workspace may choose a tier, and untiered
	// stubs can't target a shared pool.
	tier, err := resolve(types.StubConfigV1{})
	require.NoError(t, err)
	require.Empty(t, tier)
	_, err = resolve(types.StubConfigV1{CPUTier: types.CPUTierSpot})
	require.Error(t, err)
	tier, err = resolve(types.StubConfigV1{Pool: &types.PoolConfig{Name: "performance"}})
	require.NoError(t, err)
	require.Empty(t, tier)
	_, err = resolve(types.StubConfigV1{Pool: &types.PoolConfig{Name: "shared"}})
	require.ErrorContains(t, err, `pool "shared" runs shared CPU`)

	// An enrolled workspace gets its default tier but may not choose another.
	gws.billing = clients.NewContainerCostClient(types.ContainerCostHookConfig{Endpoint: server.URL, Token: "test"})
	tier, err = resolve(types.StubConfigV1{Runtime: types.Runtime{Cpu: 1000, Memory: 1024}})
	require.NoError(t, err)
	require.Equal(t, types.CPUTierShared, tier)
	_, err = resolve(types.StubConfigV1{CPUTier: types.CPUTierPerformance})
	require.Error(t, err)
	_, err = resolve(types.StubConfigV1{Runtime: types.Runtime{Cpu: 16000, Memory: 1024}})
	require.Error(t, err, "shared stubs are limited to 8 vCPU")

	// A targeted pool never sets the tier: it must run the tier the stub gets.
	tier, err = resolve(types.StubConfigV1{Pool: &types.PoolConfig{Name: "shared"}, Runtime: types.Runtime{Cpu: 1000, Memory: 1024}})
	require.NoError(t, err)
	require.Equal(t, types.CPUTierShared, tier)
	_, err = resolve(types.StubConfigV1{Pool: &types.PoolConfig{Name: "performance"}})
	require.ErrorContains(t, err, `pool "performance" does not run shared CPU work`)

	// Private and GPU stubs keep their placement and get no tier.
	for _, config := range []types.StubConfigV1{{Pool: &types.PoolConfig{Name: "private"}}, {Runtime: gpu}} {
		tier, err = resolve(config)
		require.NoError(t, err)
		require.Empty(t, tier)
		config.CPUTier = types.CPUTierShared
		_, err = resolve(config)
		require.ErrorIs(t, err, types.ErrCPUTierIneligible)
	}
}
