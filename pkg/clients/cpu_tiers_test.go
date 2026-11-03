package clients

import (
	"context"
	"encoding/json"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestCPUPriceResolutionOnCreationAndClone(t *testing.T) {
	calls := 0
	binding := &types.CPUPriceBinding{Tier: types.CPUTierShared, Version: "current", CPUPriceID: "price_cpu", MemoryPriceID: "price_memory", CPUCentsPerHour: "1.1", MemoryCentsPerGiBHour: "0.55"}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		var request ContainerCostRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
		require.Equal(t, "owner-workspace", request.WorkspaceId)
		require.True(t, request.ResolveCPUTier)
		require.True(t, request.NativeVM)
		require.True(t, request.VM)
		require.False(t, request.Sandbox, "VM and sandbox flags must be mutually exclusive")
		require.NoError(t, json.NewEncoder(w).Encode(ContainerCostResponse{CostPerMs: "0.0000024444444444444444", PricingVersion: binding.Version, CPUPricing: binding}))
	}))
	defer server.Close()
	client := NewContainerCostClient(types.ContainerCostHookConfig{Endpoint: server.URL, Token: "test"})
	config := types.StubConfigV1{Runtime: types.Runtime{Cpu: 4000, Memory: 8192}, UseVM: true}
	lookup := func(string) (types.WorkerPoolConfig, bool) { return types.WorkerPoolConfig{}, false }
	require.NoError(t, client.BindStubCPU(context.Background(), "owner-workspace", types.StubType(types.StubTypeVM), &config, lookup, true))
	require.Equal(t, types.CPUTierShared, config.CPUTier)
	require.Equal(t, "current", config.CPUPricing.Version)
	// Clones never inherit authoritative prices, even when their selected tier stays the same.
	config.CPUPricing = &types.CPUPriceBinding{Tier: types.CPUTierShared, Version: "old"}
	binding.Version = "next"
	require.NoError(t, client.BindStubCPU(context.Background(), "owner-workspace", types.StubType(types.StubTypeVM), &config, lookup, true))
	require.Equal(t, "next", config.CPUPricing.Version)
	require.Equal(t, 2, calls)
	// Workers reuse the stored amount without a live billing connection.
	var offline *ContainerCostClient
	raw, _ := json.Marshal(config)
	quote, err := offline.GetContainerCostQuote(context.Background(), &types.ContainerRequest{Cpu: 4000, Memory: 8192, UseVM: true, Stub: types.StubWithRelated{Stub: types.Stub{Config: string(raw)}}})
	require.NoError(t, err)
	require.InDelta(t, 8.8, quote.CostPerMs*3600000, 1e-12)
	require.Equal(t, "next", quote.PricingVersion)
}

func TestPrivateAndGPUKeepLegacyPricingWithoutBillingLookup(t *testing.T) {
	var offline *ContainerCostClient
	lookup := func(string) (types.WorkerPoolConfig, bool) {
		return types.WorkerPoolConfig{Mode: types.PoolModePrivate}, true
	}
	for _, config := range []types.StubConfigV1{
		{Pool: &types.PoolConfig{Name: "private"}},
		{Runtime: types.Runtime{Gpus: []types.GpuType{types.GPU_T4}, GpuCount: 1}},
	} {
		require.NoError(t, offline.BindStubCPU(context.Background(), "workspace", types.StubType("function"), &config, lookup, true))
		require.Nil(t, config.CPUPricing)
		config.CPUTier = types.CPUTierShared
		require.Error(t, offline.BindStubCPU(context.Background(), "workspace", types.StubType("function"), &config, lookup, true))
	}
}
