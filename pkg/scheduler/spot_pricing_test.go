package scheduler

import (
	"context"
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestProvisionedSpotPricingUsesSelectedPurchase(t *testing.T) {
	worker := &types.Worker{Id: "worker", MachineId: "selected", PoolName: "shared", Gpu: "RTXPro6000", ProvisionedSpotBilling: true}
	s := &Scheduler{
		ctx:               context.Background(),
		workerPoolManager: NewWorkerPoolManager(),
		provisioner: &fakeProvisioner{sold: []ProvisionedMachine{
			{MachineID: "selected", GPUCount: 4, Spot: true, HourlyMicros: 1_809_452},
			{MachineID: "other", GPUCount: 1, Spot: true, HourlyMicros: 4_000_000},
			{MachineID: "on-demand", GPUCount: 1, HourlyMicros: 2_000_000},
		}},
	}
	var priceCalls []int64
	s.SetSpotBillableMicros(func(providerMicros int64) int64 {
		priceCalls = append(priceCalls, providerMicros)
		return 1_990_398
	})
	pool := types.WorkerPoolConfig{
		Mode:         types.PoolModeExternal,
		Provisioning: types.WorkerPoolProvisioningConfig{Providers: []string{"nebius"}, Spot: true},
	}
	s.workerPoolManager.SetPool(worker.PoolName, pool, nil)

	newRequest := func() *types.ContainerRequest {
		return &types.ContainerRequest{
			ContainerId: "fallback", GpuRequest: []string{"L40S", "A100-80", "RTXPro6000"}, GpuCount: 1,
			Stub: types.StubWithRelated{Stub: types.Stub{Type: types.StubType("asgi/deployment")}},
		}
	}

	request := newRequest()
	require.NoError(t, s.priceProvisionedSpot(worker, []*types.ContainerRequest{request}))
	require.Nil(t, request.ProvisionedSpot, "production rollout flag defaults off")

	s.config.Monitoring.ProvisionedSpotBilling = true
	second := newRequest()
	second.GpuCount = 2
	require.NoError(t, s.priceProvisionedSpot(worker, []*types.ContainerRequest{request, second}))
	require.Equal(t, []int64{1_809_452}, priceCalls)
	require.Equal(t, &types.ProvisionedSpotPricing{
		MachineID: "selected", GPUCount: 4, HourlyCostMicros: 1_990_398,
	}, request.ProvisionedSpot)
	require.Equal(t, request.ProvisionedSpot, second.ProvisionedSpot)
	request.ReleaseMachine()
	require.Nil(t, request.ProvisionedSpot, "a fresh placement must not inherit a previous machine's price")

	oldWorker := *worker
	oldWorker.ProvisionedSpotBilling = false
	require.ErrorContains(t, s.priceProvisionedSpot(&oldWorker, []*types.ContainerRequest{newRequest()}), "does not support")

	for _, machineID := range []string{"manual", "on-demand"} {
		other := *worker
		other.MachineId = machineID
		other.Preemptable = true
		request := newRequest()
		require.NoError(t, s.priceProvisionedSpot(&other, []*types.ContainerRequest{request}))
		require.Nil(t, request.ProvisionedSpot)
	}

	private := *worker
	private.PoolName = "private"
	s.workerPoolManager.SetPool(private.PoolName, types.WorkerPoolConfig{Mode: types.PoolModePrivate}, nil)
	request = newRequest()
	require.NoError(t, s.priceProvisionedSpot(&private, []*types.ContainerRequest{request}))
	require.Nil(t, request.ProvisionedSpot)

	provider := *worker
	provider.PoolName = "provider"
	s.workerPoolManager.SetPool(provider.PoolName, types.WorkerPoolConfig{Mode: types.PoolModeProvider}, nil)
	request = newRequest()
	require.NoError(t, s.priceProvisionedSpot(&provider, []*types.ContainerRequest{request}))
	require.Nil(t, request.ProvisionedSpot)

	request = newRequest()
	request.Stub.Type = types.StubType("managed_endpoint")
	require.NoError(t, s.priceProvisionedSpot(worker, []*types.ContainerRequest{request}))
	require.Nil(t, request.ProvisionedSpot)
	require.Len(t, priceCalls, 1, "excluded routes do not use a managed purchase price")
}
