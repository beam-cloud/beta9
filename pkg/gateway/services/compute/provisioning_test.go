package compute

import (
	"context"
	"regexp"
	"testing"

	model "github.com/beam-cloud/beta9/pkg/compute"
	"github.com/beam-cloud/beta9/pkg/providers"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
)

// buyingTestService is a gateway that has credentials for one provider,
// verda.
func buyingTestService(config types.AppConfig) (*Service, *fakeComputeRepo) {
	repo := &fakeComputeRepo{}
	service := managedPoolTestService(config, repo)
	service.providers = providers.RegistryOf(providers.NewFake("verda", "test"))
	return service, repo
}

// spotPool is the least that makes a pool buy its machines.
func spotPool(change ...func(*types.WorkerPoolConfig)) types.WorkerPoolConfig {
	pool := types.WorkerPoolConfig{
		GPUType:      "L40S",
		Provisioning: types.WorkerPoolProvisioningConfig{Provider: "verda", Spot: true, MaxNodes: 4},
	}
	for _, apply := range change {
		apply(&pool)
	}
	return pool
}

var joinTokenInScript = regexp.MustCompile(`--join-token '?([^' ]+)'?`)

// joinAs runs what the machine's startup script would: a machine with one
// L40S joins with the token in the script.
func joinAs(t *testing.T, service *Service, script, hostname string) *pb.JoinAgentResponse {
	t.Helper()
	token := joinTokenInScript.FindStringSubmatch(script)
	require.NotNil(t, token, "no join token in %q", script)
	response, err := service.JoinAgent(context.Background(), &pb.JoinAgentRequest{
		JoinToken:          token[1],
		MachineFingerprint: "fingerprint-" + hostname,
		Hostname:           hostname,
		CpuCount:           20,
		MemoryMb:           60_000,
		Gpu:                []string{"L40S"},
		GpuCount:           1,
		Schedulable:        true,
	})
	require.NoError(t, err)
	return response
}

func TestCreateManagedPoolThatBuysItsMachines(t *testing.T) {
	ctx := context.Background()
	service, _ := buyingTestService(types.AppConfig{})

	// A provisioning block is all it takes: no mode, and spot machines make
	// the pool preemptable.
	pool, err := service.CreateManagedPool(ctx, clusterAdminAuth(), "spot-l40s", spotPool())
	require.NoError(t, err)
	require.Equal(t, types.PoolModeExternal, pool.Config.Mode)
	require.True(t, pool.Config.Preemptable)
	require.Equal(t, spotPool().Provisioning, pool.Config.Provisioning)

	onDemand, err := service.CreateManagedPool(ctx, clusterAdminAuth(), "on-demand-l40s", spotPool(func(pool *types.WorkerPoolConfig) {
		pool.Provisioning.Spot = false
	}))
	require.NoError(t, err)
	require.False(t, onDemand.Config.Preemptable)

	// A pool cannot buy from a provider the gateway has no credentials for.
	_, err = service.CreateManagedPool(ctx, clusterAdminAuth(), "spot-a10g", spotPool(func(pool *types.WorkerPoolConfig) {
		pool.Provisioning.Provider = "nebius"
	}))
	require.ErrorIs(t, err, model.ErrInvalidManagedPoolConfig)
	require.ErrorContains(t, err, `provisioning.provider "nebius" is not configured under providers (configured: verda)`)

	_, err = service.CreateManagedPool(ctx, clusterAdminAuth(), "spot-a10g", spotPool(func(pool *types.WorkerPoolConfig) {
		pool.Provisioning.MaxNodes = 0
	}))
	require.ErrorIs(t, err, model.ErrInvalidManagedPoolConfig)
	require.ErrorContains(t, err, "provisioning.maxNodes must be at least 1")

	// The same goes for a change to a pool that exists.
	_, err = service.UpdateManagedPool(ctx, clusterAdminAuth(), "spot-l40s", spotPool(func(pool *types.WorkerPoolConfig) {
		pool.Provisioning.Provider = "nebius"
	}))
	require.ErrorIs(t, err, model.ErrInvalidManagedPoolConfig)
}

func TestBuyingPoolsAreThePoolsThisReplicaRunsThatBuy(t *testing.T) {
	ctx := context.Background()
	service, _ := buyingTestService(types.AppConfig{Worker: types.WorkerConfig{Pools: map[string]types.WorkerPoolConfig{
		"spot-a10g":   spotPool(func(pool *types.WorkerPoolConfig) { pool.GPUType = "A10G" }),
		"hand-joined": {Mode: types.PoolModeExternal},
		"kubernetes":  {Mode: types.PoolModeLocal},
	}}})
	require.NoError(t, service.ReconcileManagedPools(ctx))
	_, err := service.CreateManagedPool(ctx, clusterAdminAuth(), "spot-l40s", spotPool())
	require.NoError(t, err)
	_, err = service.CreateManagedPool(ctx, clusterAdminAuth(), "public-cpu", types.WorkerPoolConfig{Mode: types.PoolModeExternal})
	require.NoError(t, err)

	pools, unloaded, err := service.BuyingPools(ctx)
	require.NoError(t, err)
	require.Empty(t, unloaded)
	require.ElementsMatch(t, []string{"spot-a10g", "spot-l40s"}, buyingPoolNames(pools))
	// A pool from the config file is what the API would have made of it.
	require.Equal(t, types.PoolModeExternal, pools["spot-a10g"].Mode)
	require.True(t, pools["spot-a10g"].Preemptable)
	require.Equal(t, 4, pools["spot-l40s"].Provisioning.MaxNodes)

	// A replica whose config no longer has the pool does not buy for it,
	// though the pool's state is still stored.
	newer, _ := buyingTestService(types.AppConfig{})
	newer.managedPoolRepo = service.managedPoolRepo
	pools, unloaded, err = newer.BuyingPools(ctx)
	require.NoError(t, err)
	require.Empty(t, unloaded)
	require.Equal(t, []string{"spot-l40s"}, buyingPoolNames(pools))

	// A pool that no longer buys is not listed.
	_, err = service.UpdateManagedPool(ctx, clusterAdminAuth(), "spot-l40s", types.WorkerPoolConfig{GPUType: "L40S"})
	require.NoError(t, err)
	pools, unloaded, err = service.BuyingPools(ctx)
	require.NoError(t, err)
	require.Empty(t, unloaded)
	require.Equal(t, []string{"spot-a10g"}, buyingPoolNames(pools))
}

// The provisioner drains the machines of a pool it is not told about, so a
// pool that failed to load must not look like a pool that is gone. It is
// named, so that the pools that did load are not held up by it.
func TestBuyingPoolsSaysWhenNotEveryPoolLoaded(t *testing.T) {
	ctx := context.Background()
	service, _ := buyingTestService(types.AppConfig{Worker: types.WorkerConfig{Pools: map[string]types.WorkerPoolConfig{
		"spot-a10g": spotPool(func(pool *types.WorkerPoolConfig) { pool.GPUType = "A10G" }),
		"spot-t4":   spotPool(func(pool *types.WorkerPoolConfig) { pool.GPUType = "T4"; pool.Provisioning.MaxNodes = 0 }),
	}}})
	_ = service.ReconcileManagedPools(ctx)

	pools, unloaded, err := service.BuyingPools(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{"spot-a10g"}, buyingPoolNames(pools))
	require.Len(t, unloaded, 1)
	require.ErrorContains(t, unloaded["spot-t4"], "provisioning.maxNodes must be at least 1")

	// The pools cannot be read at all.
	service.backendRepo = &fakeManagedPoolBackendRepo{workspaceFailures: 1}
	pools, unloaded, err = service.BuyingPools(ctx)
	require.Error(t, err)
	require.Nil(t, pools)
	require.Nil(t, unloaded)
}

// A pool that was made through the API and that a replica's config file now
// defines too is refused by that replica. Its machines are still running.
func TestBuyingPoolsNamesAPoolThatConflictsWithTheConfigFile(t *testing.T) {
	ctx := context.Background()
	service, _ := buyingTestService(types.AppConfig{})
	_, err := service.CreateManagedPool(ctx, clusterAdminAuth(), "spot-l40s", spotPool())
	require.NoError(t, err)

	newer, _ := buyingTestService(types.AppConfig{Worker: types.WorkerConfig{Pools: map[string]types.WorkerPoolConfig{
		"spot-l40s": spotPool(),
		"spot-a10g": spotPool(func(pool *types.WorkerPoolConfig) { pool.GPUType = "A10G" }),
	}}})
	newer.managedPoolRepo = service.managedPoolRepo
	_ = newer.ReconcileManagedPools(ctx)

	pools, unloaded, err := newer.BuyingPools(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{"spot-a10g"}, buyingPoolNames(pools))
	require.Len(t, unloaded, 1)
	require.ErrorContains(t, unloaded["spot-l40s"], `API-managed pool "spot-l40s" conflicts with config`)
}

func buyingPoolNames(pools map[string]types.WorkerPoolConfig) []string {
	names := make([]string, 0, len(pools))
	for name := range pools {
		names = append(names, name)
	}
	return names
}

func TestJoinScriptLetsOneMachineJoinItsPool(t *testing.T) {
	ctx := context.Background()
	service, repo := buyingTestService(types.AppConfig{
		GatewayService: types.GatewayServiceConfig{HTTP: types.HTTPConfig{ExternalHost: "gateway.example.com", ExternalPort: 443, TLS: true}},
	})
	_, err := service.CreateManagedPool(ctx, clusterAdminAuth(), "spot-l40s", spotPool())
	require.NoError(t, err)

	script, tokenHash, err := service.JoinScript(ctx, "spot-l40s", "machine-1")
	require.NoError(t, err)
	require.Contains(t, script, "https://gateway.example.com/install/agent")

	// The token is for that machine in that pool, and does not expire: how
	// long a machine takes to boot is its provider's business.
	token := repo.joinTokens[tokenHash]
	require.NotNil(t, token)
	require.Equal(t, "machine-1", token.MachineID)
	require.Equal(t, "spot-l40s", token.PoolName)
	require.Equal(t, string(types.PoolModeExternal), token.Mode)
	require.NotEmpty(t, token.ManagedPoolInstanceID)
	require.True(t, token.ExpiresAt.IsZero())

	response := joinAs(t, service, script, "verda-1")
	require.True(t, response.Ok, response.ErrMsg)
	joined, err := repo.GetAgentMachineState(ctx, "admin-workspace", "spot-l40s", "machine-1")
	require.NoError(t, err)
	require.NotNil(t, joined, "the machine joined under the ID it was bought with")

	// Only pools that buy hand out such scripts.
	_, err = service.CreateManagedPool(ctx, clusterAdminAuth(), "public-cpu", types.WorkerPoolConfig{Mode: types.PoolModeExternal})
	require.NoError(t, err)
	_, _, err = service.JoinScript(ctx, "public-cpu", "machine-2")
	require.ErrorContains(t, err, `pool "public-cpu" does not buy machines`)
	_, _, err = service.JoinScript(ctx, "no-such-pool", "machine-3")
	require.ErrorContains(t, err, `pool "no-such-pool" does not buy machines`)
}

func TestForgetMachineRemovesAMachineThatJoined(t *testing.T) {
	ctx := context.Background()
	service, repo := buyingTestService(types.AppConfig{})
	workers := &fakeWorkerRepo{}
	service.workerRepo = workers
	_, err := service.CreateManagedPool(ctx, clusterAdminAuth(), "spot-l40s", spotPool())
	require.NoError(t, err)

	script, tokenHash, err := service.JoinScript(ctx, "spot-l40s", "machine-1")
	require.NoError(t, err)
	require.True(t, joinAs(t, service, script, "verda-1").Ok)
	workers.worker = &types.Worker{Id: model.AgentMachineWorkerID("machine-1"), MachineId: "machine-1", PoolName: "spot-l40s", Status: types.WorkerStatusAvailable}

	require.NoError(t, service.ForgetMachine(ctx, "spot-l40s", "machine-1", tokenHash))

	require.Nil(t, workers.worker, "its worker is removed, which returns its requests to the backlog")
	machine, err := repo.GetAgentMachineState(ctx, "admin-workspace", "spot-l40s", "machine-1")
	require.NoError(t, err)
	require.Nil(t, machine)
	require.True(t, repo.joinTokens[tokenHash].Revoked)
	require.False(t, joinAs(t, service, script, "verda-1").Ok, "it cannot join again")

	// Forgetting is safe to repeat: a pass that failed half-way runs again.
	require.NoError(t, service.ForgetMachine(ctx, "spot-l40s", "machine-1", tokenHash))
}

func TestForgetMachineRemovesAMachineThatNeverJoined(t *testing.T) {
	ctx := context.Background()
	service, repo := buyingTestService(types.AppConfig{})
	workers := &fakeWorkerRepo{}
	service.workerRepo = workers
	_, err := service.CreateManagedPool(ctx, clusterAdminAuth(), "spot-l40s", spotPool())
	require.NoError(t, err)

	script, tokenHash, err := service.JoinScript(ctx, "spot-l40s", "machine-1")
	require.NoError(t, err)
	// All there is of it: the worker that stands for it while it boots.
	workers.worker = &types.Worker{Id: model.AgentMachineWorkerID("machine-1"), MachineId: "machine-1", PoolName: "spot-l40s", Status: types.WorkerStatusPending}

	require.NoError(t, service.ForgetMachine(ctx, "spot-l40s", "machine-1", tokenHash))
	require.Nil(t, workers.worker)
	require.True(t, repo.joinTokens[tokenHash].Revoked)

	// Should it boot after all, it is not let in.
	response := joinAs(t, service, script, "verda-1")
	require.False(t, response.Ok)
	require.Equal(t, "join token is invalid or expired", response.ErrMsg)

	// A machine with no worker and no token is forgotten without complaint.
	require.NoError(t, service.ForgetMachine(ctx, "spot-l40s", "machine-2", ""))
}

func TestForgetMachineLeavesOtherMachinesAlone(t *testing.T) {
	ctx := context.Background()
	service, repo := buyingTestService(types.AppConfig{})
	_, err := service.CreateManagedPool(ctx, clusterAdminAuth(), "spot-l40s", spotPool())
	require.NoError(t, err)

	first, firstHash, err := service.JoinScript(ctx, "spot-l40s", "machine-1")
	require.NoError(t, err)
	second, _, err := service.JoinScript(ctx, "spot-l40s", "machine-2")
	require.NoError(t, err)
	require.True(t, joinAs(t, service, first, "verda-1").Ok)
	require.True(t, joinAs(t, service, second, "verda-2").Ok)
	other := &types.Worker{Id: model.AgentMachineWorkerID("machine-2"), MachineId: "machine-2", PoolName: "spot-l40s"}
	service.workerRepo = &fakeWorkerRepo{worker: other}

	require.NoError(t, service.ForgetMachine(ctx, "spot-l40s", "machine-1", firstHash))

	machine, err := repo.GetAgentMachineState(ctx, "admin-workspace", "spot-l40s", "machine-2")
	require.NoError(t, err)
	require.NotNil(t, machine)
	require.Same(t, other, service.workerRepo.(*fakeWorkerRepo).worker)
}

// A pool either buys its machines or has them joined by hand. Machines the
// provisioner has no record of would never be given back, and would count
// against nothing.
func TestAPoolThatBuysTakesNoMachinesByHand(t *testing.T) {
	ctx := context.Background()
	service, _ := buyingTestService(types.AppConfig{})
	_, err := service.CreateManagedPool(ctx, clusterAdminAuth(), "spot-l40s", spotPool())
	require.NoError(t, err)

	_, err = service.CreateManagedMachine(ctx, clusterAdminAuth(), "spot-l40s")
	require.ErrorIs(t, err, model.ErrManagedPoolBuysMachines)
	require.ErrorContains(t, err, `"spot-l40s" gets its machines from verda; raise provisioning.maxNodes for more`)
}

func TestAPoolWithMachinesJoinedByHandCannotStartBuying(t *testing.T) {
	ctx := context.Background()
	service, _ := buyingTestService(types.AppConfig{})
	_, err := service.CreateManagedPool(ctx, clusterAdminAuth(), "l40s", types.WorkerPoolConfig{GPUType: "L40S"})
	require.NoError(t, err)
	bootstrap, err := service.CreateManagedMachine(ctx, clusterAdminAuth(), "l40s")
	require.NoError(t, err)
	require.True(t, joinAs(t, service, bootstrap.InstallCommand, "by-hand").Ok)

	_, err = service.UpdateManagedPool(ctx, clusterAdminAuth(), "l40s", spotPool())
	require.ErrorIs(t, err, model.ErrManagedPoolBuysMachines)
	state, err := service.managedPoolRepo.GetManagedPoolState(ctx, "admin-workspace", "l40s")
	require.NoError(t, err)
	require.False(t, state.WorkerConfig.Buys(), "the pool is as it was")

	// With the machine removed, it can.
	require.NoError(t, service.DeleteManagedMachine(ctx, clusterAdminAuth(), "l40s", bootstrap.MachineID))
	pool, err := service.UpdateManagedPool(ctx, clusterAdminAuth(), "l40s", spotPool())
	require.NoError(t, err)
	require.True(t, pool.Config.Buys())

	// And its limits can be changed while it holds machines it bought.
	script, _, err := service.JoinScript(ctx, "l40s", "machine-1")
	require.NoError(t, err)
	require.True(t, joinAs(t, service, script, "verda-1").Ok)
	pool, err = service.UpdateManagedPool(ctx, clusterAdminAuth(), "l40s", spotPool(func(pool *types.WorkerPoolConfig) {
		pool.Provisioning.MaxNodes = 8
	}))
	require.NoError(t, err)
	require.Equal(t, 8, pool.Config.Provisioning.MaxNodes)
}

func TestAPoolThatBuysCanBeDeletedWhileItHoldsMachines(t *testing.T) {
	ctx := context.Background()
	service, _ := buyingTestService(types.AppConfig{})

	// A pool whose machines were joined by hand cannot: nobody would take
	// them away.
	_, err := service.CreateManagedPool(ctx, clusterAdminAuth(), "l40s", types.WorkerPoolConfig{GPUType: "L40S"})
	require.NoError(t, err)
	bootstrap, err := service.CreateManagedMachine(ctx, clusterAdminAuth(), "l40s")
	require.NoError(t, err)
	require.True(t, joinAs(t, service, bootstrap.InstallCommand, "by-hand").Ok)
	require.ErrorIs(t, service.DeleteManagedPool(ctx, clusterAdminAuth(), "l40s"), model.ErrManagedPoolInUse)

	// The provisioner gives back what a pool bought once the pool is gone.
	_, err = service.CreateManagedPool(ctx, clusterAdminAuth(), "spot-l40s", spotPool())
	require.NoError(t, err)
	script, _, err := service.JoinScript(ctx, "spot-l40s", "machine-1")
	require.NoError(t, err)
	require.True(t, joinAs(t, service, script, "verda-1").Ok)

	require.NoError(t, service.DeleteManagedPool(ctx, clusterAdminAuth(), "spot-l40s"))
	pools, unloaded, err := service.BuyingPools(ctx)
	require.NoError(t, err)
	require.Empty(t, pools)
	require.Empty(t, unloaded)

	// A machine that was still booting does not join the pool's successor.
	_, err = service.CreateManagedPool(ctx, clusterAdminAuth(), "spot-l40s", spotPool())
	require.NoError(t, err)
	require.False(t, joinAs(t, service, script, "verda-1").Ok)
}
