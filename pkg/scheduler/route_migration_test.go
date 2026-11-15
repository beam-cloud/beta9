package scheduler

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/kubernetes/fake"
)

func chainsForTest(chains map[string][]string) types.FailoverConfig {
	config := types.FailoverConfig{Enabled: true, Chains: map[string]types.FailoverChain{}}
	for gpu, pools := range chains {
		config.Chains[gpu] = types.FailoverChain{Pools: pools}
	}
	return config
}

func TestChainsBecomeAlsoServes(t *testing.T) {
	pools := map[string]types.WorkerPoolConfig{
		"a10g":    {GPUType: "A10G", Priority: 10},
		"rtx4090": {GPUType: "RTX4090", Priority: 5},
		"l40s":    {GPUType: "L40S", Priority: 3},
	}

	migration := migrateFailoverChains(chainsForTest(map[string][]string{"a10g": {"rtx4090", "l40s"}}), pools)

	require.Equal(t, map[string][]string{"rtx4090": {"A10G"}, "l40s": {"A10G"}}, migration.alsoServes)
	require.Equal(t, []string{
		"worker.pools.l40s.alsoServes: [A10G]",
		"worker.pools.rtx4090.alsoServes: [A10G]",
	}, migration.write)
	// The chain's order is the order the pools' priorities give.
	require.Empty(t, migration.reordered)
	require.Empty(t, migration.notes)
}

func TestChainWhoseOrderDiffersFromPrioritiesIsFlagged(t *testing.T) {
	pools := map[string]types.WorkerPoolConfig{
		"rtx4090": {GPUType: "RTX4090", Priority: 5},
		"l40s":    {GPUType: "L40S", Priority: 9},
		"h100":    {GPUType: "H100", Priority: 5},
	}

	migration := migrateFailoverChains(chainsForTest(map[string][]string{"A10G": {"rtx4090", "l40s", "h100"}}), pools)

	require.Equal(t, []string{
		"A10G fallbacks were tried in the order [rtx4090, l40s, h100] and are now tried by priority, then name: " +
			"[l40s, h100, rtx4090]; set worker.pools.<name>.priority to keep the old order",
	}, migration.reordered)
}

func TestChainNamingAPoolOutsideTheConfigFile(t *testing.T) {
	pools := map[string]types.WorkerPoolConfig{"rtx4090": {GPUType: "RTX4090"}}

	migration := migrateFailoverChains(chainsForTest(map[string][]string{"A10G": {"spot-l40s", "rtx4090"}}), pools)

	require.Equal(t, []string{
		"worker.pools.rtx4090.alsoServes: [A10G]",
		"worker.pools.spot-l40s.alsoServes: [A10G]  (not in this config: set also_serves on it through the pools API)",
	}, migration.write)
	// Its priority is not known here, so its place in the order is not judged.
	require.Empty(t, migration.reordered)
}

func TestSeveralChainsAreNotedBecauseRequestsNowUseAllOfThem(t *testing.T) {
	pools := map[string]types.WorkerPoolConfig{"l40s": {GPUType: "L40S"}, "rtx4090": {GPUType: "RTX4090"}}

	migration := migrateFailoverChains(chainsForTest(map[string][]string{"A10G": {"l40s"}, "T4": {"l40s", "rtx4090"}}), pools)

	require.Equal(t, []string{
		"worker.pools.l40s.alsoServes: [A10G, T4]",
		"worker.pools.rtx4090.alsoServes: [T4]",
	}, migration.write)
	require.Len(t, migration.notes, 1)
	require.Contains(t, migration.notes[0], "uses the fallback pools of all of them")
}

func TestDisabledChainsHaveNoEffect(t *testing.T) {
	config := chainsForTest(map[string][]string{"A10G": {"rtx4090"}})
	config.Enabled = false

	migration := migrateFailoverChains(config, map[string]types.WorkerPoolConfig{"rtx4090": {GPUType: "RTX4090"}})

	require.Empty(t, migration.alsoServes)
	require.Empty(t, migration.write)
	require.Equal(t, []string{"scheduling.failover is disabled, so its chains have no effect; remove the section"}, migration.notes)

	require.Equal(t, chainMigration{alsoServes: map[string][]string{}}, migrateFailoverChains(types.FailoverConfig{}, nil),
		"a config without the section says nothing")
}

func TestChainsAreAddedToWhatAPoolAlreadyServes(t *testing.T) {
	migration := migrateFailoverChains(chainsForTest(map[string][]string{"A10G": {"l40s"}, "T4": {"l40s"}}), nil)
	written := []string{"t4"}

	config := migration.apply("l40s", types.WorkerPoolConfig{GPUType: "L40S", AlsoServes: written})
	require.Equal(t, []string{"t4", "A10G"}, config.AlsoServes)
	require.Equal(t, []string{"t4"}, written, "the pool's own config is not modified")
	require.NoError(t, config.Validate())

	untouched := migration.apply("a10g", types.WorkerPoolConfig{GPUType: "A10G"})
	require.Empty(t, untouched.AlsoServes)
}

// A config written for the previous release may still hold what that release
// accepted, such as a chain with only an on-demand step. The section is on
// its way out: nothing in it may stop the gateway starting.
func TestAnOldFailoverSectionNeverStopsTheSchedulerStarting(t *testing.T) {
	server := miniredis.RunT(t)
	rdb, err := common.NewRedisClient(types.RedisConfig{Addrs: []string{server.Addr()}, Mode: types.RedisModeSingle})
	require.NoError(t, err)

	manager, err := common.NewConfigManager[types.AppConfig]()
	require.NoError(t, err)
	config := manager.GetConfig()
	config.Worker.Pools = map[string]types.WorkerPoolConfig{
		"a10g": {GPUType: "A10G", Mode: types.PoolModeLocal},
		"l40s": {GPUType: "L40S", Mode: types.PoolModeLocal},
	}
	config.Scheduling.Failover = types.FailoverConfig{Enabled: true, Chains: map[string]types.FailoverChain{
		"H100": {},
		"A10G": {Pools: []string{"a10g", "l40s", "l40s", " "}},
		" ":    {Pools: []string{"l40s"}},
	}}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, err := NewScheduler(ctx, config, rdb, nil, nil, nil, WithKubernetes(fake.NewSimpleClientset()))
	require.NoError(t, err)

	l40s, _ := s.workerPoolManager.GetPool("l40s")
	require.Equal(t, []string{"A10G"}, l40s.Config.AlsoServes)
	a10g, _ := s.workerPoolManager.GetPool("a10g")
	require.Empty(t, a10g.Config.AlsoServes, "a pool does not also serve its own GPU type")
}
