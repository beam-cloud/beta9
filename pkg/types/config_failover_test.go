package types

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAppConfigIsChecked(t *testing.T) {
	config := func(change func(*AppConfig)) AppConfig {
		c := AppConfig{Worker: WorkerConfig{
			CleanupPendingWorkerAgeLimit: 10 * time.Minute,
			Pools: map[string]WorkerPoolConfig{
				"a10g": {GPUType: "A10G", Mode: PoolModeLocal, StalledAfter: Duration(2 * time.Minute)},
				"spot": {GPUType: "L40S", AlsoServes: []string{"A10G"}, Provisioning: WorkerPoolProvisioningConfig{Providers: []string{"verda"}, MaxNodes: 2}},
			},
		}}
		if change != nil {
			change(&c)
		}
		return c
	}
	pool := func(name string, change func(*WorkerPoolConfig)) func(*AppConfig) {
		return func(c *AppConfig) {
			p := c.Worker.Pools[name]
			change(&p)
			c.Worker.Pools[name] = p
		}
	}

	for _, test := range []struct {
		name    string
		config  AppConfig
		wantErr string
	}{
		{name: "nothing configured", config: AppConfig{}},
		{name: "pools and caps", config: config(func(c *AppConfig) { c.Providers.MaxHourlyCents, c.Providers.MaxDailyCents = 5000, 20000 })},
		{name: "a pool that is wrong", config: config(pool("spot", func(p *WorkerPoolConfig) { p.Provisioning.MaxNodes = 0 })),
			wantErr: `worker pool "spot": provisioning.maxNodes must be at least 1`},
		{name: "the first pool by name is reported", config: config(func(c *AppConfig) {
			c.Worker.Pools["b"] = WorkerPoolConfig{CPUShareMultiplier: 0.5}
			c.Worker.Pools["c"] = WorkerPoolConfig{CPUShareMultiplier: 0.5}
		}), wantErr: `worker pool "b"`},
		{name: "stalledAfter as long as a pending worker lives", config: config(pool("a10g", func(p *WorkerPoolConfig) { p.StalledAfter = Duration(10 * time.Minute) })),
			wantErr: `worker pool "a10g": stalledAfter is 10m0s, but a worker that is still Pending after 10m0s is removed (worker.cleanupPendingWorkerAgeLimit)`},
		{name: "stalledAfter just under it", config: config(pool("a10g", func(p *WorkerPoolConfig) { p.StalledAfter = Duration(10*time.Minute - time.Second) }))},
		{name: "no pending limit", config: config(func(c *AppConfig) {
			c.Worker.CleanupPendingWorkerAgeLimit = 0
			pool("a10g", func(p *WorkerPoolConfig) { p.StalledAfter = Duration(time.Hour) })(c)
		})},
		{name: "negative hourly cap", config: config(func(c *AppConfig) { c.Providers.MaxHourlyCents = -1 }), wantErr: "providers.maxHourlyCents cannot be negative"},
		{name: "negative daily cap", config: config(func(c *AppConfig) { c.Providers.MaxDailyCents = -1 }), wantErr: "providers.maxDailyCents cannot be negative"},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := test.config.Validate()
			if test.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, test.wantErr)
		})
	}
}

func TestAlsoServes(t *testing.T) {
	pool := WorkerPoolConfig{GPUType: "RTX4090", AlsoServes: []string{"A10G", "t4"}}

	require.NoError(t, pool.validateAlsoServes())
	require.True(t, pool.Serves("A10G"))
	require.True(t, pool.Serves("a10g"), "GPU spelling is case-insensitive")
	require.True(t, pool.Serves("T4"))
	require.False(t, pool.Serves("RTX4090"), "a pool's own gpuType is not something it also serves")
	require.False(t, pool.Serves("H100"))
	require.False(t, WorkerPoolConfig{GPUType: "RTX4090"}.Serves("A10G"))
}

func TestValidateAlsoServes(t *testing.T) {
	for _, test := range []struct {
		name    string
		pool    WorkerPoolConfig
		wantErr string
	}{
		{name: "none", pool: WorkerPoolConfig{}},
		{name: "other gpu types", pool: WorkerPoolConfig{GPUType: "L40S", AlsoServes: []string{"A10G", "T4"}}},
		{name: "cpu pool", pool: WorkerPoolConfig{AlsoServes: []string{"A10G"}}, wantErr: "a CPU pool cannot run GPU requests"},
		{name: "empty entry", pool: WorkerPoolConfig{GPUType: "L40S", AlsoServes: []string{" "}}, wantErr: "empty GPU type"},
		{name: "any", pool: WorkerPoolConfig{GPUType: "L40S", AlsoServes: []string{"any"}}, wantErr: "already reach every GPU pool"},
		{name: "its own type", pool: WorkerPoolConfig{GPUType: "L40S", AlsoServes: []string{"l40s"}}, wantErr: "the pool's own gpuType"},
		// A pool that requires a selector gets no requests for its own type
		// that name no pool, unless it lists the type.
		{name: "its own type behind a selector", pool: WorkerPoolConfig{GPUType: "L40S", RequiresPoolSelector: true, AlsoServes: []string{"L40S", "A10G"}}},
		{name: "listed twice", pool: WorkerPoolConfig{GPUType: "L40S", AlsoServes: []string{"A10G", "a10g"}}, wantErr: "twice"},
		{name: "a name with a hyphen", pool: WorkerPoolConfig{GPUType: "L40S", AlsoServes: []string{"A100-80", "RTX4000Ada"}}},
		{name: "not a GPU type", pool: WorkerPoolConfig{GPUType: "L40S", AlsoServes: []string{"L50S"}}, wantErr: `"L50S", which is not a GPU type that requests ask for`},
		{name: "no GPU", pool: WorkerPoolConfig{GPUType: "L40S", AlsoServes: []string{"cpu"}}, wantErr: "not a GPU type that requests ask for"},
		// Requests carry the catalog's spelling, which is all a pool is matched on.
		{name: "another spelling", pool: WorkerPoolConfig{GPUType: "L40S", AlsoServes: []string{"RTX 4090"}}, wantErr: `"RTX 4090": requests ask for that GPU as RTX4090`},
		{name: "a vendor's name", pool: WorkerPoolConfig{GPUType: "L40S", AlsoServes: []string{"A100-80GB"}}, wantErr: "requests ask for that GPU as A100-80"},
		{name: "its own type in another spelling", pool: WorkerPoolConfig{GPUType: "RTX 4090", AlsoServes: []string{"rtx4090"}}, wantErr: "the pool's own gpuType"},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := test.pool.validateAlsoServes()
			if test.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, test.wantErr)
		})
	}
}

func TestChainsConvertToAlsoServes(t *testing.T) {
	chains := FailoverConfig{Enabled: true, Chains: map[string]FailoverChain{
		"a10g": {Pools: []string{"rtx4090", "l40s"}},
		"T4":   {Pools: []string{"l40s"}},
	}}

	require.Equal(t, map[string][]string{"rtx4090": {"A10G"}, "l40s": {"A10G", "T4"}}, chains.AlsoServes())

	chains.Enabled = false
	require.Empty(t, chains.AlsoServes())
}

// The old section is no longer validated, so the conversion has to cope with
// whatever it holds: a chain that only had an on-demand step, blank names,
// the same GPU type spelt twice.
func TestChainsThatNameNothingConvertToNothing(t *testing.T) {
	chains := FailoverConfig{Enabled: true, Chains: map[string]FailoverChain{
		"H100": {},
		" ":    {Pools: []string{"l40s"}},
		"A10G": {Pools: []string{"l40s", "l40s", " "}},
		"a10g": {Pools: []string{"l40s"}},
	}}

	require.Equal(t, map[string][]string{"l40s": {"A10G"}}, chains.AlsoServes())
}

func TestProvisioningIsChecked(t *testing.T) {
	buys := func(change func(*WorkerPoolConfig)) WorkerPoolConfig {
		pool := WorkerPoolConfig{GPUType: "L40S", Provisioning: WorkerPoolProvisioningConfig{Providers: []string{"verda"}, MaxNodes: 8}}
		if change != nil {
			change(&pool)
		}
		return pool
	}

	for _, test := range []struct {
		name    string
		pool    WorkerPoolConfig
		wantErr string
	}{
		{name: "no block", pool: WorkerPoolConfig{GPUType: "L40S"}},
		{name: "provider and maxNodes are enough", pool: buys(nil)},
		{name: "every key", pool: buys(func(c *WorkerPoolConfig) {
			c.Provisioning = WorkerPoolProvisioningConfig{Providers: []string{"verda"}, Spot: true, MaxNodes: 1, MaxGPUs: 8, MaxNodeHourlyCents: 150,
				Regions: []string{"fin-01"}, IdleTimeout: Duration(time.Second), BootTimeout: Duration(time.Hour)}
		})},
		{name: "mode written out", pool: buys(func(c *WorkerPoolConfig) { c.Mode = PoolModeExternal })},
		{name: "a CPU pool", pool: buys(func(c *WorkerPoolConfig) { c.GPUType = "" })},
		{name: "no provider", pool: buys(func(c *WorkerPoolConfig) { c.Provisioning.Providers = nil }), wantErr: "needs a provider"},
		{name: "only spot", pool: WorkerPoolConfig{Provisioning: WorkerPoolProvisioningConfig{Spot: true}}, wantErr: "needs a provider"},
		{name: "blank provider", pool: buys(func(c *WorkerPoolConfig) { c.Provisioning.Providers = []string{" "} }), wantErr: "needs a provider"},
		{name: "several providers", pool: buys(func(c *WorkerPoolConfig) { c.Provisioning.Providers = []string{"verda", "nebius"} })},
		{name: "a blank one of several", pool: buys(func(c *WorkerPoolConfig) { c.Provisioning.Providers = []string{"verda", ""} }), wantErr: "needs a provider"},
		{name: "spot and on demand", pool: buys(func(c *WorkerPoolConfig) { c.Provisioning.Spot, c.Provisioning.OnDemand = true, true })},
		{name: "on demand twice", pool: buys(func(c *WorkerPoolConfig) { c.Provisioning.OnDemand = true }), wantErr: "onDemand goes with provisioning.spot"},
		{name: "no maxNodes", pool: buys(func(c *WorkerPoolConfig) { c.Provisioning.MaxNodes = 0 }), wantErr: "maxNodes must be at least 1"},
		{name: "negative maxNodes", pool: buys(func(c *WorkerPoolConfig) { c.Provisioning.MaxNodes = -1 }), wantErr: "maxNodes must be at least 1"},
		{name: "negative GPU cap", pool: buys(func(c *WorkerPoolConfig) { c.Provisioning.MaxGPUs = -1 }), wantErr: "maxGPUs cannot be negative"},
		{name: "a CPU pool with a GPU cap", pool: buys(func(c *WorkerPoolConfig) {
			c.GPUType, c.Provisioning.MaxGPUs = "", 8
		}), wantErr: "maxGPUs needs a gpuType"},
		{name: "only a GPU cap", pool: WorkerPoolConfig{Provisioning: WorkerPoolProvisioningConfig{MaxGPUs: 8}}, wantErr: "needs a provider"},
		{name: "negative price limit", pool: buys(func(c *WorkerPoolConfig) { c.Provisioning.MaxNodeHourlyCents = -1 }), wantErr: "maxNodeHourlyCents cannot be negative"},
		{name: "a Kubernetes pool", pool: buys(func(c *WorkerPoolConfig) { c.Mode = PoolModeLocal }), wantErr: `remove mode "local"`},
		{name: "a private pool", pool: buys(func(c *WorkerPoolConfig) { c.Mode = PoolModePrivate }), wantErr: `remove mode "private"`},
		{name: "empty region", pool: buys(func(c *WorkerPoolConfig) { c.Provisioning.Regions = []string{"fin-01", ""} }), wantErr: "empty region"},
		// `idleTimeout: 600` in YAML is 600 nanoseconds.
		{name: "a bare number of seconds", pool: buys(func(c *WorkerPoolConfig) { c.Provisioning.IdleTimeout = 600 }), wantErr: "provisioning.idleTimeout is 600ns"},
		{name: "negative idle timeout", pool: buys(func(c *WorkerPoolConfig) { c.Provisioning.IdleTimeout = Duration(-time.Minute) }), wantErr: "provisioning.idleTimeout cannot be negative"},
		{name: "a bare number for the boot timeout", pool: buys(func(c *WorkerPoolConfig) { c.Provisioning.BootTimeout = 20 }), wantErr: "provisioning.bootTimeout is 20ns"},
		{name: "stalledAfter", pool: buys(func(c *WorkerPoolConfig) { c.StalledAfter = Duration(time.Minute) }), wantErr: "provisioning.bootTimeout"},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := test.pool.Validate()
			if test.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, test.wantErr)
		})
	}
}

func TestProvisioningDefaults(t *testing.T) {
	require.Equal(t, 10*time.Minute, WorkerPoolProvisioningConfig{}.IdleAfter())
	require.Equal(t, 20*time.Minute, WorkerPoolProvisioningConfig{}.BootWithin())
	require.Equal(t, 3*time.Minute, WorkerPoolProvisioningConfig{IdleTimeout: Duration(3 * time.Minute)}.IdleAfter())
	require.Equal(t, 5*time.Minute, WorkerPoolProvisioningConfig{BootTimeout: Duration(5 * time.Minute)}.BootWithin())
}

func TestReservationProviderBootTimeoutDefaultsAndOverride(t *testing.T) {
	const set = 3 * time.Minute
	for configured, want := range map[time.Duration]time.Duration{
		0:            ManagedComputeDefaultReservationBootTimeout,
		-time.Minute: ManagedComputeDefaultReservationBootTimeout,
		set:          set,
	} {
		config := ManagedComputeReservationsConfig{BootTimeout: Duration(configured)}
		require.Equal(t, want, config.BootTimeoutOrDefault(), "configured as %s", configured)
	}
}

func TestPoolThatBuysJoinsThroughTheAgentAndSpotIsPreemptable(t *testing.T) {
	spot := WorkerPoolConfig{Provisioning: WorkerPoolProvisioningConfig{Providers: []string{"verda"}, Spot: true, MaxNodes: 2}}
	require.True(t, spot.Buys())
	require.True(t, spot.AgentHosted(), "the scheduler must not start it as a Kubernetes pool")
	spot.ApplyProvisioning()
	require.Equal(t, PoolModeExternal, spot.Mode)
	require.True(t, spot.Preemptable)

	onDemand := WorkerPoolConfig{Provisioning: WorkerPoolProvisioningConfig{Providers: []string{"verda"}, MaxNodes: 2}}
	onDemand.ApplyProvisioning()
	require.Equal(t, PoolModeExternal, onDemand.Mode)
	require.False(t, onDemand.Preemptable, "on-demand machines are not taken back")

	both := WorkerPoolConfig{Provisioning: WorkerPoolProvisioningConfig{Providers: []string{"verda", "nebius"}, Spot: true, OnDemand: true, MaxNodes: 2}}
	require.True(t, both.Buys())
	both.ApplyProvisioning()
	require.False(t, both.Preemptable, "it has machines for work that may not be interrupted")

	kubernetes := WorkerPoolConfig{GPUType: "A10G", Mode: PoolModeLocal}
	require.False(t, kubernetes.Buys())
	kubernetes.ApplyProvisioning()
	require.Equal(t, WorkerPoolConfig{GPUType: "A10G", Mode: PoolModeLocal}, kubernetes)
}

// The pools API takes a pool's config as JSON and stores it as JSON.
func TestProvisioningSurvivesJSON(t *testing.T) {
	pool := WorkerPoolConfig{GPUType: "L40S", Provisioning: WorkerPoolProvisioningConfig{
		Providers: []string{"verda"}, Spot: true, MaxNodes: 8, MaxGPUs: 8, MaxNodeHourlyCents: 150, Regions: []string{"fin-01"},
		IdleTimeout: Duration(5 * time.Minute), BootTimeout: Duration(15 * time.Minute),
	}}
	encoded, err := json.Marshal(pool)
	require.NoError(t, err)
	require.Contains(t, string(encoded), `"provisioning":{"providers":["verda"],"spot":true,"max_nodes":8`)
	require.Contains(t, string(encoded), `"max_gpus":8`)
	require.Contains(t, string(encoded), `"idle_timeout":"5m0s","boot_timeout":"15m0s"`, "durations are written as they are in the config file")

	var decoded WorkerPoolConfig
	require.NoError(t, json.Unmarshal(encoded, &decoded))
	require.Equal(t, pool, decoded)

	plain, err := json.Marshal(WorkerPoolConfig{GPUType: "A10G"})
	require.NoError(t, err)
	require.NotContains(t, string(plain), "provisioning", "a pool that does not buy has no block")
}

func TestPhysicalCPUHoldsTheSchedulableCPUAskedFor(t *testing.T) {
	for _, test := range []struct {
		multiplier  CPUShareMultiplier
		schedulable int64
		physical    int64
	}{
		{0, 4000, 4000},
		{1, 4000, 4000},
		{2, 4000, 2000},
		{2, 4001, 2001},
		{1.5, 1000, 667},
		{4, 1, 1},
		{4, 0, 0},
	} {
		physical := test.multiplier.Physical(test.schedulable)
		require.Equal(t, test.physical, physical, "%v x %d", test.multiplier, test.schedulable)
		require.GreaterOrEqual(t, test.multiplier.Schedulable(physical), test.schedulable)
	}
}

func TestStalledAfterIsChecked(t *testing.T) {
	for _, test := range []struct {
		name    string
		pool    WorkerPoolConfig
		wantErr string
	}{
		{name: "unset", pool: WorkerPoolConfig{GPUType: "A10G"}},
		{name: "a Kubernetes pool", pool: WorkerPoolConfig{GPUType: "A10G", StalledAfter: Duration(2 * time.Minute)}},
		{name: "one second is the shortest", pool: WorkerPoolConfig{StalledAfter: Duration(time.Second)}},
		{name: "negative", pool: WorkerPoolConfig{StalledAfter: Duration(-time.Minute)}, wantErr: "cannot be negative"},
		// `stalledAfter: 120` in YAML is 120 nanoseconds.
		{name: "a bare number", pool: WorkerPoolConfig{StalledAfter: 120}, wantErr: "at least one second"},
		{name: "an external pool", pool: WorkerPoolConfig{Mode: PoolModeExternal, StalledAfter: Duration(time.Minute)}, wantErr: "applies to Kubernetes pools: a pool in external mode has no worker Jobs"},
		{name: "a private pool", pool: WorkerPoolConfig{Mode: PoolModePrivate, StalledAfter: Duration(time.Minute)}, wantErr: "applies to Kubernetes pools: a pool in private mode has no worker Jobs"},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := test.pool.Validate()
			if test.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, test.wantErr)
		})
	}
}

func TestDurationIsWrittenAsText(t *testing.T) {
	encoded, err := json.Marshal(struct {
		Idle Duration `json:"idle"`
		Boot Duration `json:"boot,omitempty"`
	}{Idle: Duration(10 * time.Minute)})
	require.NoError(t, err)
	require.JSONEq(t, `{"idle":"10m0s"}`, string(encoded), "an unset duration is left out like any other")
}

func TestDurationIsReadFromTextOrNanoseconds(t *testing.T) {
	for _, test := range []struct {
		json    string
		want    time.Duration
		wantErr string
	}{
		{json: `"10m"`, want: 10 * time.Minute},
		{json: `"1h30m"`, want: 90 * time.Minute},
		{json: `"0s"`, want: 0},
		{json: `null`, want: 0},
		// What encoding/json writes for a time.Duration, and what a bare
		// number is in the config file. Validation refuses one under a second.
		{json: `600000000000`, want: 10 * time.Minute},
		{json: `600`, want: 600},
		{json: `"ten minutes"`, wantErr: `"ten minutes" is not a duration`},
		{json: `"600"`, wantErr: `"600" is not a duration`},
		{json: `true`, wantErr: "a duration is written as text"},
		{json: `1.5`, wantErr: "a duration is written as text"},
	} {
		t.Run(test.json, func(t *testing.T) {
			var decoded Duration
			err := json.Unmarshal([]byte(test.json), &decoded)
			if test.wantErr != "" {
				require.ErrorContains(t, err, test.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, Duration(test.want), decoded)
		})
	}
}
