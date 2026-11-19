package common

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/knadh/koanf/providers/rawbytes"
	"github.com/stretchr/testify/require"
)

func TestGeeseFileModes(t *testing.T) {
	t.Setenv("CONFIG_PATH", "")
	t.Setenv(types.WorkerMinimalConfigEnv, "true")

	for _, input := range []string{
		"dirMode: 0777\nfileMode: 0666",
		"dirMode: 511\nfileMode: 438",
		"dirMode: \"0777\"\nfileMode: \"0666\"",
	} {
		t.Run(input, func(t *testing.T) {
			manager, err := NewConfigManager[types.AppConfig]()
			require.NoError(t, err)
			data := "storage:\n  workspaceStorage:\n    geese:\n      " + strings.ReplaceAll(input, "\n", "\n      ")
			require.NoError(t, manager.LoadConfig(YAMLConfigFormat, rawbytes.Provider([]byte(data))))
			config := manager.GetConfig()
			require.Equal(t, "0777", config.Storage.WorkspaceStorage.Geese.DirMode)
			require.Equal(t, "0666", config.Storage.WorkspaceStorage.Geese.FileMode)

			// Gateway-to-worker JSON must retain the normalized representation.
			encoded, err := json.Marshal(config)
			require.NoError(t, err)
			require.NoError(t, manager.LoadConfig(JSONConfigFormat, rawbytes.Provider(encoded)))
			manager.tag = "json"
			require.Equal(t, config.Storage.WorkspaceStorage.Geese, manager.GetConfig().Storage.WorkspaceStorage.Geese)
		})
	}

	manager, err := NewConfigManager[types.AppConfig]()
	require.NoError(t, err)
	require.NoError(t, manager.LoadConfig(JSONConfigFormat, rawbytes.Provider([]byte(`{"storage":{"workspace_storage":{"geese":{"dir_mode":511,"file_mode":438}}}}`))))
	manager.tag = "json"
	require.Equal(t, "0777", manager.GetConfig().Storage.WorkspaceStorage.Geese.DirMode)
	require.Equal(t, "0666", manager.GetConfig().Storage.WorkspaceStorage.Geese.FileMode)

	require.NoError(t, manager.LoadConfig(JSONConfigFormat, rawbytes.Provider([]byte(`{"storage":{"workspace_storage":{"geese":{"dir_mode":"511","file_mode":"0000"}}}}`))))
	require.Equal(t, "511", manager.GetConfig().Storage.WorkspaceStorage.Geese.DirMode)
	require.Equal(t, "0000", manager.GetConfig().Storage.WorkspaceStorage.Geese.FileMode)

	before := manager.kf.Raw()
	for _, mode := range []string{"511.5", "-1", "4096"} {
		input := `{"debug_mode":true,"storage":{"workspace_storage":{"geese":{"dir_mode":` + mode + `}}}}`
		require.Error(t, manager.LoadConfig(JSONConfigFormat, rawbytes.Provider([]byte(input))))
		require.Equal(t, before, manager.kf.Raw(), "a rejected load must leave configuration unchanged")
	}
}

func TestDefaultWorkspaceGeeseHTTPTimeout(t *testing.T) {
	t.Setenv("CONFIG_PATH", "")
	t.Setenv(types.WorkerMinimalConfigEnv, "true")

	manager, err := NewConfigManager[types.AppConfig]()
	require.NoError(t, err)
	require.Equal(t, time.Minute, manager.GetConfig().Storage.WorkspaceStorage.Geese.HTTPTimeout)
}

// The embedded default config is what every gateway boots from; a malformed
// edit there (a duplicate key, for one) is fatal at startup.
func TestEmbeddedDefaultConfigLoads(t *testing.T) {
	t.Setenv("CONFIG_PATH", "")
	t.Setenv(types.WorkerMinimalConfigEnv, "")

	manager, err := NewConfigManager[types.AppConfig]()
	require.NoError(t, err)
	require.Equal(t, 0, manager.GetConfig().Database.Postgres.MaxOpenConns, "the pool stays unbounded unless configured")
	require.NoError(t, manager.GetConfig().Validate())
}

// The example in the doc comment of types.WorkerPoolProvisioningConfig is
// what gets copied into a config file, so it has to load as it is written.
func TestSpotPoolExampleInTheDocsLoads(t *testing.T) {
	const first, declaration = "//\tworker:\n", "type WorkerPoolProvisioningConfig struct"
	source, err := os.ReadFile("../types/config.go")
	require.NoError(t, err)
	_, rest, found := strings.Cut(string(source), first)
	require.True(t, found, "no example in pkg/types/config.go")
	body, _, found := strings.Cut(rest, declaration)
	require.True(t, found, "the example is not in the doc comment of WorkerPoolProvisioningConfig")
	overlay := strings.ReplaceAll(first+body, "//\t", "")

	manager, err := NewConfigManager[types.AppConfig]()
	require.NoError(t, err)
	require.NoError(t, manager.load(YAMLConfigFormat, rawbytes.Provider([]byte(overlay)), "example"), overlay)
	require.Empty(t, manager.UnknownKeys(), overlay)

	config := manager.GetConfig()
	require.NoError(t, config.Validate())
	pool := config.Worker.Pools["spot-l40s"]
	require.True(t, pool.Buys())
	require.Equal(t, []string{"verda"}, pool.Provisioning.Providers)
	require.True(t, pool.Provisioning.Spot)
	require.Equal(t, 4, pool.Provisioning.MaxNodes)
	require.Equal(t, 4, pool.Provisioning.MaxGPUs)
	require.EqualValues(t, 150, pool.Provisioning.MaxNodeHourlyCents)
	require.Equal(t, []string{"FIN-01"}, pool.Provisioning.Regions)
	require.Equal(t, 10*time.Minute, pool.Provisioning.IdleAfter())
	require.Equal(t, 20*time.Minute, pool.Provisioning.BootWithin())
	require.Equal(t, []string{"A10G"}, pool.AlsoServes)
	require.Equal(t, "1", pool.PoolSizing.MinFreeGPU)
	require.EqualValues(t, 2000, config.Providers.MaxHourlyCents)
	require.EqualValues(t, 20000, config.Providers.MaxDailyCents)
	require.Len(t, config.Providers.Nebius.Projects, 1)
}

func TestCompletionBudgetsAreLoadedForYAMLAndJSONConfig(t *testing.T) {
	for _, overlay := range []string{"", `{"gateway_service":{"host":"test"}}`} {
		t.Run(overlay, func(t *testing.T) {
			t.Setenv("CONFIG_PATH", "")
			t.Setenv("CONFIG_JSON", overlay)
			t.Setenv(types.WorkerMinimalConfigEnv, "")
			manager, err := NewConfigManager[types.AppConfig]()
			require.NoError(t, err)
			policy := manager.GetConfig().GatewayService.TaskCompletion
			require.Positive(t, policy.Timeout)
			require.Positive(t, policy.ResultUploadTimeout)
			require.Less(t, policy.ResultUploadTimeout, policy.Timeout)
		})
	}
}

func TestMinimalConfigEnabled(t *testing.T) {
	for _, value := range []string{"1", "true", "yes", "on", " TRUE "} {
		t.Setenv(types.WorkerMinimalConfigEnv, value)
		require.True(t, minimalConfigEnabled())
	}

	t.Setenv(types.WorkerMinimalConfigEnv, "false")
	require.False(t, minimalConfigEnabled())
}

func TestUnknownConfigKeysAreReportedWithTheirSource(t *testing.T) {
	manager, err := NewConfigManager[types.AppConfig]()
	require.NoError(t, err)
	require.Empty(t, manager.UnknownKeys(), "the built-in config sets keys that nothing reads")

	require.NoError(t, manager.load(YAMLConfigFormat, rawbytes.Provider([]byte(`
debugMode: true
wroker:
  namespace: beta9
worker:
  failover:
    enabled: true
  pools:
    a10g:
      gpuTyp: A10G
      GPUTYPE: A10G
      poolSizing:
        minFreeCpu: 1000m
      jobSpec:
        nodeSelector:
          any-label: is-fine
        env:
          - name: A
            value: b
            valu: c
`)), "/etc/beta9.d/extra.yaml"))

	require.Equal(t, []UnknownConfigKey{
		{Source: "/etc/beta9.d/extra.yaml", Key: "worker.failover"},
		{Source: "/etc/beta9.d/extra.yaml", Key: "worker.pools[a10g].gpuTyp"},
		{Source: "/etc/beta9.d/extra.yaml", Key: "worker.pools[a10g].jobSpec.env[0].valu"},
		{Source: "/etc/beta9.d/extra.yaml", Key: "wroker"},
	}, manager.UnknownKeys())

	// The misspelt key changed nothing; the one spelt in another case did.
	require.Equal(t, "A10G", manager.GetConfig().Worker.Pools["a10g"].GPUType)
}

func TestUnknownConfigKeysInConfigJSONUseJSONNames(t *testing.T) {
	t.Setenv("CONFIG_JSON", `{"debug_mode": true, "debugMode": true, "worker": {"namespace": "beta9", "name_space": "x"}}`)

	manager, err := NewConfigManager[types.AppConfig]()
	require.NoError(t, err)

	require.Equal(t, []UnknownConfigKey{
		{Source: "CONFIG_JSON", Key: "debugMode"},
		{Source: "CONFIG_JSON", Key: "worker.name_space"},
	}, manager.UnknownKeys())
}

// A pool's durations are text in the config file, and in CONFIG_JSON either
// text or the number of nanoseconds a marshalled config carries.
func TestPoolDurationsLoadFromYAMLAndJSON(t *testing.T) {
	want := types.WorkerPoolProvisioningConfig{Providers: []string{"verda"}, MaxNodes: 2, MaxGPUs: 8,
		IdleTimeout: types.Duration(5 * time.Minute), BootTimeout: types.Duration(15 * time.Minute)}

	t.Run("yaml", func(t *testing.T) {
		manager, err := NewConfigManager[types.AppConfig]()
		require.NoError(t, err)
		require.NoError(t, manager.load(YAMLConfigFormat, rawbytes.Provider([]byte(`
worker:
  pools:
    spot:
      provisioning:
        providers: [verda]
        maxNodes: 2
        maxGPUs: 8
        idleTimeout: 5m
        bootTimeout: 15m
    patient:
      stalledAfter: 90s
`)), "config.yaml"))
		require.Empty(t, manager.UnknownKeys())
		pools := manager.GetConfig().Worker.Pools
		require.Equal(t, want, pools["spot"].Provisioning)
		require.Equal(t, types.Duration(90*time.Second), pools["patient"].StalledAfter)
	})

	for name, overlay := range map[string]string{
		"json text":        `{"worker":{"pools":{"spot":{"provisioning":{"providers":["verda"],"max_nodes":2,"max_gpus":8,"idle_timeout":"5m","boot_timeout":"15m0s"}}}}}`,
		"json nanoseconds": `{"worker":{"pools":{"spot":{"provisioning":{"providers":["verda"],"max_nodes":2,"max_gpus":8,"idle_timeout":300000000000,"boot_timeout":900000000000}}}}}`,
	} {
		t.Run(name, func(t *testing.T) {
			t.Setenv("CONFIG_JSON", overlay)
			manager, err := NewConfigManager[types.AppConfig]()
			require.NoError(t, err)
			require.Empty(t, manager.UnknownKeys())
			require.Equal(t, want, manager.GetConfig().Worker.Pools["spot"].Provisioning)
		})
	}
}

func TestRemovedConfigKeysSayWhatReplacedThem(t *testing.T) {
	manager, err := NewConfigManager[types.AppConfig]()
	require.NoError(t, err)
	require.NoError(t, manager.load(YAMLConfigFormat, rawbytes.Provider([]byte(`
scheduling:
  failover:
    enabled: true
    health:
      maxPendingWorkers: 5
    onDemand:
      budget:
        maxHourlyCents: 100
    chains:
      A10G:
        pools: [l40s]
        onDemand:
          gpus: [A10G]
          maxNodes: 2
`)), "config.yaml"))

	hints := map[string]string{}
	for _, unknown := range manager.UnknownKeys() {
		hints[unknown.Key] = removedKeyHint(unknown.Key)
	}
	require.Len(t, hints, 3)
	require.Contains(t, hints["scheduling.failover.health"], "were removed")
	require.Contains(t, hints["scheduling.failover.onDemand"], "worker.pools.<name>.provisioning")
	require.Contains(t, hints["scheduling.failover.chains[A10G].onDemand"], "worker.pools.<name>.provisioning")

	require.Empty(t, removedKeyHint("worker.pools[a10g].gpuTyp"), "a key no release read gets the spelling advice")
	// The chain itself still converts.
	require.Equal(t, []string{"l40s"}, manager.GetConfig().Scheduling.Failover.Chains["A10G"].Pools)
}
