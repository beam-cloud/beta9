package common

import (
	"encoding/json"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
	"os"
	"path/filepath"
	"testing"
)

func TestWorkerRuntimeConfigurationBoundary(t *testing.T) {
	config := types.AppConfig{}
	config.Database.Postgres.Password = "database-password"
	config.Database.Postgres.EncryptionKey = "database-key"
	config.Database.Redis.Password = "redis-password"
	config.Database.S2.ApiKey = "telemetry-admin"
	config.ManagedCompute.Billing.AuthToken = "billing-admin"
	config.Monitoring.OpenMeter.ApiKey = "collector-admin"
	config.Monitoring.ContainerCostHookConfig.Token = "quote-admin"
	config.ImageService.BuildRegistryCredentials.Credentials = map[string]string{"PASSWORD": "registry-admin"}
	config.Cache.Coordinator.Token = "cache-admin"
	config.Worker.Pools = map[string]types.WorkerPoolConfig{"selected": {ContainerRuntime: "gvisor"}, "other": {ContainerRuntime: "runc"}}
	safe := WorkerRuntimeConfig(config, "selected")
	raw, err := json.Marshal(safe)
	require.NoError(t, err)
	for _, secret := range []string{"database-password", "database-key", "redis-password", "telemetry-admin", "billing-admin", "collector-admin", "quote-admin", "registry-admin", "cache-admin"} {
		require.NotContains(t, string(raw), secret)
	}
	require.Len(t, safe.Worker.Pools, 1)
	require.Equal(t, "gvisor", safe.Worker.Pools["selected"].ContainerRuntime)
	path := filepath.Join(t.TempDir(), "config.json")
	require.NoError(t, os.WriteFile(path, raw, 0600))
	t.Setenv("CONFIG_PATH", path)
	t.Setenv("CONFIG_JSON", "")
	t.Setenv(types.WorkerMinimalConfigEnv, "true")
	manager, err := NewConfigManager[types.AppConfig]()
	require.NoError(t, err)
	loaded := manager.GetConfig()
	require.Empty(t, loaded.Database.Postgres.Password)
	require.Empty(t, loaded.Database.Postgres.EncryptionKey)
	require.Equal(t, "gvisor", loaded.Worker.Pools["selected"].ContainerRuntime)
}
