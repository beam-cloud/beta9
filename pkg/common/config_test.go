package common

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestManagedEndpointsSecretConfigLoadsWithoutPrintingCredentials(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte(`managedEndpoints:
  repo:
    url: git@example.invalid:models.git
    branch: staging
    deployKey: private-deploy-key
  deployerSecrets:
    GITHUB_TOKEN: private-package-token
  webhook:
    secret: private-webhook-secret
`), 0o600))
	t.Setenv("CONFIG_PATH", path)
	t.Setenv(types.WorkerMinimalConfigEnv, "true")
	manager, err := NewConfigManager[types.AppConfig]()
	require.NoError(t, err)
	printed := manager.Print()
	for _, value := range []string{"private-deploy-key", "private-package-token", "private-webhook-secret"} {
		require.NotContains(t, printed, value)
	}
	config := manager.GetConfig().ManagedEndpoints
	require.Equal(t, "staging", config.Repo.Branch)
	require.Equal(t, "private-deploy-key", config.Repo.DeployKey)
	require.Equal(t, "private-package-token", config.DeployerSecrets["GITHUB_TOKEN"])
	require.Equal(t, "private-webhook-secret", config.Webhook.Secret)
	require.Contains(t, printed, "git@example.invalid:models.git")
}

func TestDefaultWorkspaceGeeseHTTPTimeout(t *testing.T) {
	t.Setenv("CONFIG_PATH", "")
	t.Setenv(types.WorkerMinimalConfigEnv, "true")

	manager, err := NewConfigManager[types.AppConfig]()
	require.NoError(t, err)
	require.Equal(t, time.Minute, manager.GetConfig().Storage.WorkspaceStorage.Geese.HTTPTimeout)
}

func TestMinimalConfigEnabled(t *testing.T) {
	for _, value := range []string{"1", "true", "yes", "on", " TRUE "} {
		t.Setenv(types.WorkerMinimalConfigEnv, value)
		require.True(t, minimalConfigEnabled())
	}

	t.Setenv(types.WorkerMinimalConfigEnv, "false")
	require.False(t, minimalConfigEnabled())
}
