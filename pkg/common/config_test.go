package common

import (
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestDefaultWorkspaceGeeseReadPolicyIsBounded(t *testing.T) {
	t.Setenv("CONFIG_PATH", "")
	t.Setenv(types.WorkerMinimalConfigEnv, "true")

	manager, err := NewConfigManager[types.AppConfig]()
	require.NoError(t, err)
	config := manager.GetConfig()

	require.Equal(t, 60*time.Second, config.Storage.WorkspaceStorage.Geese.HTTPTimeout)
	require.Equal(t, 3, config.Storage.WorkspaceStorage.Geese.ReadRetryAttempts)
}

func TestMinimalConfigEnabled(t *testing.T) {
	for _, value := range []string{"1", "true", "yes", "on", " TRUE "} {
		t.Setenv(types.WorkerMinimalConfigEnv, value)
		require.True(t, minimalConfigEnabled())
	}

	t.Setenv(types.WorkerMinimalConfigEnv, "false")
	require.False(t, minimalConfigEnabled())
}
