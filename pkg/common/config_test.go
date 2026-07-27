package common

import (
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

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
