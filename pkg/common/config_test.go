package common

import (
	"encoding/json"
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
	require.NoError(t, manager.LoadConfig(JSONConfigFormat, rawbytes.Provider([]byte(`{"storage":{"geese":{"dir_mode":511,"file_mode":438}}}`))))
	manager.tag = "json"
	require.Equal(t, "0777", manager.GetConfig().Storage.Geese.DirMode)
	require.Equal(t, "0666", manager.GetConfig().Storage.Geese.FileMode)

	require.NoError(t, manager.LoadConfig(JSONConfigFormat, rawbytes.Provider([]byte(`{"storage":{"geese":{"dir_mode":"511","file_mode":"0000"}}}`))))
	require.Equal(t, "511", manager.GetConfig().Storage.Geese.DirMode)
	require.Equal(t, "0000", manager.GetConfig().Storage.Geese.FileMode)
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
}

func TestMinimalConfigEnabled(t *testing.T) {
	for _, value := range []string{"1", "true", "yes", "on", " TRUE "} {
		t.Setenv(types.WorkerMinimalConfigEnv, value)
		require.True(t, minimalConfigEnabled())
	}

	t.Setenv(types.WorkerMinimalConfigEnv, "false")
	require.False(t, minimalConfigEnabled())
}
