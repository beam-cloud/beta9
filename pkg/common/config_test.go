package common

import (
	"encoding/json"
	"os"
	"path/filepath"
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

func TestConfigJSONOverlayPreservesFileConfig(t *testing.T) {
	const secret = "file-only-introspection-sentinel"
	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte(`gateway:
  host: config-file-host
  mcpOAuth:
    issuer: https://issuer.test
    resource: https://gateway.test/mcp
    introspectionURL: https://issuer.test/introspect
    introspectionSecret: `+secret+`
managedCompute:
  billing:
    mode: http
    endpoint: https://billing.test
    authToken: file-billing-token
    failureGracePeriod: 10m
database:
  redis:
    addrs: [redis-one:6379, redis-two:6379]
events:
  callbacks:
    - url: https://callback.test
      eventTypes: [compute.pool]
      headers:
        file_tag: retained
`), 0600))
	t.Setenv("CONFIG_PATH", path)
	t.Setenv(types.WorkerMinimalConfigEnv, "")
	t.Setenv("CONFIG_JSON", "")
	base, err := NewConfigManager[types.AppConfig]()
	require.NoError(t, err)
	expected := base.GetConfig()
	require.Equal(t, secret, expected.GatewayService.MCPOAuth.IntrospectionSecret)
	require.Equal(t, 10*time.Minute, expected.ManagedCompute.Billing.FailureGracePeriod)
	require.Equal(t, []string{"redis-one:6379", "redis-two:6379"}, expected.Database.Redis.Addrs)
	require.Equal(t, "retained", expected.Events.Callbacks[0].Headers["file_tag"])
	expected.ManagedCompute.Billing.FailureGracePeriod = 30 * time.Minute

	for _, overlay := range []string{
		`{"managed_compute":{"billing":{"failure_grace_period":1800000000000}}}`,
		`{"managed_compute":{"billing":{"failure_grace_period":"30m"}},"gateway_service":{"mcp_oauth":{"introspection_secret":"ignored","introspectionSecret":"ignored","IntrospectionSecret":"ignored","-":"ignored"}},"gateway":{"mcpOAuth":{"introspectionSecret":"ignored"}}}`,
	} {
		t.Run(overlay, func(t *testing.T) {
			t.Setenv("CONFIG_JSON", overlay)
			manager, err := NewConfigManager[types.AppConfig]()
			require.NoError(t, err)
			got := manager.GetConfig()
			// Compare the actual struct: JSON equality would omit the secret
			// from both sides and miss the original regression.
			require.Equal(t, expected, got)
			encoded, err := json.Marshal(got)
			require.NoError(t, err)
			require.NotContains(t, string(encoded), secret)
			require.NotContains(t, manager.Print(), secret)
		})
	}
}

func TestConfigJSONOverlayCollections(t *testing.T) {
	type entry struct {
		Value  string        `key:"value" json:"value,omitempty"`
		Delay  time.Duration `key:"delayDuration" json:"delay_duration"`
		Hidden string        `key:"hidden" json:"-"`
	}
	type config struct {
		Wait    time.Duration    `key:"waitTime" json:"wait_time,omitempty"`
		Entries []entry          `key:"entries" json:"entries"`
		ByName  map[string]entry `key:"byName" json:"by_name"`
		Pointer *entry           `key:"pointer" json:"pointer"`
		Hidden  string           `key:"hidden" json:"-"`
	}
	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte(`waitTime: 1s
hidden: top-secret-sentinel
entries:
  - value: old-one
    delayDuration: 1s
  - value: old-two
byName:
  literal_key:
    value: retained
    hidden: map-secret-sentinel
pointer:
  value: pointer-value
  hidden: pointer-secret-sentinel
`), 0600))
	t.Setenv("CONFIG_PATH", path)
	t.Setenv(types.WorkerMinimalConfigEnv, "")
	t.Setenv("CONFIG_JSON", "")
	base, err := NewConfigManager[config]()
	require.NoError(t, err)
	expected := base.GetConfig()
	expected.Wait = 2 * time.Second
	expected.Entries = []entry{{Value: "new", Delay: 3 * time.Second}}
	expected.ByName["literal_key"] = entry{Value: "retained", Delay: 4 * time.Second, Hidden: "map-secret-sentinel"}
	expected.ByName["new_key"] = entry{Value: "added"}
	expected.Pointer.Delay = 5 * time.Second

	t.Setenv("CONFIG_JSON", `{"WAIT_TIME":2000000000,"entries":[{"value":"new","delay_duration":"3s"}],"by_name":{"literal_key":{"delay_duration":"4s","hidden":"ignored"},"new_key":{"value":"added"}},"pointer":{"delay_duration":"5s","hidden":"ignored"},"hidden":"ignored"}`)
	manager, err := NewConfigManager[config]()
	require.NoError(t, err)
	require.Equal(t, expected, manager.GetConfig())
	for _, secret := range []string{"top-secret-sentinel", "map-secret-sentinel", "pointer-secret-sentinel"} {
		require.NotContains(t, manager.Print(), secret)
	}

	// Exact JSON field names take precedence over case-insensitive matches.
	t.Setenv("CONFIG_JSON", `{"wait_time":"2s","WAIT_TIME":"9s"}`)
	manager, err = NewConfigManager[config]()
	require.NoError(t, err)
	exact := base.GetConfig()
	exact.Wait = 2 * time.Second
	require.Equal(t, exact, manager.GetConfig())

	// A slice is replaced rather than retaining its old tail. An empty map
	// retains its entries under the existing koanf merge rules.
	t.Setenv("CONFIG_JSON", `{"entries":[],"by_name":{}}`)
	manager, err = NewConfigManager[config]()
	require.NoError(t, err)
	empty := base.GetConfig()
	empty.Entries = []entry{}
	require.Equal(t, empty, manager.GetConfig())

	t.Setenv("CONFIG_JSON", `{"entries":null,"by_name":null,"pointer":null}`)
	manager, err = NewConfigManager[config]()
	require.NoError(t, err)
	cleared := base.GetConfig()
	cleared.Entries, cleared.ByName, cleared.Pointer = nil, nil, nil
	require.Equal(t, cleared, manager.GetConfig())
}

func TestConfigJSONRejectedOverlayLeavesFileConfigUnchanged(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte("gateway:\n  mcpOAuth:\n    introspectionSecret: preserved-sentinel\n"), 0600))
	t.Setenv("CONFIG_PATH", path)
	t.Setenv(types.WorkerMinimalConfigEnv, "")
	t.Setenv("CONFIG_JSON", "")
	base, err := NewConfigManager[types.AppConfig]()
	require.NoError(t, err)
	expected := base.GetConfig()
	for _, overlay := range []string{
		`{"gateway_service":`,
		`[]`,
		`{} trailing`,
		`{"gateway_service":{"host":"must-not-apply"},"storage":{"workspace_storage":{"geese":{"dir_mode":-1}}}}`,
	} {
		t.Run(overlay, func(t *testing.T) {
			t.Setenv("CONFIG_JSON", overlay)
			manager, err := NewConfigManager[types.AppConfig]()
			require.NoError(t, err) // Invalid CONFIG_JSON remains a logged, ignored overlay.
			require.Equal(t, expected, manager.GetConfig())
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
