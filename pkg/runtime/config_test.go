package runtime

import (
	"encoding/json"
	"testing"

	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/stretchr/testify/require"
)

func TestBaseRuncConfigProtectsHostDropCaches(t *testing.T) {
	var spec specs.Spec
	require.NoError(t, json.Unmarshal([]byte(GetBaseConfig("runc")), &spec))
	require.NotNil(t, spec.Linux)
	require.Contains(t, spec.Linux.ReadonlyPaths, "/proc/sys/vm/drop_caches")
}

func TestBaseRuntimeConfigsMountContainerCgroups(t *testing.T) {
	for _, runtimeName := range []string{"runc", "gvisor"} {
		t.Run(runtimeName, func(t *testing.T) {
			var spec specs.Spec
			require.NoError(t, json.Unmarshal([]byte(GetBaseConfig(runtimeName)), &spec))
			require.Contains(t, spec.Mounts, specs.Mount{
				Destination: "/sys/fs/cgroup",
				Type:        "cgroup",
				Source:      "cgroup",
				Options:     []string{"nosuid", "noexec", "nodev", "relatime"},
			})
		})
	}
}
