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
