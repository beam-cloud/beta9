package common

import (
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestMinimalConfigEnabled(t *testing.T) {
	for _, value := range []string{"1", "true", "yes", "on", " TRUE "} {
		t.Setenv(types.WorkerMinimalConfigEnv, value)
		require.True(t, minimalConfigEnabled())
	}

	t.Setenv(types.WorkerMinimalConfigEnv, "false")
	require.False(t, minimalConfigEnabled())
}
