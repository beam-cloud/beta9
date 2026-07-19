package worker

import (
	"context"
	"log/slog"
	"sync/atomic"
	"testing"

	common "github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestSetupOOMWatcherSkipsGvisorWhenMemoryIsNotEnforced(t *testing.T) {
	worker := &Worker{
		runtime:            &mockRuntime{name: types.ContainerRuntimeGvisor.String()},
		containerInstances: common.NewSafeMap[*ContainerInstance](),
	}
	worker.containerInstances.Set("container-id", &ContainerInstance{})

	worker.setupOOMWatcher(
		context.Background(),
		"container-id",
		123,
		nil,
		&types.ContainerRequest{Memory: 128},
		slog.Default(),
		&atomic.Bool{},
	)

	instance, exists := worker.containerInstances.Get("container-id")
	require.True(t, exists)
	require.Nil(t, instance.OOMWatcher)
}
