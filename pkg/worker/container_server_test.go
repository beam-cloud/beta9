package worker

import (
	"context"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestWaitForSandboxProcessManagerDoesNotProceedBeforeReadySignal(t *testing.T) {
	containerId := "sandbox-test"
	ready := make(chan struct{})
	instance := &ContainerInstance{
		Id:                      containerId,
		ProcessManagerReadyChan: ready,
	}

	server := &ContainerRuntimeServer{
		containerInstances: common.NewSafeMap[*ContainerInstance](),
	}
	server.containerInstances.Set(containerId, instance)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		_, err := server.waitForSandboxProcessManager(ctx, containerId, instance)
		done <- err
	}()

	select {
	case err := <-done:
		t.Fatalf("waitForSandboxProcessManager returned before readiness signal: %v", err)
	case <-time.After(2100 * time.Millisecond):
	}

	cancel()
	require.ErrorContains(t, <-done, "Request cancelled")
}

func TestWaitForSandboxProcessManagerRefreshesAfterReadySignal(t *testing.T) {
	containerId := "sandbox-test"
	ready := make(chan struct{})
	instance := &ContainerInstance{
		Id:                      containerId,
		ProcessManagerReadyChan: ready,
	}

	server := &ContainerRuntimeServer{
		containerInstances: common.NewSafeMap[*ContainerInstance](),
	}
	server.containerInstances.Set(containerId, instance)

	fresh := *instance
	fresh.SandboxProcessManagerReady = true
	server.containerInstances.Set(containerId, &fresh)
	close(ready)

	got, err := server.waitForSandboxProcessManager(context.Background(), containerId, instance)
	require.NoError(t, err)
	require.True(t, got.SandboxProcessManagerReady)
}

func TestWritableContainerAddressMapHandlesNilMap(t *testing.T) {
	addressMap := writableContainerAddressMap(nil)
	addressMap[1234] = "127.0.0.1:1234"

	require.Equal(t, "127.0.0.1:1234", addressMap[1234])
}

func TestRecordSandboxExposedPortOnlyAppendsMissingPort(t *testing.T) {
	containerId := "sandbox-test"
	instances := common.NewSafeMap[*ContainerInstance]()
	instance := &ContainerInstance{
		Id: containerId,
		Request: &types.ContainerRequest{
			Ports: []uint32{8000},
		},
	}

	recordSandboxExposedPort(instances, containerId, instance, 8000)
	recordSandboxExposedPort(instances, containerId, instance, 9000)
	recordSandboxExposedPort(instances, containerId, instance, 9000)

	got, exists := instances.Get(containerId)
	require.True(t, exists)
	require.Equal(t, []uint32{8000, 9000}, got.Request.Ports)
}
