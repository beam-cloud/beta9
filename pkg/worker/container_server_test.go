package worker

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	betaruntime "github.com/beam-cloud/beta9/pkg/runtime"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/opencontainers/runtime-spec/specs-go"
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

func TestWaitForSandboxProcessManagerFailsAfterFailedReadySignal(t *testing.T) {
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
	close(ready)

	got, err := server.waitForSandboxProcessManager(context.Background(), containerId, instance)
	require.ErrorContains(t, err, "failed to become ready")
	require.False(t, got.SandboxProcessManagerReady)
}

func TestWaitForSandboxProcessManagerWaitsForLateReadyChannel(t *testing.T) {
	containerId := "sandbox-test"
	server := &ContainerRuntimeServer{
		containerInstances: common.NewSafeMap[*ContainerInstance](),
	}
	instance := &ContainerInstance{Id: containerId}
	server.containerInstances.Set(containerId, instance)

	ready := make(chan struct{})
	go func() {
		time.Sleep(25 * time.Millisecond)
		fresh := *instance
		fresh.ProcessManagerReadyChan = ready
		server.containerInstances.Set(containerId, &fresh)

		time.Sleep(25 * time.Millisecond)
		fresh.SandboxProcessManagerReady = true
		server.containerInstances.Set(containerId, &fresh)
		close(ready)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	got, err := server.waitForSandboxProcessManager(ctx, containerId, instance)
	require.NoError(t, err)
	require.True(t, got.SandboxProcessManagerReady)
}

func TestContainerSandboxExecDoesNotPollRuntimeStateBeforeProcessManagerReady(t *testing.T) {
	containerId := "sandbox-test"
	ready := make(chan struct{})
	close(ready)

	rt := &stateCountingRuntime{}
	server := &ContainerRuntimeServer{
		containerInstances: common.NewSafeMap[*ContainerInstance](),
		runtime:            rt,
	}
	server.containerInstances.Set(containerId, &ContainerInstance{
		Id:                      containerId,
		ProcessManagerReadyChan: ready,
		Spec:                    &specs.Spec{Process: &specs.Process{}},
	})

	resp, err := server.ContainerSandboxExec(context.Background(), &pb.ContainerSandboxExecRequest{
		ContainerId: containerId,
		Cmd:         "true",
	})

	require.NoError(t, err)
	require.False(t, resp.Ok)
	require.Contains(t, resp.ErrorMsg, "Process manager failed")
	require.Equal(t, int32(0), rt.stateCalls.Load())
}

func TestContainerSandboxStatusReportsPendingBeforeProcessManagerReady(t *testing.T) {
	containerId := "sandbox-test"
	server := &ContainerRuntimeServer{
		containerInstances: common.NewSafeMap[*ContainerInstance](),
	}
	server.containerInstances.Set(containerId, &ContainerInstance{Id: containerId})

	resp, err := server.ContainerSandboxStatus(context.Background(), &pb.ContainerSandboxStatusRequest{
		ContainerId: containerId,
	})

	require.NoError(t, err)
	require.True(t, resp.Ok)
	require.Equal(t, "pending", resp.Status)
	require.Equal(t, int32(-1), resp.ExitCode)
}

func TestContainerSandboxStatusRequiresProcessManagerForPid(t *testing.T) {
	containerId := "sandbox-test"
	server := &ContainerRuntimeServer{
		containerInstances: common.NewSafeMap[*ContainerInstance](),
	}
	server.containerInstances.Set(containerId, &ContainerInstance{Id: containerId})

	resp, err := server.ContainerSandboxStatus(context.Background(), &pb.ContainerSandboxStatusRequest{
		ContainerId: containerId,
		Pid:         1,
	})

	require.NoError(t, err)
	require.False(t, resp.Ok)
	require.Contains(t, resp.ErrorMsg, "process manager")
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

type stateCountingRuntime struct {
	mockRuntime
	stateCalls atomic.Int32
}

func (r *stateCountingRuntime) State(ctx context.Context, containerID string) (betaruntime.State, error) {
	r.stateCalls.Add(1)
	return betaruntime.State{ID: containerID, Pid: 1, Status: types.RuncContainerStatusRunning}, nil
}
