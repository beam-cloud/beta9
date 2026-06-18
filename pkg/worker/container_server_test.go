package worker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	betaruntime "github.com/beam-cloud/beta9/pkg/runtime"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	goprocpb "github.com/beam-cloud/goproc/proto"
	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
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

func TestSandboxKilledProcessMarksPersistUntilExpiryOrClear(t *testing.T) {
	server := &ContainerRuntimeServer{}

	require.False(t, server.sandboxProcessMarkedExited("sandbox-test", 42))
	server.markSandboxProcessExited("sandbox-test", 42)
	require.True(t, server.sandboxProcessMarkedExited("sandbox-test", 42))

	server.clearSandboxProcessExited("sandbox-test", 42)
	require.False(t, server.sandboxProcessMarkedExited("sandbox-test", 42))

	server.killedSandboxProcesses.Store(sandboxProcessMarkKey("sandbox-test", 43), time.Now().Add(-11*time.Minute))
	require.False(t, server.sandboxProcessMarkedExited("sandbox-test", 43))
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

	server.containerInstances.Set(containerId, &ContainerInstance{
		Id:                         containerId,
		ProcessManagerReadyChan:    ready,
		SandboxProcessManagerReady: true,
	})
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
		fresh := &ContainerInstance{
			Id:                      containerId,
			ProcessManagerReadyChan: ready,
		}
		server.containerInstances.Set(containerId, fresh)

		time.Sleep(25 * time.Millisecond)
		fresh.SandboxProcessManagerReady = true
		server.containerInstances.Set(containerId, fresh)
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

func TestSandboxExecStreamPersistsChunkBeforeAck(t *testing.T) {
	repo := &fakeProcessLogRepo{}
	server := &ContainerRuntimeServer{eventRepo: repo, workerID: "worker-1"}
	instance := sandboxExecTestInstance("sandbox-test")
	stream := newFakeStreamExecStream(
		&goprocpb.StreamExecResponse{Message: &goprocpb.StreamExecResponse_Started{Started: &goprocpb.ExecProcessStarted{Pid: 123}}},
		&goprocpb.StreamExecResponse{Message: &goprocpb.StreamExecResponse_Chunk{Chunk: &goprocpb.ProcessLogChunk{
			Pid: 123, Stream: types.EventLogStreamStdout, Seq: 7, Data: []byte("hello\n"),
		}}},
	)

	pidCh, errCh := make(chan int, 1), make(chan error, 1)
	go server.drainSandboxExecStream(stream, func() {}, func() error { return nil }, instance.Id, instance, []string{"echo", "hello"}, "/workspace", pidCh, errCh)

	require.Equal(t, 123, <-pidCh)
	require.True(t, (<-stream.sent).GetAck().Ok)
	require.Len(t, repo.entries, 1)
	require.Equal(t, "hello\n", repo.entries[0].Line)
	require.Equal(t, "hello\n", readLogBuffer(t, instance.LogBuffer))
	require.Empty(t, errCh)
}

func TestSandboxExecStreamAcksEvenWhenEventWriteFails(t *testing.T) {
	server := &ContainerRuntimeServer{eventRepo: &fakeProcessLogRepo{err: errors.New("s2 unavailable")}}
	instance := sandboxExecTestInstance("sandbox-test")
	stream := newFakeStreamExecStream(
		&goprocpb.StreamExecResponse{Message: &goprocpb.StreamExecResponse_Started{Started: &goprocpb.ExecProcessStarted{Pid: 123}}},
		&goprocpb.StreamExecResponse{Message: &goprocpb.StreamExecResponse_Chunk{Chunk: &goprocpb.ProcessLogChunk{
			Pid: 123, Stream: types.EventLogStreamStderr, Seq: 9, Data: []byte("still buffered"),
		}}},
	)

	pidCh, errCh := make(chan int, 1), make(chan error, 1)
	go server.drainSandboxExecStream(stream, func() {}, func() error { return nil }, instance.Id, instance, nil, "", pidCh, errCh)

	require.Equal(t, 123, <-pidCh)
	require.True(t, (<-stream.sent).GetAck().Ok)
	require.Equal(t, "still buffered", readLogBuffer(t, instance.LogBuffer))
	require.Empty(t, errCh)
}

func TestSandboxExecStreamReportsExitBeforeStart(t *testing.T) {
	server := &ContainerRuntimeServer{}
	stream := newFakeStreamExecStream(&goprocpb.StreamExecResponse{
		Message: &goprocpb.StreamExecResponse_Exited{Exited: &goprocpb.ExecProcessExited{ErrorMsg: "boom"}},
	})

	pidCh, errCh := make(chan int, 1), make(chan error, 1)
	go server.drainSandboxExecStream(stream, func() {}, func() error { return nil }, "sandbox-test", nil, nil, "", pidCh, errCh)

	require.Empty(t, pidCh)
	require.ErrorContains(t, <-errCh, "boom")
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

func TestContainerSandboxExposePortRegistersBackendRoute(t *testing.T) {
	containerId := "sandbox-test"
	repoClient := &fakeContainerRepoClient{
		addressMap: map[int32]string{
			8080: "route://existing",
		},
	}
	networkController := &fakeContainerNetworkController{}
	instances := common.NewSafeMap[*ContainerInstance]()
	instances.Set(containerId, &ContainerInstance{
		Id:   containerId,
		Spec: &specs.Spec{},
		Request: &types.ContainerRequest{
			ContainerId: containerId,
			WorkspaceId: "workspace-1",
			Ports:       []uint32{8080},
		},
	})

	server := &ContainerRuntimeServer{
		containerInstances:      instances,
		containerRepoClient:     repoClient,
		containerNetworkManager: networkController,
		runtime:                 &stateCountingRuntime{},
		podAddr:                 "10.0.0.2",
		backendRoute: func(request *types.ContainerRequest, kind string, port int32, localTarget string) *pb.BackendRoute {
			return &pb.BackendRoute{
				RouteId:     "route-new",
				WorkspaceId: request.WorkspaceId,
				ContainerId: request.ContainerId,
				Kind:        kind,
				Port:        port,
				Transport:   types.BackendRouteTransportTSNet,
				LocalTarget: localTarget,
			}
		},
	}

	resp, err := server.ContainerSandboxExposePort(context.Background(), &pb.ContainerSandboxExposePortRequest{
		ContainerId: containerId,
		Port:        9000,
	})

	require.NoError(t, err)
	require.True(t, resp.Ok)
	require.Len(t, networkController.exposedPorts, 1)
	require.Equal(t, 9000, networkController.exposedPorts[0].containerPort)
	require.NotNil(t, repoClient.lastSetAddressMap)
	require.Equal(t, "route://existing", repoClient.lastSetAddressMap.AddressMap[8080])
	require.Contains(t, repoClient.lastSetAddressMap.AddressMap[9000], "10.0.0.2:")
	require.Len(t, repoClient.lastSetAddressMap.Routes, 1)
	require.Equal(t, "route-new", repoClient.lastSetAddressMap.Routes[0].RouteId)
	require.Equal(t, int32(9000), repoClient.lastSetAddressMap.Routes[0].Port)
	require.Equal(t, repoClient.lastSetAddressMap.AddressMap[9000], repoClient.lastSetAddressMap.Routes[0].LocalTarget)
}

func TestContainerSandboxExposePortUsesNetworkManagerAddressForBackendRoute(t *testing.T) {
	containerId := "sandbox-test"
	repoClient := &fakeContainerRepoClient{}
	networkController := &fakeContainerNetworkController{
		addresses: map[int]string{
			9000: "192.168.0.44:9000",
		},
	}
	instances := common.NewSafeMap[*ContainerInstance]()
	instances.Set(containerId, &ContainerInstance{
		Id:   containerId,
		Spec: &specs.Spec{},
		Request: &types.ContainerRequest{
			ContainerId: containerId,
			WorkspaceId: "workspace-1",
		},
	})

	server := &ContainerRuntimeServer{
		containerInstances:      instances,
		containerRepoClient:     repoClient,
		containerNetworkManager: networkController,
		runtime:                 &stateCountingRuntime{},
		podAddr:                 "127.0.0.1",
		backendRoute: func(request *types.ContainerRequest, kind string, port int32, localTarget string) *pb.BackendRoute {
			return &pb.BackendRoute{
				RouteId:     "route-backend",
				WorkspaceId: request.WorkspaceId,
				ContainerId: request.ContainerId,
				Kind:        kind,
				Port:        port,
				Transport:   types.BackendRouteTransportTSNet,
				LocalTarget: localTarget,
			}
		},
	}

	resp, err := server.ContainerSandboxExposePort(context.Background(), &pb.ContainerSandboxExposePortRequest{
		ContainerId: containerId,
		Port:        9000,
	})

	require.NoError(t, err)
	require.True(t, resp.Ok)
	require.Len(t, networkController.exposedPorts, 1)
	require.Equal(t, 9000, networkController.exposedPorts[0].containerPort)
	require.NotNil(t, repoClient.lastSetAddressMap)
	require.Equal(t, "192.168.0.44:9000", repoClient.lastSetAddressMap.AddressMap[9000])
	require.Len(t, repoClient.lastSetAddressMap.Routes, 1)
	require.Equal(t, "192.168.0.44:9000", repoClient.lastSetAddressMap.Routes[0].LocalTarget)
}

func TestContainerSandboxExposePortUpgradesExistingRawAddressToBackendRoute(t *testing.T) {
	containerId := "sandbox-test"
	repoClient := &fakeContainerRepoClient{
		addressMap: map[int32]string{
			9000: "10.0.0.2:32000",
		},
	}
	networkController := &fakeContainerNetworkController{}
	instances := common.NewSafeMap[*ContainerInstance]()
	instances.Set(containerId, &ContainerInstance{
		Id:   containerId,
		Spec: &specs.Spec{},
		Request: &types.ContainerRequest{
			ContainerId: containerId,
			WorkspaceId: "workspace-1",
		},
	})

	server := &ContainerRuntimeServer{
		containerInstances:      instances,
		containerRepoClient:     repoClient,
		containerNetworkManager: networkController,
		runtime:                 &stateCountingRuntime{},
		podAddr:                 "10.0.0.2",
		backendRoute: func(request *types.ContainerRequest, kind string, port int32, localTarget string) *pb.BackendRoute {
			return &pb.BackendRoute{
				RouteId:     "route-existing",
				WorkspaceId: request.WorkspaceId,
				ContainerId: request.ContainerId,
				Kind:        kind,
				Port:        port,
				Transport:   types.BackendRouteTransportTSNet,
				LocalTarget: localTarget,
			}
		},
	}

	resp, err := server.ContainerSandboxExposePort(context.Background(), &pb.ContainerSandboxExposePortRequest{
		ContainerId: containerId,
		Port:        9000,
	})

	require.NoError(t, err)
	require.True(t, resp.Ok)
	require.Empty(t, networkController.exposedPorts)
	require.NotNil(t, repoClient.lastSetAddressMap)
	require.Equal(t, "10.0.0.2:32000", repoClient.lastSetAddressMap.AddressMap[9000])
	require.Len(t, repoClient.lastSetAddressMap.Routes, 1)
	require.Equal(t, "route-existing", repoClient.lastSetAddressMap.Routes[0].RouteId)
	require.Equal(t, "10.0.0.2:32000", repoClient.lastSetAddressMap.Routes[0].LocalTarget)
}

type fakeProcessLogRepo struct {
	entries []types.EventContainerLogSchema
	err     error
}

func (r *fakeProcessLogRepo) PushContainerLogEventQueued(entry types.EventContainerLogSchema) error {
	if r.err != nil {
		return r.err
	}
	r.entries = append(r.entries, entry)
	return nil
}

type fakeStreamExecStream struct {
	grpc.ClientStream
	recv chan *goprocpb.StreamExecResponse
	sent chan *goprocpb.StreamExecRequest
}

func newFakeStreamExecStream(responses ...*goprocpb.StreamExecResponse) *fakeStreamExecStream {
	stream := &fakeStreamExecStream{
		recv: make(chan *goprocpb.StreamExecResponse, len(responses)),
		sent: make(chan *goprocpb.StreamExecRequest, len(responses)),
	}
	for _, response := range responses {
		stream.recv <- response
	}
	close(stream.recv)
	return stream
}

func (s *fakeStreamExecStream) Send(req *goprocpb.StreamExecRequest) error {
	s.sent <- req
	return nil
}

func (s *fakeStreamExecStream) Recv() (*goprocpb.StreamExecResponse, error) {
	resp, ok := <-s.recv
	if !ok {
		return nil, io.EOF
	}
	return resp, nil
}

func sandboxExecTestInstance(containerId string) *ContainerInstance {
	return &ContainerInstance{
		Id:        containerId,
		LogBuffer: common.NewLogBuffer(),
		Request: &types.ContainerRequest{
			ContainerId: containerId,
			StubId:      "stub-1",
			WorkspaceId: "workspace-1",
			AppId:       "app-1",
			Stub: types.StubWithRelated{
				Stub: types.Stub{Type: types.StubType(types.StubTypeSandbox)},
			},
		},
	}
}

func readLogBuffer(t *testing.T, lb *common.LogBuffer) string {
	t.Helper()

	buf := make([]byte, 1024)
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		n, err := lb.Read(buf)
		require.NoError(t, err)
		if n > 0 {
			return string(buf[:n])
		}
		time.Sleep(5 * time.Millisecond)
	}
	return ""
}

type stateCountingRuntime struct {
	mockRuntime
	stateCalls atomic.Int32
}

func (r *stateCountingRuntime) State(ctx context.Context, containerID string) (betaruntime.State, error) {
	r.stateCalls.Add(1)
	return betaruntime.State{ID: containerID, Pid: 1, Status: types.RuncContainerStatusRunning}, nil
}

type fakeContainerNetworkController struct {
	exposedPorts []fakeExposedPort
	addresses    map[int]string
}

type fakeExposedPort struct {
	containerID   string
	hostPort      int
	containerPort int
}

func (f *fakeContainerNetworkController) ExposePort(containerId string, hostPort, containerPort int) error {
	f.exposedPorts = append(f.exposedPorts, fakeExposedPort{
		containerID:   containerId,
		hostPort:      hostPort,
		containerPort: containerPort,
	})
	return nil
}

func (f *fakeContainerNetworkController) ExposePorts(containerId string, bindings []PortBinding) error {
	for _, binding := range bindings {
		if err := f.ExposePort(containerId, binding.HostPort, binding.ContainerPort); err != nil {
			return err
		}
	}
	return nil
}

func (f *fakeContainerNetworkController) Setup(containerId string, spec *specs.Spec, request *types.ContainerRequest) error {
	return nil
}

func (f *fakeContainerNetworkController) TearDown(containerId string) error {
	return nil
}

func (f *fakeContainerNetworkController) UpdateNetworkPermissions(containerId string, request *types.ContainerRequest) error {
	return nil
}

func (f *fakeContainerNetworkController) ContainerPortAddress(containerId string, binding PortBinding) (string, error) {
	if f.addresses != nil {
		if address, ok := f.addresses[binding.ContainerPort]; ok {
			return address, nil
		}
	}
	return fmt.Sprintf("10.0.0.2:%d", binding.HostPort), nil
}

func (f *fakeContainerNetworkController) ContainerPortAddressMap(containerId string, bindings []PortBinding) (map[int32]string, error) {
	addressMap := make(map[int32]string, len(bindings))
	for _, binding := range bindings {
		address, err := f.ContainerPortAddress(containerId, binding)
		if err != nil {
			return nil, err
		}
		addressMap[int32(binding.ContainerPort)] = address
	}
	return addressMap, nil
}

func (f *fakeContainerNetworkController) Close() error {
	return nil
}
