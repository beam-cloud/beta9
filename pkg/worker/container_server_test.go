package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	betaruntime "github.com/beam-cloud/beta9/pkg/runtime"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	clipCommon "github.com/beam-cloud/clip/pkg/common"
	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type logAttachmentStream struct {
	pb.ContainerService_ContainerStreamLogsServer
	attached bool
}

type execCapturingRuntime struct {
	mockRuntime
	processes []specs.Process
}

func (r *execCapturingRuntime) Exec(_ context.Context, _ string, process specs.Process, _ *betaruntime.ExecOpts) error {
	r.processes = append(r.processes, process)
	return nil
}

func (s *logAttachmentStream) SendHeader(metadata.MD) error {
	s.attached = true
	return nil
}

func (s *logAttachmentStream) Context() context.Context {
	return context.Background()
}

func TestContainerStreamLogsAcknowledgesAttachment(t *testing.T) {
	logBuffer := common.NewLogBuffer()
	logBuffer.Close()
	server := &ContainerRuntimeServer{containerInstances: common.NewSafeMap[*ContainerInstance]()}
	server.containerInstances.Set("container-id", &ContainerInstance{LogBuffer: logBuffer})
	stream := &logAttachmentStream{}

	require.NoError(t, server.ContainerStreamLogs(&pb.ContainerStreamLogsRequest{ContainerId: "container-id"}, stream))
	require.True(t, stream.attached)
}

func TestContainerExecDoesNotMutateBaseOrInstanceProcess(t *testing.T) {
	containerId := "container-exec-isolated"
	instanceEnv := []string{"INSTANCE=original"}
	capturingRuntime := &execCapturingRuntime{}
	server := &ContainerRuntimeServer{
		baseConfigSpec: specs.Spec{
			Process: &specs.Process{
				Args: []string{"base-command"},
				Cwd:  "/base",
				Env:  []string{"BASE=original"},
			},
		},
		containerInstances: common.NewSafeMap[*ContainerInstance](),
		runtime:            capturingRuntime,
	}
	server.containerInstances.Set(containerId, &ContainerInstance{
		Spec: &specs.Spec{
			Process: &specs.Process{
				Cwd: "/workspace",
				Env: instanceEnv,
			},
		},
		Request: &types.ContainerRequest{},
	})

	response, err := server.ContainerExec(context.Background(), &pb.ContainerExecRequest{
		ContainerId: containerId,
		Cmd:         "echo isolated",
		Env:         []string{"REQUEST=isolated"},
	})

	require.NoError(t, err)
	require.True(t, response.Ok)
	require.Len(t, capturingRuntime.processes, 1)
	require.Equal(t, []string{"base-command"}, server.baseConfigSpec.Process.Args)
	require.Equal(t, "/base", server.baseConfigSpec.Process.Cwd)
	require.Equal(t, []string{"BASE=original"}, server.baseConfigSpec.Process.Env)
	require.Equal(t, []string{"INSTANCE=original"}, instanceEnv)

	capturingRuntime.processes[0].Args[0] = "mutated-command"
	capturingRuntime.processes[0].Env[0] = "INSTANCE=mutated"
	require.Equal(t, []string{"base-command"}, server.baseConfigSpec.Process.Args)
	require.Equal(t, []string{"BASE=original"}, server.baseConfigSpec.Process.Env)
	require.Equal(t, []string{"INSTANCE=original"}, instanceEnv)
}

func TestContainerExecProcessesDoNotShareMutableState(t *testing.T) {
	containerId := "container-exec-repeated"
	instanceEnv := make([]string, 1, 4)
	instanceEnv[0] = "INSTANCE=shared-source"
	instanceProcess := &specs.Process{
		Cwd: "/first",
		Env: instanceEnv,
	}
	capturingRuntime := &execCapturingRuntime{}
	server := &ContainerRuntimeServer{
		baseConfigSpec: specs.Spec{
			Process: &specs.Process{
				Args: []string{"base-command"},
				Cwd:  "/base",
				Env:  []string{"BASE=original"},
			},
		},
		containerInstances: common.NewSafeMap[*ContainerInstance](),
		runtime:            capturingRuntime,
	}
	server.containerInstances.Set(containerId, &ContainerInstance{
		Spec:    &specs.Spec{Process: instanceProcess},
		Request: &types.ContainerRequest{},
	})

	first, err := server.ContainerExec(context.Background(), &pb.ContainerExecRequest{
		ContainerId: containerId,
		Cmd:         "echo first",
		Env:         []string{"REQUEST=first"},
	})
	require.NoError(t, err)
	require.True(t, first.Ok)

	instanceProcess.Cwd = "/second"
	second, err := server.ContainerExec(context.Background(), &pb.ContainerExecRequest{
		ContainerId: containerId,
		Cmd:         "echo second",
		Env:         []string{"REQUEST=second"},
	})
	require.NoError(t, err)
	require.True(t, second.Ok)

	require.Len(t, capturingRuntime.processes, 2)
	firstProcess := &capturingRuntime.processes[0]
	secondProcess := &capturingRuntime.processes[1]
	require.Equal(t, []string{"sh", "-c", "echo first"}, firstProcess.Args)
	require.Equal(t, "/first", firstProcess.Cwd)
	require.Equal(t, []string{"INSTANCE=shared-source", "REQUEST=first"}, firstProcess.Env)
	require.Equal(t, []string{"sh", "-c", "echo second"}, secondProcess.Args)
	require.Equal(t, "/second", secondProcess.Cwd)
	require.Equal(t, []string{"INSTANCE=shared-source", "REQUEST=second"}, secondProcess.Env)

	firstProcess.Args[0] = "mutated"
	firstProcess.Env[0] = "INSTANCE=mutated"
	require.Equal(t, []string{"sh", "-c", "echo second"}, secondProcess.Args)
	require.Equal(t, []string{"INSTANCE=shared-source", "REQUEST=second"}, secondProcess.Env)
	require.Equal(t, []string{"INSTANCE=shared-source"}, instanceEnv)
}

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

func TestProcessManagerDialFailureRetriesReadyDeadline(t *testing.T) {
	err := status.Error(codes.DeadlineExceeded, "context deadline exceeded while waiting for connections to become ready")
	require.True(t, isProcessManagerDialFailure(err))
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

func TestWriteInitialSpecFromImagePreservesBaseCwd(t *testing.T) {
	baseSpec := specs.Spec{Process: &specs.Process{Cwd: "/workspace", Args: []string{"sh"}}}
	server := &ContainerRuntimeServer{
		baseConfigSpec: baseSpec,
		imageClient:    &ImageClient{v2ArchiveMetadata: common.NewSafeMap[*clipCommon.ClipArchiveMetadata]()},
	}
	destPath := filepath.Join(t.TempDir(), "initial_config.json")

	err := server.writeInitialSpecFromImage(context.Background(), &ContainerInstance{
		Request: &types.ContainerRequest{ImageId: "image-without-clip-metadata"},
	}, destPath)
	require.NoError(t, err)

	spec := readSpecFile(t, destPath)
	require.Equal(t, "/workspace", spec.Process.Cwd)
	require.Equal(t, "/workspace", server.baseConfigSpec.Process.Cwd)
}

func TestWriteInitialSpecFromImageUsesClipWorkingDirWithoutMutatingBase(t *testing.T) {
	imageId := "image-with-workdir"
	imageClient := &ImageClient{v2ArchiveMetadata: common.NewSafeMap[*clipCommon.ClipArchiveMetadata]()}
	imageClient.v2ArchiveMetadata.Set(imageId, &clipCommon.ClipArchiveMetadata{
		StorageInfo: &clipCommon.OCIStorageInfo{
			ImageMetadata: &clipCommon.ImageMetadata{
				WorkingDir: "/app",
				Cmd:        []string{"python", "app.py"},
			},
		},
	})
	server := &ContainerRuntimeServer{
		baseConfigSpec: specs.Spec{Process: &specs.Process{Cwd: "/workspace", Args: []string{"sh"}}},
		imageClient:    imageClient,
	}
	destPath := filepath.Join(t.TempDir(), "initial_config.json")

	err := server.writeInitialSpecFromImage(context.Background(), &ContainerInstance{
		Request: &types.ContainerRequest{ImageId: imageId},
	}, destPath)
	require.NoError(t, err)

	spec := readSpecFile(t, destPath)
	require.Equal(t, "/app", spec.Process.Cwd)
	require.Equal(t, []string{"python", "app.py"}, spec.Process.Args)
	require.Equal(t, "/workspace", server.baseConfigSpec.Process.Cwd)
}

func TestWriteInitialSpecFromImageCombinesEntrypointAndCmd(t *testing.T) {
	imageId := "image-with-entrypoint-and-cmd"
	imageClient := &ImageClient{v2ArchiveMetadata: common.NewSafeMap[*clipCommon.ClipArchiveMetadata]()}
	imageClient.v2ArchiveMetadata.Set(imageId, &clipCommon.ClipArchiveMetadata{
		StorageInfo: &clipCommon.OCIStorageInfo{
			ImageMetadata: &clipCommon.ImageMetadata{
				Entrypoint: []string{"vllm", "serve"},
				Cmd:        []string{"--model", "Qwen/Qwen2.5-1.5B-Instruct"},
			},
		},
	})
	server := &ContainerRuntimeServer{
		baseConfigSpec: specs.Spec{Process: &specs.Process{Cwd: "/workspace", Args: []string{"sh"}}},
		imageClient:    imageClient,
	}
	destPath := filepath.Join(t.TempDir(), "initial_config.json")

	err := server.writeInitialSpecFromImage(context.Background(), &ContainerInstance{
		Request: &types.ContainerRequest{ImageId: imageId},
	}, destPath)
	require.NoError(t, err)

	spec := readSpecFile(t, destPath)
	require.Equal(t, []string{
		"vllm",
		"serve",
		"--model",
		"Qwen/Qwen2.5-1.5B-Instruct",
	}, spec.Process.Args)
}

func TestWriteInitialSpecFromImageDefaultsEmptyCwd(t *testing.T) {
	server := &ContainerRuntimeServer{
		baseConfigSpec: specs.Spec{Process: &specs.Process{Args: []string{"sh"}}},
		imageClient:    &ImageClient{v2ArchiveMetadata: common.NewSafeMap[*clipCommon.ClipArchiveMetadata]()},
	}
	destPath := filepath.Join(t.TempDir(), "initial_config.json")

	err := server.writeInitialSpecFromImage(context.Background(), &ContainerInstance{
		Request: &types.ContainerRequest{ImageId: "image-with-empty-cwd"},
	}, destPath)
	require.NoError(t, err)

	spec := readSpecFile(t, destPath)
	require.Equal(t, "/", spec.Process.Cwd)
	require.Empty(t, server.baseConfigSpec.Process.Cwd)
}

func readSpecFile(t *testing.T, path string) specs.Spec {
	t.Helper()

	data, err := os.ReadFile(path)
	require.NoError(t, err)

	var spec specs.Spec
	require.NoError(t, json.Unmarshal(data, &spec))
	require.NotNil(t, spec.Process)
	return spec
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
		fresh.signalProcessManagerReadiness(true)
		server.containerInstances.Set(containerId, fresh)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	got, err := server.waitForSandboxProcessManager(ctx, containerId, instance)
	require.NoError(t, err)
	require.True(t, got.processManagerReady())
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
	require.Equal(t, string(types.SandboxStatusPending), resp.Status)
	require.Equal(t, int32(-1), resp.ExitCode)
}

func TestContainerSandboxStatusReportsFailedProcessManagerInitialization(t *testing.T) {
	containerId := "sandbox-test"
	ready := make(chan struct{})
	close(ready)
	server := &ContainerRuntimeServer{
		containerInstances: common.NewSafeMap[*ContainerInstance](),
	}
	server.containerInstances.Set(containerId, &ContainerInstance{
		Id:                      containerId,
		ProcessManagerReadyChan: ready,
	})

	resp, err := server.ContainerSandboxStatus(context.Background(), &pb.ContainerSandboxStatusRequest{
		ContainerId: containerId,
	})

	require.NoError(t, err)
	require.False(t, resp.Ok)
	require.Contains(t, resp.ErrorMsg, "failed to become ready")
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

func TestWritableContainerAddressMapClonesInput(t *testing.T) {
	input := map[int32]string{1234: "127.0.0.1:1234"}
	addressMap := writableContainerAddressMap(input)
	addressMap[1234] = "127.0.0.1:5678"

	require.Equal(t, "127.0.0.1:1234", input[1234])
	require.Equal(t, "127.0.0.1:5678", addressMap[1234])
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

func (f *fakeContainerNetworkController) ReservePorts(_ string, count int) ([]int, error) {
	ports := make([]int, count)
	for i := range ports {
		ports[i] = 30000 + i
	}
	return ports, nil
}

func (f *fakeContainerNetworkController) ReleasePortReservations(string) {}

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
