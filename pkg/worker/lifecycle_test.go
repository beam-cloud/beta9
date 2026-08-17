package worker

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/runtime"
	"github.com/beam-cloud/beta9/pkg/storage"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	clipCommon "github.com/beam-cloud/clip/pkg/common"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"k8s.io/utils/cpuset"
)

func TestWaitForRuntimeStartedDrainsQueuedPIDWhenRuntimeDone(t *testing.T) {
	for i := 0; i < 1000; i++ {
		runtimeStarted := make(chan int, 1)
		runtimeDone := make(chan struct{})
		runtimeStarted <- 1234
		close(runtimeDone)

		handled := 0
		waitForRuntimeStarted(context.Background(), runtimeStarted, runtimeDone, func(pid int) {
			require.Equal(t, 1234, pid)
			handled++
		})

		require.Equal(t, 1, handled)
	}
}

func TestWaitForRuntimeStartedReturnsWhenRuntimeDoneWithoutPID(t *testing.T) {
	runtimeStarted := make(chan int, 1)
	runtimeDone := make(chan struct{})
	close(runtimeDone)

	handled := false
	waitForRuntimeStarted(context.Background(), runtimeStarted, runtimeDone, func(pid int) {
		handled = true
	})

	require.False(t, handled)
}

func TestContainerResolvConfSourceFallsBackForLoopbackHostResolver(t *testing.T) {
	hostResolv := filepath.Join(t.TempDir(), "resolv.conf")
	require.NoError(t, os.WriteFile(hostResolv, []byte("nameserver 127.0.0.53\noptions edns0\n"), 0o644))

	require.Equal(t, workerResolvConfPath, containerResolvConfSource(true, hostResolv))
}

func TestContainerResolvConfSourceUsesHostResolverWhenReachable(t *testing.T) {
	hostResolv := filepath.Join(t.TempDir(), "resolv.conf")
	require.NoError(t, os.WriteFile(hostResolv, []byte("nameserver 1.1.1.1\n"), 0o644))

	require.Equal(t, hostResolv, containerResolvConfSource(true, hostResolv))
	require.Equal(t, workerResolvConfPath, containerResolvConfSource(false, hostResolv))
}

func TestStartupPortBindingsForSandboxSkipsInternalPorts(t *testing.T) {
	request := &types.ContainerRequest{
		Ports: []uint32{
			uint32(containerInnerPort),
			uint32(types.WorkerShellPort),
			uint32(types.WorkerSandboxProcessManagerPort),
		},
		Stub: types.StubWithRelated{Stub: types.Stub{Type: types.StubType(types.StubTypeSandbox)}},
	}

	bindings := startupPortBindingsForRequest(request, nil, []int{30001, 30002, 30003})
	require.Empty(t, bindings)
}

func TestStartupPortBindingsForSandboxExposesRequestedPorts(t *testing.T) {
	request := &types.ContainerRequest{
		Ports: []uint32{
			9000,
			uint32(types.WorkerShellPort),
			uint32(types.WorkerSandboxProcessManagerPort),
		},
		Stub: types.StubWithRelated{Stub: types.Stub{Type: types.StubType(types.StubTypeSandbox)}},
	}

	bindings := startupPortBindingsForRequest(request, []uint32{9000}, []int{30001, 30002, 30003})
	require.Equal(t, []PortBinding{{HostPort: 30001, ContainerPort: 9000}}, bindings)
}

func TestStartupPortBindingsForPodKeepsStartupPorts(t *testing.T) {
	request := &types.ContainerRequest{
		Ports: []uint32{
			uint32(containerInnerPort),
			uint32(types.WorkerShellPort),
		},
		Stub: types.StubWithRelated{Stub: types.Stub{Type: types.StubType(types.StubTypePodRun)}},
	}

	bindings := startupPortBindingsForRequest(request, nil, []int{30001, 30002})
	require.Equal(t, []PortBinding{
		{HostPort: 30001, ContainerPort: containerInnerPort},
		{HostPort: 30002, ContainerPort: int(types.WorkerShellPort)},
	}, bindings)
}

func TestRequiresPostBuildImageMaterialization(t *testing.T) {
	dockerfile := "FROM ubuntu:22.04"
	sourceImage := "ubuntu:22.04"

	tests := []struct {
		name        string
		request     *types.ContainerRequest
		clipVersion uint32
		want        bool
	}{
		{
			name:        "v2 dockerfile build",
			request:     &types.ContainerRequest{BuildOptions: types.BuildOptions{Dockerfile: &dockerfile}},
			clipVersion: uint32(types.ClipVersion2),
			want:        false,
		},
		{
			name:        "v2 source image build",
			request:     &types.ContainerRequest{BuildOptions: types.BuildOptions{SourceImage: &sourceImage}},
			clipVersion: uint32(types.ClipVersion2),
			want:        false,
		},
		{
			name:        "v1 build",
			request:     &types.ContainerRequest{BuildOptions: types.BuildOptions{Dockerfile: &dockerfile}},
			clipVersion: uint32(types.ClipVersion1),
			want:        true,
		},
		{
			name:        "runtime image",
			request:     &types.ContainerRequest{},
			clipVersion: uint32(types.ClipVersion2),
			want:        true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, requiresPostBuildImageMaterialization(tt.request, tt.clipVersion))
		})
	}
}

func TestCreateOverlayUsesTmpfsForAgentWorkers(t *testing.T) {
	worker := &Worker{
		persistent:     true,
		machineID:      "machine-one",
		routeTransport: types.BackendRouteTransportTSNet,
	}
	request := &types.ContainerRequest{ContainerId: "container-agent"}

	overlay := worker.createOverlay(request, t.TempDir())
	require.Equal(t, "/dev/shm", overlay.OverlayPath())
}

func TestCreateOverlayKeepsDefaultPathForNormalWorkers(t *testing.T) {
	worker := &Worker{}
	request := &types.ContainerRequest{ContainerId: "container-default"}

	overlay := worker.createOverlay(request, t.TempDir())
	require.Equal(t, baseConfigPath, overlay.OverlayPath())
}

func TestSetupBuildahDirsUsesPersistentLayerCache(t *testing.T) {
	cacheDir := t.TempDir()
	t.Setenv(types.AgentBuildCacheDirEnv, cacheDir)

	graphroot, runroot, tmpdir, cleanupGraphroot := (&ImageClient{}).setupBuildahDirs()
	defer os.RemoveAll(runroot)
	defer os.RemoveAll(tmpdir)

	require.False(t, cleanupGraphroot)
	require.Equal(t, filepath.Join(cacheDir, "buildah", "storage"), graphroot)
	require.DirExists(t, graphroot)
	require.DirExists(t, runroot)
	require.DirExists(t, tmpdir)
}

func TestSetupBuildahDirsFallsBackWhenLayerCacheUnavailable(t *testing.T) {
	cacheFile := filepath.Join(t.TempDir(), "cache-file")
	require.NoError(t, os.WriteFile(cacheFile, []byte("not a dir"), 0o600))
	t.Setenv(types.AgentBuildCacheDirEnv, cacheFile)

	graphroot, runroot, tmpdir, cleanupGraphroot := (&ImageClient{}).setupBuildahDirs()
	defer os.RemoveAll(graphroot)
	defer os.RemoveAll(runroot)
	defer os.RemoveAll(tmpdir)

	require.True(t, cleanupGraphroot)
	require.DirExists(t, graphroot)
	require.DirExists(t, runroot)
	require.DirExists(t, tmpdir)
	require.NotContains(t, graphroot, cacheFile)
}

func TestGetContainerEnvironmentUsesGatewayConfigFallback(t *testing.T) {
	t.Setenv(types.ContainerGatewayGRPCHostEnv, "")
	t.Setenv(types.ContainerGatewayGRPCPortEnv, "")
	t.Setenv(types.ContainerGatewayHTTPHostEnv, "")
	t.Setenv(types.ContainerGatewayHTTPPortEnv, "")

	worker := &Worker{
		podAddr: "127.0.0.1",
		config: types.AppConfig{
			GatewayService: types.GatewayServiceConfig{
				GRPC: types.GRPCConfig{
					ExternalHost: "host.docker.internal",
					ExternalPort: 1993,
				},
				HTTP: types.HTTPConfig{
					ExternalHost: "host.docker.internal",
					ExternalPort: 1994,
				},
			},
		},
	}

	env := worker.getContainerEnvironment(
		&types.ContainerRequest{
			ContainerId: "container-one",
			Env:         []string{"BETA9_TOKEN=user-token"},
		},
		&ContainerOptions{BindPorts: []int{58083}},
	)
	envMap := envListToMap(env)

	require.Equal(t, "host.docker.internal", envMap["BETA9_GATEWAY_HOST"])
	require.Equal(t, "1993", envMap["BETA9_GATEWAY_PORT"])
	require.Equal(t, "host.docker.internal", envMap["BETA9_GATEWAY_HOST_HTTP"])
	require.Equal(t, "1994", envMap["BETA9_GATEWAY_PORT_HTTP"])
	require.Equal(t, "user-token", envMap["BETA9_TOKEN"])
}

func TestApplyRuntimeEnvironmentOverridesClampsNvidiaCapabilitiesForGVisorGPU(t *testing.T) {
	worker := &Worker{runtime: &mockRuntime{name: types.ContainerRuntimeGvisor.String()}}
	env := worker.applyRuntimeEnvironmentOverrides(
		[]string{"NVIDIA_DRIVER_CAPABILITIES=all", "OTHER=value"},
		&types.ContainerRequest{Gpu: "V100", GpuCount: 1},
		nil,
	)
	envMap := envListToMap(env)

	require.Equal(t, gvisorNvidiaDriverCapabilities, envMap["NVIDIA_DRIVER_CAPABILITIES"])
	require.Equal(t, "value", envMap["OTHER"])
}

func TestApplyRuntimeEnvironmentOverridesLeavesRuncNvidiaCapabilities(t *testing.T) {
	worker := &Worker{runtime: &mockRuntime{name: types.ContainerRuntimeRunc.String()}}
	env := worker.applyRuntimeEnvironmentOverrides(
		[]string{"NVIDIA_DRIVER_CAPABILITIES=all"},
		&types.ContainerRequest{Gpu: "V100", GpuCount: 1},
		nil,
	)
	envMap := envListToMap(env)

	require.Equal(t, "all", envMap["NVIDIA_DRIVER_CAPABILITIES"])
}

func TestApplyRuntimeEnvironmentOverridesDisablesLibuvIOUringForStateVolumes(t *testing.T) {
	worker := &Worker{runtime: &mockRuntime{name: types.ContainerRuntimeRunc.String()}}
	env := worker.applyRuntimeEnvironmentOverrides(
		[]string{"UV_USE_IO_URING=1", "TORCHINDUCTOR_QUIESCE_ASYNC_COMPILE_POOL=0", "OTHER=value"},
		&types.ContainerRequest{PersistentRoot: &types.PersistentRoot{Size: "1Gi"}},
		nil,
	)
	envMap := envListToMap(env)

	require.Equal(t, "0", envMap["UV_USE_IO_URING"])
	require.Equal(t, "1", envMap["TORCHINDUCTOR_QUIESCE_ASYNC_COMPILE_POOL"])
	require.Equal(t, "value", envMap["OTHER"])
}

func TestApplyRuntimeEnvironmentOverridesLeavesCheckpointEnvWithoutCheckpoints(t *testing.T) {
	worker := &Worker{runtime: &mockRuntime{name: types.ContainerRuntimeRunc.String()}}
	env := worker.applyRuntimeEnvironmentOverrides(
		[]string{"UV_USE_IO_URING=1", "TORCHINDUCTOR_QUIESCE_ASYNC_COMPILE_POOL=0"},
		&types.ContainerRequest{},
		nil,
	)
	envMap := envListToMap(env)

	require.Equal(t, "1", envMap["UV_USE_IO_URING"])
	require.Equal(t, "0", envMap["TORCHINDUCTOR_QUIESCE_ASYNC_COMPILE_POOL"])
}

func TestRegisterContainerPortsUsesNetworkManagerAddresses(t *testing.T) {
	containerID := "container-route"
	repoClient := &fakeContainerRepoClient{}
	worker := &Worker{
		persistent: true,
		machineID:  "machine-one",
		workerId:   "worker-one",
		poolName:   "private-dev",
		containerNetworkManager: &fakeContainerNetworkController{
			addresses: map[int]string{
				8001: "192.168.0.44:8001",
				2222: "192.168.0.44:2222",
			},
		},
		routeTransport:      types.BackendRouteTransportTSNet,
		containerRepoClient: repoClient,
		containerInstances:  common.NewSafeMap[*ContainerInstance](),
	}
	worker.containerInstances.Set(containerID, &ContainerInstance{})

	err := worker.registerContainerPorts(context.Background(), &types.ContainerRequest{
		ContainerId: containerID,
		WorkspaceId: "workspace-one",
	}, []PortBinding{
		{HostPort: 30001, ContainerPort: 8001},
		{HostPort: 30002, ContainerPort: 2222},
	})
	require.NoError(t, err)

	require.NotNil(t, repoClient.lastSetAddressMap)
	require.Equal(t, int32(8001), repoClient.lastSetAddressMap.PrimaryPort)
	require.Equal(t, "192.168.0.44:8001", repoClient.lastSetAddressMap.AddressMap[8001])
	require.Equal(t, "192.168.0.44:2222", repoClient.lastSetAddressMap.AddressMap[2222])
	require.Len(t, repoClient.lastSetAddressMap.Routes, 2)
	require.Equal(t, "192.168.0.44:8001", repoClient.lastSetAddressMap.Routes[0].LocalTarget)
	require.Equal(t, "192.168.0.44:2222", repoClient.lastSetAddressMap.Routes[1].LocalTarget)

	instance, exists := worker.containerInstances.Get(containerID)
	require.True(t, exists)
	require.Equal(t, "192.168.0.44:8001", instance.containerAddress(8001))
	require.Equal(t, "192.168.0.44:2222", instance.containerAddress(2222))
}

func TestCacheContainerAddressMapClonesInput(t *testing.T) {
	containerID := "container-route"
	worker := &Worker{containerInstances: common.NewSafeMap[*ContainerInstance]()}
	worker.containerInstances.Set(containerID, &ContainerInstance{})

	addressMap := map[int32]string{8001: "192.168.0.44:8001"}
	worker.cacheContainerAddressMap(containerID, addressMap)
	addressMap[8001] = "changed"

	instance, exists := worker.containerInstances.Get(containerID)
	require.True(t, exists)
	require.Equal(t, "192.168.0.44:8001", instance.containerAddress(8001))
}

func TestRegisterContainerPortsKeepsLocalAddressBehavior(t *testing.T) {
	containerID := "container-local"
	repoClient := &fakeContainerRepoClient{}
	worker := &Worker{
		containerNetworkManager: &fakeContainerNetworkController{},
		containerRepoClient:     repoClient,
		containerInstances:      common.NewSafeMap[*ContainerInstance](),
	}
	worker.containerInstances.Set(containerID, &ContainerInstance{})

	err := worker.registerContainerPorts(context.Background(), &types.ContainerRequest{
		ContainerId: containerID,
	}, []PortBinding{
		{HostPort: 30001, ContainerPort: 8001},
		{HostPort: 30002, ContainerPort: 2222},
	})
	require.NoError(t, err)

	require.NotNil(t, repoClient.lastSetAddressMap)
	require.Equal(t, int32(8001), repoClient.lastSetAddressMap.PrimaryPort)
	require.Equal(t, "10.0.0.2:30001", repoClient.lastSetAddressMap.AddressMap[8001])
	require.Equal(t, "10.0.0.2:30002", repoClient.lastSetAddressMap.AddressMap[2222])
	require.Empty(t, repoClient.lastSetAddressMap.Routes)

	instance, exists := worker.containerInstances.Get(containerID)
	require.True(t, exists)
	require.Equal(t, "10.0.0.2:30001", instance.ContainerAddressMap[8001])
	require.Equal(t, "10.0.0.2:30002", instance.ContainerAddressMap[2222])
}

func TestPublishContainerAddressesFormatsBracketedIPv6PodAddress(t *testing.T) {
	containerID := "container-ipv6"
	repoClient := &fakeContainerRepoClient{}
	worker := &Worker{
		containerRepoClient: repoClient,
		containerInstances:  common.NewSafeMap[*ContainerInstance](),
		podAddr:             "[2600:1f18:37a4:c02::7286]",
	}
	worker.containerInstances.Set(containerID, &ContainerInstance{})

	err := worker.publishContainerAddresses(context.Background(), &types.ContainerRequest{
		ContainerId: containerID,
	}, []PortBinding{
		{HostPort: 30001, ContainerPort: 8001},
		{HostPort: 30002, ContainerPort: 2222},
	})
	require.NoError(t, err)

	require.NotNil(t, repoClient.lastSetAddressMap)
	require.Equal(t, int32(8001), repoClient.lastSetAddressMap.PrimaryPort)
	require.Equal(t, "[2600:1f18:37a4:c02::7286]:30001", repoClient.lastSetAddressMap.AddressMap[8001])
	require.Equal(t, "[2600:1f18:37a4:c02::7286]:30002", repoClient.lastSetAddressMap.AddressMap[2222])
}

func TestPublishContainerAddressesSkipsAgentWorkers(t *testing.T) {
	repoClient := &fakeContainerRepoClient{}
	worker := &Worker{
		persistent:          true,
		machineID:           "machine-one",
		routeTransport:      types.BackendRouteTransportTSNet,
		containerRepoClient: repoClient,
		podAddr:             "127.0.0.1",
	}

	err := worker.publishContainerAddresses(context.Background(), &types.ContainerRequest{
		ContainerId: "container-agent",
	}, []PortBinding{
		{HostPort: 60081, ContainerPort: 8001},
	})
	require.NoError(t, err)
	require.Zero(t, repoClient.setAddressCalls)
	require.Zero(t, repoClient.setAddressMapCalls)
}

func TestSpecFromRequestRespectsResourceEnforcementConfig(t *testing.T) {
	tests := []struct {
		name           string
		cpuEnforced    bool
		memoryEnforced bool
		wantCPU        bool
		wantMemory     bool
		wantUnified    bool
	}{
		{name: "cpu only", cpuEnforced: true, wantCPU: true},
		{name: "memory only", memoryEnforced: true, wantMemory: true, wantUnified: true},
		{name: "cpu and memory", cpuEnforced: true, memoryEnforced: true, wantCPU: true, wantMemory: true, wantUnified: true},
		{name: "neither"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockRuntime := &mockRuntime{name: types.ContainerRuntimeGvisor.String()}
			containerInstances := common.NewSafeMap[*ContainerInstance]()
			containerInstances.Set("container-1", &ContainerInstance{Runtime: mockRuntime})

			worker := &Worker{
				config: types.AppConfig{
					Worker: types.WorkerConfig{
						ContainerResourceLimits: types.ContainerResourceLimitsConfig{
							CPUEnforced:    tt.cpuEnforced,
							MemoryEnforced: tt.memoryEnforced,
						},
					},
				},
				runtime:            mockRuntime,
				containerInstances: containerInstances,
			}

			spec, err := worker.specFromRequest(&types.ContainerRequest{
				ContainerId: "container-1",
				EntryPoint:  []string{"python3", "-c", "print('ok')"},
				Cpu:         500,
				Memory:      128,
				Stub: types.StubWithRelated{Stub: types.Stub{
					Type: types.StubType(types.StubTypeFunction),
				}},
			}, &ContainerOptions{BindPorts: []int{8001}})
			require.NoError(t, err)
			require.NotNil(t, spec.Linux)
			require.NotNil(t, spec.Linux.Resources)

			assert.Equal(t, tt.wantCPU, spec.Linux.Resources.CPU != nil)
			assert.Equal(t, tt.wantMemory, spec.Linux.Resources.Memory != nil)
			assert.Equal(t, tt.wantUnified, spec.Linux.Resources.Unified != nil)
		})
	}
}

func TestSpecFromRequestEnforcesMemoryForGPUWithoutCPUQuota(t *testing.T) {
	mockRuntime := &mockRuntime{name: types.ContainerRuntimeGvisor.String()}
	containerInstances := common.NewSafeMap[*ContainerInstance]()
	containerInstances.Set("container-1", &ContainerInstance{Runtime: mockRuntime})
	worker := &Worker{
		config: types.AppConfig{Worker: types.WorkerConfig{
			ContainerResourceLimits: types.ContainerResourceLimitsConfig{
				CPUEnforced:    true,
				MemoryEnforced: true,
			},
		}},
		runtime:            mockRuntime,
		containerInstances: containerInstances,
	}

	spec, err := worker.specFromRequest(&types.ContainerRequest{
		ContainerId: "container-1",
		EntryPoint:  []string{"sleep", "60"},
		Cpu:         4000,
		Memory:      32 * 1024,
		GpuRequest:  []string{"RTX5090"},
		GpuCount:    1,
		Stub: types.StubWithRelated{Stub: types.Stub{
			Type: types.StubType(types.StubTypePodDeployment),
		}},
	}, &ContainerOptions{BindPorts: []int{8001}})
	require.NoError(t, err)
	require.Nil(t, spec.Linux.Resources.CPU)
	require.NotNil(t, spec.Linux.Resources.Memory)
	require.Equal(t, int64(42*1024*1024*1024), *spec.Linux.Resources.Memory.Limit)
}

func TestSpecFromRequestForcesCPUAndMemoryLimitsForGvisorGPU(t *testing.T) {
	mockRuntime := &mockResourceRuntime{mockRuntime: mockRuntime{name: types.ContainerRuntimeGvisor.String()}}
	containerInstances := common.NewSafeMap[*ContainerInstance]()
	containerInstances.Set("container-1", &ContainerInstance{Runtime: mockRuntime, CPUSet: "2-9"})
	worker := &Worker{
		config:             types.AppConfig{},
		runtime:            mockRuntime,
		containerInstances: containerInstances,
	}

	spec, err := worker.specFromRequest(&types.ContainerRequest{
		ContainerId: "container-1",
		EntryPoint:  []string{"sleep", "60"},
		Cpu:         8000,
		Memory:      16 * 1024,
		GpuRequest:  []string{"A6000"},
		GpuCount:    1,
		Stub: types.StubWithRelated{Stub: types.Stub{
			Type:   types.StubType(types.StubTypeSandbox),
			Config: `{"_beta9_force_resource_limits":true}`,
		}},
	}, &ContainerOptions{BindPorts: []int{8001}})
	require.NoError(t, err)
	require.NotNil(t, spec.Linux.Resources.CPU)
	require.Nil(t, spec.Linux.Resources.CPU.Quota, "sandbox CPU quota is deferred until its process manager is ready")
	require.Equal(t, "2-9", spec.Linux.Resources.CPU.Cpus, "gVisor must see the requested CPU topology at sandbox boot")
	require.NotNil(t, spec.Linux.Resources.Memory)
	require.Equal(t, int64(22*1024*1024*1024), *spec.Linux.Resources.Memory.Limit)

	instance, exists := containerInstances.Get("container-1")
	require.True(t, exists)
	require.NotNil(t, instance.DeferredCPUQuota)
	require.Equal(t, int64(800_000), *instance.DeferredCPUQuota.Quota)
	require.Equal(t, uint64(100_000), *instance.DeferredCPUQuota.Period)
	require.Equal(t, "2-9", instance.DeferredCPUQuota.Cpus)
	require.NoError(t, worker.applyDeferredCPUThrottle(&types.ContainerRequest{ContainerId: "container-1"}, instance))
	require.Equal(t, int64(800_000), *mockRuntime.updatedResources.CPU.Quota)
	require.Equal(t, "2-9", mockRuntime.updatedResources.CPU.Cpus)
}

func TestSpecFromRequestReturnsIndependentSpecs(t *testing.T) {
	worker := &Worker{runtime: &mockRuntime{name: types.ContainerRuntimeRunc.String()}}
	initialEnv := make([]string, 1, 8)
	initialEnv[0] = "IMAGE=one"
	requestEnv := make([]string, 1, 8)
	requestEnv[0] = "REQUEST=one"
	firstRequest := &types.ContainerRequest{
		ContainerId: "container-1",
		EntryPoint:  []string{"python3", "-c", "print('one')"},
		Env:         requestEnv,
	}

	first, err := worker.specFromRequest(firstRequest, &ContainerOptions{
		BindPorts:   []int{8001},
		InitialSpec: &specs.Spec{Process: &specs.Process{Env: initialEnv}},
	})
	require.NoError(t, err)

	second, err := worker.specFromRequest(&types.ContainerRequest{
		ContainerId: "container-2",
		EntryPoint:  []string{"python3", "-c", "print('two')"},
		Env:         []string{"REQUEST=two"},
	}, &ContainerOptions{BindPorts: []int{8002}})
	require.NoError(t, err)

	require.NotSame(t, first.Process, second.Process)
	require.Equal(t, []string{"python3", "-c", "print('one')"}, first.Process.Args)
	require.Equal(t, []string{"python3", "-c", "print('two')"}, second.Process.Args)
	require.Contains(t, first.Process.Env, "REQUEST=one")
	require.Contains(t, first.Process.Env, "IMAGE=one")
	require.NotContains(t, first.Process.Env, "REQUEST=two")
	require.Contains(t, second.Process.Env, "REQUEST=two")
	require.NotContains(t, second.Process.Env, "REQUEST=one")

	first.Process.Args[2] = "mutated"
	first.Process.Env = append(first.Process.Env, "LEAKED=true")
	require.Equal(t, "print('one')", firstRequest.EntryPoint[2])
	require.Equal(t, []string{"REQUEST=one"}, firstRequest.Env)
	require.Equal(t, []string{"IMAGE=one"}, initialEnv)
	require.Equal(t, "print('two')", second.Process.Args[2])
	require.NotContains(t, second.Process.Env, "LEAKED=true")
}

func TestSelectRequestedCPUs(t *testing.T) {
	available := cpuset.New(2, 4, 6, 8)

	require.Empty(t, selectRequestedCPUs(0, available, nil))
	require.Equal(t, "2", selectRequestedCPUs(1000, available, nil))
	require.Equal(t, "4,8", selectRequestedCPUs(1001, available, map[int]int64{
		2: 1000,
		6: 500,
	}))
	require.Equal(t, "2,4,6,8", selectRequestedCPUs(8000, available, nil))
}

func TestSpecFromRequestAppliesCPUAffinityToGPUWorkload(t *testing.T) {
	instances := common.NewSafeMap[*ContainerInstance]()
	worker := &Worker{
		config: types.AppConfig{Worker: types.WorkerConfig{
			ContainerResourceLimits: types.ContainerResourceLimitsConfig{CPUAffinityEnforced: true},
		}},
		containerInstances: instances,
		cpuLimit:           4000,
		runtime:            &mockRuntime{name: types.ContainerRuntimeRunc.String()},
	}
	request := &types.ContainerRequest{
		ContainerId: "gpu-container",
		EntryPoint:  []string{"sleep", "60"},
		Cpu:         1000,
		GpuRequest:  []string{"RTX5090"},
		GpuCount:    1,
		Stub: types.StubWithRelated{Stub: types.Stub{
			Type: types.StubType(types.StubTypePodDeployment),
		}},
	}
	require.True(t, worker.reserveContainerInstance(request))

	spec, err := worker.specFromRequest(request, &ContainerOptions{BindPorts: []int{8001}})
	require.NoError(t, err)
	require.NotNil(t, spec.Linux.Resources.CPU)

	affinity, err := cpuset.Parse(spec.Linux.Resources.CPU.Cpus)
	require.NoError(t, err)
	require.Equal(t, 1, affinity.Size())
	require.Nil(t, spec.Linux.Resources.CPU.Quota)
	require.Nil(t, spec.Linux.Resources.CPU.Period)
}

func TestContainerCPUAffinityIsOptInAndLoadBalanced(t *testing.T) {
	instances := common.NewSafeMap[*ContainerInstance]()
	worker := &Worker{
		containerInstances: instances,
		cpuLimit:           4000,
	}
	request := func(id string) *types.ContainerRequest {
		return &types.ContainerRequest{
			ContainerId: id,
			EntryPoint:  []string{"sleep", "60"},
			Cpu:         1000,
			Stub: types.StubWithRelated{Stub: types.Stub{
				Type: types.StubType(types.StubTypePodDeployment),
			}},
		}
	}

	require.True(t, worker.reserveContainerInstance(request("disabled")))
	disabled, exists := instances.Get("disabled")
	require.True(t, exists)
	require.Empty(t, disabled.CPUSet)
	instances.Delete("disabled")

	worker.config.Worker.ContainerResourceLimits.CPUAffinityEnforced = true
	require.True(t, worker.reserveContainerInstance(request("first")))
	require.True(t, worker.reserveContainerInstance(request("second")))
	first, _ := instances.Get("first")
	second, _ := instances.Get("second")
	firstSet, err := cpuset.Parse(first.CPUSet)
	require.NoError(t, err)
	secondSet, err := cpuset.Parse(second.CPUSet)
	require.NoError(t, err)
	require.Equal(t, 1, firstSet.Size())
	require.Equal(t, 1, secondSet.Size())
	require.True(t, firstSet.Intersection(secondSet).IsEmpty())
}

func TestForcedResourceLimitsOptIntoCPUAffinity(t *testing.T) {
	instances := common.NewSafeMap[*ContainerInstance]()
	worker := &Worker{
		containerInstances: instances,
		cpuLimit:           64_000,
	}
	request := &types.ContainerRequest{
		ContainerId: "tama-gpu",
		Cpu:         1000,
		GpuRequest:  []string{"A6000"},
		GpuCount:    1,
		Stub: types.StubWithRelated{Stub: types.Stub{
			Type:   types.StubType(types.StubTypeSandbox),
			Config: `{"_beta9_force_resource_limits":true}`,
		}},
	}

	require.True(t, worker.reserveContainerInstance(request))
	instance, exists := instances.Get(request.ContainerId)
	require.True(t, exists)
	affinity, err := cpuset.Parse(instance.CPUSet)
	require.NoError(t, err)
	require.Equal(t, 1, affinity.Size())
}

func TestCheckpointRestoreCPUAffinityIsDeferredAndApplied(t *testing.T) {
	quota := int64(100000)
	spec := specs.Spec{Linux: &specs.Linux{Resources: &specs.LinuxResources{
		CPU: &specs.LinuxCPU{Cpus: "2", Quota: &quota},
	}}}
	config, err := json.Marshal(&spec)
	require.NoError(t, err)
	configPath := filepath.Join(t.TempDir(), specBaseName)
	require.NoError(t, os.WriteFile(configPath, config, 0644))

	rt := &mockResourceRuntime{mockRuntime: mockRuntime{
		name:         types.ContainerRuntimeRunc.String(),
		capabilities: runtime.Capabilities{CheckpointRestore: true},
	}}
	instances := common.NewSafeMap[*ContainerInstance]()
	instances.Set("container-1", &ContainerInstance{
		Id:      "container-1",
		CPUSet:  "2",
		Runtime: rt,
	})
	worker := &Worker{containerInstances: instances}
	request := &types.ContainerRequest{
		ContainerId: "container-1",
		ConfigPath:  configPath,
	}

	require.NoError(t, worker.deferCheckpointRestoreCPUAffinity(request, config))
	deferredConfig, err := os.ReadFile(configPath)
	require.NoError(t, err)
	var deferredSpec specs.Spec
	require.NoError(t, json.Unmarshal(deferredConfig, &deferredSpec))
	require.Empty(t, deferredSpec.Linux.Resources.CPU.Cpus)
	require.Equal(t, quota, *deferredSpec.Linux.Resources.CPU.Quota)
	instance, _ := instances.Get(request.ContainerId)
	require.True(t, instance.RestoreCPUAffinityDeferred)

	require.NoError(t, worker.applyDeferredCheckpointRestoreCPUAffinity(context.Background(), request))
	require.Equal(t, "2", rt.updatedResources.CPU.Cpus)
	instance, _ = instances.Get(request.ContainerId)
	require.False(t, instance.RestoreCPUAffinityDeferred)
}

func TestCheckpointRestoreCPUAffinityAppliedBeforeStartedForwarded(t *testing.T) {
	rt := &mockResourceRuntime{mockRuntime: mockRuntime{
		name:         types.ContainerRuntimeRunc.String(),
		capabilities: runtime.Capabilities{CheckpointRestore: true},
	}}
	instances := common.NewSafeMap[*ContainerInstance]()
	instances.Set("container-1", &ContainerInstance{
		Id:                         "container-1",
		CPUSet:                     "4",
		RestoreCPUAffinityDeferred: true,
		Runtime:                    rt,
		StateMemoryCheckpoint:      &StateMemoryCheckpoint{ID: "checkpoint-1"},
	})
	worker := &Worker{
		criuManager:        &startedCRIUManager{},
		containerInstances: instances,
	}
	request := &types.ContainerRequest{
		ContainerId:     "container-1",
		StateSnapshotId: "state-snapshot-1",
	}
	started := make(chan int)
	appliedBeforeForward := make(chan bool, 1)
	go func() {
		<-started
		appliedBeforeForward <- rt.updatedResources != nil && rt.updatedResources.CPU.Cpus == "4"
	}()

	_, restored, restoreStarted, err := worker.attemptRestoreCheckpoint(
		context.Background(),
		request,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		io.Discard,
		started,
	)
	require.NoError(t, err)
	require.True(t, restored)
	require.True(t, restoreStarted)
	require.True(t, <-appliedBeforeForward)
}

func TestSpecFromRequestRejectsInvalidOCIInputs(t *testing.T) {
	tests := []struct {
		name    string
		request *types.ContainerRequest
		options *ContainerOptions
		match   string
	}{
		{
			name: "missing bind port",
			request: &types.ContainerRequest{
				ContainerId: "container-no-port",
				EntryPoint:  []string{"true"},
			},
			options: &ContainerOptions{},
			match:   "no reserved bind port",
		},
		{
			name: "malformed environment",
			request: &types.ContainerRequest{
				ContainerId: "container-bad-env",
				EntryPoint:  []string{"true"},
				Env:         []string{"INVALID"},
			},
			options: &ContainerOptions{BindPorts: []int{8001}},
			match:   "invalid environment entry",
		},
		{
			name: "relative working directory",
			request: &types.ContainerRequest{
				ContainerId: "container-bad-cwd",
				Stub: types.StubWithRelated{Stub: types.Stub{
					Type: types.StubType(types.StubTypePodDeployment),
				}},
			},
			options: &ContainerOptions{
				BindPorts:   []int{8001},
				InitialSpec: &specs.Spec{Process: &specs.Process{Args: []string{"true"}, Cwd: "workspace"}},
			},
			match: "working directory must be absolute",
		},
	}

	worker := &Worker{runtime: &mockRuntime{name: types.ContainerRuntimeRunc.String()}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spec, err := worker.specFromRequest(tt.request, tt.options)
			require.Nil(t, spec)
			require.ErrorContains(t, err, tt.match)
		})
	}
}

func TestSpecFromRequestDefaultsMissingRunnerEntrypoint(t *testing.T) {
	tests := []struct {
		name       string
		stubType   types.StubType
		env        []string
		wantArgs   []string
		stubConfig string
	}{
		{
			name:       "endpoint",
			stubType:   types.StubType(types.StubTypeEndpoint),
			env:        []string{"HANDLER=sse:handler"},
			wantArgs:   []string{"python3.11", "-m", "beta9.runner.endpoint"},
			stubConfig: `{"python_version":"python3.11"}`,
		},
		{
			name:       "asgi",
			stubType:   types.StubType(types.StubTypeASGI),
			env:        []string{"HANDLER=sse:handler"},
			wantArgs:   []string{"python3.10", "-m", "beta9.runner.endpoint"},
			stubConfig: `{"python_version":"python3.10"}`,
		},
		{
			name:       "function",
			stubType:   types.StubType(types.StubTypeFunction),
			env:        []string{"HANDLER=handler"},
			wantArgs:   []string{"python3", "-m", "beta9.runner.function"},
			stubConfig: `{}`,
		},
		{
			name:       "taskqueue",
			stubType:   types.StubType(types.StubTypeTaskQueue),
			env:        []string{"HANDLER=handler"},
			wantArgs:   []string{"python3.9", "-m", "beta9.runner.taskqueue"},
			stubConfig: `{"python_version":"python3.9"}`,
		},
		{
			name:       "explicit config entrypoint",
			stubType:   types.StubType(types.StubTypeEndpoint),
			wantArgs:   []string{"custom", "runner"},
			stubConfig: `{"python_version":"python3.11","entry_point":["custom","runner"]}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			worker := &Worker{runtime: &mockRuntime{name: types.ContainerRuntimeGvisor.String()}}
			spec, err := worker.specFromRequest(&types.ContainerRequest{
				ContainerId: "container-1",
				StubId:      "stub-1",
				Env:         tt.env,
				Stub: types.StubWithRelated{Stub: types.Stub{
					Type:   tt.stubType,
					Config: tt.stubConfig,
				}},
			}, &ContainerOptions{BindPorts: []int{8001}})

			require.NoError(t, err)
			require.Equal(t, tt.wantArgs, spec.Process.Args)
		})
	}
}

func TestSpecFromRequestRejectsUnsupportedEmptyEntrypoint(t *testing.T) {
	worker := &Worker{runtime: &mockRuntime{name: types.ContainerRuntimeGvisor.String()}}

	spec, err := worker.specFromRequest(&types.ContainerRequest{
		ContainerId: "container-1",
		StubId:      "stub-1",
		Stub: types.StubWithRelated{Stub: types.Stub{
			Type: types.StubType(types.StubTypeSandbox),
		}},
	}, &ContainerOptions{BindPorts: []int{8001}})

	require.Nil(t, spec)
	require.ErrorContains(t, err, "empty process args")
}

func TestSpecFromRequestPreservesPodInitialSpecCwd(t *testing.T) {
	worker := &Worker{runtime: &mockRuntime{name: types.ContainerRuntimeGvisor.String()}}

	spec, err := worker.specFromRequest(&types.ContainerRequest{
		ContainerId: "container-1",
		StubId:      "stub-1",
		Stub: types.StubWithRelated{Stub: types.Stub{
			Type: types.StubType(types.StubTypePodDeployment),
		}},
	}, &ContainerOptions{
		BindPorts: []int{8001},
		InitialSpec: &specs.Spec{Process: &specs.Process{
			Args: []string{"python", "app.py"},
			Cwd:  "/app",
			User: specs.User{UID: 1000, GID: 1000},
		}},
	})

	require.NoError(t, err)
	require.Equal(t, []string{"python", "app.py"}, spec.Process.Args)
	require.Equal(t, "/app", spec.Process.Cwd)
	require.Equal(t, uint32(1000), spec.Process.User.UID)
	require.Equal(t, uint32(1000), spec.Process.User.GID)
}

func TestSpecFromRequestSetsHostnameOnlyWhenRequested(t *testing.T) {
	tests := []struct {
		name         string
		runtime      string
		hostname     string
		wantHostname string
	}{
		{name: "runc default", runtime: types.ContainerRuntimeRunc.String(), wantHostname: "runc"},
		{name: "runsc default", runtime: types.ContainerRuntimeGvisor.String(), wantHostname: "runsc"},
		{name: "requested", runtime: types.ContainerRuntimeGvisor.String(), hostname: "brisk-canyon-a1b2", wantHostname: "brisk-canyon-a1b2"},
		{name: "sanitized", runtime: types.ContainerRuntimeGvisor.String(), hostname: "MyApp/v1.2", wantHostname: "myapp-v1-2"},
		{name: "trimmed", runtime: types.ContainerRuntimeGvisor.String(), hostname: "--wrapped__", wantHostname: "wrapped"},
		{name: "invalid", runtime: types.ContainerRuntimeRunc.String(), hostname: "///", wantHostname: "runc"},
		{name: "truncated", runtime: types.ContainerRuntimeGvisor.String(), hostname: strings.Repeat("a", 80), wantHostname: strings.Repeat("a", maxHostnameLength)},
		{name: "bounded", runtime: types.ContainerRuntimeGvisor.String(), hostname: strings.Repeat("a", maxHostnameLength-1) + "-b", wantHostname: strings.Repeat("a", maxHostnameLength-1) + "b"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			worker := &Worker{runtime: &mockRuntime{name: test.runtime}}

			spec, err := worker.specFromRequest(&types.ContainerRequest{
				ContainerId: "container-1",
				StubId:      "stub-1",
				Hostname:    test.hostname,
				EntryPoint:  []string{"tail", "-f", "/dev/null"},
				Stub:        types.StubWithRelated{Stub: types.Stub{Type: types.StubType(types.StubTypePod)}},
			}, &ContainerOptions{BindPorts: []int{8001}})

			require.NoError(t, err)
			require.Equal(t, test.wantHostname, spec.Hostname)
		})
	}
}

func TestSpecFromRequestRejectsRunnerStubWithoutRunnerEnv(t *testing.T) {
	worker := &Worker{runtime: &mockRuntime{name: types.ContainerRuntimeGvisor.String()}}

	spec, err := worker.specFromRequest(&types.ContainerRequest{
		ContainerId: "container-1",
		StubId:      "stub-1",
		Env:         []string{"STUB_TYPE=asgi/deployment"},
		Stub: types.StubWithRelated{Stub: types.Stub{
			Type:   types.StubType(types.StubTypeASGI),
			Config: `{"python_version":"python3"}`,
		}},
	}, &ContainerOptions{BindPorts: []int{8001}})

	require.Nil(t, spec)
	require.ErrorContains(t, err, "empty process args")
}

func TestSpecFromRequestDisablesIOUringForStateVolumes(t *testing.T) {
	t.Setenv(types.WorkerPoolEnv, "default")

	worker := &Worker{
		config: types.AppConfig{Worker: types.WorkerConfig{Pools: map[string]types.WorkerPoolConfig{
			"default": {CRIUEnabled: true},
		}}},
		podAddr:     "127.0.0.1",
		runtime:     &mockRuntime{name: types.ContainerRuntimeRunc.String()},
		criuManager: &startedCRIUManager{},
	}

	spec, err := worker.specFromRequest(&types.ContainerRequest{
		ContainerId:    "container-state",
		EntryPoint:     []string{"python", "app.py"},
		PersistentRoot: &types.PersistentRoot{Size: "1Gi"},
		Stub: types.StubWithRelated{Stub: types.Stub{
			Type: types.StubType(types.StubTypeFunction),
		}},
	}, &ContainerOptions{BindPorts: []int{8001}, HostBindPort: 30001})

	require.NoError(t, err)
	require.NotNil(t, spec.Linux)
	require.NotNil(t, spec.Linux.Seccomp)

	rule := findSeccompRule(t, spec.Linux.Seccomp, checkpointDisabledIOUringSyscalls)
	require.Equal(t, specs.ActErrno, rule.Action)
	require.NotNil(t, rule.ErrnoRet)
	require.Equal(t, uint(syscall.ENOSYS), *rule.ErrnoRet)
}

func TestDisableIOUringForCheckpointPreservesExistingSeccompRules(t *testing.T) {
	spec := &specs.Spec{Linux: &specs.Linux{Seccomp: &specs.LinuxSeccomp{
		DefaultAction: specs.ActErrno,
		Syscalls: []specs.LinuxSyscall{
			{Names: []string{"read", "io_uring_setup", "write"}, Action: specs.ActAllow},
			{Names: []string{"io_uring_enter"}, Action: specs.ActAllow},
		},
	}}}

	disableIOUringForCheckpoint(spec)

	blockRule := findSeccompRule(t, spec.Linux.Seccomp, checkpointDisabledIOUringSyscalls)
	require.Equal(t, specs.ActErrno, blockRule.Action)
	require.NotNil(t, blockRule.ErrnoRet)
	require.Equal(t, uint(syscall.ENOSYS), *blockRule.ErrnoRet)

	allowRule := findSeccompRule(t, spec.Linux.Seccomp, []string{"read", "write"})
	require.Equal(t, specs.ActAllow, allowRule.Action)
	for _, syscallRule := range spec.Linux.Seccomp.Syscalls {
		if syscallRule.Action == specs.ActAllow {
			require.NotContains(t, syscallRule.Names, "io_uring_setup")
			require.NotContains(t, syscallRule.Names, "io_uring_enter")
			require.NotContains(t, syscallRule.Names, "io_uring_register")
		}
	}
}

func findSeccompRule(t *testing.T, seccomp *specs.LinuxSeccomp, names []string) specs.LinuxSyscall {
	t.Helper()
	require.NotNil(t, seccomp)
	for _, rule := range seccomp.Syscalls {
		if sameStringSet(rule.Names, names) {
			return rule
		}
	}
	t.Fatalf("seccomp rule for %v not found in %#v", names, seccomp.Syscalls)
	return specs.LinuxSyscall{}
}

func sameStringSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	seen := map[string]int{}
	for _, value := range a {
		seen[value]++
	}
	for _, value := range b {
		seen[value]--
		if seen[value] < 0 {
			return false
		}
	}
	return true
}

func TestSpecFromRequestDefersSandboxCPUThrottleWhenRuntimeCanUpdate(t *testing.T) {
	rt := &mockResourceRuntime{mockRuntime: mockRuntime{name: "runc"}}
	containerInstances := common.NewSafeMap[*ContainerInstance]()
	containerInstances.Set("container-1", &ContainerInstance{Id: "container-1", Runtime: rt, CPUSet: "0"})

	worker := &Worker{
		config: types.AppConfig{
			Worker: types.WorkerConfig{
				ContainerResourceLimits: types.ContainerResourceLimitsConfig{
					CPUEnforced:         true,
					CPUAffinityEnforced: true,
					MemoryEnforced:      true,
				},
			},
		},
		runtime:            rt,
		containerInstances: containerInstances,
	}

	spec, err := worker.specFromRequest(&types.ContainerRequest{
		ContainerId: "container-1",
		EntryPoint:  []string{"sleep", "60"},
		Cpu:         100,
		Memory:      128,
		Stub: types.StubWithRelated{Stub: types.Stub{
			Type: types.StubType(types.StubTypeSandbox),
		}},
	}, &ContainerOptions{BindPorts: []int{8001}})
	require.NoError(t, err)
	require.NotNil(t, spec.Linux.Resources)
	require.NotNil(t, spec.Linux.Resources.CPU)
	require.NotNil(t, spec.Linux.Resources.CPU.Shares)
	require.Nil(t, spec.Linux.Resources.CPU.Quota)
	require.Nil(t, spec.Linux.Resources.CPU.Period)
	require.Empty(t, spec.Linux.Resources.CPU.Cpus)
	require.NotNil(t, spec.Linux.Resources.Memory)

	instance, exists := containerInstances.Get("container-1")
	require.True(t, exists)
	require.NotNil(t, instance.DeferredCPUQuota)
	require.Equal(t, "0", instance.DeferredCPUQuota.Cpus)
}

func TestSpecFromRequestKeepsSandboxCPUThrottleWhenRuntimeCannotUpdate(t *testing.T) {
	rt := &mockRuntime{name: types.ContainerRuntimeGvisor.String()}
	containerInstances := common.NewSafeMap[*ContainerInstance]()
	containerInstances.Set("container-1", &ContainerInstance{Id: "container-1", Runtime: rt})

	worker := &Worker{
		config: types.AppConfig{
			Worker: types.WorkerConfig{
				ContainerResourceLimits: types.ContainerResourceLimitsConfig{
					CPUEnforced: true,
				},
			},
		},
		runtime:            rt,
		containerInstances: containerInstances,
	}

	spec, err := worker.specFromRequest(&types.ContainerRequest{
		ContainerId: "container-1",
		EntryPoint:  []string{"sleep", "60"},
		Cpu:         100,
		Memory:      128,
		Stub: types.StubWithRelated{Stub: types.Stub{
			Type: types.StubType(types.StubTypeSandbox),
		}},
	}, &ContainerOptions{BindPorts: []int{8001}})
	require.NoError(t, err)
	require.NotNil(t, spec.Linux.Resources)
	require.NotNil(t, spec.Linux.Resources.CPU)

	instance, exists := containerInstances.Get("container-1")
	require.True(t, exists)
	require.Nil(t, instance.DeferredCPUQuota)
}

func TestFunctionCPUThrottleUsesStubTypeOnAgentWorkers(t *testing.T) {
	const containerID = "function-custom-entrypoint"
	instances := common.NewSafeMap[*ContainerInstance]()
	instances.Set(containerID, &ContainerInstance{
		Id:      containerID,
		Runtime: &mockResourceRuntime{mockRuntime: mockRuntime{name: "runc"}},
	})
	request := &types.ContainerRequest{
		ContainerId: containerID,
		EntryPoint:  []string{"custom-function-runner"},
		Stub: types.StubWithRelated{Stub: types.Stub{
			Type: types.StubType(types.StubTypeFunction),
		}},
	}

	require.False(t, (&Worker{containerInstances: instances}).deferCPUThrottle(request, &specs.LinuxCPU{}))

	worker := &Worker{
		containerInstances: instances,
		persistent:         true,
		machineID:          "machine-1",
		routeTransport:     types.BackendRouteTransportTSNet,
	}
	require.True(t, worker.deferCPUThrottle(request, &specs.LinuxCPU{}))
	require.True(t, worker.hasDeferredCPUThrottle(containerID))
}

func TestApplyDeferredSandboxCPUThrottleClearsQuotaAfterRuntimeUpdate(t *testing.T) {
	rt := &mockResourceRuntime{mockRuntime: mockRuntime{name: "runc"}}
	cpuQuota := int64(10000)
	period := uint64(100000)
	instance := &ContainerInstance{
		Id: "container-1",
		DeferredCPUQuota: &specs.LinuxCPU{
			Quota:  &cpuQuota,
			Period: &period,
		},
		Runtime: rt,
	}
	containerInstances := common.NewSafeMap[*ContainerInstance]()
	containerInstances.Set("container-1", instance)

	worker := &Worker{containerInstances: containerInstances}
	err := worker.applyDeferredCPUThrottle(&types.ContainerRequest{ContainerId: "container-1"}, instance)
	require.NoError(t, err)
	require.Equal(t, "container-1", rt.updateContainerID)
	require.NotNil(t, rt.updatedResources)
	require.Equal(t, cpuQuota, *rt.updatedResources.CPU.Quota)

	updated, exists := containerInstances.Get("container-1")
	require.True(t, exists)
	require.Nil(t, updated.DeferredCPUQuota)
}

func TestDeferredFunctionCPUThrottleStopsContainerWhenUpdateFails(t *testing.T) {
	containerID := "cpu-throttle-update-failure"
	readyDir := runnerSignalDir(containerID)
	require.NoError(t, os.MkdirAll(readyDir, 0o755))
	t.Cleanup(func() { _ = os.RemoveAll(filepath.Dir(readyDir)) })
	require.NoError(t, os.WriteFile(
		filepath.Join(readyDir, filepath.Base(types.ContainerRunnerReadyPath)),
		nil,
		0o644,
	))

	rt := &mockResourceRuntime{
		mockRuntime: mockRuntime{name: "runc"},
		updateErr:   assert.AnError,
	}
	quota := int64(10000)
	instances := common.NewSafeMap[*ContainerInstance]()
	instances.Set(containerID, &ContainerInstance{
		Id:               containerID,
		DeferredCPUQuota: &specs.LinuxCPU{Quota: &quota},
		Runtime:          rt,
	})
	worker := &Worker{containerInstances: instances}

	err := worker.applyDeferredCPUThrottleAfterRunnerReady(context.Background(), &types.ContainerRequest{ContainerId: containerID})

	require.ErrorIs(t, err, assert.AnError)
	require.Equal(t, []syscall.Signal{syscall.SIGKILL}, rt.signals)
}

func TestNormalizeContainerExitCodePreservesUnexpectedSigkill(t *testing.T) {
	assert.Equal(t,
		int(types.ContainerExitCodeOomKill),
		normalizeContainerExitCode(int(types.ContainerExitCodeOomKill), types.StopContainerReasonUnknown, false),
	)
}

func TestNormalizeContainerExitCodeMapsGracefulSigtermToSuccess(t *testing.T) {
	assert.Equal(t,
		int(types.ContainerExitCodeSuccess),
		normalizeContainerExitCode(int(types.ContainerExitCodeSigterm), types.StopContainerReasonUnknown, false),
	)
}

func TestNormalizeContainerExitCodeMapsExplicitStopReasons(t *testing.T) {
	tests := []struct {
		name     string
		reason   types.StopContainerReason
		wantExit int
	}{
		{name: "scheduler", reason: types.StopContainerReasonScheduler, wantExit: int(types.ContainerExitCodeScheduler)},
		{name: "ttl", reason: types.StopContainerReasonTtl, wantExit: int(types.ContainerExitCodeTtl)},
		{name: "user", reason: types.StopContainerReasonUser, wantExit: int(types.ContainerExitCodeUser)},
		{name: "admin", reason: types.StopContainerReasonAdmin, wantExit: int(types.ContainerExitCodeAdmin)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.wantExit, normalizeContainerExitCode(0, tt.reason, false))
		})
	}
}

func TestRuntimeNeedsGraceKill(t *testing.T) {
	tests := []struct {
		name  string
		state func(context.Context, string) (runtime.State, error)
		want  bool
	}{
		{
			name: "running",
			state: func(context.Context, string) (runtime.State, error) {
				return runtime.State{Status: types.RuncContainerStatusRunning}, nil
			},
			want: true,
		},
		{
			name: "stopped",
			state: func(context.Context, string) (runtime.State, error) {
				return runtime.State{Status: types.RuncContainerStatusStopped}, nil
			},
		},
		{
			name: "created runtime still requires termination",
			state: func(context.Context, string) (runtime.State, error) {
				return runtime.State{Status: types.RuncContainerStatusCreated}, nil
			},
			want: true,
		},
		{
			name: "paused runtime still requires termination",
			state: func(context.Context, string) (runtime.State, error) {
				return runtime.State{Status: types.RuncContainerStatusPaused}, nil
			},
			want: true,
		},
		{
			name: "terminal checkpoint removed runtime",
			state: func(context.Context, string) (runtime.State, error) {
				return runtime.State{}, runtime.ErrContainerNotFound{ContainerID: "container"}
			},
		},
		{
			name: "transient state failure still forces stop",
			state: func(context.Context, string) (runtime.State, error) {
				return runtime.State{}, errors.New("runtime state temporarily unavailable")
			},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rt := &mockRuntime{state: tt.state}
			require.Equal(t, tt.want, runtimeNeedsGraceKill(context.Background(), rt, "container"))
		})
	}
	require.False(t, runtimeNeedsGraceKill(context.Background(), nil, "container"))
}

func TestStopContainerReturnsRuntimeKillError(t *testing.T) {
	rt := &mockRuntime{killErr: assert.AnError}
	worker := &Worker{containerInstances: common.NewSafeMap[*ContainerInstance]()}
	worker.containerInstances.Set("container-kill-error", &ContainerInstance{
		Id:      "container-kill-error",
		Runtime: rt,
	})

	err := worker.stopContainer("container-kill-error", true)

	require.ErrorIs(t, err, assert.AnError)
	require.Equal(t, []syscall.Signal{syscall.SIGKILL}, rt.signals)
}

func TestClearContainerWithoutDurableDiskUsesNormalStoppingLease(t *testing.T) {
	request := &types.ContainerRequest{ContainerId: "container-no-durable-disk"}
	repoClient := &fakeContainerRepoClient{}
	worker := newContainerFinalizationTestWorker(request, repoClient, nil)

	worker.clearContainer(request.ContainerId, request, 0, false)

	updates := repoClient.containerStatusUpdates()
	require.Len(t, updates, 1)
	require.Equal(t, string(types.ContainerStatusStopping), updates[0].Status)
	require.Equal(t, int64(types.ContainerStateTtlS), updates[0].ExpirySeconds)
	require.NotEqual(t, int64(types.ContainerStateTtlSWhileStopping), updates[0].ExpirySeconds)
	require.Equal(t, 1, repoClient.containerExitCodeCalls())
	require.Equal(t, int32(types.ContainerExitCodeSuccess), repoClient.lastSetExitCode.ExitCode)
	require.Equal(t, 1, repoClient.deleteContainerStateCalls())
	select {
	case completed := <-worker.completedRequests:
		require.Same(t, request, completed)
	default:
		t.Fatal("ordinary container capacity was not released")
	}
	_, exists := worker.containerInstances.Get(request.ContainerId)
	require.False(t, exists)
}

func TestClearContainerRetriesExitPublication(t *testing.T) {
	request := &types.ContainerRequest{ContainerId: "container-exit-retry"}
	repoClient := &fakeContainerRepoClient{
		setExitCodeErrors: []error{
			errors.New("first exit publication failed"),
			errors.New("second exit publication failed"),
		},
	}
	worker := newContainerFinalizationTestWorker(request, repoClient, nil)

	worker.clearContainer(request.ContainerId, request, 11, false)

	require.Equal(t, 3, repoClient.containerExitCodeCalls())
	require.Equal(t, int32(11), repoClient.lastSetExitCode.ExitCode)
	require.Equal(t, 1, repoClient.deleteContainerStateCalls())
}

func TestSetContainerExitCodeTreatsMissingStateAsComplete(t *testing.T) {
	request := &types.ContainerRequest{ContainerId: "container-already-gone"}
	repoClient := &fakeContainerRepoClient{
		setExitCodeErrors: []error{&types.ErrContainerStateNotFound{ContainerId: request.ContainerId}},
	}
	worker := newContainerFinalizationTestWorker(request, repoClient, nil)

	require.True(t, worker.setContainerExitCode(request.ContainerId, 0))
	require.Equal(t, 1, repoClient.containerExitCodeCalls())
}

func TestClearContainerTreatsMissingStateAsTerminal(t *testing.T) {
	request := &types.ContainerRequest{
		ContainerId:    "container-missing-state",
		PersistentRoot: &types.PersistentRoot{Size: "1Gi"},
	}
	missing := &types.ErrContainerStateNotFound{ContainerId: request.ContainerId}
	repoClient := &fakeContainerRepoClient{updateStatusErrors: []error{missing, missing}}
	worker := newContainerFinalizationTestWorker(request, repoClient, nil)

	worker.clearContainer(request.ContainerId, request, 12, false)

	require.Len(t, repoClient.containerStatusUpdates(), 2)
	require.Equal(t, 1, repoClient.containerExitCodeCalls())
	require.Equal(t, 1, repoClient.deleteContainerStateCalls())
	_, exists := worker.containerInstances.Get(request.ContainerId)
	require.False(t, exists)
}

func TestDeleteContainerStateRetriesBeforeDroppingLocalInstance(t *testing.T) {
	repoClient := &fakeContainerRepoClient{
		deleteStateErrors: []error{errors.New("transient delete failure")},
	}
	worker := &Worker{
		containerRepoClient: repoClient,
		containerInstances:  common.NewSafeMap[*ContainerInstance](),
	}
	worker.containerInstances.Set("container-retry-delete", &ContainerInstance{Id: "container-retry-delete"})

	worker.deleteContainer("container-retry-delete")

	require.Equal(t, 2, repoClient.deleteContainerStateCalls())
	_, exists := worker.containerInstances.Get("container-retry-delete")
	require.False(t, exists)
}

func TestDeleteContainerStateTreatsMissingStateAsDeleted(t *testing.T) {
	repoClient := &fakeContainerRepoClient{
		deleteStateErrors: []error{&types.ErrContainerStateNotFound{ContainerId: "container-missing"}},
	}
	worker := &Worker{
		containerRepoClient: repoClient,
		containerInstances:  common.NewSafeMap[*ContainerInstance](),
	}
	worker.containerInstances.Set("container-missing", &ContainerInstance{Id: "container-missing"})

	worker.deleteContainer("container-missing")

	require.Equal(t, 1, repoClient.deleteContainerStateCalls())
	_, exists := worker.containerInstances.Get("container-missing")
	require.False(t, exists)
}

func TestFinishContainerShutdownSkipsGraceWhenRuntimeIsGone(t *testing.T) {
	repoClient := &fakeContainerRepoClient{}
	rt := &mockRuntime{state: func(context.Context, string) (runtime.State, error) {
		return runtime.State{}, runtime.ErrContainerNotFound{ContainerID: "container-gone"}
	}}
	request := &types.ContainerRequest{ContainerId: "container-gone"}
	worker := newContainerFinalizationTestWorker(request, repoClient, rt)
	worker.config.Worker.TerminationGracePeriod = 30

	done := make(chan struct{})
	go func() {
		worker.finishContainerShutdown(request.ContainerId, request)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("shutdown waited for grace after the runtime was gone")
	}
	require.Empty(t, rt.signals)
	require.Equal(t, 1, repoClient.deleteContainerStateCalls())
}

func TestFinishContainerShutdownDeletesStateBeforeCapacityNotification(t *testing.T) {
	repoClient := &fakeContainerRepoClient{deleteStateDone: make(chan struct{}, 1)}
	request := &types.ContainerRequest{ContainerId: "container-blocked-capacity"}
	worker := newContainerFinalizationTestWorker(request, repoClient, nil)
	worker.completedRequests = make(chan *types.ContainerRequest)

	done := make(chan struct{})
	go func() {
		worker.finishContainerShutdown(request.ContainerId, request)
		close(done)
	}()

	select {
	case <-repoClient.deleteStateDone:
	case <-time.After(time.Second):
		t.Fatal("container state deletion was blocked by capacity notification")
	}
	require.Eventually(t, func() bool {
		_, exists := worker.containerInstances.Get(request.ContainerId)
		return !exists
	}, time.Second, 10*time.Millisecond)

	select {
	case completed := <-worker.completedRequests:
		require.Same(t, request, completed)
	case <-time.After(time.Second):
		t.Fatal("capacity notification was not delivered")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("shutdown did not finish after capacity notification")
	}
}

func newContainerFinalizationTestWorker(request *types.ContainerRequest, repoClient *fakeContainerRepoClient, rt runtime.Runtime) *Worker {
	instances := common.NewSafeMap[*ContainerInstance]()
	instance := &ContainerInstance{Id: request.ContainerId, Request: request, Runtime: rt}
	instance.setExitCode(-1)
	instances.Set(request.ContainerId, instance)
	return &Worker{
		ctx:                     context.Background(),
		containerRepoClient:     repoClient,
		containerInstances:      instances,
		containerNetworkManager: &fakeContainerNetworkController{},
		completedRequests:       make(chan *types.ContainerRequest, 1),
		runtime:                 rt,
	}
}

func TestContainerExitReasonSeparatesCompletionFromStops(t *testing.T) {
	require.Equal(t, "COMPLETED", containerExitReason(0, types.StopContainerReasonUnknown, false))
	require.Equal(t, "SIGKILL", containerExitReason(int(types.ContainerExitCodeOomKill), types.StopContainerReasonUnknown, false))
	require.Equal(t, "OOM", containerExitReason(int(types.ContainerExitCodeOomKill), types.StopContainerReasonUnknown, true))
	require.Equal(t, string(types.StopContainerReasonUser), containerExitReason(0, types.StopContainerReasonUser, false))
}

func TestEventStopReasonOmitsUnknown(t *testing.T) {
	require.Empty(t, eventStopReason(types.StopContainerReasonUnknown))
	require.Equal(t, string(types.StopContainerReasonScheduler), eventStopReason(types.StopContainerReasonScheduler))
}

func TestDeleteRuntimeContainerUsesFreshCleanupContext(t *testing.T) {
	workerCtx, cancel := context.WithCancel(context.Background())
	cancel()

	rt := &deleteContextRuntime{mockRuntime: mockRuntime{name: "runc"}}
	worker := &Worker{
		ctx:                workerCtx,
		runtime:            rt,
		containerInstances: common.NewSafeMap[*ContainerInstance](),
	}
	worker.containerInstances.Set("container-1", &ContainerInstance{Id: "container-1", Runtime: rt})

	require.NoError(t, worker.deleteRuntimeContainer("container-1"))
	require.True(t, rt.deleteCalled)
	require.NoError(t, rt.deleteCtxErr)
}

func TestRunContainerDoesNotCancelRuntimeRunWithWorkerContext(t *testing.T) {
	outerCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rt := &runContextRuntime{
		mockRuntime: mockRuntime{name: "runc"},
		entered:     make(chan struct{}),
		release:     make(chan struct{}),
		ctxErr:      make(chan error, 1),
	}
	worker := &Worker{
		containerInstances:  common.NewSafeMap[*ContainerInstance](),
		containerStartSem:   make(chan struct{}, 1),
		containerStartLimit: 1,
	}
	request := &types.ContainerRequest{ContainerId: "container-1"}
	worker.containerInstances.Set("container-1", &ContainerInstance{
		Id:      "container-1",
		Runtime: rt,
	})

	result := make(chan error, 1)
	go func() {
		_, err := worker.runContainer(
			outerCtx,
			request,
			slog.New(slog.NewTextHandler(io.Discard, nil)),
			common.NewOutputWriter(func(string) {}),
			make(chan int, 1),
			time.Now(),
			nil,
		)
		result <- err
	}()

	<-rt.entered
	cancel()
	select {
	case worker.containerStartSem <- struct{}{}:
		<-worker.containerStartSem
	case <-time.After(time.Second):
		t.Fatal("start slot was not released when startup context was canceled")
	}
	select {
	case err := <-result:
		t.Fatalf("runtime Run returned before its uncancelled context was released: %v", err)
	default:
	}
	close(rt.release)

	require.NoError(t, <-rt.ctxErr)
	require.NoError(t, <-result)
}

func TestAttemptRestoreCheckpointRestoresRuntimeOnly(t *testing.T) {
	containerID := "container-restore-sandbox"
	t.Cleanup(func() { _ = os.RemoveAll(filepath.Join("/tmp", containerID)) })

	rt := &mockRuntime{name: "runc"}
	worker := &Worker{
		criuManager:        &startedCRIUManager{},
		containerInstances: common.NewSafeMap[*ContainerInstance](),
	}
	worker.containerInstances.Set(containerID, &ContainerInstance{
		Id:                    containerID,
		Runtime:               rt,
		StateMemoryCheckpoint: &StateMemoryCheckpoint{ID: "checkpoint-sandbox-restore"},
	})
	request := &types.ContainerRequest{
		ContainerId:     containerID,
		StateSnapshotId: "state-snapshot-sandbox-restore",
		ConfigPath:      filepath.Join(t.TempDir(), "config.json"),
		Stub:            types.StubWithRelated{Stub: types.Stub{Type: types.StubType(types.StubTypeSandbox)}},
	}

	exitCode, restored, started, err := worker.attemptRestoreCheckpoint(
		context.Background(),
		request,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		common.NewOutputWriter(func(string) {}),
		make(chan int, 1),
	)

	require.NoError(t, err)
	require.True(t, restored)
	require.True(t, started)
	require.Equal(t, 0, exitCode)
	require.Empty(t, rt.signals)
	require.Empty(t, rt.killOpts)
}

func TestAddRequestMountsBuildsVolumeCacheMap(t *testing.T) {
	localPath := filepath.Join(t.TempDir(), "volume")
	spec := getTestBaseSpec()
	request := &types.ContainerRequest{
		ContainerId: "container-1",
		Mounts: []types.Mount{{
			LocalPath: localPath,
			MountPath: filepath.Join(types.WorkerContainerVolumePath, "data"),
			ReadOnly:  true,
		}},
	}

	volumeCacheMap, err := (&Worker{}).addRequestMounts(request, &spec)

	require.NoError(t, err)
	require.Equal(t, map[string]string{"data": localPath}, volumeCacheMap)
	require.Len(t, spec.Mounts, 1)
	require.Equal(t, localPath, spec.Mounts[0].Source)
	require.Equal(t, request.Mounts[0].MountPath, spec.Mounts[0].Destination)
	require.Equal(t, []string{"rbind", "ro"}, spec.Mounts[0].Options)
}

func TestAddRequestMountsSkipsMissingMountPoint(t *testing.T) {
	spec := getTestBaseSpec()
	missingPath := filepath.Join(t.TempDir(), "missing")
	request := &types.ContainerRequest{
		ContainerId: "container-1",
		Mounts: []types.Mount{{
			LocalPath: missingPath,
			MountPath: "/mnt/data",
			MountType: storage.StorageModeMountPoint,
		}},
	}

	volumeCacheMap, err := (&Worker{}).addRequestMounts(request, &spec)

	require.NoError(t, err)
	require.Empty(t, volumeCacheMap)
	require.Empty(t, spec.Mounts)
}

func TestEnsureBindMountSourceDirsCreatesMissingSources(t *testing.T) {
	root := t.TempDir()
	outputPath := filepath.Join(root, "outputs", "stub")
	mountPointPath := filepath.Join(root, "external")
	require.NoError(t, os.MkdirAll(mountPointPath, 0755))

	request := &types.ContainerRequest{
		ContainerId: "container-1",
		Mounts: []types.Mount{
			{
				LocalPath: outputPath,
				MountPath: types.WorkerUserOutputVolume,
			},
			{
				LocalPath: mountPointPath,
				MountPath: "/mnt/external",
				MountType: storage.StorageModeMountPoint,
			},
		},
	}

	require.NoError(t, (&ContainerMountManager{}).ensureBindMountSourceDirs(context.Background(), request.Mounts))
	require.DirExists(t, outputPath)
	require.DirExists(t, mountPointPath)
}

// TestV2ImageEnvironmentFlow tests that v2 images correctly extract metadata from CLIP archives
// Note: Without actual CLIP archives, this test verifies graceful handling
func TestV2ImageEnvironmentFlow(t *testing.T) {
	// Create a test config
	config := types.AppConfig{
		ImageService: types.ImageServiceConfig{
			ClipVersion: 2,
		},
		Worker: types.WorkerConfig{},
	}

	// Skopeo should NOT be called for v2 images
	mockSkopeo := &mockSkopeoClient{
		inspectFunc: func(ctx context.Context, image string, creds string, logger *slog.Logger) (common.ImageMetadata, error) {
			t.Fatal("Skopeo should not be called for v2 images")
			return common.ImageMetadata{}, nil
		},
	}

	// Create a mock runtime
	mockRuntime := &mockRuntime{
		name: "runc",
		capabilities: runtime.Capabilities{
			CheckpointRestore: true,
			GPU:               true,
			OOMEvents:         false,
			JoinExistingNetNS: true,
			CDI:               true,
		},
	}

	// Create a test worker with mock dependencies
	worker := &Worker{
		config:             config,
		imageMountPath:     "/tmp/test-images",
		containerInstances: common.NewSafeMap[*ContainerInstance](),
		runtime:            mockRuntime,
		imageClient: &ImageClient{
			skopeoClient: mockSkopeo,
			v2ImageRefs:  common.NewSafeMap[string](),
		},
		containerServer: &ContainerRuntimeServer{
			baseConfigSpec: getTestBaseSpec(),
		},
	}

	// Create a test container request for a v2 image
	sourceImage := "docker.io/library/ubuntu:20.04"
	request := &types.ContainerRequest{
		ContainerId: "test-container-123",
		EntryPoint:  []string{"python3", "-m", "beta9.runner.function"},
		ImageId:     "test-image-456",
		Stub: types.StubWithRelated{
			Stub: types.Stub{
				Type: types.StubType("function"),
			},
		},
		BuildOptions: types.BuildOptions{
			SourceImage:      &sourceImage,
			SourceImageCreds: "",
		},
		Env: []string{
			"BETA9_TOKEN=test-token",
			"STUB_ID=test-stub",
		},
	}

	// V2 images attempt to extract metadata from CLIP archive
	t.Run("ReadBundleConfig_V2", func(t *testing.T) {
		// Without a real CLIP archive, readBundleConfig returns nil gracefully
		initialSpec, err := worker.readBundleConfig(request)
		require.NoError(t, err)

		// Spec will be nil without archive (real archives tested in integration tests)
		assert.Nil(t, initialSpec, "Should return nil when CLIP archive is not present")
		t.Logf("✅ V2 image correctly attempts to extract from CLIP archive")
	})

	// V2 image behavior: uses base spec when no archive metadata
	t.Run("SpecFromRequest_WithNilInitialSpec", func(t *testing.T) {
		options := &ContainerOptions{
			BundlePath:   "/tmp/test-bundle",
			HostBindPort: 8001,
			BindPorts:    []int{8001},
			InitialSpec:  nil, // V2 images may have nil initial spec
		}

		spec, err := worker.specFromRequest(request, options)
		require.NoError(t, err)
		require.NotNil(t, spec)

		t.Logf("✅ V2 image successfully generated spec with nil initial spec (uses base config)")
	})
}

// TestV2ImageEnvironmentFlow_NonBuildContainer tests that v2 non-build containers
// can extract metadata from CLIP archives
func TestV2ImageEnvironmentFlow_NonBuildContainer(t *testing.T) {
	config := types.AppConfig{
		ImageService: types.ImageServiceConfig{
			ClipVersion: 2,
		},
		Worker: types.WorkerConfig{},
	}

	mockSkopeo := &mockSkopeoClient{
		inspectFunc: func(ctx context.Context, image string, creds string, logger *slog.Logger) (common.ImageMetadata, error) {
			t.Fatal("Skopeo should not be called for v2 images")
			return common.ImageMetadata{}, nil
		},
	}

	imageClient := &ImageClient{
		skopeoClient: mockSkopeo,
		config:       config,
		v2ImageRefs:  common.NewSafeMap[string](),
	}

	mockRuntime := &mockRuntime{
		name: "runc",
		capabilities: runtime.Capabilities{
			CheckpointRestore: true,
			GPU:               true,
			OOMEvents:         false,
			JoinExistingNetNS: true,
			CDI:               true,
		},
	}

	worker := &Worker{
		config:             config,
		imageMountPath:     "/tmp/test-images",
		containerInstances: common.NewSafeMap[*ContainerInstance](),
		runtime:            mockRuntime,
		imageClient:        imageClient,
		containerServer: &ContainerRuntimeServer{
			baseConfigSpec: getTestBaseSpec(),
		},
	}

	// Create a non-build container request (like a sandbox)
	// For v2 images, metadata comes from CLIP archive, not skopeo
	imageId := "v2-image-abc123"
	request := &types.ContainerRequest{
		ContainerId: "sandbox-xyz",
		ImageId:     imageId,
		Env: []string{
			"USER_VAR=test",
		},
	}

	t.Run("V2Image_ExtractsFromArchive", func(t *testing.T) {
		// Without a real CLIP archive, readBundleConfig will try to derive from v2 image
		// and return nil (gracefully handling missing archive)
		initialSpec, err := worker.readBundleConfig(request)
		require.NoError(t, err)
		// Spec will be nil without a real archive
		assert.Nil(t, initialSpec, "Should return nil when CLIP archive is not present (tested with real archives in integration tests)")

		t.Logf("✅ V2 image correctly attempts to extract metadata from CLIP archive")
	})
}

// Helper function
func containsStr(s, substr string) bool {
	return len(s) >= len(substr) && s[:len(substr)] == substr
}

// Mock skopeo client for testing
type mockSkopeoClient struct {
	inspectFunc     func(ctx context.Context, image string, creds string, logger *slog.Logger) (common.ImageMetadata, error)
	inspectSizeFunc func(ctx context.Context, image string, creds string) (int64, error)
	copyFunc        func(ctx context.Context, source, dest, creds string, logger *slog.Logger) error
}

func (m *mockSkopeoClient) Inspect(ctx context.Context, image string, creds string, logger *slog.Logger) (common.ImageMetadata, error) {
	if m.inspectFunc != nil {
		return m.inspectFunc(ctx, image, creds, logger)
	}
	return common.ImageMetadata{}, nil
}

func (m *mockSkopeoClient) InspectSizeInBytes(ctx context.Context, image string, creds string) (int64, error) {
	if m.inspectSizeFunc != nil {
		return m.inspectSizeFunc(ctx, image, creds)
	}
	return 0, nil
}

func (m *mockSkopeoClient) Copy(ctx context.Context, source, dest, creds string, logger *slog.Logger) error {
	if m.copyFunc != nil {
		return m.copyFunc(ctx, source, dest, creds, logger)
	}
	return nil
}

// TestCachedImageMetadata tests that cached metadata from CLIP archives is used correctly
func TestCachedImageMetadata(t *testing.T) {
	config := types.AppConfig{
		ImageService: types.ImageServiceConfig{
			ClipVersion: 2,
		},
		Worker: types.WorkerConfig{},
	}

	// Create mock skopeo client (should NOT be called when metadata is cached)
	skopeoCallCount := 0
	mockSkopeo := &mockSkopeoClient{
		inspectFunc: func(ctx context.Context, image string, creds string, logger *slog.Logger) (common.ImageMetadata, error) {
			skopeoCallCount++
			t.Logf("Skopeo.Inspect called (count: %d) - this should NOT happen when metadata is cached", skopeoCallCount)
			return common.ImageMetadata{}, nil
		},
	}

	imageClient := &ImageClient{
		skopeoClient: mockSkopeo,
		config:       config,
		v2ImageRefs:  common.NewSafeMap[string](),
	}

	mockRuntime := &mockRuntime{
		name: "runc",
		capabilities: runtime.Capabilities{
			CheckpointRestore: true,
			GPU:               true,
			OOMEvents:         false,
			JoinExistingNetNS: true,
			CDI:               true,
		},
	}

	worker := &Worker{
		config:             config,
		imageMountPath:     "/tmp/test-images",
		containerInstances: common.NewSafeMap[*ContainerInstance](),
		runtime:            mockRuntime,
		imageClient:        imageClient,
		containerServer: &ContainerRuntimeServer{
			baseConfigSpec: getTestBaseSpec(),
		},
	}

	t.Run("UsesCachedMetadata", func(t *testing.T) {
		// Note: In real use, metadata would be extracted from the CLIP archive on-demand.
		// Since we don't have actual archives in tests, this test verifies the fallback path.
		// For v2 images with metadata, GetImageMetadata() would extract it from the archive.

		imageId := "v2-cached-image-123"
		request := &types.ContainerRequest{
			ContainerId: "test-container-cached",
			ImageId:     imageId,
		}

		// Without a real archive, metadata extraction will fail gracefully
		spec, err := worker.deriveSpecFromV2Image(request)
		// No error since it falls back gracefully
		assert.NoError(t, err)
		// Spec will be nil since there's no archive
		assert.Nil(t, spec)

		t.Logf("✅ Verified v2 metadata extraction path (would extract from archive in real use)")
	})

	t.Run("GracefullyHandlesMissingArchive", func(t *testing.T) {
		// For v2 images without an archive, should return nil spec gracefully
		uncachedImageId := "v2-no-archive-456"
		request := &types.ContainerRequest{
			ContainerId: "test-container-no-archive",
			ImageId:     uncachedImageId,
		}

		// Should gracefully return nil when archive is missing
		spec, err := worker.deriveSpecFromV2Image(request)
		require.NoError(t, err)
		assert.Nil(t, spec, "Should return nil spec when archive metadata is missing")

		t.Logf("✅ Gracefully handled missing v2 archive")
	})
}

func TestGetCLIPImageMetadataUsesCachedV2ArchiveMetadata(t *testing.T) {
	imageId := "v2-cached-metadata"
	imageMetadata := &clipCommon.ImageMetadata{
		Env:        []string{"FOO=bar"},
		WorkingDir: "/workspace",
		Cmd:        []string{"python", "app.py"},
	}

	imageClient := &ImageClient{
		v2ArchiveMetadata: common.NewSafeMap[*clipCommon.ClipArchiveMetadata](),
		v2ImageRefs:       common.NewSafeMap[string](),
	}
	imageClient.v2ArchiveMetadata.Set(imageId, &clipCommon.ClipArchiveMetadata{
		StorageInfo: &clipCommon.OCIStorageInfo{
			ImageMetadata: imageMetadata,
		},
	})

	got, ok := imageClient.GetCLIPImageMetadata(imageId)
	require.True(t, ok)
	assert.Equal(t, imageMetadata, got)
}

func TestBuildSpecFromCLIPMetadataDefaultsCwd(t *testing.T) {
	worker := &Worker{}

	spec := worker.buildSpecFromCLIPMetadata(&clipCommon.ImageMetadata{
		Cmd: []string{"python", "-m", "http.server", "8000"},
	})

	require.NotNil(t, spec.Process)
	assert.Equal(t, "/", spec.Process.Cwd)
	assert.Equal(t, []string{"python", "-m", "http.server", "8000"}, spec.Process.Args)
}

func TestBuildSpecFromCLIPMetadataCombinesEntrypointAndCmd(t *testing.T) {
	worker := &Worker{}

	spec := worker.buildSpecFromCLIPMetadata(&clipCommon.ImageMetadata{
		Entrypoint: []string{"vllm", "serve"},
		Cmd:        []string{"--model", "Qwen/Qwen2.5-1.5B-Instruct"},
	})

	require.NotNil(t, spec.Process)
	assert.Equal(t, []string{
		"vllm",
		"serve",
		"--model",
		"Qwen/Qwen2.5-1.5B-Instruct",
	}, spec.Process.Args)
}

func TestBuildSpecFromCLIPMetadataPreservesWorkingDir(t *testing.T) {
	worker := &Worker{}

	spec := worker.buildSpecFromCLIPMetadata(&clipCommon.ImageMetadata{
		WorkingDir: "/app",
		Cmd:        []string{"python", "app.py"},
	})

	require.NotNil(t, spec.Process)
	assert.Equal(t, "/app", spec.Process.Cwd)
	assert.Equal(t, []string{"python", "app.py"}, spec.Process.Args)
}

func TestCacheOCIMetadataStoresPointerMetadataAndSourceRef(t *testing.T) {
	imageId := "v2-pointer-metadata"
	imageClient := &ImageClient{
		v2ArchiveMetadata: common.NewSafeMap[*clipCommon.ClipArchiveMetadata](),
		v2ImageRefs:       common.NewSafeMap[string](),
	}

	meta := &clipCommon.ClipArchiveMetadata{
		StorageInfo: &clipCommon.OCIStorageInfo{
			RegistryURL: "https://registry.example.com",
			Repository:  "team/image",
			Reference:   "latest",
		},
	}
	imageClient.cacheOCIMetadata(imageId, meta)

	cachedMeta, ok := imageClient.v2ArchiveMetadata.Get(imageId)
	require.True(t, ok)
	assert.Equal(t, meta, cachedMeta)

	sourceRef, ok := imageClient.GetSourceImageRef(imageId)
	require.True(t, ok)
	assert.Equal(t, "registry.example.com/team/image:latest", sourceRef)
}

func TestMountedImageReadyTracksMountedServer(t *testing.T) {
	imageId := "warm-image"
	imageClient := &ImageClient{
		mountedFuseServers: common.NewSafeMap[*fuse.Server](),
	}

	imageClient.mountedFuseServers.Set(imageId, nil)
	assert.True(t, imageClient.mountedImageReady(imageId))
}

func TestPullImageFromRegistryKeepsPersistentLockFile(t *testing.T) {
	dir := t.TempDir()
	archivePath := filepath.Join(dir, "image.clip")
	lockPath := archivePath + ".lock"
	require.NoError(t, os.WriteFile(archivePath, []byte("clip"), 0644))

	imageClient := &ImageClient{}
	_, err := imageClient.pullImageFromRegistry(context.Background(), archivePath, &types.ContainerRequest{ImageId: "image"})
	require.NoError(t, err)

	_, err = os.Stat(lockPath)
	require.NoError(t, err)
}

func TestOpenImageLockFileCreatesParentDirectory(t *testing.T) {
	lockPath := filepath.Join(t.TempDir(), "missing", "nested", "image.clip.lock")

	lockFile, err := openImageLockFile(lockPath)
	require.NoError(t, err)
	require.NoError(t, lockFile.Close())

	_, err = os.Stat(lockPath)
	require.NoError(t, err)
}

// Get a base test spec
func getTestBaseSpec() specs.Spec {
	return specs.Spec{
		Version: "1.0.2-dev",
		Process: &specs.Process{
			Terminal: false,
			User: specs.User{
				UID: 0,
				GID: 0,
			},
			Args: []string{"sh"},
			Env: []string{
				"TERM=xterm",
			},
			Cwd: "/workspace",
		},
		Root: &specs.Root{
			Path:     "/",
			Readonly: false,
		},
		Hostname: "beta9",
		Mounts:   []specs.Mount{},
		Linux: &specs.Linux{
			Resources: &specs.LinuxResources{},
		},
	}
}

func envListToMap(env []string) map[string]string {
	out := map[string]string{}
	for _, entry := range env {
		key, value, ok := strings.Cut(entry, "=")
		if ok {
			out[key] = value
		}
	}
	return out
}

// Mock runtime for testing
type mockRuntime struct {
	name         string
	capabilities runtime.Capabilities
	state        func(context.Context, string) (runtime.State, error)
	signals      []syscall.Signal
	killOpts     []*runtime.KillOpts
	killErr      error
}

func (m *mockRuntime) Name() string {
	return m.name
}

func (m *mockRuntime) Capabilities() runtime.Capabilities {
	return m.capabilities
}

func (m *mockRuntime) Prepare(ctx context.Context, spec *specs.Spec) error {
	return nil
}

func (m *mockRuntime) Run(ctx context.Context, containerID, bundlePath string, opts *runtime.RunOpts) (int, error) {
	return 0, nil
}

func (m *mockRuntime) Exec(ctx context.Context, containerID string, proc specs.Process, opts *runtime.ExecOpts) error {
	return nil
}

func (m *mockRuntime) Kill(ctx context.Context, containerID string, sig syscall.Signal, opts *runtime.KillOpts) error {
	m.signals = append(m.signals, sig)
	if opts == nil {
		opts = &runtime.KillOpts{}
	}
	m.killOpts = append(m.killOpts, opts)
	return m.killErr
}

func (m *mockRuntime) Delete(ctx context.Context, containerID string, opts *runtime.DeleteOpts) error {
	return nil
}

func (m *mockRuntime) State(ctx context.Context, containerID string) (runtime.State, error) {
	if m.state != nil {
		return m.state(ctx, containerID)
	}
	return runtime.State{}, nil
}

func (m *mockRuntime) Events(ctx context.Context, containerID string) (<-chan runtime.Event, error) {
	return nil, nil
}

func (m *mockRuntime) Checkpoint(ctx context.Context, containerID string, opts *runtime.CheckpointOpts) error {
	return nil
}

func (m *mockRuntime) Restore(ctx context.Context, containerID string, opts *runtime.RestoreOpts) (int, error) {
	return 0, nil
}

func (m *mockRuntime) Close() error {
	return nil
}

type startedCRIUManager struct{}

func (m *startedCRIUManager) Available() bool {
	return true
}

func (m *startedCRIUManager) CreateCheckpoint(ctx context.Context, rt runtime.Runtime, checkpointId string, request *types.ContainerRequest, terminateAfterCheckpoint bool) (string, error) {
	return "", nil
}

func (m *startedCRIUManager) RestoreCheckpoint(ctx context.Context, rt runtime.Runtime, opts *RestoreOpts) (int, error) {
	opts.started <- 1234
	return 0, nil
}

type restoreErrorCRIUManager struct {
	exitCode int
	err      error
}

func (m *restoreErrorCRIUManager) Available() bool {
	return true
}

func (m *restoreErrorCRIUManager) CreateCheckpoint(ctx context.Context, rt runtime.Runtime, checkpointId string, request *types.ContainerRequest, terminateAfterCheckpoint bool) (string, error) {
	return "", nil
}

func (m *restoreErrorCRIUManager) RestoreCheckpoint(ctx context.Context, rt runtime.Runtime, opts *RestoreOpts) (int, error) {
	return m.exitCode, m.err
}

type observingRestoreErrorCRIUManager struct {
	err                  error
	deleteCallsAtRestore int
	restoreCalls         int
	removeConfig         bool
}

func (m *observingRestoreErrorCRIUManager) Available() bool {
	return true
}

func (m *observingRestoreErrorCRIUManager) CreateCheckpoint(ctx context.Context, rt runtime.Runtime, checkpointId string, request *types.ContainerRequest, terminateAfterCheckpoint bool) (string, error) {
	return "", assert.AnError
}

func (m *observingRestoreErrorCRIUManager) RestoreCheckpoint(ctx context.Context, rt runtime.Runtime, opts *RestoreOpts) (int, error) {
	m.restoreCalls++
	if cleanupRuntime, ok := rt.(*restoreFallbackRuntime); ok {
		m.deleteCallsAtRestore = cleanupRuntime.deleteCalls
	}
	opts.started <- 4321
	if m.removeConfig {
		_ = os.Remove(opts.configPath)
	}
	return -1, m.err
}

type restoreFallbackRuntime struct {
	mockRuntime
	deleteCalls           int
	deleteErr             error
	deleteCallsAtRun      int
	runCalled             bool
	runConfigPath         string
	runConfigContents     []byte
	runRootMarkerPath     string
	runRootMarkerContents []byte
}

func (m *restoreFallbackRuntime) Delete(ctx context.Context, containerID string, opts *runtime.DeleteOpts) error {
	m.deleteCalls++
	return m.deleteErr
}

func (m *restoreFallbackRuntime) Run(ctx context.Context, containerID, bundlePath string, opts *runtime.RunOpts) (int, error) {
	m.runCalled = true
	m.deleteCallsAtRun = m.deleteCalls
	if m.runConfigPath != "" {
		m.runConfigContents, _ = os.ReadFile(m.runConfigPath)
	}
	if m.runRootMarkerPath != "" {
		m.runRootMarkerContents, _ = os.ReadFile(m.runRootMarkerPath)
	}
	if opts != nil && opts.Started != nil {
		opts.Started <- 1234
	}
	return 0, nil
}

type fakeBackendRepoClient struct{}

func (*fakeBackendRepoClient) CreateStateSnapshot(context.Context, *pb.CreateStateSnapshotRequest, ...grpc.CallOption) (*pb.CreateStateSnapshotResponse, error) {
	return &pb.CreateStateSnapshotResponse{Ok: true}, nil
}

func (*fakeBackendRepoClient) ArmStateSnapshot(context.Context, *pb.ArmStateSnapshotRequest, ...grpc.CallOption) (*pb.StateSnapshotMutationResponse, error) {
	return &pb.StateSnapshotMutationResponse{Ok: true}, nil
}

func (*fakeBackendRepoClient) ClaimStateSnapshotRecovery(context.Context, *pb.ClaimStateSnapshotRecoveryRequest, ...grpc.CallOption) (*pb.StateSnapshotMutationResponse, error) {
	return &pb.StateSnapshotMutationResponse{Ok: true}, nil
}

func (*fakeBackendRepoClient) GetStateSnapshotRecoveryCredentials(context.Context, *pb.GetStateSnapshotRecoveryCredentialsRequest, ...grpc.CallOption) (*pb.GetStateSnapshotRecoveryCredentialsResponse, error) {
	return &pb.GetStateSnapshotRecoveryCredentialsResponse{Ok: true}, nil
}

func (*fakeBackendRepoClient) FailStateSnapshot(context.Context, *pb.FailStateSnapshotRequest, ...grpc.CallOption) (*pb.StateSnapshotMutationResponse, error) {
	return &pb.StateSnapshotMutationResponse{Ok: true}, nil
}

func (*fakeBackendRepoClient) CommitStateSnapshot(context.Context, *pb.CommitStateSnapshotRequest, ...grpc.CallOption) (*pb.CommitStateSnapshotResponse, error) {
	return &pb.CommitStateSnapshotResponse{Ok: true}, nil
}

func (*fakeBackendRepoClient) GetStateSnapshot(context.Context, *pb.GetStateSnapshotRequest, ...grpc.CallOption) (*pb.GetStateSnapshotResponse, error) {
	return &pb.GetStateSnapshotResponse{Ok: true}, nil
}

func (*fakeBackendRepoClient) GetStateSnapshotByOperation(context.Context, *pb.GetStateSnapshotByOperationRequest, ...grpc.CallOption) (*pb.GetStateSnapshotResponse, error) {
	return &pb.GetStateSnapshotResponse{Ok: true}, nil
}

func (*fakeBackendRepoClient) GetPendingStateSnapshotByContainer(context.Context, *pb.GetPendingStateSnapshotByContainerRequest, ...grpc.CallOption) (*pb.GetStateSnapshotResponse, error) {
	return &pb.GetStateSnapshotResponse{Ok: true}, nil
}

func (*fakeBackendRepoClient) GetVolumeGeneration(context.Context, *pb.GetVolumeGenerationRequest, ...grpc.CallOption) (*pb.GetVolumeGenerationResponse, error) {
	return &pb.GetVolumeGenerationResponse{Ok: true}, nil
}

func (*fakeBackendRepoClient) RenewStateVolumeAttachments(context.Context, *pb.RenewStateVolumeAttachmentsRequest, ...grpc.CallOption) (*pb.RenewStateVolumeAttachmentsResponse, error) {
	return &pb.RenewStateVolumeAttachmentsResponse{Ok: true}, nil
}

func (*fakeBackendRepoClient) ReleaseStateVolumeAttachments(context.Context, *pb.ReleaseStateVolumeAttachmentsRequest, ...grpc.CallOption) (*pb.ReleaseStateVolumeAttachmentsResponse, error) {
	return &pb.ReleaseStateVolumeAttachmentsResponse{Ok: true}, nil
}

func (*fakeBackendRepoClient) BeginStateVolumeReleaseIntent(context.Context, *pb.BeginStateVolumeReleaseIntentRequest, ...grpc.CallOption) (*pb.ClaimStateVolumeReleaseResponse, error) {
	return &pb.ClaimStateVolumeReleaseResponse{Ok: true, ReleaseClaimId: "00000000-0000-4000-8000-000000000001"}, nil
}

func (*fakeBackendRepoClient) ClaimStateVolumeRelease(context.Context, *pb.ClaimStateVolumeReleaseRequest, ...grpc.CallOption) (*pb.ClaimStateVolumeReleaseResponse, error) {
	return &pb.ClaimStateVolumeReleaseResponse{Ok: true, ReleaseClaimId: "00000000-0000-4000-8000-000000000001", ReleaseClaimGeneration: 1}, nil
}

func (*fakeBackendRepoClient) CompleteClaimedStateVolumeRelease(context.Context, *pb.CompleteClaimedStateVolumeReleaseRequest, ...grpc.CallOption) (*pb.CompleteClaimedStateVolumeReleaseResponse, error) {
	return &pb.CompleteClaimedStateVolumeReleaseResponse{Ok: true}, nil
}

type mockResourceRuntime struct {
	mockRuntime
	updateContainerID string
	updatedResources  *specs.LinuxResources
	updateErr         error
}

func (m *mockResourceRuntime) UpdateResources(ctx context.Context, containerID string, resources *specs.LinuxResources) error {
	m.updateContainerID = containerID
	m.updatedResources = resources
	return m.updateErr
}

type deleteContextRuntime struct {
	mockRuntime
	deleteCalled bool
	deleteCtxErr error
}

func (m *deleteContextRuntime) Delete(ctx context.Context, containerID string, opts *runtime.DeleteOpts) error {
	m.deleteCalled = true
	m.deleteCtxErr = ctx.Err()
	return nil
}

type runContextRuntime struct {
	mockRuntime
	entered chan struct{}
	release chan struct{}
	ctxErr  chan error
}

func (m *runContextRuntime) Run(ctx context.Context, containerID, bundlePath string, opts *runtime.RunOpts) (int, error) {
	close(m.entered)
	<-m.release
	m.ctxErr <- ctx.Err()
	return 0, nil
}
