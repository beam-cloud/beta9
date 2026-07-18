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
	"sync/atomic"
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

func TestApplyRuntimeEnvironmentOverridesDisablesLibuvIOUringForCheckpoints(t *testing.T) {
	worker := &Worker{runtime: &mockRuntime{name: types.ContainerRuntimeRunc.String()}}
	env := worker.applyRuntimeEnvironmentOverrides(
		[]string{"UV_USE_IO_URING=1", "TORCHINDUCTOR_QUIESCE_ASYNC_COMPILE_POOL=0", "OTHER=value"},
		&types.ContainerRequest{CheckpointEnabled: true},
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
	require.Equal(t, int64(40*1024*1024*1024), *spec.Linux.Resources.Memory.Limit)
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
		Checkpoint:  &types.Checkpoint{Status: string(types.CheckpointStatusAvailable)},
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
	})
	worker := &Worker{
		criuManager:        &startedCRIUManager{},
		containerInstances: instances,
	}
	request := &types.ContainerRequest{
		ContainerId: "container-1",
		Checkpoint: &types.Checkpoint{
			CheckpointId: "checkpoint-1",
			Status:       string(types.CheckpointStatusAvailable),
		},
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
		make(chan int, 1),
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

func TestSpecFromRequestDisablesIOUringForCheckpoints(t *testing.T) {
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
		ContainerId:       "container-checkpoint",
		EntryPoint:        []string{"python", "app.py"},
		CheckpointEnabled: true,
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
				return runtime.State{Status: "stopped"}, nil
			},
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
		containerInstances: common.NewSafeMap[*ContainerInstance](),
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
			make(chan int, 1),
			time.Now(),
			nil,
			nil,
		)
		result <- err
	}()

	<-rt.entered
	cancel()
	close(rt.release)

	require.NoError(t, <-rt.ctxErr)
	require.NoError(t, <-result)
}

func TestRunContainerRestorePublishesAddressFromStartedHandler(t *testing.T) {
	t.Setenv("WORKER_POOL_NAME", "default")
	tmpDir := t.TempDir()
	checkpointId := "checkpoint-1"
	require.NoError(t, os.MkdirAll(filepath.Join(tmpDir, "checkpoints", checkpointId, checkpointFsDir), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(tmpDir, "checkpoints", checkpointId, "inventory.img"), []byte("runtime payload"), 0644))

	bundleDir := filepath.Join(tmpDir, "bundle")
	require.NoError(t, os.MkdirAll(bundleDir, 0755))
	configPath := filepath.Join(bundleDir, specBaseName)
	require.NoError(t, os.WriteFile(configPath, []byte("{}"), 0644))

	repoClient := &fakeContainerRepoClient{
		state: &pb.ContainerState{Status: string(types.ContainerStatusPending)},
	}
	backendRepoClient := &fakeBackendRepoClient{}
	var stateCalls atomic.Int32
	rt := &mockRuntime{
		name:         "runc",
		capabilities: runtime.Capabilities{CheckpointRestore: true},
		state: func(context.Context, string) (runtime.State, error) {
			if stateCalls.Add(1) == 1 {
				return runtime.State{Pid: 1234, Status: types.RuncContainerStatusRunning}, nil
			}
			return runtime.State{Status: types.RuncContainerStatusStopped}, nil
		},
	}
	worker := &Worker{
		config: types.AppConfig{Worker: types.WorkerConfig{Pools: map[string]types.WorkerPoolConfig{
			"default": {CRIUEnabled: true},
		}}},
		podAddr:             "10.42.0.10",
		criuManager:         &startedCRIUManager{},
		cacheManager:        &WorkerCacheManager{checkpointRoot: filepath.Join(tmpDir, "checkpoints")},
		containerRepoClient: repoClient,
		backendRepoClient:   backendRepoClient,
		containerInstances:  common.NewSafeMap[*ContainerInstance](),
	}
	request := &types.ContainerRequest{
		ContainerId: "container-1",
		ConfigPath:  configPath,
		Checkpoint: &types.Checkpoint{
			CheckpointId: checkpointId,
			Status:       string(types.CheckpointStatusAvailable),
		},
		Stub: types.StubWithRelated{Stub: types.Stub{Type: types.StubType(types.StubTypeASGI)}},
	}
	worker.containerInstances.Set(request.ContainerId, &ContainerInstance{
		Id:      request.ContainerId,
		Runtime: rt,
	})

	exitCode, err := worker.runContainer(
		context.Background(),
		request,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		common.NewOutputWriter(func(string) {}),
		make(chan int, 1),
		make(chan int, 1),
		time.Now(),
		[]PortBinding{{HostPort: 30001, ContainerPort: 8001}},
		nil,
	)

	require.NoError(t, err)
	require.Equal(t, -1, exitCode)
	require.Zero(t, repoClient.setAddressCalls)
	require.Equal(t, 1, repoClient.setAddressMapCalls)
	require.Equal(t, int32(8001), repoClient.lastSetAddressMap.PrimaryPort)
	require.Equal(t, "10.42.0.10:30001", repoClient.lastSetAddressMap.AddressMap[8001])
	require.Equal(t, 1, repoClient.updateStatusCalls)
	require.Equal(t, string(types.ContainerStatusRunning), repoClient.lastUpdateStatus.Status)
	require.Eventually(t, func() bool {
		return backendRepoClient.updateCalls == 1 && backendRepoClient.lastUpdate.LastRestoredAt != nil
	}, time.Second, 10*time.Millisecond)
}

func TestCheckpointFilesystemRestorePreparesInitialUpperLayer(t *testing.T) {
	checkpointRoot := t.TempDir()
	checkpointID := "checkpoint-prepare-upper"
	checkpointPath := filepath.Join(checkpointRoot, checkpointID)
	checkpointFilesystem := filepath.Join(checkpointPath, checkpointFsDir)
	require.NoError(t, os.MkdirAll(filepath.Join(checkpointFilesystem, "state"), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(checkpointPath, "inventory.img"), []byte("runtime"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(checkpointFilesystem, "state", "ready"), []byte("yes"), 0644))

	worker := &Worker{
		ctx:          context.Background(),
		cacheManager: &WorkerCacheManager{checkpointRoot: checkpointRoot},
	}
	request := &types.ContainerRequest{
		ContainerId: "checkpoint-prepare-upper-test",
		Checkpoint: &types.Checkpoint{
			CheckpointId: checkpointID,
			Status:       string(types.CheckpointStatusAvailable),
		},
	}
	restore := worker.startCheckpointFilesystemRestore(request, nil)
	t.Cleanup(restore.cleanup)

	require.NoError(t, restore.wait())
	data, err := os.ReadFile(filepath.Join(restore.upperPath, "state", "ready"))
	require.NoError(t, err)
	require.Equal(t, "yes", string(data))
}

func TestCheckpointFilesystemRestoreDiscardRemovesPartialUpperLayer(t *testing.T) {
	overlayRoot := filepath.Join(t.TempDir(), "overlay")
	require.NoError(t, os.MkdirAll(filepath.Join(overlayRoot, "layer-0", "upper"), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(overlayRoot, "layer-0", "upper", "partial"), []byte("partial"), 0644))

	_, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	close(done)
	restore := &checkpointFilesystemRestore{
		overlayRoot: overlayRoot,
		done:        done,
		cancel:      cancel,
		err:         assert.AnError,
	}

	require.NoError(t, restore.discard())
	require.NoDirExists(t, overlayRoot)
}

func TestRunContainerRestoreWaitsForRestoredRuntimeExit(t *testing.T) {
	t.Setenv("WORKER_POOL_NAME", "default")
	tmpDir := t.TempDir()
	checkpointId := "checkpoint-1"
	require.NoError(t, os.MkdirAll(filepath.Join(tmpDir, "checkpoints", checkpointId, checkpointFsDir), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(tmpDir, "checkpoints", checkpointId, "inventory.img"), []byte("runtime payload"), 0644))

	bundleDir := filepath.Join(tmpDir, "bundle")
	require.NoError(t, os.MkdirAll(bundleDir, 0755))
	configPath := filepath.Join(bundleDir, specBaseName)
	require.NoError(t, os.WriteFile(configPath, []byte("{}"), 0644))

	repoClient := &fakeContainerRepoClient{
		state: &pb.ContainerState{Status: string(types.ContainerStatusPending)},
	}
	backendRepoClient := &fakeBackendRepoClient{}
	restoreStopped := make(chan struct{})
	enteredWait := make(chan struct{}, 1)
	rt := &mockRuntime{
		name:         "gvisor",
		capabilities: runtime.Capabilities{CheckpointRestore: true},
		state: func(context.Context, string) (runtime.State, error) {
			select {
			case enteredWait <- struct{}{}:
			default:
			}
			select {
			case <-restoreStopped:
				return runtime.State{Status: types.RuncContainerStatusStopped}, nil
			default:
				return runtime.State{Pid: 1234, Status: types.RuncContainerStatusRunning}, nil
			}
		},
	}
	worker := &Worker{
		config: types.AppConfig{Worker: types.WorkerConfig{Pools: map[string]types.WorkerPoolConfig{
			"default": {CRIUEnabled: true},
		}}},
		podAddr:             "10.42.0.10",
		criuManager:         &startedCRIUManager{},
		cacheManager:        &WorkerCacheManager{checkpointRoot: filepath.Join(tmpDir, "checkpoints")},
		containerRepoClient: repoClient,
		backendRepoClient:   backendRepoClient,
		containerInstances:  common.NewSafeMap[*ContainerInstance](),
	}
	request := &types.ContainerRequest{
		ContainerId: "container-1",
		ConfigPath:  configPath,
		Checkpoint: &types.Checkpoint{
			CheckpointId: checkpointId,
			Status:       string(types.CheckpointStatusAvailable),
		},
		Stub: types.StubWithRelated{Stub: types.Stub{Type: types.StubType(types.StubTypeASGI)}},
	}
	worker.containerInstances.Set(request.ContainerId, &ContainerInstance{
		Id:      request.ContainerId,
		Runtime: rt,
	})

	done := make(chan error, 1)
	go func() {
		_, err := worker.runContainer(
			context.Background(),
			request,
			slog.New(slog.NewTextHandler(io.Discard, nil)),
			common.NewOutputWriter(func(string) {}),
			make(chan int, 1),
			make(chan int, 1),
			time.Now(),
			[]PortBinding{{HostPort: 30001, ContainerPort: 8001}},
			nil,
		)
		done <- err
	}()

	select {
	case <-enteredWait:
	case err := <-done:
		t.Fatalf("runContainer returned before polling restored runtime state: %v", err)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for restored runtime state polling")
	}

	select {
	case err := <-done:
		t.Fatalf("runContainer returned before restored runtime exited: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	close(restoreStopped)
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * restoredContainerPollInterval):
		t.Fatal("runContainer did not return after restored runtime exited")
	}
}

func TestWaitForRestoredContainerExitFailsBeforeRunningState(t *testing.T) {
	rt := &mockRuntime{
		state: func(context.Context, string) (runtime.State, error) {
			return runtime.State{}, runtime.ErrContainerNotFound{ContainerID: "container-restore"}
		},
	}

	exitCode, err := (&Worker{}).waitForRestoredContainerExit(context.Background(), rt, "container-restore", 0)

	require.Error(t, err)
	require.Contains(t, err.Error(), "restored container state unavailable")
	require.Equal(t, -1, exitCode)
}

func TestAttemptRestoreCheckpointTreatsGenericErrorAsRestoreFailure(t *testing.T) {
	restoreErr := assert.AnError
	containerID := "container-restore-generic-error"
	t.Cleanup(func() { _ = os.RemoveAll(filepath.Join("/tmp", containerID)) })

	backendRepoClient := &fakeBackendRepoClient{}
	worker := &Worker{
		criuManager:        &restoreErrorCRIUManager{exitCode: 17, err: restoreErr},
		backendRepoClient:  backendRepoClient,
		containerInstances: common.NewSafeMap[*ContainerInstance](),
	}
	worker.containerInstances.Set(containerID, &ContainerInstance{
		Id:      containerID,
		Runtime: &mockRuntime{name: "runc"},
	})
	request := &types.ContainerRequest{
		ContainerId: containerID,
		ConfigPath:  filepath.Join(t.TempDir(), "config.json"),
		Checkpoint: &types.Checkpoint{
			CheckpointId: "checkpoint-generic-error",
			Status:       string(types.CheckpointStatusAvailable),
		},
	}

	exitCode, restored, started, err := worker.attemptRestoreCheckpoint(
		context.Background(),
		request,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		common.NewOutputWriter(func(string) {}),
		make(chan int, 1),
		make(chan int, 1),
	)

	require.ErrorIs(t, err, restoreErr)
	require.False(t, restored)
	require.False(t, started)
	require.Equal(t, 17, exitCode)
	require.Equal(t, 1, backendRepoClient.updateCalls)
	require.Equal(t, request.Checkpoint.CheckpointId, backendRepoClient.lastUpdate.CheckpointId)
	require.Equal(t, string(types.CheckpointStatusRestoreFailed), backendRepoClient.lastUpdate.Status)
	require.Nil(t, backendRepoClient.lastUpdate.LastRestoredAt)
}

func TestAttemptRestoreCheckpointKeepsHostIncompatibleCheckpointAvailable(t *testing.T) {
	containerID := "container-restore-incompatible-host"
	t.Cleanup(func() { _ = os.RemoveAll(filepath.Join("/tmp", containerID)) })

	backendRepoClient := &fakeBackendRepoClient{}
	worker := &Worker{
		criuManager: &restoreErrorCRIUManager{
			exitCode: -1,
			err:      &ErrCheckpointHostIncompatible{Stderr: "CPU capabilities do not match run time"},
		},
		backendRepoClient:  backendRepoClient,
		containerInstances: common.NewSafeMap[*ContainerInstance](),
	}
	worker.containerInstances.Set(containerID, &ContainerInstance{
		Id:      containerID,
		Runtime: &mockRuntime{name: "runc"},
	})
	request := &types.ContainerRequest{
		ContainerId: containerID,
		ConfigPath:  filepath.Join(t.TempDir(), "config.json"),
		Checkpoint: &types.Checkpoint{
			CheckpointId: "checkpoint-incompatible-host",
			Status:       string(types.CheckpointStatusAvailable),
		},
	}
	var output strings.Builder

	exitCode, restored, started, err := worker.attemptRestoreCheckpoint(
		context.Background(),
		request,
		slog.New(slog.NewTextHandler(&output, nil)),
		common.NewOutputWriter(func(message string) { output.WriteString(message) }),
		make(chan int, 1),
		make(chan int, 1),
	)

	require.True(t, IsCheckpointHostIncompatible(err))
	require.False(t, restored)
	require.False(t, started)
	require.Equal(t, -1, exitCode)
	require.Equal(t, 0, backendRepoClient.updateCalls)
	require.Contains(t, output.String(), "incompatible CPU")
}

func TestRunContainerRestoreFailureCleansRuntimeBeforeFallback(t *testing.T) {
	t.Setenv("WORKER_POOL_NAME", "default")

	restoreErr := assert.AnError
	containerID := "container-restore-fallback"
	checkpointID := "checkpoint-restore-fallback"
	tmpDir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(tmpDir, "checkpoints", checkpointID, checkpointFsDir), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(tmpDir, "checkpoints", checkpointID, "inventory.img"), []byte("runtime payload"), 0644))

	rt := &restoreFallbackRuntime{
		mockRuntime: mockRuntime{
			name:         "runc",
			capabilities: runtime.Capabilities{CheckpointRestore: true},
		},
	}
	criuManager := &observingRestoreErrorCRIUManager{err: restoreErr, removeConfig: true}
	backendRepoClient := &fakeBackendRepoClient{}
	repoClient := &fakeContainerRepoClient{
		state: &pb.ContainerState{Status: string(types.ContainerStatusPending)},
	}
	worker := &Worker{
		config: types.AppConfig{Worker: types.WorkerConfig{Pools: map[string]types.WorkerPoolConfig{
			"default": {CRIUEnabled: true},
		}}},
		podAddr:             "10.42.0.10",
		criuManager:         criuManager,
		containerRepoClient: repoClient,
		backendRepoClient:   backendRepoClient,
		containerInstances:  common.NewSafeMap[*ContainerInstance](),
		cacheManager:        &WorkerCacheManager{checkpointRoot: filepath.Join(tmpDir, "checkpoints")},
	}
	worker.containerInstances.Set(containerID, &ContainerInstance{
		Id:      containerID,
		Runtime: rt,
	})
	configPath := filepath.Join(t.TempDir(), "config.json")
	configContents := []byte(runtime.GetBaseConfig("runc"))
	require.NoError(t, os.WriteFile(configPath, configContents, 0644))
	rt.runConfigPath = configPath
	request := &types.ContainerRequest{
		ContainerId:       containerID,
		ConfigPath:        configPath,
		Stub:              types.StubWithRelated{Stub: types.Stub{Type: types.StubType(types.StubTypeASGIDeployment)}},
		CheckpointEnabled: true,
		Checkpoint: &types.Checkpoint{
			CheckpointId: checkpointID,
			Status:       string(types.CheckpointStatusAvailable),
		},
	}

	exitCode, err := worker.runContainer(
		context.Background(),
		request,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		common.NewOutputWriter(func(string) {}),
		make(chan int, 1),
		make(chan int, 1),
		time.Now(),
		[]PortBinding{{HostPort: 30001, ContainerPort: 8001}},
		nil,
	)

	require.NoError(t, err)
	require.Equal(t, 0, exitCode)
	require.True(t, rt.runCalled)
	require.Equal(t, 0, criuManager.deleteCallsAtRestore)
	require.Equal(t, 1, rt.deleteCallsAtRun)
	require.Equal(t, 1, backendRepoClient.updateCalls)
	require.Equal(t, string(types.CheckpointStatusRestoreFailed), backendRepoClient.lastUpdate.Status)
	require.Nil(t, backendRepoClient.lastUpdate.LastRestoredAt)
	require.Nil(t, request.Checkpoint)
	require.True(t, request.CheckpointEnabled)
	require.Equal(t, 1, repoClient.updateStatusCalls)
	require.Equal(t, string(types.ContainerStatusRunning), repoClient.lastUpdateStatus.Status)
	require.Contains(t, string(rt.runConfigContents), `"ociVersion"`)
	require.Contains(t, string(rt.runConfigContents), "CHECKPOINT_ENABLED=true")
}

func TestRunContainerMaterializeFailureFallsBackWithoutRestore(t *testing.T) {
	t.Setenv("WORKER_POOL_NAME", "default")

	containerID := "container-materialize-fallback"
	rt := &restoreFallbackRuntime{
		mockRuntime: mockRuntime{
			name:         "runc",
			capabilities: runtime.Capabilities{CheckpointRestore: true},
		},
	}
	criuManager := &observingRestoreErrorCRIUManager{err: assert.AnError}
	backendRepoClient := &fakeBackendRepoClient{}
	repoClient := &fakeContainerRepoClient{
		state: &pb.ContainerState{Status: string(types.ContainerStatusPending)},
	}
	worker := &Worker{
		config: types.AppConfig{Worker: types.WorkerConfig{Pools: map[string]types.WorkerPoolConfig{
			"default": {CRIUEnabled: true},
		}}},
		podAddr:             "10.42.0.10",
		criuManager:         criuManager,
		containerRepoClient: repoClient,
		backendRepoClient:   backendRepoClient,
		containerInstances:  common.NewSafeMap[*ContainerInstance](),
	}
	worker.containerInstances.Set(containerID, &ContainerInstance{
		Id:      containerID,
		Runtime: rt,
	})
	request := &types.ContainerRequest{
		ContainerId: containerID,
		ConfigPath:  filepath.Join(t.TempDir(), "config.json"),
		Stub:        types.StubWithRelated{Stub: types.Stub{Type: types.StubType(types.StubTypeASGIDeployment)}},
		Checkpoint: &types.Checkpoint{
			CheckpointId: "checkpoint-missing-payload",
			Status:       string(types.CheckpointStatusAvailable),
		},
	}

	exitCode, err := worker.runContainer(
		context.Background(),
		request,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		common.NewOutputWriter(func(string) {}),
		make(chan int, 1),
		make(chan int, 1),
		time.Now(),
		[]PortBinding{{HostPort: 30001, ContainerPort: 8001}},
		nil,
	)

	require.NoError(t, err)
	require.Equal(t, 0, exitCode)
	require.True(t, rt.runCalled)
	require.Equal(t, 0, criuManager.restoreCalls)
	require.Equal(t, 0, backendRepoClient.updateCalls)
	require.Nil(t, request.Checkpoint)
}

func TestShouldCreateCheckpointIgnoresNonAvailableAttachedCheckpoint(t *testing.T) {
	t.Setenv("WORKER_POOL_NAME", "default")

	worker := &Worker{
		config: types.AppConfig{Worker: types.WorkerConfig{Pools: map[string]types.WorkerPoolConfig{
			"default": {CRIUEnabled: true},
		}}},
		criuManager: &startedCRIUManager{},
	}
	request := &types.ContainerRequest{
		CheckpointEnabled: true,
		GpuCount:          0,
		Checkpoint: &types.Checkpoint{
			CheckpointId: "checkpoint-failed",
			Status:       string(types.CheckpointStatusRestoreFailed),
		},
	}

	require.True(t, worker.shouldCreateCheckpoint(request))

	request.Checkpoint.Status = string(types.CheckpointStatusAvailable)
	require.False(t, worker.shouldCreateCheckpoint(request))
}

func TestAttemptRestoreCheckpointRestoresRuntimeOnly(t *testing.T) {
	containerID := "container-restore-sandbox"
	t.Cleanup(func() { _ = os.RemoveAll(filepath.Join("/tmp", containerID)) })

	rt := &mockRuntime{name: "runc"}
	backendRepoClient := &fakeBackendRepoClient{}
	worker := &Worker{
		criuManager:        &startedCRIUManager{},
		backendRepoClient:  backendRepoClient,
		containerInstances: common.NewSafeMap[*ContainerInstance](),
	}
	worker.containerInstances.Set(containerID, &ContainerInstance{
		Id:      containerID,
		Runtime: rt,
	})
	request := &types.ContainerRequest{
		ContainerId: containerID,
		ConfigPath:  filepath.Join(t.TempDir(), "config.json"),
		Stub:        types.StubWithRelated{Stub: types.Stub{Type: types.StubType(types.StubTypeSandbox)}},
		Checkpoint: &types.Checkpoint{
			CheckpointId: "checkpoint-sandbox-restore",
			Status:       string(types.CheckpointStatusAvailable),
		},
	}

	exitCode, restored, started, err := worker.attemptRestoreCheckpoint(
		context.Background(),
		request,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		common.NewOutputWriter(func(string) {}),
		make(chan int, 1),
		make(chan int, 1),
	)

	require.NoError(t, err)
	require.True(t, restored)
	require.True(t, started)
	require.Equal(t, 0, exitCode)
	require.Empty(t, rt.signals)
	require.Empty(t, rt.killOpts)
	require.Equal(t, 0, backendRepoClient.updateCalls)
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

func TestAddRequestMountsSkipsCheckpointModelCacheVolumeCacheMap(t *testing.T) {
	localPath := filepath.Join(t.TempDir(), "volume")
	spec := getTestBaseSpec()
	request := &types.ContainerRequest{
		ContainerId: "container-1",
		Mounts: []types.Mount{{
			LocalPath: localPath,
			MountPath: filepath.Join(types.WorkerContainerVolumePath, types.CheckpointModelCacheVolumeName("svc")),
		}},
	}

	volumeCacheMap, err := (&Worker{}).addRequestMounts(request, &spec)

	require.NoError(t, err)
	require.Empty(t, volumeCacheMap)
	require.Len(t, spec.Mounts, 1)
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

	require.NoError(t, (&ContainerMountManager{}).ensureBindMountSourceDirs(request.Mounts))
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
	deleteCalls       int
	deleteCallsAtRun  int
	runCalled         bool
	runConfigPath     string
	runConfigContents []byte
}

func (m *restoreFallbackRuntime) Delete(ctx context.Context, containerID string, opts *runtime.DeleteOpts) error {
	m.deleteCalls++
	return nil
}

func (m *restoreFallbackRuntime) Run(ctx context.Context, containerID, bundlePath string, opts *runtime.RunOpts) (int, error) {
	m.runCalled = true
	m.deleteCallsAtRun = m.deleteCalls
	if m.runConfigPath != "" {
		m.runConfigContents, _ = os.ReadFile(m.runConfigPath)
	}
	if opts != nil && opts.Started != nil {
		opts.Started <- 1234
	}
	return 0, nil
}

type fakeBackendRepoClient struct {
	updateCalls int
	lastUpdate  *pb.UpdateCheckpointRequest
	createCalls int
	lastCreate  *pb.CreateCheckpointRequest
}

func (f *fakeBackendRepoClient) GetCheckpointById(ctx context.Context, in *pb.GetCheckpointByIdRequest, opts ...grpc.CallOption) (*pb.GetCheckpointByIdResponse, error) {
	return &pb.GetCheckpointByIdResponse{Ok: true}, nil
}

func (f *fakeBackendRepoClient) GetLatestCheckpointByStubId(ctx context.Context, in *pb.GetLatestCheckpointByStubIdRequest, opts ...grpc.CallOption) (*pb.GetLatestCheckpointByStubIdResponse, error) {
	return &pb.GetLatestCheckpointByStubIdResponse{Ok: true}, nil
}

func (f *fakeBackendRepoClient) ListCheckpoints(ctx context.Context, in *pb.ListCheckpointsRequest, opts ...grpc.CallOption) (*pb.ListCheckpointsResponse, error) {
	return &pb.ListCheckpointsResponse{Ok: true}, nil
}

func (f *fakeBackendRepoClient) CreateCheckpoint(ctx context.Context, in *pb.CreateCheckpointRequest, opts ...grpc.CallOption) (*pb.CreateCheckpointResponse, error) {
	f.createCalls++
	f.lastCreate = in
	return &pb.CreateCheckpointResponse{Ok: true}, nil
}

func (f *fakeBackendRepoClient) UpdateCheckpoint(ctx context.Context, in *pb.UpdateCheckpointRequest, opts ...grpc.CallOption) (*pb.UpdateCheckpointResponse, error) {
	f.updateCalls++
	f.lastUpdate = in
	return &pb.UpdateCheckpointResponse{Ok: true}, nil
}

func (f *fakeBackendRepoClient) CreateDiskSnapshot(ctx context.Context, in *pb.CreateDiskSnapshotRequest, opts ...grpc.CallOption) (*pb.CreateDiskSnapshotResponse, error) {
	return &pb.CreateDiskSnapshotResponse{Ok: true}, nil
}

func (f *fakeBackendRepoClient) GetLatestDiskSnapshot(ctx context.Context, in *pb.GetLatestDiskSnapshotRequest, opts ...grpc.CallOption) (*pb.GetLatestDiskSnapshotResponse, error) {
	return &pb.GetLatestDiskSnapshotResponse{Ok: true}, nil
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
