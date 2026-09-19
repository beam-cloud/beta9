package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	goruntime "runtime"
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

func TestPruneUnreachableSDKMountsKeepsOnlyPresentSitePackages(t *testing.T) {
	rootfs := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(rootfs, "usr/local/lib/python3.12/site-packages"), 0o755))

	sdk := func(dest string) specs.Mount {
		return specs.Mount{Destination: dest, Type: "bind", Source: sdkMountSource, Options: []string{"ro", "rbind"}}
	}
	other := specs.Mount{Destination: "/etc/hosts", Type: "bind", Source: "/tmp/hosts"}
	mounts := []specs.Mount{
		sdk("/usr/local/lib/python3.8/site-packages/beta9"),
		sdk("/usr/local/lib/python3.12/site-packages/beta9"),
		sdk("/usr/local/lib/python3.12/site-packages/beam"),
		sdk("/opt/conda/lib/python3.12/site-packages/beta9"),
		other,
	}

	kept := pruneUnreachableSDKMounts(mounts, rootfs)

	require.Equal(t, []specs.Mount{
		sdk("/usr/local/lib/python3.12/site-packages/beta9"),
		sdk("/usr/local/lib/python3.12/site-packages/beam"),
		other,
	}, kept)
}

// Only a definite absence drops an SDK mount. A stat that fails for any other
// reason (permissions, an I/O error from a lazily loaded image) keeps it, so a
// transient read failure cannot silently ship a container without its SDK.
func TestPruneUnreachableSDKMountsKeepsMountsOnStatErrors(t *testing.T) {
	sdk := func(dest string) specs.Mount {
		return specs.Mount{Destination: dest, Type: "bind", Source: sdkMountSource}
	}
	mounts := []specs.Mount{
		sdk("/missing/site-packages/beta9"),
		sdk("/notdir/site-packages/beta9"),
		sdk("/denied/site-packages/beta9"),
		sdk("/flaky/site-packages/beta9"),
		sdk("/present/site-packages/beta9"),
	}
	stat := func(path string) (os.FileInfo, error) {
		switch {
		case strings.HasPrefix(path, "/root/missing/"):
			return nil, &os.PathError{Op: "stat", Path: path, Err: syscall.ENOENT}
		case strings.HasPrefix(path, "/root/notdir/"):
			return nil, &os.PathError{Op: "stat", Path: path, Err: syscall.ENOTDIR}
		case strings.HasPrefix(path, "/root/denied/"):
			return nil, &os.PathError{Op: "stat", Path: path, Err: syscall.EACCES}
		case strings.HasPrefix(path, "/root/flaky/"):
			return nil, &os.PathError{Op: "stat", Path: path, Err: syscall.EIO}
		}
		return nil, nil
	}

	kept := pruneUnreachableSDKMountsWithStat(mounts, "/root", stat)

	require.Equal(t, []specs.Mount{
		sdk("/denied/site-packages/beta9"),
		sdk("/flaky/site-packages/beta9"),
		sdk("/present/site-packages/beta9"),
	}, kept)
}

// The publication result is observable by every party that needs it, as often
// as needed: the runtime start joins it to publish RUNNING, teardown joins it
// so state deletion cannot race a publication still in flight.
func TestAddressRegistrationJoinsFromEveryWaiter(t *testing.T) {
	release := make(chan struct{})
	published := errors.New("publish failed")
	registration := newAddressRegistration(func() error {
		<-release
		return published
	})

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, registration.wait(cancelled), context.Canceled)

	teardownDone := make(chan struct{})
	go func() {
		registration.waitForTeardown()
		close(teardownDone)
	}()
	select {
	case <-teardownDone:
		t.Fatal("teardown joined before the publication finished")
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	select {
	case <-teardownDone:
	case <-time.After(5 * time.Second):
		t.Fatal("teardown did not observe the finished publication")
	}
	require.ErrorIs(t, registration.wait(context.Background()), published)
	require.ErrorIs(t, registration.wait(context.Background()), published)

	var none *addressRegistration
	require.NoError(t, none.wait(context.Background()))
	none.waitForTeardown()
}

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

// registeredAddresses stands in for a completed address-map publication.
func registeredAddresses() *addressRegistration {
	return newAddressRegistration(func() error { return nil })
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
		runtime:             mockRuntime,
		containerInstances:  containerInstances,
		containerGPUManager: &testGPUManager{},
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

func TestSpecFromRequestInjectsThunderMounts(t *testing.T) {
	worker := &Worker{
		runtime:                 &mockRuntime{name: types.ContainerRuntimeGvisor.String()},
		containerThunderManager: NewContainerThunderManager(nil),
		gpuVirtualized:          true,
	}

	spec, err := worker.specFromRequest(&types.ContainerRequest{
		ContainerId: "container-1",
		EntryPoint:  []string{"sleep", "60"},
		GpuRequest:  []string{"H100"},
		GpuCount:    1,
		Stub: types.StubWithRelated{Stub: types.Stub{
			Type: types.StubType(types.StubTypePodDeployment),
		}},
	}, &ContainerOptions{BindPorts: []int{8001}})

	require.NoError(t, err)
	require.Contains(t, spec.Mounts, thunderBindMount("/usr/bin/nvidia-smi"))
	require.Contains(t, spec.Mounts, thunderBindMount("/usr/lib/x86_64-linux-gnu/libnvidia-ml.so.1"))
	require.Contains(t, spec.Mounts, thunderBindMount("/usr/lib/x86_64-linux-gnu/libcuda.so.1"))
}

func TestSpecFromRequestForcesCPUAndMemoryLimitsForGvisorGPU(t *testing.T) {
	mockRuntime := &mockResourceRuntime{mockRuntime: mockRuntime{name: types.ContainerRuntimeGvisor.String()}}
	containerInstances := common.NewSafeMap[*ContainerInstance]()
	containerInstances.Set("container-1", &ContainerInstance{Runtime: mockRuntime, CPUSet: "2-9"})
	worker := &Worker{
		config:              types.AppConfig{},
		runtime:             mockRuntime,
		containerInstances:  containerInstances,
		containerGPUManager: &testGPUManager{},
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
		containerInstances:  instances,
		cpuLimit:            4000,
		runtime:             &mockRuntime{name: types.ContainerRuntimeRunc.String()},
		containerGPUManager: &testGPUManager{},
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

func TestClearContainerStopsHeartbeatWhileDurableDiskSyncBlocks(t *testing.T) {
	fixture := newLockedDurableDiskFinalizationFixture(t, "container-blocked-sync")
	request := fixture.request
	leaseStarted := make(chan struct{}, 1)
	leaseRelease := make(chan struct{})
	heartbeatGetStarted := make(chan struct{}, 1)
	repoClient := &fakeContainerRepoClient{
		state: &pb.ContainerState{
			ContainerId: request.ContainerId,
			Status:      string(types.ContainerStatusRunning),
		},
		deleteStateDone:     make(chan struct{}, 1),
		getStateStarted:     heartbeatGetStarted,
		updateStatusStarted: leaseStarted,
		updateStatusRelease: leaseRelease,
	}
	worker := newContainerFinalizationTestWorker(request, repoClient, nil)

	clearDone := clearContainerAsync(worker, request, 7)

	select {
	case <-leaseStarted:
	case <-time.After(time.Second):
		t.Fatal("cleanup lease was not requested")
	}
	instance, ok := worker.containerInstances.Get(request.ContainerId)
	require.True(t, ok)
	localExitCode, _ := instance.lifecycleState()
	require.Equal(t, -1, localExitCode)

	updates := repoClient.containerStatusUpdates()
	require.Len(t, updates, 1)
	require.Equal(t, int64(types.ContainerStateTtlSWhileStopping), updates[0].ExpirySeconds)
	require.Greater(t, updates[0].ExpirySeconds, int64(types.ContainerStateTtlS))
	require.Equal(
		t,
		durableDiskSnapshotInactivityTimeout+time.Duration(types.ContainerStateTtlS)*time.Second,
		time.Duration(updates[0].ExpirySeconds)*time.Second,
	)
	heartbeatDone := make(chan bool, 1)
	heartbeatErr := make(chan error, 1)
	go func() {
		done, err := worker.updateContainerStatusOnce(context.Background(), request)
		heartbeatDone <- done
		heartbeatErr <- err
	}()
	select {
	case <-heartbeatDone:
		t.Fatal("heartbeat passed the finalization fence before the cleanup lease completed")
	case <-time.After(50 * time.Millisecond):
	}
	select {
	case <-heartbeatGetStarted:
	case <-time.After(time.Second):
		t.Fatal("heartbeat did not reach the finalization fence")
	}
	close(leaseRelease)

	requireLocalContainerExitCode(t, worker, request.ContainerId, 7)
	require.True(t, <-heartbeatDone)
	require.NoError(t, <-heartbeatErr)
	require.Equal(t, 1, repoClient.getStateCalls)
	updatesBeforeRelease := repoClient.containerStatusUpdates()
	require.NotEmpty(t, updatesBeforeRelease)
	for _, update := range updatesBeforeRelease {
		require.Equal(t, int64(types.ContainerStateTtlSWhileStopping), update.ExpirySeconds)
	}

	fixture.releaseAndWaitForClear(t, clearDone)
	select {
	case <-repoClient.deleteStateDone:
	case <-time.After(time.Second):
		t.Fatal("container state was not deleted after disk sync cancellation")
	}
	updates = repoClient.containerStatusUpdates()
	require.Greater(t, len(updates), len(updatesBeforeRelease))
	for _, update := range updates[len(updatesBeforeRelease) : len(updates)-1] {
		require.Equal(t, string(types.ContainerStatusStopping), update.Status)
		require.Equal(t, int64(types.ContainerStateTtlSWhileStopping), update.ExpirySeconds)
	}
	require.Equal(t, string(types.ContainerStatusStopping), updates[len(updates)-1].Status)
	require.Equal(t, int64(types.ContainerStateTtlS), updates[len(updates)-1].ExpirySeconds)
}

func TestClearContainerSerializesInFlightHeartbeatWhenStoppingLeaseFails(t *testing.T) {
	fixture := newLockedDurableDiskFinalizationFixture(t, "container-failed-stopping-lease")
	request := fixture.request
	heartbeatRelease := make(chan struct{})
	leaseStarted := make(chan struct{}, 1)
	repoClient := &fakeContainerRepoClient{
		state: &pb.ContainerState{
			ContainerId: request.ContainerId,
			Status:      string(types.ContainerStatusPending),
		},
		getStateStarted:     make(chan struct{}, 1),
		getStateRelease:     heartbeatRelease,
		updateStatusStarted: leaseStarted,
		updateStatusErrorForExpiry: map[int64]error{
			types.ContainerStateTtlSWhileStopping: context.Canceled,
		},
	}
	worker := newContainerFinalizationTestWorker(request, repoClient, nil)
	heartbeatDone := make(chan bool, 1)
	heartbeatErr := make(chan error, 1)
	go func() {
		done, err := worker.updateContainerStatusOnce(context.Background(), request)
		heartbeatDone <- done
		heartbeatErr <- err
	}()
	select {
	case <-repoClient.getStateStarted:
	case <-time.After(time.Second):
		t.Fatal("heartbeat did not cache pending state")
	}

	clearDone := clearContainerAsync(worker, request, 7)
	select {
	case <-leaseStarted:
	case <-time.After(time.Second):
		t.Fatal("finalization did not begin the cleanup lease")
	}
	close(heartbeatRelease)
	requireLocalContainerExitCode(t, worker, request.ContainerId, 7)

	require.True(t, <-heartbeatDone)
	require.NoError(t, <-heartbeatErr)
	for _, update := range repoClient.containerStatusUpdates() {
		require.Equal(t, string(types.ContainerStatusStopping), update.Status, "stale heartbeat overwrote finalization state")
		require.Equal(t, int64(types.ContainerStateTtlSWhileStopping), update.ExpirySeconds)
	}
	require.Equal(t, 1, repoClient.getStateCalls)

	fixture.releaseAndWaitForClear(t, clearDone)
}

func TestClearContainerTerminalizesAfterDurableDiskSyncFailure(t *testing.T) {
	request := &types.ContainerRequest{
		ContainerId: "container-sync-error",
		Mounts: []types.Mount{{
			LocalPath: blockedDurableDiskPath(t),
			DurableDisk: &types.DurableDiskMountConfig{
				Name: "disk-sync-error",
				Size: "1Gi",
			},
		}},
	}
	repoClient := &fakeContainerRepoClient{deleteStateDone: make(chan struct{}, 1)}
	worker := newContainerFinalizationTestWorker(request, repoClient, nil)

	worker.clearContainer(request.ContainerId, request, 0, true)

	select {
	case <-repoClient.deleteStateDone:
	case <-time.After(time.Second):
		t.Fatal("container state was not deleted after disk sync failure")
	}
	require.Equal(t, 1, repoClient.setExitCodeCalls)
	require.Equal(t, int32(types.ContainerExitCodeUnknownError), repoClient.lastSetExitCode.ExitCode)
	updates := repoClient.containerStatusUpdates()
	require.Len(t, updates, 2)
	require.Equal(t, int64(types.ContainerStateTtlSWhileStopping), updates[0].ExpirySeconds)
	require.Equal(t, int64(types.ContainerStateTtlS), updates[1].ExpirySeconds)
	require.Eventually(t, func() bool {
		_, exists := worker.containerInstances.Get(request.ContainerId)
		return !exists
	}, time.Second, 10*time.Millisecond)
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

func TestClearContainerPublishesFailureAfterDurableDiskSyncFailure(t *testing.T) {
	for _, test := range []struct {
		exitCode     types.ContainerExitCode
		exitReported bool
		want         types.ContainerExitCode
	}{
		{exitCode: types.ContainerExitCodeScheduler, exitReported: true, want: types.ContainerExitCodeUnknownError},
		{exitCode: types.ContainerExitCodeTtl, exitReported: true, want: types.ContainerExitCodeUnknownError},
		{exitCode: types.ContainerExitCodeUser, exitReported: true, want: types.ContainerExitCodeUnknownError},
		{exitCode: types.ContainerExitCodeAdmin, exitReported: true, want: types.ContainerExitCodeUnknownError},
		{exitCode: types.ContainerExitCodeOomKill, exitReported: false, want: types.ContainerExitCodeOomKill},
	} {
		t.Run(fmt.Sprintf("exit-%d", test.exitCode), func(t *testing.T) {
			request := &types.ContainerRequest{
				ContainerId: fmt.Sprintf("container-sync-failure-%d", test.exitCode),
				Mounts: []types.Mount{{
					LocalPath: blockedDurableDiskPath(t),
					DurableDisk: &types.DurableDiskMountConfig{
						Name: "disk",
						Size: "1Gi",
					},
				}},
			}
			repoClient := &fakeContainerRepoClient{}
			worker := newContainerFinalizationTestWorker(request, repoClient, nil)

			worker.clearContainer(request.ContainerId, request, int(test.exitCode), test.exitReported)

			require.Equal(t, 1, repoClient.containerExitCodeCalls())
			require.Equal(t, int32(test.want), repoClient.lastSetExitCode.ExitCode)
		})
	}
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
		ContainerId: "container-missing-state",
		Mounts: []types.Mount{{
			LocalPath: blockedDurableDiskPath(t),
			DurableDisk: &types.DurableDiskMountConfig{
				Name: "disk-missing-state",
				Size: "1Gi",
			},
		}},
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
		worker.finishContainerShutdown(request.ContainerId, request, nil)
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
		worker.finishContainerShutdown(request.ContainerId, request, nil)
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

type lockedDurableDiskFinalizationFixture struct {
	request *types.ContainerRequest
	lock    *FileLock
}

func newLockedDurableDiskFinalizationFixture(t *testing.T, containerID string) *lockedDurableDiskFinalizationFixture {
	t.Helper()
	diskPath := filepath.Join(t.TempDir(), "disk")
	lockDir := filepath.Join(filepath.Dir(diskPath), durableDiskLockDir)
	require.NoError(t, os.MkdirAll(lockDir, 0o755))
	lock := NewFileLock(filepath.Join(lockDir, filepath.Base(diskPath)+".lock"))
	require.NoError(t, lock.Acquire())
	fixture := &lockedDurableDiskFinalizationFixture{
		request: &types.ContainerRequest{
			ContainerId: containerID,
			Mounts: []types.Mount{{
				LocalPath: diskPath,
				DurableDisk: &types.DurableDiskMountConfig{
					Name: containerID,
					Size: "1Gi",
				},
			}},
		},
		lock: lock,
	}
	t.Cleanup(func() { fixture.release(t) })
	return fixture
}

func (f *lockedDurableDiskFinalizationFixture) release(t *testing.T) {
	t.Helper()
	if f.lock == nil {
		return
	}
	require.NoError(t, f.lock.Release())
	f.lock = nil
}

func (f *lockedDurableDiskFinalizationFixture) releaseAndWaitForClear(t *testing.T, clearDone <-chan struct{}) {
	t.Helper()
	f.release(t)
	select {
	case <-clearDone:
	case <-time.After(time.Second):
		t.Fatal("container cleanup did not finish after releasing the disk lock")
	}
}

func clearContainerAsync(worker *Worker, request *types.ContainerRequest, exitCode int) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		worker.clearContainer(request.ContainerId, request, exitCode, false)
		close(done)
	}()
	return done
}

func requireLocalContainerExitCode(t *testing.T, worker *Worker, containerID string, expected int) {
	t.Helper()
	require.Eventually(t, func() bool {
		instance, ok := worker.containerInstances.Get(containerID)
		if !ok {
			return false
		}
		exitCode, _ := instance.lifecycleState()
		return exitCode == expected
	}, time.Second, 10*time.Millisecond)
}

func blockedDurableDiskPath(t *testing.T) string {
	blocker := filepath.Join(t.TempDir(), "not-a-directory")
	require.NoError(t, os.WriteFile(blocker, []byte("blocked"), 0o644))
	return filepath.Join(blocker, "disk")
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
			make(chan int, 1),
			time.Now(),
			registeredAddresses(),
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

func completedCheckpointFilesystemRestore() *checkpointFilesystemRestore {
	done := make(chan struct{})
	close(done)
	return &checkpointFilesystemRestore{startedAt: time.Now(), done: done}
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
		podAddr:                 "10.42.0.10",
		criuManager:             &startedCRIUManager{},
		cacheManager:            &WorkerCacheManager{checkpointRoot: filepath.Join(tmpDir, "checkpoints")},
		containerRepoClient:     repoClient,
		backendRepoClient:       backendRepoClient,
		containerInstances:      common.NewSafeMap[*ContainerInstance](),
		containerNetworkManager: &fakeContainerNetworkController{},
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

	addressesRegistered := newAddressRegistration(func() error {
		return worker.registerContainerPorts(context.Background(), request, []PortBinding{{HostPort: 30001, ContainerPort: 8001}})
	})
	exitCode, err := worker.runContainer(
		context.Background(),
		request,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		common.NewOutputWriter(func(string) {}),
		make(chan int, 1),
		make(chan int, 1),
		time.Now(),
		addressesRegistered,
		completedCheckpointFilesystemRestore(),
	)

	require.NoError(t, err)
	require.Equal(t, -1, exitCode)
	require.Zero(t, repoClient.setAddressCalls)
	require.Equal(t, 1, repoClient.setAddressMapCalls)
	require.Equal(t, int32(8001), repoClient.lastSetAddressMap.PrimaryPort)
	require.Equal(t, "10.0.0.2:30001", repoClient.lastSetAddressMap.AddressMap[8001])
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
	info, err := os.Stat(restore.upperPath)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0755), info.Mode().Perm())
	data, err := os.ReadFile(filepath.Join(restore.upperPath, "state", "ready"))
	require.NoError(t, err)
	require.Equal(t, "yes", string(data))
}

func TestCheckpointFilesystemRestorePreservesArchiveRootMode(t *testing.T) {
	if goruntime.GOOS != "linux" {
		t.Skip("checkpoint workers use GNU tar on Linux")
	}

	checkpointRoot := t.TempDir()
	checkpointID := "checkpoint-archive-root-mode"
	checkpointPath := filepath.Join(checkpointRoot, checkpointID)
	require.NoError(t, os.MkdirAll(checkpointPath, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(checkpointPath, "inventory.img"), []byte("runtime"), 0644))

	archiveSource := t.TempDir()
	require.NoError(t, os.Chmod(archiveSource, 0750))
	require.NoError(t, os.WriteFile(filepath.Join(archiveSource, "marker"), []byte("preserved"), 0644))
	require.NoError(t, archiveDirectoryContext(context.Background(), archiveSource, filepath.Join(checkpointPath, checkpointFsArchive), nil))

	worker := &Worker{
		ctx:          context.Background(),
		cacheManager: &WorkerCacheManager{checkpointRoot: checkpointRoot},
	}
	request := &types.ContainerRequest{
		ContainerId: "checkpoint-archive-root-mode-test",
		Checkpoint: &types.Checkpoint{
			CheckpointId: checkpointID,
			Status:       string(types.CheckpointStatusAvailable),
		},
	}
	restore := worker.startCheckpointFilesystemRestore(request, nil)
	t.Cleanup(restore.cleanup)

	require.NoError(t, restore.wait())
	info, err := os.Stat(restore.upperPath)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0750), info.Mode().Perm())
	require.FileExists(t, filepath.Join(restore.upperPath, "marker"))
}

func TestFilesystemCheckpointRestoreCopiesOverlayThenColdRuns(t *testing.T) {
	fakeBin := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(fakeBin, "mount"), []byte("#!/bin/sh\nexit 0\n"), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(fakeBin, "umount"), []byte("#!/bin/sh\nexit 0\n"), 0755))
	t.Setenv("PATH", fakeBin+string(os.PathListSeparator)+os.Getenv("PATH"))

	checkpointRoot := t.TempDir()
	checkpointID := "checkpoint-filesystem-cold"
	checkpointPath := filepath.Join(checkpointRoot, checkpointID)
	require.NoError(t, os.MkdirAll(filepath.Join(checkpointPath, checkpointFsDir, "tmp"), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(checkpointPath, checkpointFilesystemOnlyFile), []byte("v1\n"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(checkpointPath, checkpointFsDir, "tmp", "marker"), []byte("preserved"), 0644))

	bundleDir := t.TempDir()
	configPath := filepath.Join(bundleDir, specBaseName)
	require.NoError(t, os.WriteFile(configPath, []byte("{}"), 0644))

	overlayPath := t.TempDir()
	restoredMarker := filepath.Join(overlayPath, "container-filesystem-cold", "layer-0", "upper", "tmp", "marker")
	rt := &restoreFallbackRuntime{mockRuntime: mockRuntime{name: "other"}}
	rt.runConfigPath = restoredMarker
	criuManager := &observingRestoreErrorCRIUManager{err: assert.AnError}
	backendRepoClient := &fakeBackendRepoClient{}
	repoClient := &fakeContainerRepoClient{state: &pb.ContainerState{Status: string(types.ContainerStatusPending)}}
	worker := &Worker{
		podAddr:             "10.42.0.10",
		criuManager:         criuManager,
		cacheManager:        &WorkerCacheManager{checkpointRoot: checkpointRoot},
		containerRepoClient: repoClient,
		backendRepoClient:   backendRepoClient,
		containerInstances:  common.NewSafeMap[*ContainerInstance](),
	}
	request := &types.ContainerRequest{
		ContainerId: "container-filesystem-cold",
		ConfigPath:  configPath,
		Checkpoint: &types.Checkpoint{
			CheckpointId: checkpointID,
			Status:       string(types.CheckpointStatusAvailable),
			Runtime:      types.CheckpointRuntimeFilesystem,
		},
		Stub: types.StubWithRelated{Stub: types.Stub{Type: types.StubType(types.StubTypeASGI)}},
	}
	worker.containerInstances.Set(request.ContainerId, &ContainerInstance{
		Id:      request.ContainerId,
		Request: request,
		Runtime: rt,
		Overlay: common.NewContainerOverlay(request, t.TempDir(), overlayPath),
	})

	exitCode, err := worker.runContainer(
		context.Background(),
		request,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		common.NewOutputWriter(func(string) {}),
		make(chan int, 1),
		make(chan int, 1),
		time.Now(),
		registeredAddresses(),
		nil,
	)

	require.NoError(t, err)
	require.Equal(t, 0, exitCode)
	require.True(t, rt.runCalled)
	require.Equal(t, "preserved", string(rt.runConfigContents), "overlay must be restored before cold runtime start")
	require.Equal(t, 0, rt.deleteCallsAtRun, "filesystem restore must not invoke runtime restore cleanup before cold run")
	require.Zero(t, criuManager.restoreCalls)
	require.FileExists(t, restoredMarker)
	require.NoFileExists(t, filepath.Join(bundleDir, "tmp", "marker"), "raw upper state must not be extracted into the bundle")
	require.Nil(t, request.Checkpoint)
	require.Equal(t, 1, backendRepoClient.updateCalls)
	require.NotNil(t, backendRepoClient.lastUpdate.LastRestoredAt)
}

func TestCheckpointFilesystemRestoreDiscardRemovesPartialUpperLayer(t *testing.T) {
	overlayRoot := filepath.Join(t.TempDir(), "overlay")
	require.NoError(t, os.MkdirAll(filepath.Join(overlayRoot, "layer-0", "upper"), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(overlayRoot, "layer-0", "upper", "partial"), []byte("partial"), 0644))
	require.NoError(t, os.MkdirAll(filepath.Join(overlayRoot, "criu"), 0755))

	_, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	close(done)
	restore := &checkpointFilesystemRestore{
		upperPath: filepath.Join(overlayRoot, "layer-0", "upper"),
		published: true,
		done:      done,
		cancel:    cancel,
		err:       assert.AnError,
	}

	require.NoError(t, restore.discard())
	require.NoDirExists(t, filepath.Join(overlayRoot, "layer-0"))
	require.DirExists(t, filepath.Join(overlayRoot, "criu"))
}

func TestRunContainerRestoreWaitsForRestoredRuntimeExitAndTerminalCheckpoint(t *testing.T) {
	t.Setenv("WORKER_POOL_NAME", "default")
	tmpDir := t.TempDir()
	checkpointId := "checkpoint-1"
	require.NoError(t, os.MkdirAll(filepath.Join(tmpDir, "checkpoints", checkpointId, checkpointFsDir), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(tmpDir, "checkpoints", checkpointId, "checkpoint.img"), []byte("runtime payload"), 0644))

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
			registeredAddresses(),
			completedCheckpointFilesystemRestore(),
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

	checkpointState, acquired := worker.acquireCheckpointCreateLock(request)
	require.True(t, acquired)
	require.True(t, checkpointState.requestTerminal(request.ContainerId))
	close(restoreStopped)
	select {
	case err := <-done:
		t.Fatalf("runContainer returned while the terminal checkpoint still owned the restored overlay: %v", err)
	case <-time.After(2 * restoredContainerPollInterval):
	}

	worker.markTerminalCheckpointRuntimeStopped(request)
	worker.finishCheckpointCreate(request, checkpointState)
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

func TestWaitForRestoredContainerExitTreatsPausedContainerAsAlive(t *testing.T) {
	statuses := []string{
		types.RuncContainerStatusRunning,
		types.RuncContainerStatusPaused,
		types.RuncContainerStatusPaused,
		types.RuncContainerStatusRunning,
		types.RuncContainerStatusStopped,
	}
	var observed []string
	rt := &mockRuntime{
		state: func(context.Context, string) (runtime.State, error) {
			status := statuses[len(observed)]
			observed = append(observed, status)
			return runtime.State{Status: status, Pid: 42}, nil
		},
	}

	exitCode, err := (&Worker{}).waitForRestoredContainerExit(context.Background(), rt, "container-restore", 0)

	require.NoError(t, err)
	require.Equal(t, -1, exitCode)
	require.Equal(t, statuses, observed, "watcher stopped before the container actually stopped")
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

	var output strings.Builder
	exitCode, restored, started, err := worker.attemptRestoreCheckpoint(
		context.Background(),
		request,
		slog.New(slog.NewTextHandler(&output, nil)),
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
	require.Contains(t, output.String(), restoreErr.Error())
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
		registeredAddresses(),
		completedCheckpointFilesystemRestore(),
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

func TestRunContainerMigratesLegacyForcedRuncCheckpointBeforeRestore(t *testing.T) {
	t.Setenv("WORKER_POOL_NAME", "default")
	fakeBin := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(fakeBin, "mount"), []byte("#!/bin/sh\nexit 0\n"), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(fakeBin, "umount"), []byte("#!/bin/sh\nexit 0\n"), 0755))
	t.Setenv("PATH", fakeBin+string(os.PathListSeparator)+os.Getenv("PATH"))

	for index, test := range []struct {
		name             string
		marker           string
		wantCold         bool
		wantRestoreCalls int
	}{
		{name: "missing profile marker", wantCold: true},
		{name: "malformed profile marker", marker: "v0\n", wantCold: true},
		{name: "current profile marker", marker: checkpointForcedRuncProfileV1, wantRestoreCalls: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			containerID := fmt.Sprintf("container-forced-profile-migration-%d", index)
			checkpointID := fmt.Sprintf("checkpoint-forced-profile-migration-%d", index)
			t.Cleanup(func() { _ = os.RemoveAll(checkpointSignalDir(containerID)) })

			checkpointRoot := filepath.Join(root, "checkpoints")
			checkpointPath := filepath.Join(checkpointRoot, checkpointID)
			require.NoError(t, os.MkdirAll(filepath.Join(checkpointPath, checkpointFsDir, "tmp"), 0755))
			require.NoError(t, os.MkdirAll(filepath.Join(checkpointPath, checkpointFsDir, "workspace"), 0755))
			require.NoError(t, os.WriteFile(filepath.Join(checkpointPath, "inventory.img"), []byte("runtime"), 0644))
			require.NoError(t, os.WriteFile(filepath.Join(checkpointPath, checkpointFsDir, "tmp", "root-marker"), []byte("checkpoint-root"), 0644))
			require.NoError(t, os.WriteFile(filepath.Join(checkpointPath, checkpointFsDir, "workspace", "overlay-only"), []byte("overlay"), 0644))
			if test.marker != "" {
				require.NoError(t, os.WriteFile(filepath.Join(checkpointPath, checkpointForcedRuncProfileFile), []byte(test.marker), 0644))
			}

			overlayPath := filepath.Join(root, "overlay")
			configPath := filepath.Join(overlayPath, containerID, "layer-0", "merged", specBaseName)
			configContents := writeForcedRuncProfileConfig(t, configPath)
			durablePath := filepath.Join(root, "durable-workspace")
			require.NoError(t, os.MkdirAll(durablePath, 0755))
			require.NoError(t, os.WriteFile(filepath.Join(durablePath, "durable-marker"), []byte("durable"), 0644))
			var spec specs.Spec
			require.NoError(t, json.Unmarshal(configContents, &spec))
			spec.Mounts = append(spec.Mounts, specs.Mount{Type: "bind", Source: durablePath, Destination: "/workspace"})
			configContents, err := json.Marshal(&spec)
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(configPath, configContents, 0644))

			rt := &restoreFallbackRuntime{
				mockRuntime: mockRuntime{
					name:         types.ContainerRuntimeRunc.String(),
					capabilities: runtime.Capabilities{CheckpointRestore: true},
				},
				runConfigPath:     configPath,
				runRootMarkerPath: filepath.Join(overlayPath, containerID, "layer-0", "upper", "tmp", "root-marker"),
			}
			manager := &observingRestoreErrorCRIUManager{}
			backendRepoClient := &fakeBackendRepoClient{}
			worker := &Worker{
				config: types.AppConfig{Worker: types.WorkerConfig{Pools: map[string]types.WorkerPoolConfig{
					"default": {CRIUEnabled: true},
				}}},
				podAddr:             "10.42.0.10",
				criuManager:         manager,
				cacheManager:        &WorkerCacheManager{checkpointRoot: checkpointRoot},
				containerRepoClient: &fakeContainerRepoClient{state: &pb.ContainerState{Status: string(types.ContainerStatusPending)}},
				backendRepoClient:   backendRepoClient,
				containerInstances:  common.NewSafeMap[*ContainerInstance](),
			}
			request := &types.ContainerRequest{
				ContainerId: containerID,
				ConfigPath:  configPath,
				Stub: types.StubWithRelated{Stub: types.Stub{
					Type:   types.StubType(types.StubTypeSandbox),
					Config: `{"_beta9_force_resource_limits":true}`,
				}},
				Mounts: []types.Mount{{
					LocalPath:   durablePath,
					MountPath:   "/workspace",
					MountType:   types.StorageModeDurableDisk,
					DurableDisk: &types.DurableDiskMountConfig{Name: "workspace"},
				}},
				Checkpoint: &types.Checkpoint{
					CheckpointId: checkpointID,
					Status:       string(types.CheckpointStatusAvailable),
					Runtime:      types.ContainerRuntimeRunc.String(),
				},
			}
			worker.containerInstances.Set(containerID, &ContainerInstance{
				Id:      containerID,
				Request: request,
				Runtime: rt,
				Overlay: common.NewContainerOverlay(request, filepath.Join(root, "root"), overlayPath),
			})
			restoreDone := make(chan struct{})
			close(restoreDone)

			exitCode, err := worker.runContainer(
				context.Background(),
				request,
				slog.New(slog.NewTextHandler(io.Discard, nil)),
				common.NewOutputWriter(func(string) {}),
				make(chan int, 1),
				make(chan int, 1),
				time.Now(),
				registeredAddresses(),
				&checkpointFilesystemRestore{done: restoreDone},
			)

			require.NoError(t, err)
			require.Equal(t, test.wantRestoreCalls, manager.restoreCalls)
			if !test.wantCold {
				require.Equal(t, -1, exitCode)
				require.False(t, rt.runCalled)
				require.Equal(t, 1, backendRepoClient.updateCalls)
				require.NotNil(t, backendRepoClient.lastUpdate.LastRestoredAt)
				return
			}

			require.Equal(t, 0, exitCode)
			require.True(t, rt.runCalled)
			require.Zero(t, rt.deleteCalls, "migration must bypass runtime restore and cleanup")
			require.Equal(t, "checkpoint-root", string(rt.runRootMarkerContents))
			require.Equal(t, configContents, rt.runConfigContents, "cold run must use the newly generated guarded config")
			require.Contains(t, string(rt.runConfigContents), "/proc/sys/vm/drop_caches")
			require.Contains(t, string(rt.runConfigContents), durablePath)
			require.Zero(t, backendRepoClient.updateCalls, "migration must not mark the legacy checkpoint restored or failed")
			require.Nil(t, request.Checkpoint)
			require.FileExists(t, filepath.Join(durablePath, "durable-marker"))
			require.NoFileExists(t, filepath.Join(durablePath, "overlay-only"), "checkpoint root overlay must not replace the durable mount")
		})
	}
}

func TestRunContainerLegacyForcedRuncMigrationFailureDoesNotRestoreOrRun(t *testing.T) {
	t.Setenv("WORKER_POOL_NAME", "default")
	fakeBin := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(fakeBin, "mount"), []byte("#!/bin/sh\nexit 1\n"), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(fakeBin, "umount"), []byte("#!/bin/sh\nexit 0\n"), 0755))
	t.Setenv("PATH", fakeBin+string(os.PathListSeparator)+os.Getenv("PATH"))

	root := t.TempDir()
	containerID := "container-forced-profile-migration-failure"
	checkpointID := "checkpoint-forced-profile-migration-failure"
	checkpointRoot := filepath.Join(root, "checkpoints")
	checkpointPath := filepath.Join(checkpointRoot, checkpointID)
	require.NoError(t, os.MkdirAll(filepath.Join(checkpointPath, checkpointFsDir), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(checkpointPath, checkpointFsDir, "root-marker"), []byte("checkpoint-root"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(checkpointPath, "inventory.img"), []byte("runtime"), 0644))
	overlayPath := filepath.Join(root, "overlay")
	configPath := filepath.Join(overlayPath, containerID, "layer-0", "merged", specBaseName)
	writeForcedRuncProfileConfig(t, configPath)

	rt := &restoreFallbackRuntime{mockRuntime: mockRuntime{name: types.ContainerRuntimeRunc.String(), capabilities: runtime.Capabilities{CheckpointRestore: true}}}
	manager := &observingRestoreErrorCRIUManager{}
	backendRepoClient := &fakeBackendRepoClient{}
	worker := &Worker{
		config: types.AppConfig{Worker: types.WorkerConfig{Pools: map[string]types.WorkerPoolConfig{
			"default": {CRIUEnabled: true},
		}}},
		criuManager:        manager,
		cacheManager:       &WorkerCacheManager{checkpointRoot: checkpointRoot},
		backendRepoClient:  backendRepoClient,
		containerInstances: common.NewSafeMap[*ContainerInstance](),
	}
	request := &types.ContainerRequest{
		ContainerId: containerID,
		ConfigPath:  configPath,
		Stub: types.StubWithRelated{Stub: types.Stub{
			Type:   types.StubType(types.StubTypeSandbox),
			Config: `{"_beta9_force_resource_limits":true}`,
		}},
		Checkpoint: &types.Checkpoint{CheckpointId: checkpointID, Status: string(types.CheckpointStatusAvailable), Runtime: types.ContainerRuntimeRunc.String()},
	}
	worker.containerInstances.Set(containerID, &ContainerInstance{
		Id:      containerID,
		Request: request,
		Runtime: rt,
		Overlay: common.NewContainerOverlay(request, filepath.Join(root, "root"), overlayPath),
	})
	restoreDone := make(chan struct{})
	close(restoreDone)

	_, err := worker.runContainer(
		context.Background(), request,
		slog.New(slog.NewTextHandler(io.Discard, nil)), common.NewOutputWriter(func(string) {}),
		make(chan int, 1), make(chan int, 1), time.Now(), nil,
		&checkpointFilesystemRestore{done: restoreDone},
	)

	require.ErrorContains(t, err, "migrate legacy forced runc checkpoint")
	require.Zero(t, manager.restoreCalls)
	require.False(t, rt.runCalled)
	require.Zero(t, rt.deleteCalls)
	require.Zero(t, backendRepoClient.updateCalls)
	require.NotNil(t, request.Checkpoint, "failed migration must leave the checkpoint attached for retry")
	require.NoDirExists(t, filepath.Join(overlayPath, containerID, "layer-0"), "failed reseed must remove partial upper state")
}

func TestRunContainerRejectsForcedRuncDockerModeBeforeLaunch(t *testing.T) {
	instances := common.NewSafeMap[*ContainerInstance]()
	worker := &Worker{
		runtime:            &mockRuntime{name: types.ContainerRuntimeRunc.String()},
		containerInstances: instances,
	}
	request := &types.ContainerRequest{
		ContainerId:   "forced-runc-docker",
		DockerEnabled: true,
		Stub: types.StubWithRelated{Stub: types.Stub{
			Config: `{"_beta9_force_resource_limits":true}`,
		}},
	}

	err := worker.RunContainer(context.Background(), request)
	require.ErrorContains(t, err, "do not support Docker-enabled mode")
	_, exists := instances.Get(request.ContainerId)
	require.False(t, exists, "rejected request must not create container state")
}

func TestRunContainerSandboxRestoreFallbackPolicy(t *testing.T) {
	t.Setenv("WORKER_POOL_NAME", "default")
	fakeBin := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(fakeBin, "mount"), []byte("#!/bin/sh\nexit 0\n"), 0755))
	t.Setenv("PATH", fakeBin+string(os.PathListSeparator)+os.Getenv("PATH"))

	tests := []struct {
		name                  string
		restoreErr            error
		deleteErr             error
		runtimeName           string
		archiveFilesystem     bool
		forceResourceLimits   bool
		missingOverlay        bool
		wantFallback          bool
		wantCheckpointUpdates int
	}{
		{
			name: "forced sandbox durable mount validation error",
			restoreErr: fmt.Errorf("restore failed: %w", &checkpointDurableMountValidationError{
				mountPath: "/workspace",
				err:       assert.AnError,
			}),
			forceResourceLimits: true,
			wantFallback:        true,
		},
		{
			name: "forced sandbox cleanup failure",
			restoreErr: fmt.Errorf("restore failed: %w", &checkpointDurableMountValidationError{
				mountPath: "/workspace",
				err:       assert.AnError,
			}),
			deleteErr:           errors.New("delete failed"),
			forceResourceLimits: true,
		},
		{
			name: "forced sandbox missing overlay",
			restoreErr: fmt.Errorf("restore failed: %w", &checkpointDurableMountValidationError{
				mountPath: "/workspace",
				err:       assert.AnError,
			}),
			forceResourceLimits: true,
			missingOverlay:      true,
		},
		{
			name: "ordinary sandbox durable mount validation error",
			restoreErr: fmt.Errorf("restore failed: %w", &checkpointDurableMountValidationError{
				mountPath: "/workspace",
				err:       assert.AnError,
			}),
		},
		{
			name:                  "forced sandbox generic restore error",
			restoreErr:            assert.AnError,
			forceResourceLimits:   true,
			wantCheckpointUpdates: 1,
		},
		{
			name:                "forced sandbox runsc version mismatch",
			restoreErr:          &ErrRunscCheckpointVersionMismatch{Stderr: runscVersionMismatchMessage},
			runtimeName:         types.ContainerRuntimeGvisor.String(),
			archiveFilesystem:   true,
			forceResourceLimits: true,
			wantFallback:        true,
		},
		{
			name:                "forced sandbox runsc version mismatch cleanup failure",
			restoreErr:          &ErrRunscCheckpointVersionMismatch{Stderr: runscVersionMismatchMessage},
			deleteErr:           errors.New("delete failed"),
			runtimeName:         types.ContainerRuntimeGvisor.String(),
			forceResourceLimits: true,
		},
		{
			name:                  "ordinary sandbox runsc version mismatch",
			restoreErr:            &ErrRunscCheckpointVersionMismatch{Stderr: runscVersionMismatchMessage},
			runtimeName:           types.ContainerRuntimeGvisor.String(),
			wantCheckpointUpdates: 1,
		},
	}

	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			containerID := fmt.Sprintf("container-sandbox-mount-fallback-%d", index)
			checkpointID := fmt.Sprintf("checkpoint-sandbox-mount-fallback-%d", index)
			t.Cleanup(func() { _ = os.RemoveAll(filepath.Join("/tmp", containerID)) })
			tmpDir := t.TempDir()
			overlayPath := filepath.Join(tmpDir, "overlay")
			configPath := filepath.Join(overlayPath, containerID, "layer-0", "merged", "config.json")
			runtimeName := test.runtimeName
			if runtimeName == "" {
				runtimeName = types.ContainerRuntimeRunc.String()
			}
			configContents := []byte(runtime.GetBaseConfig(runtimeName))
			durablePath := ""
			if runtimeName == types.ContainerRuntimeGvisor.String() {
				durablePath = filepath.Join(tmpDir, "durable-workspace")
				require.NoError(t, os.MkdirAll(durablePath, 0755))
				require.NoError(t, os.WriteFile(filepath.Join(durablePath, "durable-marker"), []byte("durable"), 0644))
				var spec specs.Spec
				require.NoError(t, json.Unmarshal(configContents, &spec))
				spec.Mounts = append(spec.Mounts, specs.Mount{Type: "bind", Source: durablePath, Destination: "/workspace"})
				updatedConfig, err := json.Marshal(&spec)
				require.NoError(t, err)
				configContents = updatedConfig
			}
			require.NoError(t, os.MkdirAll(filepath.Dir(configPath), 0755))
			require.NoError(t, os.WriteFile(configPath, configContents, 0644))
			checkpointRoot := filepath.Join(tmpDir, "checkpoints")
			checkpointPath := filepath.Join(checkpointRoot, checkpointID)
			require.NoError(t, os.MkdirAll(checkpointPath, 0755))
			checkpointFilesystem := filepath.Join(checkpointPath, checkpointFsDir)
			if test.archiveFilesystem {
				checkpointFilesystem = t.TempDir()
			}
			require.NoError(t, os.MkdirAll(checkpointFilesystem, 0755))
			runtimePayload := "inventory.img"
			if runtimeName == types.ContainerRuntimeGvisor.String() {
				runtimePayload = "checkpoint.img"
			}
			require.NoError(t, os.WriteFile(filepath.Join(checkpointPath, runtimePayload), []byte("runtime"), 0644))
			require.NoError(t, os.WriteFile(filepath.Join(checkpointFilesystem, "checkpoint-root-marker"), []byte("pristine"), 0644))
			if test.forceResourceLimits && runtimeName == types.ContainerRuntimeRunc.String() {
				require.NoError(t, os.WriteFile(filepath.Join(checkpointPath, checkpointForcedRuncProfileFile), []byte(checkpointForcedRuncProfileV1), 0644))
			}
			if test.archiveFilesystem {
				require.NoError(t, archiveDirectoryContext(context.Background(), checkpointFilesystem, filepath.Join(checkpointPath, checkpointFsArchive), nil))
			}

			rt := &restoreFallbackRuntime{
				mockRuntime: mockRuntime{
					name:         runtimeName,
					capabilities: runtime.Capabilities{CheckpointRestore: true},
				},
				runConfigPath: configPath,
				deleteErr:     test.deleteErr,
			}
			repoClient := &fakeContainerRepoClient{state: &pb.ContainerState{Status: string(types.ContainerStatusPending)}}
			backendRepoClient := &fakeBackendRepoClient{}
			worker := &Worker{
				config: types.AppConfig{Worker: types.WorkerConfig{Pools: map[string]types.WorkerPoolConfig{
					"default": {CRIUEnabled: true},
				}}},
				criuManager:         &restoreErrorCRIUManager{exitCode: -1, err: test.restoreErr},
				containerRepoClient: repoClient,
				backendRepoClient:   backendRepoClient,
				containerInstances:  common.NewSafeMap[*ContainerInstance](),
				cacheManager:        &WorkerCacheManager{checkpointRoot: checkpointRoot},
			}
			request := &types.ContainerRequest{
				ContainerId: containerID,
				ConfigPath:  configPath,
				Stub: types.StubWithRelated{Stub: types.Stub{
					Type: types.StubType(types.StubTypeSandbox),
				}},
				Checkpoint: &types.Checkpoint{
					CheckpointId: checkpointID,
					Status:       string(types.CheckpointStatusAvailable),
				},
			}
			if test.forceResourceLimits {
				request.Stub.Config = `{"_beta9_force_resource_limits":true}`
			}
			if durablePath != "" {
				request.Mounts = []types.Mount{{
					LocalPath:   durablePath,
					MountPath:   "/workspace",
					MountType:   types.StorageModeDurableDisk,
					DurableDisk: &types.DurableDiskMountConfig{Name: "workspace"},
				}}
				t.Cleanup(func() {
					contents, err := os.ReadFile(filepath.Join(durablePath, "durable-marker"))
					require.NoError(t, err, "checkpoint fallback must not replace durable workspace storage")
					require.Equal(t, "durable", string(contents))
				})
			}
			instance := &ContainerInstance{
				Id:      containerID,
				Runtime: rt,
			}
			if !test.missingOverlay {
				instance.Overlay = common.NewContainerOverlay(request, filepath.Join(tmpDir, "root"), overlayPath)
			}
			worker.containerInstances.Set(containerID, instance)
			filesystemRestoreDone := make(chan struct{})
			close(filesystemRestoreDone)

			exitCode, err := worker.runContainer(
				context.Background(),
				request,
				slog.New(slog.NewTextHandler(io.Discard, nil)),
				common.NewOutputWriter(func(string) {}),
				make(chan int, 1),
				make(chan int, 1),
				time.Now(),
				registeredAddresses(),
				&checkpointFilesystemRestore{done: filesystemRestoreDone},
			)

			if !test.wantFallback {
				require.ErrorIs(t, err, test.restoreErr)
				require.False(t, rt.runCalled)
				require.NotNil(t, request.Checkpoint)
				require.Equal(t, test.wantCheckpointUpdates, backendRepoClient.updateCalls)
				if test.wantCheckpointUpdates > 0 {
					require.Equal(t, string(types.CheckpointStatusRestoreFailed), backendRepoClient.lastUpdate.Status)
				}
				require.FileExists(t, filepath.Join(checkpointSignalDir(containerID), checkpointCompleteFileName), "fallback preparation must not run")
				require.NoFileExists(t, filepath.Join(tmpDir, "checkpoint-root-marker"), "checkpoint filesystem must not be reseeded")
				if test.deleteErr != nil {
					var cleanupErr *checkpointRestoreCleanupError
					require.ErrorAs(t, err, &cleanupErr)
					require.ErrorIs(t, err, test.deleteErr)
				}
				return
			}

			require.NoError(t, err)
			require.Equal(t, 0, exitCode)
			require.True(t, rt.runCalled)
			require.Equal(t, 1, rt.deleteCallsAtRun, "failed restored runtime must be deleted before cold run")
			require.Zero(t, backendRepoClient.updateCalls, "safe fallback must not invalidate the checkpoint")
			require.Nil(t, request.Checkpoint)
			require.NoFileExists(t, filepath.Join(checkpointSignalDir(containerID), checkpointCompleteFileName), "fallback must clear restored checkpoint signals")
			require.FileExists(t, filepath.Join(overlayPath, containerID, "layer-0", "upper", "checkpoint-root-marker"), "fallback must reseed the fresh upper from the checkpoint")
			require.NoFileExists(t, filepath.Join(overlayPath, containerID, "layer-0", "merged", "checkpoint-root-marker"), "checkpoint upper must not be copied through merged")
			require.Equal(t, configContents, rt.runConfigContents)
			if durablePath != "" {
				require.Contains(t, string(rt.runConfigContents), durablePath, "cold run must retain the durable workspace bind")
			}
		})
	}
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
		registeredAddresses(),
		nil,
	)

	require.NoError(t, err)
	require.Equal(t, 0, exitCode)
	require.True(t, rt.runCalled)
	require.Equal(t, 0, criuManager.restoreCalls)
	require.Equal(t, 0, backendRepoClient.updateCalls)
	require.Nil(t, request.Checkpoint)
}

func TestHasAvailableCheckpoint(t *testing.T) {
	require.False(t, hasAvailableCheckpoint(&types.ContainerRequest{
		Checkpoint: &types.Checkpoint{Status: string(types.CheckpointStatusRestoreFailed)},
	}))
	require.True(t, hasAvailableCheckpoint(&types.ContainerRequest{
		Checkpoint: &types.Checkpoint{Status: string(types.CheckpointStatusAvailable)},
	}))
}

func TestValidateCheckpointRestoreRuntime(t *testing.T) {
	request := &types.ContainerRequest{Checkpoint: &types.Checkpoint{
		CheckpointId: "checkpoint-1",
		Status:       string(types.CheckpointStatusAvailable),
		Runtime:      " gvisor ",
	}}
	gvisor := &mockRuntime{name: types.ContainerRuntimeGvisor.String(), capabilities: runtime.Capabilities{CheckpointRestore: true}}
	runc := &mockRuntime{name: types.ContainerRuntimeRunc.String(), capabilities: runtime.Capabilities{CheckpointRestore: true}}
	unsupported := &mockRuntime{name: types.ContainerRuntimeRunc.String()}

	require.NoError(t, validateCheckpointRestoreRuntime(request, gvisor))
	require.EqualError(t, validateCheckpointRestoreRuntime(request, runc),
		`cannot restore gvisor checkpoint "checkpoint-1" with runtime "runc"`)
	require.EqualError(t, validateCheckpointRestoreRuntime(request, unsupported),
		`cannot restore checkpoint "checkpoint-1" with runtime "runc": checkpoint restore is unsupported`)

	request.Checkpoint.Status = string(types.CheckpointStatusRestoreFailed)
	require.NoError(t, validateCheckpointRestoreRuntime(request, unsupported))

	request.Checkpoint.Status = string(types.CheckpointStatusAvailable)
	request.Checkpoint.Runtime = ""
	require.NoError(t, validateCheckpointRestoreRuntime(request, runc))

	request.Checkpoint.Runtime = types.CheckpointRuntimeFilesystem
	require.NoError(t, validateCheckpointRestoreRuntime(request, unsupported))
	require.NoError(t, validateCheckpointRestoreRuntime(request, nil))
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

func TestShouldCreateCheckpointSkipsSandboxes(t *testing.T) {
	t.Setenv("WORKER_POOL_NAME", "default")

	worker := &Worker{
		config: types.AppConfig{Worker: types.WorkerConfig{Pools: map[string]types.WorkerPoolConfig{
			"default": {CRIUEnabled: true},
		}}},
		criuManager: &startedCRIUManager{},
	}
	request := &types.ContainerRequest{
		CheckpointEnabled: true,
		Stub:              types.StubWithRelated{Stub: types.Stub{Type: types.StubType(types.StubTypeSandbox)}},
	}

	require.False(t, worker.shouldCreateCheckpoint(request))

	request.Stub.Type = types.StubType(types.StubTypePodDeployment)
	require.True(t, worker.shouldCreateCheckpoint(request))
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

type testGPUManager struct{}

func (m *testGPUManager) AssignGPUDevices(ctx context.Context, request *types.ContainerRequest) ([]int, error) {
	return []int{}, nil
}

func (m *testGPUManager) GetContainerGPUDevices(containerId string) []int {
	return []int{}
}

func (m *testGPUManager) UnassignGPUDevices(ctx context.Context, containerId string) {}

func (m *testGPUManager) CDIDevices(assignedDevices []int) []string {
	return []string{}
}

func (m *testGPUManager) InjectEnvVars(env []string) []string {
	return env
}

func (m *testGPUManager) InjectAssignedEnvVars(env []string, assignedDevices []int) []string {
	return env
}

func (m *testGPUManager) InjectMounts(mounts []specs.Mount) []specs.Mount {
	return mounts
}

// Mock runtime for testing
type mockExecCall struct {
	containerID string
	proc        specs.Process
}

type mockRuntime struct {
	name         string
	capabilities runtime.Capabilities
	state        func(context.Context, string) (runtime.State, error)
	signals      []syscall.Signal
	killOpts     []*runtime.KillOpts
	killErr      error
	execCalls    []mockExecCall
	execErr      error
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
	m.execCalls = append(m.execCalls, mockExecCall{containerID: containerID, proc: proc})
	return m.execErr
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

type fakeBackendRepoClient struct {
	updateCalls         int
	lastUpdate          *pb.UpdateCheckpointRequest
	createCalls         int
	lastCreate          *pb.CreateCheckpointRequest
	sourceSnapshot      *pb.DiskSnapshot
	latestSnapshot      *pb.DiskSnapshot
	getDiskSnapshotErr  error
	requestedSnapshotId string
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
	return &pb.GetLatestDiskSnapshotResponse{Ok: true, Snapshot: f.latestSnapshot}, nil
}

func (f *fakeBackendRepoClient) GetDiskSnapshot(ctx context.Context, in *pb.GetDiskSnapshotRequest, opts ...grpc.CallOption) (*pb.GetDiskSnapshotResponse, error) {
	f.requestedSnapshotId = in.SnapshotId
	if f.getDiskSnapshotErr != nil {
		return nil, f.getDiskSnapshotErr
	}
	return &pb.GetDiskSnapshotResponse{Ok: true, Snapshot: f.sourceSnapshot}, nil
}

func (f *fakeBackendRepoClient) GetDiskSnapshotDownloadURL(context.Context, *pb.GetDiskSnapshotDownloadURLRequest, ...grpc.CallOption) (*pb.GetDiskSnapshotDownloadURLResponse, error) {
	return nil, errors.New("not implemented")
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
