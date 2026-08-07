package worker

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/runtime"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/stretchr/testify/require"
	"github.com/tj/assert"
	"google.golang.org/grpc"
)

func TestThunderInjectEnvVarsAddsThunderLDPreload(t *testing.T) {
	manager := NewContainerThunderManager(nil)

	env := manager.InjectEnvVars([]string{"A=1"})

	assert.Contains(t, env, "LD_PRELOAD=/etc/thunder/libthunder.so")
}

func TestThunderInjectEnvVarsAppendsThunderLDPreload(t *testing.T) {
	manager := NewContainerThunderManager(nil)

	env := manager.InjectEnvVars([]string{"LD_PRELOAD=/lib/existing.so"})

	assert.Contains(t, env, "LD_PRELOAD=/lib/existing.so:/etc/thunder/libthunder.so")
}

func TestThunderInjectMountsAddsNvidiaSMINVMLAndCUDA(t *testing.T) {
	manager := NewContainerThunderManager(nil)
	initialMounts := []specs.Mount{{Type: "bind", Source: "/src", Destination: "/dst"}}

	mounts := manager.InjectMounts(initialMounts)

	assert.Contains(t, mounts, thunderBindMount("/usr/bin/nvidia-smi"))
	assert.Contains(t, mounts, thunderBindMount("/usr/lib/x86_64-linux-gnu/libnvidia-ml.so.1"))
	assert.Contains(t, mounts, thunderBindMount("/usr/lib/x86_64-linux-gnu/libcuda.so.1"))
}

func TestThunderAssignCreatesClientEnrollmentAndCachesInstallCommand(t *testing.T) {
	client := &fakeThunderServiceClient{
		createResp: &pb.CreateClientEnrollmentResponse{Ok: true, InstallCommand: "curl install thunder"},
	}
	manager := NewContainerThunderManager(client)
	request := &types.ContainerRequest{ContainerId: "container-123", GpuRequest: []string{"H100"}}

	assigned, err := manager.AssignGPUDevices(request)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, []int{}, assigned)
	if len(client.createReqs) != 1 || client.createReqs[0].ContainerId != "container-123" {
		t.Fatalf("create requests = %+v", client.createReqs)
	}
	cmd, ok := manager.installCache.Get("container-123")
	assert.True(t, ok)
	assert.Equal(t, "curl install thunder", cmd)

	env := manager.InjectAssignedEnvVars([]string{"A=1", "NVIDIA_VISIBLE_DEVICES=void", "WORKER_GPU_DEVICES=0"}, assigned)
	assert.Equal(t, []string{"A=1", "NVIDIA_VISIBLE_DEVICES=void", "WORKER_GPU_DEVICES=0"}, env)
}

func TestThunderAssignReturnsGatewayError(t *testing.T) {
	manager := NewContainerThunderManager(&fakeThunderServiceClient{
		createResp: &pb.CreateClientEnrollmentResponse{ErrorMsg: "gateway refused enrollment"},
	})

	_, err := manager.AssignGPUDevices(&types.ContainerRequest{ContainerId: "container-123", GpuRequest: []string{"H100"}})
	if err == nil {
		t.Fatal("expected Thunder enrollment error")
	}
	assert.Contains(t, err.Error(), "gateway refused enrollment")
}

func TestThunderAssignRequiresServiceClient(t *testing.T) {
	manager := NewContainerThunderManager(nil)
	_, err := manager.AssignGPUDevices(&types.ContainerRequest{ContainerId: "container-123", GpuRequest: []string{"H100"}})
	if err == nil {
		t.Fatal("expected missing Thunder client error")
	}
	assert.Contains(t, err.Error(), "Thunder service client")
}

func TestThunderAssignRequiresInstallCommand(t *testing.T) {
	manager := NewContainerThunderManager(&fakeThunderServiceClient{
		createResp: &pb.CreateClientEnrollmentResponse{Ok: true},
	})
	_, err := manager.AssignGPUDevices(&types.ContainerRequest{ContainerId: "container-123", GpuRequest: []string{"H100"}})
	if err == nil {
		t.Fatal("expected missing install command error")
	}
	assert.Contains(t, err.Error(), "install command")
}

func TestThunderUnassignDeletesClientEnrollment(t *testing.T) {
	client := &fakeThunderServiceClient{
		deleteResp: &pb.DeleteClientEnrollmentResponse{Ok: true},
	}
	manager := NewContainerThunderManager(client)
	manager.installCache.Set("container-123", "curl install thunder")

	manager.UnassignGPUDevices("container-123")

	if len(client.deleteReqs) != 1 || client.deleteReqs[0].ContainerId != "container-123" {
		t.Fatalf("delete requests = %+v", client.deleteReqs)
	}
	_, ok := manager.installCache.Get("container-123")
	assert.False(t, ok)
}

func TestThunderStartupHookUsesCachedInstaller(t *testing.T) {
	manager := NewContainerThunderManager(nil)
	manager.installCache.Set("container-123", "curl -fsSL https://get.thundercompute.com/install.sh | sudo THUNDER_NOWARN=1 THUNDER_INSTALL_MODE=client THUNDER_CENTRAL_URL='https://gateway.example' THUNDER_ENROLLMENT_TOKEN='enroll-token' sh")
	worker := &Worker{
		containerThunderManager: manager,
		gpuVirtualized:          true,
	}

	hook, err := worker.thunderStartupHook(&types.ContainerRequest{ContainerId: "container-123", GpuRequest: []string{"H100"}}, &specs.Spec{Process: &specs.Process{Cwd: "/workspace", Env: []string{"A=1"}}})
	if err != nil {
		t.Fatal(err)
	}

	execHook, ok := hook.(runtime.StartupExecHook)
	require.True(t, ok)
	assert.Equal(t, "thunder_client_install", execHook.HookName)
	assert.Equal(t, 2*time.Minute, execHook.Timeout)
	assert.Equal(t, "/workspace", execHook.Process.Cwd)
	assert.Equal(t, []string{"sh", "-c", "curl -fsSL https://get.thundercompute.com/install.sh | sudo THUNDER_NOWARN=1 THUNDER_INSTALL_MODE=client THUNDER_CENTRAL_URL='https://gateway.example' THUNDER_ENROLLMENT_TOKEN='enroll-token' sh"}, execHook.Process.Args)
	assert.Contains(t, execHook.Process.Env, "A=1")
	assert.Contains(t, execHook.Process.Env, "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin")
}

func TestThunderStartupHookRequiresCachedInstaller(t *testing.T) {
	worker := &Worker{
		containerThunderManager: NewContainerThunderManager(nil),
		gpuVirtualized:          true,
	}

	hook, err := worker.thunderStartupHook(&types.ContainerRequest{ContainerId: "container-123", GpuRequest: []string{"H100"}}, &specs.Spec{Process: &specs.Process{}})
	require.Nil(t, hook)
	if err == nil || !strings.Contains(err.Error(), "install command") {
		t.Fatalf("thunderStartupHook() error = %v", err)
	}
}

type fakeThunderServiceClient struct {
	createResp *pb.CreateClientEnrollmentResponse
	createErr  error
	createReqs []*pb.CreateClientEnrollmentRequest
	deleteResp *pb.DeleteClientEnrollmentResponse
	deleteErr  error
	deleteReqs []*pb.DeleteClientEnrollmentRequest
}

func (f *fakeThunderServiceClient) CreateClientEnrollment(ctx context.Context, in *pb.CreateClientEnrollmentRequest, opts ...grpc.CallOption) (*pb.CreateClientEnrollmentResponse, error) {
	f.createReqs = append(f.createReqs, in)
	if f.createErr != nil {
		return nil, f.createErr
	}
	if f.createResp != nil {
		return f.createResp, nil
	}
	return &pb.CreateClientEnrollmentResponse{Ok: true, InstallCommand: "curl install thunder"}, nil
}

func (f *fakeThunderServiceClient) DeleteClientEnrollment(ctx context.Context, in *pb.DeleteClientEnrollmentRequest, opts ...grpc.CallOption) (*pb.DeleteClientEnrollmentResponse, error) {
	f.deleteReqs = append(f.deleteReqs, in)
	if f.deleteErr != nil {
		return nil, f.deleteErr
	}
	if f.deleteResp != nil {
		return f.deleteResp, nil
	}
	return &pb.DeleteClientEnrollmentResponse{Ok: true}, nil
}
