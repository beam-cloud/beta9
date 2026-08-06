package worker

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	common "github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/tj/assert"
	"google.golang.org/grpc"
)

func TestThunderSetupTrackerWaitsUntilComplete(t *testing.T) {
	tracker := newThunderSetupTracker()
	tracker.Begin("container-123")

	done := make(chan error, 1)
	go func() {
		done <- tracker.Wait(context.Background(), "container-123")
	}()

	select {
	case err := <-done:
		t.Fatalf("wait returned before completion: %v", err)
	case <-time.After(25 * time.Millisecond):
	}

	tracker.Complete("container-123", nil)
	requireNoErrorEventually(t, done)
}

func TestThunderSetupTrackerReturnsFailure(t *testing.T) {
	tracker := newThunderSetupTracker()
	tracker.Begin("container-123")
	tracker.Complete("container-123", errors.New("install failed"))

	err := tracker.Wait(context.Background(), "container-123")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "install failed")
}

func TestThunderSetupTrackerDeleteWakesWaiters(t *testing.T) {
	tracker := newThunderSetupTracker()
	tracker.Begin("container-123")
	tracker.mu.Lock()
	status := tracker.statuses["container-123"]
	tracker.mu.Unlock()

	done := make(chan error, 1)
	go func() {
		done <- status.wait(context.Background())
	}()

	tracker.Delete("container-123")
	err := <-done
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cancelled")
}

func requireNoErrorEventually(t *testing.T, done <-chan error) {
	t.Helper()
	select {
	case err := <-done:
		assert.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for Thunder setup wait")
	}
}

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

func TestInstallThunderClientExecutesCachedInstaller(t *testing.T) {
	rt := &mockRuntime{name: "runc"}
	manager := NewContainerThunderManager(nil)
	manager.installCache.Set("container-123", "curl -fsSL https://get.thundercompute.com/install.sh | sudo THUNDER_NOWARN=1 THUNDER_INSTALL_MODE=client THUNDER_CENTRAL_URL='https://gateway.example' THUNDER_ENROLLMENT_TOKEN='enroll-token' sh")
	instances := common.NewSafeMap[*ContainerInstance]()
	instances.Set("container-123", &ContainerInstance{
		Runtime: rt,
		Spec:    &specs.Spec{Process: &specs.Process{Cwd: "/workspace", Env: []string{"A=1"}}},
	})
	worker := &Worker{
		containerThunderManager: manager,
		containerInstances:      instances,
		gpuVirtualized:          true,
	}

	err := worker.installThunderClient(context.Background(), &types.ContainerRequest{ContainerId: "container-123", GpuRequest: []string{"H100"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(rt.execCalls) != 1 {
		t.Fatalf("execCalls = %d, want 1", len(rt.execCalls))
	}
	call := rt.execCalls[0]
	assert.Equal(t, "container-123", call.containerID)
	assert.Equal(t, "/workspace", call.proc.Cwd)
	assert.Equal(t, []string{"sh", "-c", "curl -fsSL https://get.thundercompute.com/install.sh | sudo THUNDER_NOWARN=1 THUNDER_INSTALL_MODE=client THUNDER_CENTRAL_URL='https://gateway.example' THUNDER_ENROLLMENT_TOKEN='enroll-token' sh"}, call.proc.Args)
	assert.Contains(t, call.proc.Env, "A=1")
	assert.Contains(t, call.proc.Env, "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin")
}

func TestInstallThunderClientRequiresCachedInstaller(t *testing.T) {
	worker := &Worker{
		containerThunderManager: NewContainerThunderManager(nil),
		containerInstances:      common.NewSafeMap[*ContainerInstance](),
		gpuVirtualized:          true,
	}
	err := worker.installThunderClient(context.Background(), &types.ContainerRequest{ContainerId: "container-123", GpuRequest: []string{"H100"}})
	if err == nil || !strings.Contains(err.Error(), "install command") {
		t.Fatalf("installThunderClient() error = %v", err)
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
