package pod

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestSandboxConnectErrorMessageDoesNotLeakDetails(t *testing.T) {
	got := sandboxConnectErrorMessage(errors.New("container state not found: sandbox-123 on worker 10.0.0.12"))
	if got != "Failed to connect to sandbox" {
		t.Fatalf("sandboxConnectErrorMessage leaked details: %q", got)
	}
}

func TestWaitForSandboxReadyWaitsThroughPending(t *testing.T) {
	probes := 0
	err := waitForSandboxReady(context.Background(), func(context.Context) (*pb.ContainerSandboxStatusResponse, error) {
		probes++
		status := "pending"
		if probes == 2 {
			status = "running"
		}
		return &pb.ContainerSandboxStatusResponse{Ok: true, Status: status}, nil
	})

	if err != nil {
		t.Fatalf("waitForSandboxReady returned error: %v", err)
	}
	if probes != 2 {
		t.Fatalf("waitForSandboxReady probes = %d, want 2", probes)
	}
}

func TestWaitForSandboxReadyRetriesTransientProbeFailure(t *testing.T) {
	probes := 0
	err := waitForSandboxReady(context.Background(), func(context.Context) (*pb.ContainerSandboxStatusResponse, error) {
		probes++
		if probes == 1 {
			return nil, status.Error(codes.Unavailable, "transport is closing")
		}
		return &pb.ContainerSandboxStatusResponse{Ok: true, Status: "running"}, nil
	})

	if err != nil {
		t.Fatalf("waitForSandboxReady returned error: %v", err)
	}
	if probes != 2 {
		t.Fatalf("waitForSandboxReady probes = %d, want 2", probes)
	}
}

func TestWaitForSandboxReadyRejectsStoppingContainer(t *testing.T) {
	err := waitForSandboxReady(context.Background(), func(context.Context) (*pb.ContainerSandboxStatusResponse, error) {
		return &pb.ContainerSandboxStatusResponse{Ok: true, Status: "stopping"}, nil
	})
	if err == nil {
		t.Fatal("waitForSandboxReady returned nil error for stopping sandbox")
	}
}

func TestWaitForSandboxReadyHonorsContextDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()

	err := waitForSandboxReady(ctx, func(context.Context) (*pb.ContainerSandboxStatusResponse, error) {
		return &pb.ContainerSandboxStatusResponse{Ok: true, Status: "pending"}, nil
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waitForSandboxReady error = %v, want context deadline exceeded", err)
	}
}

func TestWaitForSandboxReadyReturnsWorkerFailure(t *testing.T) {
	err := waitForSandboxReady(context.Background(), func(context.Context) (*pb.ContainerSandboxStatusResponse, error) {
		return &pb.ContainerSandboxStatusResponse{Ok: false, ErrorMsg: "process manager failed"}, nil
	})
	if err == nil || err.Error() != "process manager failed" {
		t.Fatalf("waitForSandboxReady error = %v, want worker failure", err)
	}
}

func TestSandboxExecFailureMessageKeepsTransientErrorsRetryable(t *testing.T) {
	got := sandboxExecFailureMessage(status.Error(codes.Unavailable, "transport is closing"))
	if got != "Failed to connect to sandbox" {
		t.Fatalf("transient exec error message = %q, want retryable connect message", got)
	}
}

func TestSandboxExecFailureMessageKeepsCommandErrorsGeneric(t *testing.T) {
	got := sandboxExecFailureMessage(errors.New("permission denied"))
	if got != "Failed to execute command" {
		t.Fatalf("command exec error message = %q, want generic command failure", got)
	}
}

func TestSandboxExecFailureMessageKeepsConnectErrorsRetryable(t *testing.T) {
	got := sandboxExecFailureMessage(sandboxConnectionError{err: errors.New("container not found")})
	if got != "Failed to connect to sandbox" {
		t.Fatalf("connect error message = %q, want retryable connect message", got)
	}
}

func TestSandboxClientCacheKeyUsesStableWorkerRouteAddress(t *testing.T) {
	address := types.BackendRouteAddress(types.BackendRouteID("machine-a", "worker-a", "", types.BackendRouteKindWorker, 0))

	keyA := sandboxClientCacheKey(address, "token")
	keyB := sandboxClientCacheKey(address, "token")
	if keyA != keyB {
		t.Fatalf("worker route cache keys differ: %q != %q", keyA, keyB)
	}
}

func TestSandboxClientCacheKeyKeepsDifferentWorkerAddressesDistinct(t *testing.T) {
	keyA := sandboxClientCacheKey("route://worker-a", "token")
	keyB := sandboxClientCacheKey("route://worker-b", "token")
	if keyA == keyB {
		t.Fatalf("worker route cache keys unexpectedly matched: %q", keyA)
	}
}

func TestPodRunnableStubOnlyAllowsPodAndSandboxKinds(t *testing.T) {
	tests := []struct {
		name     string
		stubType types.StubType
		want     bool
	}{
		{name: "pod run", stubType: types.StubType(types.StubTypePodRun), want: true},
		{name: "pod deployment", stubType: types.StubType(types.StubTypePodDeployment), want: true},
		{name: "sandbox", stubType: types.StubType(types.StubTypeSandbox), want: true},
		{name: "endpoint", stubType: types.StubType(types.StubTypeEndpoint), want: false},
		{name: "asgi deployment", stubType: types.StubType(types.StubTypeASGIDeployment), want: false},
		{name: "function", stubType: types.StubType(types.StubTypeFunction), want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := podRunnableStub(tt.stubType); got != tt.want {
				t.Fatalf("podRunnableStub(%q) = %t, want %t", tt.stubType, got, tt.want)
			}
		})
	}
}

func TestPodRunWorkspacePrefersStubWorkspace(t *testing.T) {
	storageID := uint(2)
	authWorkspace := &types.Workspace{ExternalId: "workspace-id", Name: "auth"}
	stub := &types.StubWithRelated{
		Workspace: types.Workspace{
			ExternalId: "workspace-id",
			Name:       "stub",
			Storage:    &types.WorkspaceStorage{Id: &storageID},
		},
	}

	got, err := podRunWorkspace(&auth.AuthInfo{Workspace: authWorkspace}, stub)
	if err != nil {
		t.Fatalf("podRunWorkspace returned error: %v", err)
	}
	if got != &stub.Workspace {
		t.Fatalf("podRunWorkspace did not return stub workspace")
	}
	if !got.StorageAvailable() {
		t.Fatalf("podRunWorkspace returned workspace without storage")
	}
}

func TestPodRunWorkspaceRejectsWorkspaceMismatch(t *testing.T) {
	_, err := podRunWorkspace(
		&auth.AuthInfo{Workspace: &types.Workspace{ExternalId: "auth-workspace"}},
		&types.StubWithRelated{Workspace: types.Workspace{ExternalId: "other-workspace"}},
	)
	if err == nil {
		t.Fatal("podRunWorkspace returned nil error for workspace mismatch")
	}
}

func TestSandboxKillFailureMessageHandlesNilResponse(t *testing.T) {
	if got := sandboxKillFailureMessage(nil); got != "Failed to kill sandbox process" {
		t.Fatalf("sandboxKillFailureMessage(nil) = %q", got)
	}

	resp := &pb.ContainerSandboxKillResponse{ErrorMsg: "worker said no"}
	if got := sandboxKillFailureMessage(resp); got != "worker said no" {
		t.Fatalf("sandboxKillFailureMessage(resp) = %q", got)
	}
}

func TestSandboxKillRejectsNilRequest(t *testing.T) {
	service := &GenericPodService{}

	resp, err := service.SandboxKill(context.Background(), nil)
	if resp != nil {
		t.Fatalf("SandboxKill response = %#v, want nil", resp)
	}
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("SandboxKill error code = %s, want %s", status.Code(err), codes.InvalidArgument)
	}
}

func TestSandboxKillRejectsMissingAuthContext(t *testing.T) {
	service := &GenericPodService{}

	resp, err := service.SandboxKill(context.Background(), &pb.PodSandboxKillRequest{ContainerId: "sandbox-123", Pid: 1})
	if resp != nil {
		t.Fatalf("SandboxKill response = %#v, want nil", resp)
	}
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("SandboxKill error code = %s, want %s", status.Code(err), codes.Unauthenticated)
	}
}
