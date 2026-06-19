package pod

import (
	"context"
	"errors"
	"testing"

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
