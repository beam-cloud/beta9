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

func TestSandboxClientCacheKeyUsesStableWorkerRouteTarget(t *testing.T) {
	routeA := &types.BackendRoute{
		RouteID:     "route-a",
		Kind:        types.BackendRouteKindWorker,
		Transport:   types.BackendRouteTransportTSNet,
		ProxyTarget: "beam-agent-machine:29443",
	}
	routeB := &types.BackendRoute{
		RouteID:     "route-b",
		Kind:        types.BackendRouteKindWorker,
		Transport:   types.BackendRouteTransportTSNet,
		ProxyTarget: "beam-agent-machine:29443",
	}

	keyA := sandboxClientCacheKeyForRoute(routeA, "route://route-a", "token")
	keyB := sandboxClientCacheKeyForRoute(routeB, "route://route-b", "token")
	if keyA != keyB {
		t.Fatalf("worker route cache keys differ: %q != %q", keyA, keyB)
	}
}

func TestSandboxClientCacheKeyKeepsContainerRoutesDistinct(t *testing.T) {
	route := &types.BackendRoute{
		RouteID:     "route-a",
		Kind:        types.BackendRouteKindContainer,
		Transport:   types.BackendRouteTransportTSNet,
		ProxyTarget: "beam-agent-machine:29443",
	}

	got := sandboxClientCacheKeyForRoute(route, "route://route-a", "token")
	if got != "route://route-a:token" {
		t.Fatalf("container route cache key = %q, want address scoped key", got)
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
