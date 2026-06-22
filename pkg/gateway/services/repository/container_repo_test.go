package repository_services

import (
	"fmt"
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
)

func TestSeedReadyAgentWorkerRoute(t *testing.T) {
	route := backendRouteFromProto(&pb.BackendRoute{
		RouteId:     "machine-a:worker-a:container-a:worker:0",
		MachineId:   "machine-a",
		Kind:        types.BackendRouteKindWorker,
		Transport:   types.BackendRouteTransportTSNet,
		LocalTarget: "127.0.0.1:50051",
		State:       types.BackendRouteStateOpening,
	})

	seedReadyAgentWorkerRoute(&route)

	if route.State != types.BackendRouteStateReady {
		t.Fatalf("state = %q, want %q", route.State, types.BackendRouteStateReady)
	}
	wantProxy := fmt.Sprintf("beam-agent-machine-a:%d", types.DefaultAgentTSNetRouteProxyPort)
	if route.ProxyTarget != wantProxy {
		t.Fatalf("proxy target = %q, want %q", route.ProxyTarget, wantProxy)
	}
	if route.UpdatedAt == 0 {
		t.Fatal("expected updated_at to be set")
	}
}

func TestSeedReadyAgentWorkerRouteLeavesContainerRoutesOpening(t *testing.T) {
	route := backendRouteFromProto(&pb.BackendRoute{
		RouteId:     "machine-a:worker-a:container-a:container:8080",
		MachineId:   "machine-a",
		Kind:        types.BackendRouteKindContainer,
		Transport:   types.BackendRouteTransportTSNet,
		LocalTarget: "127.0.0.1:8080",
		State:       types.BackendRouteStateOpening,
	})

	seedReadyAgentWorkerRoute(&route)

	if route.State != types.BackendRouteStateOpening {
		t.Fatalf("state = %q, want %q", route.State, types.BackendRouteStateOpening)
	}
	if route.ProxyTarget != "" {
		t.Fatalf("proxy target = %q, want empty", route.ProxyTarget)
	}
}

func TestRegisteredRouteContainerIDKeepsSharedWorkerRouteUnscoped(t *testing.T) {
	route := types.BackendRoute{Kind: types.BackendRouteKindWorker}

	got := registeredRouteContainerID("container-a", route)
	if got != "" {
		t.Fatalf("container id = %q, want empty", got)
	}
}

func TestRegisteredRouteContainerIDScopesContainerRoutes(t *testing.T) {
	route := types.BackendRoute{Kind: types.BackendRouteKindContainer}

	got := registeredRouteContainerID("container-a", route)
	if got != "container-a" {
		t.Fatalf("container id = %q, want container-a", got)
	}
}
