package worker

import (
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
)

func TestBackendRouteForWorkerIsStableAcrossContainers(t *testing.T) {
	worker := &Worker{
		machineID:      "machine-one",
		workerId:       "worker-one",
		poolName:       "pool-one",
		persistent:     true,
		routeTransport: types.BackendRouteTransportTSNet,
	}

	reqA := &types.ContainerRequest{ContainerId: "container-a", WorkspaceId: "workspace-one"}
	reqB := &types.ContainerRequest{ContainerId: "container-b", WorkspaceId: "workspace-one"}

	routeA := worker.backendRouteFor(reqA, types.BackendRouteKindWorker, 0, "127.0.0.1:50051")
	routeB := worker.backendRouteFor(reqB, types.BackendRouteKindWorker, 0, "127.0.0.1:50051")

	if routeA.RouteId != routeB.RouteId {
		t.Fatalf("worker route IDs differ: %q != %q", routeA.RouteId, routeB.RouteId)
	}
	if routeA.ContainerId != "" {
		t.Fatalf("worker route container_id = %q, want empty", routeA.ContainerId)
	}
}

func TestBackendRouteForContainerIsScopedToContainer(t *testing.T) {
	worker := &Worker{
		machineID:      "machine-one",
		workerId:       "worker-one",
		poolName:       "pool-one",
		persistent:     true,
		routeTransport: types.BackendRouteTransportTSNet,
	}

	req := &types.ContainerRequest{ContainerId: "container-a", WorkspaceId: "workspace-one"}
	route := worker.backendRouteFor(req, types.BackendRouteKindContainer, 8080, "127.0.0.1:38080")

	if route.ContainerId != req.ContainerId {
		t.Fatalf("container route container_id = %q, want %q", route.ContainerId, req.ContainerId)
	}
	if route.RouteId != types.BackendRouteID("machine-one", "worker-one", "container-a", types.BackendRouteKindContainer, 8080) {
		t.Fatalf("container route id = %q", route.RouteId)
	}
}
