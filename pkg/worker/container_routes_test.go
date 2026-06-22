package worker

import (
	"context"
	"testing"

	"github.com/beam-cloud/beta9/pkg/common"
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

func TestRegisterContainerPortsScopesSamePortAcrossContainersOnAgentWorker(t *testing.T) {
	repoClient := &fakeContainerRepoClient{}
	worker := &Worker{
		persistent:              true,
		machineID:               "machine-one",
		workerId:                "worker-one",
		poolName:                "pool-one",
		containerNetworkManager: &fakeContainerNetworkController{},
		routeTransport:          types.BackendRouteTransportTSNet,
		containerRepoClient:     repoClient,
		containerInstances:      common.NewSafeMap[*ContainerInstance](),
	}

	containerA := "sandbox-e9c29586-c465-4a67-9c9b-25293d1ce77b-aaaa1111"
	containerB := "sandbox-e9c29586-c465-4a67-9c9b-25293d1ce77b-bbbb2222"
	worker.containerInstances.Set(containerA, &ContainerInstance{})
	worker.containerInstances.Set(containerB, &ContainerInstance{})

	err := worker.registerContainerPorts(context.Background(), &types.ContainerRequest{
		ContainerId: containerA,
		WorkspaceId: "workspace-one",
	}, []PortBinding{{HostPort: 30001, ContainerPort: 8765}})
	if err != nil {
		t.Fatal(err)
	}
	routeA := repoClient.lastSetAddressMap.Routes[0]

	err = worker.registerContainerPorts(context.Background(), &types.ContainerRequest{
		ContainerId: containerB,
		WorkspaceId: "workspace-one",
	}, []PortBinding{{HostPort: 30002, ContainerPort: 8765}})
	if err != nil {
		t.Fatal(err)
	}
	routeB := repoClient.lastSetAddressMap.Routes[0]

	if routeA.RouteId == routeB.RouteId {
		t.Fatalf("same exposed port on different containers reused route id %q", routeA.RouteId)
	}
	if routeA.RouteId != types.BackendRouteID("machine-one", "worker-one", containerA, types.BackendRouteKindContainer, 8765) {
		t.Fatalf("route A id = %q", routeA.RouteId)
	}
	if routeB.RouteId != types.BackendRouteID("machine-one", "worker-one", containerB, types.BackendRouteKindContainer, 8765) {
		t.Fatalf("route B id = %q", routeB.RouteId)
	}
	if routeA.LocalTarget != "10.0.0.2:30001" {
		t.Fatalf("route A local target = %q, want host port 30001", routeA.LocalTarget)
	}
	if routeB.LocalTarget != "10.0.0.2:30002" {
		t.Fatalf("route B local target = %q, want host port 30002", routeB.LocalTarget)
	}
}
