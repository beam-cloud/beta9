package repository_services

import (
	"testing"

	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

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

type authorizationContainerRepo struct {
	repository.ContainerRepository
	state *types.ContainerState
}

func (r *authorizationContainerRepo) GetContainerState(string) (*types.ContainerState, error) {
	return r.state, nil
}

func TestPrivateWorkerContainerOwnership(t *testing.T) {
	for _, workspace := range []string{"own", "other"} {
		t.Run(workspace, func(t *testing.T) {
			containers := &ContainerRepositoryService{containerRepo: &authorizationContainerRepo{state: &types.ContainerState{ContainerId: "container", WorkspaceId: workspace}}}
			client := pb.NewContainerRepositoryServiceClient(repositoryAuthClient(t, &WorkerRepositoryService{}, containers))
			response, err := client.GetContainerState(repositoryAuthContext(types.TokenTypeWorkerPrivate), &pb.GetContainerStateRequest{ContainerId: "container"})
			if workspace == "own" {
				require.NoError(t, err)
				require.True(t, response.Ok)
			} else {
				require.Equal(t, codes.PermissionDenied, status.Code(err))
			}
		})
	}
}

func TestPrivateWorkerCannotPublishForeignRoutes(t *testing.T) {
	state := &types.ContainerState{ContainerId: "container", WorkspaceId: "own", WorkerId: "worker", MachineId: "machine"}
	valid := &pb.BackendRoute{
		RouteId:     types.BackendRouteID("machine", "worker", "container", types.BackendRouteKindContainer, 80),
		WorkspaceId: "own", WorkerId: "worker", MachineId: "machine", ContainerId: "container", Kind: types.BackendRouteKindContainer, Port: 80,
	}
	require.NoError(t, authorizeBackendRoute(state, valid))
	containers := &ContainerRepositoryService{containerRepo: &authorizationContainerRepo{state: state}}
	conn := repositoryAuthClient(t, &WorkerRepositoryService{}, containers)
	for _, tc := range []struct {
		name   string
		change func(*pb.BackendRoute)
	}{
		{"route", func(r *pb.BackendRoute) { r.RouteId = "foreign" }},
		{"workspace", func(r *pb.BackendRoute) { r.WorkspaceId = "foreign" }},
		{"worker", func(r *pb.BackendRoute) { r.WorkerId = "foreign" }},
		{"machine", func(r *pb.BackendRoute) { r.MachineId = "foreign" }},
		{"container", func(r *pb.BackendRoute) { r.ContainerId = "foreign" }},
		{"kind", func(r *pb.BackendRoute) { r.Kind = "foreign" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			route := proto.Clone(valid).(*pb.BackendRoute)
			tc.change(route)
			for _, rpc := range []struct {
				method            string
				request, response proto.Message
			}{
				{pb.ContainerRepositoryService_SetContainerAddress_FullMethodName, &pb.SetContainerAddressRequest{ContainerId: "container", Route: route}, &pb.SetContainerAddressResponse{}},
				{pb.ContainerRepositoryService_SetContainerAddressMap_FullMethodName, &pb.SetContainerAddressMapRequest{ContainerId: "container", Routes: []*pb.BackendRoute{route}}, &pb.SetContainerAddressMapResponse{}},
				{pb.ContainerRepositoryService_SetWorkerAddress_FullMethodName, &pb.SetWorkerAddressRequest{ContainerId: "container", Route: route}, &pb.SetWorkerAddressResponse{}},
			} {
				err := conn.Invoke(repositoryAuthContext(types.TokenTypeWorkerPrivate), rpc.method, rpc.request, rpc.response)
				require.Equal(t, codes.PermissionDenied, status.Code(err), rpc.method)
			}
		})
	}
}
