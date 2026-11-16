package repository_services

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/redis/go-redis/v9"
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
	err   error
	route *types.BackendRoute
}

func (r *authorizationContainerRepo) GetContainerState(string) (*types.ContainerState, error) {
	return r.state, r.err
}

func (r *authorizationContainerRepo) GetBackendRoute(context.Context, string) (*types.BackendRoute, error) {
	return r.route, nil
}

func TestMissingContainerStatePassesOnlyForTrustedWorkersFinishingIt(t *testing.T) {
	containers := &ContainerRepositoryService{containerRepo: &authorizationContainerRepo{err: &types.ErrContainerStateNotFound{ContainerId: "container"}}}
	finishing := []interface{}{
		&pb.GetContainerStateRequest{ContainerId: "container"},
		&pb.UpdateContainerStatusRequest{ContainerId: "container"},
		&pb.DeleteContainerStateRequest{ContainerId: "container"},
		&pb.SetContainerExitCodeRequest{ContainerId: "vm-container"},
	}
	others := []interface{}{
		&pb.SetContainerExitCodeRequest{ContainerId: "container"},
		&pb.SetContainerAddressRequest{ContainerId: "container", Address: "10.0.0.1:80"},
		&pb.SetWorkerAddressRequest{ContainerId: "container", Address: "10.0.0.1:80"},
	}
	for tokenType, want := range map[string]codes.Code{types.TokenTypeWorker: codes.OK, types.TokenTypeWorkerPrivate: codes.PermissionDenied} {
		ctx := auth.ContextWithAuthInfo(context.Background(), &auth.AuthInfo{
			Token: &types.Token{TokenType: tokenType, WorkerID: "worker"}, Workspace: &types.Workspace{ExternalId: "own"},
		})
		for _, req := range finishing {
			require.Equal(t, want, status.Code(containers.AuthorizeRequest(ctx, req)), "%s %T", tokenType, req)
		}
		for _, req := range others {
			require.Equal(t, codes.PermissionDenied, status.Code(containers.AuthorizeRequest(ctx, req)), "%s %T", tokenType, req)
		}
	}
}

func TestPrivateWorkerContainerOwnership(t *testing.T) {
	for _, worker := range []string{"worker", "other-worker", ""} {
		t.Run(worker, func(t *testing.T) {
			containers := &ContainerRepositoryService{containerRepo: &authorizationContainerRepo{state: &types.ContainerState{ContainerId: "container", WorkspaceId: "own", WorkerId: worker}}}
			client := pb.NewContainerRepositoryServiceClient(repositoryAuthClient(t, &WorkerRepositoryService{}, containers))
			response, err := client.GetContainerState(repositoryAuthContext(types.TokenTypeWorkerPrivate), &pb.GetContainerStateRequest{ContainerId: "container"})
			if worker == "worker" {
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
		Transport:   types.BackendRouteTransportTSNet,
		RouteId:     types.BackendRouteID("machine", "worker", "container", types.BackendRouteKindContainer, 80),
		WorkspaceId: "own", WorkerId: "worker", MachineId: "machine", ContainerId: "container", Kind: types.BackendRouteKindContainer, Port: 80,
	}
	require.NoError(t, authorizeBackendRoute(state, backendRouteFromProto(valid)))
	repo := &authorizationContainerRepo{state: state}
	containers := &ContainerRepositoryService{containerRepo: repo}
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
			saved := backendRouteFromProto(route)
			repo.route = &saved
			for _, rpc := range []struct {
				method            string
				request, response proto.Message
			}{
				{pb.ContainerRepositoryService_SetContainerAddress_FullMethodName, &pb.SetContainerAddressRequest{ContainerId: "container", Route: route}, &pb.SetContainerAddressResponse{}},
				{pb.ContainerRepositoryService_SetContainerAddressMap_FullMethodName, &pb.SetContainerAddressMapRequest{ContainerId: "container", Routes: []*pb.BackendRoute{route}}, &pb.SetContainerAddressMapResponse{}},
				{pb.ContainerRepositoryService_SetContainerAddressMap_FullMethodName, &pb.SetContainerAddressMapRequest{ContainerId: "container", AddressMap: map[int32]string{80: types.BackendRouteAddress(valid.RouteId)}}, &pb.SetContainerAddressMapResponse{}},
				{pb.ContainerRepositoryService_SetWorkerAddress_FullMethodName, &pb.SetWorkerAddressRequest{ContainerId: "container", Route: route}, &pb.SetWorkerAddressResponse{}},
			} {
				err := conn.Invoke(repositoryAuthContext(types.TokenTypeWorkerPrivate), rpc.method, rpc.request, rpc.response)
				require.Equal(t, codes.PermissionDenied, status.Code(err), rpc.method)
			}
		})
	}
}

func TestPrivateWorkerCannotBypassAgentRouteFence(t *testing.T) {
	state := &types.ContainerState{ContainerId: "container", WorkspaceId: "own", WorkerId: "worker", MachineId: "machine"}
	service := &ContainerRepositoryService{containerRepo: &authorizationContainerRepo{state: state}}
	ctx := auth.ContextWithAuthInfo(context.Background(), &auth.AuthInfo{Token: &types.Token{TokenType: types.TokenTypeWorkerPrivate, WorkerID: "worker"}, Workspace: &types.Workspace{ExternalId: "own"}})
	route := func(transport string) *pb.BackendRoute {
		return &pb.BackendRoute{RouteId: types.BackendRouteID("machine", "worker", "container", types.BackendRouteKindContainer, 80), WorkspaceId: "own", WorkerId: "worker", MachineId: "machine", ContainerId: "container", Kind: types.BackendRouteKindContainer, Port: 80, Transport: transport}
	}
	for name, req := range map[string]any{
		"raw container address":  &pb.SetContainerAddressRequest{ContainerId: "container", Address: "100.64.0.20:6379"},
		"raw worker address":     &pb.SetWorkerAddressRequest{ContainerId: "container", Address: "100.64.0.20:6379"},
		"direct transport":       &pb.SetContainerAddressRequest{ContainerId: "container", Route: route(types.BackendRouteTransportDirect)},
		"local direct transport": &pb.SetContainerAddressRequest{ContainerId: "container", Route: route(types.BackendRouteTransportLocalDirect)},
		"omitted transport":      &pb.SetContainerAddressRequest{ContainerId: "container", Route: route("")},
		"raw map":                &pb.SetContainerAddressMapRequest{ContainerId: "container", AddressMap: map[int32]string{80: "100.64.0.20:6379"}},
		"partly covered map":     &pb.SetContainerAddressMapRequest{ContainerId: "container", AddressMap: map[int32]string{80: "127.0.0.1:80", 81: "100.64.0.20:6379"}, Routes: []*pb.BackendRoute{route(types.BackendRouteTransportTSNet)}},
	} {
		t.Run(name, func(t *testing.T) {
			if got := status.Code(service.AuthorizeRequest(ctx, req)); got != codes.PermissionDenied {
				t.Fatalf("private address publication = %v, want PermissionDenied", got)
			}
		})
	}
	worker := &WorkerRepositoryService{containerRepo: &authorizationContainerRepo{state: state}}
	for _, transport := range []string{"", types.BackendRouteTransportDirect, types.BackendRouteTransportLocalDirect} {
		t.Run("claim/"+transport, func(t *testing.T) {
			var r *pb.BackendRoute
			if transport != "" {
				r = route(transport)
				r.Kind = types.BackendRouteKindWorker
				r.ContainerId = ""
				r.RouteId = types.BackendRouteID("machine", "worker", "", types.BackendRouteKindWorker, 80)
			}
			req := &pb.ClaimContainerRequest{WorkerId: "worker", ContainerId: "container", WorkerAddress: &pb.SetWorkerAddressRequest{ContainerId: "container", Address: "100.64.0.20:6379", Route: r}}
			if got := status.Code(worker.AuthorizeRequest(ctx, req)); got != codes.PermissionDenied {
				t.Fatalf("private claim address = %v, want PermissionDenied", got)
			}
		})
	}
}

func TestPrivateAgentRoutesAndTrustedDirectRoutesRemainAllowed(t *testing.T) {
	state := &types.ContainerState{ContainerId: "container", WorkspaceId: "own", WorkerId: "worker", MachineId: "machine"}
	service := &ContainerRepositoryService{containerRepo: &authorizationContainerRepo{state: state}}
	for _, tokenType := range []string{types.TokenTypeWorkerPrivate, types.TokenTypeWorker} {
		ctx := auth.ContextWithAuthInfo(context.Background(), &auth.AuthInfo{Token: &types.Token{TokenType: tokenType, WorkerID: "worker"}, Workspace: &types.Workspace{ExternalId: "own"}})
		transport := types.BackendRouteTransportTSNet
		if tokenType == types.TokenTypeWorker {
			transport = types.BackendRouteTransportDirect
		}
		route := &pb.BackendRoute{RouteId: types.BackendRouteID("machine", "worker", "container", types.BackendRouteKindContainer, 80), WorkspaceId: "own", WorkerId: "worker", MachineId: "machine", ContainerId: "container", Kind: types.BackendRouteKindContainer, Port: 80, Transport: transport}
		for _, request := range []any{
			&pb.SetContainerAddressRequest{ContainerId: "container", Route: route},
			&pb.SetContainerAddressMapRequest{ContainerId: "container", AddressMap: map[int32]string{80: "127.0.0.1:80"}, Routes: []*pb.BackendRoute{route}},
			&pb.SetContainerAddressRequest{ContainerId: "container"},
			&pb.SetContainerAddressMapRequest{ContainerId: "container"},
		} {
			if err := service.AuthorizeRequest(ctx, request); err != nil {
				t.Fatalf("%s %T: %v", tokenType, request, err)
			}
		}
	}
}

func TestPrivateWorkerAddsPortWithRetainedRoutes(t *testing.T) {
	state := &types.ContainerState{ContainerId: "container", WorkspaceId: "own", WorkerId: "worker", MachineId: "machine"}
	ctx := auth.ContextWithAuthInfo(context.Background(), &auth.AuthInfo{Token: &types.Token{TokenType: types.TokenTypeWorkerPrivate, WorkerID: "worker"}, Workspace: &types.Workspace{ExternalId: "own"}})
	route := types.BackendRoute{RouteID: types.BackendRouteID("machine", "worker", "container", types.BackendRouteKindContainer, 80), WorkspaceID: "own", WorkerID: "worker", MachineID: "machine", ContainerID: "container", Kind: types.BackendRouteKindContainer, Port: 80}
	added := &pb.BackendRoute{RouteId: types.BackendRouteID("machine", "worker", "container", types.BackendRouteKindContainer, 81), WorkspaceId: "own", WorkerId: "worker", MachineId: "machine", ContainerId: "container", Kind: types.BackendRouteKindContainer, Port: 81, Transport: types.BackendRouteTransportTSNet}
	request := &pb.SetContainerAddressMapRequest{ContainerId: "container", AddressMap: map[int32]string{80: types.BackendRouteAddress(route.RouteID), 81: "127.0.0.1:81"}, Routes: []*pb.BackendRoute{added}}
	for _, transport := range []string{types.BackendRouteTransportTSNet, types.BackendRouteTransportDirect, "missing"} {
		t.Run(transport, func(t *testing.T) {
			route.Transport = transport
			repo := &authorizationContainerRepo{state: state, route: &route}
			if transport == "missing" {
				repo.route = nil
			}
			err := (&ContainerRepositoryService{containerRepo: repo}).AuthorizeRequest(ctx, request)
			want := codes.PermissionDenied
			if transport == types.BackendRouteTransportTSNet {
				want = codes.OK
			}
			require.Equal(t, want, status.Code(err))
		})
	}
}

// separateAddressesContainerRepo is a repository that can only write a
// container's address map and its address one after the other.
type separateAddressesContainerRepo struct {
	repository.ContainerRepository
	writes     []string
	failingMap error
}

func (r *separateAddressesContainerRepo) SetBackendRoutes(context.Context, []types.BackendRoute) error {
	return nil
}

func (r *separateAddressesContainerRepo) SetContainerAddressMap(containerID string, addressMap map[int32]string) error {
	if r.failingMap != nil {
		return r.failingMap
	}
	r.writes = append(r.writes, fmt.Sprintf("map %s %v", containerID, addressMap))
	return nil
}

func (r *separateAddressesContainerRepo) SetContainerAddress(containerID, address string) error {
	r.writes = append(r.writes, fmt.Sprintf("address %s %s", containerID, address))
	return nil
}

func TestContainerAddressMapAndPrimaryAddressAreWrittenInThatOrder(t *testing.T) {
	ctx := context.Background()
	request := &pb.SetContainerAddressMapRequest{ContainerId: "container", AddressMap: map[int32]string{80: "10.0.0.1:30080"}, PrimaryPort: 80}

	repo := &separateAddressesContainerRepo{}
	response, err := (&ContainerRepositoryService{containerRepo: repo}).SetContainerAddressMap(ctx, request)
	require.NoError(t, err)
	require.True(t, response.Ok, response.ErrorMsg)
	require.Equal(t, []string{"map container map[80:10.0.0.1:30080]", "address container 10.0.0.1:30080"}, repo.writes)

	// A map that cannot be written is reported, and the address not written.
	repo = &separateAddressesContainerRepo{failingMap: errors.New("redis is away")}
	response, err = (&ContainerRepositoryService{containerRepo: repo}).SetContainerAddressMap(ctx, request)
	require.NoError(t, err)
	require.False(t, response.Ok)
	require.Equal(t, "redis is away", response.ErrorMsg)
	require.Empty(t, repo.writes)
}

func TestContainerWithoutAPrimaryPortGetsOnlyItsAddressMap(t *testing.T) {
	rdb, err := repository.NewRedisClientForTest()
	require.NoError(t, err)
	containers := repository.NewContainerRedisRepositoryForTest(rdb)

	response, err := (&ContainerRepositoryService{containerRepo: containers}).SetContainerAddressMap(context.Background(),
		&pb.SetContainerAddressMapRequest{ContainerId: "container", AddressMap: map[int32]string{80: "10.0.0.1:30080"}})
	require.NoError(t, err)
	require.True(t, response.Ok, response.ErrorMsg)
	addressMap, err := containers.GetContainerAddressMap("container")
	require.NoError(t, err)
	require.Equal(t, map[int32]string{80: "10.0.0.1:30080"}, addressMap)
	_, err = containers.GetContainerAddress("container")
	require.ErrorIs(t, err, redis.Nil)
}
