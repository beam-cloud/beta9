package repository_services

import (
	"context"
	"testing"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

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
