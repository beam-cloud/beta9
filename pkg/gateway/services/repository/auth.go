package repository_services

import (
	"context"
	"strings"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	_ auth.RequestAuthorizer = (*ContainerRepositoryService)(nil)
	_ auth.RequestAuthorizer = (*WorkerRepositoryService)(nil)
)

type containerRequest interface{ GetContainerId() string }
type workerRequest interface{ GetWorkerId() string }

func repositoryWorkerAuth(ctx context.Context) (*auth.AuthInfo, error) {
	info, ok := auth.AuthInfoFromContext(ctx)
	if !ok || info == nil || info.Token == nil {
		return nil, status.Error(codes.Unauthenticated, "worker token is required")
	}
	if !types.IsWorkerTokenType(info.Token.TokenType) {
		return nil, status.Error(codes.PermissionDenied, "worker token is required")
	}
	if info.Token.TokenType == types.TokenTypeWorkerPrivate && (info.Workspace == nil || info.Workspace.ExternalId == "") {
		return nil, status.Error(codes.PermissionDenied, "worker workspace is required")
	}
	return info, nil
}

// AuthorizeRequest is called by the gateway's auth interceptor. Cluster workers
// serve multiple tenants; private workers are restricted to their workspace.
func (s *ContainerRepositoryService) AuthorizeRequest(ctx context.Context, req interface{}) error {
	info, err := repositoryWorkerAuth(ctx)
	if err != nil || info.Token.TokenType != types.TokenTypeWorkerPrivate {
		return err
	}
	request, ok := req.(containerRequest)
	if !ok {
		return status.Error(codes.PermissionDenied, "container is required")
	}
	state, err := containerStateForWorkspace(s.containerRepo, request.GetContainerId(), info.Workspace.ExternalId)
	if err != nil {
		return err
	}
	var routes []*pb.BackendRoute
	switch req := req.(type) {
	case *pb.SetContainerAddressRequest:
		routes = []*pb.BackendRoute{req.Route}
	case *pb.SetContainerAddressMapRequest:
		routes = req.Routes
	case *pb.SetWorkerAddressRequest:
		routes = []*pb.BackendRoute{req.Route}
	}
	for _, route := range routes {
		if err := authorizeBackendRoute(state, route); err != nil {
			return err
		}
	}
	return nil
}

func (s *WorkerRepositoryService) AuthorizeRequest(ctx context.Context, req interface{}) error {
	info, err := repositoryWorkerAuth(ctx)
	if err != nil || info.Token.TokenType != types.TokenTypeWorkerPrivate {
		return err
	}
	if request, ok := req.(workerRequest); ok {
		worker, err := s.workerRepo.GetWorkerById(request.GetWorkerId())
		if err != nil || worker == nil || worker.WorkspaceId != info.Workspace.ExternalId {
			return status.Error(codes.PermissionDenied, "worker is not in token workspace")
		}
		if err := s.authorizeWorkerLifecycleWorker(ctx, info, request.GetWorkerId(), worker); err != nil {
			return status.Error(codes.PermissionDenied, "worker token is not assigned to worker")
		}
		if request, ok := req.(*pb.SetWorkerKeepAliveRequest); ok && request.MachineId != "" && strings.TrimSpace(request.MachineId) != worker.MachineId {
			return status.Error(codes.PermissionDenied, "worker cannot change its machine")
		}
	}
	switch req := req.(type) {
	case *pb.AddContainerToWorkerRequest:
		_, err = containerStateForWorkspace(s.containerRepo, req.ContainerId, info.Workspace.ExternalId)
	case *pb.ClaimContainerRequest:
		var state *types.ContainerState
		state, err = containerStateForWorkspace(s.containerRepo, req.ContainerId, info.Workspace.ExternalId)
		if err == nil && req.WorkerAddress != nil {
			err = authorizeBackendRoute(state, req.WorkerAddress.Route)
		}
	}
	return err
}

func containerStateForWorkspace(repo repository.ContainerRepository, containerID, workspaceID string) (*types.ContainerState, error) {
	state, err := repo.GetContainerState(containerID)
	if err != nil || state == nil || state.WorkspaceId != workspaceID {
		return nil, status.Error(codes.PermissionDenied, "container is not in worker workspace")
	}
	return state, nil
}

func authorizeBackendRoute(state *types.ContainerState, route *pb.BackendRoute) error {
	if route == nil {
		return nil
	}
	if route.WorkspaceId != state.WorkspaceId || route.WorkerId != state.WorkerId || route.MachineId != state.MachineId {
		return status.Error(codes.PermissionDenied, "route does not belong to container")
	}
	containerID := state.ContainerId
	if route.Kind == types.BackendRouteKindWorker {
		containerID = ""
	} else if route.Kind != types.BackendRouteKindContainer {
		return status.Error(codes.PermissionDenied, "invalid route kind")
	}
	if route.ContainerId != containerID || route.RouteId != types.BackendRouteID(state.MachineId, state.WorkerId, containerID, route.Kind, route.Port) {
		return status.Error(codes.PermissionDenied, "invalid route identity")
	}
	return nil
}

func scopedWorkerNetwork(ctx context.Context, prefix string) string {
	// Authentication and resource permissions are enforced by AuthorizeRequest.
	info, ok := auth.AuthInfoFromContext(ctx)
	if ok && info != nil && info.Token != nil && info.Token.TokenType == types.TokenTypeWorkerPrivate {
		return "workspace:" + info.Workspace.ExternalId + ":" + prefix
	}
	return prefix
}
