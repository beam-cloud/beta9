package repository_services

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/compute"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

type repositoryAuthWorkspaceRepo struct{ repository.WorkspaceRepository }

func (repositoryAuthWorkspaceRepo) AuthorizeToken(key string) (*types.Token, *types.Workspace, error) {
	storageID := uint(1)
	return &types.Token{TokenType: key, ExternalId: "assigned", Active: true}, &types.Workspace{
		ExternalId: "own", Storage: &types.WorkspaceStorage{Id: &storageID},
	}, nil
}

func repositoryAuthContext(tokenType string) context.Context {
	return metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer "+tokenType)
}

func repositoryAuthClient(t *testing.T, workers *WorkerRepositoryService, containers *ContainerRepositoryService) *grpc.ClientConn {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	interceptor := auth.NewAuthInterceptor(types.AppConfig{}, nil, repositoryAuthWorkspaceRepo{})
	server := grpc.NewServer(grpc.UnaryInterceptor(interceptor.Unary()), grpc.StreamInterceptor(interceptor.Stream()))
	pb.RegisterWorkerRepositoryServiceServer(server, workers)
	pb.RegisterContainerRepositoryServiceServer(server, containers)
	go server.Serve(listener)
	t.Cleanup(server.Stop)
	conn, err := grpc.NewClient("passthrough:///repository-auth", grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return conn
}

// Exercise every registered method through the production interceptor. Nil
// repositories make any access before authorization fail the test.
func TestRepositoryRPCsRejectCustomerTokensBeforeAccess(t *testing.T) {
	conn := repositoryAuthClient(t, &WorkerRepositoryService{}, &ContainerRepositoryService{})
	for _, tokenType := range []string{"", types.TokenTypeWorkspacePrimary, types.TokenTypeWorkspace, types.TokenTypeWorkspaceRestricted, types.TokenTypeMachine, types.TokenTypePlatformDeployer, types.TokenTypeClusterAdmin} {
		for _, file := range []protoreflect.FileDescriptor{pb.File_container_repo_proto, pb.File_worker_repo_proto} {
			service := file.Services().Get(0)
			for i := 0; i < service.Methods().Len(); i++ {
				method := service.Methods().Get(i)
				t.Run(tokenType+"/"+string(method.FullName()), func(t *testing.T) {
					ctx, cancel := context.WithTimeout(context.Background(), time.Second)
					defer cancel()
					if tokenType != "" {
						ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+tokenType)
					}
					name := "/" + string(service.FullName()) + "/" + string(method.Name())
					req, response := dynamicpb.NewMessage(method.Input()), dynamicpb.NewMessage(method.Output())
					var err error
					if method.IsStreamingServer() {
						var stream grpc.ClientStream
						stream, err = conn.NewStream(ctx, &grpc.StreamDesc{ServerStreams: true}, name)
						require.NoError(t, err)
						if err := stream.SendMsg(req); err != nil {
							require.ErrorIs(t, err, io.EOF)
						}
						require.NoError(t, stream.CloseSend())
						err = stream.RecvMsg(response)
					} else {
						err = conn.Invoke(ctx, name, req, response)
					}
					if err != nil {
						require.Contains(t, []codes.Code{codes.Unauthenticated, codes.PermissionDenied}, status.Code(err))
					} else {
						// Cache RPCs authenticate their coordinator secret in the handler.
						fields := response.Descriptor().Fields()
						require.NotNil(t, fields.ByName("ok"))
						require.NotNil(t, fields.ByName("error_msg"))
						require.False(t, response.Get(fields.ByName("ok")).Bool())
						require.NotEmpty(t, response.Get(fields.ByName("error_msg")).String())
					}
				})
			}
		}
	}
}

type streamAuthWorkerRepo struct{ *lifecycleWorkerRepo }

func (*streamAuthWorkerRepo) RecoverPendingContainerRequests(string) error {
	return status.Error(codes.FailedPrecondition, "authorized request reached worker repository")
}

func TestPrivateWorkerStreamsEnforceOwnership(t *testing.T) {
	for _, workspace := range []string{"own", "other"} {
		t.Run(workspace, func(t *testing.T) {
			service := &WorkerRepositoryService{
				workerRepo:  &streamAuthWorkerRepo{&lifecycleWorkerRepo{worker: &types.Worker{Id: "worker", WorkspaceId: workspace}}},
				computeRepo: &lifecycleComputeRepo{slots: []*compute.AgentWorkerSlotState{{WorkerID: "worker", WorkerTokenID: "assigned"}}},
			}
			client := pb.NewWorkerRepositoryServiceClient(repositoryAuthClient(t, service, &ContainerRepositoryService{}))
			ctx := repositoryAuthContext(types.TokenTypeWorkerPrivate)
			requests, err := client.GetNextContainerRequest(ctx, &pb.GetNextContainerRequestRequest{WorkerId: "worker"})
			require.NoError(t, err)
			_, err = requests.Recv()
			want := codes.PermissionDenied
			if workspace == "own" {
				want = codes.FailedPrecondition
			}
			require.Equal(t, want, status.Code(err))
			events, err := client.StreamWorkerEvents(ctx, &pb.StreamWorkerEventsRequest{WorkerId: "worker"})
			require.NoError(t, err)
			_, err = events.Recv()
			require.Equal(t, want, status.Code(err))
		})
	}
}

type authorizationContainerRepo struct {
	repository.ContainerRepository
	state *types.ContainerState
}

func (r *authorizationContainerRepo) GetContainerState(string) (*types.ContainerState, error) {
	return r.state, nil
}

func TestPrivateWorkerAuthorization(t *testing.T) {
	for _, tc := range []struct {
		name, workerWorkspace, assignedToken string
		allowed                              bool
	}{
		{name: "foreign worker", workerWorkspace: "other", assignedToken: "assigned"},
		{name: "unassigned token", workerWorkspace: "own", assignedToken: "another"},
		{name: "assigned worker", workerWorkspace: "own", assignedToken: "assigned", allowed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			service := &WorkerRepositoryService{
				workerRepo:  &lifecycleWorkerRepo{worker: &types.Worker{Id: "worker", WorkspaceId: tc.workerWorkspace, PoolName: "pool", MachineId: "machine"}},
				computeRepo: &lifecycleComputeRepo{slots: []*compute.AgentWorkerSlotState{{WorkerID: "worker", WorkerTokenID: tc.assignedToken}}},
			}
			client := pb.NewWorkerRepositoryServiceClient(repositoryAuthClient(t, service, &ContainerRepositoryService{}))
			ctx := repositoryAuthContext(types.TokenTypeWorkerPrivate)
			response, err := client.GetWorkerById(ctx, &pb.GetWorkerByIdRequest{WorkerId: "worker"})
			if tc.allowed {
				require.NoError(t, err)
				require.True(t, response.Ok)
				_, err = client.SetWorkerKeepAlive(ctx, &pb.SetWorkerKeepAliveRequest{WorkerId: "worker", MachineId: "foreign-machine"})
			}
			require.Equal(t, codes.PermissionDenied, status.Code(err))
		})
	}
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

func TestPrivateWorkerNetworkIsolation(t *testing.T) {
	server := miniredis.RunT(t)
	rdb, err := common.NewRedisClient(types.RedisConfig{Addrs: []string{server.Addr()}, Mode: types.RedisModeSingle})
	require.NoError(t, err)
	t.Cleanup(func() { rdb.Close() })
	service := &WorkerRepositoryService{workerRepo: repository.NewWorkerRedisRepositoryForTest(rdb)}
	// Use separate authenticated contexts to exercise the Redis namespace itself.
	contexts := []context.Context{
		auth.ContextWithAuthInfo(context.Background(), &auth.AuthInfo{Token: &types.Token{TokenType: types.TokenTypeWorker}}),
	}
	for _, workspace := range []string{"one", "two"} {
		contexts = append(contexts, auth.ContextWithAuthInfo(context.Background(), &auth.AuthInfo{
			Token: &types.Token{TokenType: types.TokenTypeWorkerPrivate}, Workspace: &types.Workspace{ExternalId: workspace},
		}))
	}
	ips := []string{"10.0.0.10", "10.0.0.11", "10.0.0.12"}
	const prefix = "cluster:beta9:node:shared"
	for i, ctx := range contexts {
		response, err := service.SetContainerIp(ctx, &pb.SetContainerIpRequest{NetworkPrefix: prefix, ContainerId: "slot", IpAddress: ips[i]})
		require.NoError(t, err)
		require.True(t, response.Ok)
		lock, err := service.SetNetworkLock(ctx, &pb.SetNetworkLockRequest{NetworkPrefix: prefix, Ttl: 10})
		require.NoError(t, err)
		require.True(t, lock.Ok)
	}
	for i, ctx := range contexts {
		response, err := service.GetContainerIps(ctx, &pb.GetContainerIpsRequest{NetworkPrefix: prefix})
		require.NoError(t, err)
		require.True(t, response.Ok)
		require.Equal(t, []string{ips[i]}, response.Ips)
	}
	removed, err := service.RemoveContainerIp(contexts[1], &pb.RemoveContainerIpRequest{NetworkPrefix: prefix, ContainerId: "slot"})
	require.NoError(t, err)
	require.True(t, removed.Ok)
	for _, i := range []int{0, 2} {
		response, err := service.GetContainerIp(contexts[i], &pb.GetContainerIpRequest{NetworkPrefix: prefix, ContainerId: "slot"})
		require.NoError(t, err)
		require.True(t, response.Ok)
		require.Equal(t, ips[i], response.IpAddress)
	}
}
