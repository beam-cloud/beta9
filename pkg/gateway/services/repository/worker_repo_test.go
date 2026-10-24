package repository_services

import (
	"context"
	"encoding/json"
	"errors"
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
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

type cordonWorkerRepo struct {
	repository.WorkerRepository
	cordoned bool
	disabled bool
	err      error
}

func (r *cordonWorkerRepo) GetWorkerById(id string) (*types.Worker, error) {
	return &types.Worker{Id: id}, nil
}

func (r *cordonWorkerRepo) SetWorkerCordon(_ string, cordon bool) error {
	r.cordoned = cordon
	return r.err
}

func (r *cordonWorkerRepo) UpdateWorkerStatus(_ string, status types.WorkerStatus) error {
	r.disabled = status == types.WorkerStatusDisabled
	return r.err
}

func TestDisableWorker(t *testing.T) {
	for _, cordon := range []bool{false, true} {
		for _, repoErr := range []error{nil, errors.New("repository unavailable")} {
			repo := &cordonWorkerRepo{err: repoErr}
			service := &WorkerRepositoryService{workerRepo: repo}
			response, err := service.DisableWorker(context.Background(), &pb.DisableWorkerRequest{WorkerId: "worker-1", Cordon: cordon})
			require.NoError(t, err)
			require.Equal(t, repoErr == nil, response.Ok)
			require.Equal(t, cordon, repo.cordoned)
			require.Equal(t, !cordon, repo.disabled)
			if repoErr != nil {
				require.Equal(t, repoErr.Error(), response.ErrorMsg)
			}
		}
	}
}

func TestGetWorkerMeteringConfigRequiresAssignedManagedSlot(t *testing.T) {
	service := &WorkerRepositoryService{
		workerRepo: &lifecycleWorkerRepo{worker: &types.Worker{
			Id: "worker-1", WorkspaceId: "workspace-1", PoolName: "pool-1", MachineId: "machine-1", ControlPlaneManaged: true,
		}},
		computeRepo: &lifecycleComputeRepo{slots: []*compute.AgentWorkerSlotState{{
			WorkerID: "worker-1", WorkerTokenID: "token-1",
		}}},
		appConfig: types.AppConfig{Monitoring: types.MonitoringConfig{
			MetricsCollector:        string(types.MetricsCollectorOpenMeter),
			OpenMeter:               types.OpenMeterConfig{ServerUrl: "https://meter.example.com", ApiKey: "meter-key"},
			ContainerCostHookConfig: types.ContainerCostHookConfig{Endpoint: "https://billing.example.com/quote", Token: "quote-key"},
		}},
	}
	ctx := auth.ContextWithAuthInfo(context.Background(), &auth.AuthInfo{
		Token:     &types.Token{ExternalId: "token-1", TokenType: types.TokenTypeWorker},
		Workspace: &types.Workspace{ExternalId: "workspace-1"},
	})

	response, err := service.GetWorkerMeteringConfig(ctx, &pb.GetWorkerMeteringConfigRequest{WorkerId: "worker-1"})
	require.NoError(t, err)
	require.True(t, response.Ok)
	var metering types.MonitoringConfig
	require.NoError(t, json.Unmarshal([]byte(response.MeteringJson), &metering))
	require.Equal(t, service.appConfig.Monitoring, metering)

	service.computeRepo = &lifecycleComputeRepo{slots: []*compute.AgentWorkerSlotState{{WorkerID: "worker-1", WorkerTokenID: "another-token"}}}
	response, err = service.GetWorkerMeteringConfig(ctx, &pb.GetWorkerMeteringConfigRequest{WorkerId: "worker-1"})
	require.NoError(t, err)
	require.False(t, response.Ok)
}

func TestGetWorkerServiceProxyConfigNamesThePublicTCPListener(t *testing.T) {
	request := &pb.GetWorkerServiceProxyConfigRequest{WorkerId: "worker-1"}
	service := &WorkerRepositoryService{}
	service.appConfig.Abstractions.Pod.TCP = types.PodTCPConfig{
		Enabled: true, ExternalHost: "svc.beam.cloud", ExternalPort: 443, Port: 1995,
		CertFile: "/etc/tls/tls.crt", ServiceProxyTarget: "beta9-gateway:1995",
	}

	response, err := service.GetWorkerServiceProxyConfig(context.Background(), request)
	require.NoError(t, err)
	require.True(t, response.Ok)
	require.Equal(t, "svc.beam.cloud", response.ExternalHost)
	require.EqualValues(t, 443, response.ExternalPort)
	require.True(t, response.VerifyTls, "a gateway with its own certificate is verified")

	service.appConfig.Abstractions.Pod.TCP.CertFile = ""
	response, err = service.GetWorkerServiceProxyConfig(context.Background(), request)
	require.NoError(t, err)
	require.False(t, response.VerifyTls)

	for _, host := range []string{"", "203.0.113.7"} {
		service.appConfig.Abstractions.Pod.TCP.ExternalHost = host
		response, err = service.GetWorkerServiceProxyConfig(context.Background(), request)
		require.NoError(t, err)
		require.False(t, response.Ok, "external host %q", host)
	}
}

type claimWorkerRepo struct {
	repository.WorkerRepository
	claimErr error
	moves    []*pb.MoveContainerIpRequest
	moveErr  error
}

func (r *claimWorkerRepo) MoveContainerIp(prefix, from, to, ip string) error {
	r.moves = append(r.moves, &pb.MoveContainerIpRequest{NetworkPrefix: prefix, FromContainerId: from, ToContainerId: to, IpAddress: ip})
	return r.moveErr
}

func (r *claimWorkerRepo) AddContainerToWorker(workerID, containerID, deliveryToken string) error {
	return r.claimErr
}

// claimContainerRepo mirrors the redis repository's transition rules: a
// renewal of PENDING is refused silently once the container is STOPPING.
type claimContainerRepo struct {
	repository.ContainerRepository
	state         *types.ContainerState
	beforeUpdate  func()
	updates       []types.ContainerStatus
	updateExpiry  []int64
	getStateCalls int
	address       string
	addressErr    error
}

func (r *claimContainerRepo) SetWorkerAddress(_ string, address string) error {
	r.address = address
	return r.addressErr
}

func (r *claimContainerRepo) UpdateContainerStatus(containerID string, status types.ContainerStatus, expirySeconds int64) error {
	if r.beforeUpdate != nil {
		r.beforeUpdate()
	}
	r.updates = append(r.updates, status)
	r.updateExpiry = append(r.updateExpiry, expirySeconds)
	if r.state == nil {
		return &types.ErrContainerStateNotFound{ContainerId: containerID}
	}
	if containerStatusTransitionAllowedForTest(types.ContainerStatus(r.state.Status), status) {
		r.state.Status = status
	}
	return nil
}

func (r *claimContainerRepo) GetContainerState(containerID string) (*types.ContainerState, error) {
	r.getStateCalls++
	if r.state == nil {
		return nil, &types.ErrContainerStateNotFound{ContainerId: containerID}
	}
	copied := *r.state
	return &copied, nil
}

func containerStatusTransitionAllowedForTest(stored, requested types.ContainerStatus) bool {
	switch stored {
	case types.ContainerStatusPending:
		return true
	case types.ContainerStatusRunning:
		return requested != types.ContainerStatusPending
	default:
		return requested == types.ContainerStatusStopping
	}
}

func TestClaimContainerReturnsStoppingWhenStopRacesTheRenewal(t *testing.T) {
	containerRepo := &claimContainerRepo{
		state: &types.ContainerState{ContainerId: "container-id", WorkspaceId: "workspace-id", StubId: "stub-id", Status: types.ContainerStatusPending},
	}
	// The scheduler stops the container after the worker's claim is accepted
	// but before the pending lease is renewed.
	containerRepo.beforeUpdate = func() { containerRepo.state.Status = types.ContainerStatusStopping }
	service := &WorkerRepositoryService{workerRepo: &claimWorkerRepo{}, containerRepo: containerRepo}

	resp, err := service.ClaimContainer(context.Background(), &pb.ClaimContainerRequest{WorkerId: "worker-1", ContainerId: "container-id", DeliveryToken: "token"})

	require.NoError(t, err)
	require.True(t, resp.Claimed)
	require.True(t, resp.Ok)
	require.Equal(t, []types.ContainerStatus{types.ContainerStatusPending}, containerRepo.updates)
	require.Equal(t, string(types.ContainerStatusStopping), resp.State.Status, "the claim must report the persisted status, not the pre-renewal snapshot")
}

func TestClaimContainerRenewsPendingLease(t *testing.T) {
	containerRepo := &claimContainerRepo{
		state: &types.ContainerState{ContainerId: "container-id", WorkspaceId: "workspace-id", StubId: "stub-id", Status: types.ContainerStatusPending},
	}
	service := &WorkerRepositoryService{workerRepo: &claimWorkerRepo{}, containerRepo: containerRepo}

	resp, err := service.ClaimContainer(context.Background(), &pb.ClaimContainerRequest{WorkerId: "worker-1", ContainerId: "container-id", DeliveryToken: "token"})

	require.NoError(t, err)
	require.True(t, resp.Ok)
	require.Equal(t, string(types.ContainerStatusPending), resp.State.Status)
	require.Equal(t, []int64{int64(types.ContainerStateTtlSWhilePending)}, containerRepo.updateExpiry)
	require.Equal(t, 1, containerRepo.getStateCalls)
}

func TestClaimContainerReportsMissingStateAfterClaim(t *testing.T) {
	service := &WorkerRepositoryService{workerRepo: &claimWorkerRepo{}, containerRepo: &claimContainerRepo{}}

	resp, err := service.ClaimContainer(context.Background(), &pb.ClaimContainerRequest{WorkerId: "worker-1", ContainerId: "container-id", DeliveryToken: "token"})

	require.NoError(t, err)
	require.True(t, resp.Claimed)
	require.False(t, resp.Ok)
	require.True(t, (&types.ErrContainerStateNotFound{}).From(errors.New(resp.ErrorMsg)))
}

func TestClaimContainerPublishesStartupOnlyAfterAcceptance(t *testing.T) {
	for _, tc := range []struct {
		name                                              string
		claimErr, moveErr, addressErr                     error
		stopping                                          bool
		wantClaimed, wantMoved, wantAddress, wantPrepared bool
	}{
		{name: "accepted", wantClaimed: true, wantMoved: true, wantAddress: true, wantPrepared: true},
		{name: "rejected", claimErr: errors.New("delivery owned elsewhere")},
		{name: "stopping", stopping: true, wantClaimed: true},
		{name: "move failed", moveErr: errors.New("source ownership changed"), wantClaimed: true, wantMoved: true},
		{name: "address failed", addressErr: errors.New("route unavailable"), wantClaimed: true, wantMoved: true, wantAddress: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			worker := &claimWorkerRepo{claimErr: tc.claimErr, moveErr: tc.moveErr}
			containers := &claimContainerRepo{state: &types.ContainerState{ContainerId: "c1", WorkspaceId: "ws1", Status: types.ContainerStatusPending}, addressErr: tc.addressErr}
			if tc.stopping {
				containers.state.Status = types.ContainerStatusStopping
			}
			service := &WorkerRepositoryService{workerRepo: worker, containerRepo: containers}
			resp, err := service.ClaimContainer(context.Background(), &pb.ClaimContainerRequest{
				WorkerId: "w1", ContainerId: "c1", DeliveryToken: "token",
				Network:       &pb.MoveContainerIpRequest{NetworkPrefix: "node1", FromContainerId: "network-slot:w1:s1", ToContainerId: "c1", IpAddress: "192.168.0.2"},
				WorkerAddress: &pb.SetWorkerAddressRequest{ContainerId: "c1", Address: "worker:1234"},
			})
			require.NoError(t, err)
			require.Equal(t, tc.wantClaimed, resp.Claimed)
			require.Equal(t, tc.wantMoved, len(worker.moves) == 1)
			require.Equal(t, tc.wantAddress, containers.address != "")
			require.Equal(t, tc.wantPrepared, resp.StartupPrepared)
		})
	}
}

func TestClaimContainerRefusesForeignStartupResources(t *testing.T) {
	for _, req := range []*pb.ClaimContainerRequest{
		{WorkerId: "w1", ContainerId: "c1", WorkerAddress: &pb.SetWorkerAddressRequest{ContainerId: "other"}},
		{WorkerId: "w1", ContainerId: "c1", Network: &pb.MoveContainerIpRequest{FromContainerId: "network-slot:w2:s1", ToContainerId: "c1"}},
		{WorkerId: "w1", ContainerId: "c1", Network: &pb.MoveContainerIpRequest{FromContainerId: "network-slot:w1:s1", ToContainerId: "other"}},
		{WorkerId: "w1", ContainerId: "c1", WorkerAddress: &pb.SetWorkerAddressRequest{ContainerId: "c1", Route: &pb.BackendRoute{WorkerId: "w2", Kind: types.BackendRouteKindWorker}}},
	} {
		// Missing repositories make any acceptance or publication fail the test.
		resp, err := (&WorkerRepositoryService{}).ClaimContainer(context.Background(), req)
		require.NoError(t, err)
		require.False(t, resp.Claimed)
		require.False(t, resp.Ok)
	}
}

type updateStatusContainerRepo struct {
	repository.ContainerRepository
	updates int
}

func (r *updateStatusContainerRepo) UpdateContainerStatus(string, types.ContainerStatus, int64) error {
	r.updates++
	return nil
}

func TestUpdateContainerStatusRejectsNonPositiveExpiryBeforeWriting(t *testing.T) {
	containerRepo := &updateStatusContainerRepo{}
	service := &ContainerRepositoryService{containerRepo: containerRepo}

	for _, expiry := range []int64{0, -5} {
		resp, err := service.UpdateContainerStatus(context.Background(), &pb.UpdateContainerStatusRequest{
			ContainerId:   "container-id",
			Status:        string(types.ContainerStatusRunning),
			ExpirySeconds: expiry,
		})
		require.NoError(t, err)
		require.False(t, resp.Ok)
		require.Contains(t, resp.ErrorMsg, "expiry_seconds")
	}
	require.Zero(t, containerRepo.updates)
}

// keepAliveWorkerRepo records how often the pool is consulted for a headroom
// answer and can fail the worker lookup.
type keepAliveWorkerRepo struct {
	repository.WorkerRepository
	worker        *types.Worker
	lookupErr     error
	poolScans     int
	poolScanError error
}

func (r *keepAliveWorkerRepo) SetWorkerKeepAlive(workerId string, keepAlive types.WorkerKeepAlive) error {
	return nil
}

func (r *keepAliveWorkerRepo) GetWorkerById(workerId string) (*types.Worker, error) {
	if r.lookupErr != nil {
		return nil, r.lookupErr
	}
	return r.worker, nil
}

func (r *keepAliveWorkerRepo) GetAllWorkersInPool(poolName string) ([]*types.Worker, error) {
	r.poolScans++
	if r.poolScanError != nil {
		return nil, r.poolScanError
	}
	return []*types.Worker{r.worker}, nil
}

func headroomTestConfig() types.AppConfig {
	return types.AppConfig{Worker: types.WorkerConfig{Pools: map[string]types.WorkerPoolConfig{
		"default": {PoolSizing: types.WorkerPoolJobSpecPoolSizingConfig{MinFreeCPU: "1000m", MinFreeMemory: "1Gi", MinFreeGPU: "0"}},
	}}}
}

func TestSetWorkerKeepAliveOnlyScansThePoolForIdleWorkers(t *testing.T) {
	workerRepo := &keepAliveWorkerRepo{worker: &types.Worker{Id: "worker-1", PoolName: "default", Status: types.WorkerStatusAvailable, FreeCpu: 4000, FreeMemory: 8192}}
	service := &WorkerRepositoryService{workerRepo: workerRepo, appConfig: headroomTestConfig()}

	resp, err := service.SetWorkerKeepAlive(context.Background(), &pb.SetWorkerKeepAliveRequest{WorkerId: "worker-1"})
	require.NoError(t, err)
	require.True(t, resp.Ok)
	require.False(t, resp.PoolHeadroom, "a busy worker cannot spin down, so it is not told it holds headroom")
	require.Equal(t, 0, workerRepo.poolScans, "busy keepalives must not scan the pool")

	resp, err = service.SetWorkerKeepAlive(context.Background(), &pb.SetWorkerKeepAliveRequest{WorkerId: "worker-1", Idle: true})
	require.NoError(t, err)
	require.True(t, resp.Ok)
	require.True(t, resp.PoolHeadroom, "the only ready worker holds the pool's minimum")
	require.Equal(t, 1, workerRepo.poolScans)
}

func TestSetWorkerKeepAliveFailsClosedWhenHeadroomCannotBeDetermined(t *testing.T) {
	for name, repo := range map[string]*keepAliveWorkerRepo{
		"worker lookup fails": {lookupErr: errors.New("redis: connection refused")},
		"pool scan fails":     {worker: &types.Worker{Id: "worker-1", PoolName: "default", Status: types.WorkerStatusAvailable}, poolScanError: errors.New("redis: timeout")},
	} {
		t.Run(name, func(t *testing.T) {
			service := &WorkerRepositoryService{workerRepo: repo, appConfig: headroomTestConfig()}
			resp, err := service.SetWorkerKeepAlive(context.Background(), &pb.SetWorkerKeepAliveRequest{WorkerId: "worker-1", Idle: true})
			require.NoError(t, err)
			require.True(t, resp.Ok)
			require.True(t, resp.PoolHeadroom, "an idle worker must stay up until the pool can actually be read")
		})
	}
}

func TestClaimContainerRefusesForeignRouteWorkspaceAfterAcceptance(t *testing.T) {
	workers := &claimWorkerRepo{}
	containers := &claimContainerRepo{state: &types.ContainerState{ContainerId: "c1", WorkspaceId: "ws1", Status: types.ContainerStatusPending}}
	service := &WorkerRepositoryService{workerRepo: workers, containerRepo: containers}
	resp, err := service.ClaimContainer(context.Background(), &pb.ClaimContainerRequest{WorkerId: "w1", ContainerId: "c1", DeliveryToken: "token", Network: &pb.MoveContainerIpRequest{FromContainerId: "network-slot:w1:s1", ToContainerId: "c1", IpAddress: "192.168.0.2"}, WorkerAddress: &pb.SetWorkerAddressRequest{ContainerId: "c1", Route: &pb.BackendRoute{WorkerId: "w1", WorkspaceId: "other", Kind: types.BackendRouteKindWorker}}})
	require.NoError(t, err)
	require.True(t, resp.Claimed)
	require.False(t, resp.Ok)
	require.Contains(t, resp.ErrorMsg, "workspace")
	require.Empty(t, workers.moves)
	require.Empty(t, containers.address)
}

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

func TestRepositoryAuthorizationRequiresPrivateWorkspace(t *testing.T) {
	for _, workspace := range []*types.Workspace{nil, {}} {
		ctx := auth.ContextWithAuthInfo(context.Background(), &auth.AuthInfo{
			Token: &types.Token{TokenType: types.TokenTypeWorkerPrivate}, Workspace: workspace,
		})
		// No repositories: incomplete identities must be rejected before lookup.
		err := (&WorkerRepositoryService{}).AuthorizeRequest(ctx, &pb.GetWorkerByIdRequest{WorkerId: "worker"})
		require.Equal(t, codes.PermissionDenied, status.Code(err))
		err = (&ContainerRepositoryService{}).AuthorizeRequest(ctx, &pb.GetContainerStateRequest{ContainerId: "container"})
		require.Equal(t, codes.PermissionDenied, status.Code(err))
	}
}

func TestRepositoryAuthorizationRequiresResourceID(t *testing.T) {
	ctx := auth.ContextWithAuthInfo(context.Background(), &auth.AuthInfo{
		Token: &types.Token{TokenType: types.TokenTypeWorkerPrivate}, Workspace: &types.Workspace{ExternalId: "own"},
	})
	err := (&WorkerRepositoryService{}).AuthorizeRequest(ctx, &pb.GetWorkerByIdRequest{})
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	err = (&ContainerRepositoryService{}).AuthorizeRequest(ctx, &pb.GetContainerStateRequest{})
	require.Equal(t, codes.PermissionDenied, status.Code(err))
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
	for _, tokenType := range []string{"", types.TokenTypeWorkspacePrimary, types.TokenTypeWorkspace, types.TokenTypeWorkspaceRestricted, types.TokenTypePlatformDeployer, types.TokenTypeClusterAdmin} {
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

func workerNetworkRequests() map[string]interface{ GetNetworkPrefix() string } {
	const prefix = "cluster:beta9:node:shared"
	return map[string]interface{ GetNetworkPrefix() string }{
		"SetNetworkLock":            &pb.SetNetworkLockRequest{NetworkPrefix: prefix},
		"RemoveNetworkLock":         &pb.RemoveNetworkLockRequest{NetworkPrefix: prefix},
		"SetContainerIp":            &pb.SetContainerIpRequest{NetworkPrefix: prefix},
		"MoveContainerIp":           &pb.MoveContainerIpRequest{NetworkPrefix: prefix},
		"GetContainerIp":            &pb.GetContainerIpRequest{NetworkPrefix: prefix},
		"GetContainerIps":           &pb.GetContainerIpsRequest{NetworkPrefix: prefix},
		"GetContainerIpAssignments": &pb.GetContainerIpAssignmentsRequest{NetworkPrefix: prefix},
		"RemoveContainerIp":         &pb.RemoveContainerIpRequest{NetworkPrefix: prefix},
	}
}

func TestWorkerNetworkAuthorizationDoesNotAccessRepositories(t *testing.T) {
	// Nil repositories ensure authorization and scoping cannot add remote lookups.
	service := &WorkerRepositoryService{}
	for _, tokenType := range []string{types.TokenTypeWorker, types.TokenTypeWorkerPrivate} {
		ctx := auth.ContextWithAuthInfo(context.Background(), &auth.AuthInfo{
			Token: &types.Token{TokenType: tokenType}, Workspace: &types.Workspace{ExternalId: "own"},
		})
		for name, req := range workerNetworkRequests() {
			t.Run(tokenType+"/"+name, func(t *testing.T) {
				require.NoError(t, service.AuthorizeRequest(ctx, req))
				want := req.GetNetworkPrefix()
				if tokenType == types.TokenTypeWorkerPrivate {
					want = "workspace:own:" + want
				}
				require.Equal(t, want, service.scopedNetworkPrefix(ctx, req.GetNetworkPrefix()))
			})
		}
	}
}

// Benchmark only the added local work; transport, token validation and the
// existing Redis operations are outside the measured section.
func BenchmarkWorkerNetworkRequestPreparation(b *testing.B) {
	service := &WorkerRepositoryService{}
	for _, tokenType := range []string{types.TokenTypeWorker, types.TokenTypeWorkerPrivate} {
		ctx := auth.ContextWithAuthInfo(context.Background(), &auth.AuthInfo{
			Token:     &types.Token{TokenType: tokenType},
			Workspace: &types.Workspace{ExternalId: "00000000-0000-4000-8000-000000000001"},
		})
		for name, req := range workerNetworkRequests() {
			b.Run(tokenType+"/"+name, func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					if err := service.AuthorizeRequest(ctx, req); err != nil {
						b.Fatal(err)
					}
					service.scopedNetworkPrefix(ctx, req.GetNetworkPrefix())
				}
			})
		}
	}
}
