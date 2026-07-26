package pod

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestSandboxConnectErrorMessageDoesNotLeakDetails(t *testing.T) {
	got := sandboxConnectErrorMessage(errors.New("container state not found: sandbox-123 on worker 10.0.0.12"))
	if got != "Failed to connect to sandbox" {
		t.Fatalf("sandboxConnectErrorMessage leaked details: %q", got)
	}
}

func TestWaitForSandboxReadyWaitsThroughPending(t *testing.T) {
	probes := 0
	err := waitForSandboxReady(context.Background(), func(context.Context) (*pb.ContainerSandboxStatusResponse, error) {
		probes++
		status := types.SandboxStatusPending
		if probes == 2 {
			status = types.SandboxStatusRunning
		}
		return &pb.ContainerSandboxStatusResponse{Ok: true, Status: string(status)}, nil
	})

	if err != nil {
		t.Fatalf("waitForSandboxReady returned error: %v", err)
	}
	if probes != 2 {
		t.Fatalf("waitForSandboxReady probes = %d, want 2", probes)
	}
}

func TestWaitForSandboxReadyRetriesTransientProbeFailure(t *testing.T) {
	probes := 0
	err := waitForSandboxReady(context.Background(), func(context.Context) (*pb.ContainerSandboxStatusResponse, error) {
		probes++
		if probes == 1 {
			return nil, status.Error(codes.Unavailable, "transport is closing")
		}
		return &pb.ContainerSandboxStatusResponse{Ok: true, Status: string(types.SandboxStatusRunning)}, nil
	})

	if err != nil {
		t.Fatalf("waitForSandboxReady returned error: %v", err)
	}
	if probes != 2 {
		t.Fatalf("waitForSandboxReady probes = %d, want 2", probes)
	}
}

func TestWaitForSandboxReadyRejectsStoppingContainer(t *testing.T) {
	err := waitForSandboxReady(context.Background(), func(context.Context) (*pb.ContainerSandboxStatusResponse, error) {
		return &pb.ContainerSandboxStatusResponse{Ok: true, Status: string(types.SandboxStatusStopping)}, nil
	})
	if err == nil {
		t.Fatal("waitForSandboxReady returned nil error for stopping sandbox")
	}
}

func TestWaitForSandboxReadyHonorsContextDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()

	err := waitForSandboxReady(ctx, func(context.Context) (*pb.ContainerSandboxStatusResponse, error) {
		return &pb.ContainerSandboxStatusResponse{Ok: true, Status: string(types.SandboxStatusPending)}, nil
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waitForSandboxReady error = %v, want context deadline exceeded", err)
	}
}

func TestWaitForSandboxReadyReturnsWorkerFailure(t *testing.T) {
	err := waitForSandboxReady(context.Background(), func(context.Context) (*pb.ContainerSandboxStatusResponse, error) {
		return &pb.ContainerSandboxStatusResponse{Ok: false, ErrorMsg: "process manager failed"}, nil
	})
	if err == nil || err.Error() != "process manager failed" {
		t.Fatalf("waitForSandboxReady error = %v, want worker failure", err)
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

func TestSandboxClientCacheKeyUsesStableWorkerRouteAddress(t *testing.T) {
	address := types.BackendRouteAddress(types.BackendRouteID("machine-a", "worker-a", "", types.BackendRouteKindWorker, 0))

	keyA := sandboxClientCacheKey(address, "token")
	keyB := sandboxClientCacheKey(address, "token")
	if keyA != keyB {
		t.Fatalf("worker route cache keys differ: %q != %q", keyA, keyB)
	}
}

func TestSandboxClientCacheKeyKeepsDifferentWorkerAddressesDistinct(t *testing.T) {
	keyA := sandboxClientCacheKey("route://worker-a", "token")
	keyB := sandboxClientCacheKey("route://worker-b", "token")
	if keyA == keyB {
		t.Fatalf("worker route cache keys unexpectedly matched: %q", keyA)
	}
}

func TestSandboxClientDialIsSingleFlightPerWorkerDuringBurst(t *testing.T) {
	const (
		workerCount         = 4
		containersPerWorker = 25
	)

	service := &GenericPodService{}
	start := make(chan struct{})
	releaseCreates := make(chan struct{})
	createStarted := make(chan struct{}, workerCount)
	var createCount atomic.Int32
	var wg sync.WaitGroup
	errs := make(chan error, workerCount*containersPerWorker)

	for worker := 0; worker < workerCount; worker++ {
		cacheKey := fmt.Sprintf("route://machine-a:worker-%d::worker:0:token", worker)
		for container := 0; container < containersPerWorker; container++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				client, _, err := service.loadOrCreateSandboxClient(context.Background(), cacheKey, func() (*common.ContainerClient, error) {
					createCount.Add(1)
					createStarted <- struct{}{}
					<-releaseCreates
					return &common.ContainerClient{}, nil
				})
				if err != nil {
					errs <- err
					return
				}
				if client == nil {
					errs <- errors.New("nil sandbox client")
				}
			}()
		}
	}

	close(start)
	for worker := 0; worker < workerCount; worker++ {
		select {
		case <-createStarted:
		case <-time.After(time.Second):
			t.Fatal("worker client dial did not start")
		}
	}
	close(releaseCreates)
	wg.Wait()
	close(errs)

	for err := range errs {
		t.Fatal(err)
	}
	if got := createCount.Load(); got != workerCount {
		t.Fatalf("client creates = %d, want one per worker (%d)", got, workerCount)
	}
}

type burstStubRepository struct {
	repository.BackendRepository
	release <-chan struct{}
	calls   atomic.Int32
}

func (r *burstStubRepository) GetStubByExternalId(context.Context, string, ...types.QueryFilter) (*types.StubWithRelated, error) {
	r.calls.Add(1)
	<-r.release
	return &types.StubWithRelated{}, nil
}

func TestSandboxStubLoadIsSingleFlightDuringBurst(t *testing.T) {
	const callers = 100
	release := make(chan struct{})
	repo := &burstStubRepository{release: release}
	service := &GenericPodService{backendRepo: repo}
	start := make(chan struct{})
	var wg sync.WaitGroup
	ready := sync.WaitGroup{}
	ready.Add(callers)

	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ready.Done()
			<-start
			if _, err := service.loadStub(context.Background(), "stub"); err != nil {
				t.Error(err)
			}
		}()
	}
	ready.Wait()
	close(start)
	time.Sleep(20 * time.Millisecond)
	close(release)
	wg.Wait()

	if calls := repo.calls.Load(); calls != 1 {
		t.Fatalf("stub reads = %d, want 1", calls)
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

func TestPodRunWorkspacePrefersStubWorkspace(t *testing.T) {
	storageID := uint(2)
	authWorkspace := &types.Workspace{ExternalId: "workspace-id", Name: "auth"}
	stub := &types.StubWithRelated{
		Workspace: types.Workspace{
			ExternalId: "workspace-id",
			Name:       "stub",
			Storage:    &types.WorkspaceStorage{Id: &storageID},
		},
	}

	got, err := podRunWorkspace(&auth.AuthInfo{Workspace: authWorkspace}, stub)
	if err != nil {
		t.Fatalf("podRunWorkspace returned error: %v", err)
	}
	if got != &stub.Workspace {
		t.Fatalf("podRunWorkspace did not return stub workspace")
	}
	if !got.StorageAvailable() {
		t.Fatalf("podRunWorkspace returned workspace without storage")
	}
}

func TestPodRunWorkspaceRejectsWorkspaceMismatch(t *testing.T) {
	_, err := podRunWorkspace(
		&auth.AuthInfo{Workspace: &types.Workspace{ExternalId: "auth-workspace"}},
		&types.StubWithRelated{Workspace: types.Workspace{ExternalId: "other-workspace"}},
	)
	if err == nil {
		t.Fatal("podRunWorkspace returned nil error for workspace mismatch")
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

const sandboxReconnectTarget = 500 * time.Millisecond

type sandboxReconnectTestRepository struct {
	repository.ContainerRepository
	container       *types.ContainerState
	address         string
	addressFailures atomic.Int32
}

func (r *sandboxReconnectTestRepository) GetContainerState(string) (*types.ContainerState, error) {
	return r.container, nil
}

func (r *sandboxReconnectTestRepository) GetWorkerAddress(context.Context, string) (string, error) {
	for remaining := r.addressFailures.Load(); remaining > 0; remaining = r.addressFailures.Load() {
		if r.addressFailures.CompareAndSwap(remaining, remaining-1) {
			return "", errors.New("failed to schedule container")
		}
	}
	return r.address, nil
}

type trackedSandboxListener struct {
	net.Listener
	accepted chan net.Conn
	count    atomic.Int32
}

func newTrackedSandboxListener(listener net.Listener) *trackedSandboxListener {
	return &trackedSandboxListener{
		Listener: listener,
		accepted: make(chan net.Conn, 4),
	}
}

func (l *trackedSandboxListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	l.count.Add(1)
	select {
	case l.accepted <- conn:
	default:
	}
	return conn, nil
}

func (l *trackedSandboxListener) nextConnection(t *testing.T) net.Conn {
	t.Helper()
	select {
	case conn := <-l.accepted:
		return conn
	case <-time.After(time.Second):
		t.Fatal("worker connection was not accepted")
		return nil
	}
}

type sandboxReconnectStatusServer struct {
	pb.UnimplementedContainerServiceServer

	mu                   sync.Mutex
	unavailableResponses int
}

func (s *sandboxReconnectStatusServer) failNextStatusCalls(count int) {
	s.mu.Lock()
	s.unavailableResponses = count
	s.mu.Unlock()
}

func (s *sandboxReconnectStatusServer) ContainerSandboxStatus(
	context.Context,
	*pb.ContainerSandboxStatusRequest,
) (*pb.ContainerSandboxStatusResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.unavailableResponses > 0 {
		s.unavailableResponses--
		return nil, status.Error(codes.Unavailable, "temporary application response")
	}
	return &pb.ContainerSandboxStatusResponse{
		Ok:     true,
		Status: string(types.SandboxStatusRunning),
	}, nil
}

type sandboxReconnectTestHarness struct {
	service     *GenericPodService
	repository  *sandboxReconnectTestRepository
	listener    *trackedSandboxListener
	server      *sandboxReconnectStatusServer
	context     context.Context
	containerID string
	stubID      string
	cacheKey    string
}

func newSandboxReconnectTestHarness(t *testing.T) *sandboxReconnectTestHarness {
	t.Helper()

	rawListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	listener := newTrackedSandboxListener(rawListener)
	statusServer := &sandboxReconnectStatusServer{}
	workerServer := grpc.NewServer()
	pb.RegisterContainerServiceServer(workerServer, statusServer)
	go func() {
		_ = workerServer.Serve(listener)
	}()
	t.Cleanup(workerServer.Stop)

	const (
		containerID = "sandbox-reconnect"
		stubID      = "stub-reconnect"
		workspaceID = "workspace-reconnect"
		token       = "token-reconnect"
	)
	workerAddress := listener.Addr().String()
	repo := &sandboxReconnectTestRepository{
		container: &types.ContainerState{
			ContainerId: containerID,
			StubId:      stubID,
			WorkspaceId: workspaceID,
			Status:      types.ContainerStatusRunning,
		},
		address: workerAddress,
	}
	service := &GenericPodService{containerRepo: repo}
	t.Cleanup(func() {
		service.clientCache.Range(func(_, value any) bool {
			if client, ok := value.(*common.ContainerClient); ok {
				_ = client.Close()
			}
			return true
		})
	})

	return &sandboxReconnectTestHarness{
		service:    service,
		repository: repo,
		listener:   listener,
		server:     statusServer,
		context: auth.ContextWithAuthInfo(context.Background(), &auth.AuthInfo{
			Workspace: &types.Workspace{ExternalId: workspaceID},
			Token:     &types.Token{Key: token},
		}),
		containerID: containerID,
		stubID:      stubID,
		cacheKey:    sandboxClientCacheKey(workerAddress, token),
	}
}

func (h *sandboxReconnectTestHarness) connect(t *testing.T, ctx context.Context) {
	t.Helper()
	resp, err := h.service.SandboxConnect(ctx, &pb.PodSandboxConnectRequest{
		ContainerId: h.containerID,
	})
	if err != nil {
		t.Fatalf("SandboxConnect: %v", err)
	}
	if !resp.Ok {
		t.Fatalf("SandboxConnect failed: %s", resp.ErrorMsg)
	}
	if resp.StubId != h.stubID {
		t.Fatalf("stub id = %q, want %q", resp.StubId, h.stubID)
	}
}

func (h *sandboxReconnectTestHarness) cachedClient(t *testing.T) *common.ContainerClient {
	t.Helper()
	value, ok := h.service.clientCache.Load(h.cacheKey)
	if !ok {
		t.Fatal("sandbox client was not cached")
	}
	client, ok := value.(*common.ContainerClient)
	if !ok {
		t.Fatalf("cached sandbox client has type %T", value)
	}
	return client
}

func TestSandboxConnectBurstRedialsDroppedTransportUnderTarget(t *testing.T) {
	const burstSize = 100

	harness := newSandboxReconnectTestHarness(t)
	primeCtx, primeCancel := context.WithTimeout(harness.context, time.Second)
	defer primeCancel()
	harness.connect(t, primeCtx)
	firstConnection := harness.listener.nextConnection(t)
	cachedClient := harness.cachedClient(t)

	start := make(chan struct{})
	results := make(chan error, burstSize)
	var waitGroup sync.WaitGroup
	for index := 0; index < burstSize; index++ {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			<-start
			callCtx, cancel := context.WithTimeout(harness.context, time.Second)
			defer cancel()
			resp, err := harness.service.SandboxConnect(callCtx, &pb.PodSandboxConnectRequest{
				ContainerId: harness.containerID,
			})
			if err != nil {
				results <- err
				return
			}
			if !resp.Ok {
				results <- status.Error(codes.Internal, resp.ErrorMsg)
				return
			}
			results <- nil
		}()
	}

	recoveryStarted := time.Now()
	if err := firstConnection.Close(); err != nil {
		t.Fatalf("close worker transport: %v", err)
	}
	close(start)
	waitGroup.Wait()
	recoveryDuration := time.Since(recoveryStarted)
	close(results)

	for err := range results {
		if err != nil {
			t.Fatalf("SandboxConnect burst failed after transport drop: %v", err)
		}
	}
	if recoveryDuration >= sandboxReconnectTarget {
		t.Fatalf("transport recovery took %s, target is under %s", recoveryDuration, sandboxReconnectTarget)
	}
	if got := harness.listener.count.Load(); got != 2 {
		t.Fatalf("worker connections = %d, want initial connection plus one reconnect", got)
	}
	if reconnectedClient := harness.cachedClient(t); reconnectedClient != cachedClient {
		t.Fatal("transport recovery replaced the cached ContainerClient")
	}
}

func TestSandboxConnectApplicationUnavailableKeepsSharedClient(t *testing.T) {
	harness := newSandboxReconnectTestHarness(t)
	primeCtx, primeCancel := context.WithTimeout(harness.context, time.Second)
	defer primeCancel()
	harness.connect(t, primeCtx)
	_ = harness.listener.nextConnection(t)
	cachedClient := harness.cachedClient(t)
	harness.server.failNextStatusCalls(1)

	retryCtx, retryCancel := context.WithTimeout(harness.context, time.Second)
	defer retryCancel()
	started := time.Now()
	harness.connect(t, retryCtx)
	retryDuration := time.Since(started)

	if retryDuration >= sandboxReconnectTarget {
		t.Fatalf("application retry took %s, target is under %s", retryDuration, sandboxReconnectTarget)
	}
	if got := harness.listener.count.Load(); got != 1 {
		t.Fatalf("worker connections = %d, application Unavailable must not redial", got)
	}
	if retriedClient := harness.cachedClient(t); retriedClient != cachedClient {
		t.Fatal("application Unavailable replaced the cached ContainerClient")
	}
}

func TestSandboxConnectRetriesInitialAddressLookupUnderTarget(t *testing.T) {
	harness := newSandboxReconnectTestHarness(t)
	harness.repository.addressFailures.Store(1)

	connectCtx, cancel := context.WithTimeout(harness.context, time.Second)
	defer cancel()
	started := time.Now()
	harness.connect(t, connectCtx)
	connectDuration := time.Since(started)

	if connectDuration >= sandboxReconnectTarget {
		t.Fatalf("address lookup recovery took %s, target is under %s", connectDuration, sandboxReconnectTarget)
	}
	if got := harness.listener.count.Load(); got != 1 {
		t.Fatalf("worker connections = %d, want one connection after address publication", got)
	}
}
