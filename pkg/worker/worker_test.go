package worker

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/runtime"
	"github.com/beam-cloud/beta9/pkg/storage"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestGPUManagerForRequestUsesWorkerVirtualizationFlag(t *testing.T) {
	physical := &testGPUManager{}
	thunder := &testGPUManager{}
	worker := &Worker{
		gpuVirtualized:          true,
		containerGPUManager:     physical,
		containerThunderManager: thunder,
	}

	require.Same(t, thunder, worker.gpuManagerForRequest(&types.ContainerRequest{GpuRequest: []string{"H100"}}))
	require.Same(t, physical, worker.gpuManagerForRequest(&types.ContainerRequest{}))

	worker.gpuVirtualized = false
	require.Same(t, physical, worker.gpuManagerForRequest(&types.ContainerRequest{GpuRequest: []string{"H100"}}))
}

type shutdownSignalRuntime struct {
	mockRuntime
	mu      sync.Mutex
	signals []syscall.Signal
	onKill  func(syscall.Signal)
}

type startupTrackingRuntime struct {
	mockRuntime
	killMu  sync.Mutex
	started chan struct{}
}

type retryStoppingRuntime struct {
	mockRuntime
	mu          sync.Mutex
	worker      *Worker
	containerID string
	failFirst   bool
	killError   func(syscall.Signal) error
	forceKills  int
	signals     []syscall.Signal
}

func (r *retryStoppingRuntime) Kill(_ context.Context, _ string, signal syscall.Signal, _ *runtime.KillOpts) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.signals = append(r.signals, signal)
	if r.killError != nil {
		if err := r.killError(signal); err != nil {
			return err
		}
	}
	if signal != syscall.SIGKILL {
		return nil
	}
	r.forceKills++
	if r.failFirst && r.forceKills == 1 {
		return errors.New("injected force-stop failure")
	}
	r.worker.containerInstances.Delete(r.containerID)
	return nil
}

func (r *retryStoppingRuntime) observedSignals() []syscall.Signal {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]syscall.Signal(nil), r.signals...)
}

func (r *startupTrackingRuntime) Run(ctx context.Context, containerID, bundlePath string, opts *runtime.RunOpts) (int, error) {
	select {
	case r.started <- struct{}{}:
	default:
	}
	return 0, nil
}

func (r *startupTrackingRuntime) Kill(ctx context.Context, containerID string, signal syscall.Signal, opts *runtime.KillOpts) error {
	r.killMu.Lock()
	defer r.killMu.Unlock()
	return r.mockRuntime.Kill(ctx, containerID, signal, opts)
}

// fakeWorkerRepoClient answers ClaimContainer. Errors are returned for the
// first len(claimErrors) calls; claimStarted/claimRelease gate a call so a test
// can observe what happens while the claim is in flight.
type fakeWorkerRepoClient struct {
	pb.WorkerRepositoryServiceClient
	mu           sync.Mutex
	claims       int
	claimErrors  []error
	claim        *pb.ClaimContainerResponse
	lastClaim    *pb.ClaimContainerRequest
	claimStarted chan struct{}
	claimRelease <-chan struct{}
}

// gateFakeCall lets a test observe a fake RPC while it is in flight: it signals
// started (without blocking) and then holds the call until release fires or
// ctx ends. A nil channel disables that half of the gate.
func gateFakeCall(ctx context.Context, started chan struct{}, release <-chan struct{}) error {
	if started != nil {
		select {
		case started <- struct{}{}:
		default:
		}
	}
	if release != nil {
		select {
		case <-release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (f *fakeWorkerRepoClient) ClaimContainer(ctx context.Context, req *pb.ClaimContainerRequest, _ ...grpc.CallOption) (*pb.ClaimContainerResponse, error) {
	f.mu.Lock()
	call := f.claims
	f.claims++
	f.lastClaim = req
	started, release := f.claimStarted, f.claimRelease
	f.mu.Unlock()
	if err := gateFakeCall(ctx, started, release); err != nil {
		return nil, err
	}
	if call < len(f.claimErrors) {
		return nil, f.claimErrors[call]
	}
	if f.claim == nil {
		return &pb.ClaimContainerResponse{Ok: true, Claimed: true}, nil
	}
	return f.claim, nil
}

func (f *fakeWorkerRepoClient) claimCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.claims
}

func (m *shutdownSignalRuntime) Kill(ctx context.Context, containerID string, sig syscall.Signal, opts *runtime.KillOpts) error {
	m.mu.Lock()
	m.signals = append(m.signals, sig)
	m.mu.Unlock()
	if m.onKill != nil {
		m.onKill(sig)
	}
	return nil
}

func (m *shutdownSignalRuntime) recordedSignals() []syscall.Signal {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]syscall.Signal(nil), m.signals...)
}

func TestCalculateCPUShares(t *testing.T) {
	tests := []struct {
		name       string
		millicores int64
		wantShares uint64
		wantQuota  int64
	}{
		{
			name:       "100m",
			millicores: 100,
			wantShares: 102,
			wantQuota:  10_000,
		},
		{
			name:       "250m",
			millicores: 250,
			wantShares: 256,
			wantQuota:  25_000,
		},
		{
			name:       "1000m",
			millicores: 1000,
			wantShares: 1024,
			wantQuota:  100_000,
		},
		{
			name:       "2000m",
			millicores: 2000,
			wantShares: 2048,
			wantQuota:  200_000,
		},
		{
			name:       "32000m",
			millicores: 32_000,
			wantShares: 32_768,
			wantQuota:  3_200_000,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := calculateCPUShares(test.millicores)
			if got != test.wantShares {
				t.Errorf("calculateCPUShares(%d) = %d, want %d", test.millicores, got, test.wantShares)
			}

			gotQuota := calculateCPUQuota(test.millicores)
			if gotQuota != test.wantQuota {
				t.Errorf("calculateCPUQuota(%d) = %d, want %d", test.millicores, gotQuota, test.wantQuota)
			}
		})
	}
}

func TestEnsureGVisorShmemTHP(t *testing.T) {
	tests := []struct {
		name       string
		policy     string
		wantChange bool
		wantPolicy string
	}{
		{
			name:       "enables disabled policy",
			policy:     "always within_size advise [never] deny force\n",
			wantChange: true,
			wantPolicy: "advise",
		},
		{
			name:       "preserves enabled policy",
			policy:     "always within_size [advise] never deny force\n",
			wantChange: false,
			wantPolicy: "always within_size [advise] never deny force\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "shmem_enabled")
			require.NoError(t, os.WriteFile(path, []byte(tt.policy), 0644))

			changed, err := ensureGVisorShmemTHP(path)
			require.NoError(t, err)
			require.Equal(t, tt.wantChange, changed)
			policy, err := os.ReadFile(path)
			require.NoError(t, err)
			require.Equal(t, tt.wantPolicy, string(policy))
		})
	}
}

func TestContainerStartLimitForRuntimeUsesRuntimeName(t *testing.T) {
	t.Setenv(types.WorkerStartConcurrencyEnv, "")

	require.Equal(t, 16, containerStartLimitForRuntimeWithDefaults(types.ContainerRuntimeRunc.String(), 16, 2))
	require.Equal(t, 2, containerStartLimitForRuntimeWithDefaults(types.ContainerRuntimeGvisor.String(), 16, 2))
	require.Equal(t, 16, containerStartLimitForRuntimeWithDefaults("unknown", 16, 2))
}

func TestContainerStartLimitForRuntimeAllowsExplicitOverride(t *testing.T) {
	t.Setenv(types.WorkerStartConcurrencyEnv, "4")

	require.Equal(t, 4, containerStartLimitForRuntimeWithDefaults(types.ContainerRuntimeRunc.String(), 16, 2))
	require.Equal(t, 4, containerStartLimitForRuntimeWithDefaults(types.ContainerRuntimeGvisor.String(), 16, 2))
}

func TestContainerStartLimitForPoolRuntimeUsesPoolConfig(t *testing.T) {
	t.Setenv(types.WorkerStartConcurrencyEnv, "")

	poolConfig := types.WorkerPoolConfig{ContainerStartConcurrency: 64}

	require.Equal(t, 64, containerStartLimitForPoolRuntime(poolConfig, "", types.ContainerRuntimeGvisor.String(), 0))
	require.Equal(t, 64, containerStartLimitForPoolRuntime(poolConfig, "", types.ContainerRuntimeRunc.String(), 0))
}

func TestContainerStartLimitForPoolRuntimeCapsByWorkerCPU(t *testing.T) {
	t.Setenv(types.WorkerStartConcurrencyEnv, "")

	poolConfig := types.WorkerPoolConfig{
		ContainerStartConcurrency: 128,
		PoolSizing: types.WorkerPoolJobSpecPoolSizingConfig{
			DefaultWorkerCPU: "1000m",
		},
	}

	require.Equal(t, 2, containerStartLimitForPoolRuntime(poolConfig, "", types.ContainerRuntimeRunc.String(), 1000))
	require.Equal(t, 4, containerStartLimitForPoolRuntime(poolConfig, "", types.ContainerRuntimeGvisor.String(), 1000))
}

func TestContainerStartLimitForPoolRuntimeScalesWithWorkerCPU(t *testing.T) {
	t.Setenv(types.WorkerStartConcurrencyEnv, "")

	poolConfig := types.WorkerPoolConfig{
		ContainerStartConcurrency: 128,
		PoolSizing: types.WorkerPoolJobSpecPoolSizingConfig{
			DefaultWorkerCPU: "8000m",
		},
	}

	require.Equal(t, 16, containerStartLimitForPoolRuntime(poolConfig, "", types.ContainerRuntimeRunc.String(), 8000))
	require.Equal(t, 32, containerStartLimitForPoolRuntime(poolConfig, "", types.ContainerRuntimeGvisor.String(), 8000))
}

func TestContainerStartLimitForPoolRuntimeAllowsEnvOverride(t *testing.T) {
	t.Setenv(types.WorkerStartConcurrencyEnv, "8")

	poolConfig := types.WorkerPoolConfig{ContainerStartConcurrency: 64}

	require.Equal(t, 8, containerStartLimitForPoolRuntime(poolConfig, "", types.ContainerRuntimeGvisor.String(), 0))
}

func TestUpdateContainerStatusOnceStopsHeartbeatForExitedInstance(t *testing.T) {
	repoClient := &fakeContainerRepoClient{
		state: &pb.ContainerState{
			ContainerId: "container-1",
			Status:      string(types.ContainerStatusRunning),
		},
	}
	worker := &Worker{
		containerInstances:  common.NewSafeMap[*ContainerInstance](),
		containerRepoClient: repoClient,
		stopContainerChan:   make(chan stopContainerEvent, 1),
	}
	worker.containerInstances.Set("container-1", &ContainerInstance{ExitCode: 0})

	done, err := worker.updateContainerStatusOnce(context.Background(), &types.ContainerRequest{
		ContainerId: "container-1",
		ImageId:     "image-1",
	})

	require.NoError(t, err)
	require.True(t, done)
	require.Equal(t, 0, repoClient.getStateCalls)
	require.Equal(t, 0, repoClient.updateStatusCalls)
}

func TestClaimOutcomeGatesStartup(t *testing.T) {
	tests := []struct {
		name         string
		claim        *pb.ClaimContainerResponse
		wantReleased bool
		wantExitCode bool
		wantRuntime  bool
	}{
		{
			name:         "rejected claim releases capacity only",
			claim:        &pb.ClaimContainerResponse{Ok: false, ErrorMsg: "container <container-claim> is assigned to another worker"},
			wantReleased: true,
		},
		{
			name:         "claimed container with missing state is failed",
			claim:        &pb.ClaimContainerResponse{Ok: false, Claimed: true, ErrorMsg: "container state not found: container-claim"},
			wantExitCode: true,
		},
		{
			name: "claimed container already stopping does not start",
			claim: &pb.ClaimContainerResponse{Ok: true, Claimed: true, State: &pb.ContainerState{
				ContainerId: "container-claim",
				Status:      string(types.ContainerStatusStopping),
			}},
			wantExitCode: true,
		},
		{
			name:        "claimed pending container starts",
			claim:       &pb.ClaimContainerResponse{Ok: true, Claimed: true, State: &pb.ContainerState{ContainerId: "container-claim", Status: string(types.ContainerStatusPending)}},
			wantRuntime: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			workerCtx, cancelWorker := context.WithCancel(context.Background())
			t.Cleanup(cancelWorker)
			request := &types.ContainerRequest{ContainerId: "container-claim", DeliveryToken: "delivery-1"}
			workerRepo := &fakeWorkerRepoClient{claim: tt.claim}
			repoClient := &fakeContainerRepoClient{deleteStateDone: make(chan struct{}, 1)}
			worker := &Worker{
				ctx:                     workerCtx,
				workerId:                "worker-1",
				runtime:                 &mockRuntime{name: types.ContainerRuntimeRunc.String()},
				workerRepoClient:        workerRepo,
				containerRepoClient:     repoClient,
				containerInstances:      common.NewSafeMap[*ContainerInstance](),
				containerCancels:        common.NewSafeMap[context.CancelFunc](),
				containerNetworkManager: &fakeContainerNetworkController{},
				completedRequests:       make(chan *types.ContainerRequest, 1),
				stopContainerChan:       make(chan stopContainerEvent, 1),
			}
			require.True(t, worker.reserveContainerInstance(request))

			runtimeStarted := make(chan struct{}, 1)
			done := make(chan struct{})
			go func() {
				worker.runContainerRequestWithRunner(request, func(ctx context.Context, _ *types.ContainerRequest) error {
					// Mirror the runtime: a stop observed during the claim
					// cancels the startup context before the runner runs.
					if ctx.Err() != nil {
						return ctx.Err()
					}
					runtimeStarted <- struct{}{}
					return nil
				})
				close(done)
			}()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("container request did not complete")
			}

			require.Equal(t, 1, workerRepo.claimCalls())
			require.Equal(t, "delivery-1", workerRepo.lastClaim.DeliveryToken)
			_, cancelRegistered := worker.containerCancels.Get(request.ContainerId)
			require.False(t, cancelRegistered)
			_, instanceExists := worker.containerInstances.Get(request.ContainerId)
			require.Equal(t, !tt.wantReleased, instanceExists || tt.wantExitCode)
			require.Equal(t, tt.wantRuntime, len(runtimeStarted) == 1)
			// Every terminal path completes the request; a started container
			// completes when it exits.
			require.Equal(t, !tt.wantRuntime, len(worker.completedRequests) == 1)
			if tt.wantExitCode {
				require.Equal(t, 1, repoClient.setExitCodeCalls)
				require.Equal(t, int32(1), repoClient.lastSetExitCode.ExitCode)
			} else {
				require.Zero(t, repoClient.setExitCodeCalls)
			}
			if tt.wantRuntime {
				instance, exists := worker.containerInstances.Get(request.ContainerId)
				require.True(t, exists)
				instance.setExitCode(0)
			}
		})
	}
}

func TestRunContainerRequestWaitsForClaimBeforeRuntimeHandoff(t *testing.T) {
	workerCtx, cancelWorker := context.WithCancel(context.Background())
	defer cancelWorker()
	claimStarted := make(chan struct{}, 1)
	releaseClaim := make(chan struct{})
	workerRepo := &fakeWorkerRepoClient{claimStarted: claimStarted, claimRelease: releaseClaim}
	worker := &Worker{
		ctx:                 workerCtx,
		workerRepoClient:    workerRepo,
		containerInstances:  common.NewSafeMap[*ContainerInstance](),
		containerCancels:    common.NewSafeMap[context.CancelFunc](),
		containerRepoClient: &fakeContainerRepoClient{},
	}
	request := &types.ContainerRequest{ContainerId: "container-heartbeat-handoff"}
	require.True(t, worker.reserveContainerInstance(request))

	runnerStarted := make(chan struct{}, 1)
	requestDone := make(chan struct{})
	go func() {
		worker.runContainerRequestWithRunner(request, func(context.Context, *types.ContainerRequest) error {
			runnerStarted <- struct{}{}
			return nil
		})
		close(requestDone)
	}()

	select {
	case <-claimStarted:
	case <-time.After(time.Second):
		t.Fatal("container claim did not start")
	}
	select {
	case <-runnerStarted:
		t.Fatal("runtime started before the claim was acknowledged")
	default:
	}
	close(releaseClaim)
	select {
	case <-runnerStarted:
	case <-time.After(time.Second):
		t.Fatal("runtime did not start after the claim")
	}
	select {
	case <-requestDone:
	case <-time.After(time.Second):
		t.Fatal("runtime handoff did not complete")
	}
	_, startupCancelRegistered := worker.containerCancels.Get(request.ContainerId)
	require.False(t, startupCancelRegistered)

	instance, exists := worker.containerInstances.Get(request.ContainerId)
	require.True(t, exists)
	instance.setExitCode(0)
}

func TestClaimRetriesAmbiguousTransportFailure(t *testing.T) {
	workerRepo := &fakeWorkerRepoClient{claimErrors: []error{status.Error(codes.Unavailable, "gateway restarting")}}
	worker := &Worker{workerRepoClient: workerRepo}

	claimed, err := worker.claimContainer(context.Background(), &types.ContainerRequest{ContainerId: "container-retry"})
	require.NoError(t, err)
	require.True(t, claimed)
	require.Equal(t, 2, workerRepo.claimCalls())
}

func TestClaimReturnsAuthoritativeTransportError(t *testing.T) {
	workerRepo := &fakeWorkerRepoClient{claimErrors: []error{status.Error(codes.Unimplemented, "unknown method ClaimContainer")}}
	worker := &Worker{workerRepoClient: workerRepo}

	claimed, err := worker.claimContainer(context.Background(), &types.ContainerRequest{ContainerId: "container-old-gateway"})
	require.Equal(t, codes.Unimplemented, status.Code(err))
	require.False(t, claimed)
	require.Equal(t, 1, workerRepo.claimCalls())
}

func TestClaimHonorsContextDeadline(t *testing.T) {
	workerRepo := &fakeWorkerRepoClient{claimErrors: make([]error, 100)}
	for i := range workerRepo.claimErrors {
		workerRepo.claimErrors[i] = status.Error(codes.Unavailable, "gateway unavailable")
	}
	worker := &Worker{workerRepoClient: workerRepo}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	started := time.Now()

	claimed, err := worker.claimContainer(ctx, &types.ContainerRequest{ContainerId: "container-claim-timeout"})

	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.False(t, claimed)
	require.Less(t, time.Since(started), 500*time.Millisecond)
}

func TestUpdateContainerStatusOnceReconcilesStartedPendingContainer(t *testing.T) {
	repoClient := &fakeContainerRepoClient{
		state: &pb.ContainerState{
			ContainerId: "container-1",
			Status:      string(types.ContainerStatusPending),
		},
	}
	worker := &Worker{
		containerInstances:  common.NewSafeMap[*ContainerInstance](),
		containerRepoClient: repoClient,
		stopContainerChan:   make(chan stopContainerEvent, 1),
	}
	worker.containerInstances.Set("container-1", &ContainerInstance{
		ExitCode:       -1,
		RuntimeStarted: true,
		RuntimePid:     1234,
	})

	done, err := worker.updateContainerStatusOnce(context.Background(), &types.ContainerRequest{
		ContainerId: "container-1",
		ImageId:     "image-1",
	})

	require.NoError(t, err)
	require.False(t, done)
	require.Equal(t, 1, repoClient.getStateCalls)
	require.Equal(t, 1, repoClient.updateStatusCalls)
	require.Equal(t, string(types.ContainerStatusRunning), repoClient.lastUpdateStatus.Status)
	require.Equal(t, int64(types.ContainerStateTtlS), repoClient.lastUpdateStatus.ExpirySeconds)
}

func TestRuntimeStartStateIsPublishedAtomically(t *testing.T) {
	instance := &ContainerInstance{}
	var writers sync.WaitGroup
	writers.Add(1)
	go func() {
		defer writers.Done()
		for pid := 1; pid <= 1000; pid++ {
			instance.markRuntimeStarted(pid)
			instance.resetRuntimeStarted()
		}
	}()

	for range 1000 {
		started, pid := instance.runtimeStartState()
		if started {
			require.Positive(t, pid)
		} else {
			require.Zero(t, pid)
		}
	}
	writers.Wait()
}

func TestUpdateContainerStatusStopsWhenRequestEnds(t *testing.T) {
	repoClient := &fakeContainerRepoClient{
		state: &pb.ContainerState{
			ContainerId: "container-cancelled-heartbeat",
			Status:      string(types.ContainerStatusRunning),
		},
	}
	worker := &Worker{
		containerInstances:  common.NewSafeMap[*ContainerInstance](),
		containerRepoClient: repoClient,
		stopContainerChan:   make(chan stopContainerEvent, 1),
	}
	request := &types.ContainerRequest{ContainerId: "container-cancelled-heartbeat"}
	worker.containerInstances.Set(request.ContainerId, &ContainerInstance{ExitCode: -1})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		worker.updateContainerStatusLoop(ctx, request)
	}()
	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("heartbeat outlived its container request")
	}
}

func TestMissingStateHeartbeatCancelsStartupWhenStopQueueUnavailable(t *testing.T) {
	worker := &Worker{
		containerInstances:  common.NewSafeMap[*ContainerInstance](),
		containerCancels:    common.NewSafeMap[context.CancelFunc](),
		containerRepoClient: &missingContainerRepoClient{},
		stopContainerChan:   make(chan stopContainerEvent),
	}
	request := &types.ContainerRequest{ContainerId: "container-missing"}
	worker.containerInstances.Set(request.ContainerId, &ContainerInstance{ExitCode: -1})
	startupCtx, cancelStartup := context.WithCancel(context.Background())
	worker.registerContainerCancel(request.ContainerId, cancelStartup)

	done, err := worker.updateContainerStatusOnce(startupCtx, request)

	require.NoError(t, err)
	require.False(t, done)
	require.ErrorIs(t, startupCtx.Err(), context.Canceled)
}

func TestStoppingHeartbeatCannotShortenFinalizationLease(t *testing.T) {
	getStarted := make(chan struct{}, 1)
	getRelease := make(chan struct{})
	repoClient := &fakeContainerRepoClient{
		state: &pb.ContainerState{
			ContainerId: "container-stopping",
			Status:      string(types.ContainerStatusStopping),
		},
		getStateStarted: getStarted,
		getStateRelease: getRelease,
	}
	worker := &Worker{
		ctx:                 context.Background(),
		containerInstances:  common.NewSafeMap[*ContainerInstance](),
		containerRepoClient: repoClient,
		stopContainerChan:   make(chan stopContainerEvent, 1),
	}
	instance := &ContainerInstance{ExitCode: -1}
	instance.StopEscalationStarted.Store(true)
	worker.containerInstances.Set("container-stopping", instance)
	request := &types.ContainerRequest{ContainerId: "container-stopping"}

	done := make(chan bool, 1)
	errCh := make(chan error, 1)
	go func() {
		heartbeatDone, err := worker.updateContainerStatusOnce(context.Background(), request)
		done <- heartbeatDone
		errCh <- err
	}()

	select {
	case <-getStarted:
	case <-time.After(time.Second):
		t.Fatal("heartbeat did not read STOPPING state")
	}
	require.True(t, worker.markContainerStopping(request.ContainerId, types.ContainerStateTtlSWhileStopping))
	close(getRelease)

	require.False(t, <-done)
	require.NoError(t, <-errCh)
	updates := repoClient.containerStatusUpdates()
	require.Len(t, updates, 1)
	require.Equal(t, int64(types.ContainerStateTtlSWhileStopping), updates[0].ExpirySeconds)
}

func TestLocalStopReasonTransitionsRemoteStateToStopping(t *testing.T) {
	const containerID = "container-local-stop"
	repoClient := &fakeContainerRepoClient{state: &pb.ContainerState{
		ContainerId: containerID,
		Status:      string(types.ContainerStatusRunning),
	}}
	worker := &Worker{
		containerInstances:  common.NewSafeMap[*ContainerInstance](),
		containerRepoClient: repoClient,
	}
	worker.containerInstances.Set(containerID, &ContainerInstance{
		Id:         containerID,
		ExitCode:   -1,
		StopReason: types.StopContainerReasonUnknown,
		Request:    &types.ContainerRequest{ContainerId: containerID},
	})

	done, err := worker.updateContainerStatusOnce(context.Background(), &types.ContainerRequest{ContainerId: containerID})

	require.NoError(t, err)
	require.False(t, done)
	updates := repoClient.containerStatusUpdates()
	require.Len(t, updates, 1)
	require.Equal(t, string(types.ContainerStatusStopping), updates[0].Status)
	require.Equal(t, int64(types.ContainerStateTtlSWhileStopping), updates[0].ExpirySeconds)
}

func TestStatusHeartbeatRetriesFailedStoppingEscalation(t *testing.T) {
	const containerID = "container-retry-stopping"
	repoClient := &fakeContainerRepoClient{state: &pb.ContainerState{
		ContainerId: containerID,
		Status:      string(types.ContainerStatusStopping),
	}}
	worker := &Worker{
		ctx:                 context.Background(),
		containerInstances:  common.NewSafeMap[*ContainerInstance](),
		containerRepoClient: repoClient,
		config: types.AppConfig{Worker: types.WorkerConfig{
			TerminationGracePeriod: 0,
		}},
	}
	rt := &retryStoppingRuntime{
		mockRuntime: mockRuntime{state: func(context.Context, string) (runtime.State, error) {
			return runtime.State{Status: types.RuncContainerStatusRunning}, nil
		}},
		worker:      worker,
		containerID: containerID,
		failFirst:   true,
	}
	instance := &ContainerInstance{
		Id:       containerID,
		ExitCode: -1,
		Request:  &types.ContainerRequest{ContainerId: containerID},
		Runtime:  rt,
	}
	worker.containerInstances.Set(containerID, instance)

	done, err := worker.updateContainerStatusOnce(context.Background(), instance.Request)
	require.NoError(t, err)
	require.False(t, done)
	require.Eventually(t, func() bool {
		return !instance.StopEscalationStarted.Load()
	}, time.Second, 5*time.Millisecond, "failed force-stop must be retryable")

	done, err = worker.updateContainerStatusOnce(context.Background(), instance.Request)
	require.NoError(t, err)
	require.False(t, done)
	require.Eventually(t, func() bool {
		_, exists := worker.containerInstances.Get(containerID)
		return !exists
	}, time.Second, 5*time.Millisecond)
	require.Equal(t, []syscall.Signal{
		syscall.SIGTERM,
		syscall.SIGKILL,
		syscall.SIGTERM,
		syscall.SIGKILL,
	}, rt.observedSignals())
}

func TestStopEventKillFailureHandsOffToLifecycleReconciliation(t *testing.T) {
	const containerID = "container-stop-event-retry"
	workerCtx, cancelWorker := context.WithCancel(context.Background())
	defer cancelWorker()
	worker := &Worker{
		ctx:                workerCtx,
		containerInstances: common.NewSafeMap[*ContainerInstance](),
		stopContainerChan:  make(chan stopContainerEvent, 1),
	}
	rt := &retryStoppingRuntime{
		mockRuntime: mockRuntime{name: types.ContainerRuntimeRunc.String()},
		worker:      worker,
		containerID: containerID,
		failFirst:   true,
	}
	worker.containerInstances.Set(containerID, &ContainerInstance{
		Id:         containerID,
		ExitCode:   -1,
		StopReason: types.StopContainerReasonUser,
		Request:    &types.ContainerRequest{ContainerId: containerID},
		Runtime:    rt,
	})
	processorDone := make(chan struct{})
	go func() {
		worker.processStopContainerEvents()
		close(processorDone)
	}()
	started := time.Now()

	worker.stopContainerChan <- stopContainerEvent{ContainerId: containerID, Kill: true}

	require.Eventually(t, func() bool {
		_, exists := worker.containerInstances.Get(containerID)
		return !exists
	}, time.Second, time.Millisecond)
	require.Less(t, time.Since(started), 500*time.Millisecond)
	require.Equal(t, []syscall.Signal{syscall.SIGKILL, syscall.SIGKILL}, rt.observedSignals())
	close(worker.stopContainerChan)
	select {
	case <-processorDone:
	case <-time.After(time.Second):
		t.Fatal("stop event processor did not finish")
	}
}

func TestStatusHeartbeatRetriesStopObservedBeforeRuntimeExists(t *testing.T) {
	const containerID = "container-stop-before-runtime"
	workerCtx, cancelWorker := context.WithCancel(context.Background())
	defer cancelWorker()
	repoClient := &fakeContainerRepoClient{state: &pb.ContainerState{
		ContainerId: containerID,
		Status:      string(types.ContainerStatusStopping),
	}}
	worker := &Worker{
		ctx:                 workerCtx,
		containerInstances:  common.NewSafeMap[*ContainerInstance](),
		containerRepoClient: repoClient,
		config: types.AppConfig{Worker: types.WorkerConfig{
			TerminationGracePeriod: 0,
		}},
	}
	var runtimeExists atomic.Bool
	rt := &retryStoppingRuntime{
		mockRuntime: mockRuntime{state: func(context.Context, string) (runtime.State, error) {
			if !runtimeExists.Load() {
				return runtime.State{}, runtime.ErrContainerNotFound{ContainerID: containerID}
			}
			return runtime.State{Status: types.RuncContainerStatusRunning}, nil
		}},
		worker:      worker,
		containerID: containerID,
		killError: func(syscall.Signal) error {
			if !runtimeExists.Load() {
				return runtime.ErrContainerNotFound{ContainerID: containerID}
			}
			return nil
		},
	}
	instance := &ContainerInstance{
		Id:       containerID,
		ExitCode: -1,
		Request:  &types.ContainerRequest{ContainerId: containerID},
		Runtime:  rt,
	}
	instance.markRuntimeStarted(1234)
	worker.containerInstances.Set(containerID, instance)

	done, err := worker.updateContainerStatusOnce(context.Background(), instance.Request)
	require.NoError(t, err)
	require.False(t, done)
	require.Eventually(t, func() bool {
		return !instance.StopEscalationStarted.Load()
	}, time.Second, 5*time.Millisecond, "pre-runtime stop observation must remain retryable")
	require.Equal(t, []syscall.Signal{syscall.SIGTERM}, rt.observedSignals())

	runtimeExists.Store(true)
	done, err = worker.updateContainerStatusOnce(context.Background(), instance.Request)
	require.NoError(t, err)
	require.False(t, done)
	require.Eventually(t, func() bool {
		_, exists := worker.containerInstances.Get(containerID)
		return !exists
	}, time.Second, 5*time.Millisecond)
	require.Equal(t, []syscall.Signal{syscall.SIGTERM, syscall.SIGTERM, syscall.SIGKILL}, rt.observedSignals())
}

func TestWorkspaceOnlyStoppingProtectsRunningSiblings(t *testing.T) {
	worker := &Worker{containerInstances: common.NewSafeMap[*ContainerInstance]()}
	request := &types.ContainerRequest{Workspace: types.Workspace{Name: "shared"}}
	worker.containerInstances.Set("stopping", &ContainerInstance{ExitCode: -1, StopReason: types.StopContainerReasonTtl, Request: request})
	worker.containerInstances.Set("running", &ContainerInstance{ExitCode: -1, Request: request})
	require.False(t, worker.workspaceOnlyStopping("shared"))

	instance, _ := worker.containerInstances.Get("running")
	done := make(chan struct{})
	go func() {
		for range 1000 {
			instance.setStopReason(types.StopContainerReasonUser)
			instance.setExitCode(-1)
		}
		close(done)
	}()
	for range 1000 {
		_ = worker.workspaceOnlyStopping("shared")
	}
	<-done
	require.True(t, worker.workspaceOnlyStopping("shared"))
}

func TestStuckWorkspaceMountRecoveryRunsWithoutRuntimeState(t *testing.T) {
	workspaceName := "shared"
	request := &types.ContainerRequest{
		ContainerId: "container-1",
		Workspace:   types.Workspace{Name: workspaceName},
	}
	instances := common.NewSafeMap[*ContainerInstance]()
	instance := &ContainerInstance{
		ExitCode:   -1,
		StopReason: types.StopContainerReasonUser,
		Request:    request,
	}
	instances.Set(request.ContainerId, instance)
	mount := &trackedStorage{mode: storage.StorageModeGeese}
	manager := &WorkspaceStorageManager{
		mounts:             common.NewSafeMap[storage.Storage](),
		mountLastUsed:      common.NewSafeMap[time.Time](),
		containerInstances: instances,
		mountLocks:         make(map[string]*sync.RWMutex),
		poolConfig:         types.WorkerPoolConfig{StorageMode: storage.StorageModeGeese},
		config: types.StorageConfig{WorkspaceStorage: types.WorkspaceStorageConfig{
			BaseMountPath: t.TempDir(),
		}},
	}
	manager.mounts.Set(workspaceName, mount)
	manager.mountLastUsed.Set(workspaceName, time.Now())
	worker := &Worker{ctx: context.Background(), containerInstances: instances, storageManager: manager}

	worker.scheduleStuckWorkspaceMountRecovery(instance, request, time.Millisecond)

	require.Eventually(t, func() bool {
		_, mounted := manager.mounts.Get(workspaceName)
		return !mounted
	}, time.Second, time.Millisecond)
	require.True(t, mount.unmounted)
	require.Eventually(t, func() bool { return !instance.StuckMountRecoveryStarted.Load() }, time.Second, time.Millisecond)
}

func TestPreRuntimeStopSchedulesStuckWorkspaceMountRecovery(t *testing.T) {
	workerCtx, cancelWorker := context.WithCancel(context.Background())
	defer cancelWorker()
	workspaceName := "shared"
	request := &types.ContainerRequest{
		ContainerId: "container-pre-runtime-mount",
		Workspace:   types.Workspace{Name: workspaceName},
	}
	instances := common.NewSafeMap[*ContainerInstance]()
	rt := &mockRuntime{
		killErr: runtime.ErrContainerNotFound{ContainerID: request.ContainerId},
		state: func(context.Context, string) (runtime.State, error) {
			return runtime.State{}, runtime.ErrContainerNotFound{ContainerID: request.ContainerId}
		},
	}
	instance := &ContainerInstance{
		Id:         request.ContainerId,
		ExitCode:   -1,
		StopReason: types.StopContainerReasonUser,
		Request:    request,
		Runtime:    rt,
	}
	instance.StopEscalationStarted.Store(true)
	instances.Set(request.ContainerId, instance)
	manager := &WorkspaceStorageManager{
		mounts:             common.NewSafeMap[storage.Storage](),
		mountLastUsed:      common.NewSafeMap[time.Time](),
		containerInstances: instances,
		mountLocks:         make(map[string]*sync.RWMutex),
		poolConfig:         types.WorkerPoolConfig{StorageMode: storage.StorageModeGeese},
		config: types.StorageConfig{WorkspaceStorage: types.WorkspaceStorageConfig{
			BaseMountPath: t.TempDir(),
		}},
	}
	worker := &Worker{
		ctx:                workerCtx,
		containerInstances: instances,
		storageManager:     manager,
		config: types.AppConfig{Worker: types.WorkerConfig{
			TerminationGracePeriod: 0,
		}},
	}

	worker.stopObservedContainer(request.ContainerId, request, types.EventSourceWorkerStatusHeartbeat, false)

	require.False(t, instance.StopEscalationStarted.Load(), "runtime stop must remain retryable")
	require.True(t, instance.StuckMountRecoveryStarted.Load(), "canceled pre-runtime startup must retain bounded mount recovery")
}

func TestShutdownWaitDrainsWithoutStoppingActiveContainer(t *testing.T) {
	worker := &Worker{
		containerInstances: common.NewSafeMap[*ContainerInstance](),
		containerWg:        sync.WaitGroup{},
	}
	rt := &shutdownSignalRuntime{}
	worker.containerInstances.Set("container-1", &ContainerInstance{
		Id:      "container-1",
		Runtime: rt,
	})

	done := make(chan struct{})
	go func() {
		worker.waitForActiveContainersBeforeShutdown()
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("shutdown wait returned before active instance drained")
	case <-time.After(25 * time.Millisecond):
	}
	require.Empty(t, rt.recordedSignals())

	worker.containerInstances.Delete("container-1")

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("shutdown wait did not return after active instance drained")
	}
	require.Empty(t, rt.recordedSignals())
}

func TestStopActiveContainersForShutdownStopsNestedRuntimeBeforeWorkerExit(t *testing.T) {
	worker := &Worker{
		containerInstances: common.NewSafeMap[*ContainerInstance](),
		containerWg:        sync.WaitGroup{},
		config: types.AppConfig{
			Worker: types.WorkerConfig{TerminationGracePeriod: 30},
		},
	}
	rt := &shutdownSignalRuntime{}
	rt.onKill = func(sig syscall.Signal) {
		worker.containerInstances.Delete("container-1")
	}
	worker.containerInstances.Set("container-1", &ContainerInstance{
		Id:      "container-1",
		Runtime: rt,
	})

	worker.stopActiveContainersForShutdown()

	require.Empty(t, worker.activeContainerIDs())
	require.Equal(t, []syscall.Signal{syscall.SIGTERM}, rt.recordedSignals())
}

func TestStopActiveContainersForShutdownForceKillsStuckRuntime(t *testing.T) {
	worker := &Worker{
		containerInstances: common.NewSafeMap[*ContainerInstance](),
		containerWg:        sync.WaitGroup{},
		config: types.AppConfig{
			Worker: types.WorkerConfig{TerminationGracePeriod: 1},
		},
	}
	rt := &shutdownSignalRuntime{}
	rt.onKill = func(sig syscall.Signal) {
		if sig == syscall.SIGKILL {
			worker.containerInstances.Delete("container-1")
		}
	}
	worker.containerInstances.Set("container-1", &ContainerInstance{
		Id:      "container-1",
		Runtime: rt,
	})

	start := time.Now()
	worker.stopActiveContainersForShutdown()

	require.GreaterOrEqual(t, time.Since(start), time.Second)
	require.Empty(t, worker.activeContainerIDs())
	require.Equal(t, []syscall.Signal{syscall.SIGTERM, syscall.SIGKILL}, rt.recordedSignals())
}

func TestFinishShutdownSuppressesCleanupErrorsAfterCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := (&Worker{ctx: ctx}).finishShutdown(errors.New("cleanup failed"))

	require.NoError(t, err)
}

func TestFinishShutdownReturnsCleanupErrorsWithoutCancellation(t *testing.T) {
	cleanupErr := errors.New("cleanup failed")

	err := (&Worker{ctx: context.Background()}).finishShutdown(cleanupErr)

	require.ErrorIs(t, err, cleanupErr)
}

func TestMarkContainerStoppingUsesStoppingTTL(t *testing.T) {
	repoClient := &fakeContainerRepoClient{}
	worker := &Worker{containerRepoClient: repoClient}

	worker.markContainerStopping("container-1", types.ContainerStateTtlS)

	require.Equal(t, 1, repoClient.updateStatusCalls)
	require.Equal(t, "container-1", repoClient.lastUpdateStatus.ContainerId)
	require.Equal(t, string(types.ContainerStatusStopping), repoClient.lastUpdateStatus.Status)
	require.Equal(t, int64(types.ContainerStateTtlS), repoClient.lastUpdateStatus.ExpirySeconds)
}

func TestFailContainerRequestReportsExitCode(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	repoClient := &fakeContainerRepoClient{}
	request := &types.ContainerRequest{ContainerId: "container-failed-start-cleanup"}
	tempDir := filepath.Join(baseConfigPath, request.ContainerId)
	require.NoError(t, os.MkdirAll(tempDir, 0o755))
	t.Cleanup(func() { _ = os.RemoveAll(tempDir) })
	worker := &Worker{
		ctx:                     ctx,
		containerRepoClient:     repoClient,
		containerInstances:      common.NewSafeMap[*ContainerInstance](),
		containerNetworkManager: &fakeContainerNetworkController{},
		completedRequests:       make(chan *types.ContainerRequest, 1),
	}

	worker.failContainerRequest(request.ContainerId, request, errors.New("startup failed"))

	require.Equal(t, 1, repoClient.setExitCodeCalls)
	require.Equal(t, request.ContainerId, repoClient.lastSetExitCode.ContainerId)
	require.Equal(t, int32(1), repoClient.lastSetExitCode.ExitCode)
	require.NoDirExists(t, tempDir)
}

type fakeContainerRepoClient struct {
	mu                         sync.Mutex
	state                      *pb.ContainerState
	getStateCalls              int
	getStateErrors             []error
	getStateError              error
	getStateStarted            chan struct{}
	getStateRelease            <-chan struct{}
	updateStatusCalls          int
	lastUpdateStatus           *pb.UpdateContainerStatusRequest
	updateStatuses             []*pb.UpdateContainerStatusRequest
	updateStatusErrors         []error
	updateStatusStarted        chan struct{}
	updateStatusRelease        <-chan struct{}
	updateStatusErrorForExpiry map[int64]error
	addressMap                 map[int32]string
	setAddressCalls            int
	lastSetAddress             *pb.SetContainerAddressRequest
	setAddressMapCalls         int
	lastSetAddressMap          *pb.SetContainerAddressMapRequest
	setExitCodeCalls           int
	lastSetExitCode            *pb.SetContainerExitCodeRequest
	setExitCodeErrors          []error
	deleteStateCalls           int
	deleteStateErrors          []error
	deleteStateDone            chan struct{}
}

type missingContainerRepoClient struct {
	pb.ContainerRepositoryServiceClient
}

func (*missingContainerRepoClient) GetContainerState(context.Context, *pb.GetContainerStateRequest, ...grpc.CallOption) (*pb.GetContainerStateResponse, error) {
	return nil, &types.ErrContainerStateNotFound{ContainerId: "container-missing"}
}

func (f *fakeContainerRepoClient) GetContainerState(ctx context.Context, in *pb.GetContainerStateRequest, opts ...grpc.CallOption) (*pb.GetContainerStateResponse, error) {
	f.mu.Lock()
	call := f.getStateCalls
	f.getStateCalls++
	state := f.state
	if state != nil {
		state = proto.Clone(state).(*pb.ContainerState)
	}
	started := f.getStateStarted
	release := f.getStateRelease
	var err error
	if call < len(f.getStateErrors) {
		err = f.getStateErrors[call]
	} else {
		err = f.getStateError
	}
	f.mu.Unlock()
	if err := gateFakeCall(ctx, started, release); err != nil {
		return nil, err
	}
	if err != nil {
		return nil, err
	}
	return &pb.GetContainerStateResponse{
		Ok:          true,
		ContainerId: in.ContainerId,
		State:       state,
	}, nil
}

func (f *fakeContainerRepoClient) getContainerStateCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.getStateCalls
}

func (f *fakeContainerRepoClient) DeleteContainerState(ctx context.Context, in *pb.DeleteContainerStateRequest, opts ...grpc.CallOption) (*pb.DeleteContainerStateResponse, error) {
	f.mu.Lock()
	call := f.deleteStateCalls
	f.deleteStateCalls++
	var err error
	if call < len(f.deleteStateErrors) {
		err = f.deleteStateErrors[call]
	}
	done := f.deleteStateDone
	f.mu.Unlock()
	if err != nil {
		return nil, err
	}
	if done != nil {
		select {
		case done <- struct{}{}:
		default:
		}
	}
	return &pb.DeleteContainerStateResponse{Ok: true}, nil
}

func (f *fakeContainerRepoClient) deleteContainerStateCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.deleteStateCalls
}

func (f *fakeContainerRepoClient) UpdateContainerStatus(ctx context.Context, in *pb.UpdateContainerStatusRequest, opts ...grpc.CallOption) (*pb.UpdateContainerStatusResponse, error) {
	f.mu.Lock()
	call := f.updateStatusCalls
	f.updateStatusCalls++
	f.lastUpdateStatus = in
	f.updateStatuses = append(f.updateStatuses, in)
	var err error
	if configured, ok := f.updateStatusErrorForExpiry[in.ExpirySeconds]; ok {
		err = configured
	} else if call < len(f.updateStatusErrors) {
		err = f.updateStatusErrors[call]
	}
	started := f.updateStatusStarted
	release := f.updateStatusRelease
	f.mu.Unlock()
	if err := gateFakeCall(ctx, started, release); err != nil {
		return nil, err
	}
	if err != nil {
		return nil, err
	}
	// Mirror the gateway: STOPPING is terminal, otherwise the update is persisted.
	status := in.Status
	f.mu.Lock()
	if f.state != nil && f.state.Status == string(types.ContainerStatusStopping) {
		status = f.state.Status
	}
	f.mu.Unlock()
	return &pb.UpdateContainerStatusResponse{Ok: true, Status: status}, nil
}

func (f *fakeContainerRepoClient) SetContainerExitCode(ctx context.Context, in *pb.SetContainerExitCodeRequest, opts ...grpc.CallOption) (*pb.SetContainerExitCodeResponse, error) {
	f.mu.Lock()
	call := f.setExitCodeCalls
	f.setExitCodeCalls++
	f.lastSetExitCode = in
	var err error
	if call < len(f.setExitCodeErrors) {
		err = f.setExitCodeErrors[call]
	}
	f.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return &pb.SetContainerExitCodeResponse{Ok: true}, nil
}

func (f *fakeContainerRepoClient) containerStatusUpdates() []*pb.UpdateContainerStatusRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*pb.UpdateContainerStatusRequest(nil), f.updateStatuses...)
}

func (f *fakeContainerRepoClient) containerExitCodeCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.setExitCodeCalls
}

func (f *fakeContainerRepoClient) SetContainerAddress(ctx context.Context, in *pb.SetContainerAddressRequest, opts ...grpc.CallOption) (*pb.SetContainerAddressResponse, error) {
	f.setAddressCalls++
	f.lastSetAddress = in
	return &pb.SetContainerAddressResponse{Ok: true}, nil
}

func (f *fakeContainerRepoClient) SetContainerAddressMap(ctx context.Context, in *pb.SetContainerAddressMapRequest, opts ...grpc.CallOption) (*pb.SetContainerAddressMapResponse, error) {
	f.setAddressMapCalls++
	f.lastSetAddressMap = in
	return &pb.SetContainerAddressMapResponse{Ok: true}, nil
}

func (f *fakeContainerRepoClient) GetContainerAddressMap(ctx context.Context, in *pb.GetContainerAddressMapRequest, opts ...grpc.CallOption) (*pb.GetContainerAddressMapResponse, error) {
	return &pb.GetContainerAddressMapResponse{Ok: true, AddressMap: f.addressMap}, nil
}

func (f *fakeContainerRepoClient) SetWorkerAddress(ctx context.Context, in *pb.SetWorkerAddressRequest, opts ...grpc.CallOption) (*pb.SetWorkerAddressResponse, error) {
	return &pb.SetWorkerAddressResponse{Ok: true}, nil
}
