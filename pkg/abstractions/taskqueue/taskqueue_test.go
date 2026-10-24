package taskqueue

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	abstractions "github.com/beam-cloud/beta9/pkg/abstractions/common"
	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/task"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func TestTaskQueueCompletionKeepsDispatcherStateUntilPersisted(t *testing.T) {
	for _, failUpdate := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "database_failure"}[failUpdate], func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			ctx = auth.ContextWithAuthInfo(ctx, &auth.AuthInfo{Workspace: &types.Workspace{Name: "workspace"}})
			rdb, err := repository.NewRedisClientForTest()
			require.NoError(t, err)
			t.Cleanup(func() { _ = rdb.Close() })
			tasks := repository.NewTaskRedisRepository(rdb)
			require.NoError(t, tasks.SetTaskState(ctx, "workspace", "stub", "task", []byte(`{"task_id":"task"}`)))
			require.NoError(t, tasks.ClaimTask(ctx, "workspace", "stub", "task", "container"))
			dispatcher, err := task.NewDispatcher(ctx, tasks)
			require.NoError(t, err)
			backend := completionBackend{update: func() error {
				claimed, err := tasks.IsClaimed(ctx, "workspace", "stub", "task")
				require.NoError(t, err)
				require.True(t, claimed, "the monitor must not report completion before the database update")
				_, err = tasks.GetTaskState(ctx, "workspace", "stub", "task")
				require.NoError(t, err, "a failed database update must remain recoverable by the dispatcher")
				if failUpdate {
					return errors.New("database unavailable")
				}
				return nil
			}}
			queue := &RedisTaskQueue{config: completionTestConfig(), rdb: rdb, backendRepo: backend, taskDispatcher: dispatcher}
			response, err := queue.TaskQueueComplete(ctx, &pb.TaskQueueCompleteRequest{
				TaskId: "task", StubId: "stub", ContainerId: "container", TaskStatus: string(types.TaskStatusComplete),
			})
			if failUpdate {
				require.Equal(t, codes.Unavailable, status.Code(err))
			} else {
				require.NoError(t, err)
				require.True(t, response.GetOk())
			}
			claimed, err := tasks.IsClaimed(ctx, "workspace", "stub", "task")
			require.NoError(t, err)
			require.Equal(t, failUpdate, claimed)
			inFlight, err := tasks.TasksInFlight(ctx, "workspace", "stub")
			require.NoError(t, err)
			require.Equal(t, map[bool]int{false: 0, true: 1}[failUpdate], inFlight)
		})
	}
}

func TestTaskQueueCompletionRecoversFailedCleanupWithoutReexecuting(t *testing.T) {
	for _, recovery := range []string{"completion_replay", "dispatcher_retry"} {
		t.Run(recovery, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			ctx = auth.ContextWithAuthInfo(ctx, &auth.AuthInfo{Workspace: &types.Workspace{Name: "workspace"}})
			rdb, err := repository.NewRedisClientForTest()
			require.NoError(t, err)
			t.Cleanup(func() { _ = rdb.Close() })
			tasks := &completionTasks{TaskRepository: repository.NewTaskRedisRepository(rdb), err: errors.New("Redis cleanup unavailable")}
			require.NoError(t, tasks.SetTaskState(ctx, "workspace", "stub", "task", []byte(`{"task_id":"task"}`)))
			require.NoError(t, tasks.ClaimTask(ctx, "workspace", "stub", "task", "container"))
			dispatcher, err := task.NewDispatcher(ctx, tasks)
			require.NoError(t, err)
			backend := &completionLifecycleBackend{current: types.Task{ExternalId: "task", Status: types.TaskStatusRunning}, updates: make(chan error, 2)}
			queue := &RedisTaskQueue{config: completionTestConfig(), rdb: rdb, backendRepo: backend, taskDispatcher: dispatcher}
			request := &pb.TaskQueueCompleteRequest{TaskId: "task", StubId: "stub", ContainerId: "container", TaskStatus: string(types.TaskStatusComplete), TaskDuration: 1}
			response, err := queue.TaskQueueComplete(ctx, request)
			require.Equal(t, codes.Unavailable, status.Code(err))
			require.Nil(t, response)
			persisted, err := backend.GetTask(ctx, "task")
			require.NoError(t, err)
			require.Equal(t, types.TaskStatusComplete, persisted.Status)
			claimed, err := tasks.IsClaimed(ctx, "workspace", "stub", "task")
			require.NoError(t, err)
			require.True(t, claimed)

			tasks.err = nil
			if recovery == "completion_replay" {
				// A replay must not change the recorded outcome or duration.
				request.TaskStatus = string(types.TaskStatusComplete)
				request.TaskDuration = 99
				response, err = queue.TaskQueueComplete(ctx, request)
				require.NoError(t, err)
				require.True(t, response.Ok)
			} else {
				// No queue client is configured: retrying user code would fail.
				require.NoError(t, (&TaskQueueTask{tq: queue, msg: &types.TaskMessage{WorkspaceName: "workspace", StubId: "stub", TaskId: "task"}}).Retry(ctx))
			}
			current, err := backend.GetTask(ctx, "task")
			require.NoError(t, err)
			require.Equal(t, *persisted, *current)
			require.Len(t, backend.updates, 1, "cleanup recovery must not rewrite the terminal task")
			require.EqualValues(t, 1, rdb.LLen(ctx, Keys.taskQueueTaskDuration("workspace", "stub")).Val())
			inFlight, err := tasks.TasksInFlight(ctx, "workspace", "stub")
			require.NoError(t, err)
			require.Zero(t, inFlight)
		})
	}
}

func TestGatewayRestartDoesNotReleaseClaimedContainer(t *testing.T) {
	ctx := context.Background()
	redisServer := miniredis.RunT(t)
	rdb, err := common.NewRedisClient(types.RedisConfig{Addrs: []string{redisServer.Addr()}, Mode: types.RedisModeSingle})
	require.NoError(t, err)
	t.Cleanup(func() { _ = rdb.Close() })
	tasks := repository.NewTaskRedisRepository(rdb)
	require.NoError(t, tasks.ClaimTask(ctx, "workspace", "stub", "task", "container"))
	lease := Keys.taskQueueTaskRunningLock("workspace", "stub", "container", "task")
	index := Keys.taskQueueTaskRunningLockIndex("workspace", "stub", "container")
	require.NoError(t, rdb.SAdd(ctx, index, "task").Err())
	require.NoError(t, rdb.Set(ctx, lease, 1, types.TaskQueueRunningLeaseTTL).Err())
	require.NoError(t, rdb.Set(ctx, Keys.taskQueueTaskHeartbeat("workspace", "stub", "task"), 1, types.TaskHeartbeatTTL).Err())
	// No gateway remains to renew either key. Ownership must survive their TTLs.
	redisServer.FastForward(types.TaskHeartbeatTTL + types.TaskQueueRunningLeaseTTL)
	instance := &taskQueueInstance{AutoscaledInstance: &abstractions.AutoscaledInstance{
		Rdb: rdb, Workspace: &types.Workspace{Name: "workspace"}, IsActive: true, ContainerRepo: completionContainerRepository{},
		Stub: &types.StubWithRelated{Stub: types.Stub{ExternalId: "stub", Type: types.StubType(types.StubTypeTaskQueueDeployment)}},
	}}
	stoppable, err := instance.stoppableContainers()
	require.NoError(t, err)
	require.Empty(t, stoppable)
	require.True(t, rdb.SIsMember(ctx, index, "task").Val(), "the task must stay indexed across a gateway restart")

	for _, state := range []types.ContainerStatus{types.ContainerStatusRunning, types.ContainerStatusStopping} {
		job := &TaskQueueTask{msg: &types.TaskMessage{WorkspaceName: "workspace", StubId: "stub", TaskId: "task"}, tq: &RedisTaskQueue{
			rdb: rdb, backendRepo: &completionLifecycleBackend{current: types.Task{Status: types.TaskStatusRunning, ContainerId: "container"}},
			containerRepo: completionHeartbeatContainer{status: state},
		}}
		alive, err := job.HeartBeat(ctx)
		require.NoError(t, err)
		require.Equal(t, state == types.ContainerStatusRunning, alive, "a live runner must not be retried because its gateway restarted")
	}
	// Ownership moved to another container (or was released). The old container
	// must not stay pinned by the stale task index.
	require.NoError(t, tasks.ClaimTask(ctx, "workspace", "stub", "task", "replacement"))
	stoppable, err = instance.stoppableContainers()
	require.NoError(t, err)
	require.Equal(t, []string{"container"}, stoppable)
}

type completionHeartbeatContainer struct {
	repository.ContainerRepository
	status types.ContainerStatus
}

func (c completionHeartbeatContainer) GetContainerState(string) (*types.ContainerState, error) {
	return &types.ContainerState{ContainerId: "container", Status: c.status}, nil
}

func TestTaskQueueCompletionPersistsBeforeMonitorAllowsScaleDown(t *testing.T) {
	for _, disconnect := range []string{"connected", "monitor", "completion", "both"} {
		t.Run(disconnect, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			redisServer := miniredis.RunT(t)
			rdb, err := common.NewRedisClient(types.RedisConfig{Addrs: []string{redisServer.Addr()}, Mode: types.RedisModeSingle})
			require.NoError(t, err)
			t.Cleanup(func() { _ = rdb.Close() })
			uploadStarted, releaseUpload := make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(releaseUpload) }) }
			storageServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				close(uploadStarted)
				<-releaseUpload
				w.WriteHeader(http.StatusOK)
			}))
			t.Cleanup(storageServer.Close)
			t.Cleanup(release)
			id := uint(1)
			region, bucket, key, endpoint := "us-east-1", "bucket", "test-key", storageServer.URL
			workspace := &types.Workspace{Name: "workspace", Storage: &types.WorkspaceStorage{
				Id: &id, Region: &region, BucketName: &bucket, AccessKey: &key, SecretKey: &key, EndpointUrl: &endpoint,
			}}
			tasks := repository.NewTaskRedisRepository(rdb)
			require.NoError(t, tasks.SetTaskState(ctx, "workspace", "stub", "task", []byte(`{"task_id":"task"}`)))
			require.NoError(t, tasks.ClaimTask(ctx, "workspace", "stub", "task", "container"))
			leaseKey := Keys.taskQueueTaskRunningLock("workspace", "stub", "container", "task")
			require.NoError(t, rdb.SAdd(ctx, Keys.taskQueueTaskRunningLockIndex("workspace", "stub", "container"), "task").Err())
			require.NoError(t, rdb.Set(ctx, leaseKey, 1, time.Minute).Err())
			dispatcher, err := task.NewDispatcher(ctx, tasks)
			require.NoError(t, err)
			backend := &completionLifecycleBackend{
				current: types.Task{ExternalId: "task", Status: types.TaskStatusRunning, StartedAt: types.NullTime{}.Now()},
				updates: make(chan error, 1),
			}
			configs := common.NewSafeMap[*types.StubConfigV1]()
			configs.Set("stub", &types.StubConfigV1{TaskPolicy: types.TaskPolicy{Timeout: 480}})
			queue := &RedisTaskQueue{config: completionTestConfig(), ctx: ctx, rdb: rdb, taskRepo: tasks, backendRepo: backend, taskDispatcher: dispatcher, stubConfigCache: configs}
			rpcStarted := make(chan context.Context, 1)
			completionReturned := make(chan struct{})
			monitorReturned := make(chan struct{})
			server := grpc.NewServer(
				grpc.UnaryInterceptor(func(ctx context.Context, req interface{}, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
					defer close(completionReturned)
					rpcStarted <- ctx
					return handler(auth.ContextWithAuthInfo(ctx, &auth.AuthInfo{Workspace: workspace}), req)
				}),
				grpc.StreamInterceptor(func(srv interface{}, stream grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
					defer close(monitorReturned)
					return handler(srv, completionAuthStream{ServerStream: stream, ctx: auth.ContextWithAuthInfo(stream.Context(), &auth.AuthInfo{Workspace: workspace})})
				}),
			)
			pb.RegisterTaskQueueServiceServer(server, queue)
			listener := bufconn.Listen(1 << 20)
			go func() { _ = server.Serve(listener) }()
			t.Cleanup(server.Stop)
			connection, err := grpc.NewClient("passthrough:///taskqueue", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}))
			require.NoError(t, err)
			t.Cleanup(func() { _ = connection.Close() })
			client := pb.NewTaskQueueServiceClient(connection)
			monitorCtx, stopMonitor := context.WithCancel(ctx)
			defer stopMonitor()
			monitor, err := client.TaskQueueMonitor(monitorCtx, &pb.TaskQueueMonitorRequest{TaskId: "task", StubId: "stub", ContainerId: "container"})
			require.NoError(t, err)
			monitorReady, monitorCompleted := make(chan struct{}), make(chan struct{})
			go func() {
				var readyOnce sync.Once
				for {
					response, err := monitor.Recv()
					if err != nil {
						return
					}
					readyOnce.Do(func() { close(monitorReady) })
					if response.Complete {
						// Match the SDK: completion ends the monitor stream, so the
						// gateway no longer renews the container's five-second lease.
						stopMonitor()
						close(monitorCompleted)
						return
					}
				}
			}()
			awaitCompletion(t, monitorReady, "monitor did not start")
			completeCtx, stopRunner := context.WithCancel(ctx)
			defer stopRunner()
			type rpcOutcome struct {
				response *pb.TaskQueueCompleteResponse
				err      error
			}
			finished := make(chan rpcOutcome, 1)
			go func() {
				response, err := client.TaskQueueComplete(completeCtx, &pb.TaskQueueCompleteRequest{
					TaskId: "task", StubId: "stub", ContainerId: "container", TaskStatus: string(types.TaskStatusComplete), KeepWarmSeconds: 10, Result: []byte("result"),
				})
				finished <- rpcOutcome{response, err}
			}()
			awaitCompletion(t, uploadStarted, "result upload did not start")
			rpcCtx := <-rpcStarted
			if disconnect == "monitor" || disconnect == "both" {
				stopMonitor()
				awaitCompletion(t, monitorReturned, "monitor disconnect did not close the stream")
			}
			if disconnect == "completion" || disconnect == "both" {
				stopRunner()
				awaitCompletion(t, rpcCtx.Done(), "completion disconnect did not cancel the transport context")
			}
			claimedDuringUpload, err := tasks.IsClaimed(ctx, "workspace", "stub", "task")
			require.NoError(t, err)
			// Expire the deployment's ten-second keep-warm interval and the
			// monitor lease, then give an active monitor a chance to renew its lease.
			redisServer.FastForward(11 * time.Second)
			if !claimedDuringUpload {
				// The original ordering causes a premature Complete response. Wait
				// for the stream to close before expiring its final lease renewal.
				awaitCompletion(t, monitorCompleted, "monitor did not observe the removed claim")
				awaitCompletion(t, monitorReturned, "monitor stream did not close")
				redisServer.FastForward(6 * time.Second)
			} else {
				require.Eventually(t, func() bool { return rdb.Exists(ctx, leaseKey).Val() == 1 }, 3*time.Second, 10*time.Millisecond)
			}
			instance := &taskQueueInstance{AutoscaledInstance: &abstractions.AutoscaledInstance{
				Rdb: rdb, Workspace: workspace, IsActive: true, ContainerRepo: completionContainerRepository{},
				Stub: &types.StubWithRelated{Stub: types.Stub{ExternalId: "stub", Type: types.StubType(types.StubTypeTaskQueueDeployment)}},
			}}
			stoppable, err := instance.stoppableContainers()
			require.NoError(t, err)
			t.Logf("while result PUT is blocked: claimed=%v, stoppable=%v", claimedDuringUpload, stoppable)
			if len(stoppable) > 0 {
				// Exercise the original failure fully: the autoscaler can stop the
				// runner, disconnecting its completion RPC before the upload returns.
				stopRunner()
				awaitCompletion(t, rpcCtx.Done(), "runner disconnect did not cancel the server RPC")
			}
			release()
			updateErr := awaitCompletion(t, backend.updates, "completion never attempted its final database update")
			result := awaitCompletion(t, finished, "completion RPC did not finish")
			awaitCompletion(t, completionReturned, "accepted finalization did not finish after transport disconnection")
			current, err := backend.GetTask(ctx, "task")
			require.NoError(t, err)
			inFlight, err := tasks.TasksInFlight(ctx, "workspace", "stub")
			require.NoError(t, err)
			t.Logf("after result PUT finishes: update_error=%v, task_status=%s, dispatcher_entries=%d", updateErr, current.Status, inFlight)
			require.True(t, claimedDuringUpload)
			require.Empty(t, stoppable, "the autoscaler must not stop a container during finalization")
			require.NoError(t, updateErr)
			if disconnect == "completion" || disconnect == "both" {
				require.Equal(t, codes.Canceled, status.Code(result.err))
			} else {
				require.NoError(t, result.err)
				require.True(t, result.response.GetOk())
			}
			require.Equal(t, types.TaskStatusComplete, current.Status)
			require.Zero(t, inFlight)
		})
	}
}

type completionAuthStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s completionAuthStream) Context() context.Context { return s.ctx }

type completionContainerRepository struct{ repository.ContainerRepository }

func (completionContainerRepository) GetFailedContainersByStubId(string) ([]string, error) {
	return nil, nil
}

func (completionContainerRepository) GetActiveContainersByStubId(string) ([]types.ContainerState, error) {
	return []types.ContainerState{{ContainerId: "container", Status: types.ContainerStatusRunning}}, nil
}

type completionLifecycleBackend struct {
	repository.BackendRepository
	mu          sync.Mutex
	current     types.Task
	updates     chan error
	getErr      error
	blockUpdate bool
}

func (b *completionLifecycleBackend) GetTask(ctx context.Context, _ string) (*types.Task, error) {
	if b.getErr != nil {
		return nil, b.getErr
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	current := b.current
	return &current, nil
}

func (b *completionLifecycleBackend) GetTaskWithRelated(ctx context.Context, id string) (*types.TaskWithRelated, error) {
	task, err := b.GetTask(ctx, id)
	if err != nil {
		return nil, err
	}
	if task.ContainerId == "" {
		task.ContainerId = "container"
	}
	return &types.TaskWithRelated{Task: *task, Workspace: types.Workspace{Name: "workspace"}, Stub: types.Stub{ExternalId: "stub"}}, nil
}

func completionTestConfig() types.AppConfig {
	return types.AppConfig{GatewayService: types.GatewayServiceConfig{TaskCompletion: types.TaskCompletionConfig{Timeout: time.Minute, ResultUploadTimeout: time.Minute / 2}}}
}

func (b *completionLifecycleBackend) UpdateTask(ctx context.Context, _ string, updated types.Task) (*types.Task, error) {
	if b.blockUpdate {
		<-ctx.Done()
	}
	err := ctx.Err()
	if err == nil {
		b.mu.Lock()
		b.current = updated
		b.mu.Unlock()
	}
	b.updates <- err
	return &updated, err
}

func newBoundedCompletionFixture(t *testing.T) (context.Context, *RedisTaskQueue, *completionLifecycleBackend, *miniredis.Miniredis) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	ctx = auth.ContextWithAuthInfo(ctx, &auth.AuthInfo{Workspace: &types.Workspace{Name: "workspace"}})
	server := miniredis.RunT(t)
	rdb, err := common.NewRedisClient(types.RedisConfig{Addrs: []string{server.Addr()}, Mode: types.RedisModeSingle})
	require.NoError(t, err)
	t.Cleanup(func() { _ = rdb.Close() })
	tasks := repository.NewTaskRedisRepository(rdb)
	require.NoError(t, tasks.SetTaskState(ctx, "workspace", "stub", "task", []byte(`{"task_id":"task"}`)))
	require.NoError(t, tasks.ClaimTask(ctx, "workspace", "stub", "task", "container"))
	dispatcher, err := task.NewDispatcher(ctx, tasks)
	require.NoError(t, err)
	backend := &completionLifecycleBackend{current: types.Task{ExternalId: "task", Status: types.TaskStatusRunning, ContainerId: "container"}, updates: make(chan error, 4)}
	queue := &RedisTaskQueue{config: completionTestConfig(), rdb: rdb, taskRepo: tasks, taskDispatcher: dispatcher, backendRepo: backend}
	return ctx, queue, backend, server
}

func boundedCompletionRequest() *pb.TaskQueueCompleteRequest {
	return &pb.TaskQueueCompleteRequest{TaskId: "task", StubId: "stub", ContainerId: "container", TaskStatus: string(types.TaskStatusComplete)}
}

func TestCompletionRejectsPermanentFailuresBeforeRenewingAnyLease(t *testing.T) {
	for _, failure := range []string{"deleted", "expired", "wrong_container", "wrong_claim", "wrong_stub", "wrong_workspace", "missing_state"} {
		t.Run(failure, func(t *testing.T) {
			ctx, queue, backend, _ := newBoundedCompletionFixture(t)
			request := boundedCompletionRequest()
			request.KeepWarmSeconds = 30
			expected := codes.FailedPrecondition
			switch failure {
			case "deleted":
				backend.getErr = sql.ErrNoRows
				expected = codes.NotFound
			case "expired":
				backend.current.Status = types.TaskStatusExpired
			case "wrong_container":
				request.ContainerId = "intruder"
			case "wrong_claim":
				require.NoError(t, queue.taskRepo.ClaimTask(ctx, "workspace", "stub", "task", "replacement"))
			case "wrong_stub":
				request.StubId = "another-stub"
				expected = codes.PermissionDenied
			case "wrong_workspace":
				ctx = auth.ContextWithAuthInfo(ctx, &auth.AuthInfo{Workspace: &types.Workspace{Name: "intruder", ExternalId: "other"}})
				expected = codes.PermissionDenied
			case "missing_state":
				require.NoError(t, queue.rdb.Del(ctx, common.RedisKeys.TaskEntry("workspace", "stub", "task")).Err())
				expected = codes.NotFound
			}
			response, err := queue.TaskQueueComplete(ctx, request)
			require.Nil(t, response)
			require.Equal(t, expected, status.Code(err))
			require.Zero(t, queue.rdb.Exists(context.Background(),
				Keys.taskQueueTaskHeartbeat("workspace", "stub", "task"),
				Keys.taskQueueTaskRunningLock("workspace", "stub", request.ContainerId, "task"),
				Keys.taskQueueKeepWarmLock("workspace", "stub", request.ContainerId),
				common.RedisKeys.TaskCompletionDeadline("workspace", "stub", "task")).Val())
			require.Empty(t, backend.updates)
		})
	}
}

func TestHungDatabaseCannotRenewCompletionForeverOrResetItsDeadline(t *testing.T) {
	ctx, queue, backend, redisServer := newBoundedCompletionFixture(t)
	queue.config.GatewayService.TaskCompletion = types.TaskCompletionConfig{Timeout: 100 * time.Millisecond, ResultUploadTimeout: 50 * time.Millisecond}
	backend.blockUpdate = true
	_, err := queue.TaskQueueComplete(ctx, boundedCompletionRequest())
	require.Equal(t, codes.DeadlineExceeded, status.Code(err))
	key := common.RedisKeys.TaskCompletionDeadline("workspace", "stub", "task")
	deadline := queue.rdb.Get(ctx, key).Val()
	require.NotEmpty(t, deadline)
	_, err = queue.TaskQueueComplete(ctx, boundedCompletionRequest())
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Equal(t, deadline, queue.rdb.Get(ctx, key).Val(), "a retry must not extend the completion window")
	require.Len(t, backend.updates, 1)
	claimed, err := queue.taskRepo.IsClaimed(ctx, "workspace", "stub", "task")
	require.NoError(t, err)
	require.True(t, claimed, "recovery state must survive the failed write")
	redisServer.FastForward(time.Second)
	require.Zero(t, queue.rdb.Exists(ctx, Keys.taskQueueTaskHeartbeat("workspace", "stub", "task"), Keys.taskQueueTaskRunningLock("workspace", "stub", "container", "task")).Val())
	alive, err := (&TaskQueueTask{tq: queue, msg: &types.TaskMessage{WorkspaceName: "workspace", StubId: "stub", TaskId: "task"}}).HeartBeat(ctx)
	require.NoError(t, err)
	require.False(t, alive, "a live container must not hide an expired completion from recovery")
	instance := &taskQueueInstance{client: newRedisTaskQueueClient(queue.rdb, queue.taskRepo), AutoscaledInstance: &abstractions.AutoscaledInstance{Ctx: ctx, Rdb: queue.rdb, Workspace: &types.Workspace{Name: "workspace"}, IsActive: true, ContainerRepo: completionContainerRepository{}, Stub: &types.StubWithRelated{Stub: types.Stub{ExternalId: "stub", Type: types.StubType(types.StubTypeTaskQueueDeployment)}}}}
	stoppable, err := instance.stoppableContainers()
	require.NoError(t, err)
	require.Equal(t, []string{"container"}, stoppable, "retained recovery state must not pin the container")
	sample, err := taskQueueAutoscalerSampleFunc(instance)
	require.NoError(t, err)
	require.Zero(t, sample.QueueLength, "recovery state must not create demand for a replacement container")
	require.Zero(t, taskQueueScaleFunc(instance, sample).DesiredContainers)
	backend.getErr = errors.New("database remains unavailable")
	require.Error(t, queue.taskDispatcher.RetryTask(ctx, &TaskQueueTask{tq: queue, msg: &types.TaskMessage{WorkspaceName: "workspace", StubId: "stub", TaskId: "task", Policy: types.TaskPolicy{MaxRetries: 1}}}))
	sample, err = taskQueueAutoscalerSampleFunc(instance)
	require.NoError(t, err)
	require.Zero(t, sample.QueueLength, "failed recovery must not create demand before work is queued")
}

func TestCompletionDemandReturnsWhenTaskIsRetriedOrReclaimed(t *testing.T) {
	for _, reset := range []string{"retry", "claim"} {
		t.Run(reset, func(t *testing.T) {
			ctx, queue, _, _ := newBoundedCompletionFixture(t)
			client := newRedisTaskQueueClient(queue.rdb, queue.taskRepo)
			meta := types.TaskMetadata{WorkspaceName: "workspace", StubId: "stub", TaskId: "task", ContainerId: "container"}
			_, cancel, err := task.BeginCompletion(ctx, queue.rdb, meta, time.Minute)
			require.NoError(t, err)
			cancel()
			demand, err := client.ExecutionDemand(ctx, "workspace", "stub")
			require.NoError(t, err)
			require.Zero(t, demand)
			if reset == "retry" {
				require.NoError(t, queue.taskRepo.RemoveTaskClaim(ctx, "workspace", "stub", "task"))
				demand, err = client.ExecutionDemand(ctx, "workspace", "stub")
				require.NoError(t, err)
				require.Zero(t, demand, "recovery creates execution demand only once work is requeued")
				require.NoError(t, client.Push(ctx, &types.TaskMessage{WorkspaceName: "workspace", StubId: "stub", TaskId: "task"}))
			} else {
				require.NoError(t, queue.taskRepo.ClaimTask(ctx, "workspace", "stub", "task", "container"))
			}
			demand, err = client.ExecutionDemand(ctx, "workspace", "stub")
			require.NoError(t, err)
			require.Equal(t, 1, demand)
			require.NoError(t, queue.taskRepo.DeleteTaskState(ctx, "workspace", "stub", "task"))
			require.Zero(t, queue.rdb.SCard(ctx, common.RedisKeys.TaskCompletionIndex("workspace", "stub")).Val())
		})
	}
}

func TestHungResultUploadIsCancelledWithoutAcknowledgingCompletion(t *testing.T) {
	ctx, queue, backend, _ := newBoundedCompletionFixture(t)
	queue.config.GatewayService.TaskCompletion = types.TaskCompletionConfig{Timeout: time.Second, ResultUploadTimeout: 50 * time.Millisecond}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		<-r.Context().Done()
	}))
	t.Cleanup(server.Close)
	id := uint(1)
	region, bucket, credential, endpoint := "us-east-1", "bucket", "test-key", server.URL
	workspace := &types.Workspace{Name: "workspace", Storage: &types.WorkspaceStorage{Id: &id, Region: &region, BucketName: &bucket, AccessKey: &credential, SecretKey: &credential, EndpointUrl: &endpoint}}
	ctx = auth.ContextWithAuthInfo(ctx, &auth.AuthInfo{Workspace: workspace})
	request := boundedCompletionRequest()
	request.Result = []byte("result")
	_, err := queue.TaskQueueComplete(ctx, request)
	require.Equal(t, codes.DeadlineExceeded, status.Code(err))
	require.Empty(t, backend.updates)
	require.Equal(t, types.TaskStatusRunning, backend.current.Status)
	inFlight, err := queue.taskRepo.TasksInFlight(ctx, "workspace", "stub")
	require.NoError(t, err)
	require.Equal(t, 1, inFlight)
}

func TestCompletionLosingOwnershipCancelsItsUpload(t *testing.T) {
	ctx, queue, backend, _ := newBoundedCompletionFixture(t)
	started := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		close(started)
		<-r.Context().Done()
	}))
	t.Cleanup(server.Close)
	id := uint(1)
	region, bucket, credential, endpoint := "us-east-1", "bucket", "test-key", server.URL
	workspace := &types.Workspace{Name: "workspace", Storage: &types.WorkspaceStorage{Id: &id, Region: &region, BucketName: &bucket, AccessKey: &credential, SecretKey: &credential, EndpointUrl: &endpoint}}
	ctx = auth.ContextWithAuthInfo(ctx, &auth.AuthInfo{Workspace: workspace})
	request := boundedCompletionRequest()
	request.Result = []byte("result")
	finished := make(chan error, 1)
	go func() { _, err := queue.TaskQueueComplete(ctx, request); finished <- err }()
	awaitCompletion(t, started, "upload did not start")
	require.NoError(t, queue.taskRepo.ClaimTask(ctx, "workspace", "stub", "task", "replacement"))
	require.Equal(t, codes.Unavailable, status.Code(awaitCompletion(t, finished, "stale owner continued its upload")))
	require.Empty(t, backend.updates)
	require.Equal(t, "replacement", queue.rdb.Get(ctx, common.RedisKeys.TaskClaim("workspace", "stub", "task")).Val())
}

// Bound test coordination independently of the configured completion window.
func awaitCompletion[T any](t *testing.T, ch <-chan T, failure string) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(5 * time.Second):
		t.Fatal(failure)
		var zero T
		return zero
	}
}
