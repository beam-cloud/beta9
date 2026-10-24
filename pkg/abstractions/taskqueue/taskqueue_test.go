package taskqueue

import (
	"context"
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
			queue := &RedisTaskQueue{rdb: rdb, backendRepo: backend, taskDispatcher: dispatcher}
			response, err := queue.TaskQueueComplete(ctx, &pb.TaskQueueCompleteRequest{
				TaskId: "task", StubId: "stub", ContainerId: "container", TaskStatus: string(types.TaskStatusComplete),
			})
			require.NoError(t, err)
			require.Equal(t, !failUpdate, response.Ok)
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
			queue := &RedisTaskQueue{rdb: rdb, backendRepo: backend, taskDispatcher: dispatcher}
			request := &pb.TaskQueueCompleteRequest{TaskId: "task", StubId: "stub", ContainerId: "container", TaskStatus: string(types.TaskStatusComplete), TaskDuration: 1}
			response, err := queue.TaskQueueComplete(ctx, request)
			require.NoError(t, err)
			require.False(t, response.Ok)
			persisted, err := backend.GetTask(ctx, "task")
			require.NoError(t, err)
			require.Equal(t, types.TaskStatusComplete, persisted.Status)
			claimed, err := tasks.IsClaimed(ctx, "workspace", "stub", "task")
			require.NoError(t, err)
			require.True(t, claimed)

			tasks.err = nil
			if recovery == "completion_replay" {
				// A replay must not change the recorded outcome or duration.
				request.TaskStatus = string(types.TaskStatusError)
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
			queue := &RedisTaskQueue{ctx: ctx, rdb: rdb, taskRepo: tasks, backendRepo: backend, taskDispatcher: dispatcher, stubConfigCache: configs}
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
			select {
			case <-monitorReady:
			case <-time.After(5 * time.Second):
				t.Fatal("monitor did not start")
			}
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
			select {
			case <-uploadStarted:
			case <-time.After(5 * time.Second):
				t.Fatal("result upload did not start")
			}
			rpcCtx := <-rpcStarted
			if disconnect == "monitor" || disconnect == "both" {
				stopMonitor()
				select {
				case <-monitorReturned:
				case <-time.After(5 * time.Second):
					t.Fatal("monitor disconnect did not close the stream")
				}
			}
			if disconnect == "completion" || disconnect == "both" {
				stopRunner()
				select {
				case <-rpcCtx.Done():
				case <-time.After(5 * time.Second):
					t.Fatal("completion disconnect did not cancel the transport context")
				}
			}
			claimedDuringUpload, err := tasks.IsClaimed(ctx, "workspace", "stub", "task")
			require.NoError(t, err)
			// Expire the deployment's ten-second keep-warm interval and the
			// monitor lease, then give an active monitor a chance to renew its lease.
			redisServer.FastForward(11 * time.Second)
			if !claimedDuringUpload {
				// The original ordering causes a premature Complete response. Wait
				// for the stream to close before expiring its final lease renewal.
				select {
				case <-monitorCompleted:
				case <-time.After(5 * time.Second):
					t.Fatal("monitor did not observe the removed claim")
				}
				select {
				case <-monitorReturned:
				case <-time.After(5 * time.Second):
					t.Fatal("monitor stream did not close")
				}
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
				select {
				case <-rpcCtx.Done():
				case <-time.After(5 * time.Second):
					t.Fatal("runner disconnect did not cancel the server RPC")
				}
			}
			release()
			var updateErr error
			select {
			case updateErr = <-backend.updates:
			case <-time.After(5 * time.Second):
				t.Fatal("completion never attempted its final database update")
			}
			var result rpcOutcome
			select {
			case result = <-finished:
			case <-time.After(5 * time.Second):
				t.Fatal("completion RPC did not finish")
			}
			select {
			case <-completionReturned:
			case <-time.After(5 * time.Second):
				t.Fatal("accepted finalization did not finish after transport disconnection")
			}
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

func (completionContainerRepository) GetActiveContainersByStubId(string) ([]types.ContainerState, error) {
	return []types.ContainerState{{ContainerId: "container", Status: types.ContainerStatusRunning}}, nil
}

type completionLifecycleBackend struct {
	repository.BackendRepository
	mu      sync.Mutex
	current types.Task
	updates chan error
}

func (b *completionLifecycleBackend) GetTask(ctx context.Context, _ string) (*types.Task, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	current := b.current
	return &current, nil
}

func (b *completionLifecycleBackend) UpdateTask(ctx context.Context, _ string, updated types.Task) (*types.Task, error) {
	err := ctx.Err()
	if err == nil {
		b.mu.Lock()
		b.current = updated
		b.mu.Unlock()
	}
	b.updates <- err
	return &updated, err
}
