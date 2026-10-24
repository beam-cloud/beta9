package gatewayservices

import (
	"context"
	"errors"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	taskdispatcher "github.com/beam-cloud/beta9/pkg/task"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
)

type taskLifecycleBackendRepo struct {
	repository.BackendRepository
	task    *types.TaskWithRelated
	updates []types.Task
	update  func()
	err     error
}

func (r *taskLifecycleBackendRepo) GetTaskWithRelated(ctx context.Context, externalID string) (*types.TaskWithRelated, error) {
	if r.task == nil || r.task.ExternalId != externalID {
		return nil, nil
	}
	task := *r.task
	return &task, nil
}

func (r *taskLifecycleBackendRepo) UpdateTask(ctx context.Context, externalID string, updatedTask types.Task) (*types.Task, error) {
	if r.update != nil {
		r.update()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if r.err != nil {
		return nil, r.err
	}
	r.updates = append(r.updates, updatedTask)
	if r.task != nil && r.task.ExternalId == externalID {
		r.task.Task = updatedTask
	}
	return &updatedTask, nil
}

func TestStartTaskClaimsBeforePersisting(t *testing.T) {
	gws, taskRepo := newTaskLifecycleGateway(t, "workspace-id")
	updateStarted := make(chan struct{})
	allowUpdate := make(chan struct{})
	gws.backendRepo.(*taskLifecycleBackendRepo).update = func() {
		close(updateStarted)
		<-allowUpdate
	}

	response := make(chan *pb.StartTaskResponse, 1)
	go func() {
		resp, _ := gws.StartTask(restrictedTaskLifecycleContext("workspace-id"), &pb.StartTaskRequest{
			TaskId:      "task-id",
			ContainerId: "container-id",
		})
		response <- resp
	}()

	<-updateStarted
	claimed, err := taskRepo.IsClaimed(context.Background(), "workspace", "stub-id", "task-id")
	require.NoError(t, err)
	require.True(t, claimed)

	close(allowUpdate)
	require.True(t, (<-response).Ok)
	claimed, err = taskRepo.IsClaimed(context.Background(), "workspace", "stub-id", "task-id")
	require.NoError(t, err)
	require.True(t, claimed)
}

func TestStartTaskReleasesClaimAfterFailedUpdate(t *testing.T) {
	gws, taskRepo := newTaskLifecycleGateway(t, "workspace-id")
	gws.backendRepo.(*taskLifecycleBackendRepo).err = errors.New("update failed")

	resp, err := gws.StartTask(restrictedTaskLifecycleContext("workspace-id"), &pb.StartTaskRequest{
		TaskId:      "task-id",
		ContainerId: "container-id",
	})

	require.NoError(t, err)
	require.False(t, resp.Ok)
	claimed, err := taskRepo.IsClaimed(context.Background(), "workspace", "stub-id", "task-id")
	require.NoError(t, err)
	require.False(t, claimed)
}

func TestStartTaskReleasesClaimWhenUpdateCancelsRequest(t *testing.T) {
	gws, taskRepo := newTaskLifecycleGateway(t, "workspace-id")
	ctx, cancel := context.WithCancel(restrictedTaskLifecycleContext("workspace-id"))
	gws.backendRepo.(*taskLifecycleBackendRepo).update = cancel
	gws.backendRepo.(*taskLifecycleBackendRepo).err = context.Canceled

	resp, err := gws.StartTask(ctx, &pb.StartTaskRequest{
		TaskId:      "task-id",
		ContainerId: "container-id",
	})

	require.NoError(t, err)
	require.False(t, resp.Ok)
	claimed, err := taskRepo.IsClaimed(context.Background(), "workspace", "stub-id", "task-id")
	require.NoError(t, err)
	require.False(t, claimed)
}

func TestStartTaskAllowsRestrictedRuntimeToken(t *testing.T) {
	gws, taskRepo := newTaskLifecycleGateway(t, "workspace-id")
	ctx := restrictedTaskLifecycleContext("workspace-id")

	resp, err := gws.StartTask(ctx, &pb.StartTaskRequest{
		TaskId:      "task-id",
		ContainerId: "container-id",
	})

	require.NoError(t, err)
	require.True(t, resp.Ok)
	claimed, err := taskRepo.IsClaimed(context.Background(), "workspace", "stub-id", "task-id")
	require.NoError(t, err)
	require.True(t, claimed)
	require.Equal(t, types.TaskStatusRunning, gws.backendRepo.(*taskLifecycleBackendRepo).task.Status)
	require.Equal(t, "container-id", gws.backendRepo.(*taskLifecycleBackendRepo).task.ContainerId)
}

func TestStartTaskRejectsRestrictedRuntimeTokenForAnotherWorkspace(t *testing.T) {
	gws, taskRepo := newTaskLifecycleGateway(t, "workspace-id")
	ctx := restrictedTaskLifecycleContext("other-workspace")

	resp, err := gws.StartTask(ctx, &pb.StartTaskRequest{
		TaskId:      "task-id",
		ContainerId: "container-id",
	})

	require.NoError(t, err)
	require.False(t, resp.Ok)
	claimed, err := taskRepo.IsClaimed(context.Background(), "workspace", "stub-id", "task-id")
	require.NoError(t, err)
	require.False(t, claimed)
}

func TestEndTaskAllowsRestrictedRuntimeToken(t *testing.T) {
	gws, taskRepo := newTaskLifecycleGateway(t, "workspace-id")
	task := gws.backendRepo.(*taskLifecycleBackendRepo).task
	task.Status = types.TaskStatusRunning
	task.ContainerId = "container-id"
	require.NoError(t, taskRepo.ClaimTask(context.Background(), "workspace", "stub-id", "task-id", "container-id"))

	resp, err := gws.EndTask(restrictedTaskLifecycleContext("workspace-id"), &pb.EndTaskRequest{
		TaskId:          "task-id",
		ContainerId:     "container-id",
		TaskStatus:      string(types.TaskStatusComplete),
		TaskDuration:    1,
		KeepWarmSeconds: 10,
	})

	require.NoError(t, err)
	require.True(t, resp.Ok)
	claimed, err := taskRepo.IsClaimed(context.Background(), "workspace", "stub-id", "task-id")
	require.NoError(t, err)
	require.False(t, claimed)
	require.Equal(t, types.TaskStatusComplete, task.Status)
}

func TestEndTaskPersistsAfterClientDisconnect(t *testing.T) {
	gws, tasks := newTaskLifecycleGateway(t, "workspace-id")
	ctx, cancel := context.WithCancel(restrictedTaskLifecycleContext("workspace-id"))
	defer cancel()
	backend := gws.backendRepo.(*taskLifecycleBackendRepo)
	backend.task.Status = types.TaskStatusRunning
	backend.task.Stub.Type = types.StubType(types.StubTypeFunction)
	require.NoError(t, tasks.SetTaskState(ctx, "workspace", "stub-id", "task-id", []byte(`{"task_id":"task-id"}`)))
	require.NoError(t, tasks.ClaimTask(ctx, "workspace", "stub-id", "task-id", "container-id"))
	backend.update = func() {
		cancel()
		claimed, err := tasks.IsClaimed(context.Background(), "workspace", "stub-id", "task-id")
		require.NoError(t, err)
		require.True(t, claimed, "the final database write must still own the claim")
	}
	response, err := gws.EndTask(ctx, &pb.EndTaskRequest{TaskId: "task-id", TaskStatus: string(types.TaskStatusComplete)})
	require.NoError(t, err)
	require.True(t, response.Ok)
	require.Equal(t, types.TaskStatusComplete, backend.task.Status)
	inFlight, err := tasks.TasksInFlight(context.Background(), "workspace", "stub-id")
	require.NoError(t, err)
	require.Zero(t, inFlight)
}

func TestEndTaskRetainsRecoveryStateOnDatabaseFailure(t *testing.T) {
	gws, tasks := newTaskLifecycleGateway(t, "workspace-id")
	ctx := restrictedTaskLifecycleContext("workspace-id")
	backend := gws.backendRepo.(*taskLifecycleBackendRepo)
	backend.task.Status = types.TaskStatusRunning
	backend.err = errors.New("database unavailable")
	require.NoError(t, tasks.SetTaskState(ctx, "workspace", "stub-id", "task-id", []byte(`{"task_id":"task-id"}`)))
	require.NoError(t, tasks.ClaimTask(ctx, "workspace", "stub-id", "task-id", "container-id"))
	response, err := gws.EndTask(ctx, &pb.EndTaskRequest{TaskId: "task-id", TaskStatus: string(types.TaskStatusComplete)})
	require.NoError(t, err)
	require.False(t, response.Ok)
	require.Equal(t, types.TaskStatusRunning, backend.task.Status)
	claimed, err := tasks.IsClaimed(ctx, "workspace", "stub-id", "task-id")
	require.NoError(t, err)
	require.True(t, claimed)
	inFlight, err := tasks.TasksInFlight(ctx, "workspace", "stub-id")
	require.NoError(t, err)
	require.Equal(t, 1, inFlight)
}

type completionCleanupRepository struct {
	repository.TaskRepository
	err error
}

func (r *completionCleanupRepository) DeleteTaskState(ctx context.Context, workspace, stub, taskID string) error {
	if r.err != nil {
		return r.err
	}
	return r.TaskRepository.DeleteTaskState(ctx, workspace, stub, taskID)
}

func TestEndTaskReplayRecoversFailedCleanup(t *testing.T) {
	gws, tasks := newTaskLifecycleGateway(t, "workspace-id")
	ctx, cancel := context.WithCancel(restrictedTaskLifecycleContext("workspace-id"))
	defer cancel()
	cleanup := &completionCleanupRepository{TaskRepository: tasks, err: errors.New("Redis cleanup unavailable")}
	dispatcher, err := taskdispatcher.NewDispatcher(ctx, cleanup)
	require.NoError(t, err)
	gws.taskDispatcher = dispatcher
	backend := gws.backendRepo.(*taskLifecycleBackendRepo)
	backend.task.Status = types.TaskStatusRunning
	require.NoError(t, tasks.SetTaskState(ctx, "workspace", "stub-id", "task-id", []byte(`{"task_id":"task-id"}`)))
	require.NoError(t, tasks.ClaimTask(ctx, "workspace", "stub-id", "task-id", "container-id"))
	request := &pb.EndTaskRequest{TaskId: "task-id", TaskStatus: string(types.TaskStatusComplete)}
	response, err := gws.EndTask(ctx, request)
	require.NoError(t, err)
	require.False(t, response.Ok)
	require.Equal(t, types.TaskStatusComplete, backend.task.Status)
	persisted := backend.task.Task
	claimed, err := tasks.IsClaimed(ctx, "workspace", "stub-id", "task-id")
	require.NoError(t, err)
	require.True(t, claimed)

	cleanup.err = nil
	response, err = gws.EndTask(ctx, request)
	require.NoError(t, err)
	require.True(t, response.Ok)
	require.Equal(t, persisted, backend.task.Task)
	require.Len(t, backend.updates, 1)
	inFlight, err := tasks.TasksInFlight(ctx, "workspace", "stub-id")
	require.NoError(t, err)
	require.Zero(t, inFlight)
}

func newTaskLifecycleGateway(t *testing.T, workspaceID string) (*GatewayService, repository.TaskRepository) {
	t.Helper()

	server, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(server.Close)

	rdb, err := common.NewRedisClient(types.RedisConfig{Addrs: []string{server.Addr()}, Mode: types.RedisModeSingle})
	require.NoError(t, err)
	t.Cleanup(func() { _ = rdb.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	taskRepo := repository.NewTaskRedisRepository(rdb)
	dispatcher, err := taskdispatcher.NewDispatcher(ctx, taskRepo)
	require.NoError(t, err)

	backendRepo := &taskLifecycleBackendRepo{
		task: &types.TaskWithRelated{
			Task: types.Task{
				ExternalId:  "task-id",
				Status:      types.TaskStatusPending,
				WorkspaceId: 1,
				StubId:      2,
			},
			Workspace: types.Workspace{
				Id:         1,
				ExternalId: workspaceID,
				Name:       "workspace",
			},
			Stub: types.Stub{
				Id:         2,
				ExternalId: "stub-id",
				Type:       types.StubType(types.StubTypeEndpointServe),
				Config:     "{}",
			},
		},
	}

	return &GatewayService{
		backendRepo:    backendRepo,
		redisClient:    rdb,
		taskDispatcher: dispatcher,
	}, taskRepo
}

func restrictedTaskLifecycleContext(workspaceID string) context.Context {
	return auth.ContextWithAuthInfo(context.Background(), &auth.AuthInfo{
		Workspace: &types.Workspace{Id: 1, ExternalId: workspaceID, Name: "workspace"},
		Token:     &types.Token{TokenType: types.TokenTypeWorkspaceRestricted, Active: true},
	})
}

func TestStartTaskReplayKeepsOriginalStartAndClaim(t *testing.T) {
	gws, taskRepo := newTaskLifecycleGateway(t, "workspace-id")
	ctx := restrictedTaskLifecycleContext("workspace-id")
	request := &pb.StartTaskRequest{TaskId: "task-id", ContainerId: "container-id"}
	first, err := gws.StartTask(ctx, request)
	require.NoError(t, err)
	require.True(t, first.Ok)
	backend := gws.backendRepo.(*taskLifecycleBackendRepo)
	startedAt := backend.task.StartedAt
	second, err := gws.StartTask(ctx, request)
	require.NoError(t, err)
	require.True(t, second.Ok)
	require.Equal(t, startedAt, backend.task.StartedAt)
	require.Len(t, backend.updates, 1)
	claimed, err := taskRepo.IsClaimed(ctx, "workspace", "stub-id", "task-id")
	require.NoError(t, err)
	require.True(t, claimed)
	request.ContainerId = "another-container"
	second, err = gws.StartTask(ctx, request)
	require.NoError(t, err)
	require.False(t, second.Ok)
}
