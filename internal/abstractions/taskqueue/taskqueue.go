package taskqueue

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	acommon "github.com/beam-cloud/beta9/internal/abstractions/common"
	"github.com/beam-cloud/beta9/internal/auth"
	common "github.com/beam-cloud/beta9/internal/common"

	"github.com/beam-cloud/beta9/internal/repository"
	"github.com/beam-cloud/beta9/internal/scheduler"
	"github.com/beam-cloud/beta9/internal/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/labstack/echo/v4"
)

type TaskQueueService interface {
	pb.TaskQueueServiceServer
	TaskQueuePut(context.Context, *pb.TaskQueuePutRequest) (*pb.TaskQueuePutResponse, error)
	TaskQueuePop(context.Context, *pb.TaskQueuePopRequest) (*pb.TaskQueuePopResponse, error)
	TaskQueueLength(context.Context, *pb.TaskQueueLengthRequest) (*pb.TaskQueueLengthResponse, error)
	TaskQueueComplete(ctx context.Context, in *pb.TaskQueueCompleteRequest) (*pb.TaskQueueCompleteResponse, error)
	TaskQueueMonitor(req *pb.TaskQueueMonitorRequest, stream pb.TaskQueueService_TaskQueueMonitorServer) error
}

const (
	taskQueueContainerPrefix string = "taskqueue"
	taskQueueRoutePrefix     string = "/taskqueue"
)

type RedisTaskQueue struct {
	ctx           context.Context
	rdb           *common.RedisClient
	containerRepo repository.ContainerRepository
	backendRepo   repository.BackendRepository
	scheduler     *scheduler.Scheduler
	pb.UnimplementedTaskQueueServiceServer
	queueInstances  *common.SafeMap[*taskQueueInstance]
	keyEventManager *common.KeyEventManager
	keyEventChan    chan common.KeyEvent
	queueClient     *taskQueueClient
}

func NewRedisTaskQueueService(
	ctx context.Context,
	rdb *common.RedisClient,
	scheduler *scheduler.Scheduler,
	containerRepo repository.ContainerRepository,
	backendRepo repository.BackendRepository,
	baseRouteGroup *echo.Group,
) (TaskQueueService, error) {
	keyEventChan := make(chan common.KeyEvent)
	keyEventManager, err := common.NewKeyEventManager(rdb)
	if err != nil {
		return nil, err
	}

	go keyEventManager.ListenForPattern(ctx, common.RedisKeys.SchedulerContainerState(taskQueueContainerPrefix), keyEventChan)

	tq := &RedisTaskQueue{
		ctx:             ctx,
		rdb:             rdb,
		scheduler:       scheduler,
		keyEventChan:    keyEventChan,
		keyEventManager: keyEventManager,
		containerRepo:   containerRepo,
		backendRepo:     backendRepo,
		queueClient:     newRedisTaskQueueClient(rdb),
		queueInstances:  common.NewSafeMap[*taskQueueInstance](),
	}

	go tq.handleContainerEvents()
	go tq.monitorTasks()

	// Register HTTP routes
	authMiddleware := auth.AuthMiddleware(backendRepo)
	registerTaskQueueRoutes(baseRouteGroup.Group(taskQueueRoutePrefix, authMiddleware), tq)

	return tq, nil
}

func (tq *RedisTaskQueue) TaskQueuePut(ctx context.Context, in *pb.TaskQueuePutRequest) (*pb.TaskQueuePutResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	var payload acommon.FunctionPayload
	err := json.Unmarshal(in.Payload, &payload)
	if err != nil {
		return &pb.TaskQueuePutResponse{
			Ok: false,
		}, nil
	}

	taskId, err := tq.put(ctx, authInfo, in.StubId, &payload)
	return &pb.TaskQueuePutResponse{
		Ok:     err == nil,
		TaskId: taskId,
	}, nil
}

func (tq *RedisTaskQueue) put(ctx context.Context, authInfo *auth.AuthInfo, stubId string, payload *acommon.FunctionPayload) (string, error) {
	queue, exists := tq.queueInstances.Get(stubId)
	if !exists {
		err := tq.createQueueInstance(stubId)
		if err != nil {
			return "", err
		}

		queue, _ = tq.queueInstances.Get(stubId)
	}

	task, err := tq.backendRepo.CreateTask(ctx, "", authInfo.Workspace.Id, queue.stub.Id)
	if err != nil {
		return "", err
	}

	err = queue.client.Push(queue.workspace.Name, queue.stub.ExternalId, task.ExternalId, payload.Args, payload.Kwargs)
	if err != nil {
		tq.backendRepo.DeleteTask(ctx, task.ExternalId)
		return "", err
	}

	return task.ExternalId, nil
}

func (tq *RedisTaskQueue) TaskQueuePop(ctx context.Context, in *pb.TaskQueuePopRequest) (*pb.TaskQueuePopResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	queue, exists := tq.queueInstances.Get(in.StubId)
	if !exists {
		err := tq.createQueueInstance(in.StubId)
		if err != nil {
			return &pb.TaskQueuePopResponse{
				Ok: false,
			}, nil
		}

		queue, _ = tq.queueInstances.Get(in.StubId)
	}

	msg, err := queue.client.Pop(authInfo.Workspace.Name, in.StubId, in.ContainerId)
	if err != nil || msg == nil {
		return &pb.TaskQueuePopResponse{Ok: false}, nil
	}

	var tm types.TaskMessage
	err = tm.Decode(msg)
	if err != nil {
		return nil, err
	}

	task, err := tq.backendRepo.GetTask(ctx, tm.ID)
	if err != nil {
		return &pb.TaskQueuePopResponse{
			Ok: false,
		}, nil
	}
	task.ContainerId = in.ContainerId
	task.StartedAt = sql.NullTime{Time: time.Now(), Valid: true}
	task.Status = types.TaskStatusRunning

	err = tq.rdb.SetEx(context.TODO(), Keys.taskQueueTaskRunningLock(authInfo.Workspace.Name, in.StubId, in.ContainerId, tm.ID), 1, time.Duration(defaultTaskRunningExpiration)*time.Second).Err()
	if err != nil {
		return &pb.TaskQueuePopResponse{
			Ok: false,
		}, nil
	}

	_, err = tq.backendRepo.UpdateTask(ctx, task.ExternalId, *task)
	return &pb.TaskQueuePopResponse{
		Ok: true, TaskMsg: msg,
	}, nil
}

func (tq *RedisTaskQueue) TaskQueueComplete(ctx context.Context, in *pb.TaskQueueCompleteRequest) (*pb.TaskQueueCompleteResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	task, err := tq.backendRepo.GetTask(ctx, in.TaskId)
	if err != nil {
		return &pb.TaskQueueCompleteResponse{
			Ok: false,
		}, nil
	}

	err = tq.rdb.SetEx(context.TODO(), Keys.taskQueueKeepWarmLock(authInfo.Workspace.Name, in.StubId, in.ContainerId), 1, time.Duration(in.KeepWarmSeconds)*time.Second).Err()
	if err != nil {
		return &pb.TaskQueueCompleteResponse{
			Ok: false,
		}, nil
	}

	err = tq.rdb.Del(context.TODO(), Keys.taskQueueTaskClaim(authInfo.Workspace.Name, in.StubId, task.ExternalId)).Err()
	if err != nil {
		return &pb.TaskQueueCompleteResponse{
			Ok: false,
		}, nil
	}

	err = tq.rdb.Del(context.TODO(), Keys.taskQueueTaskRunningLock(authInfo.Workspace.Name, in.StubId, in.ContainerId, in.TaskId)).Err()
	if err != nil {
		return &pb.TaskQueueCompleteResponse{
			Ok: false,
		}, nil
	}

	err = tq.rdb.RPush(context.TODO(), Keys.taskQueueTaskDuration(authInfo.Workspace.Name, in.StubId), in.TaskDuration).Err()
	if err != nil {
		return &pb.TaskQueueCompleteResponse{
			Ok: false,
		}, nil
	}

	task.EndedAt = sql.NullTime{Time: time.Now(), Valid: true}
	task.Status = types.TaskStatus(in.TaskStatus)

	_, err = tq.backendRepo.UpdateTask(ctx, task.ExternalId, *task)
	return &pb.TaskQueueCompleteResponse{
		Ok: err == nil,
	}, nil
}

func (tq *RedisTaskQueue) TaskQueueMonitor(req *pb.TaskQueueMonitorRequest, stream pb.TaskQueueService_TaskQueueMonitorServer) error {
	authInfo, _ := auth.AuthInfoFromContext(stream.Context())

	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()

	task, err := tq.backendRepo.GetTask(ctx, req.TaskId)
	if err != nil {
		return err
	}

	stub, err := tq.backendRepo.GetStubByExternalId(stream.Context(), req.StubId)
	if err != nil {
		return err
	}

	var stubConfig types.StubConfigV1 = types.StubConfigV1{}
	err = json.Unmarshal([]byte(stub.Config), &stubConfig)
	if err != nil {
		return err
	}

	timeout := int64(stubConfig.TaskPolicy.Timeout)
	timeoutCallback := func() error {
		task.Status = types.TaskStatusTimeout
		_, err = tq.backendRepo.UpdateTask(
			stream.Context(),
			task.ExternalId,
			*task,
		)
		if err != nil {
			return err
		}

		return nil
	}

	// Listen for task cancellation events
	channelKey := Keys.taskQueueTaskCancel(authInfo.Workspace.Name, req.StubId, task.ExternalId)
	cancelFlag := make(chan bool, 1)
	timeoutFlag := make(chan bool, 1)

	var timeoutChan <-chan time.Time
	taskStartTimeSeconds := task.StartedAt.Time.Unix()
	currentTimeSeconds := time.Now().Unix()
	timeoutTimeSeconds := taskStartTimeSeconds + timeout
	leftoverTimeoutSeconds := timeoutTimeSeconds - currentTimeSeconds

	if timeout <= 0 {
		timeoutChan = make(<-chan time.Time) // Indicates that the task does not have a timeout
	} else if leftoverTimeoutSeconds <= 0 {
		err := timeoutCallback()
		if err != nil {
			log.Printf("error timing out task: %v", err)
			return err
		}

		timeoutFlag <- true
		timeoutChan = make(<-chan time.Time)
	} else {
		timeoutChan = time.After(time.Duration(leftoverTimeoutSeconds) * time.Second)
	}

	go func() {
	retry:
		for {
			messages, errs := tq.rdb.Subscribe(ctx, channelKey)

			for {
				select {
				case <-timeoutChan:
					err := timeoutCallback()
					if err != nil {
						log.Printf("error timing out task: %v", err)
					}
					timeoutFlag <- true
					return

				case msg := <-messages:
					if msg.Payload == task.ExternalId {
						cancelFlag <- true
						return
					}

				case <-ctx.Done():
					return

				case err := <-errs:
					log.Printf("error with monitor task subscription: %v", err)
					break retry
				}
			}
		}
	}()

	for {
		select {
		case <-stream.Context().Done():
			return nil

		case <-cancelFlag:
			err := tq.rdb.Del(context.TODO(), Keys.taskQueueTaskClaim(authInfo.Workspace.Name, req.StubId, task.ExternalId)).Err()
			if err != nil {
				return err
			}

			stream.Send(&pb.TaskQueueMonitorResponse{Ok: true, Cancelled: true, Complete: false, TimedOut: false})
			return nil

		case <-timeoutFlag:
			err := tq.rdb.Del(context.TODO(), Keys.taskQueueTaskClaim(authInfo.Workspace.Name, req.StubId, task.ExternalId)).Err()
			if err != nil {
				log.Printf("error deleting task claim: %v", err)
				continue
			}

			stream.Send(&pb.TaskQueueMonitorResponse{Ok: true, Cancelled: false, Complete: false, TimedOut: true})
			return nil

		default:
			if task.Status == types.TaskStatusCancelled {
				stream.Send(&pb.TaskQueueMonitorResponse{Ok: true, Cancelled: true, Complete: false, TimedOut: false})
				return nil
			}

			err := tq.rdb.SetEx(context.TODO(), Keys.taskQueueTaskHeartbeat(authInfo.Workspace.Name, req.StubId, task.ExternalId), 1, time.Duration(60)*time.Second).Err()
			if err != nil {
				return err
			}

			err = tq.rdb.SetEx(context.TODO(), Keys.taskQueueTaskRunningLock(authInfo.Workspace.Name, req.StubId, req.ContainerId, task.ExternalId), 1, time.Duration(defaultTaskRunningExpiration)*time.Second).Err()
			if err != nil {
				return err
			}

			// Check if the task is currently claimed
			exists, err := tq.rdb.Exists(context.TODO(), Keys.taskQueueTaskClaim(authInfo.Workspace.Name, req.StubId, task.ExternalId)).Result()
			if err != nil {
				return err
			}

			// If the task claim key doesn't exist, send a message and return
			if exists == 0 {
				stream.Send(&pb.TaskQueueMonitorResponse{Ok: true, Cancelled: false, Complete: true, TimedOut: false})
				return nil
			}

			stream.Send(&pb.TaskQueueMonitorResponse{Ok: true, Cancelled: false, Complete: false, TimedOut: false})
			time.Sleep(time.Second * 1)
		}
	}
}

func (tq *RedisTaskQueue) TaskQueueLength(ctx context.Context, in *pb.TaskQueueLengthRequest) (*pb.TaskQueueLengthResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	length, err := tq.queueClient.QueueLength(authInfo.Workspace.Name, in.StubId)
	if err != nil {
		return &pb.TaskQueueLengthResponse{Ok: false}, nil
	}

	return &pb.TaskQueueLengthResponse{
		Ok:     true,
		Length: length,
	}, nil
}

func (tq *RedisTaskQueue) createQueueInstance(stubId string) error {
	_, exists := tq.queueInstances.Get(stubId)
	if exists {
		return errors.New("queue already in memory")
	}

	stub, err := tq.backendRepo.GetStubByExternalId(tq.ctx, stubId)
	if err != nil {
		return errors.New("invalid stub id")
	}

	var stubConfig *types.StubConfigV1 = &types.StubConfigV1{}
	err = json.Unmarshal([]byte(stub.Config), stubConfig)
	if err != nil {
		return err
	}

	token, err := tq.backendRepo.RetrieveActiveToken(tq.ctx, stub.Workspace.Id)
	if err != nil {
		return err
	}

	ctx, cancelFunc := context.WithCancel(tq.ctx)
	lock := common.NewRedisLock(tq.rdb)
	queue := &taskQueueInstance{
		ctx:                ctx,
		cancelFunc:         cancelFunc,
		lock:               lock,
		name:               fmt.Sprintf("%s-%s", stub.Name, stub.ExternalId),
		workspace:          &stub.Workspace,
		stub:               &stub.Stub,
		object:             &stub.Object,
		token:              token,
		stubConfig:         stubConfig,
		scheduler:          tq.scheduler,
		containerRepo:      tq.containerRepo,
		containerEventChan: make(chan types.ContainerEvent, 1),
		containers:         make(map[string]bool),
		scaleEventChan:     make(chan int, 1),
		rdb:                tq.rdb,
		client:             tq.queueClient,
	}

	autoscaler := newAutoscaler(queue)
	queue.autoscaler = autoscaler

	tq.queueInstances.Set(stubId, queue)
	go queue.monitor()

	// Clean up the queue instance once it's done
	go func(q *taskQueueInstance) {
		<-q.ctx.Done()
		tq.queueInstances.Delete(stubId)
	}(queue)

	return nil
}

// Monitor tasks -- if a container is killed unexpectedly, it will automatically be re-added to the queue
func (tq *RedisTaskQueue) monitorTasks() {
	monitorRate := time.Duration(5) * time.Second
	ticker := time.NewTicker(monitorRate)
	defer ticker.Stop()

	for {
		select {
		case <-tq.ctx.Done():
			return
		case <-ticker.C:
			claimedTasks, err := tq.rdb.Scan(tq.ctx, Keys.taskQueueTaskClaim("*", "*", "*"))
			if err != nil {
				return
			}

			for _, claimKey := range claimedTasks {
				v := strings.Split(claimKey, ":")
				workspaceName := v[1]
				stubId := v[2]
				taskId := v[5]

				res, err := tq.rdb.Exists(tq.ctx, Keys.taskQueueTaskHeartbeat(workspaceName, stubId, taskId)).Result()
				if err != nil {
					continue
				}

				recentHeartbeat := res > 0
				if !recentHeartbeat {
					log.Printf("<taskqueue> missing heartbeat, reinserting task<%s:%s> into queue: %s\n", workspaceName, taskId, stubId)

					retries, err := tq.rdb.Get(tq.ctx, Keys.taskQueueTaskRetries(workspaceName, stubId, taskId)).Int()
					if err != nil {
						retries = 0
					}

					task, err := tq.backendRepo.GetTaskWithRelated(tq.ctx, taskId)
					if err != nil {
						continue
					}

					var stubConfig types.StubConfigV1 = types.StubConfigV1{}
					err = json.Unmarshal([]byte(task.Stub.Config), &stubConfig)
					if err != nil {
						continue
					}

					taskPolicy := stubConfig.TaskPolicy
					if retries >= int(taskPolicy.MaxRetries) {
						log.Printf("<taskqueue> hit retry limit, not reinserting task <%s> into queue: %s\n", taskId, stubId)

						task.Task.Status = types.TaskStatusError
						_, err = tq.backendRepo.UpdateTask(tq.ctx, taskId, task.Task)
						if err != nil {
							continue
						}

						err = tq.rdb.Del(tq.ctx, Keys.taskQueueTaskClaim(workspaceName, stubId, taskId)).Err()
						if err != nil {
							log.Printf("<taskqueue> unable to delete task claim: %s\n", taskId)
						}

						continue
					}

					retries += 1
					err = tq.rdb.Set(tq.ctx, Keys.taskQueueTaskRetries(workspaceName, stubId, taskId), retries, 0).Err()
					if err != nil {
						continue
					}

					task.Task.Status = types.TaskStatusRetry
					_, err = tq.backendRepo.UpdateTask(tq.ctx, taskId, task.Task)
					if err != nil {
						continue
					}

					encodedMessage, err := tq.rdb.Get(tq.ctx, Keys.taskQueueTaskClaim(workspaceName, stubId, taskId)).Bytes()
					if err != nil {
						continue
					}

					err = tq.rdb.Del(tq.ctx, Keys.taskQueueTaskClaim(workspaceName, stubId, taskId)).Err()
					if err != nil {
						log.Printf("<taskqueue> unable to delete task claim: %s\n", taskId)
						continue
					}

					err = tq.rdb.RPush(tq.ctx, Keys.taskQueueList(workspaceName, stubId), encodedMessage).Err()
					if err != nil {
						log.Printf("<taskqueue> unable to insert task <%s> into queue <%s>: %v\n", taskId, stubId, err)
						continue
					}

					_, exists := tq.queueInstances.Get(stubId)
					if !exists {
						err := tq.createQueueInstance(stubId)
						if err != nil {
							continue
						}
					}

				}
			}
		}
	}
}

func (tq *RedisTaskQueue) handleContainerEvents() {
	for {
		select {
		case event := <-tq.keyEventChan:
			containerId := fmt.Sprintf("%s-%s", taskQueueContainerPrefix, event.Key)

			operation := event.Operation
			containerIdParts := strings.Split(containerId, "-")
			stubId := strings.Join(containerIdParts[1:6], "-")

			queue, exists := tq.queueInstances.Get(stubId)
			if !exists {
				err := tq.createQueueInstance(stubId)
				if err != nil {
					continue
				}

				queue, _ = tq.queueInstances.Get(stubId)
			}

			switch operation {
			case common.KeyOperationSet, common.KeyOperationHSet:
				queue.containerEventChan <- types.ContainerEvent{
					ContainerId: containerId,
					Change:      +1,
				}
			case common.KeyOperationDel, common.KeyOperationExpired:
				queue.containerEventChan <- types.ContainerEvent{
					ContainerId: containerId,
					Change:      -1,
				}
			}

		case <-tq.ctx.Done():
			return
		}
	}
}

// Redis keys
var (
	taskQueuePrefix              string = "taskqueue"
	taskQueueInstanceLock        string = "taskqueue:%s:%s:instance_lock"
	taskQueueList                string = "taskqueue:%s:%s"
	taskQueueTaskDuration        string = "taskqueue:%s:%s:task_duration"
	taskQueueAverageTaskDuration string = "taskqueue:%s:%s:avg_task_duration"
	taskQueueTaskClaim           string = "taskqueue:%s:%s:task:claim:%s"
	taskQueueTaskCancel          string = "taskqueue:%s:%s:task:cancel:%s"
	taskQueueTaskRetries         string = "taskqueue:%s:%s:task:retries:%s"
	taskQueueTaskHeartbeat       string = "taskqueue:%s:%s:task:heartbeat:%s"
	taskQueueProcessingLock      string = "taskqueue:%s:%s:processing_lock:%s"
	taskQueueKeepWarmLock        string = "taskqueue:%s:%s:keep_warm_lock:%s"
	taskQueueTaskRunningLock     string = "taskqueue:%s:%s:task_running:%s:%s"
)

var Keys = &keys{}

type keys struct{}

func (k *keys) taskQueuePrefix() string {
	return taskQueuePrefix
}

func (k *keys) taskQueueInstanceLock(workspaceName, stubId string) string {
	return fmt.Sprintf(taskQueueInstanceLock, workspaceName, stubId)
}

func (k *keys) taskQueueList(workspaceName, stubId string) string {
	return fmt.Sprintf(taskQueueList, workspaceName, stubId)
}

func (k *keys) taskQueueTaskClaim(workspaceName, stubId, taskId string) string {
	return fmt.Sprintf(taskQueueTaskClaim, workspaceName, stubId, taskId)
}

func (k *keys) taskQueueTaskCancel(workspaceName, stubId, taskId string) string {
	return fmt.Sprintf(taskQueueTaskCancel, workspaceName, stubId, taskId)
}

func (k *keys) taskQueueTaskHeartbeat(workspaceName, stubId, taskId string) string {
	return fmt.Sprintf(taskQueueTaskHeartbeat, workspaceName, stubId, taskId)
}

func (k *keys) taskQueueTaskRetries(workspaceName, stubId, taskId string) string {
	return fmt.Sprintf(taskQueueTaskRetries, workspaceName, stubId, taskId)
}

func (k *keys) taskQueueTaskDuration(workspaceName, stubId string) string {
	return fmt.Sprintf(taskQueueTaskDuration, workspaceName, stubId)
}

func (k *keys) taskQueueAverageTaskDuration(workspaceName, stubId string) string {
	return fmt.Sprintf(taskQueueAverageTaskDuration, workspaceName, stubId)
}

func (k *keys) taskQueueTaskRunningLock(workspaceName, stubId, containerId, taskId string) string {
	return fmt.Sprintf(taskQueueTaskRunningLock, workspaceName, stubId, containerId, taskId)
}

func (k *keys) taskQueueProcessingLock(workspaceName, stubId, containerId string) string {
	return fmt.Sprintf(taskQueueProcessingLock, workspaceName, stubId, containerId)
}

func (k *keys) taskQueueKeepWarmLock(workspaceName, stubId, containerId string) string {
	return fmt.Sprintf(taskQueueKeepWarmLock, workspaceName, stubId, containerId)
}
