package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"time"

	common "github.com/beam-cloud/beam/pkg/common"
	"github.com/beam-cloud/beam/pkg/repository"
	"github.com/beam-cloud/beam/pkg/types"
	pb "github.com/beam-cloud/beam/proto"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

const IgnoreScalingEventInterval = 15 * time.Second

func NewDeploymentRequestBucket(config DeploymentRequestBucketConfig, activator *Activator) (types.RequestBucket, error) {
	rb := &DeploymentRequestBucket{}

	// Configuration
	rb.Name = common.Names.RequestBucketName(config.AppId, config.Version)
	rb.AppId = config.AppId
	rb.BucketId = config.BucketId
	rb.AppConfig = config.AppConfig
	rb.AutoscalingConfig = config.AutoscalingConfig
	rb.AutoscalerConfig = config.AutoscalerConfig
	rb.Status = config.Status
	rb.IdentityId = config.IdentityId
	rb.Version = config.Version
	rb.TriggerType = config.TriggerType
	rb.ImageTag = config.ImageTag
	rb.AutoScaler = NewAutoscaler(rb, activator.stateStore)
	rb.ScaleEventChan = make(chan int, 1)
	rb.ContainerEventChan = make(chan types.ContainerEvent, 1)
	rb.Containers = make(map[string]bool)
	rb.ScaleDownDelay = &config.ScaleDownDelay
	rb.maxPendingTasks = config.MaxPendingTasks
	rb.workers = config.Workers
	rb.unloadBucketChan = activator.unloadBucketChan

	if config.AppConfig.Triggers[0].TaskPolicy != nil {
		rb.TaskPolicy = config.AppConfig.Triggers[0].TaskPolicy
	} else {
		rb.TaskPolicy = &types.TaskPolicy{}
		*rb.TaskPolicy = common.DefaultTaskPolicy
	}

	taskPolicyRaw, err := json.Marshal(rb.TaskPolicy)
	if err != nil {
		return nil, err
	}
	rb.TaskPolicyRaw = taskPolicyRaw

	// Clients
	rb.workBus = activator.WorkBus
	rb.beamRepo = activator.BeamRepo
	rb.bucketRepo = repository.NewRequestBucketRedisRepository(rb.Name, rb.IdentityId, activator.redisClient, types.RequestBucketTypeDeployment)
	rb.taskRepo = repository.NewTaskRedisRepository(activator.redisClient)
	rb.queueClient = activator.QueueClient

	bucketLock := common.NewRedisLock(activator.redisClient)

	bucketCtx, closeBucketFunc := context.WithCancel(context.Background())
	rb.bucketCtx = bucketCtx
	rb.closeFunc = closeBucketFunc
	rb.bucketLock = bucketLock

	go rb.Monitor() // Monitor request bucket
	return rb, nil
}

func (rb *DeploymentRequestBucket) ForwardRequest(ctx *gin.Context) error {
	if rb.AutoScaler != nil && rb.AutoScaler.MostRecentSample != nil && rb.AutoScaler.MostRecentSample.QueueLength >= int64(rb.maxPendingTasks) {
		ctx.JSON(http.StatusTooManyRequests, types.QueueResponse{
			Error: "exceeded_max_pending_tasks",
		})

		return errors.New("exceeded max pending task limit")
	}

	if rb.Status == types.DeploymentStatusError || rb.Status == types.DeploymentStatusStopped {
		ctx.JSON(http.StatusBadRequest, types.QueueResponse{
			Error: "invalid_deployment_status",
		})

		return errors.New("invalid deployment status")
	}

	taskId := ""

	// Only create tasks for non-ASGI deployments
	if rb.TriggerType != common.TriggerTypeASGI {
		taskId = uuid.New().String()
		_, err := rb.beamRepo.CreateDeploymentTask(rb.BucketId, taskId, rb.TaskPolicyRaw)
		if err != nil {
			ctx.JSON(http.StatusInternalServerError, types.QueueResponse{
				TaskId: taskId,
				Error:  "unable_to_create_task",
			})

			return err
		}
	}

	return rb.HandleRequest(ctx, taskId)
}

func (rb *DeploymentRequestBucket) Monitor() {
	go rb.AutoScaler.Start() // Start the autoscaler
	ignoreScalingEventWindow := time.Now().Add(-IgnoreScalingEventInterval)

	for {
		select {

		case <-rb.bucketCtx.Done():
			return

		case containerEvent := <-rb.ContainerEventChan:
			initialContainerCount := len(rb.Containers)

			_, exists := rb.Containers[containerEvent.ContainerId]
			switch {
			case !exists && containerEvent.Change == 1: // Container created and doesn't exist in map
				rb.Containers[containerEvent.ContainerId] = true
			case exists && containerEvent.Change == -1: // Container removed and exists in map
				delete(rb.Containers, containerEvent.ContainerId)
			}

			if initialContainerCount != len(rb.Containers) {
				log.Printf("<%s> Scaled from %d->%d", rb.Name, initialContainerCount, len(rb.Containers))
			}

		case desiredContainers := <-rb.ScaleEventChan:
			// If the event is in the end of the ignore scaling event window, ignore it
			if time.Now().Before(ignoreScalingEventWindow) {
				continue
			}

			err := rb.handleScalingEvent(desiredContainers)
			if err == nil {
				continue
			}

			if errors.Is(err, types.ErrBucketNotInUse) {
				rb.unloadBucketChan <- rb.Name
				continue
			}

			if errors.Is(err, &types.ThrottledByConcurrencyLimitError{}) {
				if time.Now().After(ignoreScalingEventWindow) {
					log.Printf("<%s> Ignoring scaling event due to throttling", rb.Name)
					ignoreScalingEventWindow = time.Now().Add(IgnoreScalingEventInterval)
				}
			}
		}
	}
}

func (rb *DeploymentRequestBucket) handleScalingEvent(desiredContainers int) error {
	err := rb.bucketLock.Acquire(rb.bucketCtx, common.RedisKeys.ActivatorBucketLock(rb.Name), common.RedisLockOptions{TtlS: 10, Retries: 0})
	if err != nil {
		return err
	}
	defer rb.bucketLock.Release(common.RedisKeys.ActivatorBucketLock(rb.Name))

	state, err := rb.bucketRepo.GetRequestBucketState()
	if err != nil {
		return err
	}

	if state.FailedContainers >= types.FailedContainerThreshold {
		log.Printf("<%s> Reached failed container threshold, scaling to zero.", rb.Name)
		desiredContainers = 0
	}

	if rb.Status == types.DeploymentStatusStopped {
		desiredContainers = 0
	}

	noContainersRunning := (state.PendingContainers == 0) && (state.RunningContainers == 0) && (state.StoppingContainers == 0)
	if desiredContainers == 0 && noContainersRunning {
		return types.ErrBucketNotInUse
	}

	containerDelta := desiredContainers - (state.RunningContainers + state.PendingContainers)
	if containerDelta > 0 {
		err = rb.startContainers(containerDelta)
	} else if containerDelta < 0 {
		err = rb.stopContainers(-containerDelta)
	}

	return err
}

func (rb *DeploymentRequestBucket) getContainerEntryPoint() []string {
	switch rb.TriggerType {
	case common.TriggerTypeCronJob, common.TriggerTypeRestApi, common.TriggerTypeWebhook:
		return []string{rb.AppConfig.Runtime.Image.PythonVersion, "-m", "runner.gateway.deployment.function"}
	case common.TriggerTypeASGI:
		return []string{rb.AppConfig.Runtime.Image.PythonVersion, "-m", "runner.gateway.deployment.asgi"}
	default:
		return []string{}
	}
}

func (rb *DeploymentRequestBucket) startContainers(containersToRun int) error {
	for i := 0; i < containersToRun; i++ {
		runRequest := &pb.RunContainerRequest{
			ContainerId: rb.generateContainerId(),
			// Mode:        worker.WorkerModeAppDeployment,
			EntryPoint: rb.getContainerEntryPoint(),
			Env:        rb.containerEnv(),
			Cpu:        rb.AppConfig.Runtime.Cpu,
			Memory:     rb.AppConfig.Runtime.Memory,
			Gpu:        string(rb.AppConfig.Runtime.Gpu),
			ImageTag:   rb.ImageTag,
			IdentityId: rb.IdentityId,
		}

		response, err := rb.workBus.Client.RunContainer(rb.bucketCtx, runRequest)
		if err != nil {
			return err
		}

		if response.Success {
			continue
		}

		if response.Error == (&types.ThrottledByConcurrencyLimitError{}).Error() {
			log.Printf("<%s> Throttled by concurrency limit", rb.Name)
			return &types.ThrottledByConcurrencyLimitError{}
		}

		log.Printf("<%s> Unable to run container: %v", rb.Name, response.Error)
		continue
	}

	return nil
}

func (rb *DeploymentRequestBucket) containerEnv() []string {
	return []string{
		"PYTHONUNBUFFERED=1",
		fmt.Sprintf("APP_ID=%s", rb.AppId),
		fmt.Sprintf("APP_QUEUE_NAME=%s", rb.Name),
		fmt.Sprintf("APP_BUCKET_NAME=%s", rb.Name),
		fmt.Sprintf("APP_CONFIG=%s", rb.AppConfig.AsString()),
		fmt.Sprintf("SCALE_DOWN_DELAY=%f", *rb.ScaleDownDelay),
		fmt.Sprintf("WORKERS=%d", rb.workers),
	}

}

func (rb *DeploymentRequestBucket) generateContainerId() string {
	return fmt.Sprintf("%s%s-%s", types.DeploymentContainerPrefix, rb.Name, uuid.New().String()[:8])
}
