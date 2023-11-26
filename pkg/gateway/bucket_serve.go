package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	common "github.com/beam-cloud/beam/pkg/common"
	"github.com/beam-cloud/beam/pkg/repository"
	"github.com/beam-cloud/beam/pkg/types"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

func NewServeRequestBucket(config RequestBucketConfig, gateway *Gateway) (types.RequestBucket, error) {
	rb := &ServeRequestBucket{}

	// Configuration
	rb.Name = common.Names.RequestBucketNameId(config.AppId, config.BucketId)
	rb.AppId = config.AppId
	rb.BucketId = config.BucketId
	rb.AppConfig = config.AppConfig
	rb.Status = config.Status
	rb.IdentityId = config.IdentityId
	rb.TriggerType = config.TriggerType
	rb.ImageTag = config.ImageTag
	rb.ContainerEventChan = make(chan types.ContainerEvent, 1)
	rb.Containers = make(map[string]bool)
	rb.unloadBucketChan = gateway.unloadBucketChan
	rb.ScaleDownDelay = nil

	bucketRepo := repository.NewRequestBucketRedisRepository(rb.Name, rb.IdentityId, gateway.redisClient, types.RequestBucketTypeServe)

	// Clients
	rb.rdb = gateway.redisClient
	rb.scheduler = gateway.Scheduler
	rb.beamRepo = gateway.BeamRepo
	rb.bucketRepo = bucketRepo
	rb.taskRepo = repository.NewTaskRedisRepository(gateway.redisClient)
	rb.queueClient = gateway.QueueClient

	bucketLock := common.NewRedisLock(gateway.redisClient)

	bucketCtx, closeBucketFunc := context.WithCancel(context.Background())
	rb.bucketCtx = bucketCtx
	rb.closeFunc = closeBucketFunc
	rb.bucketLock = bucketLock

	if config.AppConfig.Triggers[0].TaskPolicy != nil {
		rb.TaskPolicy = config.AppConfig.Triggers[0].TaskPolicy
	} else {
		rb.TaskPolicy = &types.TaskPolicy{}
		*rb.TaskPolicy = common.DefaultTaskPolicy
	}

	// Override retries for serves to 0 (no retries)
	rb.TaskPolicy.MaxRetries = 0

	taskPolicyRaw, err := json.Marshal(rb.TaskPolicy)
	if err != nil {
		return nil, err
	}
	rb.TaskPolicyRaw = taskPolicyRaw

	go rb.Monitor()
	return rb, nil
}

func (rb *ServeRequestBucket) ForwardRequest(ctx *gin.Context) error {
	taskId := ""

	// Only create tasks for non-ASGI deployments
	if rb.TriggerType != common.TriggerTypeASGI {
		taskId = uuid.New().String()
		_, err := rb.beamRepo.CreateServeTask(rb.BucketId, taskId, rb.TaskPolicyRaw)
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

func (rb *ServeRequestBucket) EndServe() {
	err := rb.beamRepo.EndServe(rb.AppId, rb.BucketId, rb.IdentityId)
	if err != nil {
		log.Printf("<%s> Unable to end serve: %v", rb.BucketId, err)
	}
}

func (rb *ServeRequestBucket) Monitor() {
	containerId := fmt.Sprintf("serve-%s-%s", rb.AppId, rb.BucketId)
	missedHealthChecks := 0

	log.Printf("<%s> Starting health check for serve container: %s \n", rb.BucketId, containerId)

	for {
		hostname, err := rb.rdb.Get(context.TODO(), common.RedisKeys.SchedulerContainerHost(containerId)).Result()
		if err != nil {
			rb.EndServe()
			rb.unloadBucketChan <- rb.Name
			break
		}

		// Make http request to container
		url := fmt.Sprintf("http://%s/healthz", hostname)
		resp, err := http.Get(url)

		if err != nil || resp.StatusCode != http.StatusOK {
			missedHealthChecks += 1
		} else if err == nil && resp.StatusCode == http.StatusOK {
			missedHealthChecks = 0
		}

		if missedHealthChecks > common.BucketMissedHealthChecksThreshold {
			rb.EndServe()
			rb.unloadBucketChan <- rb.Name
			break
		}

		time.Sleep(time.Duration(common.BucketHealthCheckIntervalS) * time.Second)
	}
}
