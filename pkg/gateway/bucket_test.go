package gateway

import (
	"context"
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2"
	common "github.com/beam-cloud/beam/pkg/common"
	"github.com/beam-cloud/beam/pkg/repository"
	"github.com/beam-cloud/beam/pkg/types"
	pb "github.com/beam-cloud/beam/proto"
	"github.com/tj/assert"
	"google.golang.org/grpc"
)

type WorkBusForTest struct {
	Client pb.SchedulerClient
	Conn   *grpc.ClientConn
}

func NewRequestBucketForTest() (*DeploymentRequestBucket, error) {
	s, err := miniredis.Run()
	if err != nil {
		return nil, err
	}

	redisClient, err := common.NewRedisClient(common.WithAddress(s.Addr()))
	if err != nil {
		return nil, err
	}

	stateStore := common.Store(redisClient)

	rb := &DeploymentRequestBucket{}

	appConfig := types.BeamAppConfig{
		Triggers: []types.Trigger{
			{
				TriggerType: common.TriggerTypeWebhook,
			},
		},
	}
	config := DeploymentRequestBucketConfig{
		RequestBucketConfig: RequestBucketConfig{
			AppId:       "app_id",
			BucketId:    "app_deployment_id",
			AppConfig:   appConfig,
			Status:      types.DeploymentStatusReady,
			IdentityId:  "identity_id",
			TriggerType: "webhook",
			ImageTag:    "image_tag",
		},
		Version:         1,
		ScaleDownDelay:  float32(30),
		MaxPendingTasks: 10,
		Workers:         2,
	}

	rb.Name = common.Names.RequestBucketName(config.AppId, config.Version)
	rb.AppId = config.AppId
	rb.BucketId = config.RequestBucketConfig.BucketId
	rb.IdentityId = config.IdentityId
	rb.Status = config.RequestBucketConfig.Status
	rb.AppConfig = config.AppConfig
	rb.Version = config.Version
	rb.TriggerType = config.TriggerType
	rb.ImageTag = config.ImageTag
	rb.AutoScaler = NewAutoscaler(rb, stateStore)
	rb.ScaleEventChan = make(chan int, 1)
	rb.ContainerEventChan = make(chan types.ContainerEvent, 1)
	rb.Containers = make(map[string]bool)
	rb.ScaleDownDelay = &config.ScaleDownDelay
	rb.maxPendingTasks = config.MaxPendingTasks
	rb.workers = config.Workers
	rb.unloadBucketChan = make(chan string)

	// Clients
	rb.workBus = &WorkBus{}
	// rb.beamRepo = beamRepo
	rb.bucketRepo = repository.NewRequestBucketRedisRepositoryForTest(rb.Name, rb.IdentityId, redisClient)
	// rb.queueClient = queueClient

	bucketCtx, closeBucketFunc := context.WithCancel(context.Background())
	rb.bucketCtx = bucketCtx
	rb.closeFunc = closeBucketFunc
	rb.bucketLock = common.NewRedisLock(redisClient)

	go rb.Monitor() // Monitor request bucket
	return rb, nil
}

func TestNewRequestBucket(t *testing.T) {
	rb, err := NewRequestBucketForTest()
	assert.Nil(t, err)
	assert.NotNil(t, rb)
}

func TestGetContainerEnv(t *testing.T) {
	rb, err := NewRequestBucketForTest()
	assert.Nil(t, err)
	assert.NotNil(t, rb)

	expected := []string{
		"PYTHONUNBUFFERED=1",
		"APP_ID=app_id",
		"APP_QUEUE_NAME=app_id-0001",
		"APP_BUCKET_NAME=app_id-0001",
		`APP_CONFIG={"app_spec_version":"","sdk_version":null,"name":"","runtime":{"cpu":"","gpu":"","memory":"","image":{"commands":null,"python_version":"","python_packages":null,"base_image":null,"base_image_creds":null}},"mounts":null,"triggers":[{"outputs":null,"trigger_type":"webhook","handler":"","method":null,"path":null,"runtime":null,"autoscaler":null,"autoscaling":null,"when":null,"loader":null,"keep_warm_seconds":null,"max_pending_tasks":null,"callback_url":null,"task_policy":null,"workers":null,"authorized":null}],"run":null}`,
		"SCALE_DOWN_DELAY=30.000000",
		"WORKERS=2",
	}

	env := rb.containerEnv()
	for i, v := range env {
		if v != expected[i] {
			t.Fatalf("\nExpected: %s\n Got: %s", expected[i], v)
		}
	}
}

func TestGenerateContainerId(t *testing.T) {
	rb, err := NewRequestBucketForTest()
	assert.Nil(t, err)
	assert.NotNil(t, rb)

	id := rb.generateContainerId()
	expectedPrefix := types.DeploymentContainerPrefix + rb.Name + "-"
	if !strings.HasPrefix(id, expectedPrefix) {
		t.Fatalf("Expected ID to start with prefix: %s, got: %s", expectedPrefix, id)
	}

	suffix := strings.TrimPrefix(id, expectedPrefix)
	if len(suffix) != 8 {
		t.Fatalf("Expected suffix to be a string of length 8, got: %s", suffix)
	}
}
