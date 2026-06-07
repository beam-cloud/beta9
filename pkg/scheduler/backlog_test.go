package scheduler

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/tj/assert"
)

func NewRequestBacklogForTest(rdb *common.RedisClient) *RequestBacklog {
	return &RequestBacklog{rdb: rdb}
}

func TestRequestBacklogOrdering(t *testing.T) {
	s, err := miniredis.Run()
	assert.NotNil(t, s)
	assert.NoError(t, err)

	redisClient, err := common.NewRedisClient(types.RedisConfig{Addrs: []string{s.Addr()}, Mode: types.RedisModeSingle})
	assert.NotNil(t, redisClient)
	assert.NoError(t, err)

	rb := NewRequestBacklogForTest(redisClient)

	// Create some container requests with increasing timestamps
	req1 := &types.ContainerRequest{Timestamp: time.Unix(1, 0)}
	req2 := &types.ContainerRequest{Timestamp: time.Unix(2, 0)}
	req3 := &types.ContainerRequest{Timestamp: time.Unix(3, 0)}

	// Push them in out of order
	if err := rb.Push(req2); err != nil {
		t.Fatalf("Could not push request: %v", err)
	}
	if err := rb.Push(req1); err != nil {
		t.Fatalf("Could not push request: %v", err)
	}
	if err := rb.Push(req3); err != nil {
		t.Fatalf("Could not push request: %v", err)
	}

	// Pop them and verify the order based on timestamp
	var poppedReq *types.ContainerRequest

	poppedReq, err = rb.Pop()
	if err != nil {
		t.Fatalf("Could not pop request: %v", err)
	}
	if poppedReq.Timestamp.Unix() != req1.Timestamp.Unix() {
		t.Errorf("Expected timestamp %v, got %v", req1.Timestamp.Unix(), poppedReq.Timestamp.Unix())
	}

	poppedReq, err = rb.Pop()
	if err != nil {
		t.Fatalf("Could not pop request: %v", err)
	}
	if poppedReq.Timestamp.Unix() != req2.Timestamp.Unix() {
		t.Errorf("Expected timestamp %v, got %v", req2.Timestamp.Unix(), poppedReq.Timestamp.Unix())
	}

	poppedReq, err = rb.Pop()
	if err != nil {
		t.Fatalf("Could not pop request: %v", err)
	}
	if poppedReq.Timestamp.Unix() != req3.Timestamp.Unix() {
		t.Errorf("Expected timestamp %v, got %v", req3.Timestamp.Unix(), poppedReq.Timestamp.Unix())
	}
}

func TestRequestBacklogDelayedRequestsOnlyPopWhenReady(t *testing.T) {
	s, err := miniredis.Run()
	assert.NotNil(t, s)
	assert.NoError(t, err)

	redisClient, err := common.NewRedisClient(types.RedisConfig{Addrs: []string{s.Addr()}, Mode: types.RedisModeSingle})
	assert.NotNil(t, redisClient)
	assert.NoError(t, err)

	rb := NewRequestBacklogForTest(redisClient)
	request := &types.ContainerRequest{
		ContainerId: "delayed",
		Timestamp:   time.Now(),
	}

	assert.NoError(t, rb.PushAfter(request, 50*time.Millisecond))

	_, err = rb.PopN(1)
	assert.Error(t, err)

	time.Sleep(75 * time.Millisecond)

	poppedReq, err := rb.Pop()
	assert.NoError(t, err)
	assert.Equal(t, request.ContainerId, poppedReq.ContainerId)
}

func TestSchedulerPushBacklogSanitizesPrivatePoolRequests(t *testing.T) {
	s, err := miniredis.Run()
	assert.NotNil(t, s)
	assert.NoError(t, err)

	redisClient, err := common.NewRedisClient(types.RedisConfig{Addrs: []string{s.Addr()}, Mode: types.RedisModeSingle})
	assert.NotNil(t, redisClient)
	assert.NoError(t, err)

	manager := NewWorkerPoolManager(false)
	manager.SetPool("private-pool", types.WorkerPoolConfig{Mode: types.PoolModePrivate}, nil)

	storageID := uint(1)
	storageSecret := "storage-secret"
	signingKey := "workspace-signing-key"
	scheduler := &Scheduler{
		requestBacklog:    NewRequestBacklogForTest(redisClient),
		workerPoolManager: manager,
	}
	stubConfig, err := json.Marshal(types.StubConfigV1{
		Secrets: []types.Secret{{Name: "SECRET"}},
	})
	assert.NoError(t, err)

	request := &types.ContainerRequest{
		ContainerId:  "container-1",
		PoolSelector: "private-pool",
		Env:          []string{"BETA9_TOKEN=user-token", "SECRET=value", "SAFE=value"},
		Stub: types.StubWithRelated{
			Stub: types.Stub{Config: string(stubConfig)},
		},
		Workspace: types.Workspace{
			SigningKey: &signingKey,
			Storage: &types.WorkspaceStorage{
				Id:        &storageID,
				SecretKey: &storageSecret,
			},
		},
		Mounts: []types.Mount{{
			MountPointConfig: &types.MountPointConfig{
				AccessKey: "mount-access",
				SecretKey: "mount-secret",
			},
		}},
	}

	assert.NoError(t, scheduler.pushBacklog(request, 0))

	poppedReq, err := scheduler.requestBacklog.Pop()
	assert.NoError(t, err)
	assert.Equal(t, []string{"SAFE=value"}, poppedReq.Env)
	assert.Nil(t, poppedReq.Workspace.SigningKey)
	assert.NotNil(t, poppedReq.Workspace.Storage)
	assert.Nil(t, poppedReq.Workspace.Storage.SecretKey)
	assert.Empty(t, poppedReq.Mounts[0].MountPointConfig.AccessKey)
	assert.Empty(t, poppedReq.Mounts[0].MountPointConfig.SecretKey)
}
