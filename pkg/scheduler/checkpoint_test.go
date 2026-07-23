package scheduler

import (
	"context"
	"testing"

	repo "github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/tj/assert"
)

type checkpointBackendRepoForTest struct {
	repo.BackendRepository
	latest *types.Checkpoint
	calls  int
}

func (r *checkpointBackendRepoForTest) GetLatestCheckpointByStubId(context.Context, string) (*types.Checkpoint, error) {
	r.calls++
	return r.latest, nil
}

func TestCheckpointReadyWaitsWhileContainerStopping(t *testing.T) {
	scheduler, err := NewSchedulerForTest()
	assert.NoError(t, err)

	const (
		stubID    = "checkpoint-stub"
		sourceID  = "source-container"
		requestID = "replacement-container"
		siblingID = "sibling-replacement"
	)
	assert.NoError(t, scheduler.containerRepo.SetContainerState(sourceID, &types.ContainerState{
		ContainerId: sourceID,
		StubId:      stubID,
		Status:      types.ContainerStatusStopping,
	}))
	assert.NoError(t, scheduler.containerRepo.SetContainerState(requestID, &types.ContainerState{
		ContainerId: requestID,
		StubId:      stubID,
		Status:      types.ContainerStatusPending,
	}))
	assert.NoError(t, scheduler.containerRepo.SetContainerState(siblingID, &types.ContainerState{
		ContainerId: siblingID,
		StubId:      stubID,
		Status:      types.ContainerStatusPending,
	}))

	backend := &checkpointBackendRepoForTest{BackendRepository: scheduler.backendRepo}
	scheduler.backendRepo = backend
	request := &types.ContainerRequest{
		ContainerId:       requestID,
		StubId:            stubID,
		CheckpointEnabled: true,
	}

	assert.False(t, scheduler.checkpointReady(request))
	assert.Nil(t, request.Checkpoint)

	backend.latest = &types.Checkpoint{
		CheckpointId:      "older-checkpoint",
		SourceContainerId: "older-container",
		Status:            string(types.CheckpointStatusAvailable),
	}
	assert.True(t, scheduler.checkpointReady(request))
	assert.NotNil(t, request.Checkpoint)
	if request.Checkpoint != nil {
		assert.Equal(t, "older-checkpoint", request.Checkpoint.CheckpointId)
	}
}

func TestCheckpointReadySkipsRequestWithCheckpoint(t *testing.T) {
	scheduler, err := NewSchedulerForTest()
	assert.NoError(t, err)
	backend := &checkpointBackendRepoForTest{BackendRepository: scheduler.backendRepo}
	scheduler.backendRepo = backend

	request := &types.ContainerRequest{
		StubId:            "checkpoint-stub",
		CheckpointEnabled: true,
		Checkpoint:        &types.Checkpoint{CheckpointId: "explicit-checkpoint"},
	}
	assert.True(t, scheduler.checkpointReady(request))
	assert.Equal(t, 0, backend.calls)
}

func TestCheckpointReadySkipsDisabledRequest(t *testing.T) {
	scheduler, err := NewSchedulerForTest()
	assert.NoError(t, err)
	backend := &checkpointBackendRepoForTest{BackendRepository: scheduler.backendRepo}
	scheduler.backendRepo = backend
	scheduler.containerRepo = nil

	request := &types.ContainerRequest{
		StubId:            "checkpoint-stub",
		CheckpointEnabled: false,
	}
	assert.True(t, scheduler.checkpointReady(request))
	assert.Equal(t, 0, backend.calls)
}

func TestPrepareWorkerRequestRefreshesMissingCheckpoint(t *testing.T) {
	scheduler, err := NewSchedulerForTest()
	assert.NoError(t, err)
	backend := &checkpointBackendRepoForTest{
		BackendRepository: scheduler.backendRepo,
		latest: &types.Checkpoint{
			CheckpointId: "checkpoint-1",
			Status:       string(types.CheckpointStatusAvailable),
		},
	}
	scheduler.backendRepo = backend

	request := &types.ContainerRequest{StubId: "checkpoint-stub", CheckpointEnabled: true}
	workerRequest := scheduler.prepareWorkerRequest(&types.Worker{Gpu: "RTX5090"}, request)

	assert.Nil(t, request.Checkpoint)
	assert.NotNil(t, workerRequest.Checkpoint)
	if workerRequest.Checkpoint != nil {
		assert.Equal(t, "checkpoint-1", workerRequest.Checkpoint.CheckpointId)
	}
}
