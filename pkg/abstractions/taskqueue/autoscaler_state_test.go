package taskqueue

import (
	"context"
	"errors"
	"testing"

	abstractions "github.com/beam-cloud/beta9/pkg/abstractions/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type queuedTaskRepo struct {
	repository.TaskRepository
}

func (queuedTaskRepo) TasksInFlight(ctx context.Context, workspaceName, stubId string) (int, error) {
	return 3, nil
}

func (queuedTaskRepo) TasksClaimed(ctx context.Context, workspaceName, stubId string) (int, error) {
	return 1, nil
}

type unavailableContainerRepo struct {
	repository.ContainerRepository
}

func (unavailableContainerRepo) GetActiveContainersByStubId(stubId string) ([]types.ContainerState, error) {
	return nil, errors.New("redis: connection pool timeout")
}

func TestTaskQueueSampleFuncReturnsErrorWhenStateIsUnavailable(t *testing.T) {
	rdb, err := repository.NewRedisClientForTest()
	require.NoError(t, err)

	instance := &taskQueueInstance{
		AutoscaledInstance: &abstractions.AutoscaledInstance{
			Ctx:           context.Background(),
			Workspace:     &types.Workspace{Name: "ws"},
			Stub:          &types.StubWithRelated{Stub: types.Stub{ExternalId: "stub"}},
			TaskRepo:      queuedTaskRepo{},
			ContainerRepo: unavailableContainerRepo{},
		},
		client: newRedisTaskQueueClient(rdb, queuedTaskRepo{}),
	}

	var sample *taskQueueAutoscalerSample
	assert.NotPanics(t, func() {
		sample, err = taskQueueAutoscalerSampleFunc(instance)
	})
	assert.Error(t, err)
	assert.Nil(t, sample)
}
