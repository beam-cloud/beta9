package endpoint

import (
	"context"
	"errors"
	"testing"

	abstractions "github.com/beam-cloud/beta9/pkg/abstractions/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/assert"
)

type inFlightTaskRepo struct {
	repository.TaskRepository
}

func (inFlightTaskRepo) TasksInFlight(ctx context.Context, workspaceName, stubId string) (int, error) {
	return 3, nil
}

type unavailableContainerRepo struct {
	repository.ContainerRepository
}

func (unavailableContainerRepo) GetActiveContainersByStubId(stubId string) ([]types.ContainerState, error) {
	return nil, errors.New("redis: connection pool timeout")
}

func TestEndpointSampleFuncReturnsErrorWhenStateIsUnavailable(t *testing.T) {
	instance := &endpointInstance{
		AutoscaledInstance: &abstractions.AutoscaledInstance{
			Ctx:           context.Background(),
			Workspace:     &types.Workspace{Name: "ws"},
			Stub:          &types.StubWithRelated{Stub: types.Stub{ExternalId: "stub"}},
			TaskRepo:      inFlightTaskRepo{},
			ContainerRepo: unavailableContainerRepo{},
		},
	}

	var sample *endpointAutoscalerSample
	var err error
	assert.NotPanics(t, func() {
		sample, err = endpointSampleFunc(instance)
	})
	assert.Error(t, err)
	assert.Nil(t, sample)
}
