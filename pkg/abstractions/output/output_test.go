package output

import (
	"context"
	"testing"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
)

type missingTaskBackendRepo struct {
	repository.BackendRepository
}

func (missingTaskBackendRepo) GetTaskWithRelated(context.Context, string) (*types.TaskWithRelated, error) {
	return nil, nil
}

func TestOutputStatMissingTask(t *testing.T) {
	service := &OutputRedisService{backendRepo: missingTaskBackendRepo{}}
	ctx := auth.ContextWithAuthInfo(context.Background(), &auth.AuthInfo{
		Workspace: &types.Workspace{Name: "workspace"},
	})

	response, err := service.OutputStat(ctx, &pb.OutputStatRequest{Id: "test"})
	require.NoError(t, err)
	require.False(t, response.Ok)
	require.Equal(t, "Unable stat output", response.ErrMsg)
	require.Nil(t, response.Stat)
}
