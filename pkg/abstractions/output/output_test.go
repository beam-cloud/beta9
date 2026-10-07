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

func TestOutputMissingTask(t *testing.T) {
	service := &OutputRedisService{backendRepo: missingTaskBackendRepo{}}
	ctx := auth.ContextWithAuthInfo(context.Background(), &auth.AuthInfo{
		Workspace: &types.Workspace{Name: "workspace"},
	})

	t.Run("stat", func(t *testing.T) {
		response, err := service.OutputStat(ctx, &pb.OutputStatRequest{Id: "test"})
		require.NoError(t, err)
		require.False(t, response.Ok)
		require.Equal(t, "Unable stat output", response.ErrMsg)
		require.Nil(t, response.Stat)
	})

	t.Run("public URL", func(t *testing.T) {
		response, err := service.OutputPublicURL(ctx, &pb.OutputPublicURLRequest{Id: "test"})
		require.NoError(t, err)
		require.False(t, response.Ok)
		require.Equal(t, "Unable to get public URL", response.ErrMsg)
		require.Empty(t, response.PublicUrl)
	})

	t.Run("save", func(t *testing.T) {
		contentCh := make(chan OutputSaveContent, 1)
		contentCh <- OutputSaveContent{Filename: "test.txt", Content: []byte("test")}
		close(contentCh)

		id, err := service.writeToFile(ctx, contentCh, "workspace")
		require.EqualError(t, err, "task not found")
		require.Empty(t, id)
	})
}
