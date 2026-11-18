package function

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"testing"

	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/scheduler"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

type spotFunctionBackend struct{ repository.BackendRepository }

func (spotFunctionBackend) RetrieveActiveToken(context.Context, uint, string) (*types.Token, error) {
	return &types.Token{Key: "test-token"}, nil
}

func TestFunctionRequestAppliesServerlessSpotPolicy(t *testing.T) {
	for _, test := range []struct {
		name string
		gpu  types.GpuType
		pool *types.PoolConfig
		want bool
	}{
		{name: "ordinary GPU", gpu: types.GPU_H200, want: true},
		{name: "CPU"},
		{name: "explicit pool", gpu: types.GPU_H200, pool: &types.PoolConfig{Name: "selected"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			rdb, err := repository.NewRedisClientForTest()
			require.NoError(t, err)
			t.Cleanup(func() { _ = rdb.Close() })
			workspaceRepo := repository.NewWorkspaceRedisRepository(rdb, "")
			require.NoError(t, workspaceRepo.SetConcurrencyLimitByWorkspaceId("workspace", &types.ConcurrencyLimit{CPUMillicoreLimit: 100_000, GPULimit: 10}))
			s, err := scheduler.NewScheduler(ctx, types.AppConfig{}, rdb, nil, nil, workspaceRepo, scheduler.WithComputeEvents(func(types.EventComputeSchema) {}))
			require.NoError(t, err)
			key := "sk_" + base64.StdEncoding.EncodeToString(make([]byte, 32))
			id := uint(1)
			workspace := types.Workspace{ExternalId: "workspace", Name: "workspace", SigningKey: &key, Storage: &types.WorkspaceStorage{Id: &id}}
			stubConfig, err := json.Marshal(types.StubConfigV1{Runtime: types.Runtime{Gpu: test.gpu, Cpu: 500, Memory: 512}, Pool: test.pool})
			require.NoError(t, err)
			stub := &types.StubWithRelated{Stub: types.Stub{ExternalId: "stub", Type: types.StubType(types.StubTypeFunction), Config: string(stubConfig)}, Workspace: workspace, App: &types.App{}}
			task := &FunctionTask{containerId: "function-container", msg: &types.TaskMessage{TaskId: "task"}, fs: &ContainerFunctionService{rdb: rdb, scheduler: s, backendRepo: spotFunctionBackend{}}}
			require.NoError(t, task.run(ctx, stub, &types.Task{ExternalId: "task"}))
			requests, err := scheduler.NewRequestBacklog(rdb).PopN(1)
			require.NoError(t, err)
			require.Len(t, requests, 1)
			require.Equal(t, test.want, requests[0].Preemptable)
			require.Equal(t, "function-container", requests[0].ContainerId)
		})
	}
}
