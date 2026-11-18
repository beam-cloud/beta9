package taskqueue

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"testing"

	abstractions "github.com/beam-cloud/beta9/pkg/abstractions/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/scheduler"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestTaskQueueRequestAppliesServerlessSpotPolicy(t *testing.T) {
	for _, test := range []struct {
		name string
		gpu  types.GpuType
		pool *types.PoolConfig
		want bool
	}{
		{name: "ordinary GPU", gpu: types.GPU_B300, want: true},
		{name: "CPU"},
		{name: "explicit pool", gpu: types.GPU_B300, pool: &types.PoolConfig{Name: "selected"}},
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
			workspace := &types.Workspace{ExternalId: "workspace", Name: "workspace", SigningKey: &key, Storage: &types.WorkspaceStorage{Id: &id}}
			stubConfig := &types.StubConfigV1{Runtime: types.Runtime{Gpu: test.gpu, Cpu: 500, Memory: 512}, Pool: test.pool}
			encoded, err := json.Marshal(stubConfig)
			require.NoError(t, err)
			stub := &types.StubWithRelated{Stub: types.Stub{ExternalId: "stub", Type: types.StubType(types.StubTypeTaskQueue), Config: string(encoded)}, App: &types.App{}}
			instance := &taskQueueInstance{AutoscaledInstance: &abstractions.AutoscaledInstance{Ctx: ctx, Rdb: rdb, Workspace: workspace, Stub: stub, StubConfig: stubConfig, Token: &types.Token{Key: "test-token"}, Scheduler: s}}
			require.NoError(t, instance.startContainers(1))
			requests, err := scheduler.NewRequestBacklog(rdb).PopN(1)
			require.NoError(t, err)
			require.Len(t, requests, 1)
			require.Equal(t, test.want, requests[0].Preemptable)
			require.Equal(t, test.pool != nil, requests[0].PoolSelector != "")
		})
	}
}
