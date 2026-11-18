package pod

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"testing"

	"github.com/alicebob/miniredis/v2"
	abstractions "github.com/beam-cloud/beta9/pkg/abstractions/common"
	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/scheduler"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

type spotRequestPodBackend struct{ repository.BackendRepository }

func (spotRequestPodBackend) GetConcurrencyLimitByWorkspaceId(context.Context, string) (*types.ConcurrencyLimit, error) {
	return &types.ConcurrencyLimit{GPULimit: 100, CPUMillicoreLimit: 100_000}, nil
}

func (spotRequestPodBackend) GetAdminWorkspace(context.Context) (*types.Workspace, error) {
	return &types.Workspace{ExternalId: "admin"}, nil
}

type spotRequestPodEvents struct{ repository.EventRepository }

func (spotRequestPodEvents) PushRunStubEvent(string, *types.Stub, types.EventActor) {}

// Capture requests after the real Pod builders and Scheduler.Run have placed
// them on the real Redis backlog. This catches a missing policy assignment at
// either call site even when the policy helper's own tests still pass.
func TestPodBuildersCarryServerlessSpotEligibilityToBacklog(t *testing.T) {
	for _, builder := range []string{"one-off", "autoscaled"} {
		for _, tc := range []struct {
			name string
			gpu  types.GpuType
			pool string
			want bool
		}{
			{name: "ordinary GPU", gpu: types.GPU_H100, want: true},
			{name: "CPU", want: false},
			{name: "explicit GPU pool", gpu: types.GPU_H100, pool: "dedicated", want: false},
		} {
			t.Run(builder+"/"+tc.name, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				mini := miniredis.RunT(t)
				rdb, err := common.NewRedisClient(types.RedisConfig{Addrs: []string{mini.Addr()}, Mode: types.RedisModeSingle})
				require.NoError(t, err)
				t.Cleanup(func() { _ = rdb.Close() })
				backend := spotRequestPodBackend{}
				sched, err := scheduler.NewScheduler(ctx, types.AppConfig{}, rdb, nil, backend,
					repository.NewWorkspaceRedisRepositoryForTest(rdb), scheduler.WithComputeEvents(func(types.EventComputeSchema) {}))
				require.NoError(t, err)
				signingKey := "sk_" + base64.StdEncoding.EncodeToString(make([]byte, 32))
				storageID := uint(1)
				workspace := &types.Workspace{ExternalId: "workspace", Name: "workspace", SigningKey: &signingKey,
					Storage: &types.WorkspaceStorage{Id: &storageID}}
				config := &types.StubConfigV1{Runtime: types.Runtime{Cpu: 500, Memory: 512, Gpu: tc.gpu}}
				if tc.pool != "" {
					config.Pool = &types.PoolConfig{Name: tc.pool, Selector: tc.pool}
				}
				encoded, err := json.Marshal(config)
				require.NoError(t, err)
				stub := &types.StubWithRelated{Stub: types.Stub{ExternalId: "stub", Type: types.StubType(types.StubTypePodRun), Config: string(encoded)},
					Workspace: *workspace, App: &types.App{ExternalId: "app"}, Object: types.Object{ExternalId: "object"}}
				containerRepo := repository.NewContainerRedisRepositoryForTest(rdb)
				if builder == "one-off" {
					service := &GenericPodService{ctx: ctx, backendRepo: backend, containerRepo: containerRepo,
						scheduler: sched, eventRepo: spotRequestPodEvents{}}
					_, err = service.run(ctx, &auth.AuthInfo{Workspace: workspace, Token: &types.Token{Key: "unit-test"}}, stub, runOptions{})
					require.NoError(t, err)
				} else {
					instance := &podInstance{AutoscaledInstance: &abstractions.AutoscaledInstance{Ctx: ctx, Workspace: workspace,
						Stub: stub, StubConfig: config, Token: &types.Token{Key: "unit-test"}, Scheduler: sched,
						Rdb: rdb, ContainerRepo: containerRepo, BackendRepo: backend}}
					require.NoError(t, instance.startContainers(1))
				}
				requests, err := scheduler.NewRequestBacklog(rdb).PopN(1)
				require.NoError(t, err)
				require.Len(t, requests, 1)
				request := requests[0]
				require.Equal(t, tc.want, request.Preemptable)
				require.Equal(t, tc.pool, request.PoolSelector)
				if tc.gpu != "" {
					require.Equal(t, []string{"H100"}, request.GpuRequest)
					require.EqualValues(t, 1, request.GpuCount)
				} else {
					require.Zero(t, request.GpuCount)
				}
			})
		}
	}
}
