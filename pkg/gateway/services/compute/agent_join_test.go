package compute

import (
	"context"
	"errors"
	"testing"
	"time"

	model "github.com/beam-cloud/beta9/pkg/compute"
	"github.com/beam-cloud/beta9/pkg/scheduler"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
)

// An agent that is refused goes on with the identity it has saved. These
// tests pin what a refused join leaves behind: what was there before it, and
// nothing else.

// unwritableWorkers is a worker repository that can refuse to add a worker,
// which fails a join after its machine has been saved.
type unwritableWorkers struct {
	*fakeWorkerRepo
	addErr error
}

func (r *unwritableWorkers) AddWorker(worker *types.Worker) error {
	if r.addErr != nil {
		return r.addErr
	}
	return r.fakeWorkerRepo.AddWorker(worker)
}

func joinTestService() (*Service, *fakeComputeRepo, *unwritableWorkers) {
	service, repo := buyingTestService(types.AppConfig{})
	workers := &unwritableWorkers{fakeWorkerRepo: &fakeWorkerRepo{}}
	service.workerRepo = workers
	service.scheduler = scheduler.NewSchedulerForCapacityChecks(workers, repo, scheduler.NewWorkerPoolManager())
	return service, repo, workers
}

// joinedByHand makes a pool of L40S machines and joins one to it.
func joinedByHand(t *testing.T, service *Service, repo *fakeComputeRepo) (script string, machine model.AgentTokenState) {
	t.Helper()
	ctx := context.Background()
	_, err := service.CreateManagedPool(ctx, clusterAdminAuth(), "l40s", types.WorkerPoolConfig{GPUType: "L40S"})
	require.NoError(t, err)
	bootstrap, err := service.CreateManagedMachine(ctx, clusterAdminAuth(), "l40s")
	require.NoError(t, err)
	response := joinAs(t, service, bootstrap.InstallCommand, "by-hand")
	require.True(t, response.Ok, response.ErrMsg)

	joined, err := repo.GetAgentMachineState(ctx, "admin-workspace", "l40s", response.MachineId)
	require.NoError(t, err)
	require.NotNil(t, joined)
	return bootstrap.InstallCommand, *joined
}

func TestARefusedJoinLeavesAMachineThatHadJoinedAsItWas(t *testing.T) {
	ctx := context.Background()

	for name, refuse := range map[string]func(*Service, *unwritableWorkers, string) *pb.JoinAgentResponse{
		"before the machine is saved": func(service *Service, _ *unwritableWorkers, script string) *pb.JoinAgentResponse {
			token := joinTokenInScript.FindStringSubmatch(script)[1]
			response, err := service.JoinAgent(ctx, &pb.JoinAgentRequest{
				JoinToken: token, MachineFingerprint: "fingerprint-by-hand", Hostname: "by-hand",
				CpuCount: 20, MemoryMb: 60_000, Schedulable: true,
			})
			require.NoError(t, err)
			return response
		},
		"after the machine is saved": func(service *Service, workers *unwritableWorkers, script string) *pb.JoinAgentResponse {
			workers.addErr = errors.New("the worker cannot be written")
			return joinAs(t, service, script, "by-hand")
		},
	} {
		t.Run(name, func(t *testing.T) {
			service, repo, workers := joinTestService()
			script, joined := joinedByHand(t, service, repo)
			worker := workers.worker
			require.NotNil(t, worker)

			response := refuse(service, workers, script)
			require.False(t, response.Ok)

			machine, err := repo.GetAgentMachineState(ctx, "admin-workspace", "l40s", joined.MachineID)
			require.NoError(t, err)
			require.NotNil(t, machine, "the machine is still in its pool")
			require.Equal(t, joined.TokenHash, machine.TokenHash, "under the token its agent holds")
			require.Same(t, worker, workers.worker, "with its worker")
		})
	}
}

func TestARefusedFirstJoinLeavesNothing(t *testing.T) {
	ctx := context.Background()
	service, repo, workers := joinTestService()
	_, err := service.CreateManagedPool(ctx, clusterAdminAuth(), "l40s", types.WorkerPoolConfig{GPUType: "L40S"})
	require.NoError(t, err)
	bootstrap, err := service.CreateManagedMachine(ctx, clusterAdminAuth(), "l40s")
	require.NoError(t, err)

	workers.addErr = errors.New("the worker cannot be written")
	response := joinAs(t, service, bootstrap.InstallCommand, "by-hand")
	require.False(t, response.Ok)
	require.Contains(t, response.ErrMsg, "the worker cannot be written")

	machine, err := repo.GetAgentMachineState(ctx, "admin-workspace", "l40s", bootstrap.MachineID)
	require.NoError(t, err)
	require.Nil(t, machine)
	require.Nil(t, workers.worker)
}

// The worker of a machine that a pool bought stands for it from the purchase
// on, and requests wait on it. A join that fails is tried again by the
// machine; what gives up on it is the pool's boot timeout.
func TestARefusedJoinLeavesTheWorkerOfAMachineThatWasBought(t *testing.T) {
	ctx := context.Background()
	service, repo, workers := joinTestService()
	_, err := service.CreateManagedPool(ctx, clusterAdminAuth(), "spot-l40s", spotPool())
	require.NoError(t, err)
	script, _, err := service.JoinScript(ctx, "spot-l40s", "machine-1")
	require.NoError(t, err)
	booting := &types.Worker{Id: model.AgentMachineWorkerID("machine-1"), MachineId: "machine-1", PoolName: "spot-l40s", Status: types.WorkerStatusPending}
	workers.worker = booting

	workers.addErr = errors.New("the worker cannot be written")
	require.False(t, joinAs(t, service, script, "verda-1").Ok)

	require.Same(t, booting, workers.worker)
	machine, err := repo.GetAgentMachineState(ctx, "admin-workspace", "spot-l40s", "machine-1")
	require.NoError(t, err)
	require.Nil(t, machine, "the machine has not joined")

	workers.addErr = nil
	require.True(t, joinAs(t, service, script, "verda-1").Ok)
}

func TestARefusedJoinLeavesAMachineOfAWorkspacesOwnPoolAsItWas(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	repo := &fakeComputeRepo{
		pools: map[string][]*model.PoolState{"workspace-1": {{
			WorkspaceID: "workspace-1",
			Name:        "own",
			Mode:        string(types.PoolModePrivate),
			Config:      &pb.PoolConfig{Name: "own"},
			CreatedAt:   now,
		}}},
		joinTokens: map[string]*model.JoinTokenState{hashComputeToken("join-token"): {
			TokenHash:     hashComputeToken("join-token"),
			WorkspaceID:   "workspace-1",
			PoolName:      "own",
			PoolCreatedAt: now,
		}},
	}
	workers := &unwritableWorkers{fakeWorkerRepo: &fakeWorkerRepo{}}
	service := &Service{
		computeRepo: repo,
		workerRepo:  workers,
		scheduler:   scheduler.NewSchedulerForCapacityChecks(workers, repo, scheduler.NewWorkerPoolManager()),
	}
	join := func() *pb.JoinAgentResponse {
		response, err := service.JoinAgent(ctx, &pb.JoinAgentRequest{
			JoinToken: "join-token", MachineFingerprint: "fingerprint-1",
			CpuCount: 4, MemoryMb: 8192, Schedulable: true,
		})
		require.NoError(t, err)
		return response
	}

	first := join()
	require.True(t, first.Ok, first.ErrMsg)
	joined, err := repo.GetAgentMachineState(ctx, "workspace-1", "own", first.MachineId)
	require.NoError(t, err)
	tokenHash := joined.TokenHash
	worker := workers.worker
	require.NotNil(t, worker)

	workers.addErr = errors.New("the worker cannot be written")
	require.False(t, join().Ok)

	machine, err := repo.GetAgentMachineState(ctx, "workspace-1", "own", first.MachineId)
	require.NoError(t, err)
	require.NotNil(t, machine, "the machine is still in its pool")
	require.Equal(t, tokenHash, machine.TokenHash, "under the token its agent holds")
	require.Same(t, worker, workers.worker, "with its worker")
}
