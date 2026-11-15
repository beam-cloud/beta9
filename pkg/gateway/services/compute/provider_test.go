package compute

import (
	"context"
	"testing"

	model "github.com/beam-cloud/beta9/pkg/compute"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
)

func TestProviderJoinCommandCreatesPoolOncePerGPU(t *testing.T) {
	ctx := testAuthContext("workspace-1", "owner-token")
	repo := &fakeComputeRepo{}
	service := &Service{computeRepo: repo}

	first, err := service.GetProviderJoinCommand(ctx, &pb.GetProviderJoinCommandRequest{Gpu: "h100"})
	if err != nil || !first.Ok {
		t.Fatalf("GetProviderJoinCommand() = %+v, %v", first, err)
	}
	if first.Command == "" || first.Token == "" || first.PoolName != model.ProviderPoolName("workspace-1", "H100") {
		t.Fatalf("unexpected join command response: %+v", first)
	}

	state, err := repo.GetPoolState(ctx, "workspace-1", first.PoolName)
	if err != nil || state == nil {
		t.Fatalf("provider pool state = %+v, %v", state, err)
	}
	if state.Mode != string(types.PoolModeProvider) || state.Fallback != types.PrivatePoolFallbackFail || state.Priority != providerPoolPriority {
		t.Fatalf("provider pool state = %+v", state)
	}
	if got := state.Config.GetGpu(); len(got) != 1 || got[0] != "H100" {
		t.Fatalf("provider pool gpu = %v, want [H100]", got)
	}

	// A second machine for the same GPU reuses the pool.
	second, err := service.GetProviderJoinCommand(ctx, &pb.GetProviderJoinCommandRequest{Gpu: "H100"})
	if err != nil || !second.Ok || second.PoolName != first.PoolName {
		t.Fatalf("second GetProviderJoinCommand() = %+v, %v", second, err)
	}
	if pools, _ := repo.ListPoolStates(ctx, "workspace-1", 0); len(pools) != 1 {
		t.Fatalf("pool count = %d, want 1", len(pools))
	}

	// Machines that join through the token are listed as provider machines.
	joined, err := service.JoinAgent(context.Background(), &pb.JoinAgentRequest{
		JoinToken: first.Token, MachineFingerprint: "fp-1", CpuCount: 8, MemoryMb: 32768, Gpu: []string{"H100"}, GpuCount: 1, Schedulable: true,
	})
	if err != nil || !joined.Ok {
		t.Fatalf("JoinAgent() = %+v, %v", joined, err)
	}
	machines, err := service.ListProviderMachines(ctx, &pb.ListProviderMachinesRequest{})
	if err != nil || !machines.Ok || len(machines.Machines) != 1 || machines.Machines[0].PoolName != first.PoolName {
		t.Fatalf("ListProviderMachines() = %+v, %v", machines, err)
	}
	if other, _ := service.ListProviderMachines(testAuthContext("workspace-2", "t"), &pb.ListProviderMachinesRequest{}); len(other.Machines) != 0 {
		t.Fatalf("cross-workspace ListProviderMachines() = %+v", other)
	}
}

func TestProviderJoinCommandRequiresGPU(t *testing.T) {
	service := &Service{computeRepo: &fakeComputeRepo{}}
	for _, gpu := range []string{"", "any", string(types.NO_GPU)} {
		res, err := service.GetProviderJoinCommand(testAuthContext("workspace-1", "t"), &pb.GetProviderJoinCommandRequest{Gpu: gpu})
		if err != nil || res.Ok {
			t.Fatalf("GetProviderJoinCommand(%q) = %+v, %v; want gpu error", gpu, res, err)
		}
	}
	// Existing non-provider pools with the reserved name are never hijacked.
	repo := &fakeComputeRepo{}
	name := model.ProviderPoolName("workspace-1", "A10G")
	_ = repo.SavePoolState(context.Background(), "workspace-1", &model.PoolState{Name: name, Mode: string(types.PoolModePrivate)})
	res, err := (&Service{computeRepo: repo}).GetProviderJoinCommand(testAuthContext("workspace-1", "t"), &pb.GetProviderJoinCommandRequest{Gpu: "A10G"})
	if err != nil || res.Ok {
		t.Fatalf("GetProviderJoinCommand() on private pool = %+v, %v; want error", res, err)
	}
}

func TestDeleteProviderMachineStopsReplicasFirst(t *testing.T) {
	ctx := testAuthContext("workspace-1", "owner-token")
	repo := &fakeComputeRepo{}
	containerRepo := &fakeContainerRepo{containers: []types.ContainerState{
		{ContainerId: "managed-stub-1-aaa", Status: types.ContainerStatusRunning},
		{ContainerId: "managed-stub-1-bbb", Status: types.ContainerStatusStopping},
	}}
	service := &Service{computeRepo: repo, containerRepo: containerRepo, workerRepo: &fakeWorkerRepo{}}

	join, err := service.GetProviderJoinCommand(ctx, &pb.GetProviderJoinCommandRequest{Gpu: "H100"})
	if err != nil || !join.Ok {
		t.Fatalf("GetProviderJoinCommand() = %+v, %v", join, err)
	}
	joined, err := service.JoinAgent(context.Background(), &pb.JoinAgentRequest{
		JoinToken: join.Token, MachineFingerprint: "fp-1", CpuCount: 8, MemoryMb: 32768, Gpu: []string{"H100"}, GpuCount: 1, Schedulable: true,
	})
	if err != nil || !joined.Ok {
		t.Fatalf("JoinAgent() = %+v, %v", joined, err)
	}

	// Another workspace cannot remove the provider's machine.
	res, err := service.DeletePrivateMachine(testAuthContext("workspace-2", "t"), &pb.DeleteMachineRequest{PoolName: join.PoolName, MachineId: joined.MachineId})
	if err != nil || res.Ok {
		t.Fatalf("cross-workspace DeletePrivateMachine() = %+v, %v; want failure", res, err)
	}

	res, err = service.DeletePrivateMachine(ctx, &pb.DeleteMachineRequest{PoolName: join.PoolName, MachineId: joined.MachineId})
	if err != nil || !res.Ok {
		t.Fatalf("DeletePrivateMachine() = %+v, %v", res, err)
	}
	// The running replica is stopped through the scheduler path so the
	// endpoint controller replaces it; one already stopping is left alone.
	if got, want := containerRepo.stopped, []string{"managed-stub-1-aaa"}; !sameStrings(got, want) {
		t.Fatalf("stopped containers = %v, want %v", got, want)
	}
	if machines, _ := service.ListProviderMachines(ctx, &pb.ListProviderMachinesRequest{}); len(machines.Machines) != 0 {
		t.Fatalf("ListProviderMachines() after delete = %+v, want none", machines.Machines)
	}
}
