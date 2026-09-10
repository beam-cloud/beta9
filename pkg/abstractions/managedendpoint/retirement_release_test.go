package managedendpoint

import (
	"context"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/scheduler"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Worker ownership changes are covered by repository tests. This fixture
// persists the controller's selected role without a running worker process.
type retirementProtectionRepository struct {
	repository.ManagedEndpointRepository
}

func (r retirementProtectionRepository) SetReplicaProtection(ctx context.Context, id string, protected bool) (*types.EndpointReplica, error) {
	replica, err := r.GetReplica(ctx, id)
	if err != nil {
		return nil, err
	}
	replica.Protected = protected
	return replica, r.SaveReplica(ctx, replica)
}

func TestReplaceWaitsForDonorRetirementDuringHotPolicyChange(t *testing.T) {
	for _, donorDrain := range []uint32{0, 5} {
		name := "immediate stop"
		if donorDrain > 0 {
			name = "graceful drain"
		}
		t.Run(name, func(t *testing.T) {
			s := newFillService(t)
			qwen, donorEndpoint, endpoints := fillEndpoints(t, s)
			s.repo = retirementProtectionRepository{s.repo}
			qwen.Version, qwen.Spec.Rollout, qwen.Spec.DrainSeconds = 2, "replace", 0
			donorEndpoint.Status, donorEndpoint.Spec.DrainSeconds = types.EndpointStatusRetired, donorDrain
			old := reclaimableReplica(t, s, qwen, "old-qwen", "w1")
			old.Version = 1 // previously serverless: true and minReplicas: 0
			donor := reclaimableReplica(t, s, donorEndpoint, "donor", "w1")
			donor.Protected = true
			ctx := context.Background()
			require.NoError(t, s.repo.SaveReplica(ctx, old))
			require.NoError(t, s.repo.SaveReplica(ctx, donor))
			noPreemption := false
			fleet := seedFleet(t, s, map[string]types.FleetEndpoint{qwen.Spec.ID: {
				Enabled: true, GPUs: map[string]types.FleetPlacement{"H100": {Priority: 1, MinReplicas: 1, MaxReplicas: 2, Preemption: &noPreemption}},
			}})
			live := []*types.EndpointReplica{old, donor}
			blocked, err := s.controller.reconcileProtection(ctx, fleet, endpoints, live)
			require.NoError(t, err)
			require.Empty(t, blocked)
			require.True(t, old.Protected, "the serving on-demand copy becomes the protected hot minimum")

			inv := noRoomInventory()
			// Qwen deliberately comes before its removed donor in the input.
			s.controller.retireEndpoints(ctx, []*types.ManagedEndpoint{qwen, donorEndpoint}, fleet, live, inv, blocked)
			if donorDrain == 0 {
				require.Equal(t, types.ReplicaStatusStopped, donor.Status)
			} else {
				require.Equal(t, types.ReplicaStatusDraining, donor.Status)
			}
			assert.Equal(t, types.ReplicaStatusReady, statusOf(t, s, old.ID), "replace waits for the donor's already-requested release")
			assert.Zero(t, inv.free["H100"]["gpu-a"], "projected release must not become schedulable capacity")
			assert.Zero(t, inv.workers["w1"].free)

			// The next real inventory sees the released GPU. Start a new copy
			// and retain the old serving minimum throughout loading.
			donor.Status = types.ReplicaStatusStopped
			require.NoError(t, s.repo.SaveReplica(ctx, donor))
			fresh := idleInventory(1)
			s.controller.fill(ctx, "H100", fleet.Entries("H100"), endpoints, live, fresh)
			requests, err := scheduler.NewRequestBacklog(s.rdb).PopN(10)
			require.NoError(t, err)
			require.Len(t, requests, 1)
			assert.True(t, requests[0].Evictable, "the old ready copy owns protection until the new copy is ready")
			current, err := s.repo.GetReplicaByContainer(ctx, requests[0].ContainerId)
			require.NoError(t, err)
			require.NotNil(t, current)
			current.Status = types.ReplicaStatusLoading
			require.NoError(t, s.repo.SaveReplica(ctx, current))
			live = append(live, current)
			s.controller.retire(ctx, qwen, fleet, live, fresh, nil)
			assert.Equal(t, types.ReplicaStatusReady, statusOf(t, s, old.ID))

			current.Status = types.ReplicaStatusReady
			require.NoError(t, s.repo.SaveReplica(ctx, current))
			blocked, err = s.controller.reconcileProtection(ctx, fleet, endpoints, live)
			require.NoError(t, err)
			assert.True(t, current.Protected)
			assert.False(t, old.Protected)
			s.controller.retire(ctx, qwen, fleet, live, fresh, blocked)
			assert.Equal(t, types.ReplicaStatusStopped, statusOf(t, s, old.ID))
			assert.Equal(t, types.ReplicaStatusReady, statusOf(t, s, current.ID))
		})
	}
}

func TestRetireUnplacedGPUPrecedesServingRollout(t *testing.T) {
	s := newFillService(t)
	qwen, donorEndpoint, _ := fillEndpoints(t, s)
	qwen.Version, qwen.Spec.Rollout = 2, "replace"
	donorEndpoint.Spec.DrainSeconds = 0
	old := reclaimableReplica(t, s, qwen, "old-qwen", "w1")
	old.Version, old.Protected = 1, true
	donor := reclaimableReplica(t, s, donorEndpoint, "removed-h100", "w1")
	ctx := context.Background()
	require.NoError(t, s.repo.SaveReplica(ctx, old))
	fleet := seedFleet(t, s, map[string]types.FleetEndpoint{
		qwen.Spec.ID:          {Enabled: true, GPUs: map[string]types.FleetPlacement{"H100": {Priority: 1, MinReplicas: 1, MaxReplicas: 2}}},
		donorEndpoint.Spec.ID: {Enabled: true, GPUs: map[string]types.FleetPlacement{"A100-80": {Priority: 1, MaxReplicas: 1}}},
	})
	s.controller.retireEndpoints(ctx, []*types.ManagedEndpoint{qwen, donorEndpoint}, fleet, []*types.EndpointReplica{old, donor}, noRoomInventory(), nil)
	assert.Equal(t, types.ReplicaStatusStopped, statusOf(t, s, donor.ID))
	assert.Equal(t, types.ReplicaStatusReady, statusOf(t, s, old.ID), "removing one GPU placement also releases capacity before version replacement")
}

func TestPendingRetirementRequiresBoundedUsableOwnedCapacity(t *testing.T) {
	for _, reason := range []string{
		"fits", "immediate stop", "historical stop", "expired drain", "eviction already reassigned",
		"ineligible pool", "wrong owner", "GPU floor", "CPU floor", "memory floor", "memory overhead",
		"insufficient GPUs", "fragmented GPUs", "duplicate container", "unknown request", "pending assignment",
	} {
		t.Run(reason, func(t *testing.T) {
			s := newFillService(t)
			target, donorEndpoint, _ := fillEndpoints(t, s)
			s.backend = fillBackend{config: `{"runtime":{"cpu":2000,"memory":101}}`}
			donor := reclaimableReplica(t, s, donorEndpoint, "donor", "w1")
			donor.Status, donor.DrainDeadline = types.ReplicaStatusDraining, time.Now().Add(time.Minute)
			live := []*types.EndpointReplica{donor}
			ctx := context.Background()
			state, err := s.containers.GetContainerState(donor.ContainerID)
			require.NoError(t, err)
			state.Cpu, state.Memory = 1000, 101
			inv := noRoomInventory()
			worker := inv.workers["w1"]
			worker.total, worker.cpu, worker.totalCPU, worker.totalMemory = 2, 1000, 2000, 127
			want := reason == "fits" || reason == "immediate stop"
			switch reason {
			case "immediate stop":
				donor.Status, donor.DrainDeadline = types.ReplicaStatusStopped, time.Time{}
				state.Status = types.ContainerStatusStopping
				inv.noteRetirement(donor.ID)
			case "historical stop":
				donor.Status, donor.EndedAt = types.ReplicaStatusStopped, time.Now().Add(-time.Hour)
			case "expired drain":
				donor.DrainDeadline = time.Now().Add(-time.Second)
				donorEndpoint.Status = types.EndpointStatusRetired
				require.NoError(t, s.repo.SaveReplica(ctx, donor))
				s.controller.retire(ctx, donorEndpoint, &types.Fleet{}, live, inv, nil)
				assert.Empty(t, inv.retiring, "a no-op on an existing drain must not renew its wait")
			case "eviction already reassigned":
				donor.Status, state.Status, state.Evicting = types.ReplicaStatusEvicting, types.ContainerStatusStopping, true
			case "ineligible pool":
				inv.pools["H100"] = nil
			case "wrong owner":
				state.WorkerId = "different-worker"
			case "GPU floor":
				inv.floors = map[string]uint32{"gpu-a": 1}
			case "CPU floor":
				inv.resourceFloors = map[string]replicaResources{"gpu-a": {cpu: 1}}
			case "memory floor":
				inv.resourceFloors = map[string]replicaResources{"gpu-a": {memory: 1}}
			case "memory overhead":
				worker.totalMemory = 126
			case "insufficient GPUs":
				target.Spec.Gpu = map[string]types.GpuSpec{"H100": {Count: 2}}
			case "fragmented GPUs":
				target.Spec.Gpu = map[string]types.GpuSpec{"H100": {Count: 2}}
				worker.total = 1
				other := reclaimableReplica(t, s, donorEndpoint, "other", "w2")
				other.Status, other.DrainDeadline = donor.Status, donor.DrainDeadline
				live = append(live, other)
				inv.workers["w2"] = &inventoryWorker{pool: "gpu-a", gpu: "H100", total: 1}
			case "duplicate container":
				target.Spec.Gpu = map[string]types.GpuSpec{"H100": {Count: 2}}
				duplicate := *donor
				duplicate.ID = "duplicate"
				live = append(live, &duplicate)
			case "unknown request":
				s.backend = fillBackend{config: `{"runtime":`}
			case "pending assignment":
				inv.pending = map[string]bool{"gpu-a": true}
			}
			require.NoError(t, s.containers.SetContainerState(donor.ContainerID, state))
			if state.Evicting {
				// Evicting is owned by the atomic scheduler eviction path;
				// ordinary state initialization intentionally does not set it.
				require.NoError(t, s.rdb.HSet(ctx, common.RedisKeys.SchedulerContainerState(donor.ContainerID), "evicting", 1).Err())
			}
			assert.Equal(t, want, s.controller.pendingReleaseFits(ctx, &placementTarget{endpoint: target}, "H100", live, inv))
			assert.Zero(t, worker.free, "the original inventory remains authoritative")
			assert.EqualValues(t, 1000, worker.cpu)
			if reason == "immediate stop" {
				fresh := *inv
				fresh.retiring = nil
				assert.False(t, s.controller.pendingReleaseFits(ctx, &placementTarget{endpoint: target}, "H100", live, &fresh), "an immediate-stop marker is scoped to one inventory snapshot")
			}
		})
	}
}
