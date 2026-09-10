package managedendpoint

import (
	"context"
	"testing"

	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProtectedMinimumKeepsReadyCopiesAcrossRollout(t *testing.T) {
	noPreemption := false
	fleet := &types.Fleet{Endpoints: map[string]types.FleetEndpoint{
		"model": {Enabled: true, GPUs: map[string]types.FleetPlacement{
			"H100": {Priority: 1, MinReplicas: 1, MaxReplicas: 2, Preemption: &noPreemption},
		}},
	}}
	endpoint := &types.ManagedEndpoint{Spec: types.ManagedEndpointSpec{ID: "model"}, Version: 2, Status: types.EndpointStatusActive}
	endpoints := map[string]*types.ManagedEndpoint{"model": endpoint}
	old := &types.EndpointReplica{ID: "old", EndpointID: "model", Version: 1, GPU: "H100", Status: types.ReplicaStatusReady, Protected: true}
	current := &types.EndpointReplica{ID: "new", EndpointID: "model", Version: 2, GPU: "H100", Status: types.ReplicaStatusLoading}
	other := &types.EndpointReplica{ID: "other-gpu", EndpointID: "model", Version: 2, GPU: "A100", Status: types.ReplicaStatusReady}
	live := []*types.EndpointReplica{old, current, other}
	assert.Equal(t, map[string]bool{"old": true}, protectedReplicas(fleet, endpoints, live, true))
	current.Status = types.ReplicaStatusReady
	assert.Equal(t, map[string]bool{"new": true}, protectedReplicas(fleet, endpoints, live, true))
	assert.Equal(t, map[string]bool{"new": true, "old": true, "other-gpu": true}, protectedReplicas(fleet, endpoints, live, false), "cluster override includes copies waiting to retire after removal from config")
	placement := fleet.Endpoints["model"].GPUs["H100"]
	placement.Preemption = nil
	fleet.Endpoints["model"].GPUs["H100"] = placement
	assert.Empty(t, protectedReplicas(fleet, endpoints, live, true), "preemption defaults to enabled")
	placement.Preemption, placement.MinReplicas = &noPreemption, 0
	fleet.Endpoints["model"].GPUs["H100"] = placement
	assert.Empty(t, protectedReplicas(fleet, endpoints, live, true), "a zero minimum protects no extras")
}

type protectionCalls struct {
	repository.ManagedEndpointRepository
	replicas map[string]*types.EndpointReplica
	calls    []string
	fail     map[string]error
}

func (r *protectionCalls) SetReplicaProtection(_ context.Context, id string, protected bool) (*types.EndpointReplica, error) {
	r.calls = append(r.calls, id)
	if err := r.fail[id]; err != nil {
		return nil, err
	}
	updated := *r.replicas[id]
	updated.Protected = protected
	return &updated, nil
}

func TestProtectionPromotesBeforeReleasingOldMinimum(t *testing.T) {
	s := newServiceForTest(t)
	s.config.Preemption.Enabled = true
	endpoint := seedEndpoint(t, s)
	noPreemption := false
	fleet := seedFleet(t, s, map[string]types.FleetEndpoint{endpoint.Spec.ID: {
		Enabled: true, GPUs: map[string]types.FleetPlacement{"H100": {Priority: 1, MinReplicas: 1, MaxReplicas: 2, Preemption: &noPreemption}},
	}})
	endpoint.Version = 2
	old := &types.EndpointReplica{ID: "old", EndpointID: endpoint.Spec.ID, Version: 1, GPU: "H100", Status: types.ReplicaStatusReady, Protected: true}
	current := &types.EndpointReplica{ID: "new", EndpointID: endpoint.Spec.ID, Version: 2, GPU: "H100", Status: types.ReplicaStatusReady}
	repo := &protectionCalls{replicas: map[string]*types.EndpointReplica{"old": old, "new": current}}
	s.repo = repo
	blocked, err := s.controller.reconcileProtection(context.Background(), fleet, map[string]*types.ManagedEndpoint{endpoint.Spec.ID: endpoint}, []*types.EndpointReplica{old, current})
	require.NoError(t, err)
	require.Empty(t, blocked)
	assert.Equal(t, []string{"new", "old"}, repo.calls)
	assert.True(t, current.Protected)
	assert.False(t, old.Protected)
}

func TestProtectionRetainsExistingRoleWhileBothVersionsLoad(t *testing.T) {
	noPreemption := false
	fleet := &types.Fleet{Endpoints: map[string]types.FleetEndpoint{"model": {
		Enabled: true, GPUs: map[string]types.FleetPlacement{"H100": {MinReplicas: 1, Preemption: &noPreemption}},
	}}}
	endpoints := map[string]*types.ManagedEndpoint{"model": {Spec: types.ManagedEndpointSpec{ID: "model"}, Version: 2, Status: types.EndpointStatusActive}}
	old := &types.EndpointReplica{ID: "old", EndpointID: "model", Version: 1, GPU: "H100", Status: types.ReplicaStatusLoading, Protected: true}
	current := &types.EndpointReplica{ID: "new", EndpointID: "model", Version: 2, GPU: "H100", Status: types.ReplicaStatusScheduling}
	assert.Equal(t, map[string]bool{"old": true}, protectedReplicas(fleet, endpoints, []*types.EndpointReplica{old, current}, true))
	current.Status = types.ReplicaStatusReady
	assert.Equal(t, map[string]bool{"new": true}, protectedReplicas(fleet, endpoints, []*types.EndpointReplica{old, current}, true))
}

func TestProtectionFailureIsIsolatedAndDoesNotReleaseOldFloor(t *testing.T) {
	for _, pending := range []bool{false, true} {
		name := "promotion_raced_eviction"
		if pending {
			name = "unassigned_minimum"
		}
		t.Run(name, func(t *testing.T) {
			s := newServiceForTest(t)
			s.config.Preemption.Enabled = true
			noPreemption := false
			fleet := &types.Fleet{Endpoints: map[string]types.FleetEndpoint{}}
			endpoints := map[string]*types.ManagedEndpoint{}
			for _, id := range []string{"blocked", "healthy"} {
				fleet.Endpoints[id] = types.FleetEndpoint{Enabled: true, GPUs: map[string]types.FleetPlacement{
					"H100": {MinReplicas: 1, MaxReplicas: 2, Preemption: &noPreemption},
				}}
				endpoints[id] = &types.ManagedEndpoint{Spec: types.ManagedEndpointSpec{ID: id}, Version: 2, Status: types.EndpointStatusActive}
			}
			old := &types.EndpointReplica{ID: "blocked-old", EndpointID: "blocked", GPU: "H100", Version: 1, Status: types.ReplicaStatusReady, Protected: true}
			current := &types.EndpointReplica{ID: "blocked-new", EndpointID: "blocked", GPU: "H100", Version: 2, Status: types.ReplicaStatusReady}
			healthyOld := &types.EndpointReplica{ID: "healthy-old", EndpointID: "healthy", GPU: "H100", Version: 1, Status: types.ReplicaStatusReady, Protected: true}
			healthyNew := &types.EndpointReplica{ID: "healthy-new", EndpointID: "healthy", GPU: "H100", Version: 2, Status: types.ReplicaStatusReady}
			live := []*types.EndpointReplica{old, current, healthyOld, healthyNew}
			if pending {
				current.Status = types.ReplicaStatusScheduling
				live = live[1:] // no old copy exists for this still-unassigned minimum
			}
			repo := &protectionCalls{
				replicas: map[string]*types.EndpointReplica{old.ID: old, current.ID: current, healthyOld.ID: healthyOld, healthyNew.ID: healthyNew},
				fail:     map[string]error{current.ID: repository.ErrReplicaProtectionChanged},
			}
			s.repo = repo
			blocked, err := s.controller.reconcileProtection(context.Background(), fleet, endpoints, live)
			require.ErrorIs(t, err, repository.ErrReplicaProtectionChanged)
			assert.Equal(t, map[protectionGroup]bool{{"blocked", "H100"}: true}, blocked)
			assert.Equal(t, []string{current.ID, healthyNew.ID, healthyOld.ID}, repo.calls)
			assert.True(t, old.Protected, "a failed promotion must not release the old floor")
			assert.True(t, healthyNew.Protected)
			assert.False(t, healthyOld.Protected, "an independent group still completes its role transfer")
		})
	}
}
