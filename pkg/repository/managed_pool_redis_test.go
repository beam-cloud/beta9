package repository

import (
	"context"
	"testing"

	"github.com/beam-cloud/beta9/pkg/compute"
	"github.com/beam-cloud/beta9/pkg/types"
)

func TestManagedPoolRepositoryIsolatedFromLegacyPoolState(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	managed := NewManagedPoolRedisRepository(rdb)
	legacy := NewComputeRedisRepository(rdb)
	state := &compute.PoolState{
		Name:             "public-h100",
		ManagementSource: types.WorkerPoolManagementSourceAPI,
		WorkerConfig:     &types.WorkerPoolConfig{Mode: types.PoolModeExternal},
	}

	if err := managed.SaveManagedPoolState(ctx, "admin-workspace", state); err != nil {
		t.Fatal(err)
	}
	got, err := managed.GetManagedPoolState(ctx, "admin-workspace", state.Name)
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || got.WorkspaceID != "admin-workspace" || got.Name != state.Name {
		t.Fatalf("managed state = %#v", got)
	}
	legacyState, err := legacy.GetPoolState(ctx, "admin-workspace", state.Name)
	if err != nil {
		t.Fatal(err)
	}
	if legacyState != nil {
		t.Fatalf("legacy repository exposed managed state: %#v", legacyState)
	}
	legacyStates, err := legacy.ListAllPoolStates(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(legacyStates) != 0 {
		t.Fatalf("legacy repository listed managed states: %#v", legacyStates)
	}
}

func TestManagedPoolRepositoryMaintainsWorkspaceIndexesAcrossInstances(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	first := NewManagedPoolRedisRepository(rdb)
	second := NewManagedPoolRedisRepository(rdb)

	for _, name := range []string{"pool-b", "pool-a"} {
		if err := first.SaveManagedPoolState(ctx, "workspace-a", &compute.PoolState{Name: name}); err != nil {
			t.Fatal(err)
		}
	}
	if err := second.SaveManagedPoolState(ctx, "workspace-b", &compute.PoolState{Name: "pool-c"}); err != nil {
		t.Fatal(err)
	}

	states, err := second.ListManagedPoolStates(ctx, "workspace-a", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(states) != 2 || states[0].Name != "pool-a" || states[1].Name != "pool-b" {
		t.Fatalf("workspace-a states = %#v", states)
	}
	states, err = first.ListManagedPoolStates(ctx, "workspace-b", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(states) != 1 || states[0].Name != "pool-c" {
		t.Fatalf("workspace-b states = %#v", states)
	}
	if err := second.DeleteManagedPoolState(ctx, "workspace-a", "pool-a"); err != nil {
		t.Fatal(err)
	}
	states, err = first.ListManagedPoolStates(ctx, "workspace-a", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(states) != 1 || states[0].Name != "pool-b" {
		t.Fatalf("workspace states after first delete = %#v", states)
	}
	if err := first.DeleteManagedPoolState(ctx, "workspace-a", "pool-b"); err != nil {
		t.Fatal(err)
	}
	states, err = second.ListManagedPoolStates(ctx, "workspace-a", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(states) != 0 {
		t.Fatalf("workspace-a states after delete = %#v", states)
	}
	states, err = second.ListManagedPoolStates(ctx, "workspace-b", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(states) != 1 || states[0].Name != "pool-c" {
		t.Fatalf("workspace-b states after delete = %#v", states)
	}
}
