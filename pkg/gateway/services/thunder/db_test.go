package thunder

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
)

func TestRedisRepositoryClientEnrollment(t *testing.T) {
	rdb := newThunderRedisClient(t)
	defer rdb.Close()

	repo := NewRedisRepository(rdb)
	ctx := context.Background()
	state := &ClientEnrollmentState{
		ContainerID:       "container-1",
		WorkspaceID:       "workspace-1",
		WorkerID:          "worker-1",
		MachineID:         "machine-1",
		PoolName:          "pool-1",
		EnrollmentTokenID: "token-1",
	}
	if err := repo.SaveClientEnrollment(ctx, state, 0); err != nil {
		t.Fatalf("SaveClientEnrollment() error = %v", err)
	}

	got, found, err := repo.GetClientEnrollment(ctx, state.ContainerID)
	if err != nil || !found {
		t.Fatalf("GetClientEnrollment() = found %v, err %v", found, err)
	}
	if got.EnrollmentTokenID != state.EnrollmentTokenID || got.WorkspaceID != state.WorkspaceID {
		t.Fatalf("client enrollment = %+v", got)
	}

	listed, err := repo.ListClientEnrollments(ctx)
	if err != nil {
		t.Fatalf("ListClientEnrollments() error = %v", err)
	}
	if len(listed) != 1 || listed[0].ContainerID != state.ContainerID {
		t.Fatalf("listed client enrollments = %+v", listed)
	}

	if err := repo.DeleteClientEnrollment(ctx, state.ContainerID); err != nil {
		t.Fatalf("DeleteClientEnrollment() error = %v", err)
	}
	_, found, err = repo.GetClientEnrollment(ctx, state.ContainerID)
	if err != nil || found {
		t.Fatalf("GetClientEnrollment() after delete = found %v, err %v", found, err)
	}
}

func TestRedisRepositoryNodeEnrollment(t *testing.T) {
	rdb := newThunderRedisClient(t)
	defer rdb.Close()

	repo := NewRedisRepository(rdb)
	ctx := context.Background()
	state := &NodeEnrollmentState{
		WorkspaceID:       "workspace-1",
		PoolName:          "pool-1",
		MachineID:         "machine-1",
		EnrollmentTokenID: "token-1",
	}
	if err := repo.SaveNodeEnrollment(ctx, state, 0); err != nil {
		t.Fatalf("SaveNodeEnrollment() error = %v", err)
	}

	got, found, err := repo.GetNodeEnrollment(ctx, state.WorkspaceID, state.PoolName, state.MachineID)
	if err != nil || !found {
		t.Fatalf("GetNodeEnrollment() = found %v, err %v", found, err)
	}
	if got.EnrollmentTokenID != state.EnrollmentTokenID {
		t.Fatalf("node enrollment = %+v", got)
	}

	listed, err := repo.ListNodeEnrollments(ctx, state.WorkspaceID, state.PoolName)
	if err != nil {
		t.Fatalf("ListNodeEnrollments() error = %v", err)
	}
	if len(listed) != 1 || listed[0].MachineID != state.MachineID {
		t.Fatalf("listed node enrollments = %+v", listed)
	}

	if err := repo.DeleteNodeEnrollment(ctx, state.WorkspaceID, state.PoolName, state.MachineID); err != nil {
		t.Fatalf("DeleteNodeEnrollment() error = %v", err)
	}
	_, found, err = repo.GetNodeEnrollment(ctx, state.WorkspaceID, state.PoolName, state.MachineID)
	if err != nil || found {
		t.Fatalf("GetNodeEnrollment() after delete = found %v, err %v", found, err)
	}
}

func TestRedisRepositoryZone(t *testing.T) {
	rdb := newThunderRedisClient(t)
	defer rdb.Close()

	repo := NewRedisRepository(rdb)
	ctx := context.Background()
	state := &ZoneState{
		WorkspaceID:   "workspace-1",
		PoolName:      "pool-1",
		ThunderZoneID: "zone-1",
	}
	if err := repo.SaveZone(ctx, state); err != nil {
		t.Fatalf("SaveZone() error = %v", err)
	}

	got, found, err := repo.GetZone(ctx, state.WorkspaceID, state.PoolName)
	if err != nil || !found {
		t.Fatalf("GetZone() = found %v, err %v", found, err)
	}
	if got.ThunderZoneID != state.ThunderZoneID {
		t.Fatalf("zone = %+v", got)
	}

	listed, err := repo.ListZones(ctx, state.WorkspaceID)
	if err != nil {
		t.Fatalf("ListZones() error = %v", err)
	}
	if len(listed) != 1 || listed[0].PoolName != state.PoolName {
		t.Fatalf("listed zones = %+v", listed)
	}

	if err := repo.DeleteZone(ctx, state.WorkspaceID, state.PoolName); err != nil {
		t.Fatalf("DeleteZone() error = %v", err)
	}
	_, found, err = repo.GetZone(ctx, state.WorkspaceID, state.PoolName)
	if err != nil || found {
		t.Fatalf("GetZone() after delete = found %v, err %v", found, err)
	}
}

func newThunderRedisClient(t *testing.T) *common.RedisClient {
	t.Helper()
	server := miniredis.RunT(t)
	rdb, err := common.NewRedisClient(types.RedisConfig{Addrs: []string{server.Addr()}, Mode: types.RedisModeSingle})
	if err != nil {
		t.Fatal(err)
	}
	return rdb
}
