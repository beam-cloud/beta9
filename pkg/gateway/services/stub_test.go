package gatewayservices

import (
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
)

func TestConfigureDurableDiskPlacementFillsDRBDReplicaWorkers(t *testing.T) {
	workerRepo := newDurableDiskPlacementWorkerRepo(t)
	for _, worker := range []*types.Worker{
		{Id: "worker-c", Status: types.WorkerStatusAvailable, PoolName: "pool-a"},
		{Id: "worker-a", Status: types.WorkerStatusAvailable, PoolName: "pool-a"},
		{Id: "worker-b", Status: types.WorkerStatusAvailable, PoolName: "pool-a"},
		{Id: "worker-disabled", Status: types.WorkerStatusDisabled, PoolName: "pool-a"},
		{Id: "worker-other", Status: types.WorkerStatusAvailable, PoolName: "pool-b"},
	} {
		require.NoError(t, workerRepo.AddWorker(worker))
	}

	config := &types.StubConfigV1{
		Pool: &types.PoolConfig{Name: "pool-a"},
		Disks: []*pb.DurableDisk{
			{
				Name:   "pg-data",
				Driver: "drbd",
				Replication: &pb.DiskReplication{
					Replicas: 3,
				},
			},
		},
	}

	gws := &GatewayService{workerRepo: workerRepo}
	require.NoError(t, gws.configureDurableDiskPlacement(config))

	require.Equal(t, "worker-a", config.Disks[0].Replication.PrimaryWorkerId)
	require.Equal(t, []string{"worker-a", "worker-b", "worker-c"}, config.Disks[0].Replication.ReplicaWorkerIds)
	require.Equal(t, "sync", config.Disks[0].Replication.Mode)
	require.Equal(t, "majority", config.Disks[0].Replication.Quorum)
}

func TestConfigureDurableDiskPlacementFailsClosedWithoutQuorumWorkers(t *testing.T) {
	workerRepo := newDurableDiskPlacementWorkerRepo(t)
	require.NoError(t, workerRepo.AddWorker(&types.Worker{Id: "worker-a", Status: types.WorkerStatusAvailable, PoolName: "pool-a"}))

	config := &types.StubConfigV1{
		Pool: &types.PoolConfig{Name: "pool-a"},
		Disks: []*pb.DurableDisk{
			{
				Name:   "pg-data",
				Driver: "drbd",
				Replication: &pb.DiskReplication{
					Replicas: 3,
				},
			},
		},
	}

	gws := &GatewayService{workerRepo: workerRepo}
	err := gws.configureDurableDiskPlacement(config)
	require.Error(t, err)
	require.Contains(t, err.Error(), "needs 3 DRBD replica workers")
}

func TestConfigureDurableDiskPlacementPreservesExplicitPrimary(t *testing.T) {
	workerRepo := newDurableDiskPlacementWorkerRepo(t)
	for _, worker := range []*types.Worker{
		{Id: "worker-a", Status: types.WorkerStatusAvailable, PoolName: "pool-a"},
		{Id: "worker-b", Status: types.WorkerStatusAvailable, PoolName: "pool-a"},
		{Id: "worker-c", Status: types.WorkerStatusAvailable, PoolName: "pool-a"},
	} {
		require.NoError(t, workerRepo.AddWorker(worker))
	}

	config := &types.StubConfigV1{
		Pool: &types.PoolConfig{Name: "pool-a"},
		Disks: []*pb.DurableDisk{
			{
				Name:   "pg-data",
				Driver: "drbd",
				Replication: &pb.DiskReplication{
					Replicas:        3,
					PrimaryWorkerId: "worker-c",
				},
			},
		},
	}

	gws := &GatewayService{workerRepo: workerRepo}
	require.NoError(t, gws.configureDurableDiskPlacement(config))

	require.Equal(t, "worker-c", config.Disks[0].Replication.PrimaryWorkerId)
	require.Equal(t, []string{"worker-c", "worker-a", "worker-b"}, config.Disks[0].Replication.ReplicaWorkerIds)
}

func newDurableDiskPlacementWorkerRepo(t *testing.T) repository.WorkerRepository {
	t.Helper()

	server, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(server.Close)

	rdb, err := common.NewRedisClient(types.RedisConfig{
		Addrs: []string{server.Addr()},
		Mode:  types.RedisModeSingle,
	})
	require.NoError(t, err)

	return repository.NewWorkerRedisRepository(rdb, types.WorkerConfig{
		CleanupPendingWorkerAgeLimit: 10 * time.Minute,
	})
}
