package gatewayservices

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
)

func TestConfigureDurableDiskPlacementPinsDevDiskToSingleStorageNode(t *testing.T) {
	workerRepo := newDurableDiskPlacementWorkerRepo(t)
	for _, worker := range []*types.Worker{
		{Id: "worker-b", MachineId: "node-b", Status: types.WorkerStatusAvailable, PoolName: "pool-a"},
		{Id: "worker-a", MachineId: "node-a", Status: types.WorkerStatusAvailable, PoolName: "pool-a"},
	} {
		require.NoError(t, workerRepo.AddWorker(worker))
	}

	config := &types.StubConfigV1{
		Pool: &types.PoolConfig{Name: "pool-a"},
		Disks: []*pb.DurableDisk{
			{
				Name:        "pg-data",
				Driver:      types.DurableDiskDriverDev,
				Replication: &pb.DiskReplication{},
			},
		},
	}

	gws := &GatewayService{workerRepo: workerRepo}
	require.NoError(t, gws.configureDurableDiskPlacement(context.Background(), nil, config))

	require.Equal(t, uint32(0), config.Disks[0].Replication.Replicas)
	require.Equal(t, "node-b", config.Disks[0].Replication.PrimaryWorkerId)
	require.Equal(t, []string{"node-b"}, config.Disks[0].Replication.ReplicaWorkerIds)
}

func TestConfigureDurableDiskPlacementReplacesMissingDevPrimary(t *testing.T) {
	workerRepo := newDurableDiskPlacementWorkerRepo(t)
	require.NoError(t, workerRepo.AddWorker(&types.Worker{
		Id:        "worker-b",
		MachineId: "node-b",
		Status:    types.WorkerStatusAvailable,
		PoolName:  "pool-a",
	}))

	config := &types.StubConfigV1{
		Pool: &types.PoolConfig{Name: "pool-a"},
		Disks: []*pb.DurableDisk{
			{
				Name:   "pg-data",
				Driver: types.DurableDiskDriverDev,
				Replication: &pb.DiskReplication{
					PrimaryWorkerId: "node-a",
				},
			},
		},
	}

	gws := &GatewayService{workerRepo: workerRepo}
	require.NoError(t, gws.configureDurableDiskPlacement(context.Background(), nil, config))

	require.Equal(t, "node-b", config.Disks[0].Replication.PrimaryWorkerId)
	require.Equal(t, []string{"node-b"}, config.Disks[0].Replication.ReplicaWorkerIds)
}

func TestConfigureDurableDiskPlacementAllowsDevDiskWithoutRegisteredNode(t *testing.T) {
	config := &types.StubConfigV1{
		Pool: &types.PoolConfig{Name: "pool-a"},
		Disks: []*pb.DurableDisk{
			{
				Name:   "pg-data",
				Driver: types.DurableDiskDriverDev,
				Replication: &pb.DiskReplication{
					PrimaryWorkerId:  "node-a",
					ReplicaWorkerIds: []string{"node-a"},
				},
			},
		},
	}

	gws := &GatewayService{workerRepo: newDurableDiskPlacementWorkerRepo(t)}
	require.NoError(t, gws.configureDurableDiskPlacement(context.Background(), nil, config))
	require.Empty(t, config.Disks[0].Replication.PrimaryWorkerId)
	require.Empty(t, config.Disks[0].Replication.ReplicaWorkerIds)
}

func TestConfigureDurableDiskPlacementRejectsUnsupportedDriver(t *testing.T) {
	config := &types.StubConfigV1{
		Disks: []*pb.DurableDisk{{
			Name:   "pg-data",
			Driver: "drbd",
		}},
	}

	err := (&GatewayService{}).configureDurableDiskPlacement(context.Background(), nil, config)
	require.ErrorContains(t, err, `unsupported driver "drbd"`)
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
