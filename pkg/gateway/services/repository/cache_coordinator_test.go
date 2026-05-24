package repository_services

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
)

func newCacheCoordinatorTestService(t *testing.T) *WorkerRepositoryService {
	t.Helper()

	server, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(server.Close)

	rdb, err := common.NewRedisClient(types.RedisConfig{
		Addrs: []string{server.Addr()},
		Mode:  types.RedisModeSingle,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = rdb.Close() })

	return NewWorkerRepositoryService(context.Background(), nil, rdb)
}

func TestCacheCoordinatorDeduplicatesLogicalHostsAndPromotesRegistrations(t *testing.T) {
	ctx := context.Background()
	service := newCacheCoordinatorTestService(t)

	logicalHostID := "cache-host-default-node-a-path-0"
	register := func(registrationID, addr string) {
		resp, err := service.RegisterCacheHost(ctx, &pb.RegisterCacheHostRequest{
			TtlSeconds: 30,
			Host: &pb.CacheCoordinatorHost{
				LogicalHostId:  logicalHostID,
				RegistrationId: registrationID,
				PoolName:       "default",
				Locality:       "default",
				NodeId:         "node-a",
				CachePathId:    "path",
				Slot:           0,
				Addr:           addr,
				PrivateAddr:    addr,
			},
		})
		require.NoError(t, err)
		require.True(t, resp.Ok, resp.ErrorMsg)
	}

	register("worker-a", "10.0.0.1:2049")
	register("worker-b", "10.0.0.2:2049")

	listResp, err := service.ListCacheHosts(ctx, &pb.ListCacheHostsRequest{PoolName: "default", Locality: "default"})
	require.NoError(t, err)
	require.True(t, listResp.Ok, listResp.ErrorMsg)
	require.Len(t, listResp.Hosts, 1)
	require.Equal(t, logicalHostID, listResp.Hosts[0].LogicalHostId)
	require.Equal(t, "worker-a", listResp.Hosts[0].RegistrationId)

	unregisterResp, err := service.UnregisterCacheHost(ctx, &pb.UnregisterCacheHostRequest{
		LogicalHostId:  logicalHostID,
		RegistrationId: "worker-a",
		PoolName:       "default",
		Locality:       "default",
	})
	require.NoError(t, err)
	require.True(t, unregisterResp.Ok, unregisterResp.ErrorMsg)

	listResp, err = service.ListCacheHosts(ctx, &pb.ListCacheHostsRequest{PoolName: "default", Locality: "default"})
	require.NoError(t, err)
	require.True(t, listResp.Ok, listResp.ErrorMsg)
	require.Len(t, listResp.Hosts, 1)
	require.Equal(t, logicalHostID, listResp.Hosts[0].LogicalHostId)
	require.Equal(t, "worker-b", listResp.Hosts[0].RegistrationId)
	require.Equal(t, "10.0.0.2:2049", listResp.Hosts[0].PrivateAddr)
}
