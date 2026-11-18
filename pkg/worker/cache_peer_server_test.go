package worker

import (
	"context"
	"net"
	"strings"
	"testing"

	"github.com/beam-cloud/beta9/pkg/cache"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type cacheRouteRegistrationClient struct {
	pb.WorkerRepositoryServiceClient
	requests []*pb.RegisterCacheHostRequest
}

func (c *cacheRouteRegistrationClient) RegisterCacheHost(_ context.Context, req *pb.RegisterCacheHostRequest, _ ...grpc.CallOption) (*pb.RegisterCacheHostResponse, error) {
	c.requests = append(c.requests, req)
	return &pb.RegisterCacheHostResponse{Ok: true}, nil
}

func TestCacheWorkerRPCTracksOwnershipAndExcludesOriginAccess(t *testing.T) {
	manager := &WorkerCacheManager{}
	worker := &Worker{persistent: true, machineID: "node", routeTransport: types.BackendRouteTransportTSNet}
	service := agentCacheRPCServer(worker, manager)
	require.NotNil(t, service)
	_, err := service.GetState(context.Background(), &pb.CacheGetStateRequest{})
	require.Equal(t, codes.Unavailable, status.Code(err))
	server, err := cache.NewServerWithOptions(context.Background(), cache.Config{
		Disk:   cache.DiskConfig{Enabled: true, MaxUsagePct: 100},
		Server: cache.ServerConfig{DiskCacheDir: t.TempDir(), DiskCacheMaxUsagePct: 100, PageSizeBytes: 4096},
	}, "test", cache.WithServerMetadataStore(cache.NewMockCacheMetadataStore()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, server.Close()) })
	manager.server = server
	state, err := service.GetState(context.Background(), &pb.CacheGetStateRequest{})
	require.NoError(t, err)
	require.Equal(t, cache.Version, state.Version)
	_, err = service.StoreContentFromSource(context.Background(), &pb.CacheStoreContentFromSourceRequest{})
	require.Equal(t, codes.Unimplemented, status.Code(err))
	_, err = service.StoreContentFromSourceWithLock(context.Background(), &pb.CacheStoreContentFromSourceRequest{})
	require.Equal(t, codes.Unimplemented, status.Code(err))
	manager.draining = true
	_, err = service.GetState(context.Background(), &pb.CacheGetStateRequest{})
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.Nil(t, agentCacheRPCServer(&Worker{}, manager), "cluster listeners retain their existing services")
}

func TestCacheWorkerRouteIsPublishedOnExistingLeaseAndRetained(t *testing.T) {
	client := &cacheRouteRegistrationClient{}
	manager := &WorkerCacheManager{workerRepo: client, workerID: "worker", poolName: "pool", locality: "workspace/pool", nodeID: "node"}
	registration := newGatewayCacheRegistration(manager, nil, cache.Config{}, "127.0.0.1:2050")
	manager.registration = registration
	route := &pb.BackendRoute{RouteId: "cache-route", LocalTarget: "127.0.0.1:12345"}
	require.NoError(t, manager.PublishWorkerRoute(context.Background(), route))
	require.Len(t, client.requests, 1)
	require.Equal(t, "cache-route", client.requests[0].WorkerRoute.RouteId)
	// Callers cannot mutate the capability after publication. A subsequent
	// cache-owner heartbeat carries the same immutable route.
	route.RouteId = "changed"
	require.NoError(t, registration.registerOnce(context.Background()))
	require.Len(t, client.requests, 2)
	require.Equal(t, "cache-route", client.requests[1].WorkerRoute.RouteId)
	require.Equal(t, "cache-route", manager.cacheWorkerRoute.RouteId)
}

func TestAgentWorkerRPCListenerIsLoopbackOnly(t *testing.T) {
	worker := &Worker{persistent: true, machineID: "node", routeTransport: types.BackendRouteTransportTSNet}
	listener, err := listenForContainerRuntimeServer(nil, agentRPCBindHost(worker))
	require.NoError(t, err)
	defer listener.Close()
	require.True(t, listener.Addr().(*net.TCPAddr).IP.IsLoopback(), "agent cache and worker RPCs must not bind public interfaces")
	for name, worker := range map[string]*Worker{
		"cluster":       {},
		"localdev":      {persistent: true, machineID: "node", routeTransport: types.BackendRouteTransportLocalDirect},
		"custom target": {persistent: true, machineID: "node", routeTransport: types.BackendRouteTransportTSNet, routeLocalTargetHost: "192.0.2.10"},
	} {
		t.Run(name, func(t *testing.T) {
			require.Empty(t, agentRPCBindHost(worker))
			require.Nil(t, agentCacheRPCServer(worker, &WorkerCacheManager{}), "an existing nonloopback listener must not gain a cache service")
		})
	}
	worker.routeLocalTargetHost = "127.0.0.2"
	require.Equal(t, "127.0.0.2", agentRPCBindHost(worker), "explicit loopback route targets stay consistent with the listener")
}

func TestCustomAgentTargetGetsCacheOnlyLoopbackListener(t *testing.T) {
	manager := &WorkerCacheManager{}
	address, err := manager.startCachePeerListener()
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(address, "127.0.0.1:"))
	t.Cleanup(func() { manager.Drain() })
	conn, err := grpc.NewClient("passthrough:///"+address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()
	_, err = pb.NewCacheClient(conn).GetState(context.Background(), &pb.CacheGetStateRequest{})
	require.Equal(t, codes.Unavailable, status.Code(err), "only a live cache owner serves cache requests")
	err = conn.Invoke(context.Background(), "/ContainerService/ContainerKill", &pb.CacheGetStateRequest{}, &pb.CacheGetStateResponse{})
	// An arbitrary method cannot be tunnelled through this cache-only socket.
	require.Equal(t, codes.Unimplemented, status.Code(err))
	manager.Drain()
	_, err = manager.startCachePeerListener()
	require.Equal(t, codes.Unavailable, status.Code(err))
}
