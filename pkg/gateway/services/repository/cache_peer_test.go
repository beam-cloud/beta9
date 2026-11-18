package repository_services

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/cache"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type cachePeerWorkers struct {
	repository.WorkerRepository
	workers map[string]*types.Worker
}

func (r *cachePeerWorkers) GetWorkerById(id string) (*types.Worker, error) {
	worker := r.workers[id]
	if worker == nil {
		return nil, errors.New("worker not found")
	}
	return worker, nil
}

type cachePeerFixture struct {
	service  *WorkerRepositoryService
	client   pb.WorkerRepositoryServiceClient
	ctx      context.Context
	requests []*pb.RegisterCacheHostRequest
	peers    []pb.CacheClient
	dials    atomic.Int64
	redis    *miniredis.Miniredis
}

func cachePeerAuth(worker string) context.Context {
	return auth.ContextWithAuthInfo(context.Background(), &auth.AuthInfo{
		Token:     &types.Token{TokenType: types.TokenTypeWorkerPrivate, WorkerID: worker},
		Workspace: &types.Workspace{ExternalId: "own"},
	})
}

func newCachePeerFixture(t *testing.T) *cachePeerFixture {
	t.Helper()
	f := &cachePeerFixture{redis: miniredis.RunT(t), ctx: repositoryWorkerAuthContext(types.TokenTypeWorkerPrivate, "worker-a")}
	rdb, err := common.NewRedisClient(types.RedisConfig{Addrs: []string{f.redis.Addr()}, Mode: types.RedisModeSingle})
	require.NoError(t, err)
	t.Cleanup(func() { rdb.Close() })
	f.service = &WorkerRepositoryService{
		workerRepo: &cachePeerWorkers{workers: map[string]*types.Worker{
			"worker-a": {Id: "worker-a", WorkspaceId: "own", PoolName: "pool", MachineId: "node-a"},
			"worker-b": {Id: "worker-b", WorkspaceId: "own", PoolName: "pool", MachineId: "node-b"},
		}},
		containerRepo:    repository.NewContainerRedisRepository(rdb),
		cacheCoordinator: cache.NewCoordinator(repository.NewCacheRedisRepository(rdb)),
	}
	addresses := map[string]string{}
	for i, worker := range []string{"worker-a", "worker-b"} {
		cfg := cache.Config{
			Disk:   cache.DiskConfig{Enabled: true, MaxUsagePct: 100},
			Server: cache.ServerConfig{DiskCacheDir: t.TempDir(), DiskCacheMaxUsagePct: 100, PageSizeBytes: 64 << 10, ObjectTtlS: 300},
			Global: cache.GlobalConfig{GRPCMessageSizeBytes: 4 << 20, GRPCDialTimeoutS: 1},
		}
		server, err := cache.NewServerWithOptions(context.Background(), cfg, "own/pool", cache.WithServerMetadataStore(cache.NewMockCacheMetadataStore()))
		require.NoError(t, err)
		address, err := server.Serve("127.0.0.1:0", "")
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, server.Close()) })
		conn, err := grpc.NewClient("passthrough:///"+address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		require.NoError(t, err)
		t.Cleanup(func() { conn.Close() })
		f.peers = append(f.peers, pb.NewCacheClient(conn))
		node := fmt.Sprintf("node-%c", 'a'+i)
		path := fmt.Sprintf("%012x", i+1)
		routeID := types.BackendRouteID(node, worker, "", types.BackendRouteKindCache, 0)
		addresses[routeID] = address
		req := &pb.RegisterCacheHostRequest{
			Host:        &pb.CacheCoordinatorHost{LogicalHostId: "cache-host-own-pool-" + node + "-" + path, RegistrationId: worker + "-lease", PoolName: "pool", Locality: "own/pool", NodeId: node, CachePathId: path, Addr: "192.0.2.1:2050", PrivateAddr: "10.1.0.2:2050"},
			TtlSeconds:  30,
			WorkerRoute: &pb.BackendRoute{RouteId: routeID, WorkerId: worker, MachineId: node, PoolName: "pool", Kind: types.BackendRouteKindCache, Protocol: types.BackendRouteProtocolTCP, Transport: types.BackendRouteTransportTSNet, LocalTarget: address},
		}
		response, err := f.service.RegisterCacheHost(cachePeerAuth(worker), req)
		require.NoError(t, err)
		require.True(t, response.Ok, response.ErrorMsg)
		f.requests = append(f.requests, req)
	}
	f.service.SetCachePeerDialer(func(ctx context.Context, address string) (net.Conn, error) {
		f.dials.Add(1)
		routeID, ok := types.ParseBackendRouteAddress(address)
		if !ok || addresses[routeID] == "" {
			return nil, fmt.Errorf("unexpected route %q", address)
		}
		return (&net.Dialer{}).DialContext(ctx, "tcp", addresses[routeID])
	})
	f.client = pb.NewWorkerRepositoryServiceClient(repositoryAuthClient(t, f.service, &ContainerRepositoryService{}))
	return f
}

func (f *cachePeerFixture) target(index int) *pb.CachePeerTarget {
	host := f.requests[index].Host
	return &pb.CachePeerTarget{WorkerId: "worker-a", LogicalHostId: host.LogicalHostId, RegistrationId: host.RegistrationId, PoolName: host.PoolName, Locality: host.Locality}
}

func cachePeerDigest(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func storeCachePeer(t *testing.T, f *cachePeerFixture, index int, body []byte) string {
	t.Helper()
	stream, err := f.client.StoreCachePeer(f.ctx)
	require.NoError(t, err)
	target, hash := f.target(index), cachePeerDigest(body)
	require.NoError(t, stream.Send(&pb.CachePeerStoreRequest{Target: target, ExpectedSize: int64(len(body)), ExpectedHash: hash}))
	for offset := 0; offset < len(body); {
		end := min(offset+cachePeerChunkBytes, len(body))
		require.NoError(t, stream.Send(&pb.CachePeerStoreRequest{Target: target, Content: &pb.CacheStoreContentRequest{Content: body[offset:end]}}))
		offset = end
	}
	require.NoError(t, stream.Send(&pb.CachePeerStoreRequest{Target: target, Finish: true}))
	response, err := stream.CloseAndRecv()
	require.NoError(t, err)
	require.True(t, response.Ok)
	require.Equal(t, hash, response.Hash)
	return hash
}

func readCachePeer(t *testing.T, f *cachePeerFixture, index int, hash string, size int64) []byte {
	t.Helper()
	stream, err := f.client.ReadCachePeer(f.ctx, &pb.CachePeerReadRequest{Target: f.target(index), Hash: hash, Length: size})
	require.NoError(t, err)
	var body []byte
	for {
		frame, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		require.True(t, frame.Ok)
		require.LessOrEqual(t, len(frame.Content), cachePeerChunkBytes)
		body = append(body, frame.Content...)
	}
	return body
}

func TestCachePeerRelayTransfersBetweenTwoRealCaches(t *testing.T) {
	f := newCachePeerFixture(t)
	body := bytes.Repeat([]byte("peer-content-"), 180000)
	hash := storeCachePeer(t, f, 1, body)
	state, err := f.client.InspectCachePeer(f.ctx, &pb.CachePeerInspectRequest{Target: f.target(1), Request: &pb.CachePeerInspectRequest_State{State: &pb.CacheGetStateRequest{}}})
	require.NoError(t, err)
	require.Equal(t, cache.Version, state.GetState().Version)
	missing, err := f.peers[0].HasContent(context.Background(), &pb.CacheHasContentRequest{Hash: hash})
	require.NoError(t, err)
	require.False(t, missing.Exists, "the other cache has not received the object")
	got := readCachePeer(t, f, 1, hash, int64(len(body)))
	require.Equal(t, body, got)
	require.Equal(t, hash, storeCachePeer(t, f, 0, got))
	require.Equal(t, body, readCachePeer(t, f, 0, hash, 0), "whole-object streaming retains Cache semantics")
	has, err := f.client.InspectCachePeer(f.ctx, &pb.CachePeerInspectRequest{Target: f.target(0), Request: &pb.CachePeerInspectRequest_Has{Has: &pb.CacheHasContentRequest{Hash: hash, ExpectedSize: int64(len(body))}}})
	require.NoError(t, err)
	require.True(t, has.GetHas().Exists)
	// The gateway exposes typed repository operations, never a Cache or
	// ContainerService tunnel selected by a worker-supplied method name.
	conn := repositoryAuthClient(t, f.service, &ContainerRepositoryService{})
	_, err = pb.NewCacheClient(conn).GetState(f.ctx, &pb.CacheGetStateRequest{})
	require.Equal(t, codes.Unimplemented, status.Code(err))
}

func TestCachePeerRelayRejectsScopeAndStaleLeasesBeforeDial(t *testing.T) {
	f := newCachePeerFixture(t)
	for name, alter := range map[string]func(*pb.CachePeerTarget){
		"worker":    func(target *pb.CachePeerTarget) { target.WorkerId = "worker-b" },
		"pool":      func(target *pb.CachePeerTarget) { target.PoolName = "other" },
		"workspace": func(target *pb.CachePeerTarget) { target.Locality = "other/pool" },
		"host":      func(target *pb.CachePeerTarget) { target.LogicalHostId = "other" },
		"lease":     func(target *pb.CachePeerTarget) { target.RegistrationId = "old" },
	} {
		t.Run(name, func(t *testing.T) {
			target := f.target(1)
			alter(target)
			_, err := f.client.InspectCachePeer(f.ctx, &pb.CachePeerInspectRequest{Target: target, Request: &pb.CachePeerInspectRequest_State{State: &pb.CacheGetStateRequest{}}})
			require.Contains(t, []codes.Code{codes.PermissionDenied, codes.FailedPrecondition}, status.Code(err))
		})
	}
	newLease := proto.Clone(f.requests[1]).(*pb.RegisterCacheHostRequest)
	newLease.Host.RegistrationId = "new-lease"
	response, err := f.service.RegisterCacheHost(cachePeerAuth("worker-b"), newLease)
	require.NoError(t, err)
	require.True(t, response.Ok, response.ErrorMsg)
	_, err = f.client.InspectCachePeer(f.ctx, &pb.CachePeerInspectRequest{Target: f.target(1), Request: &pb.CachePeerInspectRequest_State{State: &pb.CacheGetStateRequest{}}})
	require.Equal(t, codes.FailedPrecondition, status.Code(err), "a still-live superseded process lease is rejected")
	f.redis.FastForward(31 * time.Second)
	_, err = f.client.InspectCachePeer(f.ctx, &pb.CachePeerInspectRequest{Target: f.target(0), Request: &pb.CachePeerInspectRequest_State{State: &pb.CacheGetStateRequest{}}})
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Zero(t, f.dials.Load())
}

func TestCachePeerRegistrationRejectsSpoofedCapabilities(t *testing.T) {
	f := newCachePeerFixture(t)
	for name, alter := range map[string]func(*pb.RegisterCacheHostRequest){
		"node":              func(req *pb.RegisterCacheHostRequest) { req.Host.NodeId = "node-a" },
		"logical host":      func(req *pb.RegisterCacheHostRequest) { req.Host.LogicalHostId = f.requests[0].Host.LogicalHostId },
		"container service": func(req *pb.RegisterCacheHostRequest) { req.WorkerRoute.Kind = types.BackendRouteKindContainer },
		"public socket":     func(req *pb.RegisterCacheHostRequest) { req.WorkerRoute.LocalTarget = "203.0.113.1:2050" },
		"workspace":         func(req *pb.RegisterCacheHostRequest) { req.WorkerRoute.WorkspaceId = "other" },
	} {
		t.Run(name, func(t *testing.T) {
			req := proto.Clone(f.requests[1]).(*pb.RegisterCacheHostRequest)
			alter(req)
			response, err := f.service.RegisterCacheHost(cachePeerAuth("worker-b"), req)
			require.NoError(t, err)
			require.False(t, response.Ok)
		})
	}
	// A legacy registration cannot inject a peer route ID.
	req := proto.Clone(f.requests[1]).(*pb.RegisterCacheHostRequest)
	req.WorkerRoute = nil
	req.Host.CacheRouteId = f.requests[0].WorkerRoute.RouteId
	response, err := f.service.RegisterCacheHost(cachePeerAuth("worker-b"), req)
	require.NoError(t, err)
	require.True(t, response.Ok, response.ErrorMsg)
	hosts, err := f.service.cacheCoordinator.ListHosts(context.Background(), "pool", "own/pool")
	require.NoError(t, err)
	for _, host := range hosts {
		if host.NodeID == "node-b" {
			require.Empty(t, host.CacheRouteID)
		}
	}
}

func TestCachePeerRegistrationPreservesReadyRouteUntilListenerChanges(t *testing.T) {
	f := newCachePeerFixture(t)
	request := f.requests[1]
	route, err := f.service.containerRepo.GetBackendRoute(context.Background(), request.WorkerRoute.RouteId)
	require.NoError(t, err)
	route.State, route.ProxyTarget = types.BackendRouteStateReady, "100.64.0.1:29443"
	require.NoError(t, f.service.containerRepo.SetBackendRoute(context.Background(), *route))
	response, err := f.service.RegisterCacheHost(cachePeerAuth("worker-b"), request)
	require.NoError(t, err)
	require.True(t, response.Ok, response.ErrorMsg)
	got, err := f.service.containerRepo.GetBackendRoute(context.Background(), route.RouteID)
	require.NoError(t, err)
	require.Equal(t, types.BackendRouteStateReady, got.State)
	require.Equal(t, route.ProxyTarget, got.ProxyTarget)
	changed := proto.Clone(request).(*pb.RegisterCacheHostRequest)
	changed.WorkerRoute.LocalTarget = "127.0.0.1:54321"
	response, err = f.service.RegisterCacheHost(cachePeerAuth("worker-b"), changed)
	require.NoError(t, err)
	require.True(t, response.Ok, response.ErrorMsg)
	got, err = f.service.containerRepo.GetBackendRoute(context.Background(), route.RouteID)
	require.NoError(t, err)
	require.Equal(t, types.BackendRouteStateOpening, got.State)
	require.Empty(t, got.ProxyTarget)
}

func TestCachePeerRegistrationAcceptsScopedAndLegacyConfigGroupIdentity(t *testing.T) {
	f := newCachePeerFixture(t)
	f.service.appConfig.Worker.Pools = map[string]types.WorkerPoolConfig{"pool": {ConfigGroup: "gpu-group"}}
	for _, locality := range []string{"gpu-group", "own/gpu-group"} {
		req := proto.Clone(f.requests[1]).(*pb.RegisterCacheHostRequest)
		req.Host.Locality = locality
		req.Host.LogicalHostId = "cache-host-" + strings.ReplaceAll(locality, "/", "-") + "-node-b-" + req.Host.CachePathId
		response, err := f.service.RegisterCacheHost(cachePeerAuth("worker-b"), req)
		require.NoError(t, err)
		require.True(t, response.Ok, response.ErrorMsg)
		target := &pb.CachePeerTarget{WorkerId: "worker-a", LogicalHostId: req.Host.LogicalHostId, RegistrationId: req.Host.RegistrationId, PoolName: "pool", Locality: "own/gpu-group"}
		_, err = f.service.authorizeCachePeer(cachePeerAuth("worker-a"), target)
		require.NoError(t, err)
	}
}

func TestCachePeerRejectsRouteWhoseWorkerMovedWorkspaces(t *testing.T) {
	f := newCachePeerFixture(t)
	workers := f.service.workerRepo.(*cachePeerWorkers)
	workers.workers["worker-b"] = &types.Worker{Id: "worker-b", WorkspaceId: "other", PoolName: "pool", MachineId: "node-b"}
	_, err := f.client.InspectCachePeer(f.ctx, &pb.CachePeerInspectRequest{Target: f.target(1), Request: &pb.CachePeerInspectRequest_State{State: &pb.CacheGetStateRequest{}}})
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	require.Zero(t, f.dials.Load())
}

func TestCachePeerIncompleteStoresNeverPublishContent(t *testing.T) {
	f := newCachePeerFixture(t)
	for _, failure := range []string{"missing finish", "short", "checksum", "changed target", "canceled"} {
		t.Run(failure, func(t *testing.T) {
			body := []byte("unpublished-" + failure)
			ctx, cancel := context.WithCancel(f.ctx)
			defer cancel()
			stream, err := f.client.StoreCachePeer(ctx)
			require.NoError(t, err)
			target := f.target(1)
			size := int64(len(body))
			hash := cachePeerDigest(body)
			if failure == "short" {
				size++
			}
			expectedHash := hash
			if failure == "checksum" {
				expectedHash = strings.Repeat("0", 64)
			}
			require.NoError(t, stream.Send(&pb.CachePeerStoreRequest{Target: target, ExpectedSize: size, ExpectedHash: expectedHash}))
			require.NoError(t, stream.Send(&pb.CachePeerStoreRequest{Target: target, Content: &pb.CacheStoreContentRequest{Content: body}}))
			if failure == "canceled" {
				cancel()
			} else if failure != "missing finish" {
				if failure == "changed target" {
					target = f.target(0)
				}
				require.NoError(t, stream.Send(&pb.CachePeerStoreRequest{Target: target, Finish: true}))
			}
			_, err = stream.CloseAndRecv()
			require.Error(t, err)
			require.Never(t, func() bool {
				has, err := f.peers[1].HasContent(context.Background(), &pb.CacheHasContentRequest{Hash: hash})
				return err != nil || has.Exists
			}, 100*time.Millisecond, 10*time.Millisecond)
		})
	}
}

func TestCachePeerAdmissionCancellationReleasesSlots(t *testing.T) {
	var transport cachePeerTransport
	var releases []func()
	for i := 0; i < 8; i++ {
		release, err := transport.acquire(context.Background(), "worker", "host")
		require.NoError(t, err)
		releases = append(releases, release)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	_, err := transport.acquire(ctx, "worker", "host")
	require.ErrorIs(t, err, context.DeadlineExceeded)
	for _, release := range releases {
		release()
	}
	require.Empty(t, transport.workers)
	require.Empty(t, transport.hosts)
	require.Empty(t, transport.global)
}

func TestCachePeerRechecksLeaseAfterWaitingForAdmission(t *testing.T) {
	f := newCachePeerFixture(t)
	var releases []func()
	for i := 0; i < 8; i++ {
		release, err := f.service.cachePeer.acquire(context.Background(), "worker-a", f.target(1).LogicalHostId)
		require.NoError(t, err)
		releases = append(releases, release)
	}
	done := make(chan error, 1)
	ctx, cancel := context.WithTimeout(f.ctx, 5*time.Second)
	defer cancel()
	go func() {
		_, err := f.client.InspectCachePeer(ctx, &pb.CachePeerInspectRequest{Target: f.target(1), Request: &pb.CachePeerInspectRequest_State{State: &pb.CacheGetStateRequest{}}})
		done <- err
	}()
	require.Eventually(t, func() bool { return len(f.service.cachePeer.global) == 9 }, time.Second, time.Millisecond)
	newLease := proto.Clone(f.requests[1]).(*pb.RegisterCacheHostRequest)
	newLease.Host.RegistrationId = "replacement-while-queued"
	response, err := f.service.RegisterCacheHost(cachePeerAuth("worker-b"), newLease)
	require.NoError(t, err)
	require.True(t, response.Ok, response.ErrorMsg)
	releases[0]()
	select {
	case err := <-done:
		require.Equal(t, codes.FailedPrecondition, status.Code(err))
	case <-ctx.Done():
		t.Fatal("queued cache request did not finish")
	}
	for _, release := range releases[1:] {
		release()
	}
	require.Zero(t, f.dials.Load())
}

type blockingCachePeerStore struct {
	grpc.ServerStream
	ctx     context.Context
	frames  []*pb.CachePeerStoreRequest
	entered chan struct{}
	release chan struct{}
}

func (s *blockingCachePeerStore) Context() context.Context { return s.ctx }
func (s *blockingCachePeerStore) Recv() (*pb.CachePeerStoreRequest, error) {
	if len(s.frames) > 0 {
		frame := s.frames[0]
		s.frames = s.frames[1:]
		return frame, nil
	}
	close(s.entered)
	<-s.release
	return nil, io.EOF
}
func (s *blockingCachePeerStore) SendAndClose(*pb.CacheStoreContentResponse) error { return nil }

func TestCachePeerStoreTimeoutBoundsHeaderBodyAndFinishEOF(t *testing.T) {
	for _, phase := range []string{"header", "body", "finish EOF"} {
		t.Run(phase, func(t *testing.T) {
			f := newCachePeerFixture(t)
			f.service.cachePeer.operationTimeout = 150 * time.Millisecond
			body := []byte("timeout-must-not-publish-" + phase)
			s := &blockingCachePeerStore{ctx: cachePeerAuth("worker-a"), entered: make(chan struct{}), release: make(chan struct{})}
			if phase != "header" {
				s.frames = append(s.frames, &pb.CachePeerStoreRequest{Target: f.target(1), ExpectedSize: int64(len(body)), ExpectedHash: cachePeerDigest(body), Content: &pb.CacheStoreContentRequest{Content: body}})
			}
			if phase == "finish EOF" {
				s.frames = append(s.frames, &pb.CachePeerStoreRequest{Target: f.target(1), Finish: true})
			}
			done := make(chan error, 1)
			go func() { done <- f.service.StoreCachePeer(s) }()
			select {
			case <-s.entered:
			case <-time.After(time.Second):
				t.Fatal("store did not reach blocked stream receive")
			}
			select {
			case err := <-done:
				require.Equal(t, codes.DeadlineExceeded, status.Code(err))
			case <-time.After(time.Second):
				close(s.release)
				t.Fatal("store outlived its server deadline")
			}
			// A real gRPC handler return cancels its original stream. Release
			// the fake operation explicitly so its single goroutine also exits.
			close(s.release)
			require.Zero(t, len(f.service.cachePeer.global), "timed out handlers release admission")
			require.Never(t, func() bool {
				has, err := f.peers[1].HasContent(context.Background(), &pb.CacheHasContentRequest{Hash: cachePeerDigest(body)})
				return err != nil || has.Exists
			}, 100*time.Millisecond, 10*time.Millisecond)
		})
	}
}

type blockingCachePeerRead struct {
	grpc.ServerStream
	ctx     context.Context
	entered chan struct{}
	release chan struct{}
}

func (s *blockingCachePeerRead) Context() context.Context { return s.ctx }
func (s *blockingCachePeerRead) Send(*pb.CacheGetContentResponse) error {
	close(s.entered)
	<-s.release
	return nil
}

func TestCachePeerReadTimeoutBoundsSlowConsumerAndReleasesAdmission(t *testing.T) {
	f := newCachePeerFixture(t)
	body := []byte("blocked-cache-reader")
	hash := storeCachePeer(t, f, 1, body)
	f.service.cachePeer.operationTimeout = 150 * time.Millisecond
	s := &blockingCachePeerRead{ctx: cachePeerAuth("worker-a"), entered: make(chan struct{}), release: make(chan struct{})}
	done := make(chan error, 1)
	go func() {
		done <- f.service.ReadCachePeer(&pb.CachePeerReadRequest{Target: f.target(1), Hash: hash, Length: int64(len(body))}, s)
	}()
	select {
	case <-s.entered:
	case <-time.After(time.Second):
		t.Fatal("read did not reach blocked stream send")
	}
	select {
	case err := <-done:
		require.Equal(t, codes.DeadlineExceeded, status.Code(err))
	case <-time.After(time.Second):
		close(s.release)
		t.Fatal("read outlived its server deadline")
	}
	close(s.release)
	require.Zero(t, len(f.service.cachePeer.global))
}

func TestCachePeerRejectsMutatedProtocolOrLocalTargetBeforeDial(t *testing.T) {
	for _, mutation := range []string{"protocol", "public target", "invalid port"} {
		t.Run(mutation, func(t *testing.T) {
			f := newCachePeerFixture(t)
			route, err := f.service.containerRepo.GetBackendRoute(context.Background(), f.requests[1].WorkerRoute.RouteId)
			require.NoError(t, err)
			switch mutation {
			case "protocol":
				route.Protocol = "udp"
			case "public target":
				route.LocalTarget = "203.0.113.1:2050"
			case "invalid port":
				route.LocalTarget = "127.0.0.1:0"
			}
			require.NoError(t, f.service.containerRepo.SetBackendRoute(context.Background(), *route))
			_, err = f.client.InspectCachePeer(f.ctx, &pb.CachePeerInspectRequest{Target: f.target(1), Request: &pb.CachePeerInspectRequest_State{State: &pb.CacheGetStateRequest{}}})
			require.Equal(t, codes.PermissionDenied, status.Code(err))
			require.Zero(t, f.dials.Load())
		})
	}
}
