package repository_services

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"math"
	"net"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/cache"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	cachePeerChunkBytes    = 1 << 20
	cachePeerMaxReadBytes  = 64 << 20
	cachePeerMaxStoreBytes = 64 << 30
	cachePeerTimeout       = 5 * time.Minute
)

type cachePeerTransport struct {
	dial func(context.Context, string) (net.Conn, error)
	// An override is used by bounded-I/O regression tests. Production uses
	// cachePeerTimeout regardless of whether the caller supplied a deadline.
	operationTimeout time.Duration
	mu               sync.Mutex
	global           chan struct{}
	workers          map[string]*cachePeerSlots
	hosts            map[string]*cachePeerSlots
}

type cachePeerSlots struct {
	slots chan struct{}
	refs  int
}

// SetCachePeerDialer supplies the gateway's existing authenticated agent route
// transport. Requests never supply a socket address or arbitrary RPC method.
func (s *WorkerRepositoryService) SetCachePeerDialer(dial func(context.Context, string) (net.Conn, error)) {
	s.cachePeer.dial = dial
}

func cachePeerHash(hash string) bool {
	if len(hash) != 64 {
		return false
	}
	_, err := hex.DecodeString(hash)
	return err == nil && hash == strings.ToLower(hash)
}

var cachePeerNamePattern = regexp.MustCompile(`[^a-zA-Z0-9_.-]+`)

func cachePeerLoopbackTarget(target string) bool {
	host, port, err := net.SplitHostPort(target)
	if err != nil {
		return false
	}
	number, err := strconv.Atoi(port)
	ip := net.ParseIP(host)
	return err == nil && number > 0 && number <= 65535 && ip != nil && ip.IsLoopback()
}

func (p *cachePeerTransport) operationContext(parent context.Context) (context.Context, context.CancelFunc) {
	timeout := p.operationTimeout
	if timeout <= 0 {
		timeout = cachePeerTimeout
	}
	return context.WithTimeout(parent, timeout)
}

// Server-stream I/O is tied to the original RPC context, not a derived timeout.
// Bound one pending operation explicitly. Returning the handler cancels the
// original gRPC stream, which releases the operation goroutine; its buffered
// result never waits for a handler that already returned.
func cachePeerIO[T any](ctx context.Context, operation func() (T, error)) (T, error) {
	type result struct {
		value T
		err   error
	}
	var zero T
	if err := ctx.Err(); err != nil {
		return zero, status.FromContextError(err).Err()
	}
	done := make(chan result, 1)
	go func() { value, err := operation(); done <- result{value: value, err: err} }()
	select {
	case <-ctx.Done():
		return zero, status.FromContextError(ctx.Err()).Err()
	case got := <-done:
		return got.value, got.err
	}
}

func cachePeerSend(ctx context.Context, send func() error) error {
	_, err := cachePeerIO(ctx, func() (struct{}, error) { return struct{}{}, send() })
	return err
}

func (s *WorkerRepositoryService) registerCachePeerRoute(ctx context.Context, host *cache.CoordinatorHost, in *pb.BackendRoute) error {
	caller, worker, locality, err := s.cachePeerWorker(ctx)
	if err != nil {
		return err
	}
	if s.containerRepo == nil {
		return status.Error(codes.Unavailable, "cache routes are unavailable")
	}
	pathBytes, pathErr := hex.DecodeString(host.CachePathID)
	logicalID := func(locality string) string {
		return "cache-host-" + cachePeerNamePattern.ReplaceAllString(locality, "-") + "-" + cachePeerNamePattern.ReplaceAllString(worker.MachineId, "-") + "-" + host.CachePathID
	}
	// Config-group agents historically use the unscoped group in their local
	// cache identity; the coordinator scopes its locality on registration.
	// Both identities remain bound to this assigned machine and scoped pool.
	legacyGroup := s.appConfig.Worker.Pools[worker.PoolName].ConfigGroup
	validLogicalID := host.LogicalHostID == logicalID(locality) || (legacyGroup != "" && host.LogicalHostID == logicalID(legacyGroup))
	if host.PoolName != worker.PoolName || host.NodeID != worker.MachineId || host.Locality != locality || len(pathBytes) != 6 || pathErr != nil || !validLogicalID || host.RegistrationID == "" {
		return status.Error(codes.PermissionDenied, "cache registration does not match its worker")
	}
	route := backendRouteFromProto(in)
	if route.WorkerID != caller.ID || route.MachineID != worker.MachineId || route.PoolName != worker.PoolName || (route.WorkspaceID != "" && route.WorkspaceID != caller.Workspace.ExternalId) || route.Kind != types.BackendRouteKindCache || route.ContainerID != "" || route.Port != 0 || route.Protocol != types.BackendRouteProtocolTCP || route.Transport != types.BackendRouteTransportTSNet || route.RouteID != types.BackendRouteID(worker.MachineId, worker.Id, "", types.BackendRouteKindCache, 0) || !cachePeerLoopbackTarget(route.LocalTarget) {
		return status.Error(codes.PermissionDenied, "invalid cache agent route")
	}
	route.WorkspaceID = caller.Workspace.ExternalId
	route.ProxyTarget, route.Error, route.UpdatedAt = "", "", 0
	route.State = types.BackendRouteStateOpening
	// Heartbeats must not reset a route the agent already marked ready. A
	// changed local listener is re-opened and checked by the agent.
	if previous, err := s.containerRepo.GetBackendRoute(ctx, route.RouteID); err == nil && previous != nil && previous.WorkspaceID == route.WorkspaceID && previous.WorkerID == route.WorkerID && previous.LocalTarget == route.LocalTarget && previous.Transport == route.Transport {
		route.State, route.ProxyTarget = previous.State, previous.ProxyTarget
	}
	if err := s.containerRepo.SetBackendRoute(ctx, route); err != nil {
		return err
	}
	host.CacheRouteID = route.RouteID
	return nil
}

func (s *WorkerRepositoryService) cachePeerWorker(ctx context.Context) (*auth.Worker, *types.Worker, string, error) {
	caller, err := auth.WorkerFromContext(ctx)
	if err != nil {
		return nil, nil, "", err
	}
	if caller.Trusted || caller.Workspace == nil || s.workerRepo == nil {
		return nil, nil, "", status.Error(codes.PermissionDenied, "cache relay requires an agent worker")
	}
	worker, err := s.workerRepo.GetWorkerById(caller.ID)
	if err != nil || worker == nil || worker.Id != caller.ID || worker.WorkspaceId != caller.Workspace.ExternalId || worker.PoolName == "" || worker.MachineId == "" {
		return nil, nil, "", status.Error(codes.PermissionDenied, "cache relay worker is not assigned to this workspace")
	}
	base := worker.PoolName
	if configured := s.appConfig.Worker.Pools[worker.PoolName].ConfigGroup; configured != "" {
		base = configured
	}
	return caller, worker, caller.Workspace.ExternalId + cacheLocalityScopeSeparator + base, nil
}

func (s *WorkerRepositoryService) authorizeCachePeer(ctx context.Context, target *pb.CachePeerTarget) (*types.BackendRoute, error) {
	caller, worker, locality, err := s.cachePeerWorker(ctx)
	if err != nil {
		return nil, err
	}
	if target == nil || target.WorkerId != caller.ID || target.PoolName != worker.PoolName || target.Locality != locality || target.LogicalHostId == "" || target.RegistrationId == "" {
		return nil, status.Error(codes.PermissionDenied, "cache peer is outside this worker's pool or locality")
	}
	if s.cacheCoordinator == nil || s.containerRepo == nil {
		return nil, status.Error(codes.Unavailable, "cache relay is unavailable")
	}
	hosts, err := s.cacheCoordinator.ListHosts(ctx, worker.PoolName, locality)
	if err != nil {
		return nil, status.Error(codes.Unavailable, "cache registrations cannot be read")
	}
	for _, host := range hosts {
		if host.LogicalHostID != target.LogicalHostId {
			continue
		}
		// ListHosts places the active lease first for each logical host. A
		// stale process may still have a live fallback lease; do not relay to it.
		if host.RegistrationID != target.RegistrationId {
			return nil, status.Error(codes.FailedPrecondition, "cache peer registration changed")
		}
		if host.CacheRouteID == "" {
			return nil, status.Error(codes.FailedPrecondition, "cache peer has no agent route")
		}
		route, err := s.containerRepo.GetBackendRoute(ctx, host.CacheRouteID)
		if err != nil || route == nil {
			return nil, status.Error(codes.Unavailable, "cache peer route is unavailable")
		}
		peer, err := s.workerRepo.GetWorkerById(route.WorkerID)
		if err != nil || peer == nil || peer.Id != route.WorkerID || peer.WorkspaceId != caller.Workspace.ExternalId || peer.PoolName != worker.PoolName || peer.MachineId != host.NodeID || route.WorkspaceID != caller.Workspace.ExternalId || route.PoolName != worker.PoolName || route.MachineID != host.NodeID || route.Kind != types.BackendRouteKindCache || route.ContainerID != "" || route.Port != 0 || route.Protocol != types.BackendRouteProtocolTCP || route.Transport != types.BackendRouteTransportTSNet || !cachePeerLoopbackTarget(route.LocalTarget) || route.RouteID != types.BackendRouteID(peer.MachineId, peer.Id, "", types.BackendRouteKindCache, 0) {
			return nil, status.Error(codes.PermissionDenied, "cache peer route identity does not match its registration")
		}
		return route, nil
	}
	return nil, status.Error(codes.FailedPrecondition, "cache peer registration expired or changed")
}

func (p *cachePeerTransport) acquire(ctx context.Context, worker, host string) (func(), error) {
	p.mu.Lock()
	if p.global == nil {
		p.global = make(chan struct{}, 32)
		p.workers = map[string]*cachePeerSlots{}
		p.hosts = map[string]*cachePeerSlots{}
	}
	global := p.global
	p.mu.Unlock()
	select {
	case global <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	p.mu.Lock()
	get := func(m map[string]*cachePeerSlots, key string) *cachePeerSlots {
		v := m[key]
		if v == nil {
			v = &cachePeerSlots{slots: make(chan struct{}, 8)}
			m[key] = v
		}
		v.refs++
		return v
	}
	w, h := get(p.workers, worker), get(p.hosts, host)
	p.mu.Unlock()
	wHeld, hHeld := false, false
	release := func() {
		if hHeld {
			<-h.slots
		}
		if wHeld {
			<-w.slots
		}
		p.mu.Lock()
		w.refs--
		h.refs--
		if w.refs == 0 {
			delete(p.workers, worker)
		}
		if h.refs == 0 {
			delete(p.hosts, host)
		}
		p.mu.Unlock()
		<-global
	}
	select {
	case w.slots <- struct{}{}:
		wHeld = true
	case <-ctx.Done():
		release()
		return nil, ctx.Err()
	}
	select {
	case h.slots <- struct{}{}:
		hHeld = true
	case <-ctx.Done():
		release()
		return nil, ctx.Err()
	}
	return release, nil
}

func (s *WorkerRepositoryService) cachePeerClient(ctx context.Context, target *pb.CachePeerTarget) (pb.CacheClient, func(), error) {
	route, err := s.authorizeCachePeer(ctx, target)
	if err != nil {
		return nil, nil, err
	}
	release, err := s.cachePeer.acquire(ctx, target.WorkerId, target.LogicalHostId)
	if err != nil {
		return nil, nil, err
	}
	// Admission may wait behind long transfers. Do not use an authorization
	// snapshot that predates a lease replacement or worker reassignment.
	route, err = s.authorizeCachePeer(ctx, target)
	if err != nil {
		release()
		return nil, nil, err
	}
	if s.cachePeer.dial == nil {
		release()
		return nil, nil, status.Error(codes.Unavailable, "cache route dialer is unavailable")
	}
	address := types.BackendRouteAddress(route.RouteID)
	conn, err := grpc.NewClient("passthrough:///"+address, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return s.cachePeer.dial(ctx, address) }), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(8<<20), grpc.MaxCallSendMsgSize(2<<20)))
	if err != nil {
		release()
		return nil, nil, err
	}
	return pb.NewCacheClient(conn), func() { conn.Close(); release() }, nil
}

func (s *WorkerRepositoryService) InspectCachePeer(ctx context.Context, req *pb.CachePeerInspectRequest) (*pb.CachePeerInspectResponse, error) {
	ctx, cancel := s.cachePeer.operationContext(ctx)
	defer cancel()
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "cache inspect request is required")
	}
	if has := req.GetHas(); has != nil && (!cachePeerHash(has.Hash) || has.ExpectedSize < 0) {
		return nil, status.Error(codes.InvalidArgument, "invalid cache content identity")
	}
	if req.GetState() == nil && req.GetHas() == nil {
		return nil, status.Error(codes.InvalidArgument, "cache inspect operation is required")
	}
	client, close, err := s.cachePeerClient(ctx, req.Target)
	if err != nil {
		return nil, err
	}
	defer close()
	if req.GetState() != nil {
		response, err := client.GetState(ctx, req.GetState())
		return &pb.CachePeerInspectResponse{Response: &pb.CachePeerInspectResponse_State{State: response}}, err
	}
	response, err := client.HasContent(ctx, req.GetHas())
	return &pb.CachePeerInspectResponse{Response: &pb.CachePeerInspectResponse_Has{Has: response}}, err
}

func (s *WorkerRepositoryService) ReadCachePeer(req *pb.CachePeerReadRequest, stream pb.WorkerRepositoryService_ReadCachePeerServer) error {
	ctx, cancel := s.cachePeer.operationContext(stream.Context())
	defer cancel()
	if req == nil || !cachePeerHash(req.Hash) || req.Offset < 0 || req.Length < 0 || req.Length > cachePeerMaxReadBytes || req.Offset > math.MaxInt64-req.Length || (req.Length == 0 && req.Offset > math.MaxInt64-cachePeerMaxStoreBytes) {
		return status.Error(codes.InvalidArgument, "invalid bounded cache range")
	}
	client, close, err := s.cachePeerClient(ctx, req.Target)
	if err != nil {
		return err
	}
	defer close()
	read, err := client.GetContentStream(ctx, &pb.CacheGetContentRequest{Hash: req.Hash, Offset: req.Offset, Length: req.Length})
	if err != nil {
		return err
	}
	var sent int64
	limit := req.Length
	if limit == 0 {
		limit = cachePeerMaxStoreBytes
	}
	for {
		frame, err := read.Recv()
		if errors.Is(err, io.EOF) {
			if req.Length != 0 && sent != req.Length {
				return status.Error(codes.DataLoss, "short cache peer read")
			}
			return nil
		}
		if err != nil {
			return err
		}
		if !frame.Ok {
			return cachePeerSend(ctx, func() error { return stream.Send(&pb.CacheGetContentResponse{Ok: false}) })
		}
		if int64(len(frame.Content)) > limit-sent {
			return status.Error(codes.DataLoss, "cache peer exceeded the requested range")
		}
		for len(frame.Content) > 0 {
			n := min(len(frame.Content), cachePeerChunkBytes)
			if err := cachePeerSend(ctx, func() error { return stream.Send(&pb.CacheGetContentResponse{Ok: true, Content: frame.Content[:n]}) }); err != nil {
				return err
			}
			sent += int64(n)
			frame.Content = frame.Content[n:]
		}
	}
}

func (s *WorkerRepositoryService) StoreCachePeer(stream pb.WorkerRepositoryService_StoreCachePeerServer) error {
	ctx, cancel := s.cachePeer.operationContext(stream.Context())
	defer cancel()
	first, err := cachePeerIO(ctx, stream.Recv)
	if err != nil {
		return err
	}
	if first == nil || first.ExpectedSize < 0 || first.ExpectedSize > cachePeerMaxStoreBytes || (first.ExpectedHash != "" && !cachePeerHash(first.ExpectedHash)) {
		return status.Error(codes.InvalidArgument, "invalid bounded cache store")
	}
	client, close, err := s.cachePeerClient(ctx, first.Target)
	if err != nil {
		return err
	}
	defer close()
	store, err := client.StoreContent(ctx)
	if err != nil {
		return err
	}
	var size int64
	hash := sha256.New()
	frame := first
	for {
		if !proto.Equal(frame.Target, first.Target) {
			return status.Error(codes.PermissionDenied, "cache store target changed")
		}
		if _, err := s.authorizeCachePeer(ctx, frame.Target); err != nil {
			return err
		}
		if frame != first && (frame.ExpectedSize != 0 || frame.ExpectedHash != "") {
			return status.Error(codes.InvalidArgument, "cache store header repeated")
		}
		if frame.Content != nil {
			if len(frame.Content.Content) > cachePeerChunkBytes || int64(len(frame.Content.Content)) > first.ExpectedSize-size {
				return status.Error(codes.InvalidArgument, "cache store exceeds its declared size")
			}
			size += int64(len(frame.Content.Content))
			hash.Write(frame.Content.Content)
			if err := store.Send(frame.Content); err != nil {
				return err
			}
		}
		if frame.Finish {
			if size != first.ExpectedSize || (first.ExpectedHash != "" && hex.EncodeToString(hash.Sum(nil)) != first.ExpectedHash) {
				return status.Error(codes.DataLoss, "cache store size or checksum mismatch")
			}
			if _, err := cachePeerIO(ctx, stream.Recv); err != io.EOF {
				if err != nil {
					return err
				}
				return status.Error(codes.InvalidArgument, "cache store continued after finish")
			}
			response, err := store.CloseAndRecv()
			if err != nil {
				return err
			}
			if !response.Ok || response.Hash != hex.EncodeToString(hash.Sum(nil)) {
				return status.Error(codes.DataLoss, "cache store returned an inconsistent hash")
			}
			return cachePeerSend(ctx, func() error { return stream.SendAndClose(response) })
		}
		frame, err = cachePeerIO(ctx, stream.Recv)
		if err == io.EOF {
			return status.Error(codes.DataLoss, "cache store ended before finish")
		}
		if err != nil {
			return err
		}
	}
}
