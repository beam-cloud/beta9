package worker

import (
	"context"
	"net"

	"github.com/beam-cloud/beta9/pkg/cache"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// cacheWorkerRPC exposes only byte-oriented operations on the worker listener.
// Origin credentials and local filesystem paths must stay on the requesting
// worker, so source-fetch operations intentionally remain unimplemented.
type cacheWorkerRPC struct {
	pb.UnimplementedCacheServer
	manager *WorkerCacheManager
}

func agentCacheRPCServer(worker *Worker, manager *WorkerCacheManager) pb.CacheServer {
	if agentRPCBindHost(worker) == "" || manager == nil {
		return nil
	}
	return &cacheWorkerRPC{manager: manager}
}

// Agent routes already address the host's loopback network. Keep the newly
// registered cache API off public host interfaces. Custom nonloopback targets
// retain the old worker listener and use a separate cache-only listener.
func agentRPCBindHost(worker *Worker) string {
	if worker == nil || !worker.agentWorker() || worker.routeTransport != types.BackendRouteTransportTSNet {
		return ""
	}
	if worker.routeLocalTargetHost == "" {
		return "127.0.0.1"
	}
	if ip := net.ParseIP(worker.routeLocalTargetHost); ip != nil && ip.IsLoopback() {
		return worker.routeLocalTargetHost
	}
	return ""
}

func (m *WorkerCacheManager) startCachePeerListener() (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.draining {
		return "", status.Error(codes.Unavailable, "node-local cache is draining")
	}
	if m.cachePeerListener != nil {
		return m.cachePeerListener.Addr().String(), nil
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	server := grpc.NewServer(grpc.MaxRecvMsgSize(2<<20), grpc.MaxSendMsgSize(8<<20))
	pb.RegisterCacheServer(server, &cacheWorkerRPC{manager: m})
	m.cachePeerGRPCServer, m.cachePeerListener = server, listener
	go server.Serve(listener)
	return listener.Addr().String(), nil
}

func (s *cacheWorkerRPC) server() (*cache.Server, error) {
	s.manager.mu.Lock()
	defer s.manager.mu.Unlock()
	if s.manager.draining || s.manager.server == nil {
		return nil, status.Error(codes.Unavailable, "node-local cache is unavailable")
	}
	return s.manager.server, nil
}

func (s *cacheWorkerRPC) GetState(ctx context.Context, req *pb.CacheGetStateRequest) (*pb.CacheGetStateResponse, error) {
	server, err := s.server()
	if err != nil {
		return nil, err
	}
	return server.GetState(ctx, req)
}
func (s *cacheWorkerRPC) HasContent(ctx context.Context, req *pb.CacheHasContentRequest) (*pb.CacheHasContentResponse, error) {
	server, err := s.server()
	if err != nil {
		return nil, err
	}
	return server.HasContent(ctx, req)
}
func (s *cacheWorkerRPC) GetContent(ctx context.Context, req *pb.CacheGetContentRequest) (*pb.CacheGetContentResponse, error) {
	server, err := s.server()
	if err != nil {
		return nil, err
	}
	return server.GetContent(ctx, req)
}
func (s *cacheWorkerRPC) GetContentStream(req *pb.CacheGetContentRequest, stream pb.Cache_GetContentStreamServer) error {
	server, err := s.server()
	if err != nil {
		return err
	}
	return server.GetContentStream(req, stream)
}
func (s *cacheWorkerRPC) StoreContent(stream pb.Cache_StoreContentServer) error {
	server, err := s.server()
	if err != nil {
		return err
	}
	return server.StoreContent(stream)
}

// PublishWorkerRoute runs after the worker listener is serving. The route is
// retained across ownership changes; each new cache lease republishes it.
func (m *WorkerCacheManager) PublishWorkerRoute(ctx context.Context, route *pb.BackendRoute) error {
	if m == nil || route == nil {
		return nil
	}
	m.mu.Lock()
	m.cacheWorkerRoute = proto.Clone(route).(*pb.BackendRoute)
	registration := m.registration
	if registration != nil {
		registration.mu.Lock()
		registration.workerRoute = m.cacheWorkerRoute
		registration.mu.Unlock()
	}
	m.mu.Unlock()
	if registration != nil {
		return registration.registerOnce(ctx)
	}
	return nil
}
