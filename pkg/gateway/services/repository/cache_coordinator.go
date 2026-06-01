package repository_services

import (
	"context"
	"time"

	"github.com/beam-cloud/beta9/pkg/cache"
	pb "github.com/beam-cloud/beta9/proto"
)

func (s *WorkerRepositoryService) RegisterCacheHost(ctx context.Context, req *pb.RegisterCacheHostRequest) (*pb.RegisterCacheHostResponse, error) {
	if err := s.authorizeCacheRepositoryRequest(ctx); err != nil {
		return &pb.RegisterCacheHostResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	if s.cacheCoordinator == nil {
		return &pb.RegisterCacheHostResponse{Ok: false, ErrorMsg: cache.ErrCoordinatorUnavailable.Error()}, nil
	}
	if req == nil || req.Host == nil {
		return &pb.RegisterCacheHostResponse{Ok: false, ErrorMsg: "host is required"}, nil
	}

	ttl := time.Duration(req.TtlSeconds) * time.Second
	if err := s.cacheCoordinator.RegisterHost(ctx, cacheCoordinatorHostFromProto(req.Host), ttl); err != nil {
		return &pb.RegisterCacheHostResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	return &pb.RegisterCacheHostResponse{Ok: true}, nil
}

func (s *WorkerRepositoryService) UnregisterCacheHost(ctx context.Context, req *pb.UnregisterCacheHostRequest) (*pb.UnregisterCacheHostResponse, error) {
	if err := s.authorizeCacheRepositoryRequest(ctx); err != nil {
		return &pb.UnregisterCacheHostResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	if s.cacheCoordinator == nil {
		return &pb.UnregisterCacheHostResponse{Ok: false, ErrorMsg: cache.ErrCoordinatorUnavailable.Error()}, nil
	}
	if req == nil {
		return &pb.UnregisterCacheHostResponse{Ok: false, ErrorMsg: "request is required"}, nil
	}

	if err := s.cacheCoordinator.UnregisterHost(ctx, req.PoolName, req.Locality, req.LogicalHostId, req.RegistrationId); err != nil {
		return &pb.UnregisterCacheHostResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	return &pb.UnregisterCacheHostResponse{Ok: true}, nil
}

func (s *WorkerRepositoryService) ListCacheHosts(ctx context.Context, req *pb.ListCacheHostsRequest) (*pb.ListCacheHostsResponse, error) {
	if err := s.authorizeCacheRepositoryRequest(ctx); err != nil {
		return &pb.ListCacheHostsResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	if s.cacheCoordinator == nil {
		return &pb.ListCacheHostsResponse{Ok: false, ErrorMsg: cache.ErrCoordinatorUnavailable.Error()}, nil
	}
	if req == nil {
		return &pb.ListCacheHostsResponse{Ok: false, ErrorMsg: "request is required"}, nil
	}

	hosts, err := s.cacheCoordinator.ListHosts(ctx, req.PoolName, req.Locality)
	if err != nil {
		return &pb.ListCacheHostsResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	resp := &pb.ListCacheHostsResponse{
		Ok:    true,
		Hosts: make([]*pb.CacheCoordinatorHost, 0, len(hosts)),
	}
	for _, host := range hosts {
		resp.Hosts = append(resp.Hosts, cacheCoordinatorHostToProto(host))
	}
	return resp, nil
}

func cacheCoordinatorHostFromProto(host *pb.CacheCoordinatorHost) cache.CoordinatorHost {
	if host == nil {
		return cache.CoordinatorHost{}
	}
	return cache.CoordinatorHost{
		LogicalHostID:    host.LogicalHostId,
		RegistrationID:   host.RegistrationId,
		PoolName:         host.PoolName,
		Locality:         host.Locality,
		NodeID:           host.NodeId,
		CachePathID:      host.CachePathId,
		Addr:             host.Addr,
		PrivateAddr:      host.PrivateAddr,
		CapacityUsagePct: float64(host.CapacityUsagePct),
	}
}

func cacheCoordinatorHostToProto(host cache.CoordinatorHost) *pb.CacheCoordinatorHost {
	return &pb.CacheCoordinatorHost{
		LogicalHostId:    host.LogicalHostID,
		RegistrationId:   host.RegistrationID,
		PoolName:         host.PoolName,
		Locality:         host.Locality,
		NodeId:           host.NodeID,
		CachePathId:      host.CachePathID,
		Addr:             host.Addr,
		PrivateAddr:      host.PrivateAddr,
		CapacityUsagePct: float32(host.CapacityUsagePct),
	}
}
