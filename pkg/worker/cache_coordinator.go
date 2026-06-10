package worker

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/cache"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/rs/zerolog/log"
)

const cacheCoordinatorRPCTimeout = 5 * time.Second

type gatewayCacheHostDirectory struct {
	client   pb.WorkerRepositoryServiceClient
	poolName string
}

func (d *gatewayCacheHostDirectory) GetAvailableHosts(ctx context.Context, locality string) ([]*cache.Host, error) {
	if d == nil || d.client == nil {
		return nil, cache.ErrHostNotFound
	}

	rpcCtx, cancel := context.WithTimeout(ctx, cacheCoordinatorRPCTimeout)
	defer cancel()

	resp, err := handleGRPCResponse(d.client.ListCacheHosts(rpcCtx, &pb.ListCacheHostsRequest{
		PoolName: d.poolName,
		Locality: locality,
	}))
	if err != nil {
		return nil, err
	}

	hosts := make([]*cache.Host, 0, len(resp.Hosts))
	for _, host := range resp.Hosts {
		if host == nil || host.LogicalHostId == "" {
			continue
		}
		hosts = append(hosts, &cache.Host{
			HostId:           host.LogicalHostId,
			RegistrationID:   host.RegistrationId,
			PoolName:         host.PoolName,
			Locality:         host.Locality,
			NodeID:           host.NodeId,
			CachePathID:      host.CachePathId,
			Addr:             host.Addr,
			PrivateAddr:      host.PrivateAddr,
			CapacityUsagePct: float64(host.CapacityUsagePct),
		})
	}
	if len(hosts) == 0 {
		return nil, cache.ErrHostNotFound
	}
	return hosts, nil
}

type gatewayCacheRegistration struct {
	manager        *WorkerCacheManager
	server         *cache.Server
	cacheConfig    cache.Config
	advertisedAddr string
	logicalHostID  string
	registrationID string
	cachePathID    string
	loggedSuccess  bool
}

func newGatewayCacheRegistration(manager *WorkerCacheManager, server *cache.Server, cacheConfig cache.Config, advertisedAddr string) *gatewayCacheRegistration {
	cacheIdentityPath := ""
	if manager != nil {
		cacheIdentityPath = manager.cacheIdentityPath
	}
	if cacheIdentityPath == "" && manager != nil {
		cacheIdentityPath = cachePlacementIdentityPath(manager.config, cacheConfig)
	}
	if cacheIdentityPath == "" {
		cacheIdentityPath = cacheCanonicalPhysicalIdentityPath(cacheConfig)
	}
	cachePathID := cachePathID(cacheIdentityPath)
	logicalHostID := cacheLogicalHostID(manager.locality, manager.nodeID, cacheIdentityPath)

	return &gatewayCacheRegistration{
		manager:        manager,
		server:         server,
		cacheConfig:    cacheConfig,
		advertisedAddr: advertisedAddr,
		logicalHostID:  logicalHostID,
		registrationID: cacheRegistrationID(manager.workerID, manager.instanceID),
		cachePathID:    cachePathID,
	}
}

func runGatewayCacheRegistration(ctx context.Context, registration *gatewayCacheRegistration) {
	if registration == nil {
		return
	}

	interval := cacheRegistrationHeartbeat(registration.cacheConfig)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := registration.registerOnce(ctx); err != nil {
				log.Warn().
					Err(err).
					Str("logical_host_id", registration.logicalHostID).
					Str("registration_id", registration.registrationID).
					Msg("failed to refresh cache registration")
			}
		}
	}
}

func (r *gatewayCacheRegistration) registerOnce(ctx context.Context) error {
	if r == nil || r.manager == nil || r.manager.workerRepo == nil {
		return errors.New("cache registration is not initialized")
	}

	rpcCtx, cancel := context.WithTimeout(ctx, cacheCoordinatorRPCTimeout)
	defer cancel()

	host := r.host()
	_, err := handleGRPCResponse(r.manager.workerRepo.RegisterCacheHost(rpcCtx, &pb.RegisterCacheHostRequest{
		Host:       host,
		TtlSeconds: int32(cacheRegistrationTTL(r.cacheConfig) / time.Second),
	}))
	if err == nil {
		logger := log.Trace()
		message := "refreshed cache host registration"
		if !r.loggedSuccess {
			logger = log.Info()
			r.loggedSuccess = true
			message = "registered cache host"
		}
		logger.
			Str("logical_host_id", host.LogicalHostId).
			Str("registration_id", host.RegistrationId).
			Str("pool_name", host.PoolName).
			Str("locality", host.Locality).
			Str("node_id", host.NodeId).
			Str("cache_path_id", host.CachePathId).
			Str("addr", host.PrivateAddr).
			Int32("ttl_seconds", int32(cacheRegistrationTTL(r.cacheConfig)/time.Second)).
			Float32("capacity_usage_pct", host.CapacityUsagePct).
			Msg(message)
	}
	return err
}

func (r *gatewayCacheRegistration) unregister(ctx context.Context) error {
	if r == nil || r.manager == nil || r.manager.workerRepo == nil || r.logicalHostID == "" || r.registrationID == "" {
		return nil
	}

	rpcCtx, cancel := context.WithTimeout(ctx, cacheCoordinatorRPCTimeout)
	defer cancel()

	_, err := handleGRPCResponse(r.manager.workerRepo.UnregisterCacheHost(rpcCtx, &pb.UnregisterCacheHostRequest{
		LogicalHostId:  r.logicalHostID,
		RegistrationId: r.registrationID,
		PoolName:       r.manager.poolName,
		Locality:       r.manager.locality,
	}))
	return err
}

func (r *gatewayCacheRegistration) host() *pb.CacheCoordinatorHost {
	addr := r.advertisedAddr
	privateAddr := r.advertisedAddr
	capacityUsagePct := float32(0)
	if r.server != nil {
		capacityUsagePct = float32(r.server.UsagePct())
		if host := r.server.Host(); host != nil {
			if host.Addr != "" {
				addr = host.Addr
			}
			if host.PrivateAddr != "" {
				privateAddr = host.PrivateAddr
			}
		}
	}

	return &pb.CacheCoordinatorHost{
		LogicalHostId:    r.logicalHostID,
		RegistrationId:   r.registrationID,
		PoolName:         r.manager.poolName,
		Locality:         r.manager.locality,
		NodeId:           r.manager.nodeID,
		CachePathId:      r.cachePathID,
		Addr:             addr,
		PrivateAddr:      privateAddr,
		CapacityUsagePct: capacityUsagePct,
	}
}

// cacheLogicalHostID is the stable routing identity for one cache placement
// target. It is intentionally based on locality, node, and cache identity path, not the
// worker pool or process ID; restarted or colocated cache servers register
// under this same logical host when they serve the same disk cache target.
func cacheLogicalHostID(locality, nodeID, cacheIdentityPath string) string {
	return fmt.Sprintf(
		"cache-host-%s-%s-%s",
		safeCacheName(locality),
		safeCacheName(nodeID),
		cachePathID(cacheIdentityPath),
	)
}

// cacheRegistrationID identifies one live worker/cache-server process lease for
// a logical host. The coordinator can promote another registration without
// changing the logical host used by client routing.
func cacheRegistrationID(workerID, instanceID string) string {
	if instanceID == "" || instanceID == workerID {
		return safeCacheName(workerID)
	}
	return fmt.Sprintf("%s-%s", safeCacheName(workerID), safeCacheName(instanceID))
}

func cachePathID(path string) string {
	sum := sha256.Sum256([]byte(path))
	return fmt.Sprintf("%x", sum[:6])
}

func cacheCanonicalPhysicalIdentityPath(config cache.Config) string {
	if config.Server.DiskCacheDir == "" {
		return ""
	}
	diskCacheDir := filepath.Clean(config.Server.DiskCacheDir)
	if config.Disk.MountPath == "" || config.Disk.HostPath == "" {
		return diskCacheDir
	}
	mountPath := filepath.Clean(config.Disk.MountPath)
	hostPath := filepath.Clean(config.Disk.HostPath)

	rel, err := filepath.Rel(mountPath, diskCacheDir)
	if err != nil || relEscapesBase(rel) {
		return diskCacheDir
	}
	return filepath.Clean(filepath.Join(hostPath, rel))
}

func relEscapesBase(rel string) bool {
	return rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || filepath.IsAbs(rel)
}

func cacheRegistrationTTL(config cache.Config) time.Duration {
	if config.Coordinator.RegistrationTTLSeconds <= 0 {
		return cacheDefaultRegistrationTTL
	}
	return time.Duration(config.Coordinator.RegistrationTTLSeconds) * time.Second
}

func cacheRegistrationHeartbeat(config cache.Config) time.Duration {
	if config.Coordinator.HeartbeatIntervalSeconds <= 0 {
		return cacheDefaultRegistrationHeartbeat
	}
	return time.Duration(config.Coordinator.HeartbeatIntervalSeconds) * time.Second
}
