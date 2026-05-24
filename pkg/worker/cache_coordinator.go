package worker

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
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

	resp, err := handleGRPCResponse(d.client.ListCacheHosts(ctx, &pb.ListCacheHostsRequest{
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
	slot           int
	logicalHostID  string
	registrationID string
	cachePathID    string
}

func newGatewayCacheRegistration(manager *WorkerCacheManager, server *cache.Server, cacheConfig cache.Config, advertisedAddr string, slot int) *gatewayCacheRegistration {
	cachePathID := cachePathID(cacheConfig.Server.DiskCacheDir)
	logicalHostID := cacheLogicalHostID(manager.poolName, manager.locality, manager.nodeID, cacheConfig.Server.DiskCacheDir, slot)

	return &gatewayCacheRegistration{
		manager:        manager,
		server:         server,
		cacheConfig:    cacheConfig,
		advertisedAddr: advertisedAddr,
		slot:           slot,
		logicalHostID:  logicalHostID,
		registrationID: cacheRegistrationID(manager.workerID, manager.instanceID),
		cachePathID:    cachePathID,
	}
}

func newGatewayCacheRegistrations(manager *WorkerCacheManager, server *cache.Server, cacheConfig cache.Config, advertisedAddr string) []*gatewayCacheRegistration {
	slots := cacheConfig.Embedded.SlotsPerNode
	if slots <= 0 {
		slots = cacheDefaultSlotsPerNode
	}

	registrations := make([]*gatewayCacheRegistration, 0, slots)
	for slot := 0; slot < slots; slot++ {
		registrations = append(registrations, newGatewayCacheRegistration(manager, server, cacheConfig, advertisedAddr, slot))
	}
	return registrations
}

func registerGatewayCacheHosts(ctx context.Context, registrations []*gatewayCacheRegistration) error {
	for _, registration := range registrations {
		if err := registration.registerOnce(ctx); err != nil {
			return err
		}
	}
	return nil
}

func unregisterGatewayCacheHosts(ctx context.Context, registrations []*gatewayCacheRegistration) error {
	var err error
	for _, registration := range registrations {
		err = errors.Join(err, registration.unregister(ctx))
	}
	return err
}

func runGatewayCacheRegistrations(ctx context.Context, registrations []*gatewayCacheRegistration) {
	if len(registrations) == 0 {
		return
	}

	interval := cacheRegistrationHeartbeat(registrations[0].cacheConfig)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for _, registration := range registrations {
				if err := registration.registerOnce(ctx); err != nil {
					log.Warn().
						Err(err).
						Str("logical_host_id", registration.logicalHostID).
						Str("registration_id", registration.registrationID).
						Msg("failed to refresh embedded cache registration")
				}
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
		log.Debug().
			Str("logical_host_id", host.LogicalHostId).
			Str("registration_id", host.RegistrationId).
			Str("addr", host.PrivateAddr).
			Msg("registered embedded cache host")
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
		Slot:             int32(r.slot),
		Addr:             addr,
		PrivateAddr:      privateAddr,
		CapacityUsagePct: capacityUsagePct,
	}
}

// cacheLogicalHostID is the stable routing identity for one cache placement
// target. It is intentionally based on pool, locality, node, cache path, and
// slot, not the worker process ID; restarted or colocated cache servers register
// under this same logical host when they serve the same disk cache target.
func cacheLogicalHostID(poolName, locality, nodeID, diskCacheDir string, slot int) string {
	return fmt.Sprintf(
		"cache-host-%s-%s-%s-%s-%d",
		safeCacheName(poolName),
		safeCacheName(locality),
		safeCacheName(nodeID),
		cachePathID(diskCacheDir),
		slot,
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
