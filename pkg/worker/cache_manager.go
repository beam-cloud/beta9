package worker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"sync"
	"time"

	"github.com/beam-cloud/beta9/pkg/cache"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

const (
	cacheDefaultLocality                = "default"
	cacheDefaultDiskPath                = "/var/lib/beta9/cache"
	cacheDefaultServerPort              = 2049
	cacheDefaultDiscoveryS              = 5
	cacheDefaultDiscoveryJitterS        = 3
	cacheDefaultMaxDiscoveryConcurrency = 8
	cacheDefaultHostMonitorIntervalS    = 30
	cacheDefaultGRPCDialS               = 1
	cacheDefaultGRPCMessage             = 1024 * 1024 * 1024
	cacheDefaultPageSizeBytes           = 4_000_000
	cacheDefaultDiskMaxUsage            = 0.95
	cacheDefaultNTopHosts               = 3
	cacheDefaultMinRetryBytes           = 0
	cacheDefaultGetAttempts             = 3
	cacheDefaultS3Concurrency           = 16
	cacheDefaultS3ChunkSize             = 64_000_000
	cacheDefaultReplicasPerNode         = 2
	cachePrimaryLeaseTTL                = 30 * time.Second
	cachePrimaryRefresh                 = 10 * time.Second
	cachePrimaryRetry                   = 5 * time.Second
	cachePrimaryLeaseKeyPrefix          = "cache:primary"
	cacheReplicaLeaseKeyPrefix          = "cache:replica"
)

const refreshCachePrimaryLeaseScript = `
if redis.call("GET", KEYS[1]) == ARGV[1] then
	return redis.call("PEXPIRE", KEYS[1], ARGV[2])
end
return 0
`

const releaseCachePrimaryLeaseScript = `
if redis.call("GET", KEYS[1]) == ARGV[1] then
	return redis.call("DEL", KEYS[1])
end
return 0
`

type WorkerCacheManager struct {
	ctx            context.Context
	cancel         context.CancelFunc
	config         types.AppConfig
	redis          *common.RedisClient
	workerID       string
	instanceID     string
	podAddr        string
	nodeID         string
	locality       string
	leaseKey       string
	activeLeaseKey string
	leaseToken     string
	registry       cache.Registry
	client         *cache.Client
	server         *cache.Server
	mu             sync.Mutex
	wg             sync.WaitGroup
}

func NewWorkerCacheManager(ctx context.Context, config types.AppConfig, poolConfig types.WorkerPoolConfig, redisClient *common.RedisClient, workerID, podAddr string) *WorkerCacheManager {
	cacheCtx, cancel := context.WithCancel(ctx)
	locality := cacheLocality(config, poolConfig)
	nodeID := cacheNodeID()

	return &WorkerCacheManager{
		ctx:        cacheCtx,
		cancel:     cancel,
		config:     config,
		redis:      redisClient,
		workerID:   workerID,
		instanceID: cacheWorkerInstanceID(workerID),
		podAddr:    podAddr,
		nodeID:     nodeID,
		locality:   locality,
		leaseKey:   cachePrimaryLeaseKey(locality, nodeID),
		leaseToken: fmt.Sprintf("%s:%s", workerID, uuid.New().String()),
	}
}

func (m *WorkerCacheManager) Start() (*cache.Client, error) {
	if !m.enabled() {
		return nil, nil
	}

	cacheConfig := normalizeCacheConfig(m.config, m.nodeID, m.locality)
	registry := cache.NewRedisRegistryWithClient(cacheConfig.Global, cacheConfig.Server, m.redis.UniversalClient)
	m.registry = registry
	if cacheConfig.Embedded.Mode == cache.EmbeddedModeActiveActive {
		m.wg.Add(1)
		go m.runReplicaElection(cacheConfig)
	}

	client, err := cache.NewClientWithRegistry(m.ctx, cacheConfig, registry, m.locality)
	if err != nil {
		if m.server != nil {
			m.cancel()
			_ = m.server.Close()
		}
		return nil, err
	}

	m.client = client
	if cacheConfig.Embedded.Mode == cache.EmbeddedModeSinglePrimary {
		m.wg.Add(1)
		go m.runElection(cacheConfig)
	}

	if err := client.WaitForHosts(defaultCacheWaitTime); err != nil {
		log.Warn().
			Err(err).
			Str("locality", m.locality).
			Str("node_id", m.nodeID).
			Msg("cache has no available hosts yet")
	}

	return client, nil
}

func (m *WorkerCacheManager) Close() error {
	m.cancel()

	var errs error
	m.mu.Lock()
	server := m.server
	client := m.client
	m.mu.Unlock()

	if client != nil {
		errs = errors.Join(errs, client.Cleanup())
	}
	if server != nil {
		errs = errors.Join(errs, server.Close())
	}

	_ = m.releaseLeaseKey(context.Background(), m.leaseKey)
	_ = m.releaseActiveLease(context.Background())
	m.wg.Wait()
	return errs
}

func (m *WorkerCacheManager) enabled() bool {
	return m.config.Cache.Enabled && m.config.Worker.CacheEnabled
}

func (m *WorkerCacheManager) runReplicaElection(cacheConfig cache.Config) {
	defer m.wg.Done()

	for {
		select {
		case <-m.ctx.Done():
			return
		default:
		}

		leaseKey, acquired, err := m.acquireReplicaLease(cacheConfig)
		if err != nil {
			log.Warn().Err(err).Str("locality", m.locality).Str("node_id", m.nodeID).Msg("failed to acquire embedded cache replica lease")
		}
		if acquired {
			m.runReplica(cacheConfig, leaseKey)
		}

		timer := time.NewTimer(cacheLeaseRetry(cacheConfig))
		select {
		case <-m.ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func (m *WorkerCacheManager) acquireReplicaLease(cacheConfig cache.Config) (string, bool, error) {
	replicas := cacheConfig.Embedded.ReplicasPerNode
	if replicas <= 0 {
		replicas = cacheDefaultReplicasPerNode
	}

	for slot := 0; slot < replicas; slot++ {
		leaseKey := cacheReplicaLeaseKey(m.locality, m.nodeID, slot)
		acquired, err := m.acquireLeaseKey(leaseKey, cacheLeaseTTL(cacheConfig))
		if err != nil {
			return "", false, err
		}
		if acquired {
			return leaseKey, true, nil
		}
	}

	return "", false, nil
}

func (m *WorkerCacheManager) runReplica(cacheConfig cache.Config, leaseKey string) {
	hostID := cacheWorkerHostID(m.locality, m.nodeID, m.workerID, m.instanceID)
	server, advertisedAddr, err := m.createEmbeddedServer(cacheConfig, hostID)
	if err != nil {
		log.Warn().Err(err).Str("lease_key", leaseKey).Msg("failed to start embedded cache replica")
		_ = m.releaseLeaseKey(context.Background(), leaseKey)
		return
	}

	m.mu.Lock()
	m.activeLeaseKey = leaseKey
	m.mu.Unlock()

	log.Info().
		Str("addr", advertisedAddr).
		Str("locality", m.locality).
		Str("node_id", m.nodeID).
		Str("host_id", hostID).
		Str("lease_key", leaseKey).
		Msg("embedded cache replica started")

	ticker := time.NewTicker(cacheLeaseRefresh(cacheConfig))
	defer ticker.Stop()
	defer func() {
		_ = server.Close()
		_ = m.releaseLeaseKey(context.Background(), leaseKey)
		m.mu.Lock()
		if m.server == server {
			m.server = nil
		}
		if m.activeLeaseKey == leaseKey {
			m.activeLeaseKey = ""
		}
		m.mu.Unlock()
	}()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			ok, err := m.refreshLeaseKey(leaseKey, cacheLeaseTTL(cacheConfig))
			if err != nil {
				log.Warn().Err(err).Str("lease_key", leaseKey).Msg("failed to refresh embedded cache replica lease")
				return
			}
			if !ok {
				log.Warn().Str("lease_key", leaseKey).Msg("lost embedded cache replica lease")
				return
			}
		}
	}
}

func (m *WorkerCacheManager) runElection(cacheConfig cache.Config) {
	defer m.wg.Done()

	for {
		select {
		case <-m.ctx.Done():
			return
		default:
		}

		acquired, err := m.acquireLeaseKey(m.leaseKey, cachePrimaryLeaseTTL)
		if err != nil {
			log.Warn().Err(err).Str("lease_key", m.leaseKey).Msg("failed to acquire cache primary lease")
		}
		if acquired {
			m.runPrimary(cacheConfig)
		}

		timer := time.NewTimer(cachePrimaryRetry)
		select {
		case <-m.ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func (m *WorkerCacheManager) runPrimary(cacheConfig cache.Config) {
	server, advertisedAddr, err := m.createEmbeddedServer(cacheConfig, cacheNodeHostID(m.locality, m.nodeID))
	if err != nil {
		log.Warn().Err(err).Msg("failed to start embedded cache primary")
		_ = m.releaseLeaseKey(context.Background(), m.leaseKey)
		return
	}

	log.Info().
		Str("addr", advertisedAddr).
		Str("locality", m.locality).
		Str("node_id", m.nodeID).
		Msg("embedded cache server elected primary")

	ticker := time.NewTicker(cachePrimaryRefresh)
	defer ticker.Stop()
	defer func() {
		_ = server.Close()
		_ = m.releaseLeaseKey(context.Background(), m.leaseKey)
		m.mu.Lock()
		if m.server == server {
			m.server = nil
		}
		m.mu.Unlock()
	}()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			ok, err := m.refreshLeaseKey(m.leaseKey, cachePrimaryLeaseTTL)
			if err != nil {
				log.Warn().Err(err).Str("lease_key", m.leaseKey).Msg("failed to refresh cache primary lease")
				return
			}
			if !ok {
				log.Warn().Str("lease_key", m.leaseKey).Msg("lost cache primary lease")
				return
			}
		}
	}
}

func (m *WorkerCacheManager) startEmbeddedServer(cacheConfig cache.Config, hostID string) error {
	_, advertisedAddr, err := m.createEmbeddedServer(cacheConfig, hostID)
	if err != nil {
		return err
	}

	log.Info().
		Str("addr", advertisedAddr).
		Str("locality", m.locality).
		Str("node_id", m.nodeID).
		Str("host_id", hostID).
		Msg("embedded cache server started")
	return nil
}

func (m *WorkerCacheManager) createEmbeddedServer(cacheConfig cache.Config, hostID string) (*cache.Server, string, error) {
	server, err := cache.NewServerWithOptions(
		m.ctx,
		cacheConfig,
		m.locality,
		cache.WithServerRegistry(m.registry),
		cache.WithServerHostID(hostID),
	)
	if err != nil {
		return nil, "", err
	}

	advertisedAddr, err := server.Serve(m.bindAddr(cacheConfig), m.podAddr)
	if err != nil {
		_ = server.Close()
		return nil, "", err
	}

	m.mu.Lock()
	m.server = server
	m.mu.Unlock()

	return server, advertisedAddr, nil
}

func (m *WorkerCacheManager) bindAddr(cacheConfig cache.Config) string {
	port := cacheConfig.Global.ServerPort
	if port == 0 {
		port = cacheDefaultServerPort
	}

	if cacheHostNetwork(m.config) {
		return ":0"
	}

	return fmt.Sprintf(":%d", port)
}

func (m *WorkerCacheManager) acquireLeaseKey(key string, ttl time.Duration) (bool, error) {
	ok, err := m.redis.SetNX(m.ctx, key, m.leaseToken, ttl).Result()
	if err != nil {
		return false, err
	}
	return ok, nil
}

func (m *WorkerCacheManager) refreshLeaseKey(key string, ttl time.Duration) (bool, error) {
	result, err := m.redis.Eval(m.ctx, refreshCachePrimaryLeaseScript, []string{key}, m.leaseToken, int(ttl/time.Millisecond)).Int()
	if err != nil {
		return false, err
	}
	return result == 1, nil
}

func (m *WorkerCacheManager) releaseLeaseKey(ctx context.Context, key string) error {
	if key == "" {
		return nil
	}
	_, err := m.redis.Eval(ctx, releaseCachePrimaryLeaseScript, []string{key}, m.leaseToken).Result()
	return err
}

func (m *WorkerCacheManager) releaseActiveLease(ctx context.Context) error {
	m.mu.Lock()
	leaseKey := m.activeLeaseKey
	m.activeLeaseKey = ""
	m.mu.Unlock()
	return m.releaseLeaseKey(ctx, leaseKey)
}

func normalizeCacheConfig(config types.AppConfig, nodeID, locality string) cache.Config {
	cacheConfig := config.Cache

	if cacheConfig.Global.DefaultLocality == "" {
		cacheConfig.Global.DefaultLocality = cacheDefaultLocality
	}
	if cacheConfig.Global.ServerPort == 0 {
		cacheConfig.Global.ServerPort = cacheDefaultServerPort
	}
	if cacheConfig.Global.DiscoveryIntervalS == 0 {
		cacheConfig.Global.DiscoveryIntervalS = cacheDefaultDiscoveryS
	}
	if cacheConfig.Global.DiscoveryJitterS == 0 {
		cacheConfig.Global.DiscoveryJitterS = cacheDefaultDiscoveryJitterS
	}
	if cacheConfig.Global.MaxDiscoveryConcurrency == 0 {
		cacheConfig.Global.MaxDiscoveryConcurrency = cacheDefaultMaxDiscoveryConcurrency
	}
	if cacheConfig.Global.HostMonitorIntervalS == 0 {
		cacheConfig.Global.HostMonitorIntervalS = cacheDefaultHostMonitorIntervalS
	}
	if cacheConfig.Global.RoundTripThresholdMilliseconds == 0 {
		cacheConfig.Global.RoundTripThresholdMilliseconds = 1000
	}
	if cacheConfig.Global.HostStorageCapacityThresholdPct == 0 {
		cacheConfig.Global.HostStorageCapacityThresholdPct = cacheDefaultDiskMaxUsage
	}
	if cacheConfig.Global.GRPCDialTimeoutS == 0 {
		cacheConfig.Global.GRPCDialTimeoutS = cacheDefaultGRPCDialS
	}
	if cacheConfig.Global.GRPCMessageSizeBytes == 0 {
		cacheConfig.Global.GRPCMessageSizeBytes = cacheDefaultGRPCMessage
	}
	if cacheConfig.Embedded.Mode == "" {
		cacheConfig.Embedded.Mode = cache.EmbeddedModeActiveActive
	}
	if cacheConfig.Embedded.ReplicasPerNode == 0 {
		cacheConfig.Embedded.ReplicasPerNode = cacheDefaultReplicasPerNode
	}
	if cacheConfig.Embedded.LeaseTTLSeconds == 0 {
		cacheConfig.Embedded.LeaseTTLSeconds = int(cachePrimaryLeaseTTL / time.Second)
	}
	if cacheConfig.Embedded.LeaseRefreshSeconds == 0 {
		cacheConfig.Embedded.LeaseRefreshSeconds = int(cachePrimaryRefresh / time.Second)
	}
	if cacheConfig.Embedded.LeaseRetrySeconds == 0 {
		cacheConfig.Embedded.LeaseRetrySeconds = int(cachePrimaryRetry / time.Second)
	}

	if cacheConfig.Disk.MountPath == "" {
		cacheConfig.Disk.MountPath = cacheDefaultDiskPath
	}
	if cacheConfig.Disk.HostPath == "" {
		cacheConfig.Disk.HostPath = cacheDefaultDiskPath
	}
	if cacheConfig.Disk.MaxUsagePct == 0 {
		cacheConfig.Disk.MaxUsagePct = cacheDefaultDiskMaxUsage
	}

	if cacheConfig.Server.Mode == "" {
		cacheConfig.Server.Mode = cache.ServerModeNode
	}
	if cacheConfig.Server.DiskCacheDir == "" {
		cacheConfig.Server.DiskCacheDir = filepath.Join(cacheConfig.Disk.MountPath, safeCacheName(locality), safeCacheName(nodeID))
	}
	if cacheConfig.Server.DiskCacheMaxUsagePct == 0 {
		cacheConfig.Server.DiskCacheMaxUsagePct = cacheConfig.Disk.MaxUsagePct
	}
	if cacheConfig.Server.PageSizeBytes == 0 {
		cacheConfig.Server.PageSizeBytes = cacheDefaultPageSizeBytes
	}
	if cacheConfig.Server.S3DownloadConcurrency == 0 {
		cacheConfig.Server.S3DownloadConcurrency = cacheDefaultS3Concurrency
	}
	if cacheConfig.Server.S3DownloadChunkSize == 0 {
		cacheConfig.Server.S3DownloadChunkSize = cacheDefaultS3ChunkSize
	}

	if !cacheConfig.Memory.Enabled {
		cacheConfig.Server.MaxCachePct = 0
	} else if cacheConfig.Memory.MaxCachePct > 0 {
		cacheConfig.Server.MaxCachePct = cacheConfig.Memory.MaxCachePct
	}

	if cacheConfig.Client.NTopHosts == 0 {
		cacheConfig.Client.NTopHosts = cacheDefaultNTopHosts
	}
	if cacheConfig.Client.MinRetryLengthBytes == 0 {
		cacheConfig.Client.MinRetryLengthBytes = cacheDefaultMinRetryBytes
	}
	if cacheConfig.Client.MaxGetContentAttempts == 0 {
		cacheConfig.Client.MaxGetContentAttempts = cacheDefaultGetAttempts
	}

	applyCacheRedisConfig(&cacheConfig, config.Database.Redis)
	return cacheConfig
}

func applyCacheRedisConfig(cacheConfig *cache.Config, redisConfig types.RedisConfig) {
	if len(redisConfig.Addrs) > 0 {
		cacheConfig.Server.Metadata.RedisAddr = redisConfig.Addrs[0]
	}

	cacheConfig.Server.Metadata.RedisPasswd = redisConfig.Password
	cacheConfig.Server.Metadata.RedisTLSEnabled = redisConfig.EnableTLS
	switch redisConfig.Mode {
	case types.RedisModeCluster:
		cacheConfig.Server.Metadata.RedisMode = cache.RedisModeCluster
	default:
		cacheConfig.Server.Metadata.RedisMode = cache.RedisModeSingle
	}
}

func cacheLocality(config types.AppConfig, poolConfig types.WorkerPoolConfig) string {
	if locality := os.Getenv("CACHE_LOCALITY"); locality != "" {
		return locality
	}
	if poolConfig.ConfigGroup != "" {
		return poolConfig.ConfigGroup
	}
	if config.Cache.Global.DefaultLocality != "" {
		return config.Cache.Global.DefaultLocality
	}
	return cacheDefaultLocality
}

func cacheNodeID() string {
	for _, env := range []string{"CACHE_NODE_ID", "NETWORK_PREFIX"} {
		if value := os.Getenv(env); value != "" {
			return value
		}
	}

	if hostname, err := os.Hostname(); err == nil && hostname != "" {
		return hostname
	}

	return "unknown"
}

func cacheHostNetwork(config types.AppConfig) bool {
	if value := os.Getenv("CACHE_HOST_NETWORK"); value != "" {
		enabled, err := strconv.ParseBool(value)
		return err == nil && enabled
	}

	return config.Worker.HostNetwork
}

func cachePrimaryLeaseKey(locality, nodeID string) string {
	return fmt.Sprintf("%s:%s:%s", cachePrimaryLeaseKeyPrefix, safeCacheName(locality), safeCacheName(nodeID))
}

func cacheReplicaLeaseKey(locality, nodeID string, slot int) string {
	return fmt.Sprintf("%s:%s:%s:%d", cacheReplicaLeaseKeyPrefix, safeCacheName(locality), safeCacheName(nodeID), slot)
}

func cacheLeaseTTL(cacheConfig cache.Config) time.Duration {
	if cacheConfig.Embedded.LeaseTTLSeconds <= 0 {
		return cachePrimaryLeaseTTL
	}
	return time.Duration(cacheConfig.Embedded.LeaseTTLSeconds) * time.Second
}

func cacheLeaseRefresh(cacheConfig cache.Config) time.Duration {
	if cacheConfig.Embedded.LeaseRefreshSeconds <= 0 {
		return cachePrimaryRefresh
	}
	return time.Duration(cacheConfig.Embedded.LeaseRefreshSeconds) * time.Second
}

func cacheLeaseRetry(cacheConfig cache.Config) time.Duration {
	if cacheConfig.Embedded.LeaseRetrySeconds <= 0 {
		return cachePrimaryRetry
	}
	return time.Duration(cacheConfig.Embedded.LeaseRetrySeconds) * time.Second
}

func cacheNodeHostID(locality, nodeID string) string {
	return fmt.Sprintf("cache-host-%s-%s", safeCacheName(locality), safeCacheName(nodeID))
}

func cacheWorkerInstanceID(workerID string) string {
	for _, env := range []string{"POD_UID", "POD_HOSTNAME", "HOSTNAME"} {
		if value := os.Getenv(env); value != "" {
			return value
		}
	}

	return workerID
}

func cacheWorkerHostID(locality, nodeID, workerID, instanceID string) string {
	prefix := fmt.Sprintf("cache-host-%s-%s-%s", safeCacheName(locality), safeCacheName(nodeID), safeCacheName(workerID))
	if instanceID == "" || instanceID == workerID {
		return prefix
	}

	return fmt.Sprintf("%s-%s", prefix, safeCacheName(instanceID))
}

var cacheNameRe = regexp.MustCompile(`[^a-zA-Z0-9_.-]+`)

func safeCacheName(value string) string {
	value = cacheNameRe.ReplaceAllString(value, "-")
	if value == "" {
		return "default"
	}
	return value
}
