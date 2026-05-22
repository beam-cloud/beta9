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
	cacheDefaultPageSizeBytes           = 1024 * 1024
	cacheDefaultDiskMaxUsage            = 0.95
	cacheDefaultNTopHosts               = 3
	cacheDefaultMinRetryBytes           = 0
	cacheDefaultGetAttempts             = 3
	cacheDefaultPageFileBuckets         = 1024
	cacheDefaultPageFDCacheSize         = 64
	cacheDefaultRawMaxActiveConns       = 64
	cacheDefaultRawMaxIdleConns         = 16
	cacheDefaultPrefetchAheadBytes      = 64 * 1024 * 1024
	cacheDefaultPrefetchWorkers         = 4
	cacheDefaultPrefetchPartLength      = 4 * 1024 * 1024
	cacheDefaultPrefetchMaxParts        = 16
	cacheDefaultGRPCPayloadCodecMin     = 64 * 1024
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
	poolConfig     types.WorkerPoolConfig
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
		poolConfig: poolConfig,
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

	cacheConfig := normalizeCacheConfig(m.config, m.poolConfig, m.nodeID, m.locality)
	registry := cache.NewRedisRegistryWithClient(cacheConfig.Global, cacheConfig.Server, m.redis.UniversalClient)
	m.registry = registry

	client, err := cache.NewClientWithRegistry(m.ctx, cacheConfig, registry, m.locality)
	if err != nil {
		m.cancel()
		return nil, err
	}

	m.client = client
	switch cacheConfig.Embedded.Mode {
	case cache.EmbeddedModeActiveActive:
		m.wg.Add(1)
		go m.runReplicaElection(cacheConfig)
	case cache.EmbeddedModeSinglePrimary:
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
	if !m.config.Cache.Enabled || !m.config.Worker.CacheEnabled {
		return false
	}
	if m.poolConfig.Cache.Enabled != nil && !*m.poolConfig.Cache.Enabled {
		return false
	}
	if m.poolConfig.Cache.Disk.Enabled != nil {
		return *m.poolConfig.Cache.Disk.Enabled
	}
	return m.config.Cache.Disk.Enabled
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

type embeddedCacheLeaseOptions struct {
	leaseKey            string
	hostID              string
	leaseTTL            time.Duration
	refreshInterval     time.Duration
	startFailureMessage string
	startedMessage      string
	refreshErrorMessage string
	lostMessage         string
	logHostID           bool
	logLeaseKey         bool
}

func (m *WorkerCacheManager) runReplica(cacheConfig cache.Config, leaseKey string) {
	hostID := cacheWorkerHostID(m.locality, m.nodeID, m.workerID, m.instanceID)
	m.runLeasedServer(cacheConfig, embeddedCacheLeaseOptions{
		leaseKey:            leaseKey,
		hostID:              hostID,
		leaseTTL:            cacheLeaseTTL(cacheConfig),
		refreshInterval:     cacheLeaseRefresh(cacheConfig),
		startFailureMessage: "failed to start embedded cache replica",
		startedMessage:      "embedded cache replica started",
		refreshErrorMessage: "failed to refresh embedded cache replica lease",
		lostMessage:         "lost embedded cache replica lease",
		logHostID:           true,
		logLeaseKey:         true,
	})
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
	m.runLeasedServer(cacheConfig, embeddedCacheLeaseOptions{
		leaseKey:            m.leaseKey,
		hostID:              cacheNodeHostID(m.locality, m.nodeID),
		leaseTTL:            cachePrimaryLeaseTTL,
		refreshInterval:     cachePrimaryRefresh,
		startFailureMessage: "failed to start embedded cache primary",
		startedMessage:      "embedded cache server elected primary",
		refreshErrorMessage: "failed to refresh cache primary lease",
		lostMessage:         "lost cache primary lease",
		logLeaseKey:         true,
	})
}

func (m *WorkerCacheManager) runLeasedServer(cacheConfig cache.Config, opts embeddedCacheLeaseOptions) {
	server, advertisedAddr, err := m.createEmbeddedServer(cacheConfig, opts.hostID)
	if err != nil {
		event := log.Warn().Err(err)
		if opts.logLeaseKey {
			event = event.Str("lease_key", opts.leaseKey)
		}
		event.Msg(opts.startFailureMessage)
		_ = m.releaseLeaseKey(context.Background(), opts.leaseKey)
		return
	}

	m.mu.Lock()
	m.activeLeaseKey = opts.leaseKey
	m.mu.Unlock()
	if m.client != nil {
		m.client.AttachLocalServer(server)
	}

	event := log.Info().
		Str("addr", advertisedAddr).
		Str("locality", m.locality).
		Str("node_id", m.nodeID)
	if opts.logHostID {
		event = event.Str("host_id", opts.hostID)
	}
	if opts.logLeaseKey {
		event = event.Str("lease_key", opts.leaseKey)
	}
	event.Msg(opts.startedMessage)

	ticker := time.NewTicker(opts.refreshInterval)
	defer ticker.Stop()
	defer func() {
		if m.client != nil && server.Host() != nil {
			m.client.DetachLocalServer(server.Host().HostId)
		}
		_ = server.Close()
		_ = m.releaseLeaseKey(context.Background(), opts.leaseKey)
		m.mu.Lock()
		if m.server == server {
			m.server = nil
		}
		if m.activeLeaseKey == opts.leaseKey {
			m.activeLeaseKey = ""
		}
		m.mu.Unlock()
	}()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			ok, err := m.refreshLeaseKey(opts.leaseKey, opts.leaseTTL)
			if err != nil {
				log.Warn().Err(err).Str("lease_key", opts.leaseKey).Msg(opts.refreshErrorMessage)
				return
			}
			if !ok {
				log.Warn().Str("lease_key", opts.leaseKey).Msg(opts.lostMessage)
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

func normalizeCacheConfig(config types.AppConfig, poolConfig types.WorkerPoolConfig, nodeID, locality string) cache.Config {
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
	if cacheConfig.Global.GRPCPayloadCodecMinBytes == 0 {
		cacheConfig.Global.GRPCPayloadCodecMinBytes = cacheDefaultGRPCPayloadCodecMin
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
	applyWorkerPoolCacheOverrides(&cacheConfig, poolConfig)

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
	if cacheConfig.Server.PageFileBuckets == 0 {
		cacheConfig.Server.PageFileBuckets = cacheDefaultPageFileBuckets
	}
	cacheConfig.Server.ReadTransport.Enabled = true
	cacheConfig.Server.ReadTransport.Sendfile = true
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
	cacheConfig.Client.PreferLocalCacheHost = true
	cacheConfig.Client.ReadIntoEnabled = true
	if cacheConfig.Client.PageFDCacheSize == 0 {
		cacheConfig.Client.PageFDCacheSize = cacheDefaultPageFDCacheSize
	}
	cacheConfig.Client.ReadTransport.Enabled = true
	if cacheConfig.Client.ReadTransport.MaxActiveConnsPerHost == 0 {
		cacheConfig.Client.ReadTransport.MaxActiveConnsPerHost = cacheDefaultRawMaxActiveConns
	}
	if cacheConfig.Client.ReadTransport.MaxIdleConnsPerHost == 0 {
		cacheConfig.Client.ReadTransport.MaxIdleConnsPerHost = cacheDefaultRawMaxIdleConns
	}
	cacheConfig.Client.Prefetch.Enabled = true
	if cacheConfig.Client.Prefetch.AheadBytes == 0 {
		cacheConfig.Client.Prefetch.AheadBytes = cacheDefaultPrefetchAheadBytes
	}
	if cacheConfig.Client.Prefetch.Workers == 0 {
		cacheConfig.Client.Prefetch.Workers = cacheDefaultPrefetchWorkers
	}
	if cacheConfig.Client.Prefetch.PartLengthBytes == 0 {
		cacheConfig.Client.Prefetch.PartLengthBytes = cacheDefaultPrefetchPartLength
	}
	if cacheConfig.Client.Prefetch.MaxPartsPerRead == 0 {
		cacheConfig.Client.Prefetch.MaxPartsPerRead = cacheDefaultPrefetchMaxParts
	}

	applyCacheRedisConfig(&cacheConfig, config.Database.Redis)
	return cacheConfig
}

func applyWorkerPoolCacheOverrides(cacheConfig *cache.Config, poolConfig types.WorkerPoolConfig) {
	if poolConfig.Cache.Disk.Enabled != nil {
		cacheConfig.Disk.Enabled = *poolConfig.Cache.Disk.Enabled
	}
	if poolConfig.Cache.Disk.HostPath != "" {
		cacheConfig.Disk.HostPath = poolConfig.Cache.Disk.HostPath
	}
	if poolConfig.Cache.Disk.MountPath != "" {
		cacheConfig.Disk.MountPath = poolConfig.Cache.Disk.MountPath
	}
	if poolConfig.Cache.Disk.MaxUsagePct > 0 {
		cacheConfig.Disk.MaxUsagePct = poolConfig.Cache.Disk.MaxUsagePct
	}
	if poolConfig.Cache.Enabled != nil && !*poolConfig.Cache.Enabled {
		cacheConfig.Disk.Enabled = false
	}
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
