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
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
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
	cacheDefaultPageSizeBytes           = 4 * 1024 * 1024
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
	cacheDefaultRegistrationTTL         = 30 * time.Second
	cacheDefaultRegistrationHeartbeat   = 10 * time.Second
	cacheDefaultHostWatchInterval       = 5 * time.Second
)

type WorkerCacheManager struct {
	ctx           context.Context
	cancel        context.CancelFunc
	config        types.AppConfig
	poolConfig    types.WorkerPoolConfig
	workerRepo    pb.WorkerRepositoryServiceClient
	workerID      string
	instanceID    string
	poolName      string
	podAddr       string
	nodeID        string
	locality      string
	metadataStore cache.CacheMetadataStore
	client        *cache.Client
	server        *cache.Server
	registration  *gatewayCacheRegistration
	mu            sync.Mutex
	wg            sync.WaitGroup
}

func NewWorkerCacheManager(ctx context.Context, config types.AppConfig, poolConfig types.WorkerPoolConfig, workerRepo pb.WorkerRepositoryServiceClient, workerID, poolName, podAddr string) *WorkerCacheManager {
	cacheCtx, cancel := context.WithCancel(ctx)
	locality := cacheLocality(config, poolConfig)
	nodeID := cacheNodeID()

	return &WorkerCacheManager{
		ctx:        cacheCtx,
		cancel:     cancel,
		config:     config,
		poolConfig: poolConfig,
		workerRepo: workerRepo,
		workerID:   workerID,
		instanceID: cacheWorkerInstanceID(workerID),
		poolName:   poolName,
		podAddr:    podAddr,
		nodeID:     nodeID,
		locality:   locality,
	}
}

func (m *WorkerCacheManager) Start() (*cache.Client, error) {
	if !m.enabled() {
		return nil, nil
	}
	if m.workerRepo == nil {
		return nil, errors.New("cache coordinator client is required")
	}

	cacheConfig := normalizeCacheConfig(m.config, m.poolConfig, m.nodeID, m.locality)
	metadataStore := newGatewayCacheMetadataStore(m.workerRepo)
	m.metadataStore = metadataStore

	hostID := cacheLogicalHostID(m.poolName, m.locality, m.nodeID, cacheConfig.Server.DiskCacheDir)
	server, advertisedAddr, err := m.createEmbeddedServer(cacheConfig, hostID)
	if err != nil {
		m.cancel()
		return nil, err
	}

	registration := newGatewayCacheRegistration(m, server, cacheConfig, advertisedAddr)
	if err := registration.registerOnce(m.ctx); err != nil {
		_ = server.Close()
		m.cancel()
		return nil, err
	}
	m.registration = registration
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		runGatewayCacheRegistration(m.ctx, registration)
	}()

	hostDirectory := &gatewayCacheHostDirectory{
		client:   m.workerRepo,
		poolName: m.poolName,
	}
	client, err := cache.NewClientWithHostDirectory(m.ctx, cacheConfig, metadataStore, hostDirectory, m.locality)
	if err != nil {
		_ = registration.unregister(context.Background())
		_ = server.Close()
		m.cancel()
		return nil, err
	}
	if localStore, err := cache.NewStore(
		m.ctx,
		&cache.Host{HostId: cacheLocalReaderHostID(m.locality, m.nodeID, m.workerID, m.instanceID)},
		m.locality,
		metadataStore,
		localReaderCacheConfig(cacheConfig),
	); err != nil {
		log.Warn().Err(err).Str("locality", m.locality).Str("node_id", m.nodeID).Msg("Failed to attach local cache reader")
	} else {
		client.AttachLocalStore(localStore)
		log.Info().
			Str("cache_dir", cacheConfig.Server.DiskCacheDir).
			Str("locality", m.locality).
			Str("node_id", m.nodeID).
			Msg("Attached local cache reader")
	}

	m.client = client
	client.AttachLocalServer(server)

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
	registration := m.registration
	m.mu.Unlock()

	if registration != nil {
		errs = errors.Join(errs, registration.unregister(context.Background()))
	}
	if client != nil {
		errs = errors.Join(errs, client.Cleanup())
	}
	if server != nil {
		errs = errors.Join(errs, server.Close())
	}

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

func (m *WorkerCacheManager) createEmbeddedServer(cacheConfig cache.Config, hostID string) (*cache.Server, string, error) {
	server, err := cache.NewServerWithOptions(
		m.ctx,
		cacheConfig,
		m.locality,
		cache.WithServerMetadataStore(m.metadataStore),
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
	if cacheConfig.Coordinator.RegistrationTTLSeconds == 0 {
		cacheConfig.Coordinator.RegistrationTTLSeconds = int(cacheDefaultRegistrationTTL / time.Second)
	}
	if cacheConfig.Coordinator.HeartbeatIntervalSeconds == 0 {
		cacheConfig.Coordinator.HeartbeatIntervalSeconds = int(cacheDefaultRegistrationHeartbeat / time.Second)
	}
	if cacheConfig.Coordinator.HostWatchIntervalSeconds == 0 {
		cacheConfig.Coordinator.HostWatchIntervalSeconds = int(cacheDefaultHostWatchInterval / time.Second)
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

func cacheLocalReaderHostID(locality, nodeID, workerID, instanceID string) string {
	return fmt.Sprintf("%s:%s:%s:%s:reader", locality, nodeID, workerID, instanceID)
}

func localReaderCacheConfig(cacheConfig cache.Config) cache.Config {
	cacheConfig.Server.MaxCachePct = 0
	cacheConfig.Memory.Enabled = false
	cacheConfig.Client.Prefetch.Enabled = false
	cacheConfig.Metrics.URL = ""
	return cacheConfig
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

func cacheWorkerInstanceID(workerID string) string {
	for _, env := range []string{"POD_UID", "POD_HOSTNAME", "HOSTNAME"} {
		if value := os.Getenv(env); value != "" {
			return value
		}
	}

	return workerID
}

var cacheNameRe = regexp.MustCompile(`[^a-zA-Z0-9_.-]+`)

func safeCacheName(value string) string {
	value = cacheNameRe.ReplaceAllString(value, "-")
	if value == "" {
		return "default"
	}
	return value
}
