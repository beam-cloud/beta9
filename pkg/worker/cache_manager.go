package worker

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/beam-cloud/beta9/pkg/cache"
	"github.com/beam-cloud/beta9/pkg/common"
	repo "github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/rs/zerolog/log"
)

const (
	cacheDefaultLocality                = "default"
	cacheDefaultDiskPath                = types.AgentCachePath
	cacheDefaultServerPort              = 2050
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
	cacheDefaultSmallRangeCopyBytes     = 128 * 1024
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
	cacheServerDaemonSetMarkerName      = ".beta9-cache-server-daemonset"

	cacheDefaultReconcileIntervalS      = 60
	cacheDefaultReconcileRecentStubTTLS = cache.DefaultReconcileRecentStubTTLS
	cacheDefaultReconcileLockTTLS       = 300
	cacheDefaultReconcileMaxStubsCycle  = 256
	cacheDefaultVolumeReportMinBytes    = 128 * 1024 * 1024
)

type WorkerCacheManager struct {
	ctx                   context.Context
	cancel                context.CancelFunc
	config                types.AppConfig
	poolConfig            types.WorkerPoolConfig
	workerRepo            pb.WorkerRepositoryServiceClient
	eventRepo             repo.EventRepository
	containerInstances    *common.SafeMap[*ContainerInstance]
	workerID              string
	instanceID            string
	poolName              string
	podAddr               string
	nodeID                string
	locality              string
	accelerator           string
	checkpointRoot        string
	cacheIdentityPath     string
	metadataStore         cache.CacheMetadataStore
	reporter              *cacheContentReporter
	originCredsMu         sync.Mutex
	originCredsCache      map[string]*originCredentials
	reconcileFailuresMu   sync.Mutex
	reconcileFailures     map[string]time.Time
	reconcileNow          chan struct{}
	client                *cache.Client
	server                *cache.Server
	serverLock            *os.File
	registration          *gatewayCacheRegistration
	registrationCancel    context.CancelFunc
	registrationDone      <-chan struct{}
	daemonSetPresenceDone <-chan struct{}
	draining              bool
	mu                    sync.Mutex
	wg                    sync.WaitGroup
	drainOnce             sync.Once
	drainErr              error
}

func NewWorkerCacheManager(ctx context.Context, config types.AppConfig, poolConfig types.WorkerPoolConfig, workerRepo pb.WorkerRepositoryServiceClient, eventRepo repo.EventRepository, containerInstances *common.SafeMap[*ContainerInstance], workerID, poolName, podAddr string) *WorkerCacheManager {
	cacheCtx, cancel := context.WithCancel(ctx)
	locality := cacheLocality(config, poolConfig)
	nodeID := cacheNodeID()

	return &WorkerCacheManager{
		ctx:                cacheCtx,
		cancel:             cancel,
		config:             config,
		poolConfig:         poolConfig,
		workerRepo:         workerRepo,
		eventRepo:          eventRepo,
		containerInstances: containerInstances,
		workerID:           workerID,
		instanceID:         cacheWorkerInstanceID(workerID),
		poolName:           poolName,
		podAddr:            podAddr,
		nodeID:             nodeID,
		locality:           locality,
		accelerator:        cacheAccelerator(poolConfig),
		originCredsCache:   make(map[string]*originCredentials),
		reconcileFailures:  make(map[string]time.Time),
		reconcileNow:       make(chan struct{}, 1),
	}
}

// ContentReporter returns the required-content reporter, or nil when
// reconciliation is disabled or unavailable. Callers must tolerate nil.
func (m *WorkerCacheManager) ContentReporter() *cacheContentReporter {
	if m == nil {
		return nil
	}
	return m.reporter
}

func (m *WorkerCacheManager) Start() (*cache.Client, error) {
	if !m.enabled() {
		return nil, nil
	}
	if m.workerRepo == nil {
		return nil, errors.New("cache coordinator client is required")
	}

	cacheConfig := normalizeCacheConfig(m.config, m.poolConfig, m.nodeID, m.locality)
	m.cacheIdentityPath = cachePlacementIdentityPath(m.config, cacheConfig)
	m.checkpointRoot = checkpointRootFromCacheConfig(cacheConfig)
	if err := os.MkdirAll(m.checkpointRoot, 0755); err != nil {
		m.cancel()
		return nil, err
	}
	metadataStore := newGatewayCacheMetadataStore(m.workerRepo)
	m.metadataStore = metadataStore

	hostDirectory := &gatewayCacheHostDirectory{
		client: m.workerRepo,
	}
	client, err := cache.NewClientWithHostDirectory(m.ctx, cacheConfig, metadataStore, hostDirectory, m.locality)
	if err != nil {
		m.cancel()
		return nil, err
	}

	m.client = client
	hostID := cacheLogicalHostID(m.locality, m.nodeID, m.cacheIdentityPath)
	nodeCacheServer := m.nodeCacheServer(cacheConfig, hostID)
	nodeCacheServer.StartDaemonSetPresence()
	startedCacheServer, err := nodeCacheServer.Start()
	if err != nil {
		_ = client.Cleanup()
		m.cancel()
		return nil, err
	}
	if !startedCacheServer || !cacheServerOnlyMode() {
		nodeCacheServer.Watch()
	}

	if err := client.WaitForHosts(defaultCacheWaitTime); err != nil {
		log.Warn().
			Err(err).
			Str("locality", m.locality).
			Str("node_id", m.nodeID).
			Msg("cache has no available hosts yet")
	}

	m.startReconciliation(cacheConfig)

	return client, nil
}

// startReconciliation wires up required-content reporting and the async
// reconciliation loop when enabled. It is non-blocking and degrades to a no-op
// when Redis metadata is not configured, preserving prior startup behavior.
func (m *WorkerCacheManager) startReconciliation(cacheConfig cache.Config) {
	recCfg := cacheConfig.Reconciliation
	if !recCfg.Enabled {
		return
	}
	if m.metadataStore == nil {
		log.Warn().Msg("cache reconciliation disabled: cache metadata store is unavailable")
		return
	}
	if m.eventRepo == nil {
		log.Warn().Msg("cache reconciliation disabled: event repository is unavailable")
		return
	}

	m.reporter = newCacheContentReporter(
		m.ctx,
		m.eventRepo,
		m.metadataStore,
		m.locality,
		m.recentStubTTL(),
		m.cacheVolumeReportMinBytes(),
		m.activeStubsForWorkspace,
	)

	m.wg.Add(1)
	go m.runReconciliation()

	log.Info().
		Str("locality", m.locality).
		Dur("interval", m.reconcileInterval()).
		Dur("recent_stub_ttl", m.recentStubTTL()).
		Bool("origin_fallback", recCfg.OriginFallbackEnabled).
		Msg("cache required-content reconciliation enabled")
}

// cacheVolumeReportMinBytes is the geesefs object size threshold above which
// volume content is reported as required content for reconciliation.
func (m *WorkerCacheManager) cacheVolumeReportMinBytes() int64 {
	if m != nil && m.config.Cache.Reconciliation.VolumeMinBytes > 0 {
		return m.config.Cache.Reconciliation.VolumeMinBytes
	}
	return cacheDefaultVolumeReportMinBytes
}

func (m *WorkerCacheManager) Close() error {
	var errs error
	errs = errors.Join(errs, m.Drain())
	m.cancel()

	m.mu.Lock()
	server := m.server
	client := m.client
	lock := m.serverLock
	daemonSetPresenceDone := m.daemonSetPresenceDone
	m.server = nil
	m.serverLock = nil
	m.registration = nil
	m.registrationCancel = nil
	m.registrationDone = nil
	m.daemonSetPresenceDone = nil
	m.mu.Unlock()

	if client != nil {
		errs = errors.Join(errs, client.Cleanup())
	}
	if server != nil {
		errs = errors.Join(errs, server.Close())
	}
	if lock != nil {
		errs = errors.Join(errs, releaseCacheServerLock(lock))
	}
	if daemonSetPresenceDone != nil {
		select {
		case <-daemonSetPresenceDone:
		case <-time.After(cacheCoordinatorRPCTimeout):
			errs = errors.Join(errs, errors.New("timed out waiting for cache server daemonset marker loop to stop"))
		}
	}

	m.wg.Wait()
	return errs
}

func (m *WorkerCacheManager) runningCacheServer() bool {
	if m == nil {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.server != nil
}

func (m *WorkerCacheManager) CheckpointRoot() string {
	if m == nil {
		return ""
	}
	return m.checkpointRoot
}

func (m *WorkerCacheManager) requestReconcile() {
	if m == nil || m.reconcileNow == nil {
		return
	}
	select {
	case m.reconcileNow <- struct{}{}:
	default:
	}
}

func (m *WorkerCacheManager) Drain() error {
	if m == nil {
		return nil
	}

	m.drainOnce.Do(func() {
		m.mu.Lock()
		m.draining = true
		cancel := m.registrationCancel
		registration := m.registration
		registrationDone := m.registrationDone
		server := m.server
		m.mu.Unlock()

		if server != nil {
			server.Drain()
		}
		if cancel != nil {
			cancel()
		}
		if registrationDone != nil {
			select {
			case <-registrationDone:
			case <-time.After(cacheCoordinatorRPCTimeout):
				m.drainErr = errors.Join(m.drainErr, errors.New("timed out waiting for cache registration loop to stop"))
			}
		}

		if registration != nil {
			m.drainErr = errors.Join(m.drainErr, registration.unregister(context.Background()))
		}
	})

	return m.drainErr
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

	advertisedAddr, err := server.Serve(m.bindAddr(cacheConfig), m.cacheAdvertiseHost())
	if err != nil {
		_ = server.Close()
		return nil, "", err
	}

	return server, advertisedAddr, nil
}

type nodeCacheServer struct {
	manager *WorkerCacheManager
	config  cache.Config
	hostID  string
}

func (m *WorkerCacheManager) nodeCacheServer(cacheConfig cache.Config, hostID string) nodeCacheServer {
	return nodeCacheServer{
		manager: m,
		config:  cacheConfig,
		hostID:  hostID,
	}
}

func (s nodeCacheServer) Start() (bool, error) {
	m := s.manager
	m.mu.Lock()
	if m.draining || m.server != nil {
		m.mu.Unlock()
		return m.server != nil, nil
	}
	m.mu.Unlock()

	if !cacheServerOnlyMode() && cacheServerDaemonSetMarkerFresh(s.config.Server.DiskCacheDir, cacheRegistrationTTL(s.config)) {
		return false, nil
	}

	lock, acquired, err := acquireCacheServerLock(s.config.Server.DiskCacheDir)
	if err != nil || !acquired {
		return false, err
	}

	server, advertisedAddr, err := m.createEmbeddedServer(s.config, s.hostID)
	if err != nil {
		_ = releaseCacheServerLock(lock)
		return false, err
	}

	registration := newGatewayCacheRegistration(m, server, s.config, advertisedAddr)
	if err := registration.registerOnce(m.ctx); err != nil {
		_ = server.Close()
		_ = releaseCacheServerLock(lock)
		return false, err
	}

	registrationCtx, registrationCancel := context.WithCancel(m.ctx)
	m.mu.Lock()
	if m.draining || m.server != nil {
		m.mu.Unlock()
		registrationCancel()
		_ = registration.unregister(context.Background())
		_ = server.Close()
		_ = releaseCacheServerLock(lock)
		return false, nil
	}
	m.server = server
	m.serverLock = lock
	m.registration = registration
	m.registrationCancel = registrationCancel
	registrationDone := make(chan struct{})
	m.registrationDone = registrationDone
	m.wg.Add(1)
	m.mu.Unlock()

	if m.client != nil {
		m.client.AttachLocalServer(server)
	}

	go func() {
		defer m.wg.Done()
		defer close(registrationDone)
		runGatewayCacheRegistration(registrationCtx, registration)
	}()

	log.Info().
		Str("logical_host_id", registration.logicalHostID).
		Str("registration_id", registration.registrationID).
		Msg("acquired node-local cache server lock")

	return true, nil
}

func (s nodeCacheServer) StartDaemonSetPresence() {
	if !cacheServerOnlyMode() {
		return
	}

	m := s.manager
	interval := cacheRegistrationHeartbeat(s.config)
	if interval <= 0 {
		interval = cacheDefaultRegistrationHeartbeat
	}

	done := make(chan struct{})
	m.mu.Lock()
	m.daemonSetPresenceDone = done
	m.mu.Unlock()

	if err := writeCacheServerDaemonSetMarker(s.config.Server.DiskCacheDir, m.workerID, m.instanceID); err != nil {
		log.Warn().Err(err).Msg("failed to write cache server daemonset marker")
	}

	go func() {
		defer close(done)

		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-m.ctx.Done():
				return
			case <-ticker.C:
				if err := writeCacheServerDaemonSetMarker(s.config.Server.DiskCacheDir, m.workerID, m.instanceID); err != nil {
					log.Warn().Err(err).Msg("failed to refresh cache server daemonset marker")
				}
			}
		}
	}()
}

func (s nodeCacheServer) Watch() {
	m := s.manager
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()

		interval := time.Duration(s.config.Coordinator.HostWatchIntervalSeconds) * time.Second
		if interval <= 0 {
			interval = cacheDefaultHostWatchInterval
		}
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-m.ctx.Done():
				return
			case <-ticker.C:
				m.mu.Lock()
				draining := m.draining
				running := m.server != nil
				m.mu.Unlock()
				if draining {
					return
				}
				if !cacheServerOnlyMode() && cacheServerDaemonSetMarkerFresh(s.config.Server.DiskCacheDir, cacheRegistrationTTL(s.config)) {
					if running {
						if err := m.stopNodeCacheServer("cache server daemonset marker is fresh"); err != nil {
							log.Warn().Err(err).Msg("failed to stop embedded cache server")
						}
					}
					continue
				}
				if running {
					if cacheServerOnlyMode() {
						return
					}
					continue
				}
				startedCacheServer, err := s.Start()
				if err != nil {
					log.Warn().Err(err).Msg("failed to start standby cache server")
					continue
				}
				if startedCacheServer && cacheServerOnlyMode() {
					return
				}
			}
		}
	}()
}

func (m *WorkerCacheManager) stopNodeCacheServer(reason string) error {
	m.mu.Lock()
	server := m.server
	lock := m.serverLock
	registration := m.registration
	cancel := m.registrationCancel
	done := m.registrationDone
	if server == nil && lock == nil && registration == nil {
		m.mu.Unlock()
		return nil
	}
	m.server = nil
	m.serverLock = nil
	m.registration = nil
	m.registrationCancel = nil
	m.registrationDone = nil
	m.mu.Unlock()

	var errs error
	if registration != nil && m.client != nil {
		m.client.DetachLocalServer(registration.logicalHostID)
	}
	if server != nil {
		server.Drain()
	}
	if cancel != nil {
		cancel()
	}
	if done != nil {
		select {
		case <-done:
		case <-time.After(cacheCoordinatorRPCTimeout):
			errs = errors.Join(errs, errors.New("timed out waiting for cache registration loop to stop"))
		}
	}
	if registration != nil {
		errs = errors.Join(errs, registration.unregister(context.Background()))
	}
	if server != nil {
		errs = errors.Join(errs, server.Close())
	}
	if lock != nil {
		errs = errors.Join(errs, releaseCacheServerLock(lock))
	}
	if errs == nil {
		log.Info().Str("reason", reason).Msg("stopped embedded cache server")
	}
	return errs
}

func acquireCacheServerLock(cacheDir string) (*os.File, bool, error) {
	if err := os.MkdirAll(cacheDir, 0o755); err != nil {
		return nil, false, err
	}
	lockPath := filepath.Join(cacheDir, ".cache-server.lock")
	lock, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, false, err
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = lock.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return lock, true, nil
}

func writeCacheServerDaemonSetMarker(cacheDir, workerID, instanceID string) error {
	if err := os.MkdirAll(cacheDir, 0o755); err != nil {
		return err
	}
	markerPath := cacheServerDaemonSetMarkerPath(cacheDir)
	tmp, err := os.CreateTemp(cacheDir, cacheServerDaemonSetMarkerName+".*.tmp")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer func() {
		_ = os.Remove(tmpPath)
	}()

	body := fmt.Sprintf("worker_id=%s\ninstance_id=%s\nupdated_unix_nano=%d\n", workerID, instanceID, time.Now().UnixNano())
	if _, err := tmp.WriteString(body); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpPath, markerPath)
}

func cacheServerDaemonSetMarkerFresh(cacheDir string, ttl time.Duration) bool {
	if ttl <= 0 {
		ttl = cacheDefaultRegistrationTTL
	}
	info, err := os.Stat(cacheServerDaemonSetMarkerPath(cacheDir))
	if err != nil {
		return false
	}
	return time.Since(info.ModTime()) <= ttl
}

func cacheServerDaemonSetMarkerPath(cacheDir string) string {
	return filepath.Join(cacheDir, cacheServerDaemonSetMarkerName)
}

func releaseCacheServerLock(lock *os.File) error {
	if lock == nil {
		return nil
	}
	errs := errors.Join(syscall.Flock(int(lock.Fd()), syscall.LOCK_UN), lock.Close())
	return errs
}

func (m *WorkerCacheManager) bindAddr(cacheConfig cache.Config) string {
	port := cacheConfig.Global.ServerPort
	if port == 0 {
		port = cacheDefaultServerPort
	}

	// Host-network embedded workers default to an ephemeral port to avoid
	// collisions when many share a node (the advertised address carries the real
	// port). The one-per-node cache-server daemonset always binds the fixed port,
	// as does any worker with an explicitly configured fixed port.
	if cacheHostNetwork(m.config) && !cacheServerOnlyMode() && !m.fixedCacheServerPortConfigured() {
		return ":0"
	}

	return fmt.Sprintf(":%d", port)
}

// cacheAdvertiseHost returns the address other cache clients should use to reach
// this node's cache server. It must be an IP (the node's private IP), never a
// hostname: cache clients resolve via cluster DNS, not tailscale MagicDNS, so a
// tailscale POD_HOSTNAME would be unresolvable and the host would look down.
// It prefers an explicit pod IP and otherwise returns "" so the cache server
// falls back to its own discovered private IP.
func (m *WorkerCacheManager) cacheAdvertiseHost() string {
	if net.ParseIP(m.podAddr) != nil {
		return m.podAddr
	}
	if ip, err := getIPFromEnv(types.WorkerPodIPEnv); err == nil {
		return ip
	}
	return ""
}

// fixedCacheServerPortConfigured reports whether a cache server port was
// explicitly set (via the config or the CACHE_SERVER_PORT env), as opposed to
// falling back to the default. When set, it is honored even for embedded
// host-network workers.
func (m *WorkerCacheManager) fixedCacheServerPortConfigured() bool {
	return m.config.Cache.Global.ServerPort > 0 || cacheServerPortFromEnv() > 0
}

func normalizeCacheConfig(config types.AppConfig, poolConfig types.WorkerPoolConfig, nodeID, locality string) cache.Config {
	cacheConfig := config.Cache

	if cacheConfig.Global.DefaultLocality == "" {
		cacheConfig.Global.DefaultLocality = cacheDefaultLocality
	}
	if cacheConfig.Global.ServerPort == 0 {
		cacheConfig.Global.ServerPort = cacheDefaultServerPort
	}
	// CACHE_SERVER_PORT lets the deployment (e.g. the cache-server daemonset) pin
	// the listen port without editing the shared config secret. It takes
	// precedence over the configured/default port.
	if port := cacheServerPortFromEnv(); port > 0 {
		cacheConfig.Global.ServerPort = port
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
	cacheConfig.Disk.MountPath = filepath.Clean(cacheConfig.Disk.MountPath)
	cacheConfig.Disk.HostPath = filepath.Clean(cacheConfig.Disk.HostPath)

	if cacheConfig.Server.DiskCacheDir == "" {
		cacheConfig.Server.DiskCacheDir = filepath.Join(cacheConfig.Disk.MountPath, safeCacheName(locality), safeCacheName(nodeID))
	} else {
		cacheConfig.Server.DiskCacheDir = filepath.Clean(cacheConfig.Server.DiskCacheDir)
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
	if cacheConfig.Server.SmallRangeCopyThresholdBytes == 0 {
		cacheConfig.Server.SmallRangeCopyThresholdBytes = cacheDefaultSmallRangeCopyBytes
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

	// Reconciliation defaults are applied at read time by the manager's
	// reconcile* helpers (the single source of truth), so they are intentionally
	// not normalized here.

	return cacheConfig
}

func checkpointRootFromCacheConfig(cacheConfig cache.Config) string {
	return filepath.Join(cacheConfig.Server.DiskCacheDir, "checkpoints")
}

func cachePlacementIdentityPath(config types.AppConfig, cacheConfig cache.Config) string {
	if config.Cache.Server.DiskCacheDir != "" {
		return filepath.Clean(cacheConfig.Server.DiskCacheDir)
	}
	return cacheCanonicalPhysicalIdentityPath(cacheConfig)
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
	if locality := os.Getenv(types.CacheLocalityEnv); locality != "" {
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
	for _, env := range []string{types.CacheNodeEnv, types.WorkerNetworkPrefixEnv} {
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
	if value := os.Getenv(types.CacheHostNetworkEnv); value != "" {
		enabled, err := strconv.ParseBool(value)
		return err == nil && enabled
	}

	return config.Worker.HostNetwork
}

func cacheServerOnlyMode() bool {
	enabled, err := strconv.ParseBool(os.Getenv(types.CacheServerOnlyEnv))
	return err == nil && enabled
}

func cacheAccelerator(poolConfig types.WorkerPoolConfig) string {
	accelerator := os.Getenv(types.WorkerGPUEnv)
	if accelerator == "" {
		accelerator = poolConfig.GPUType
	}
	if accelerator == "" {
		return "CPU"
	}
	return strings.ToUpper(accelerator)
}

// cacheServerPortFromEnv returns the cache server listen port set via
// CACHE_SERVER_PORT, or 0 when unset/invalid. It lets the deployment pin the
// port without editing the shared config secret.
func cacheServerPortFromEnv() uint {
	value := os.Getenv(types.CacheServerPortEnv)
	if value == "" {
		return 0
	}
	port, err := strconv.ParseUint(value, 10, 16)
	if err != nil {
		return 0
	}
	return uint(port)
}

func cacheWorkerInstanceID(workerID string) string {
	for _, env := range []string{types.WorkerPodUIDEnv, types.WorkerPodHostEnv, types.WorkerHostnameEnv} {
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
