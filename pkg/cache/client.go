package cache

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/beam-cloud/beta9/pkg/cache/cachegrpc"
	proto "github.com/beam-cloud/beta9/proto"
	rendezvous "github.com/beam-cloud/rendezvous"
	"github.com/djherbis/atime"
	"github.com/hanwen/go-fuse/v2/fuse"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
)

// gRPC keepalive for cache peer connections so a connection broken by a cache
// server rollout is detected and replaced rather than left to hang. The client
// Time must be >= the cache server's keepalive EnforcementPolicy MinTime to
// avoid "too_many_pings" GOAWAYs.
const (
	cacheKeepaliveTime    = 20 * time.Second
	cacheKeepaliveTimeout = 10 * time.Second
	cacheKeepaliveMinTime = 10 * time.Second
)

const (
	getContentRequestTimeout        = 60 * time.Second
	getContentStreamRequestTimeout  = 600 * time.Second
	storeContentRequestTimeout      = 600 * time.Second
	storeContentLockWaitTimeout     = time.Duration(storeFromContentLockTtlS+5) * time.Second
	storeContentLockWaitInterval    = 500 * time.Millisecond
	closestHostTimeout              = 30 * time.Second
	localClientCacheCleanupInterval = 5 * time.Second
	localClientCacheTTL             = 600 * time.Second
	defaultRawReadWindowPartBytes   = 64 * 1024 * 1024
	defaultRawReadWindowMaxParts    = 8
	monitorHostFailureThreshold     = 2

	// NOTE: This value for readAheadKB is separate from the cachefs config since the FUSE library does
	// weird stuff with the other read_ahead_kb value internally
	readAheadKB = 32768
)

var rawReadWindowTraceSequence int64

type RendezvousHasher interface {
	Add(hosts ...*Host)
	Remove(host *Host)
	GetN(n int, key string) []*Host
}

type ClientOptions = struct {
	RoutingKey string
}

type StoreContentOptions struct {
	RoutingKey string
	Lock       bool
}

type LocalContentSource struct {
	Path      string
	CachePath string
}

type S3ContentSource struct {
	Path           string
	CachePath      string
	BucketName     string
	Region         string
	EndpointURL    string
	AccessKey      string
	SecretKey      string
	ForcePathStyle bool
}

// ClientLocalPageFileView describes a byte range inside a page file that is
// already present on this client/worker. It is intentionally not a remote
// address: consumers may mmap it, return it as a FUSE fd-backed response, or
// otherwise read it as a local file.
type ClientLocalPageFileView struct {
	Path   string
	Offset int64
	Length int
}

type Client struct {
	ctx                   context.Context
	locality              string
	clientConfig          ClientConfig
	globalConfig          GlobalConfig
	grpcClients           map[string]proto.CacheClient
	grpcConns             map[string]*grpc.ClientConn
	localServers          map[string]*Server
	localDiskStore        *Store
	localNodeID           string
	localCachePathID      string
	rawReadPools          map[string]*rawReadConnPool
	hostMap               *HostMap
	mu                    sync.RWMutex
	discoveryClient       *DiscoveryClient
	metadataStore         CacheMetadataStore
	localHostCache        map[localHostCacheKey]*localClientCache
	cachefsServer         *fuse.Server
	hasher                RendezvousHasher
	hostDirectory         HostDirectory
	minRetryLengthBytes   int64
	maxGetContentAttempts int64
	closed                bool
}

type localHostCacheKey struct {
	hash       string
	routingKey string
}

type localClientCache struct {
	host      *Host
	timestamp time.Time
}

func newLocalHostCacheKey(hash, routingKey string) localHostCacheKey {
	if routingKey == "" {
		routingKey = hash
	}
	return localHostCacheKey{hash: hash, routingKey: routingKey}
}

func NewClient(ctx context.Context, cfg Config) (*Client, error) {
	InitLogger(cfg.Global.DebugMode, cfg.Global.PrettyLogs)

	metadataStore, err := NewRedisCacheMetadataStore(cfg.Global, cfg.Server)
	if err != nil {
		return nil, err
	}

	locality := cfg.Global.GetLocality()
	return NewClientWithMetadataStore(ctx, cfg, metadataStore, locality)
}

func NewClientWithMetadataStore(ctx context.Context, cfg Config, metadataStore CacheMetadataStore, locality string) (*Client, error) {
	return NewClientWithHostDirectory(ctx, cfg, metadataStore, nil, locality)
}

func NewClientWithHostDirectory(ctx context.Context, cfg Config, metadataStore CacheMetadataStore, hostDirectory HostDirectory, locality string) (*Client, error) {
	InitLogger(cfg.Global.DebugMode, cfg.Global.PrettyLogs)
	startCachePathStatsLogger()

	if locality == "" {
		locality = cfg.Global.GetLocality()
	}
	if hostDirectory == nil {
		var ok bool
		hostDirectory, ok = metadataStore.(HostDirectory)
		if !ok {
			return nil, fmt.Errorf("cache host directory is required")
		}
	}

	bc := &Client{
		ctx:                   ctx,
		locality:              locality,
		clientConfig:          cfg.Client,
		globalConfig:          cfg.Global,
		grpcClients:           make(map[string]proto.CacheClient),
		grpcConns:             make(map[string]*grpc.ClientConn),
		localServers:          make(map[string]*Server),
		localNodeID:           os.Getenv("CACHE_NODE_ID"),
		localCachePathID:      cachePathIDForConfig(cfg),
		rawReadPools:          make(map[string]*rawReadConnPool),
		localHostCache:        make(map[localHostCacheKey]*localClientCache),
		mu:                    sync.RWMutex{},
		metadataStore:         metadataStore,
		hasher:                rendezvous.New[*Host](),
		hostDirectory:         hostDirectory,
		minRetryLengthBytes:   cfg.Client.MinRetryLengthBytes,
		maxGetContentAttempts: max(int64(cfg.Client.MaxGetContentAttempts), 1),
	}

	bc.hostMap = NewHostMap(cfg.Global, bc.addHost)
	bc.discoveryClient = NewDiscoveryClient(cfg.Global, bc.hostMap, hostDirectory, locality)
	bc.initLocalDiskStore(cfg, locality)

	// Start searching for nearby cache hosts
	go bc.discoveryClient.Start(bc.ctx)

	// Monitor and cleanup local client cache
	go bc.manageLocalClientCache(localClientCacheTTL, localClientCacheCleanupInterval)

	// Mount cache as a FUSE filesystem if cachefs is enabled
	if bc.clientConfig.CacheFS.Enabled {
		startServer, _, server, err := Mount(ctx, FSSystemOpts{
			Config:        cfg.Client,
			MetadataStore: metadataStore,
			Client:        bc,
			Verbose:       bc.globalConfig.DebugMode,
		})
		if err != nil {
			return nil, err
		}

		err = startServer()
		if err != nil {
			return nil, err
		}

		err = updateReadAheadKB(bc.clientConfig.CacheFS.MountPoint, readAheadKB)
		if err != nil {
			Logger.Debugf("CacheFS read-ahead tuning unavailable; continuing without read_ahead_kb update: %v", err)
		}

		bc.cachefsServer = server
	}

	return bc, nil
}

func (c *Client) Cleanup() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	cachefsServer := c.cachefsServer
	c.cachefsServer = nil
	for hostID, conn := range c.grpcConns {
		_ = conn.Close()
		delete(c.grpcConns, hostID)
		delete(c.grpcClients, hostID)
	}
	for hostID, pool := range c.rawReadPools {
		pool.close()
		delete(c.rawReadPools, hostID)
	}
	c.mu.Unlock()

	if c.clientConfig.CacheFS.Enabled && cachefsServer != nil {
		cachefsServer.Unmount()
	}

	c.mu.Lock()
	if c.localDiskStore != nil {
		c.localDiskStore.Cleanup()
		c.localDiskStore = nil
	}
	c.mu.Unlock()
	return nil
}

func (c *Client) AttachLocalServer(server *Server) {
	if server == nil || server.Host() == nil {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	// Attachment is intentionally keyed by the elected cache HostId only. It
	// does not add the host to the rendezvous set; discovery/keepalive still
	// controls which cache servers are routable. This keeps multiple embedded
	// cache servers on the same node isolated from each other.
	c.localServers[server.Host().HostId] = server
}

func (c *Client) DetachLocalServer(hostID string) {
	if hostID == "" {
		return
	}

	c.mu.Lock()
	var detachedHost *Host
	if server := c.localServers[hostID]; server != nil {
		detachedHost = server.Host()
	}
	delete(c.localServers, hostID)
	for key, entry := range c.localHostCache {
		if entry.host != nil && entry.host.HostId == hostID {
			delete(c.localHostCache, key)
		}
	}
	c.mu.Unlock()

	if detachedHost != nil {
		c.removeHost(detachedHost)
	}
}

func (c *Client) localServersSnapshot() []*Server {
	c.mu.RLock()
	defer c.mu.RUnlock()

	servers := make([]*Server, 0, len(c.localServers))
	for _, server := range c.localServers {
		if server != nil && server.cas != nil {
			servers = append(servers, server)
		}
	}
	return servers
}

func (c *Client) localServerForHost(host *Host) *Server {
	if host == nil {
		return nil
	}

	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.localServers[host.HostId]
}

type clientLocalStore struct {
	store  *Store
	source string
}

func (c *Client) preferredLocalStores() []clientLocalStore {
	localServers := c.localServersSnapshot()
	stores := make([]clientLocalStore, 0, len(localServers)+1)
	seenPaths := make(map[string]struct{}, len(localServers)+1)
	appendStore := func(store *Store, source string) {
		if store == nil {
			return
		}
		cacheDir := filepath.Clean(store.serverConfig.DiskCacheDir)
		if cacheDir != "." {
			if _, ok := seenPaths[cacheDir]; ok {
				return
			}
			seenPaths[cacheDir] = struct{}{}
		}
		stores = append(stores, clientLocalStore{store: store, source: source})
	}

	for _, server := range localServers {
		appendStore(server.cas, "local_server")
	}
	appendStore(c.localDiskStore, "local_disk")
	return stores
}

func (c *Client) initLocalDiskStore(cfg Config, locality string) {
	if cfg.Server.DiskCacheDir == "" || cfg.Server.PageSizeBytes <= 0 {
		return
	}

	// A worker and the cache-server DaemonSet can share the same node hostPath
	// while only the DaemonSet owns the active endpoint. Keep a local Store view
	// so selected same-node content can still use local reads and page views.
	localHost := &Host{
		HostId:      "client-local-disk",
		Locality:    locality,
		NodeID:      c.localNodeID,
		CachePathID: c.localCachePathID,
	}
	store, err := NewStore(c.ctx, localHost, locality, c.metadataStore, cfg)
	if err != nil {
		Logger.Warnf("failed to initialize local cache disk view: %v", err)
		return
	}
	c.localDiskStore = store
}

func cachePathIDForConfig(config Config) string {
	identity := canonicalPhysicalCacheIdentityPath(config)
	if identity == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(identity))
	return fmt.Sprintf("%x", sum[:6])
}

func canonicalPhysicalCacheIdentityPath(config Config) string {
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

func (c *Client) localDiskStoreForHost(host *Host) *Store {
	if c == nil || host == nil || c.localDiskStore == nil {
		return nil
	}
	if c.localNodeID != "" && host.NodeID != "" && host.NodeID != c.localNodeID {
		return nil
	}
	if c.localCachePathID == "" || host.CachePathID == "" || host.CachePathID != c.localCachePathID {
		return nil
	}
	return c.localDiskStore
}

func (c *Client) selectedLocalDiskStore(ctx context.Context, rt ClientRequestType, hash string, routingKey string) (*Store, *Host, error) {
	if routingKey == "" {
		routingKey = hash
	}
	host, err := c.getSelectedHostForRequest(rt, hash, routingKey)
	if err == ErrHostNotFound && c.hostDirectory != nil && c.hostMap != nil {
		_ = c.refreshRoutableHosts(ctx)
		host, err = c.getSelectedHostForRequest(rt, hash, routingKey)
	}
	if err != nil {
		return nil, nil, err
	}
	return c.localDiskStoreForHost(host), host, nil
}

func (c *Client) GetNearbyHosts() ([]*Host, error) {
	hosts, err := c.hostDirectory.GetAvailableHosts(c.ctx, c.locality)
	if err != nil {
		return nil, err
	}

	return hosts, nil
}

func (c *Client) addHost(host *Host) error {
	if host == nil || host.HostId == "" {
		return ErrHostNotFound
	}
	if !host.HasEndpoint() {
		c.mu.Lock()
		defer c.mu.Unlock()

		if oldConn, ok := c.grpcConns[host.HostId]; ok {
			_ = oldConn.Close()
			delete(c.grpcConns, host.HostId)
		}
		delete(c.grpcClients, host.HostId)
		if oldPool, ok := c.rawReadPools[host.HostId]; ok {
			oldPool.close()
			delete(c.rawReadPools, host.HostId)
		}
		c.hasher.Remove(host)
		c.hasher.Add(host.LogicalOnly())
		for key, entry := range c.localHostCache {
			if entry.host != nil && entry.host.HostId == host.HostId {
				delete(c.localHostCache, key)
			}
		}
		return nil
	}

	addr := cacheHostDialAddr(host, c.localNodeID)

	transportCredentials := grpc.WithTransportCredentials(insecure.NewCredentials())
	if isTLSEnabled(addr) {
		h2creds := credentials.NewTLS(&tls.Config{NextProtos: []string{"h2"}})
		transportCredentials = grpc.WithTransportCredentials(h2creds)
	}

	// Optimized client dial options matching server configuration
	initialWindowSize := c.globalConfig.GRPCInitialWindowSize
	if initialWindowSize == 0 {
		initialWindowSize = 4 * 1024 * 1024
	}

	initialConnWindowSize := c.globalConfig.GRPCInitialConnWindowSize
	if initialConnWindowSize == 0 {
		initialConnWindowSize = 32 * 1024 * 1024
	}

	writeBufferSize := c.globalConfig.GRPCWriteBufferSize
	if writeBufferSize == 0 {
		writeBufferSize = 256 * 1024
	}

	readBufferSize := c.globalConfig.GRPCReadBufferSize
	if readBufferSize == 0 {
		readBufferSize = 256 * 1024
	}

	var dialOpts = []grpc.DialOption{
		transportCredentials,
		grpc.WithContextDialer(DialWithTimeout),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(c.globalConfig.GRPCMessageSizeBytes),
			grpc.MaxCallSendMsgSize(c.globalConfig.GRPCMessageSizeBytes),
		),
		grpc.WithInitialWindowSize(int32(initialWindowSize)),
		grpc.WithInitialConnWindowSize(int32(initialConnWindowSize)),
		grpc.WithWriteBufferSize(writeBufferSize),
		grpc.WithReadBufferSize(readBufferSize),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                cacheKeepaliveTime,
			Timeout:             cacheKeepaliveTimeout,
			PermitWithoutStream: true,
		}),
	}

	if c.clientConfig.Token != "" {
		dialOpts = append(dialOpts,
			grpc.WithUnaryInterceptor(grpcAuthInterceptor(c.clientConfig.Token)),
			grpc.WithStreamInterceptor(grpcAuthStreamInterceptor(c.clientConfig.Token)),
		)
	}

	conn, err := grpc.Dial(addr, dialOpts...)
	if err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if oldConn, ok := c.grpcConns[host.HostId]; ok {
		_ = oldConn.Close()
	}
	if oldPool, ok := c.rawReadPools[host.HostId]; ok {
		oldPool.close()
		delete(c.rawReadPools, host.HostId)
	}
	c.hasher.Remove(host)
	c.grpcClients[host.HostId] = proto.NewCacheClient(conn)
	c.grpcConns[host.HostId] = conn
	c.hasher.Add(host)
	for key, entry := range c.localHostCache {
		if entry.host != nil && entry.host.HostId == host.HostId {
			delete(c.localHostCache, key)
		}
	}

	go c.monitorHost(host)
	return nil
}

func (c *Client) monitorHost(host *Host) {
	interval := time.Duration(c.globalConfig.HostMonitorIntervalS) * time.Second
	if interval <= 0 {
		interval = 30 * time.Second
	}

	failures := 0
	if !c.recordHostMonitorResult(host, &failures) {
		return
	}

	if jitter := time.Duration(time.Now().UnixNano() % int64(interval)); jitter > 0 {
		timer := time.NewTimer(jitter)
		select {
		case <-timer.C:
		case <-c.ctx.Done():
			timer.Stop()
			return
		}
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if !c.recordHostMonitorResult(host, &failures) {
				return
			}

		case <-c.ctx.Done():
			return
		}
	}
}

func (c *Client) recordHostMonitorResult(host *Host, failures *int) bool {
	err := c.hostEndpointHealth(host)
	if err == nil {
		*failures = 0
		return true
	}
	if errors.Is(err, ErrHostNotFound) {
		return false
	}

	*failures++
	if *failures < monitorHostFailureThreshold {
		Logger.Debugf("cache host health check failed @ %s (PrivateAddr=%s, failures=%d/%d): %v", host.HostId, host.PrivateAddr, *failures, monitorHostFailureThreshold, err)
		return true
	}

	c.removeHost(host)
	return false
}

func (c *Client) checkHostEndpoint(host *Host) bool {
	err := c.hostEndpointHealth(host)
	if err != nil {
		c.removeHost(host)
		return false
	}
	return true
}

func (c *Client) hostEndpointHealth(host *Host) error {
	if !c.isCurrentHostEndpoint(host) {
		return ErrHostNotFound
	}

	c.mu.RLock()
	client, exists := c.grpcClients[host.HostId]
	c.mu.RUnlock()
	if !exists {
		return ErrHostNotFound
	}

	timeout := time.Duration(c.globalConfig.GRPCDialTimeoutS) * time.Second
	if timeout <= 0 {
		timeout = time.Second
	}
	ctx, cancel := context.WithTimeout(c.ctx, timeout)
	defer cancel()

	resp, err := client.GetState(ctx, &proto.CacheGetStateRequest{})
	if err != nil {
		return err
	}

	if resp.GetVersion() != Version {
		return ErrInvalidHostVersion
	}

	return nil
}

func (c *Client) removeHost(host *Host) {
	if host == nil {
		return
	}

	var logicalHost *Host
	if c.hostMap != nil {
		var ok bool
		logicalHost, ok = c.hostMap.DeactivateEndpoint(host)
		if !ok {
			return
		}
	}
	if logicalHost == nil {
		logicalHost = host.LogicalOnly()
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.hasher.Remove(host)
	c.hasher.Add(logicalHost)
	if conn, ok := c.grpcConns[host.HostId]; ok {
		_ = conn.Close()
		delete(c.grpcConns, host.HostId)
	}
	delete(c.grpcClients, host.HostId)
	if pool, ok := c.rawReadPools[host.HostId]; ok {
		pool.close()
		delete(c.rawReadPools, host.HostId)
	}
	for key, entry := range c.localHostCache {
		if entry.host != nil && entry.host.HostId == host.HostId {
			delete(c.localHostCache, key)
		}
	}
}

func (c *Client) isCurrentHostEndpoint(host *Host) bool {
	if host == nil || host.HostId == "" {
		return false
	}
	if c.hostMap == nil {
		return true
	}
	return sameCacheHostEndpoint(c.hostMap.Get(host.HostId), host)
}

func (c *Client) removeLocalHostCache(hash string, routingKey ...string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(routingKey) > 0 {
		delete(c.localHostCache, newLocalHostCacheKey(hash, routingKey[0]))
		return
	}
	for key := range c.localHostCache {
		if key.hash == hash {
			delete(c.localHostCache, key)
		}
	}
}

func (c *Client) refreshRoutableHosts(ctx context.Context) error {
	if c.hostDirectory == nil || c.hostMap == nil {
		return ErrHostNotFound
	}
	if ctx == nil {
		ctx = c.ctx
	}
	if ctx == nil {
		ctx = context.Background()
	}

	refreshCtx := ctx
	cancel := func() {}
	if _, ok := ctx.Deadline(); !ok {
		refreshCtx, cancel = context.WithTimeout(ctx, closestHostTimeout)
	}
	defer cancel()

	hosts, err := c.hostDirectory.GetAvailableHosts(refreshCtx, c.locality)
	if err != nil {
		return err
	}

	routable := false
	seenHosts := map[string]struct{}{}
	for _, group := range cacheHostCandidateGroups(hosts) {
		if group.hostID == "" {
			continue
		}
		seenHosts[group.hostID] = struct{}{}

		if c.keepExistingCacheHost(group) {
			routable = true
			continue
		}

		if host, ok := group.firstReachable(refreshCtx, c.verifyCacheHost); ok {
			c.hostMap.Set(host)
			routable = true
			continue
		}
		if logicalHost := group.logicalHost(); logicalHost != nil {
			c.hostMap.Set(logicalHost)
		}
	}
	c.removeUndiscoveredLogicalHosts(seenHosts)

	if !routable {
		return ErrHostNotFound
	}
	return nil
}

func (c *Client) removeUndiscoveredLogicalHosts(seenHosts map[string]struct{}) {
	if c.hostMap == nil {
		return
	}
	for _, known := range c.hostMap.GetAll() {
		if known == nil || known.HostId == "" {
			continue
		}
		if _, ok := seenHosts[known.HostId]; ok {
			continue
		}
		removed, ok := c.hostMap.RemoveLogicalHost(known.HostId)
		if !ok {
			continue
		}

		c.mu.Lock()
		c.hasher.Remove(removed)
		if conn, ok := c.grpcConns[removed.HostId]; ok {
			_ = conn.Close()
			delete(c.grpcConns, removed.HostId)
		}
		delete(c.grpcClients, removed.HostId)
		if pool, ok := c.rawReadPools[removed.HostId]; ok {
			pool.close()
			delete(c.rawReadPools, removed.HostId)
		}
		for key, entry := range c.localHostCache {
			if entry.host != nil && entry.host.HostId == removed.HostId {
				delete(c.localHostCache, key)
			}
		}
		c.mu.Unlock()
	}
}

func (c *Client) keepExistingCacheHost(group cacheHostCandidateGroup) bool {
	known := c.hostMap.Get(group.hostID)
	if !group.hasEndpoint(known) {
		return false
	}
	if c.hasCacheClient(known.HostId) {
		return true
	}
	return c.addHost(known) == nil
}

func (c *Client) hasCacheClient(hostID string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	_, exists := c.grpcClients[hostID]
	return exists
}

func (c *Client) verifyCacheHost(ctx context.Context, host *Host) (*Host, error) {
	if c.discoveryClient == nil {
		return host, nil
	}
	return c.discoveryClient.GetHostState(ctx, host)
}

func (c *Client) getContentAttempts(length int64) int {
	attempts := int(c.maxGetContentAttempts)
	if attempts < 1 {
		return 1
	}
	if c.minRetryLengthBytes > 0 && length < c.minRetryLengthBytes {
		return 1
	}
	return attempts
}

func (c *Client) readReplicaHostCount() int {
	if c.clientConfig.NTopHosts < 1 {
		return 1
	}
	return c.clientConfig.NTopHosts
}

func (c *Client) readContentIntoHostCount(length int64) int {
	attempts := c.getContentAttempts(length)
	hostCount := c.readReplicaHostCount()
	if attempts < hostCount {
		return attempts
	}
	return hostCount
}

func (c *Client) dataCallOptions() []grpc.CallOption {
	if !c.globalConfig.GRPCPayloadCodecV2 {
		return nil
	}
	return []grpc.CallOption{grpc.ForceCodecV2(cachegrpc.New(c.globalConfig.GRPCPayloadCodecMinBytes))}
}

func (c *Client) IsPathCachedReachable(ctx context.Context, path string) bool {
	metadata, err := c.metadataStore.GetFsNode(ctx, GenerateFsID(path))
	if err != nil {
		Logger.Errorf("error getting fs node: %v, path: %s", err, path)
		return false
	}

	exists, err := c.IsCachedReachable(metadata.Hash, path)
	if err != nil {
		Logger.Errorf("error checking if content is cached on a reachable cache host: %v, hash: %s", err, metadata.Hash)
		return false
	}

	return exists
}

// CacheFSMetadata resolves a cachefs path to its content metadata without going
// through the cachefs FUSE mount.
func (c *Client) CacheFSMetadata(ctx context.Context, path string) (*FSMetadata, error) {
	if c == nil || c.metadataStore == nil {
		return nil, ErrClientNotFound
	}
	return c.metadataStore.GetFsNode(ctx, GenerateFsID(path))
}

func (c *Client) cachedLocalFileHash(ctx context.Context, cachePath string, info os.FileInfo, routingKey string) (string, bool) {
	return c.cachedLocalFileHashWithTrace(ctx, cachePath, info, routingKey, nil, "")
}

func (c *Client) cachedLocalFileHashWithTrace(ctx context.Context, cachePath string, info os.FileInfo, routingKey string, trace *OperationTrace, traceSource string) (string, bool) {
	hash, _, ok := c.cachedLocalFileHashWithStoreTrace(ctx, cachePath, info, routingKey, trace, traceSource)
	return hash, ok
}

func (c *Client) cachedLocalFileHashWithStoreTrace(ctx context.Context, cachePath string, info os.FileInfo, routingKey string, trace *OperationTrace, traceSource string) (string, string, bool) {
	if c == nil || cachePath == "" || info == nil || info.IsDir() {
		return "", "", false
	}

	metadata, err := c.CacheFSMetadata(ctx, cachePath)
	if err != nil || !cacheFSMetadataMatchesFileInfo(metadata, info) {
		return "", "", false
	}

	if routingKey == "" {
		routingKey = cachePath
	}
	check := c.selectedHostContentCheckWithTrace(ctx, metadata.Hash, routingKey, info.Size(), trace, traceSource)
	if check.err != nil || !check.exists {
		return "", check.status, false
	}
	return metadata.Hash, check.status, true
}

func (c *Client) cachedContentHash(ctx context.Context, hash string, expectedSize int64) bool {
	return c.cachedContentHashWithTrace(ctx, hash, expectedSize, nil, "")
}

func (c *Client) cachedContentHashWithTrace(ctx context.Context, hash string, expectedSize int64, trace *OperationTrace, traceSource string) bool {
	_, ok := c.cachedContentHashWithStoreTrace(ctx, hash, expectedSize, trace, traceSource)
	return ok
}

func (c *Client) cachedContentHashWithStoreTrace(ctx context.Context, hash string, expectedSize int64, trace *OperationTrace, traceSource string) (string, bool) {
	if c == nil || !isContentHash(hash) {
		return "", false
	}
	check := c.selectedHostContentCheckWithTrace(ctx, hash, hash, expectedSize, trace, traceSource)
	return check.status, check.err == nil && check.exists
}

func cacheFSMetadataMatchesFileInfo(metadata *FSMetadata, info os.FileInfo) bool {
	if metadata == nil || metadata.Hash == "" || info == nil || info.IsDir() || info.Size() < 0 {
		return false
	}

	modTime := info.ModTime()
	return metadata.Size == uint64(info.Size()) &&
		metadata.Mtime == uint64(modTime.Unix()) &&
		metadata.Mtimensec == uint32(modTime.Nanosecond())
}

// IsCachedReachable reports whether hash exists on any currently reachable cache
// host. It checks the HRW read order first, then scans remaining known hosts as
// a recovery path for placement drift or host churn. Do not use it to decide
// whether a cache-through write can be skipped.
func (c *Client) IsCachedReachable(hash string, routingKey string) (bool, error) {
	return c.IsCachedReachableContext(c.ctx, hash, routingKey)
}

func (c *Client) IsCachedReachableContext(ctx context.Context, hash string, routingKey string) (bool, error) {
	if ctx == nil {
		ctx = c.ctx
	}
	if routingKey == "" {
		routingKey = hash
	}

	checked := make(map[string]struct{})
	checkHost := func(host *Host) (bool, error) {
		if host == nil || host.HostId == "" {
			return false, nil
		}
		if _, ok := checked[host.HostId]; ok {
			return false, nil
		}
		checked[host.HostId] = struct{}{}

		c.mu.RLock()
		client, exists := c.grpcClients[host.HostId]
		c.mu.RUnlock()
		if !exists {
			return false, nil
		}

		resp, err := client.HasContent(ctx, &proto.CacheHasContentRequest{Hash: hash})
		if err != nil {
			c.removeHost(host)
			return false, err
		}
		if resp.Exists {
			return true, nil
		}

		c.removeLocalHostCache(hash)
		return false, nil
	}

	for hostIndex := 0; hostIndex < c.clientConfig.NTopHosts; hostIndex++ {
		client, host, err := c.getGRPCClient(&ClientRequest{
			rt:        ClientRequestTypeRetrieval,
			hash:      hash,
			key:       routingKey,
			hostIndex: hostIndex,
		})
		if err != nil {
			continue
		}

		checked[host.HostId] = struct{}{}
		resp, err := client.HasContent(ctx, &proto.CacheHasContentRequest{Hash: hash})
		if err != nil {
			c.removeHost(host)
			continue
		}

		if resp.Exists {
			return true, nil
		}

		c.removeLocalHostCache(hash)
	}

	for _, host := range c.remainingHostsForRequest(checked) {
		exists, _ := checkHost(host)
		if exists {
			return true, nil
		}
	}

	return false, nil
}

// IsCachedOnSelectedHost checks only the HRW-selected storage host for routingKey.
// This is intentionally stricter than IsCachedReachable: cache-through writers use
// it to avoid treating a fallback replica as proof that the primary placement is
// already populated.
func (c *Client) IsCachedOnSelectedHost(hash string, routingKey string, expectedSize ...int64) (bool, error) {
	size := int64(0)
	if len(expectedSize) > 0 {
		size = expectedSize[0]
	}
	check := c.selectedHostContentCheckWithTrace(c.ctx, hash, routingKey, size, nil, "")
	return check.exists, check.err
}

type selectedHostContentCheck struct {
	exists bool
	status string
	host   *Host
	err    error
}

func (c *Client) selectedHostContentCheckWithTrace(ctx context.Context, hash string, routingKey string, expectedSize int64, trace *OperationTrace, traceSource string) selectedHostContentCheck {
	started := time.Now()
	if traceSource == "" {
		traceSource = "selected_host_check"
	}
	if routingKey == "" {
		routingKey = hash
	}

	host, err := c.getSelectedHostForRequest(ClientRequestTypeStorage, hash, routingKey)
	if err == ErrHostNotFound && c.hostDirectory != nil && c.hostMap != nil {
		_ = c.refreshRoutableHosts(ctx)
		if trace != nil {
			trace.HostRefreshes++
		}
		host, err = c.getSelectedHostForRequest(ClientRequestTypeStorage, hash, routingKey)
	}
	if err != nil {
		trace.addStoreAttempt(0, host, traceSource, operationTraceStoreResult(err), 0, expectedSize, "", time.Since(started), err)
		return selectedHostContentCheck{host: host, err: err}
	}

	localStore := c.localDiskStoreForHost(host)
	if localStore != nil {
		status := localStore.ContentStatus(hash, expectedSize)
		exists := status == contentStatusComplete
		if !exists {
			c.removeLocalHostCache(hash, routingKey)
		}
		trace.addStoreAttempt(0, host, traceSource+"_local", contentCheckTraceResult(nil, exists, status), 0, expectedSize, status, time.Since(started), nil)
		return selectedHostContentCheck{exists: exists, status: status, host: host}
	}
	if localServer := c.localServerForHost(host); localServer != nil && localServer.cas != nil {
		status := localServer.cas.ContentStatus(hash, expectedSize)
		exists := status == contentStatusComplete
		if !exists {
			c.removeLocalHostCache(hash, routingKey)
		}
		trace.addStoreAttempt(0, host, traceSource+"_local_server", contentCheckTraceResult(nil, exists, status), 0, expectedSize, status, time.Since(started), nil)
		return selectedHostContentCheck{exists: exists, status: status, host: host}
	}

	client, host, err := c.getGRPCClient(&ClientRequest{
		rt:        ClientRequestTypeStorage,
		hash:      hash,
		key:       routingKey,
		hostIndex: 0,
	})
	if err != nil {
		if err == ErrHostNotFound {
			_ = c.refreshRoutableHosts(ctx)
			if trace != nil {
				trace.HostRefreshes++
			}
		}
		trace.addStoreAttempt(0, host, traceSource, operationTraceStoreResult(err), 0, expectedSize, "", time.Since(started), err)
		return selectedHostContentCheck{host: host, err: err}
	}

	resp, err := client.HasContent(ctx, &proto.CacheHasContentRequest{Hash: hash, ExpectedSize: expectedSize})
	if err != nil {
		c.removeHost(host)
		trace.addStoreAttempt(0, host, traceSource, operationTraceStoreResult(err), 0, expectedSize, "", time.Since(started), err)
		return selectedHostContentCheck{host: host, err: err}
	}
	exists := resp.GetExists()
	status := contentStatusFromHasContent(resp)
	if !resp.Exists {
		c.removeLocalHostCache(hash, routingKey)
	}
	trace.addStoreAttempt(0, host, traceSource+"_remote", contentCheckTraceResult(nil, exists, status), 0, expectedSize, status, time.Since(started), nil)
	return selectedHostContentCheck{exists: exists, status: status, host: host}
}

func contentStatusFromHasContent(resp *proto.CacheHasContentResponse) string {
	if resp == nil {
		return contentStatusIncomplete
	}
	if resp.Status != "" {
		return resp.Status
	}
	if resp.Exists {
		return contentStatusComplete
	}
	return contentStatusIncomplete
}

func contentCheckTraceResult(err error, exists bool, status string) string {
	if err != nil {
		return operationTraceStoreResult(err)
	}
	if exists {
		return contentStatusComplete
	}
	if status != "" {
		return status
	}
	return contentStatusIncomplete
}

func (c *Client) rawReadInto(ctx context.Context, host *Host, hash string, offset int64, dst []byte) (read int64, err error) {
	started := time.Now()
	reqCount := atomic.AddInt64(&cachePathStats.clientRawRequests, 1)
	status := "unknown"
	hostID := ""
	addr := ""
	var waitElapsed time.Duration
	var headerElapsed time.Duration
	var bodyElapsed time.Duration
	defer func() {
		elapsed := time.Since(started)
		if shouldTraceCachePath(reqCount, elapsed, err != nil || status != "ok") {
			Logger.Debugf(
				"cache raw client read trace: seq=%d status=%s host=%s addr=%s hash=%s offset=%d length=%d read=%d err=%v elapsed=%s wait=%s header=%s body=%s",
				reqCount,
				status,
				hostID,
				addr,
				hash,
				offset,
				len(dst),
				read,
				err,
				elapsed.Truncate(time.Millisecond),
				waitElapsed.Truncate(time.Microsecond),
				headerElapsed.Truncate(time.Microsecond),
				bodyElapsed.Truncate(time.Microsecond),
			)
		}
	}()
	if ctx == nil {
		ctx = c.ctx
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		status = "context_error"
		return 0, err
	}
	if host == nil || !c.clientConfig.ReadTransport.Enabled {
		status = "disabled_or_missing_host"
		atomic.AddInt64(&cachePathStats.clientRawErrors, 1)
		return 0, ErrUnableToReachHost
	}
	hostID = host.HostId
	addr = cacheHostDialAddr(host, c.localNodeID)
	if addr == "" {
		status = "missing_addr"
		atomic.AddInt64(&cachePathStats.clientRawErrors, 1)
		return 0, ErrUnableToReachHost
	}

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		status = "client_closed"
		return 0, ErrClientNotFound
	}
	pool := c.rawReadPools[host.HostId]
	if pool == nil {
		pool = newRawReadConnPool(addr, c.clientConfig.ReadTransport.MaxActiveConnsPerHost, c.clientConfig.ReadTransport.MaxIdleConnsPerHost)
		c.rawReadPools[host.HostId] = pool
	}
	c.mu.Unlock()

	waitStarted := time.Now()
	conn, err := pool.get(ctx)
	waitElapsed = time.Since(waitStarted)
	atomic.AddInt64(&cachePathStats.clientRawWaitNanos, waitElapsed.Nanoseconds())
	if err != nil {
		status = "conn_wait_error"
		atomic.AddInt64(&cachePathStats.clientRawErrors, 1)
		if ctxErr := ctx.Err(); ctxErr != nil {
			return 0, ctxErr
		}
		return 0, ErrUnableToReachHost
	}
	reusable := false
	// Interrupt I/O only after the context is canceled. Setting the socket's
	// deadline directly can surface an I/O timeout before ctx.Err is observable,
	// incorrectly turning a caller deadline into a host failure.
	var contextCancelled atomic.Bool
	cancelDone := make(chan struct{})
	stopCancellation := context.AfterFunc(ctx, func() {
		defer close(cancelDone)
		contextCancelled.Store(true)
		_ = conn.SetDeadline(time.Now())
	})
	defer func() {
		if !stopCancellation() {
			<-cancelDone
		}
		if contextCancelled.Load() {
			reusable = false
		}
		if reusable {
			_ = conn.SetDeadline(time.Time{})
			pool.put(conn)
		} else {
			pool.discard(conn)
		}
	}()

	transportError := func(err error) error {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return fmt.Errorf("%w: %v", ErrUnableToReachHost, err)
	}

	length := int64(len(dst))
	headerStarted := time.Now()
	if err := writeRawReadRequest(conn, hash, offset, length); err != nil {
		headerElapsed = time.Since(headerStarted)
		atomic.AddInt64(&cachePathStats.clientRawHeaderNanos, headerElapsed.Nanoseconds())
		status = "write_request_error"
		atomic.AddInt64(&cachePathStats.clientRawErrors, 1)
		return 0, transportError(err)
	}
	respStatus, responseLength, err := readRawReadResponseHeader(conn)
	headerElapsed = time.Since(headerStarted)
	atomic.AddInt64(&cachePathStats.clientRawHeaderNanos, headerElapsed.Nanoseconds())
	if err != nil {
		status = "read_header_error"
		atomic.AddInt64(&cachePathStats.clientRawErrors, 1)
		return 0, transportError(err)
	}
	if respStatus == rawReadStatusMiss {
		status = "miss"
		cacheReadRawFallbackTotal.Inc()
		atomic.AddInt64(&cachePathStats.clientRawMisses, 1)
		reusable = true
		return 0, ErrContentNotFound
	}
	if respStatus == rawReadStatusBusy && responseLength == 0 {
		status = "busy"
		reusable = true
		return 0, ErrRawReadBusy
	}
	if respStatus != rawReadStatusOK || responseLength != length {
		status = "bad_response"
		atomic.AddInt64(&cachePathStats.clientRawErrors, 1)
		return 0, transportError(fmt.Errorf("unexpected raw response status=%d length=%d want=%d", respStatus, responseLength, length))
	}
	bodyStarted := time.Now()
	if _, err := io.ReadFull(conn, dst); err != nil {
		bodyElapsed = time.Since(bodyStarted)
		atomic.AddInt64(&cachePathStats.clientRawBodyNanos, bodyElapsed.Nanoseconds())
		status = "body_error"
		atomic.AddInt64(&cachePathStats.clientRawErrors, 1)
		return 0, transportError(err)
	}
	bodyElapsed = time.Since(bodyStarted)
	atomic.AddInt64(&cachePathStats.clientRawBodyNanos, bodyElapsed.Nanoseconds())
	cacheReadRawTransportTotal.Inc()
	atomic.AddInt64(&cachePathStats.clientRawHits, 1)
	reusable = true
	status = "ok"
	return length, nil
}

type rawReadWindowPlan struct {
	partLength  int
	chunkCount  int
	concurrency int
}

func (c *Client) planRawReadWindows(length int) rawReadWindowPlan {
	if length <= 0 {
		return rawReadWindowPlan{}
	}

	partLength := c.clientConfig.ReadTransport.RequestSizeBytes
	if partLength <= 0 {
		partLength = defaultRawReadWindowPartBytes
	}
	maxRequestLength := int64(c.globalConfig.GRPCMessageSizeBytes)
	if maxRequestLength <= 0 {
		maxRequestLength = defaultRawReadChunkBytes
	}
	if partLength > maxRequestLength {
		partLength = maxRequestLength
	}
	if partLength > int64(length) {
		partLength = int64(length)
	}

	partLengthInt := int(partLength)
	chunkCount := (length + partLengthInt - 1) / partLengthInt
	concurrency := c.clientConfig.ReadTransport.MaxPartsPerRead
	if concurrency <= 0 {
		concurrency = defaultRawReadWindowMaxParts
	}
	if maxActive := c.clientConfig.ReadTransport.MaxActiveConnsPerHost; maxActive > 0 && concurrency > maxActive {
		concurrency = maxActive
	}
	if concurrency > chunkCount {
		concurrency = chunkCount
	}
	if concurrency < 1 {
		concurrency = 1
	}

	return rawReadWindowPlan{
		partLength:  partLengthInt,
		chunkCount:  chunkCount,
		concurrency: concurrency,
	}
}

func (c *Client) rawReadIntoWindowed(ctx context.Context, host *Host, hash string, offset int64, dst []byte) (read int64, err error) {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
	}
	if len(dst) == 0 {
		return 0, nil
	}

	plan := c.planRawReadWindows(len(dst))
	traceSequence := atomic.AddInt64(&rawReadWindowTraceSequence, 1)
	started := time.Now()
	traceHostID := ""
	if host != nil {
		traceHostID = host.HostId
	}
	defer func() {
		elapsed := time.Since(started)
		if shouldTraceCachePath(traceSequence, elapsed, err != nil) {
			Logger.Debugf(
				"cache raw client windowed read trace: seq=%d host=%s hash=%s offset=%d length=%d part_length=%d chunks=%d concurrency=%d read=%d err=%v elapsed=%s",
				traceSequence,
				traceHostID,
				hash,
				offset,
				len(dst),
				plan.partLength,
				plan.chunkCount,
				plan.concurrency,
				read,
				err,
				elapsed.Truncate(time.Millisecond),
			)
		}
	}()

	if plan.chunkCount <= 1 {
		return c.rawReadInto(ctx, host, hash, offset, dst)
	}
	if plan.concurrency <= 1 {
		for off := 0; off < len(dst); off += plan.partLength {
			n := plan.partLength
			if remaining := len(dst) - off; remaining < n {
				n = remaining
			}
			chunkRead, chunkErr := c.rawReadInto(ctx, host, hash, offset+int64(off), dst[off:off+n])
			read += chunkRead
			if chunkErr != nil {
				return read, chunkErr
			}
			if chunkRead != int64(n) {
				return read, io.ErrUnexpectedEOF
			}
		}
		return read, nil
	}

	type chunk struct {
		off int
		n   int
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	tasks := make(chan chunk)
	var wg sync.WaitGroup
	var firstErr error
	var errMu sync.Mutex
	read = 0

	setErr := func(err error) {
		if err == nil {
			return
		}
		errMu.Lock()
		if firstErr == nil {
			firstErr = err
			cancel()
		}
		errMu.Unlock()
	}

	for worker := 0; worker < plan.concurrency; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for task := range tasks {
				n64, err := c.rawReadInto(ctx, host, hash, offset+int64(task.off), dst[task.off:task.off+task.n])
				if err != nil {
					setErr(err)
					continue
				}
				if n64 != int64(task.n) {
					setErr(io.ErrUnexpectedEOF)
					continue
				}
				atomic.AddInt64(&read, n64)
			}
		}()
	}

sendLoop:
	for off := 0; off < len(dst); off += plan.partLength {
		n := plan.partLength
		if remaining := len(dst) - off; remaining < n {
			n = remaining
		}
		select {
		case tasks <- chunk{off: off, n: n}:
		case <-ctx.Done():
			break sendLoop
		}
	}
	close(tasks)
	wg.Wait()

	if firstErr != nil {
		return read, firstErr
	}
	if read != int64(len(dst)) {
		return read, io.ErrUnexpectedEOF
	}
	return read, nil
}

func (c *Client) ClientLocalPageFileView(hash string, offset int64, length int64, opts ClientOptions) (path string, pageOffset int64, n int, ok bool, err error) {
	started := time.Now()
	defer func() {
		if elapsed := time.Since(started); elapsed > 100*time.Millisecond || err != nil {
			Logger.Debugf("cache client-local page-file view result: hash=%s offset=%d length=%d path=%s page_offset=%d n=%d ok=%t err=%v elapsed=%s", hash, offset, length, path, pageOffset, n, ok, err, elapsed.Truncate(time.Millisecond))
		}
	}()
	atomic.AddInt64(&cachePathStats.clientLocalPageFileRequests, 1)
	if opts.RoutingKey == "" {
		opts.RoutingKey = hash
	}
	host, err := c.getHostForRequest(&ClientRequest{
		rt:        ClientRequestTypeRetrieval,
		hash:      hash,
		key:       opts.RoutingKey,
		hostIndex: 0,
	})
	if err != nil {
		if err == ErrHostNotFound {
			_ = c.refreshRoutableHosts(c.ctx)
		}
		atomic.AddInt64(&cachePathStats.clientLocalPageFileMisses, 1)
		return "", 0, 0, false, err
	}

	c.mu.RLock()
	localServer := c.localServers[host.HostId]
	c.mu.RUnlock()
	if localServer != nil && localServer.cas != nil {
		path, pageOffset, n, ok, err = localServer.cas.PageRegion(hash, offset, length)
		if err == nil && ok {
			atomic.AddInt64(&cachePathStats.clientLocalPageFileHits, 1)
			atomic.AddInt64(&cachePathStats.clientLocalPageFileBytes, int64(n))
			return path, pageOffset, n, ok, err
		}
	}
	if localStore := c.localDiskStoreForHost(host); localStore != nil {
		path, pageOffset, n, ok, err = localStore.PageRegion(hash, offset, length)
		if err == nil && ok {
			atomic.AddInt64(&cachePathStats.clientLocalPageFileHits, 1)
			atomic.AddInt64(&cachePathStats.clientLocalPageFileBytes, int64(n))
			return path, pageOffset, n, ok, err
		}
	}
	atomic.AddInt64(&cachePathStats.clientLocalPageFileMisses, 1)
	return "", 0, 0, false, err
}

func (c *Client) ClientLocalPageFileViews(hash string, offset int64, length int64, opts ClientOptions) (views []ClientLocalPageFileView, err error) {
	views, _, err = c.ClientLocalPageFileViewsWithTrace(hash, offset, length, opts)
	return views, err
}

func (c *Client) ClientLocalPageFileViewsWithTrace(hash string, offset int64, length int64, opts ClientOptions) (views []ClientLocalPageFileView, trace OperationTrace, err error) {
	started := time.Now()
	defer func() {
		elapsed := time.Since(started)
		trace.Operation = "client_local_page_file_views"
		trace.Result = operationTracePageViewResult(err, len(views))
		trace.Hash = hash
		trace.RoutingKey = opts.RoutingKey
		trace.Offset = offset
		trace.Length = length
		trace.Views = len(views)
		trace.DurationUs = elapsed.Microseconds()
		if elapsed := time.Since(started); elapsed > 100*time.Millisecond || err != nil {
			Logger.Debugf("cache client-local page-file views result: hash=%s offset=%d length=%d views=%d err=%v elapsed=%s", hash, offset, length, len(views), err, elapsed.Truncate(time.Millisecond))
		}
	}()
	atomic.AddInt64(&cachePathStats.clientLocalPageFileRequests, 1)
	if opts.RoutingKey == "" {
		opts.RoutingKey = hash
	}
	if offset < 0 || length <= 0 {
		return nil, trace, nil
	}
	if c.clientConfig.PreferLocalCacheHost {
		if views, host, source, ok := c.clientLocalPageFileViewsFromPreferredStores(hash, offset, length); ok {
			trace.addAttempt(-1, host, source, "hit", int64(len(views)), time.Since(started), nil)
			return views, trace, nil
		}
	}

	host, err := c.getSelectedHostForRequest(ClientRequestTypeRetrieval, hash, opts.RoutingKey)
	if err != nil {
		if err == ErrHostNotFound {
			_ = c.refreshRoutableHosts(c.ctx)
			trace.HostRefreshes++
			host, err = c.getSelectedHostForRequest(ClientRequestTypeRetrieval, hash, opts.RoutingKey)
		}
		if err != nil {
			trace.addAttempt(0, host, "host_select", operationTraceReadResult(err, 0, length), 0, time.Since(started), err)
			atomic.AddInt64(&cachePathStats.clientLocalPageFileMisses, 1)
			return nil, trace, err
		}
	}
	if views, source, ok := c.clientLocalPageFileViewsFromHost(host, hash, offset, length); ok {
		trace.addAttempt(0, host, source, "hit", int64(len(views)), time.Since(started), nil)
		return views, trace, nil
	}

	trace.addAttempt(0, host, "client_local_page_file", "miss", 0, time.Since(started), ErrContentNotFound)
	atomic.AddInt64(&cachePathStats.clientLocalPageFileMisses, 1)
	return nil, trace, ErrContentNotFound
}

func (c *Client) clientLocalPageFileViewsFromPreferredStores(hash string, offset int64, length int64) ([]ClientLocalPageFileView, *Host, string, bool) {
	for _, candidate := range c.preferredLocalStores() {
		if !candidate.store.Exists(hash) {
			continue
		}
		if views, ok := clientLocalPageFileViewsFromStore(candidate.store, hash, offset, length); ok {
			cacheReadClientLocalPageFileTotal.Inc()
			atomic.AddInt64(&cachePathStats.clientLocalPageFileHits, 1)
			atomic.AddInt64(&cachePathStats.clientLocalPageFileBytes, length)
			return views, candidate.store.currentHost, candidate.source + "_page_file", true
		}
	}
	return nil, nil, "", false
}

func (c *Client) clientLocalPageFileViewsFromHost(host *Host, hash string, offset int64, length int64) ([]ClientLocalPageFileView, string, bool) {
	if host == nil {
		return nil, "", false
	}

	c.mu.RLock()
	localServer := c.localServers[host.HostId]
	c.mu.RUnlock()
	if localServer != nil && localServer.cas != nil {
		if views, ok := clientLocalPageFileViewsFromStore(localServer.cas, hash, offset, length); ok {
			cacheReadClientLocalPageFileTotal.Inc()
			atomic.AddInt64(&cachePathStats.clientLocalPageFileHits, 1)
			atomic.AddInt64(&cachePathStats.clientLocalPageFileBytes, length)
			return views, "local_server_page_file", true
		}
	}

	if localStore := c.localDiskStoreForHost(host); localStore != nil {
		if views, ok := clientLocalPageFileViewsFromStore(localStore, hash, offset, length); ok {
			cacheReadClientLocalPageFileTotal.Inc()
			atomic.AddInt64(&cachePathStats.clientLocalPageFileHits, 1)
			atomic.AddInt64(&cachePathStats.clientLocalPageFileBytes, length)
			return views, "local_disk_page_file", true
		}
	}

	return nil, "", false
}

func clientLocalPageFileViewsFromStore(store *Store, hash string, offset int64, length int64) ([]ClientLocalPageFileView, bool) {
	if store == nil || store.serverConfig.PageSizeBytes <= 0 {
		return nil, false
	}

	views := make([]ClientLocalPageFileView, 0, 1)
	remaining := length
	currentOffset := offset
	for remaining > 0 {
		pageRemaining := store.serverConfig.PageSizeBytes - currentOffset%store.serverConfig.PageSizeBytes
		requestLength := min(remaining, pageRemaining)
		path, pageOffset, n, ok, err := store.PageRegion(hash, currentOffset, requestLength)
		if err != nil || !ok || n <= 0 {
			return nil, false
		}
		views = append(views, ClientLocalPageFileView{Path: path, Offset: pageOffset, Length: n})
		readLength := int64(n)
		currentOffset += readLength
		remaining -= readLength
	}
	return views, true
}

func (c *Client) ReadContentInto(ctx context.Context, hash string, offset int64, dst []byte, opts ClientOptions) (read int64, err error) {
	read, _, err = c.ReadContentIntoWithTrace(ctx, hash, offset, dst, opts)
	return read, err
}

func (c *Client) ReadContentIntoWithTrace(ctx context.Context, hash string, offset int64, dst []byte, opts ClientOptions) (read int64, trace OperationTrace, err error) {
	if ctx == nil {
		ctx = c.ctx
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, getContentRequestTimeout)
		defer cancel()
	}
	if len(dst) == 0 {
		return 0, trace, nil
	}
	if opts.RoutingKey == "" {
		opts.RoutingKey = hash
	}

	length := int64(len(dst))
	started := time.Now()
	defer func() {
		elapsed := time.Since(started)
		trace.Operation = "read_into"
		trace.Result = operationTraceReadResult(err, read, length)
		trace.Hash = hash
		trace.RoutingKey = opts.RoutingKey
		trace.Offset = offset
		trace.Length = length
		trace.Read = read
		trace.DurationUs = elapsed.Microseconds()
		if elapsed := time.Since(started); elapsed > time.Second || err != nil {
			Logger.Debugf("cache read-into result: hash=%s routing_key=%s offset=%d length=%d read=%d err=%v elapsed=%s", hash, opts.RoutingKey, offset, length, read, err, elapsed.Truncate(time.Millisecond))
		}
	}()
	atomic.AddInt64(&cachePathStats.clientReadIntoRequests, 1)
	atomic.AddInt64(&cachePathStats.clientReadIntoBytes, length)
	if err := ctx.Err(); err != nil {
		return 0, trace, err
	}
	c.mu.RLock()
	closed := c.closed
	c.mu.RUnlock()
	if closed {
		return 0, trace, ErrClientNotFound
	}
	if c.clientConfig.PreferLocalCacheHost {
		if n, ok := c.readContentIntoFromPreferredLocalStores(hash, offset, dst, &trace); ok {
			return n, trace, nil
		}
	}

	if n, err := c.tryReadContentIntoKnownHosts(ctx, hash, offset, dst, opts, &trace); err == nil {
		return n, trace, nil
	} else if ctxErr := ctx.Err(); ctxErr != nil {
		return 0, trace, ctxErr
	} else if !shouldRefreshReadContentIntoHosts(err) || c.hostDirectory == nil || c.hostMap == nil {
		return 0, trace, err
	}

	read, err = c.readContentIntoAfterHostRefresh(ctx, hash, offset, dst, opts, &trace)
	return read, trace, err
}

func (c *Client) readContentIntoFromPreferredLocalStores(hash string, offset int64, dst []byte, trace *OperationTrace) (int64, bool) {
	length := int64(len(dst))
	for _, candidate := range c.preferredLocalStores() {
		if !candidate.store.Exists(hash) {
			continue
		}
		started := time.Now()
		n, err := candidate.store.ReadAt(hash, offset, dst)
		trace.addAttempt(-1, candidate.store.currentHost, candidate.source, operationTraceReadResult(err, n, length), n, time.Since(started), err)
		if err == nil && n == length {
			cacheReadLocalTotal.Inc()
			atomic.AddInt64(&cachePathStats.clientLocalHits, 1)
			return n, true
		}
		atomic.AddInt64(&cachePathStats.clientLocalMisses, 1)
	}
	return 0, false
}

func shouldRefreshReadContentIntoHosts(err error) bool {
	return errors.Is(err, ErrSelectedHostUnavailable) || errors.Is(err, ErrUnableToReachHost) || errors.Is(err, ErrHostNotFound) || errors.Is(err, ErrClientNotFound) || errors.Is(err, ErrRawReadBusy)
}

func (c *Client) tryReadContentIntoKnownHosts(ctx context.Context, hash string, offset int64, dst []byte, opts ClientOptions, trace *OperationTrace) (int64, error) {
	length := int64(len(dst))
	var lastErr error
	primaryUnavailable := false
	selectedUnavailable := false
	contentMissing := false
	rawReadBusy := false
	hostCount := c.readContentIntoHostCount(length)
	checked := make(map[string]struct{}, hostCount)

	for hostIndex := 0; hostIndex < hostCount; hostIndex++ {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		host, err := c.getHostForRequest(&ClientRequest{
			rt:        ClientRequestTypeRetrieval,
			hash:      hash,
			key:       opts.RoutingKey,
			hostIndex: hostIndex,
		})
		if err != nil {
			if err == ErrHostNotFound {
				_ = c.refreshRoutableHosts(ctx)
				trace.HostRefreshes++
				host, err = c.getHostForRequest(&ClientRequest{
					rt:        ClientRequestTypeRetrieval,
					hash:      hash,
					key:       opts.RoutingKey,
					hostIndex: hostIndex,
				})
			}
			if err != nil {
				trace.addAttempt(hostIndex, host, "host_select", operationTraceReadResult(err, 0, length), 0, 0, err)
				if errors.Is(err, ErrSelectedHostUnavailable) || errors.Is(err, ErrUnableToReachHost) || errors.Is(err, ErrHostNotFound) {
					if hostIndex == 0 {
						primaryUnavailable = true
					}
					selectedUnavailable = true
					lastErr = ErrSelectedHostUnavailable
					continue
				}
				lastErr = err
				continue
			}
		}
		if host == nil || host.HostId == "" {
			lastErr = ErrHostNotFound
			continue
		}
		if _, ok := checked[host.HostId]; ok {
			continue
		}
		checked[host.HostId] = struct{}{}

		n, err := c.readContentIntoFromHost(ctx, host, hostIndex, hash, offset, dst, trace)
		if err == nil && n == length {
			return n, nil
		}
		if errors.Is(err, ErrSelectedHostUnavailable) || errors.Is(err, ErrUnableToReachHost) || errors.Is(err, ErrHostNotFound) {
			if hostIndex == 0 {
				primaryUnavailable = true
			}
			selectedUnavailable = true
			lastErr = ErrSelectedHostUnavailable
			continue
		}
		if errors.Is(err, ErrContentNotFound) {
			contentMissing = true
			lastErr = ErrContentNotFound
			continue
		}
		if errors.Is(err, ErrRawReadBusy) {
			rawReadBusy = true
			lastErr = ErrRawReadBusy
			continue
		}
		if err == ErrHostNotFound {
			if hostIndex == 0 {
				primaryUnavailable = true
			}
			selectedUnavailable = true
			lastErr = ErrSelectedHostUnavailable
			continue
		}
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return 0, ctxErr
			}
			lastErr = err
			continue
		}
		contentMissing = true
		lastErr = ErrContentNotFound
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	if primaryUnavailable || (selectedUnavailable && !contentMissing) {
		return 0, ErrSelectedHostUnavailable
	}
	if rawReadBusy {
		return 0, ErrRawReadBusy
	}
	if contentMissing {
		return 0, ErrContentNotFound
	}
	if lastErr != nil {
		return 0, lastErr
	}
	return 0, ErrHostNotFound
}

func (c *Client) readContentIntoAfterHostRefresh(ctx context.Context, hash string, offset int64, dst []byte, opts ClientOptions, trace *OperationTrace) (int64, error) {
	if c.hostDirectory == nil || c.hostMap == nil {
		return 0, ErrHostNotFound
	}

	c.removeLocalHostCache(hash)
	if err := c.refreshRoutableHosts(ctx); err != nil && ctx.Err() != nil {
		return 0, err
	}
	trace.HostRefreshes++
	if n, err := c.tryReadContentIntoKnownHosts(ctx, hash, offset, dst, opts, trace); err == nil {
		Logger.Debugf("cache read-into recovered after host refresh: hash=%s routing_key=%s offset=%d length=%d", hash, opts.RoutingKey, offset, len(dst))
		return n, nil
	} else {
		return 0, err
	}
}

func (c *Client) readContentIntoFromHost(ctx context.Context, host *Host, hostIndex int, hash string, offset int64, dst []byte, trace *OperationTrace) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if host == nil {
		trace.addAttempt(hostIndex, host, "host_select", "unavailable", 0, 0, ErrHostNotFound)
		return 0, ErrHostNotFound
	}

	length := int64(len(dst))
	c.mu.RLock()
	localServer := c.localServers[host.HostId]
	c.mu.RUnlock()
	if localServer != nil && localServer.cas != nil {
		attemptStarted := time.Now()
		n, err := localServer.cas.ReadAt(hash, offset, dst)
		trace.addAttempt(hostIndex, host, "local_server", operationTraceReadResult(err, n, length), n, time.Since(attemptStarted), err)
		if err == nil && n == length {
			cacheReadLocalTotal.Inc()
			atomic.AddInt64(&cachePathStats.clientLocalHits, 1)
			return n, nil
		}
		if err == ErrContentNotFound {
			atomic.AddInt64(&cachePathStats.clientLocalMisses, 1)
			c.removeLocalHostCache(hash)
			return 0, err
		}
		if err != nil {
			atomic.AddInt64(&cachePathStats.clientLocalMisses, 1)
		}
	}

	if localStore := c.localDiskStoreForHost(host); localStore != nil {
		attemptStarted := time.Now()
		n, err := localStore.ReadAt(hash, offset, dst)
		trace.addAttempt(hostIndex, host, "local_disk", operationTraceReadResult(err, n, length), n, time.Since(attemptStarted), err)
		if err == nil && n == length {
			cacheReadLocalTotal.Inc()
			atomic.AddInt64(&cachePathStats.clientLocalHits, 1)
			return n, nil
		}
		if err == ErrContentNotFound {
			atomic.AddInt64(&cachePathStats.clientLocalMisses, 1)
			c.removeLocalHostCache(hash)
			return 0, err
		}
		if err != nil {
			atomic.AddInt64(&cachePathStats.clientLocalMisses, 1)
		}
	}

	if !host.HasEndpoint() {
		trace.addAttempt(hostIndex, host, "endpoint", "unavailable", 0, 0, ErrSelectedHostUnavailable)
		c.removeLocalHostCache(hash)
		return 0, ErrSelectedHostUnavailable
	}

	if c.clientConfig.ReadTransport.Enabled {
		attemptStarted := time.Now()
		n, err := c.rawReadIntoWindowed(ctx, host, hash, offset, dst)
		trace.addAttempt(hostIndex, host, "raw", operationTraceReadResult(err, n, length), n, time.Since(attemptStarted), err)
		if err == nil && n == length {
			return n, nil
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return 0, ctxErr
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return 0, err
		}
		if errors.Is(err, ErrContentNotFound) {
			c.removeLocalHostCache(hash)
			return 0, err
		}
		if errors.Is(err, ErrRawReadBusy) {
			return 0, err
		}
		if errors.Is(err, ErrUnableToReachHost) {
			c.removeHost(host)
			return 0, ErrSelectedHostUnavailable
		}
		if err != nil {
			cacheReadRawFallbackTotal.Inc()
		}
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	c.mu.RLock()
	client, exists := c.grpcClients[host.HostId]
	c.mu.RUnlock()
	if !exists {
		trace.addAttempt(hostIndex, host, "grpc_client", "unavailable", 0, 0, ErrSelectedHostUnavailable)
		c.removeLocalHostCache(hash)
		return 0, ErrSelectedHostUnavailable
	}

	start := time.Now()
	getContentResponse, err := client.GetContent(ctx, &proto.CacheGetContentRequest{Hash: hash, Offset: offset, Length: length}, c.dataCallOptions()...)
	if err != nil {
		trace.addAttempt(hostIndex, host, "grpc", "unavailable", 0, time.Since(start), err)
		atomic.AddInt64(&cachePathStats.clientGRPCErrors, 1)
		if c.globalConfig.GRPCPayloadCodecV2 {
			cacheReadGRPCCodecV2FallbackTotal.Inc()
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return 0, ctxErr
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return 0, err
		}
		c.removeHost(host)
		return 0, ErrSelectedHostUnavailable
	}
	responseLength := int64(0)
	if getContentResponse != nil {
		responseLength = int64(len(getContentResponse.Content))
	}
	if getContentResponse == nil || !getContentResponse.Ok || responseLength != length {
		trace.addAttempt(hostIndex, host, "grpc", "miss", responseLength, time.Since(start), ErrContentNotFound)
		atomic.AddInt64(&cachePathStats.clientGRPCMisses, 1)
		c.removeLocalHostCache(hash)
		return 0, ErrContentNotFound
	}

	copy(dst, getContentResponse.Content)
	trace.addAttempt(hostIndex, host, "grpc", "hit", length, time.Since(start), nil)
	atomic.AddInt64(&cachePathStats.clientGRPCHits, 1)
	if c.globalConfig.GRPCPayloadCodecV2 {
		cacheReadGRPCCodecV2Total.Inc()
	}
	Logger.Debugf("Elapsed time to get content: %v", time.Since(start))
	return length, nil
}

func (c *Client) remainingHostsForRequest(checked map[string]struct{}) []*Host {
	if c.hostMap == nil {
		return nil
	}

	hosts := c.hostMap.GetAll()
	sort.Slice(hosts, func(i, j int) bool {
		return hosts[i].HostId < hosts[j].HostId
	})

	out := hosts[:0]
	for _, host := range hosts {
		if host == nil || host.HostId == "" {
			continue
		}
		if _, ok := checked[host.HostId]; ok {
			continue
		}
		checked[host.HostId] = struct{}{}
		out = append(out, host)
	}
	return out
}

func (c *Client) GetContent(hash string, offset int64, length int64, opts struct {
	RoutingKey string
}) ([]byte, error) {
	ctx, cancel := context.WithTimeout(c.ctx, getContentRequestTimeout)
	defer cancel()

	if length < 0 {
		return nil, fmt.Errorf("invalid content length: %d", length)
	}

	dst := make([]byte, int(length))
	n, err := c.ReadContentInto(ctx, hash, offset, dst, ClientOptions{RoutingKey: opts.RoutingKey})
	if err != nil {
		return nil, err
	}
	if n != length {
		return nil, ErrContentNotFound
	}

	return dst[:n], nil
}

func (c *Client) GetContentStream(hash string, offset int64, length int64, opts struct {
	RoutingKey string
}) (chan []byte, error) {
	ctx, cancel := context.WithTimeout(c.ctx, getContentStreamRequestTimeout)
	contentChan := make(chan []byte)

	if opts.RoutingKey == "" {
		opts.RoutingKey = hash
	}

	go func() {
		defer close(contentChan)
		defer cancel()

		for attempt := 0; attempt < c.getContentAttempts(length); attempt++ {
			client, host, err := c.getGRPCClient(&ClientRequest{
				rt:        ClientRequestTypeRetrieval,
				hash:      hash,
				key:       opts.RoutingKey,
				hostIndex: attempt,
			})
			if err != nil {
				if err == ErrHostNotFound {
					_ = c.refreshRoutableHosts(ctx)
				}
				continue
			}

			stream, err := client.GetContentStream(ctx, &proto.CacheGetContentRequest{Hash: hash, Offset: offset, Length: length}, c.dataCallOptions()...)
			if err != nil {
				c.removeHost(host)
				continue
			}

			for {
				resp, err := stream.Recv()
				if err == io.EOF {
					return
				}

				if err != nil || !resp.Ok {
					c.removeLocalHostCache(hash)
					break
				}

				contentChan <- resp.Content
			}
		}
	}()

	return contentChan, nil
}

func (c *Client) manageLocalClientCache(ttl time.Duration, interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				now := time.Now()
				stale := make([]localHostCacheKey, 0)

				c.mu.RLock()
				for key, entry := range c.localHostCache {
					if now.Sub(entry.timestamp) > ttl {
						stale = append(stale, key)
					}
				}
				c.mu.RUnlock()

				c.mu.Lock()
				for _, key := range stale {
					delete(c.localHostCache, key)
				}
				c.mu.Unlock()

			case <-c.ctx.Done():
				return
			}
		}
	}()
}

func (c *Client) getGRPCClient(request *ClientRequest) (proto.CacheClient, *Host, error) {
	host, err := c.getHostForRequest(request)
	if err != nil {
		return nil, nil, err
	}
	if !host.HasEndpoint() {
		c.mu.Lock()
		delete(c.localHostCache, newLocalHostCacheKey(request.hash, request.key))
		c.mu.Unlock()
		return nil, host, ErrSelectedHostUnavailable
	}

	c.mu.RLock()
	client, exists := c.grpcClients[host.HostId]
	c.mu.RUnlock()
	if !exists {
		c.mu.Lock()
		delete(c.localHostCache, newLocalHostCacheKey(request.hash, request.key))
		c.mu.Unlock()

		return nil, host, ErrSelectedHostUnavailable
	}

	return client, host, nil
}

func (c *Client) getSelectedHostForRequest(rt ClientRequestType, hash string, routingKey string) (*Host, error) {
	return c.getHostForRequest(&ClientRequest{
		rt:        rt,
		hash:      hash,
		key:       routingKey,
		hostIndex: 0,
	})
}

func (c *Client) getSelectedGRPCClient(ctx context.Context, rt ClientRequestType, hash string, routingKey string) (proto.CacheClient, *Host, error) {
	return c.getGRPCClientForHostIndex(ctx, rt, hash, routingKey, 0)
}

func (c *Client) getGRPCClientForHostIndex(ctx context.Context, rt ClientRequestType, hash string, routingKey string, hostIndex int) (proto.CacheClient, *Host, error) {
	client, host, err := c.getGRPCClient(&ClientRequest{
		rt:        rt,
		hash:      hash,
		key:       routingKey,
		hostIndex: hostIndex,
	})
	if err == nil {
		return client, host, nil
	}
	if isStoreHostUnavailable(err) && c.hostDirectory != nil && c.hostMap != nil {
		_ = c.refreshRoutableHosts(ctx)
		client, host, err = c.getGRPCClient(&ClientRequest{
			rt:        rt,
			hash:      hash,
			key:       routingKey,
			hostIndex: hostIndex,
		})
	}
	return client, host, err
}

func (c *Client) getHostForRequest(request *ClientRequest) (*Host, error) {
	var host *Host = nil

	switch request.rt {
	case ClientRequestTypeStorage:
		c.mu.RLock()
		hosts := c.hasher.GetN(c.clientConfig.NTopHosts, request.key)
		if request.hostIndex >= 0 && request.hostIndex < len(hosts) {
			host = hosts[request.hostIndex]
		}
		c.mu.RUnlock()

	case ClientRequestTypeRetrieval:
		c.mu.RLock()
		cacheKey := newLocalHostCacheKey(request.hash, request.key)
		cachedHost, hostFound := c.localHostCache[cacheKey]
		c.mu.RUnlock()

		if hostFound && request.hostIndex == 0 {
			host = cachedHost.host
		} else {
			c.mu.Lock()

			hosts := c.hasher.GetN(c.clientConfig.NTopHosts, request.key)

			if request.hostIndex < 0 || request.hostIndex >= len(hosts) {
				host = nil
			} else {
				host = hosts[request.hostIndex]
				if request.hostIndex == 0 {
					c.localHostCache[cacheKey] = &localClientCache{
						host:      host,
						timestamp: time.Now(),
					}
				}
			}

			c.mu.Unlock()

		}
	default:
	}

	if host == nil {
		return nil, ErrHostNotFound
	}

	return host, nil
}

func (c *Client) StoreContent(chunks chan []byte, hash string, opts struct {
	RoutingKey string
}) (string, error) {
	return c.storeContentFromChunks(chunks, hash, "", opts.RoutingKey)
}

func (c *Client) SelectedStoreHostAvailable(hash string, routingKey string) bool {
	if c == nil {
		return false
	}
	if routingKey == "" {
		routingKey = hash
	}

	for attempt := 0; attempt < 2; attempt++ {
		host, err := c.getSelectedHostForRequest(ClientRequestTypeStorage, hash, routingKey)
		if err == nil && host != nil {
			if c.localServerForHost(host) != nil {
				return true
			}
			if c.localDiskStoreForHost(host) != nil {
				return true
			}
			if host.HasEndpoint() {
				c.mu.RLock()
				_, exists := c.grpcClients[host.HostId]
				c.mu.RUnlock()
				if exists {
					return true
				}
			}
		}

		if attempt > 0 || c.hostDirectory == nil || c.hostMap == nil {
			return false
		}
		_ = c.refreshRoutableHosts(c.ctx)
	}
	return false
}

func (c *Client) StoreContentAtPath(content []byte, cachePath string, opts StoreContentOptions) (string, error) {
	if opts.RoutingKey == "" {
		opts.RoutingKey = cachePath
	}

	ctx, cancel := context.WithTimeout(c.ctx, storeContentRequestTimeout)
	defer cancel()

	return c.withStoreFromContentLock(ctx, storeContentLockKey(cachePath, opts.RoutingKey), opts.Lock, func() (string, error) {
		return c.storeContentFromReaderWithContext(ctx, bytes.NewReader(content), opts.RoutingKey, cachePath, nil)
	})
}

// StoreContentFromLocalFile streams a caller-local file to the selected cache host.
func (c *Client) StoreContentFromLocalFile(source LocalContentSource, opts StoreContentOptions) (string, error) {
	hash, _, err := c.StoreContentFromLocalFileWithTrace(source, opts)
	return hash, err
}

func (c *Client) StoreContentFromLocalFileWithTrace(source LocalContentSource, opts StoreContentOptions) (hash string, trace OperationTrace, err error) {
	started := time.Now()
	if source.CachePath == "" {
		source.CachePath = source.Path
	}
	if opts.RoutingKey == "" {
		opts.RoutingKey = source.CachePath
	}
	trace.Operation = "store_local_file"
	trace.Hash = opts.RoutingKey
	trace.RoutingKey = opts.RoutingKey

	info, err := os.Stat(source.Path)
	if err != nil {
		trace.Result = "error"
		trace.DurationUs = time.Since(started).Microseconds()
		return "", trace, err
	}
	trace.Bytes = info.Size()
	trace.ExpectedSize = info.Size()
	metadata := cacheFSMetadataFromFileInfo(source.CachePath, info)

	ctx, cancel := context.WithTimeout(c.ctx, storeContentRequestTimeout)
	defer cancel()

	repairStatus := contentStatusMissing
	if metadataHash, status, ok := c.cachedLocalFileHashWithStoreTrace(ctx, source.CachePath, info, opts.RoutingKey, &trace, "precheck_metadata"); ok {
		trace.Hash = metadataHash
		trace.Result = "already_present"
		trace.DurationUs = time.Since(started).Microseconds()
		return metadataHash, trace, nil
	} else if status != "" && status != contentStatusComplete {
		repairStatus = status
	}
	if status, ok := c.cachedContentHashWithStoreTrace(ctx, opts.RoutingKey, info.Size(), &trace, "precheck_content_hash"); ok {
		trace.Result = "already_present"
		trace.DurationUs = time.Since(started).Microseconds()
		return opts.RoutingKey, trace, nil
	} else if status != "" && status != contentStatusComplete {
		repairStatus = status
	}

	file, err := os.Open(source.Path)
	if err != nil {
		trace.Result = "error"
		trace.DurationUs = time.Since(started).Microseconds()
		return "", trace, err
	}
	defer file.Close()

	store := func() (string, error) {
		if metadataHash, status, ok := c.cachedLocalFileHashWithStoreTrace(ctx, source.CachePath, info, opts.RoutingKey, &trace, "lock_precheck_metadata"); ok {
			trace.Hash = metadataHash
			trace.Result = "already_present_after_lock"
			return metadataHash, nil
		} else if status != "" && status != contentStatusComplete {
			repairStatus = status
		}
		if status, ok := c.cachedContentHashWithStoreTrace(ctx, opts.RoutingKey, info.Size(), &trace, "lock_precheck_content_hash"); ok {
			trace.Result = "already_present_after_lock"
			return opts.RoutingKey, nil
		} else if status != "" && status != contentStatusComplete {
			repairStatus = status
		}
		if _, err := file.Seek(0, io.SeekStart); err != nil {
			return "", err
		}
		hash, err := c.storeContentFromReaderWithContextAndTrace(ctx, file, opts.RoutingKey, source.CachePath, metadata, &trace, info.Size())
		if err != nil {
			return hash, err
		}
		if isContentHash(opts.RoutingKey) && hash != opts.RoutingKey {
			return hash, fmt.Errorf("stored content hash mismatch: expected %s, got %s", opts.RoutingKey, hash)
		}
		trace.Hash = hash
		trace.Result = storeRepairResult(repairStatus)
		return hash, nil
	}

	hash, err = c.withStoreFromContentLock(ctx, storeContentLockKey(source.CachePath, opts.RoutingKey), opts.Lock, store)
	if errors.Is(err, ErrUnableToAcquireLock) && isContentHash(opts.RoutingKey) {
		waitStarted := time.Now()
		if c.waitForStoredContent(ctx, opts.RoutingKey, opts.RoutingKey, info.Size(), storeContentLockWaitTimeout) {
			check := c.selectedHostContentCheckWithTrace(ctx, opts.RoutingKey, opts.RoutingKey, info.Size(), &trace, "lock_wait_result")
			trace.addStoreAttempt(0, check.host, "lock_wait", "present", 0, info.Size(), check.status, time.Since(waitStarted), nil)
			trace.Result = "lock_wait_present"
			trace.DurationUs = time.Since(started).Microseconds()
			return opts.RoutingKey, trace, nil
		}
		trace.addStoreAttempt(0, nil, "lock_wait", "timeout", 0, info.Size(), "", time.Since(waitStarted), ErrUnableToAcquireLock)
		hash, err = c.withStoreFromContentLock(ctx, storeContentLockKey(source.CachePath, opts.RoutingKey), opts.Lock, store)
	}
	if err != nil {
		trace.Result = operationTraceStoreResult(err)
		trace.DurationUs = time.Since(started).Microseconds()
		return hash, trace, err
	}
	if trace.Result == "" {
		trace.Result = "stored"
	}
	trace.DurationUs = time.Since(started).Microseconds()

	return hash, trace, nil
}

func (c *Client) waitForStoredContent(ctx context.Context, hash string, routingKey string, expectedSize int64, timeout time.Duration) bool {
	if hash == "" || timeout <= 0 {
		return false
	}
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(storeContentLockWaitInterval)
	defer ticker.Stop()

	for {
		exists, err := c.IsCachedOnSelectedHost(hash, routingKey, expectedSize)
		if err == nil && exists {
			return true
		}

		select {
		case <-ctx.Done():
			return false
		case <-deadline.C:
			return false
		case <-ticker.C:
		}
	}
}

func isContentHash(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func (c *Client) StoreContentFromLocalPath(source struct {
	Path      string
	CachePath string
}, opts struct {
	RoutingKey string
	Lock       bool
}) (string, error) {
	return c.StoreContentFromLocalFile(LocalContentSource{
		Path:      source.Path,
		CachePath: source.CachePath,
	}, StoreContentOptions{
		RoutingKey: opts.RoutingKey,
		Lock:       opts.Lock,
	})
}

func cacheFSMetadataFromFileInfo(cachePath string, info os.FileInfo) *proto.CacheFSMetadata {
	if info == nil || info.IsDir() {
		return nil
	}

	modTime := info.ModTime()
	accessTime := atime.Get(info)
	return &proto.CacheFSMetadata{
		Path:      cachePath,
		Mode:      fuse.S_IFREG | uint32(info.Mode().Perm()),
		Size:      uint64(info.Size()),
		Atime:     uint64(accessTime.Unix()),
		Mtime:     uint64(modTime.Unix()),
		Ctime:     uint64(modTime.Unix()),
		Atimensec: uint32(accessTime.Nanosecond()),
		Mtimensec: uint32(modTime.Nanosecond()),
		Ctimensec: uint32(modTime.Nanosecond()),
	}
}

func (c *Client) storeContentFromChunks(chunks chan []byte, hash string, cachePath string, routingKey string) (string, error) {
	ctx, cancel := context.WithTimeout(c.ctx, storeContentRequestTimeout)
	defer cancel()

	if routingKey == "" {
		routingKey = hash
	}

	client, _, err := c.getSelectedGRPCClient(ctx, ClientRequestTypeStorage, hash, routingKey)
	if err != nil {
		return "", err
	}

	stream, err := client.StoreContent(ctx)
	if err != nil {
		return "", err
	}

	start := time.Now()
	cachePathSent := false
	for chunk := range chunks {
		req := &proto.CacheStoreContentRequest{Content: chunk}
		if !cachePathSent {
			req.CachePath = cachePath
			cachePathSent = true
		}
		if err := stream.Send(req); err != nil {
			return "", err
		}
	}
	if cachePath != "" && !cachePathSent {
		if err := stream.Send(&proto.CacheStoreContentRequest{CachePath: cachePath}); err != nil {
			return "", err
		}
	}

	resp, err := stream.CloseAndRecv()
	if err != nil {
		return "", err
	}
	if resp == nil || !resp.Ok {
		return "", ErrUnableToPopulateContent
	}
	if hash != "" && resp.Hash != hash {
		return "", fmt.Errorf("stored content hash mismatch: expected %s, got %s", hash, resp.Hash)
	}

	Logger.Debugf("StoreContent[OK] - [expected=%s actual=%s routing_key=%s elapsed=%s]", hash, resp.Hash, routingKey, time.Since(start))
	return resp.Hash, nil
}

func (c *Client) storeContentFromReaderWithContext(ctx context.Context, reader io.Reader, routingKey string, cachePath string, fileMetadata *proto.CacheFSMetadata) (string, error) {
	return c.storeContentFromReaderWithContextAndTrace(ctx, reader, routingKey, cachePath, fileMetadata, nil, 0)
}

func (c *Client) storeContentFromReaderWithContextAndTrace(ctx context.Context, reader io.Reader, routingKey string, cachePath string, fileMetadata *proto.CacheFSMetadata, trace *OperationTrace, expectedSize int64) (string, error) {
	seeker, retryable := reader.(io.Seeker)
	var lastErr error

	for attempt := 0; attempt < c.getContentAttempts(0); attempt++ {
		if retryable {
			if _, err := seeker.Seek(0, io.SeekStart); err != nil {
				return "", err
			}
		} else if attempt > 0 {
			break
		}

		hostStarted := time.Now()
		host, err := c.getSelectedHostForRequest(ClientRequestTypeStorage, routingKey, routingKey)
		if err != nil || c.localServerForHost(host) == nil {
			_, host, err = c.getGRPCClientForHostIndex(ctx, ClientRequestTypeStorage, routingKey, routingKey, 0)
		}
		if err != nil {
			trace.addStoreAttempt(0, host, "store_host_select", operationTraceStoreResult(err), 0, expectedSize, "", time.Since(hostStarted), err)
			lastErr = err
			continue
		}

		storeStarted := time.Now()
		hash, err := c.storeContentFromReaderToHostWithContext(ctx, host, reader, routingKey, cachePath, fileMetadata)
		storeResult := operationTraceStoreResult(err)
		if err == nil {
			trace.addStoreAttempt(0, host, "store_selected_host", storeResult, expectedSize, expectedSize, contentStatusComplete, time.Since(storeStarted), nil)
			return hash, nil
		}
		trace.addStoreAttempt(0, host, "store_selected_host", storeResult, 0, expectedSize, contentStatusIncomplete, time.Since(storeStarted), err)
		if ctxErr := ctx.Err(); ctxErr != nil {
			return "", ctxErr
		}
		lastErr = err
		if c.hostDirectory == nil || c.hostMap == nil || !retryable {
			break
		}
		c.removeHost(host)
		_ = c.refreshRoutableHosts(ctx)
	}

	if lastErr != nil {
		return "", lastErr
	}
	return "", ErrHostNotFound
}

func storeRepairResult(status string) string {
	switch status {
	case contentStatusMissing:
		return "stored_missing"
	case contentStatusPartial:
		return "stored_partial"
	case contentStatusSizeMismatch:
		return "stored_size_mismatch"
	case contentStatusIncomplete, "":
		return "stored_incomplete"
	default:
		return "stored_" + status
	}
}

func (c *Client) storeContentFromReaderToHostWithContext(ctx context.Context, host *Host, reader io.Reader, expectedHash string, cachePath string, fileMetadata *proto.CacheFSMetadata) (string, error) {
	if host == nil {
		return "", ErrHostNotFound
	}
	if !isContentHash(expectedHash) {
		expectedHash = ""
	}

	if localServer := c.localServerForHost(host); localServer != nil {
		hash, size, err := localServer.StoreReader(ctx, reader, expectedHash)
		if err != nil {
			return "", err
		}
		if localServer.metadataStore != nil && cachePath != "" {
			metadata := cacheFSFileMetadataFromProto(fileMetadata, hash, size)
			if err := localServer.storeContentInCacheFS(ctx, cachePath, hash, size, metadata); err != nil {
				return "", fmt.Errorf("failed to update cachefs metadata: %w", err)
			}
		}
		return hash, nil
	}

	c.mu.RLock()
	client, exists := c.grpcClients[host.HostId]
	c.mu.RUnlock()
	if !exists {
		return "", ErrHostNotFound
	}

	stream, err := client.StoreContent(ctx)
	if err != nil {
		return "", err
	}

	start := time.Now()
	cachePathSent := false
	buf := make([]byte, writeBufferSizeBytes)
	for {
		n, readErr := reader.Read(buf)
		if n > 0 {
			chunk := make([]byte, n)
			copy(chunk, buf[:n])

			req := &proto.CacheStoreContentRequest{Content: chunk}
			if !cachePathSent {
				req.CachePath = cachePath
				req.Metadata = fileMetadata
				cachePathSent = true
			}
			if err := stream.Send(req); err != nil {
				return "", err
			}
		}

		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return "", readErr
		}
	}

	if cachePath != "" && !cachePathSent {
		if err := stream.Send(&proto.CacheStoreContentRequest{CachePath: cachePath, Metadata: fileMetadata}); err != nil {
			return "", err
		}
	}

	resp, err := stream.CloseAndRecv()
	if err != nil {
		return "", err
	}
	if resp == nil || !resp.Ok {
		return "", ErrUnableToPopulateContent
	}

	Logger.Debugf("Elapsed time to send content: %v", time.Since(start))
	return resp.Hash, nil
}

func (c *Client) withStoreFromContentLock(ctx context.Context, sourcePath string, lock bool, fn func() (string, error)) (string, error) {
	if !lock {
		return fn()
	}

	if err := c.metadataStore.SetStoreFromContentLock(ctx, c.locality, sourcePath); err != nil {
		return "", ErrUnableToAcquireLock
	}
	lockReleased := false
	releaseLock := func() error {
		if err := removeStoreFromContentLock(ctx, c.metadataStore, c.locality, sourcePath, "StoreContent"); err != nil {
			return err
		}
		lockReleased = true
		return nil
	}
	defer func() {
		if lockReleased {
			return
		}
		_ = releaseLock()
	}()

	storeContext, cancel := context.WithCancel(ctx)
	defer cancel()

	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-storeContext.Done():
				return
			case <-ticker.C:
				refreshStoreFromContentLock(storeContext, c.metadataStore, c.locality, sourcePath, "StoreContent")
			}
		}
	}()

	hash, err := fn()
	if err != nil {
		return hash, err
	}

	if err := releaseLock(); err != nil {
		return hash, err
	}
	return hash, nil
}

func (c *Client) StoreContentFromFUSE(source struct {
	Path string
}, opts struct {
	RoutingKey string
	Lock       bool
}) (string, error) {
	return c.StoreContentFromLocalFile(LocalContentSource{Path: source.Path}, StoreContentOptions{
		RoutingKey: opts.RoutingKey,
		Lock:       opts.Lock,
	})
}

// StoreContentFromLocalSource asks the selected cache host to read source.Path itself.
// Prefer StoreContentFromLocalFile unless the source path is guaranteed to exist on cache hosts.
func (c *Client) StoreContentFromLocalSource(source LocalContentSource, opts StoreContentOptions) (string, error) {
	ctx, cancel := context.WithTimeout(c.ctx, storeContentRequestTimeout)
	defer cancel()

	if opts.RoutingKey == "" {
		opts.RoutingKey = source.Path
	}

	req := &proto.CacheStoreContentFromSourceRequest{Source: &proto.CacheSource{Path: source.Path, CachePath: source.CachePath}}
	if isContentHash(opts.RoutingKey) {
		req.Source.ExpectedHash = opts.RoutingKey
	}
	hash, err := c.storeContentFromSource(ctx, req, source.Path, opts.RoutingKey, opts.Lock)
	if errors.Is(err, ErrUnableToAcquireLock) && isContentHash(opts.RoutingKey) {
		if c.waitForStoredContent(ctx, opts.RoutingKey, opts.RoutingKey, 0, storeContentLockWaitTimeout) {
			return opts.RoutingKey, nil
		}
		hash, err = c.storeContentFromSource(ctx, req, source.Path, opts.RoutingKey, opts.Lock)
	}
	return hash, err
}

func (c *Client) StoreContentFromS3(source struct {
	Path        string
	BucketName  string
	Region      string
	EndpointURL string
	AccessKey   string
	SecretKey   string
}, opts struct {
	RoutingKey string
	Lock       bool
}) (string, error) {
	return c.StoreContentFromS3Source(S3ContentSource{
		Path:           source.Path,
		BucketName:     source.BucketName,
		Region:         source.Region,
		EndpointURL:    source.EndpointURL,
		AccessKey:      source.AccessKey,
		SecretKey:      source.SecretKey,
		ForcePathStyle: true,
	}, StoreContentOptions{
		RoutingKey: opts.RoutingKey,
		Lock:       opts.Lock,
	})
}

func (c *Client) StoreContentFromS3Source(source S3ContentSource, opts StoreContentOptions) (string, error) {
	ctx, cancel := context.WithTimeout(c.ctx, storeContentRequestTimeout)
	defer cancel()

	if opts.RoutingKey == "" {
		opts.RoutingKey = fmt.Sprintf("%s/%s/%s/%s", source.EndpointURL, source.Region, source.BucketName, source.Path)
	}

	req := &proto.CacheStoreContentFromSourceRequest{Source: &proto.CacheSource{
		Path:           source.Path,
		CachePath:      source.CachePath,
		BucketName:     source.BucketName,
		Region:         source.Region,
		EndpointUrl:    source.EndpointURL,
		AccessKey:      source.AccessKey,
		SecretKey:      source.SecretKey,
		ForcePathStyle: source.ForcePathStyle,
	}}
	if isContentHash(opts.RoutingKey) {
		req.Source.ExpectedHash = opts.RoutingKey
	}
	hash, err := c.storeContentFromSource(ctx, req, opts.RoutingKey, opts.RoutingKey, opts.Lock)
	if errors.Is(err, ErrUnableToAcquireLock) && isContentHash(opts.RoutingKey) {
		if c.waitForStoredContent(ctx, opts.RoutingKey, opts.RoutingKey, 0, storeContentLockWaitTimeout) {
			return opts.RoutingKey, nil
		}
		hash, err = c.storeContentFromSource(ctx, req, opts.RoutingKey, opts.RoutingKey, opts.Lock)
	}
	return hash, err
}

func (c *Client) storeContentFromSource(ctx context.Context, req *proto.CacheStoreContentFromSourceRequest, hash, routingKey string, lock bool) (string, error) {
	var lastErr error

	for attempt := 0; attempt < c.getContentAttempts(0); attempt++ {
		client, host, err := c.getGRPCClientForHostIndex(ctx, ClientRequestTypeStorage, hash, routingKey, 0)
		if err != nil {
			lastErr = err
			continue
		}

		if lock {
			resp, err := client.StoreContentFromSourceWithLock(ctx, req)
			if err != nil {
				lastErr = err
				if c.hostDirectory == nil || c.hostMap == nil {
					break
				}
				c.removeHost(host)
				_ = c.refreshRoutableHosts(ctx)
				continue
			}

			if resp == nil {
				lastErr = ErrUnableToPopulateContent
				break
			}
			if resp.FailedToAcquireLock {
				return "", ErrUnableToAcquireLock
			}
			if !resp.Ok {
				lastErr = ErrUnableToPopulateContent
				break
			}
			return resp.Hash, nil
		}

		resp, err := client.StoreContentFromSource(ctx, req)
		if err != nil {
			lastErr = err
			if c.hostDirectory == nil || c.hostMap == nil {
				break
			}
			c.removeHost(host)
			_ = c.refreshRoutableHosts(ctx)
			continue
		}
		if resp == nil || !resp.Ok {
			lastErr = ErrUnableToPopulateContent
			break
		}
		return resp.Hash, nil
	}

	if lastErr != nil {
		return "", lastErr
	}
	return "", ErrHostNotFound
}

// PrimaryReadHost returns the host a read for routingKey would resolve to: the
// highest-ranked reachable host within the read's HRW window, computed from the
// same hasher and window the read path uses. Reconciliation materializes content
// on this host so a subsequent read's HRW lookup hits it instead of missing on a
// host that does not hold the content.
//
// It deliberately walks the full ranking (which retains unreachable logical-only
// hosts so placement stays stable during churn) and returns the first reachable
// host, mirroring how reads skip unavailable hosts and fall back in rank order.
func (c *Client) PrimaryReadHost(routingKey string) (*Host, error) {
	if routingKey == "" {
		return nil, ErrHostNotFound
	}

	c.mu.RLock()
	ranked := c.hasher.GetN(c.readReplicaHostCount(), routingKey)
	c.mu.RUnlock()

	for _, host := range ranked {
		if host != nil && host.HasEndpoint() {
			return host, nil
		}
	}
	return nil, ErrHostNotFound
}

// RankedReadHosts returns the HRW-ranked hosts for routingKey, including
// hosts whose endpoints are currently unavailable. Callers that make
// placement decisions (e.g. proactive reconciliation) need the full ranking
// so they can apply their own liveness policy; reads should keep using
// PrimaryReadHost, which prefers reachable hosts.
func (c *Client) RankedReadHosts(routingKey string) []*Host {
	if routingKey == "" {
		return nil
	}

	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.hasher.GetN(c.readReplicaHostCount(), routingKey)
}

// MaterializeFromReplica streams the content for (hash, routingKey) from a
// reachable peer that already holds it into the given local server's store. It
// returns true when the content is complete locally afterward. size must be
// known (> 0); callers should fall back to an origin fetch otherwise.
func (c *Client) MaterializeFromReplica(ctx context.Context, server *Server, hash, routingKey string, size int64) (bool, error) {
	if server == nil {
		return false, errors.New("local cache server is required")
	}
	if routingKey == "" {
		routingKey = hash
	}
	if server.HasCompleteContent(hash, size) {
		return true, nil
	}
	if size <= 0 {
		return false, nil
	}

	reachable, err := c.IsCachedReachable(hash, routingKey)
	if err != nil {
		return false, err
	}
	if !reachable {
		return false, nil
	}

	contentChan, err := c.GetContentStream(hash, 0, size, struct{ RoutingKey string }{RoutingKey: routingKey})
	if err != nil {
		return false, err
	}
	reader := newChannelReader(contentChan)
	defer reader.drain()

	if _, _, err := server.StoreReader(ctx, reader, hash); err != nil {
		return false, err
	}
	return server.HasCompleteContent(hash, size), nil
}

// channelReader adapts a chan []byte (from GetContentStream) to an io.Reader.
type channelReader struct {
	ch  chan []byte
	buf []byte
}

func newChannelReader(ch chan []byte) *channelReader {
	return &channelReader{ch: ch}
}

func (r *channelReader) Read(p []byte) (int, error) {
	for len(r.buf) == 0 {
		chunk, ok := <-r.ch
		if !ok {
			return 0, io.EOF
		}
		r.buf = chunk
	}
	n := copy(p, r.buf)
	r.buf = r.buf[n:]
	return n, nil
}

func (r *channelReader) drain() {
	for range r.ch {
	}
}

func isStoreHostUnavailable(err error) bool {
	return errors.Is(err, ErrHostNotFound) ||
		errors.Is(err, ErrSelectedHostUnavailable) ||
		errors.Is(err, ErrUnableToReachHost) ||
		errors.Is(err, ErrClientNotFound)
}

func IsStoreHostUnavailable(err error) bool {
	return isStoreHostUnavailable(err)
}

func (c *Client) HostsAvailable() bool {
	if c.hostMap == nil {
		return false
	}
	for _, host := range c.hostMap.GetAll() {
		if host.HasEndpoint() {
			return true
		}
	}
	return false
}

func (c *Client) WaitForHosts(timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	if c.HostsAvailable() {
		Logger.Infof("Cache available.")
		return nil
	}

	Logger.Infof("Waiting for hosts to be available...")
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			if c.HostsAvailable() {
				Logger.Infof("Cache available.")
				return nil
			}

			time.Sleep(1 * time.Second)
		}
	}
}

func (c *Client) GetState() error {
	ctx, cancel := context.WithTimeout(c.ctx, getContentRequestTimeout)
	defer cancel()

	client, _, err := c.getGRPCClient(&ClientRequest{rt: ClientRequestTypeRetrieval, hostIndex: 0})
	if err != nil {
		return err
	}

	_, err = client.GetState(ctx, &proto.CacheGetStateRequest{})
	if err != nil {
		return err
	}

	return nil
}

func grpcAuthInterceptor(token string) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		newCtx := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
		return invoker(newCtx, method, req, reply, cc, opts...)
	}
}

func grpcAuthStreamInterceptor(token string) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		newCtx := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
		return streamer(newCtx, desc, cc, method, opts...)
	}
}
