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
	"sort"
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
	"google.golang.org/grpc/metadata"
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
	defaultRawReadWindowPartBytes   = 4 * 1024 * 1024

	// NOTE: This value for readAheadKB is separate from the cachefs config since the FUSE library does
	// weird stuff with the other read_ahead_kb value internally
	readAheadKB = 32768
)

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
	rawReadPools          map[string]*rawReadConnPool
	hostMap               *HostMap
	mu                    sync.RWMutex
	discoveryClient       *DiscoveryClient
	metadataStore         CacheMetadataStore
	localHostCache        map[string]*localClientCache
	cachefsServer         *fuse.Server
	hasher                RendezvousHasher
	hostDirectory         HostDirectory
	minRetryLengthBytes   int64
	maxGetContentAttempts int64
}

type localClientCache struct {
	host      *Host
	timestamp time.Time
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
		rawReadPools:          make(map[string]*rawReadConnPool),
		localHostCache:        make(map[string]*localClientCache),
		mu:                    sync.RWMutex{},
		metadataStore:         metadataStore,
		hasher:                rendezvous.New[*Host](),
		hostDirectory:         hostDirectory,
		minRetryLengthBytes:   cfg.Client.MinRetryLengthBytes,
		maxGetContentAttempts: max(int64(cfg.Client.MaxGetContentAttempts), 1),
	}

	bc.hostMap = NewHostMap(cfg.Global, bc.addHost)
	bc.discoveryClient = NewDiscoveryClient(cfg.Global, bc.hostMap, hostDirectory, locality)

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
			Logger.Errorf("Failed to update read_ahead_kb: %v", err)
		}

		bc.cachefsServer = server
	}

	return bc, nil
}

func (c *Client) Cleanup() error {
	if c.clientConfig.CacheFS.Enabled && c.cachefsServer != nil {
		c.cachefsServer.Unmount()
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	for hostID, conn := range c.grpcConns {
		_ = conn.Close()
		delete(c.grpcConns, hostID)
		delete(c.grpcClients, hostID)
	}
	for hostID, pool := range c.rawReadPools {
		pool.close()
		delete(c.rawReadPools, hostID)
	}
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
	for hash, entry := range c.localHostCache {
		if entry.host != nil && entry.host.HostId == hostID {
			delete(c.localHostCache, hash)
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
		logicalHost := host.LogicalOnly()
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
		c.hasher.Add(logicalHost)
		for hash, entry := range c.localHostCache {
			if entry.host != nil && entry.host.HostId == host.HostId {
				delete(c.localHostCache, hash)
			}
		}
		return nil
	}

	addr := host.Addr
	if host.PrivateAddr != "" {
		addr = host.PrivateAddr
	}

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
	for hash, entry := range c.localHostCache {
		if entry.host != nil && entry.host.HostId == host.HostId {
			delete(c.localHostCache, hash)
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
			if !c.isCurrentHostEndpoint(host) {
				return
			}

			err := func() error {
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
					return ErrInvalidHostVersion
				}

				if resp.GetVersion() != Version {
					return ErrInvalidHostVersion
				}

				return nil
			}()

			// We were unable to reach the host for some reason, remove it as a possible client
			if err != nil {
				c.removeHost(host)
				return
			}

		case <-c.ctx.Done():
			return
		}
	}
}

func (c *Client) removeHost(host *Host) {
	if host == nil {
		return
	}

	logicalHost := host.LogicalOnly()
	if c.hostMap != nil {
		var ok bool
		logicalHost, ok = c.hostMap.DeactivateEndpoint(host)
		if !ok {
			return
		}
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
	for hash, entry := range c.localHostCache {
		if entry.host != nil && entry.host.HostId == host.HostId {
			delete(c.localHostCache, hash)
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

func (c *Client) removeLocalHostCache(hash string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.localHostCache, hash)
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
	for _, group := range cacheHostCandidateGroups(hosts) {
		if group.hostID == "" {
			continue
		}

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
			routable = true
		}
	}

	if !routable && len(c.hostMap.GetAll()) == 0 {
		return ErrHostNotFound
	}
	return nil
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
	if c == nil || cachePath == "" || info == nil || info.IsDir() {
		return "", false
	}

	metadata, err := c.CacheFSMetadata(ctx, cachePath)
	if err != nil || !cacheFSMetadataMatchesFileInfo(metadata, info) {
		return "", false
	}

	if routingKey == "" {
		routingKey = cachePath
	}
	exists, err := c.IsCachedOnSelectedHost(metadata.Hash, routingKey)
	if err != nil || !exists {
		return "", false
	}
	return metadata.Hash, true
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

		resp, err := client.HasContent(c.ctx, &proto.CacheHasContentRequest{Hash: hash})
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
		resp, err := client.HasContent(c.ctx, &proto.CacheHasContentRequest{Hash: hash})
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
func (c *Client) IsCachedOnSelectedHost(hash string, routingKey string) (bool, error) {
	if routingKey == "" {
		routingKey = hash
	}

	client, host, err := c.getGRPCClient(&ClientRequest{
		rt:        ClientRequestTypeStorage,
		hash:      hash,
		key:       routingKey,
		hostIndex: 0,
	})
	if err != nil {
		if err == ErrHostNotFound {
			_ = c.refreshRoutableHosts(c.ctx)
		}
		return false, err
	}

	resp, err := client.HasContent(c.ctx, &proto.CacheHasContentRequest{Hash: hash})
	if err != nil {
		c.removeHost(host)
		return false, err
	}
	if !resp.Exists {
		c.removeLocalHostCache(hash)
	}
	return resp.Exists, nil
}

func (c *Client) rawReadInto(ctx context.Context, host *Host, hash string, offset int64, dst []byte) (read int64, err error) {
	started := time.Now()
	defer func() {
		if elapsed := time.Since(started); elapsed > time.Second || err != nil {
			hostID := ""
			addr := ""
			if host != nil {
				hostID = host.HostId
				addr = host.PrivateAddr
				if addr == "" {
					addr = host.Addr
				}
			}
			Logger.Debugf("cache remote page-region raw read result: host=%s addr=%s hash=%s offset=%d length=%d read=%d err=%v elapsed=%s", hostID, addr, hash, offset, len(dst), read, err, elapsed.Truncate(time.Millisecond))
		}
	}()
	if host == nil || !c.clientConfig.ReadTransport.Enabled {
		atomic.AddInt64(&cachePathStats.clientRawErrors, 1)
		return 0, ErrUnableToReachHost
	}
	addr := host.Addr
	if host.PrivateAddr != "" {
		addr = host.PrivateAddr
	}
	if addr == "" {
		atomic.AddInt64(&cachePathStats.clientRawErrors, 1)
		return 0, ErrUnableToReachHost
	}

	c.mu.Lock()
	pool := c.rawReadPools[host.HostId]
	if pool == nil {
		pool = newRawReadConnPool(addr, c.clientConfig.ReadTransport.MaxActiveConnsPerHost, c.clientConfig.ReadTransport.MaxIdleConnsPerHost)
		c.rawReadPools[host.HostId] = pool
	}
	c.mu.Unlock()

	conn, err := pool.get(ctx.Done())
	if err != nil {
		atomic.AddInt64(&cachePathStats.clientRawErrors, 1)
		return 0, err
	}
	reusable := false
	defer func() {
		if reusable {
			pool.put(conn)
		} else {
			_ = conn.Close()
			pool.release()
		}
	}()

	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
		defer conn.SetDeadline(time.Time{})
	}

	length := int64(len(dst))
	if err := writeRawReadRequest(conn, hash, offset, length); err != nil {
		atomic.AddInt64(&cachePathStats.clientRawErrors, 1)
		return 0, err
	}
	status, responseLength, err := readRawReadResponseHeader(conn)
	if err != nil {
		atomic.AddInt64(&cachePathStats.clientRawErrors, 1)
		return 0, err
	}
	if status == rawReadStatusMiss {
		cacheReadRawFallbackTotal.Inc()
		atomic.AddInt64(&cachePathStats.clientRawMisses, 1)
		reusable = true
		return 0, ErrContentNotFound
	}
	if status != rawReadStatusOK || responseLength != length {
		atomic.AddInt64(&cachePathStats.clientRawErrors, 1)
		return 0, ErrUnableToReachHost
	}
	if _, err := io.ReadFull(conn, dst); err != nil {
		atomic.AddInt64(&cachePathStats.clientRawErrors, 1)
		return 0, err
	}
	cacheReadRawTransportTotal.Inc()
	atomic.AddInt64(&cachePathStats.clientRawHits, 1)
	reusable = true
	return length, nil
}

func (c *Client) rawReadIntoWindowed(ctx context.Context, host *Host, hash string, offset int64, dst []byte) (int64, error) {
	if len(dst) == 0 {
		return 0, nil
	}

	partLength := c.clientConfig.Prefetch.PartLengthBytes
	if partLength <= 0 {
		partLength = defaultRawReadWindowPartBytes
	}
	if partLength > int64(len(dst)) {
		partLength = int64(len(dst))
	}

	maxParts := c.clientConfig.Prefetch.MaxPartsPerRead
	if maxParts <= 1 || int64(len(dst)) <= partLength {
		return c.rawReadInto(ctx, host, hash, offset, dst)
	}

	chunkCount := (len(dst) + int(partLength) - 1) / int(partLength)
	if maxParts > chunkCount {
		maxParts = chunkCount
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
	var read int64

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

	for worker := 0; worker < maxParts; worker++ {
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
	for off := 0; off < len(dst); off += int(partLength) {
		n := int(partLength)
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
	if localServer == nil || localServer.cas == nil {
		atomic.AddInt64(&cachePathStats.clientLocalPageFileMisses, 1)
		return "", 0, 0, false, nil
	}
	path, pageOffset, n, ok, err = localServer.cas.PageRegion(hash, offset, length)
	if err == nil && ok {
		atomic.AddInt64(&cachePathStats.clientLocalPageFileHits, 1)
		atomic.AddInt64(&cachePathStats.clientLocalPageFileBytes, int64(n))
	} else {
		atomic.AddInt64(&cachePathStats.clientLocalPageFileMisses, 1)
	}
	return path, pageOffset, n, ok, err
}

func (c *Client) ClientLocalPageFileViews(hash string, offset int64, length int64, opts ClientOptions) (views []ClientLocalPageFileView, err error) {
	started := time.Now()
	defer func() {
		if elapsed := time.Since(started); elapsed > 100*time.Millisecond || err != nil {
			Logger.Debugf("cache client-local page-file views result: hash=%s offset=%d length=%d views=%d err=%v elapsed=%s", hash, offset, length, len(views), err, elapsed.Truncate(time.Millisecond))
		}
	}()
	atomic.AddInt64(&cachePathStats.clientLocalPageFileRequests, 1)
	if opts.RoutingKey == "" {
		opts.RoutingKey = hash
	}
	if offset < 0 || length <= 0 {
		return nil, nil
	}
	checked := make(map[string]struct{})
	for attempt := 0; attempt < c.getContentAttempts(length); attempt++ {
		host, err := c.getHostForRequest(&ClientRequest{
			rt:        ClientRequestTypeRetrieval,
			hash:      hash,
			key:       opts.RoutingKey,
			hostIndex: attempt,
		})
		if err != nil {
			if err == ErrHostNotFound {
				_ = c.refreshRoutableHosts(c.ctx)
				host, err = c.getHostForRequest(&ClientRequest{
					rt:        ClientRequestTypeRetrieval,
					hash:      hash,
					key:       opts.RoutingKey,
					hostIndex: attempt,
				})
			}
			if err != nil {
				continue
			}
		}

		checked[host.HostId] = struct{}{}
		if views, ok := c.clientLocalPageFileViewsFromHost(host, hash, offset, length); ok {
			return views, nil
		}
	}

	for _, host := range c.remainingHostsForRequest(checked) {
		if views, ok := c.clientLocalPageFileViewsFromHost(host, hash, offset, length); ok {
			return views, nil
		}
	}

	atomic.AddInt64(&cachePathStats.clientLocalPageFileMisses, 1)
	return nil, ErrContentNotFound
}

func (c *Client) clientLocalPageFileViewsFromHost(host *Host, hash string, offset int64, length int64) ([]ClientLocalPageFileView, bool) {
	if host == nil {
		return nil, false
	}

	c.mu.RLock()
	localServer := c.localServers[host.HostId]
	c.mu.RUnlock()
	if localServer != nil && localServer.cas != nil {
		if views, ok := clientLocalPageFileViewsFromStore(localServer.cas, hash, offset, length); ok {
			cacheReadClientLocalPageFileTotal.Inc()
			atomic.AddInt64(&cachePathStats.clientLocalPageFileHits, 1)
			atomic.AddInt64(&cachePathStats.clientLocalPageFileBytes, length)
			return views, true
		}
	}

	return nil, false
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
	if ctx == nil {
		ctx = c.ctx
	}
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, getContentRequestTimeout)
		defer cancel()
	}
	if len(dst) == 0 {
		return 0, nil
	}
	if opts.RoutingKey == "" {
		opts.RoutingKey = hash
	}

	length := int64(len(dst))
	started := time.Now()
	defer func() {
		if elapsed := time.Since(started); elapsed > time.Second || err != nil {
			Logger.Debugf("cache read-into result: hash=%s routing_key=%s offset=%d length=%d read=%d err=%v elapsed=%s", hash, opts.RoutingKey, offset, length, read, err, elapsed.Truncate(time.Millisecond))
		}
	}()
	atomic.AddInt64(&cachePathStats.clientReadIntoRequests, 1)
	atomic.AddInt64(&cachePathStats.clientReadIntoBytes, length)

	if n, err := c.tryReadContentIntoKnownHosts(ctx, hash, offset, dst, opts); err == nil {
		return n, nil
	} else if c.hostDirectory == nil || c.hostMap == nil {
		return 0, err
	}

	return c.readContentIntoAfterHostRefresh(ctx, hash, offset, dst, opts)
}

func (c *Client) tryReadContentIntoKnownHosts(ctx context.Context, hash string, offset int64, dst []byte, opts ClientOptions) (int64, error) {
	length := int64(len(dst))
	checked := make(map[string]struct{})
	var primaryErr error
	var sawMiss bool
	var sawUnavailable bool
	var lastErr error
	for attempt := 0; attempt < c.getContentAttempts(length); attempt++ {
		host, err := c.getHostForRequest(&ClientRequest{
			rt:        ClientRequestTypeRetrieval,
			hash:      hash,
			key:       opts.RoutingKey,
			hostIndex: attempt,
		})
		if err != nil {
			if err == ErrHostNotFound {
				_ = c.refreshRoutableHosts(c.ctx)
				host, err = c.getHostForRequest(&ClientRequest{
					rt:        ClientRequestTypeRetrieval,
					hash:      hash,
					key:       opts.RoutingKey,
					hostIndex: attempt,
				})
			}
			if err != nil {
				if attempt == 0 {
					primaryErr = err
				}
				lastErr = err
				continue
			}
		}

		checked[host.HostId] = struct{}{}
		n, err := c.readContentIntoFromHost(ctx, host, hash, offset, dst)
		if err == nil && n == length {
			return n, nil
		}
		if attempt == 0 {
			primaryErr = err
		}
		if errors.Is(err, ErrSelectedHostUnavailable) || errors.Is(err, ErrUnableToReachHost) || errors.Is(err, ErrHostNotFound) {
			sawUnavailable = true
		} else if errors.Is(err, ErrContentNotFound) {
			sawMiss = true
		}
		if err != nil {
			lastErr = err
		}
	}

	for _, host := range c.remainingHostsForRequest(checked) {
		n, err := c.readContentIntoFromHost(ctx, host, hash, offset, dst)
		if err == nil && n == length {
			return n, nil
		}
		if errors.Is(err, ErrSelectedHostUnavailable) || errors.Is(err, ErrUnableToReachHost) || errors.Is(err, ErrHostNotFound) {
			sawUnavailable = true
		} else if errors.Is(err, ErrContentNotFound) {
			sawMiss = true
		}
		if err != nil {
			lastErr = err
		}
	}

	if primaryErr != nil {
		if errors.Is(primaryErr, ErrSelectedHostUnavailable) || errors.Is(primaryErr, ErrUnableToReachHost) || errors.Is(primaryErr, ErrHostNotFound) {
			return 0, ErrSelectedHostUnavailable
		}
		if errors.Is(primaryErr, ErrContentNotFound) {
			return 0, ErrContentNotFound
		}
		return 0, primaryErr
	}
	if sawUnavailable {
		return 0, ErrSelectedHostUnavailable
	}
	if sawMiss {
		return 0, ErrContentNotFound
	}
	if lastErr != nil {
		return 0, lastErr
	}
	return 0, ErrContentNotFound
}

func (c *Client) readContentIntoAfterHostRefresh(ctx context.Context, hash string, offset int64, dst []byte, opts ClientOptions) (int64, error) {
	if c.hostDirectory == nil || c.hostMap == nil {
		return 0, ErrHostNotFound
	}

	c.removeLocalHostCache(hash)
	if err := c.refreshRoutableHosts(ctx); err != nil && ctx.Err() != nil {
		return 0, err
	}
	if n, err := c.tryReadContentIntoKnownHosts(ctx, hash, offset, dst, opts); err == nil {
		Logger.Debugf("cache read-into recovered after host refresh: hash=%s routing_key=%s offset=%d length=%d", hash, opts.RoutingKey, offset, len(dst))
		return n, nil
	} else {
		return 0, err
	}
}

func (c *Client) readContentIntoFromHost(ctx context.Context, host *Host, hash string, offset int64, dst []byte) (int64, error) {
	if host == nil {
		return 0, ErrHostNotFound
	}

	length := int64(len(dst))
	c.mu.RLock()
	localServer := c.localServers[host.HostId]
	c.mu.RUnlock()
	if localServer != nil && localServer.cas != nil {
		n, err := localServer.cas.ReadAt(hash, offset, dst)
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
		c.removeLocalHostCache(hash)
		return 0, ErrSelectedHostUnavailable
	}

	if c.clientConfig.ReadTransport.Enabled {
		n, err := c.rawReadIntoWindowed(ctx, host, hash, offset, dst)
		if err == nil && n == length {
			return n, nil
		}
		if err == ErrContentNotFound {
			c.removeLocalHostCache(hash)
			return 0, err
		}
		if err != nil {
			cacheReadRawFallbackTotal.Inc()
		}
	}

	c.mu.RLock()
	client, exists := c.grpcClients[host.HostId]
	c.mu.RUnlock()
	if !exists {
		c.removeLocalHostCache(hash)
		return 0, ErrSelectedHostUnavailable
	}

	start := time.Now()
	getContentResponse, err := client.GetContent(ctx, &proto.CacheGetContentRequest{Hash: hash, Offset: offset, Length: length}, c.dataCallOptions()...)
	if err != nil {
		atomic.AddInt64(&cachePathStats.clientGRPCErrors, 1)
		if c.globalConfig.GRPCPayloadCodecV2 {
			cacheReadGRPCCodecV2FallbackTotal.Inc()
		}
		c.removeHost(host)
		return 0, ErrSelectedHostUnavailable
	}
	if !getContentResponse.Ok || int64(len(getContentResponse.Content)) != length {
		atomic.AddInt64(&cachePathStats.clientGRPCMisses, 1)
		c.removeLocalHostCache(hash)
		return 0, ErrContentNotFound
	}

	copy(dst, getContentResponse.Content)
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
				stale := make([]string, 0)

				c.mu.RLock()
				for hash, entry := range c.localHostCache {
					if now.Sub(entry.timestamp) > ttl {
						stale = append(stale, hash)
					}
				}
				c.mu.RUnlock()

				c.mu.Lock()
				for _, hash := range stale {
					delete(c.localHostCache, hash)
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
		delete(c.localHostCache, request.hash)
		c.mu.Unlock()
		return nil, host, ErrSelectedHostUnavailable
	}

	c.mu.RLock()
	client, exists := c.grpcClients[host.HostId]
	c.mu.RUnlock()
	if !exists {
		c.mu.Lock()
		delete(c.localHostCache, request.hash)
		c.mu.Unlock()

		return nil, host, ErrSelectedHostUnavailable
	}

	return client, host, nil
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
		cachedHost, hostFound := c.localHostCache[request.hash]
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
				c.localHostCache[request.hash] = &localClientCache{
					host:      host,
					timestamp: time.Now(),
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
	if source.CachePath == "" {
		source.CachePath = source.Path
	}
	if opts.RoutingKey == "" {
		opts.RoutingKey = source.CachePath
	}

	info, err := os.Stat(source.Path)
	if err != nil {
		return "", err
	}
	metadata := cacheFSMetadataFromFileInfo(source.CachePath, info)

	ctx, cancel := context.WithTimeout(c.ctx, storeContentRequestTimeout)
	defer cancel()

	if hash, ok := c.cachedLocalFileHash(ctx, source.CachePath, info, opts.RoutingKey); ok {
		return hash, nil
	}

	file, err := os.Open(source.Path)
	if err != nil {
		return "", err
	}
	defer file.Close()

	store := func() (string, error) {
		if hash, ok := c.cachedLocalFileHash(ctx, source.CachePath, info, opts.RoutingKey); ok {
			return hash, nil
		}
		if _, err := file.Seek(0, io.SeekStart); err != nil {
			return "", err
		}
		hash, err := c.storeContentFromReaderWithContext(ctx, file, opts.RoutingKey, source.CachePath, metadata)
		if err != nil {
			return hash, err
		}
		if isContentHash(opts.RoutingKey) && hash != opts.RoutingKey {
			return hash, fmt.Errorf("stored content hash mismatch: expected %s, got %s", opts.RoutingKey, hash)
		}
		return hash, nil
	}

	hash, err := c.withStoreFromContentLock(ctx, storeContentLockKey(source.CachePath, opts.RoutingKey), opts.Lock, store)
	if errors.Is(err, ErrUnableToAcquireLock) && isContentHash(opts.RoutingKey) {
		if c.waitForStoredContent(ctx, opts.RoutingKey, opts.RoutingKey, storeContentLockWaitTimeout) {
			return opts.RoutingKey, nil
		}
		hash, err = c.withStoreFromContentLock(ctx, storeContentLockKey(source.CachePath, opts.RoutingKey), opts.Lock, store)
	}
	if err != nil {
		return hash, err
	}

	return hash, nil
}

func (c *Client) waitForStoredContent(ctx context.Context, hash string, routingKey string, timeout time.Duration) bool {
	if hash == "" || timeout <= 0 {
		return false
	}
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(storeContentLockWaitInterval)
	defer ticker.Stop()

	for {
		exists, err := c.IsCachedOnSelectedHost(hash, routingKey)
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

	var client proto.CacheClient
	var err error
	for hostIndex := 0; hostIndex < c.storeHostAttempts(); hostIndex++ {
		client, _, err = c.getGRPCClient(&ClientRequest{
			rt:        ClientRequestTypeStorage,
			hash:      hash,
			key:       routingKey,
			hostIndex: hostIndex,
		})
		if err == nil {
			break
		}
		if isStoreHostUnavailable(err) {
			_ = c.refreshRoutableHosts(ctx)
			continue
		}
	}
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
	seeker, retryable := reader.(io.Seeker)
	var lastErr error
	for attempt := 0; attempt < c.getContentAttempts(0); attempt++ {
		for hostIndex := 0; hostIndex < c.storeHostAttempts(); hostIndex++ {
			if retryable {
				if _, err := seeker.Seek(0, io.SeekStart); err != nil {
					return "", err
				}
			} else if attempt > 0 || hostIndex > 0 {
				break
			}

			_, host, err := c.getGRPCClient(&ClientRequest{
				rt:        ClientRequestTypeStorage,
				hash:      routingKey,
				key:       routingKey,
				hostIndex: hostIndex,
			})
			if err != nil {
				lastErr = err
				if isStoreHostUnavailable(err) {
					_ = c.refreshRoutableHosts(ctx)
				}
				continue
			}

			hash, err := c.storeContentFromReaderToHostWithContext(ctx, host, reader, cachePath, fileMetadata)
			if err == nil {
				return hash, nil
			}
			if ctxErr := ctx.Err(); ctxErr != nil {
				return "", ctxErr
			}
			lastErr = err
			c.removeHost(host)
			_ = c.refreshRoutableHosts(ctx)
			if !retryable {
				break
			}
		}
		if !retryable {
			break
		}
	}

	if lastErr != nil {
		return "", lastErr
	}
	return "", ErrHostNotFound
}

func (c *Client) storeContentFromReaderToHostWithContext(ctx context.Context, host *Host, reader io.Reader, cachePath string, fileMetadata *proto.CacheFSMetadata) (string, error) {
	if host == nil {
		return "", ErrHostNotFound
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
		if c.waitForStoredContent(ctx, opts.RoutingKey, opts.RoutingKey, storeContentLockWaitTimeout) {
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
		if c.waitForStoredContent(ctx, opts.RoutingKey, opts.RoutingKey, storeContentLockWaitTimeout) {
			return opts.RoutingKey, nil
		}
		hash, err = c.storeContentFromSource(ctx, req, opts.RoutingKey, opts.RoutingKey, opts.Lock)
	}
	return hash, err
}

func (c *Client) storeContentFromSource(ctx context.Context, req *proto.CacheStoreContentFromSourceRequest, hash, routingKey string, lock bool) (string, error) {
	var lastErr error
	for attempt := 0; attempt < c.getContentAttempts(0); attempt++ {
		for hostIndex := 0; hostIndex < c.storeHostAttempts(); hostIndex++ {
			client, host, err := c.getGRPCClient(&ClientRequest{
				rt:        ClientRequestTypeStorage,
				hash:      hash,
				key:       routingKey,
				hostIndex: hostIndex,
			})
			if err != nil {
				lastErr = err
				if isStoreHostUnavailable(err) {
					_ = c.refreshRoutableHosts(ctx)
				}
				continue
			}

			if lock {
				resp, err := client.StoreContentFromSourceWithLock(ctx, req)
				if err != nil {
					lastErr = err
					c.removeHost(host)
					_ = c.refreshRoutableHosts(ctx)
					continue
				}

				if resp == nil {
					return "", ErrUnableToPopulateContent
				}
				if resp.FailedToAcquireLock {
					return "", ErrUnableToAcquireLock
				}
				if !resp.Ok {
					return "", ErrUnableToPopulateContent
				}
				return resp.Hash, nil
			}

			resp, err := client.StoreContentFromSource(ctx, req)
			if err != nil {
				lastErr = err
				c.removeHost(host)
				_ = c.refreshRoutableHosts(ctx)
				continue
			}
			if resp == nil || !resp.Ok {
				return "", ErrUnableToPopulateContent
			}
			return resp.Hash, nil
		}
	}

	if lastErr != nil {
		return "", lastErr
	}
	return "", ErrHostNotFound
}

func (c *Client) storeHostAttempts() int {
	if c == nil || c.clientConfig.NTopHosts <= 0 {
		return 1
	}
	return c.clientConfig.NTopHosts
}

func isStoreHostUnavailable(err error) bool {
	return errors.Is(err, ErrHostNotFound) ||
		errors.Is(err, ErrSelectedHostUnavailable) ||
		errors.Is(err, ErrUnableToReachHost) ||
		errors.Is(err, ErrClientNotFound)
}

func (c *Client) HostsAvailable() bool {
	return c.hostMap.Members().Cardinality() > 0
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
