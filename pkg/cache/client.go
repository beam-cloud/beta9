package cache

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

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
	closestHostTimeout              = 30 * time.Second
	localClientCacheCleanupInterval = 5 * time.Second
	localClientCacheTTL             = 600 * time.Second

	// NOTE: This value for readAheadKB is separate from the cachefs config since the FUSE library does
	// weird stuff with the other read_ahead_kb value internally
	readAheadKB = 32768
)

type RendezvousHasher interface {
	Add(hosts ...*Host)
	Remove(host *Host)
	GetN(n int, key string) []*Host
}

type ClientOptions struct {
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

type Client struct {
	ctx                   context.Context
	locality              string
	clientConfig          ClientConfig
	globalConfig          GlobalConfig
	grpcClients           map[string]proto.CacheClient
	grpcConns             map[string]*grpc.ClientConn
	hostMap               *HostMap
	mu                    sync.RWMutex
	discoveryClient       *DiscoveryClient
	coordinator           Registry
	localHostCache        map[string]*localClientCache
	cachefsServer         *fuse.Server
	hasher                RendezvousHasher
	minRetryLengthBytes   int64
	maxGetContentAttempts int64
}

type localClientCache struct {
	host      *Host
	timestamp time.Time
}

func NewClient(ctx context.Context, cfg Config) (*Client, error) {
	InitLogger(cfg.Global.DebugMode, cfg.Global.PrettyLogs)

	coordinator, err := NewRemoteRegistry(cfg.Global, cfg.Client.Token)
	if err != nil {
		return nil, err
	}

	locality := cfg.Global.GetLocality()
	return NewClientWithRegistry(ctx, cfg, coordinator, locality)
}

func NewClientWithRegistry(ctx context.Context, cfg Config, registry Registry, locality string) (*Client, error) {
	InitLogger(cfg.Global.DebugMode, cfg.Global.PrettyLogs)

	if locality == "" {
		locality = cfg.Global.GetLocality()
	}

	bc := &Client{
		ctx:                   ctx,
		locality:              locality,
		clientConfig:          cfg.Client,
		globalConfig:          cfg.Global,
		grpcClients:           make(map[string]proto.CacheClient),
		grpcConns:             make(map[string]*grpc.ClientConn),
		localHostCache:        make(map[string]*localClientCache),
		mu:                    sync.RWMutex{},
		coordinator:           registry,
		hasher:                rendezvous.New[*Host](),
		minRetryLengthBytes:   cfg.Client.MinRetryLengthBytes,
		maxGetContentAttempts: max(int64(cfg.Client.MaxGetContentAttempts), 1),
	}

	bc.hostMap = NewHostMap(cfg.Global, bc.addHost)
	bc.discoveryClient = NewDiscoveryClient(cfg.Global, bc.hostMap, registry, locality)

	// Start searching for nearby cache hosts
	go bc.discoveryClient.Start(bc.ctx)

	// Monitor and cleanup local client cache
	go bc.manageLocalClientCache(localClientCacheTTL, localClientCacheCleanupInterval)

	// Mount cache as a FUSE filesystem if cachefs is enabled
	if bc.clientConfig.CacheFS.Enabled {
		startServer, _, server, err := Mount(ctx, FSSystemOpts{
			Config:   cfg.Client,
			Registry: registry,
			Client:   bc,
			Verbose:  bc.globalConfig.DebugMode,
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

	return nil
}

func (c *Client) GetNearbyHosts() ([]*Host, error) {
	hosts, err := c.coordinator.GetAvailableHosts(c.ctx, c.locality)
	if err != nil {
		return nil, err
	}

	return hosts, nil
}

func (c *Client) addHost(host *Host) error {
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

	c.grpcClients[host.HostId] = proto.NewCacheClient(conn)
	c.grpcConns[host.HostId] = conn
	c.hasher.Add(host)

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

	c.hostMap.Remove(host)

	c.mu.Lock()
	defer c.mu.Unlock()

	c.hasher.Remove(host)
	if conn, ok := c.grpcConns[host.HostId]; ok {
		_ = conn.Close()
		delete(c.grpcConns, host.HostId)
	}
	delete(c.grpcClients, host.HostId)
	for hash, entry := range c.localHostCache {
		if entry.host != nil && entry.host.HostId == host.HostId {
			delete(c.localHostCache, hash)
		}
	}
}

func (c *Client) removeLocalHostCache(hash string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.localHostCache, hash)
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

func (c *Client) IsPathCachedNearby(ctx context.Context, path string) bool {
	metadata, err := c.coordinator.GetFsNode(ctx, GenerateFsID(path))
	if err != nil {
		Logger.Errorf("error getting fs node: %v, path: %s", err, path)
		return false
	}

	exists, err := c.IsCachedNearby(metadata.Hash, path)
	if err != nil {
		Logger.Errorf("error checking if content is cached nearby: %v, hash: %s", err, metadata.Hash)
		return false
	}

	return exists
}

func (c *Client) IsCachedNearby(hash string, routingKey string) (bool, error) {
	hostsToCheck := c.clientConfig.NTopHosts

	for hostIndex := 0; hostIndex < hostsToCheck; hostIndex++ {
		client, host, err := c.getGRPCClient(&ClientRequest{
			rt:        ClientRequestTypeRetrieval,
			hash:      hash,
			key:       routingKey,
			hostIndex: hostIndex,
		})
		if err != nil {
			continue
		}

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

	return false, nil
}

func (c *Client) GetContent(hash string, offset int64, length int64, opts struct {
	RoutingKey string
}) ([]byte, error) {
	ctx, cancel := context.WithTimeout(c.ctx, getContentRequestTimeout)
	defer cancel()

	if opts.RoutingKey == "" {
		opts.RoutingKey = hash
	}

	for attempt := 0; attempt < c.getContentAttempts(length); attempt++ {
		client, host, err := c.getGRPCClient(&ClientRequest{
			rt:        ClientRequestTypeRetrieval,
			hash:      hash,
			key:       opts.RoutingKey,
			hostIndex: attempt,
		})
		if err != nil {
			continue
		}

		start := time.Now()
		getContentResponse, err := client.GetContent(ctx, &proto.CacheGetContentRequest{Hash: hash, Offset: offset, Length: length})
		if err != nil {
			c.removeHost(host)
			continue
		}
		if !getContentResponse.Ok {
			c.removeLocalHostCache(hash)
			continue
		}

		Logger.Debugf("Elapsed time to get content: %v", time.Since(start))
		return getContentResponse.Content, nil
	}

	return nil, ErrContentNotFound
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
				continue
			}

			stream, err := client.GetContentStream(ctx, &proto.CacheGetContentRequest{Hash: hash, Offset: offset, Length: length})
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
		return nil, nil, ErrHostNotFound
	}

	c.mu.RLock()
	client, exists := c.grpcClients[host.HostId]
	c.mu.RUnlock()
	if !exists {
		c.mu.Lock()
		delete(c.localHostCache, request.hash)
		c.mu.Unlock()

		return nil, nil, ErrHostNotFound
	}

	return client, host, nil
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

	return c.withStoreFromContentLock(ctx, cachePath, opts.Lock, func() (string, error) {
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

	file, err := os.Open(source.Path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return "", err
	}
	metadata := cacheFSMetadataFromFileInfo(source.CachePath, info)

	ctx, cancel := context.WithTimeout(c.ctx, storeContentRequestTimeout)
	defer cancel()

	return c.withStoreFromContentLock(ctx, source.CachePath, opts.Lock, func() (string, error) {
		return c.storeContentFromReaderWithContext(ctx, file, opts.RoutingKey, source.CachePath, metadata)
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

	client, _, err := c.getGRPCClient(&ClientRequest{
		rt:        ClientRequestTypeStorage,
		hash:      hash,
		key:       routingKey,
		hostIndex: 0,
	})
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

	Logger.Infof("StoreContent[OK] - [expected=%s actual=%s routing_key=%s elapsed=%s]", hash, resp.Hash, routingKey, time.Since(start))
	return resp.Hash, nil
}

func (c *Client) storeContentFromReaderWithContext(ctx context.Context, reader io.Reader, routingKey string, cachePath string, fileMetadata *proto.CacheFSMetadata) (string, error) {
	client, _, err := c.getGRPCClient(&ClientRequest{
		rt:        ClientRequestTypeStorage,
		hash:      routingKey,
		key:       routingKey,
		hostIndex: 0,
	})
	if err != nil {
		return "", err
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

	Logger.Debugf("Elapsed time to send content: %v", time.Since(start))
	return resp.Hash, nil
}

func (c *Client) withStoreFromContentLock(ctx context.Context, sourcePath string, lock bool, fn func() (string, error)) (string, error) {
	if !lock {
		return fn()
	}

	if err := c.coordinator.SetStoreFromContentLock(ctx, c.locality, sourcePath); err != nil {
		return "", ErrUnableToAcquireLock
	}
	lockReleased := false
	releaseLock := func() error {
		if err := c.coordinator.RemoveStoreFromContentLock(ctx, c.locality, sourcePath); err != nil {
			Logger.Errorf("StoreContent[ERR] - error removing lock: %v", err)
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
				if err := c.coordinator.RefreshStoreFromContentLock(ctx, c.locality, sourcePath); err != nil {
					Logger.Errorf("StoreContent[ERR] - error refreshing lock: %v", err)
				}
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
	return c.storeContentFromSource(ctx, req, source.Path, opts.RoutingKey, opts.Lock)
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
	return c.storeContentFromSource(ctx, req, opts.RoutingKey, opts.RoutingKey, opts.Lock)
}

func (c *Client) storeContentFromSource(ctx context.Context, req *proto.CacheStoreContentFromSourceRequest, hash, routingKey string, lock bool) (string, error) {
	var lastErr error
	for attempt := 0; attempt < c.getContentAttempts(0); attempt++ {
		client, host, err := c.getGRPCClient(&ClientRequest{
			rt:        ClientRequestTypeStorage,
			hash:      hash,
			key:       routingKey,
			hostIndex: attempt,
		})
		if err != nil {
			lastErr = err
			continue
		}

		if lock {
			resp, err := client.StoreContentFromSourceWithLock(ctx, req)
			if err != nil {
				lastErr = err
				c.removeHost(host)
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
			continue
		}
		if resp == nil || !resp.Ok {
			return "", ErrUnableToPopulateContent
		}
		return resp.Hash, nil
	}

	if lastErr != nil {
		return "", lastErr
	}
	return "", ErrHostNotFound
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
