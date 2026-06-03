package cache

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/beam-cloud/beta9/pkg/cache/cachegrpc"
	proto "github.com/beam-cloud/beta9/proto"
	"github.com/djherbis/atime"
	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/uuid"
	"github.com/hanwen/go-fuse/v2/fuse"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	writeBufferSizeBytes           int   = 1024 * 1024
	getContentStreamChunkSize      int64 = 4 * 1024 * 1024 // 4MB
	cacheServerGracefulStopTimeout       = 30 * time.Second
)

type ServerOpts struct {
	HostID        string
	MetadataStore CacheMetadataStore
	AdvertiseAddr string
	PeerClient    *Client
}

type ServerOption func(*ServerOpts)

type Server struct {
	ctx    context.Context
	cancel context.CancelFunc
	proto.UnimplementedCacheServer
	hostId        string
	locality      string
	privateIpAddr string
	publicIpAddr  string
	cas           *Store
	serverConfig  ServerConfig
	globalConfig  GlobalConfig
	config        Config
	metadataStore CacheMetadataStore
	peerClient    *Client
	reconciler    *requiredContentReconciler
	grpcServer    *grpc.Server
	listener      net.Listener
	s3ClientCache sync.Map
	draining      atomic.Bool
	closeOnce     sync.Once
}

func NewServer(ctx context.Context, cfg Config, locality string) (*Server, error) {
	return NewServerWithOptions(ctx, cfg, locality)
}

func NewServerWithOptions(ctx context.Context, cfg Config, locality string, options ...ServerOption) (*Server, error) {
	InitLogger(cfg.Global.DebugMode, cfg.Global.PrettyLogs)
	startCachePathStatsLogger()
	cfg.RequiredContent = NormalizeRequiredContentConfig(cfg.RequiredContent)

	opts := &ServerOpts{}
	for _, opt := range options {
		opt(opts)
	}

	currentHost := &Host{
		RTT: 0,
	}

	var metadataStore CacheMetadataStore
	effectiveServerConfig := cfg.Server
	var err error = nil
	if opts.MetadataStore != nil {
		metadataStore = opts.MetadataStore
	} else {
		metadataStore, effectiveServerConfig, err = newMetadataStore(cfg)
		cfg.Server = effectiveServerConfig
	}
	if err != nil {
		return nil, err
	}

	// Create the disk cache directory if it doesn't exist
	err = os.MkdirAll(cfg.Server.DiskCacheDir, 0755)
	if err != nil {
		return nil, err
	}

	hostId := opts.HostID
	if hostId == "" {
		hostId = getHostId(cfg.Server)
	}
	Logger.Infof("Server<%s> started", hostId)

	publicIpAddr, _ := GetPublicIpAddr()
	if publicIpAddr != "" {
		Logger.Infof("Discovered public ip address: %s", publicIpAddr)
	}

	privateIpAddr, _ := GetPrivateIpAddr()
	if privateIpAddr != "" {
		Logger.Infof("Discovered private ip address: %s", privateIpAddr)
	}

	currentHost.HostId = hostId
	currentHost.Addr = fmt.Sprintf("%s:%d", publicIpAddr, cfg.Global.ServerPort)
	currentHost.PrivateAddr = fmt.Sprintf("%s:%d", privateIpAddr, cfg.Global.ServerPort)
	if opts.AdvertiseAddr != "" {
		currentHost.Addr = opts.AdvertiseAddr
		currentHost.PrivateAddr = opts.AdvertiseAddr
	}
	currentHost.CapacityUsagePct = 0

	serverCtx, cancel := context.WithCancel(ctx)

	cas, err := NewStore(serverCtx, currentHost, locality, metadataStore, cfg)
	if err != nil {
		cancel()
		return nil, err
	}

	for _, sourceConfig := range cfg.Server.Sources {
		_, err := NewSource(sourceConfig)
		if err != nil {
			Logger.Errorf("Failed to configure content source: %+v", err)
			continue
		}

		Logger.Infof("Configured and mounted source: %+v", sourceConfig.FilesystemName)
	}

	cs := &Server{
		ctx:           serverCtx,
		cancel:        cancel,
		hostId:        hostId,
		locality:      locality,
		cas:           cas,
		serverConfig:  effectiveServerConfig,
		globalConfig:  cfg.Global,
		config:        cfg,
		metadataStore: metadataStore,
		peerClient:    opts.PeerClient,
		privateIpAddr: privateIpAddr,
		publicIpAddr:  publicIpAddr,
		s3ClientCache: sync.Map{},
	}
	cs.reconciler = newRequiredContentReconciler(cs, cfg.RequiredContent)

	return cs, nil
}

func WithServerHostID(hostID string) ServerOption {
	return func(opts *ServerOpts) {
		opts.HostID = hostID
	}
}

func WithServerMetadataStore(metadataStore CacheMetadataStore) ServerOption {
	return func(opts *ServerOpts) {
		opts.MetadataStore = metadataStore
	}
}

func WithServerAdvertiseAddr(addr string) ServerOption {
	return func(opts *ServerOpts) {
		opts.AdvertiseAddr = addr
	}
}

func WithServerPeerClient(client *Client) ServerOption {
	return func(opts *ServerOpts) {
		opts.PeerClient = client
	}
}

func (cs *Server) Host() *Host {
	if cs == nil || cs.cas == nil {
		return nil
	}
	return cs.cas.currentHost
}

func (cs *Server) UsagePct() float64 {
	if cs == nil {
		return 0
	}
	return cs.usagePct()
}

func newMetadataStore(cfg Config) (CacheMetadataStore, ServerConfig, error) {
	metadataStore, err := NewRedisCacheMetadataStore(cfg.Global, cfg.Server)
	return metadataStore, cfg.Server, err
}

func getHostId(serverConfig ServerConfig) string {
	filePath := filepath.Join(serverConfig.DiskCacheDir, "HOST_ID")

	hostId := ""
	if content, err := os.ReadFile(filePath); err == nil {
		hostId = strings.TrimSpace(string(content))
	} else {
		hostId = fmt.Sprintf("%s-%s", HostPrefix, uuid.New().String()[:6])
		os.WriteFile(filePath, []byte(hostId), 0644)
	}

	return hostId
}

func (cs *Server) grpcServerOptions() []grpc.ServerOption {
	maxMessageSize := cs.globalConfig.GRPCMessageSizeBytes

	initialWindowSize := cs.globalConfig.GRPCInitialWindowSize
	if initialWindowSize == 0 {
		initialWindowSize = 4 * 1024 * 1024
	}

	initialConnWindowSize := cs.globalConfig.GRPCInitialConnWindowSize
	if initialConnWindowSize == 0 {
		initialConnWindowSize = 32 * 1024 * 1024
	}

	writeBufferSize := cs.globalConfig.GRPCWriteBufferSize
	if writeBufferSize == 0 {
		writeBufferSize = 256 * 1024
	}

	readBufferSize := cs.globalConfig.GRPCReadBufferSize
	if readBufferSize == 0 {
		readBufferSize = 256 * 1024
	}

	maxConcurrentStreams := cs.globalConfig.GRPCMaxConcurrentStreams
	if maxConcurrentStreams == 0 {
		maxConcurrentStreams = 1024
	}

	numStreamWorkers := cs.globalConfig.GRPCNumStreamWorkers
	if numStreamWorkers == 0 {
		numStreamWorkers = runtime.NumCPU() * 2
	}

	opts := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(maxMessageSize),
		grpc.MaxSendMsgSize(maxMessageSize),
		grpc.InitialWindowSize(int32(initialWindowSize)),
		grpc.InitialConnWindowSize(int32(initialConnWindowSize)),
		grpc.WriteBufferSize(writeBufferSize),
		grpc.ReadBufferSize(readBufferSize),
		grpc.MaxConcurrentStreams(uint32(maxConcurrentStreams)),
		grpc.NumStreamWorkers(uint32(numStreamWorkers)),
	}
	if cs.globalConfig.GRPCPayloadCodecV2 {
		opts = append(opts, grpc.ForceServerCodecV2(cachegrpc.New(cs.globalConfig.GRPCPayloadCodecMinBytes)))
	}
	return opts
}

func (cs *Server) Serve(bindAddr string, advertiseHost string) (string, error) {
	localListener, err := net.Listen("tcp", bindAddr)
	if err != nil {
		return "", err
	}

	advertiseAddr := cs.advertiseAddr(localListener.Addr(), advertiseHost)

	if advertiseAddr != "" {
		cs.cas.currentHost.Addr = advertiseAddr
		cs.cas.currentHost.PrivateAddr = advertiseAddr
	}

	serveListener := net.Listener(localListener)
	if cs.serverConfig.ReadTransport.Enabled {
		serveListener = newCacheMuxListener(localListener, cs.handleRawReadConn)
	}

	s := grpc.NewServer(cs.grpcServerOptions()...)
	proto.RegisterCacheServer(s, cs)

	cs.grpcServer = s
	cs.listener = serveListener

	Logger.Infof("Running %s@%s", cs.hostId, advertiseAddr)
	Logger.Debugf("cache server config: %+v", cs.serverConfig)

	go func() {
		if err := s.Serve(serveListener); err != nil {
			if err != grpc.ErrServerStopped {
				Logger.Warnf("cache server stopped: %v", err)
			}
		}
	}()
	if cs.reconciler != nil {
		cs.reconciler.Start()
	}

	return advertiseAddr, nil
}

func (cs *Server) advertiseAddr(listenerAddr net.Addr, advertiseHost string) string {
	advertiseAddr := cs.cas.currentHost.PrivateAddr
	tcpAddr, ok := listenerAddr.(*net.TCPAddr)
	if !ok {
		if advertiseAddr != "" {
			return advertiseAddr
		}
		return listenerAddr.String()
	}

	port := fmt.Sprintf("%d", tcpAddr.Port)
	if advertiseHost != "" {
		return net.JoinHostPort(normalizeAdvertiseHost(advertiseHost), port)
	}

	// If the listener is bound to a concrete host, advertise that same host.
	// Advertising a discovered private IP for a loopback-bound listener makes
	// tests and local-only callers dial an address the server is not listening on.
	if tcpAddr.IP != nil && !tcpAddr.IP.IsUnspecified() {
		return net.JoinHostPort(tcpAddr.IP.String(), port)
	}

	if cs.privateIpAddr != "" {
		return net.JoinHostPort(cs.privateIpAddr, port)
	}
	if advertiseAddr != "" {
		return advertiseAddr
	}
	return listenerAddr.String()
}

func normalizeAdvertiseHost(host string) string {
	if strings.HasPrefix(host, "[") && strings.HasSuffix(host, "]") {
		return strings.TrimSuffix(strings.TrimPrefix(host, "["), "]")
	}

	return host
}

func (cs *Server) StartServer(port uint) error {
	_, err := cs.Serve(fmt.Sprintf(":%d", port), "")
	if err != nil {
		return err
	}

	<-cs.ctx.Done()
	return cs.Close()
}

func (cs *Server) Close() error {
	cs.closeOnce.Do(func() {
		cs.Drain()
		if cs.grpcServer != nil {
			stopped := make(chan struct{})
			go func() {
				cs.grpcServer.GracefulStop()
				close(stopped)
			}()
			select {
			case <-stopped:
			case <-time.After(cacheServerGracefulStopTimeout):
				cs.grpcServer.Stop()
			}
		}
		if cs.cancel != nil {
			cs.cancel()
		}
		if cs.listener != nil {
			_ = cs.listener.Close()
		}
		if cs.cas != nil {
			cs.cas.Cleanup()
		}
	})

	return nil
}

func (cs *Server) Drain() {
	if cs != nil {
		cs.draining.Store(true)
	}
}

func (cs *Server) rejectIfDraining() error {
	if cs != nil && cs.draining.Load() {
		return status.Error(codes.Unavailable, "cache server is draining")
	}
	return nil
}

func (cs *Server) GetContent(ctx context.Context, req *proto.CacheGetContentRequest) (*proto.CacheGetContentResponse, error) {
	if err := cs.rejectIfDraining(); err != nil {
		return nil, err
	}
	atomic.AddInt64(&cachePathStats.serverGRPCGetRequests, 1)
	if req == nil {
		atomic.AddInt64(&cachePathStats.serverGRPCGetMisses, 1)
		return nil, status.Error(codes.InvalidArgument, "request is nil")
	}
	maxMessageSize := int64(cs.globalConfig.GRPCMessageSizeBytes)
	if maxMessageSize <= 0 {
		maxMessageSize = 4 * 1024 * 1024
	}
	if req.Length < 0 || req.Length > maxMessageSize {
		atomic.AddInt64(&cachePathStats.serverGRPCGetMisses, 1)
		return nil, status.Errorf(codes.InvalidArgument, "invalid content length: %d", req.Length)
	}

	dst := make([]byte, int(req.Length))
	n, err := cs.cas.Get(req.Hash, req.Offset, req.Length, dst)
	if err != nil {
		atomic.AddInt64(&cachePathStats.serverGRPCGetMisses, 1)
		Logger.Debugf("Get - [%s] - %v", req.Hash, err)
		return &proto.CacheGetContentResponse{Content: nil, Ok: false}, nil
	}
	if n != req.Length {
		atomic.AddInt64(&cachePathStats.serverGRPCGetMisses, 1)
		Logger.Debugf("Get - [%s] short read: requested=%d read=%d", req.Hash, req.Length, n)
		return &proto.CacheGetContentResponse{Content: nil, Ok: false}, nil
	}

	atomic.AddInt64(&cachePathStats.serverGRPCGetHits, 1)
	atomic.AddInt64(&cachePathStats.serverGRPCGetBytes, n)
	Logger.Debugf("Get[OK] - [%s] (offset=%d, length=%d)", req.Hash, req.Offset, req.Length)
	return &proto.CacheGetContentResponse{Content: dst[:n], Ok: true}, nil
}

func (cs *Server) HasContent(ctx context.Context, req *proto.CacheHasContentRequest) (*proto.CacheHasContentResponse, error) {
	if err := cs.rejectIfDraining(); err != nil {
		return nil, err
	}
	status := cs.cas.ContentStatus(req.Hash)
	if req.ExpectedSize > 0 {
		status = cs.cas.ContentStatus(req.Hash, req.ExpectedSize)
	}
	return &proto.CacheHasContentResponse{Exists: status == contentStatusComplete, Status: status, Ok: true}, nil
}

func (cs *Server) GetContentStream(req *proto.CacheGetContentRequest, stream proto.Cache_GetContentStreamServer) error {
	if err := cs.rejectIfDraining(); err != nil {
		return err
	}
	atomic.AddInt64(&cachePathStats.serverStreamRequests, 1)
	if req == nil {
		atomic.AddInt64(&cachePathStats.serverStreamErrors, 1)
		return status.Error(codes.InvalidArgument, "request is nil")
	}
	if req.Length < 0 {
		atomic.AddInt64(&cachePathStats.serverStreamErrors, 1)
		return status.Errorf(codes.InvalidArgument, "invalid content length: %d", req.Length)
	}

	const chunkSize = getContentStreamChunkSize
	offset := req.Offset
	remainingLength := req.Length

	Logger.Debugf("GetContentStream[ACK] - [%s] - offset=%d, length=%d, %d bytes", req.Hash, offset, req.Length, remainingLength)

	for remainingLength > 0 {
		currentChunkSize := chunkSize
		if remainingLength < int64(chunkSize) {
			currentChunkSize = remainingLength
		}

		dst := make([]byte, currentChunkSize)
		n, err := cs.cas.Get(req.Hash, offset, currentChunkSize, dst)
		if err != nil {
			atomic.AddInt64(&cachePathStats.serverStreamErrors, 1)
			Logger.Debugf("GetContentStream - [%s] - %v", req.Hash, err)
			return status.Errorf(codes.NotFound, "Content not found: %v", err)
		}
		if n == 0 {
			break
		}

		Logger.Debugf("GetContentStream[TX] - [%s] - %d bytes", req.Hash, n)
		atomic.AddInt64(&cachePathStats.serverStreamChunks, 1)
		atomic.AddInt64(&cachePathStats.serverStreamBytes, n)
		if err := stream.Send(&proto.CacheGetContentResponse{
			Ok:      true,
			Content: dst[:n],
		}); err != nil {
			atomic.AddInt64(&cachePathStats.serverStreamErrors, 1)
			return status.Errorf(codes.Internal, "Failed to send content chunk: %v", err)
		}

		// Break if this is the last chunk
		if n < currentChunkSize {
			break
		}

		offset += int64(n)
		remainingLength -= int64(n)
	}

	return nil
}

func (cs *Server) storeReader(ctx context.Context, reader io.Reader) (string, uint64, error) {
	Logger.Debugf("Store[ACK]")

	hash, size, err := cs.cas.AddReader(ctx, reader)
	if err != nil {
		Logger.Warnf("Store[ERR] - [%s] - %v", hash, err)
		return "", 0, status.Errorf(codes.Internal, "Failed to add content: %v", err)
	}

	Logger.Debugf("Store[OK] - [%s] (%d bytes)", hash, size)
	return hash, uint64(size), nil
}

func (cs *Server) storeReaderWithExpectedHash(ctx context.Context, reader io.Reader, expectedHash string) (string, uint64, error) {
	if !isContentHash(expectedHash) {
		return cs.storeReader(ctx, reader)
	}

	Logger.Debugf("Store[ACK] - [expected_hash=%s]", expectedHash)
	hash, size, err := cs.cas.AddReaderWithExpectedHash(ctx, reader, expectedHash)
	if err != nil {
		Logger.Warnf("Store[ERR] - [expected_hash=%s actual=%s] - %v", expectedHash, hash, err)
		return "", 0, status.Errorf(codes.Internal, "Failed to add content: %v", err)
	}

	Logger.Debugf("Store[OK] - [%s] (%d bytes)", hash, size)
	return hash, uint64(size), nil
}

func (cs *Server) StoreContentInCacheFS(ctx context.Context, path string, hash string, size uint64) error {
	fileInfo, err := os.Stat(path)
	if err != nil {
		return err
	}

	return cs.storeContentInCacheFS(ctx, path, hash, size, cacheFSFileMetadataFromInfo(fileInfo, hash, size))
}

func (cs *Server) StoreSyntheticContentInCacheFS(ctx context.Context, path string, hash string, size uint64) error {
	return cs.storeContentInCacheFS(ctx, path, hash, size, newCacheFSFileMetadata(hash, size))
}

type cacheFSFileMetadata struct {
	hash      string
	size      uint64
	mode      uint32
	atime     uint64
	mtime     uint64
	ctime     uint64
	atimensec uint32
	mtimensec uint32
	ctimensec uint32
}

func newCacheFSFileMetadata(hash string, size uint64) *cacheFSFileMetadata {
	now := time.Now()
	nowSec := uint64(now.Unix())
	nowNsec := uint32(now.Nanosecond())
	return &cacheFSFileMetadata{
		hash:      hash,
		size:      size,
		mode:      fuse.S_IFREG | 0644,
		atime:     nowSec,
		mtime:     nowSec,
		ctime:     nowSec,
		atimensec: nowNsec,
		mtimensec: nowNsec,
		ctimensec: nowNsec,
	}
}

func cacheFSFileMetadataFromInfo(fileInfo os.FileInfo, hash string, size uint64) *cacheFSFileMetadata {
	if fileInfo.IsDir() {
		return nil
	}

	modTime := fileInfo.ModTime()
	accessTime := atime.Get(fileInfo)
	mode := fuse.S_IFREG | uint32(fileInfo.Mode().Perm())

	return &cacheFSFileMetadata{
		hash:      hash,
		size:      size,
		mode:      mode,
		atime:     uint64(accessTime.Unix()),
		mtime:     uint64(modTime.Unix()),
		ctime:     uint64(modTime.Unix()),
		atimensec: uint32(accessTime.Nanosecond()),
		mtimensec: uint32(modTime.Nanosecond()),
		ctimensec: uint32(modTime.Nanosecond()),
	}
}

func cacheFSFileMetadataFromProto(metadata *proto.CacheFSMetadata, hash string, size uint64) *cacheFSFileMetadata {
	if metadata == nil {
		return newCacheFSFileMetadata(hash, size)
	}

	mode := metadata.Mode
	if mode == 0 {
		mode = fuse.S_IFREG | 0644
	}

	return &cacheFSFileMetadata{
		hash:      hash,
		size:      size,
		mode:      mode,
		atime:     metadata.Atime,
		mtime:     metadata.Mtime,
		ctime:     metadata.Ctime,
		atimensec: metadata.Atimensec,
		mtimensec: metadata.Mtimensec,
		ctimensec: metadata.Ctimensec,
	}
}

func (cs *Server) storeContentInCacheFS(ctx context.Context, path string, hash string, size uint64, leafMetadata *cacheFSFileMetadata) error {
	path = filepath.Join("/", filepath.Clean(path))
	parts := strings.Split(path, string(filepath.Separator))

	rootParentId := GenerateFsID("/")

	// Iterate over the components and construct the path hierarchy
	currentPath := "/"
	previousParentId := rootParentId // start with the root ID
	for i, part := range parts {
		if i == 0 && part == "" {
			continue // Skip the empty part for root
		}

		if currentPath == "/" {
			currentPath = filepath.Join("/", part)
		} else {
			currentPath = filepath.Join(currentPath, part)
		}

		currentNodeId := GenerateFsID(currentPath)
		inode, err := SHA1StringToUint64(currentNodeId)
		if err != nil {
			return err
		}

		// Initialize default metadata
		now := time.Now()
		nowSec := uint64(now.Unix())
		nowNsec := uint32(now.Nanosecond())
		metadata := &FSMetadata{
			PID:       previousParentId,
			ID:        currentNodeId,
			Name:      part,
			Path:      currentPath,
			Ino:       inode,
			Mode:      fuse.S_IFDIR | 0755,
			Atime:     nowSec,
			Mtime:     nowSec,
			Ctime:     nowSec,
			Atimensec: nowNsec,
			Mtimensec: nowNsec,
			Ctimensec: nowNsec,
		}

		// If currentPath matches the input path, use the actual file info
		if currentPath == path {
			if leafMetadata == nil {
				metadata.Hash = GenerateFsID(currentPath)
				metadata.Size = 0
			} else {
				metadata.Hash = leafMetadata.hash
				metadata.Size = leafMetadata.size
				metadata.Mode = leafMetadata.mode
				metadata.Atime = leafMetadata.atime
				metadata.Mtime = leafMetadata.mtime
				metadata.Ctime = leafMetadata.ctime
				metadata.Atimensec = leafMetadata.atimensec
				metadata.Mtimensec = leafMetadata.mtimensec
				metadata.Ctimensec = leafMetadata.ctimensec
			}
		}

		// Set metadata
		err = cs.metadataStore.SetFsNode(ctx, currentNodeId, metadata)
		if err != nil {
			return err
		}

		// Add the current node as a child of the previous node
		err = cs.metadataStore.AddFsNodeChild(ctx, previousParentId, currentNodeId)
		if err != nil {
			return err
		}

		previousParentId = currentNodeId
	}

	return nil
}

func (cs *Server) StoreContent(stream proto.Cache_StoreContentServer) error {
	if err := cs.rejectIfDraining(); err != nil {
		return err
	}
	ctx := stream.Context()

	Logger.Debugf("StoreContent[ACK]")

	reader := &storeContentStreamReader{stream: stream}
	hash, size, err := cs.storeReader(ctx, reader)
	if err != nil {
		return err
	}

	if cs.metadataStore != nil && reader.cachePath != "" {
		metadata := newCacheFSFileMetadata(hash, size)
		if reader.metadata != nil {
			metadata = cacheFSFileMetadataFromProto(reader.metadata, hash, size)
		}

		if err := cs.storeContentInCacheFS(ctx, reader.cachePath, hash, size, metadata); err != nil {
			Logger.Warnf("Store[ERR] - [%s] unable to store content in cachefs<path=%s> - %v", hash, reader.cachePath, err)
			return status.Errorf(codes.Internal, "Failed to update cachefs metadata: %v", err)
		}
	}

	Logger.Debugf("StoreContent[OK] - [%s]", hash)
	return stream.SendAndClose(&proto.CacheStoreContentResponse{Ok: true, Hash: hash})
}

type storeContentStreamReader struct {
	stream    proto.Cache_StoreContentServer
	pending   []byte
	cachePath string
	metadata  *proto.CacheFSMetadata
}

func (r *storeContentStreamReader) Read(p []byte) (int, error) {
	for len(r.pending) == 0 {
		req, err := r.stream.Recv()
		if err == io.EOF {
			return 0, io.EOF
		}
		if err != nil {
			Logger.Warnf("Store[ERR] - error: %v", err)
			return 0, status.Errorf(codes.Unknown, "Received an error: %v", err)
		}

		if req.CachePath != "" {
			r.cachePath = filepath.Join("/", filepath.Clean(req.CachePath))
		}
		if req.Metadata != nil {
			r.metadata = req.Metadata
			if r.cachePath == "" && req.Metadata.Path != "" {
				r.cachePath = filepath.Join("/", filepath.Clean(req.Metadata.Path))
			}
		}

		Logger.Debugf("Store[RX] - chunk (%d bytes)", len(req.Content))
		r.pending = req.Content
	}

	n := copy(p, r.pending)
	r.pending = r.pending[n:]
	return n, nil
}

func (cs *Server) usagePct() float64 {
	if cs.cas.maxCacheSizeMb <= 0 {
		_, _, diskUsagePct, err := cs.cas.GetDiskCacheMetrics()
		if err == nil {
			return diskUsagePct
		}

		return 0
	}

	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	memoryUsage := float64(memStats.Alloc) / (1024 * 1024)
	return memoryUsage / float64(cs.cas.maxCacheSizeMb)
}

func (cs *Server) GetState(ctx context.Context, req *proto.CacheGetStateRequest) (*proto.CacheGetStateResponse, error) {
	if err := cs.rejectIfDraining(); err != nil {
		return nil, err
	}
	return &proto.CacheGetStateResponse{
		Version:          Version,
		PrivateIpAddr:    cs.privateIpAddr,
		CapacityUsagePct: float32(cs.usagePct()),
	}, nil
}

func (cs *Server) openLocalSource(localPath string) (io.ReadCloser, error) {
	// Check if the file exists
	if _, err := os.Stat(localPath); os.IsNotExist(err) {
		Logger.Warnf("StoreFromContent[ERR] - source not found: %v", err)
		return nil, err
	}

	// Open the file
	file, err := os.Open(localPath)
	if err != nil {
		Logger.Warnf("StoreFromContent[ERR] - error reading source: %v", err)
		return nil, err
	}

	return file, nil
}

func (cs *Server) s3ClientForSource(source *proto.CacheSource) (*S3Client, error) {
	key := fmt.Sprintf("%s/%s/%s/%s/%t", source.EndpointUrl, source.Region, source.BucketName, source.AccessKey, source.ForcePathStyle)

	var s3Client *S3Client
	var err error

	if cachedS3Client, ok := cs.s3ClientCache.Load(key); ok {
		s3Client = cachedS3Client.(*S3Client)
	} else {
		s3Client, err = NewS3Client(cs.ctx, S3SourceConfig{
			BucketName:     source.BucketName,
			Region:         source.Region,
			EndpointURL:    source.EndpointUrl,
			AccessKey:      source.AccessKey,
			SecretKey:      source.SecretKey,
			ForcePathStyle: source.ForcePathStyle,
		}, cs.serverConfig)
		if err != nil {
			return nil, err
		}

		cs.s3ClientCache.Store(key, s3Client)
	}

	return s3Client, nil
}

func (cs *Server) openOCIOriginSource(ctx context.Context, sourcePath string) (io.ReadCloser, error) {
	source, ok := ParseOCIRequiredContentOriginPath(sourcePath)
	if !ok {
		return nil, fmt.Errorf("invalid OCI origin path: %s", sourcePath)
	}

	ref, err := name.NewDigest(fmt.Sprintf("%s/%s@%s", source.Registry, source.Repository, source.LayerDigest))
	if err != nil {
		return nil, fmt.Errorf("failed to parse OCI layer digest: %w", err)
	}
	layer, err := remote.Layer(ref, remote.WithContext(ctx), remote.WithAuthFromKeychain(authn.DefaultKeychain))
	if err != nil {
		return nil, fmt.Errorf("failed to fetch OCI layer %s: %w", source.LayerDigest, err)
	}
	reader, err := layer.Uncompressed()
	if err != nil {
		return nil, fmt.Errorf("failed to open uncompressed OCI layer %s: %w", source.LayerDigest, err)
	}
	return reader, nil
}

func (cs *Server) StoreContentFromSource(ctx context.Context, req *proto.CacheStoreContentFromSourceRequest) (*proto.CacheStoreContentFromSourceResponse, error) {
	if err := cs.rejectIfDraining(); err != nil {
		return nil, err
	}
	return cs.storeContentFromSource(ctx, req)
}

func (cs *Server) storeContentFromSource(ctx context.Context, req *proto.CacheStoreContentFromSourceRequest) (*proto.CacheStoreContentFromSourceResponse, error) {
	if req == nil || req.Source == nil {
		return &proto.CacheStoreContentFromSourceResponse{Ok: false, ErrorMsg: "source is required"}, nil
	}

	localPath := filepath.Join("/", req.Source.Path)
	cachePath := localPath
	if req.Source.CachePath != "" {
		cachePath = filepath.Join("/", filepath.Clean(req.Source.CachePath))
	}
	Logger.Debugf("StoreFromContent[ACK] - [source=%s cache_path=%s bucket=%s expected_hash=%s]", req.Source.Path, cachePath, req.Source.BucketName, req.Source.ExpectedHash)
	if req.Source.CachePath == "" && isContentHash(req.Source.ExpectedHash) && cs.cas.Exists(req.Source.ExpectedHash) {
		Logger.Debugf("StoreFromContent[EXISTS] - [%s]", req.Source.ExpectedHash)
		return &proto.CacheStoreContentFromSourceResponse{Ok: true, Hash: req.Source.ExpectedHash}, nil
	}

	var (
		hash string
		size uint64
		err  error
	)
	if _, ok := ParseOCIRequiredContentOriginPath(req.Source.Path); ok {
		var reader io.ReadCloser
		reader, err = cs.openOCIOriginSource(ctx, req.Source.Path)
		if err != nil {
			return &proto.CacheStoreContentFromSourceResponse{Ok: false, ErrorMsg: err.Error()}, err
		}
		defer reader.Close()
		hash, size, err = cs.storeReaderWithExpectedHash(ctx, reader, req.Source.ExpectedHash)
	} else if req.Source.BucketName == "" {
		var reader io.ReadCloser
		reader, err = cs.openLocalSource(localPath)
		if err != nil {
			return &proto.CacheStoreContentFromSourceResponse{Ok: false, ErrorMsg: err.Error()}, err
		}
		defer reader.Close()
		hash, size, err = cs.storeReaderWithExpectedHash(ctx, reader, req.Source.ExpectedHash)
	} else {
		var s3Client *S3Client
		s3Client, err = cs.s3ClientForSource(req.Source)
		if err != nil {
			Logger.Errorf("StoreFromContent[ERR] - error caching source: %v", err)
			return &proto.CacheStoreContentFromSourceResponse{Ok: false, ErrorMsg: err.Error()}, err
		}

		if req.Source.ExpectedHash != "" {
			hash, size, err = cs.storeS3SourceWithExpectedHash(ctx, s3Client, req.Source.Path, req.Source.ExpectedHash)
		} else {
			var reader io.ReadCloser
			reader, err = s3Client.Open(ctx, req.Source.Path)
			if err != nil {
				Logger.Errorf("StoreFromContent[ERR] - error opening source: %v", err)
				return &proto.CacheStoreContentFromSourceResponse{Ok: false, ErrorMsg: err.Error()}, err
			}
			defer reader.Close()
			hash, size, err = cs.storeReaderWithExpectedHash(ctx, reader, req.Source.ExpectedHash)
		}
	}

	if err != nil {
		Logger.Warnf("StoreFromContent[ERR] - error storing data in cache: %v", err)
		return &proto.CacheStoreContentFromSourceResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	Logger.Debugf("StoreFromContent[STORE] - [source=%s cache_path=%s hash=%s size=%d expected_hash=%s]", req.Source.Path, cachePath, hash, size, req.Source.ExpectedHash)
	if hash == "" {
		return &proto.CacheStoreContentFromSourceResponse{Ok: false, ErrorMsg: "stored content hash is empty"}, nil
	}
	if isContentHash(req.Source.ExpectedHash) && hash != req.Source.ExpectedHash {
		err := fmt.Errorf("stored content hash mismatch: expected %s, got %s", req.Source.ExpectedHash, hash)
		Logger.Warnf("StoreFromContent[ERR] - %v", err)
		return &proto.CacheStoreContentFromSourceResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	// Store references in cachefs only when the caller is publishing a path into the
	// cachefs namespace. Plain S3 source writes are content-addressed only.
	if cs.metadataStore != nil {
		if err := cs.storeSourceReferenceInCacheFS(ctx, req.Source, localPath, cachePath, hash, size); err != nil {
			Logger.Warnf("Store[ERR] - [%s] unable to store content in cachefs<path=%s> - %v", hash, cachePath, err)
			return &proto.CacheStoreContentFromSourceResponse{Ok: false, ErrorMsg: err.Error()}, nil
		}
	}

	Logger.Debugf("StoreFromContent[OK] - [%s]", hash)

	// HOTFIX: Manually trigger garbage collection
	go runtime.GC()

	return &proto.CacheStoreContentFromSourceResponse{Ok: true, Hash: hash}, nil
}

func (cs *Server) storeSourceReferenceInCacheFS(ctx context.Context, source *proto.CacheSource, localPath, cachePath, hash string, size uint64) error {
	if source == nil {
		return nil
	}
	if _, ok := ParseOCIRequiredContentOriginPath(source.Path); ok {
		return nil
	}
	if source.BucketName != "" {
		if source.CachePath == "" {
			return nil
		}
		return cs.StoreSyntheticContentInCacheFS(ctx, cachePath, hash, size)
	}
	if source.CachePath == "" {
		return cs.StoreContentInCacheFS(ctx, localPath, hash, size)
	}
	fileInfo, err := os.Stat(localPath)
	if err != nil {
		return err
	}
	return cs.storeContentInCacheFS(ctx, cachePath, hash, size, cacheFSFileMetadataFromInfo(fileInfo, hash, size))
}

func (cs *Server) storeS3SourceWithExpectedHash(ctx context.Context, s3Client *S3Client, path string, expectedHash string) (string, uint64, error) {
	ok, head, err := s3Client.Head(ctx, path)
	if err != nil {
		return "", 0, err
	}
	if !ok || head == nil || head.ContentLength == nil {
		return "", 0, fmt.Errorf("unable to resolve source size for %s", path)
	}
	size := *head.ContentLength
	if size < 0 {
		return "", 0, fmt.Errorf("invalid source size for %s: %d", path, size)
	}

	concurrency := int(s3Client.DownloadConcurrency)
	hash, storedSize, err := cs.cas.AddPageSourceWithExpectedHash(ctx, expectedHash, size, concurrency, func(ctx context.Context, _ int64, start int64, length int64) ([]byte, error) {
		return s3Client.ReadRange(ctx, path, start, length)
	})
	return hash, uint64(storedSize), err
}

func (cs *Server) StoreContentFromSourceWithLock(ctx context.Context, req *proto.CacheStoreContentFromSourceRequest) (*proto.CacheStoreContentFromSourceWithLockResponse, error) {
	if err := cs.rejectIfDraining(); err != nil {
		return nil, err
	}
	if req == nil || req.Source == nil {
		return &proto.CacheStoreContentFromSourceWithLockResponse{Ok: false, ErrorMsg: "source is required"}, nil
	}

	started := time.Now()
	sourcePath := storeContentSourceLockKey(req.Source)
	logSourcePath := req.Source.Path
	if req.Source.CachePath != "" {
		logSourcePath = filepath.Join("/", filepath.Clean(req.Source.CachePath))
	}
	Logger.Debugf("StoreContentFromSourceWithLock[ACK] - [source=%s bucket=%s cache_path=%s]", req.Source.Path, req.Source.BucketName, req.Source.CachePath)
	if err := cs.metadataStore.SetStoreFromContentLock(ctx, cs.locality, sourcePath); err != nil {
		Logger.Debugf("StoreContentFromSourceWithLock[LOCK_MISS] - [source=%s lock=%s elapsed=%s err=%v]", logSourcePath, sourcePath, time.Since(started).Truncate(time.Millisecond), err)
		return &proto.CacheStoreContentFromSourceWithLockResponse{Ok: false, FailedToAcquireLock: true, ErrorMsg: err.Error()}, nil
	}
	lockReleased := false
	defer func() {
		if lockReleased {
			return
		}
		_ = removeStoreFromContentLock(ctx, cs.metadataStore, cs.locality, sourcePath, "StoreContentFromSourceWithLock")
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
				Logger.Debugf("StoreContentFromSourceWithLock[REFRESH] - [%s]", sourcePath)
				refreshStoreFromContentLock(storeContext, cs.metadataStore, cs.locality, sourcePath, "StoreContentFromSourceWithLock")
			}
		}
	}()

	storeContentFromSourceResp, err := cs.storeContentFromSource(storeContext, req)
	if err != nil {
		Logger.Warnf("StoreContentFromSourceWithLock[ERR] - [source=%s elapsed=%s err=%v]", sourcePath, time.Since(started).Truncate(time.Millisecond), err)
		return &proto.CacheStoreContentFromSourceWithLockResponse{Hash: "", Ok: false, ErrorMsg: err.Error()}, nil
	}
	if storeContentFromSourceResp == nil || !storeContentFromSourceResp.Ok {
		errorMsg := ""
		if storeContentFromSourceResp != nil {
			errorMsg = storeContentFromSourceResp.ErrorMsg
		}
		Logger.Warnf("StoreContentFromSourceWithLock[ERR] - [source=%s lock=%s elapsed=%s err=%s]", logSourcePath, sourcePath, time.Since(started).Truncate(time.Millisecond), errorMsg)
		return &proto.CacheStoreContentFromSourceWithLockResponse{Hash: "", Ok: false, ErrorMsg: errorMsg}, nil
	}

	_ = removeStoreFromContentLock(ctx, cs.metadataStore, cs.locality, sourcePath, "StoreContentFromSourceWithLock")
	lockReleased = true

	Logger.Debugf("StoreContentFromSourceWithLock[OK] - [source=%s hash=%s elapsed=%s]", sourcePath, storeContentFromSourceResp.Hash, time.Since(started).Truncate(time.Millisecond))
	return &proto.CacheStoreContentFromSourceWithLockResponse{Hash: storeContentFromSourceResp.Hash, Ok: true}, nil
}

func storeContentSourceLockKey(source *proto.CacheSource) string {
	if source == nil {
		return ""
	}
	if isContentHash(source.ExpectedHash) {
		return storeContentHashLockKey(source.ExpectedHash)
	}
	if source.CachePath != "" {
		return filepath.Join("/", filepath.Clean(source.CachePath))
	}
	return source.Path
}
