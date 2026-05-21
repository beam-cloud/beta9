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
	"time"

	proto "github.com/beam-cloud/beta9/proto"
	"github.com/djherbis/atime"
	"github.com/google/uuid"
	"github.com/hanwen/go-fuse/v2/fuse"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	writeBufferSizeBytes      int   = 128 * 1024
	getContentStreamChunkSize int64 = 4 * 1024 * 1024 // 4MB
)

type ServerOpts struct {
	HostID        string
	Registry      Registry
	AdvertiseAddr string
}

type ServerOption func(*ServerOpts)

type hostRemover interface {
	RemoveHost(ctx context.Context, locality string, host *Host) error
}

type Server struct {
	ctx    context.Context
	cancel context.CancelFunc
	mode   ServerMode
	proto.UnimplementedCacheServer
	hostId        string
	locality      string
	privateIpAddr string
	publicIpAddr  string
	cas           *Store
	serverConfig  ServerConfig
	globalConfig  GlobalConfig
	coordinator   Registry
	grpcServer    *grpc.Server
	listener      net.Listener
	s3ClientCache sync.Map
	closeOnce     sync.Once
}

func NewServer(ctx context.Context, cfg Config, locality string) (*Server, error) {
	return NewServerWithOptions(ctx, cfg, locality)
}

func NewServerWithOptions(ctx context.Context, cfg Config, locality string, options ...ServerOption) (*Server, error) {
	InitLogger(cfg.Global.DebugMode, cfg.Global.PrettyLogs)

	opts := &ServerOpts{}
	for _, opt := range options {
		opt(opts)
	}

	currentHost := &Host{
		RTT: 0,
	}

	var coordinator Registry
	effectiveServerConfig := cfg.Server
	var err error = nil
	if opts.Registry != nil {
		coordinator = opts.Registry
	} else {
		coordinator, effectiveServerConfig, err = newRegistry(ctx, cfg, locality)
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
	Logger.Infof("Server<%s> started in %s mode", hostId, cfg.Server.Mode)

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

	cas, err := NewStore(serverCtx, currentHost, locality, coordinator, cfg)
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
		mode:          cfg.Server.Mode,
		hostId:        hostId,
		locality:      locality,
		cas:           cas,
		serverConfig:  effectiveServerConfig,
		globalConfig:  cfg.Global,
		coordinator:   coordinator,
		privateIpAddr: privateIpAddr,
		publicIpAddr:  publicIpAddr,
		s3ClientCache: sync.Map{},
	}

	return cs, nil
}

func WithServerHostID(hostID string) ServerOption {
	return func(opts *ServerOpts) {
		opts.HostID = hostID
	}
}

func WithServerRegistry(registry Registry) ServerOption {
	return func(opts *ServerOpts) {
		opts.Registry = registry
	}
}

func WithServerAdvertiseAddr(addr string) ServerOption {
	return func(opts *ServerOpts) {
		opts.AdvertiseAddr = addr
	}
}

func newRegistry(ctx context.Context, cfg Config, locality string) (Registry, ServerConfig, error) {
	switch cfg.Server.Mode {
	case ServerModeCoordinator, ServerModeNode:
		registry, err := NewRedisRegistry(cfg.Global, cfg.Server)
		return registry, cfg.Server, err
	default:
		coordinator, err := NewRemoteRegistry(cfg.Global, cfg.Client.Token)
		if err != nil {
			return nil, cfg.Server, err
		}

		regionConfig, err := coordinator.GetRegionConfig(ctx, locality)
		if err != nil {
			Logger.Infof("No region-specific config found for locality %s, using current config", locality)
		} else {
			cfg.Server = regionConfig
		}

		return coordinator, cfg.Server, nil
	}
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

func (cs *Server) HostKeepAlive() {
	err := cs.coordinator.SetHostKeepAlive(cs.ctx, cs.locality, cs.cas.currentHost)
	if err != nil {
		Logger.Warnf("Failed to set host keepalive: %v", err)
	}

	err = cs.coordinator.AddHostToIndex(cs.ctx, cs.locality, cs.cas.currentHost)
	if err != nil {
		Logger.Warnf("Failed to add host to index: %v", err)
	}

	ticker := time.NewTicker(time.Duration(defaultHostKeepAliveIntervalS) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-cs.ctx.Done():
			return
		case <-ticker.C:
			cs.cas.currentHost.CapacityUsagePct = cs.usagePct()

			cs.coordinator.AddHostToIndex(cs.ctx, cs.locality, cs.cas.currentHost)
			cs.coordinator.SetHostKeepAlive(cs.ctx, cs.locality, cs.cas.currentHost)
		}
	}
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

	return []grpc.ServerOption{
		grpc.MaxRecvMsgSize(maxMessageSize),
		grpc.MaxSendMsgSize(maxMessageSize),
		grpc.InitialWindowSize(int32(initialWindowSize)),
		grpc.InitialConnWindowSize(int32(initialConnWindowSize)),
		grpc.WriteBufferSize(writeBufferSize),
		grpc.ReadBufferSize(readBufferSize),
		grpc.MaxConcurrentStreams(uint32(maxConcurrentStreams)),
		grpc.NumStreamWorkers(uint32(numStreamWorkers)),
	}
}

func (cs *Server) Serve(bindAddr string, advertiseHost string) (string, error) {
	localListener, err := net.Listen("tcp", bindAddr)
	if err != nil {
		return "", err
	}

	advertiseAddr := cs.cas.currentHost.PrivateAddr
	if tcpAddr, ok := localListener.Addr().(*net.TCPAddr); ok {
		port := fmt.Sprintf("%d", tcpAddr.Port)
		if advertiseHost != "" {
			advertiseAddr = net.JoinHostPort(normalizeAdvertiseHost(advertiseHost), port)
		} else if cs.privateIpAddr != "" {
			advertiseAddr = net.JoinHostPort(cs.privateIpAddr, port)
		} else {
			advertiseAddr = localListener.Addr().String()
		}
	}

	if advertiseAddr != "" {
		cs.cas.currentHost.Addr = advertiseAddr
		cs.cas.currentHost.PrivateAddr = advertiseAddr
	}

	s := grpc.NewServer(cs.grpcServerOptions()...)
	proto.RegisterCacheServer(s, cs)

	cs.grpcServer = s
	cs.listener = localListener

	Logger.Infof("Running %s@%s, cfg: %+v", cs.hostId, advertiseAddr, cs.serverConfig)

	go cs.HostKeepAlive()
	go func() {
		if err := s.Serve(localListener); err != nil {
			if err != grpc.ErrServerStopped {
				Logger.Warnf("cache server stopped: %v", err)
			}
		}
	}()

	return advertiseAddr, nil
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
		if cs.cancel != nil {
			cs.cancel()
		}
		if remover, ok := cs.coordinator.(hostRemover); ok && cs.cas != nil && cs.cas.currentHost != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			if err := remover.RemoveHost(ctx, cs.locality, cs.cas.currentHost); err != nil {
				Logger.Warnf("failed to unregister cache host: %v", err)
			}
		}
		if cs.grpcServer != nil {
			stopped := make(chan struct{})
			go func() {
				cs.grpcServer.GracefulStop()
				close(stopped)
			}()
			select {
			case <-stopped:
			case <-time.After(5 * time.Second):
				cs.grpcServer.Stop()
			}
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

func (cs *Server) GetContent(ctx context.Context, req *proto.CacheGetContentRequest) (*proto.CacheGetContentResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is nil")
	}
	maxMessageSize := int64(cs.globalConfig.GRPCMessageSizeBytes)
	if maxMessageSize <= 0 {
		maxMessageSize = 4 * 1024 * 1024
	}
	if req.Length < 0 || req.Length > maxMessageSize {
		return nil, status.Errorf(codes.InvalidArgument, "invalid content length: %d", req.Length)
	}

	dst := make([]byte, int(req.Length))
	n, err := cs.cas.Get(req.Hash, req.Offset, req.Length, dst)
	if err != nil {
		Logger.Debugf("Get - [%s] - %v", req.Hash, err)
		return &proto.CacheGetContentResponse{Content: nil, Ok: false}, nil
	}

	Logger.Debugf("Get[OK] - [%s] (offset=%d, length=%d)", req.Hash, req.Offset, req.Length)
	return &proto.CacheGetContentResponse{Content: dst[:n], Ok: true}, nil
}

func (cs *Server) HasContent(ctx context.Context, req *proto.CacheHasContentRequest) (*proto.CacheHasContentResponse, error) {
	exists := cs.cas.Exists(req.Hash)
	return &proto.CacheHasContentResponse{Exists: exists, Ok: true}, nil
}

func (cs *Server) GetContentStream(req *proto.CacheGetContentRequest, stream proto.Cache_GetContentStreamServer) error {
	if req == nil {
		return status.Error(codes.InvalidArgument, "request is nil")
	}
	if req.Length < 0 {
		return status.Errorf(codes.InvalidArgument, "invalid content length: %d", req.Length)
	}

	const chunkSize = getContentStreamChunkSize
	offset := req.Offset
	remainingLength := req.Length

	Logger.Infof("GetContentStream[ACK] - [%s] - offset=%d, length=%d, %d bytes", req.Hash, offset, req.Length, remainingLength)

	dst := make([]byte, chunkSize)
	for remainingLength > 0 {
		currentChunkSize := chunkSize
		if remainingLength < int64(chunkSize) {
			currentChunkSize = remainingLength
		}

		n, err := cs.cas.Get(req.Hash, offset, currentChunkSize, dst)
		if err != nil {
			Logger.Debugf("GetContentStream - [%s] - %v", req.Hash, err)
			return status.Errorf(codes.NotFound, "Content not found: %v", err)
		}
		if n == 0 {
			break
		}

		Logger.Debugf("GetContentStream[TX] - [%s] - %d bytes", req.Hash, n)
		if err := stream.Send(&proto.CacheGetContentResponse{
			Ok:      true,
			Content: dst[:n],
		}); err != nil {
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
	Logger.Infof("Store[ACK]")

	hash, size, err := cs.cas.AddReader(ctx, reader)
	if err != nil {
		Logger.Infof("Store[ERR] - [%s] - %v", hash, err)
		return "", 0, status.Errorf(codes.Internal, "Failed to add content: %v", err)
	}

	Logger.Infof("Store[OK] - [%s] (%d bytes)", hash, size)
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
		err = cs.coordinator.SetFsNode(ctx, currentNodeId, metadata)
		if err != nil {
			return err
		}

		// Add the current node as a child of the previous node
		err = cs.coordinator.AddFsNodeChild(ctx, previousParentId, currentNodeId)
		if err != nil {
			return err
		}

		previousParentId = currentNodeId
	}

	return nil
}

func (cs *Server) StoreContent(stream proto.Cache_StoreContentServer) error {
	ctx := stream.Context()

	Logger.Infof("StoreContent[ACK]")

	reader := &storeContentStreamReader{stream: stream}
	hash, size, err := cs.storeReader(ctx, reader)
	if err != nil {
		return err
	}

	if cs.coordinator != nil && reader.cachePath != "" {
		metadata := newCacheFSFileMetadata(hash, size)
		if reader.metadata != nil {
			metadata = cacheFSFileMetadataFromProto(reader.metadata, hash, size)
		}

		if err := cs.storeContentInCacheFS(ctx, reader.cachePath, hash, size, metadata); err != nil {
			Logger.Infof("Store[ERR] - [%s] unable to store content in cachefs<path=%s> - %v", hash, reader.cachePath, err)
			return status.Errorf(codes.Internal, "Failed to update cachefs metadata: %v", err)
		}
	}

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
			Logger.Infof("Store[ERR] - error: %v", err)
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
	return &proto.CacheGetStateResponse{
		Version:          Version,
		PrivateIpAddr:    cs.privateIpAddr,
		CapacityUsagePct: float32(cs.usagePct()),
	}, nil
}

func (cs *Server) openLocalSource(localPath string) (io.ReadCloser, error) {
	// Check if the file exists
	if _, err := os.Stat(localPath); os.IsNotExist(err) {
		Logger.Infof("StoreFromContent[ERR] - source not found: %v", err)
		return nil, err
	}

	// Open the file
	file, err := os.Open(localPath)
	if err != nil {
		Logger.Infof("StoreFromContent[ERR] - error reading source: %v", err)
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

func (cs *Server) StoreContentFromSource(ctx context.Context, req *proto.CacheStoreContentFromSourceRequest) (*proto.CacheStoreContentFromSourceResponse, error) {
	if req == nil || req.Source == nil {
		return &proto.CacheStoreContentFromSourceResponse{Ok: false, ErrorMsg: "source is required"}, nil
	}

	localPath := filepath.Join("/", req.Source.Path)
	cachePath := localPath
	if req.Source.CachePath != "" {
		cachePath = filepath.Join("/", filepath.Clean(req.Source.CachePath))
	}
	Logger.Infof("StoreFromContent[ACK] - [source=%s cache_path=%s]", req.Source.Path, cachePath)

	var reader io.ReadCloser
	var err error
	if req.Source.BucketName == "" {
		reader, err = cs.openLocalSource(localPath)
		if err != nil {
			return &proto.CacheStoreContentFromSourceResponse{Ok: false, ErrorMsg: err.Error()}, err
		}
	} else {
		s3Client, err := cs.s3ClientForSource(req.Source)
		if err != nil {
			Logger.Errorf("StoreFromContent[ERR] - error caching source: %v", err)
			return &proto.CacheStoreContentFromSourceResponse{Ok: false, ErrorMsg: err.Error()}, err
		}

		reader, err = s3Client.Open(ctx, req.Source.Path)
		if err != nil {
			Logger.Errorf("StoreFromContent[ERR] - error opening source: %v", err)
			return &proto.CacheStoreContentFromSourceResponse{Ok: false, ErrorMsg: err.Error()}, err
		}
	}
	defer reader.Close()

	// Store the content
	hash, size, err := cs.storeReader(ctx, reader)
	if err != nil {
		Logger.Infof("StoreFromContent[ERR] - error storing data in cache: %v", err)
		return &proto.CacheStoreContentFromSourceResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	// Store references in cachefs only when the caller is publishing a path into the
	// cachefs namespace. Plain S3 source writes are content-addressed only.
	if cs.coordinator != nil {
		var err error
		if req.Source.BucketName == "" {
			if req.Source.CachePath == "" {
				err = cs.StoreContentInCacheFS(ctx, localPath, hash, size)
			} else {
				fileInfo, statErr := os.Stat(localPath)
				if statErr != nil {
					err = statErr
				} else {
					err = cs.storeContentInCacheFS(ctx, cachePath, hash, size, cacheFSFileMetadataFromInfo(fileInfo, hash, size))
				}
			}
		} else if req.Source.CachePath != "" {
			err = cs.StoreSyntheticContentInCacheFS(ctx, cachePath, hash, size)
		}
		if err != nil {
			Logger.Infof("Store[ERR] - [%s] unable to store content in cachefs<path=%s> - %v", hash, cachePath, err)
			return &proto.CacheStoreContentFromSourceResponse{Ok: false, ErrorMsg: err.Error()}, nil
		}
	}

	Logger.Infof("StoreFromContent[OK] - [%s]", hash)

	// HOTFIX: Manually trigger garbage collection
	go runtime.GC()

	return &proto.CacheStoreContentFromSourceResponse{Ok: true, Hash: hash}, nil
}

func (cs *Server) StoreContentFromSourceWithLock(ctx context.Context, req *proto.CacheStoreContentFromSourceRequest) (*proto.CacheStoreContentFromSourceWithLockResponse, error) {
	if req == nil || req.Source == nil {
		return &proto.CacheStoreContentFromSourceWithLockResponse{Ok: false, ErrorMsg: "source is required"}, nil
	}

	sourcePath := req.Source.Path
	if req.Source.CachePath != "" {
		sourcePath = filepath.Join("/", filepath.Clean(req.Source.CachePath))
	}
	if err := cs.coordinator.SetStoreFromContentLock(ctx, cs.locality, sourcePath); err != nil {
		return &proto.CacheStoreContentFromSourceWithLockResponse{Ok: false, FailedToAcquireLock: true, ErrorMsg: err.Error()}, nil
	}
	lockReleased := false
	defer func() {
		if lockReleased {
			return
		}
		if err := cs.coordinator.RemoveStoreFromContentLock(ctx, cs.locality, sourcePath); err != nil {
			Logger.Errorf("StoreContentFromSourceWithLock[ERR] - error removing lock: %v", err)
		}
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
				Logger.Infof("StoreContentFromSourceWithLock[REFRESH] - [%s]", sourcePath)
				cs.coordinator.RefreshStoreFromContentLock(ctx, cs.locality, sourcePath)
			}
		}
	}()

	storeContentFromSourceResp, err := cs.StoreContentFromSource(storeContext, req)
	if err != nil {
		return &proto.CacheStoreContentFromSourceWithLockResponse{Hash: "", Ok: false, ErrorMsg: err.Error()}, nil
	}
	if storeContentFromSourceResp == nil || !storeContentFromSourceResp.Ok {
		errorMsg := ""
		if storeContentFromSourceResp != nil {
			errorMsg = storeContentFromSourceResp.ErrorMsg
		}
		return &proto.CacheStoreContentFromSourceWithLockResponse{Hash: "", Ok: false, ErrorMsg: errorMsg}, nil
	}

	if err := cs.coordinator.RemoveStoreFromContentLock(ctx, cs.locality, sourcePath); err != nil {
		Logger.Errorf("StoreContentFromSourceWithLock[ERR] - error removing lock: %v", err)
	}
	lockReleased = true

	return &proto.CacheStoreContentFromSourceWithLockResponse{Hash: storeContentFromSourceResp.Hash, Ok: true}, nil
}
