package storage

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/beam-cloud/beta9/pkg/cache"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog/log"
	core "github.com/yandex-cloud/geesefs/core"
	cfg "github.com/yandex-cloud/geesefs/core/cfg"
)

const (
	defaultGeeseFSDirMode          = 0755
	defaultGeeseFSFileMode         = 0644
	defaultGeeseFSMountTimeout     = 30 * time.Second
	defaultGeeseFSRequestTimeout   = 60 * time.Second
	defaultGeeseFSFuseReadAheadKb  = 32768
	defaultGeeseFSReadAheadKb      = 32768
	defaultGeeseFSReadAheadLargeKb = 131072
	defaultGeeseFSMaxReadBytes     = 1048576
	defaultGeeseFSPartSizeBytes    = 64 * 1024 * 1024
	defaultGeeseFSHashMinFileKb    = 1024
)

type GeeseStorage struct {
	config      types.GeeseConfig
	mfs         core.MountedFS
	fs          *core.Goofys
	mu          sync.Mutex
	cacheClient *cache.Client
}

type geeseContentCache struct {
	client *cache.Client
}

var _ cfg.ContentCache = (*geeseContentCache)(nil)
var _ cfg.ContentCacheReadInto = (*geeseContentCache)(nil)
var _ cfg.ContentCacheStoreLocalPath = (*geeseContentCache)(nil)
var _ cfg.ContentCacheClientLocalPageFileViews = (*geeseContentCache)(nil)

func newGeeseContentCache(client *cache.Client) *geeseContentCache {
	if client == nil {
		return nil
	}
	return &geeseContentCache{client: client}
}

func (c *geeseContentCache) GetContent(hash string, offset int64, length int64, opts struct{ RoutingKey string }) ([]byte, error) {
	return c.client.GetContent(hash, offset, length, opts)
}

func (c *geeseContentCache) GetContentStream(hash string, offset int64, length int64, opts struct {
	RoutingKey string
}) (chan []byte, error) {
	return c.client.GetContentStream(hash, offset, length, opts)
}

func (c *geeseContentCache) StoreContent(chunks chan []byte, hash string, opts struct{ RoutingKey string }) (string, error) {
	return c.client.StoreContent(chunks, hash, opts)
}

func (c *geeseContentCache) StoreContentFromS3(source struct {
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
	return c.client.StoreContentFromS3(source, opts)
}

func (c *geeseContentCache) ReadContentInto(ctx context.Context, hash string, offset int64, dst []byte, opts struct{ RoutingKey string }) (int64, error) {
	return c.client.ReadContentInto(ctx, hash, offset, dst, opts)
}

func (c *geeseContentCache) StoreContentFromLocalPath(source struct {
	Path      string
	CachePath string
}, opts struct {
	RoutingKey string
	Lock       bool
}) (string, error) {
	return c.client.StoreContentFromLocalPath(source, opts)
}

func (c *geeseContentCache) ClientLocalPageFileViews(hash string, offset int64, length int64, opts struct{ RoutingKey string }) ([]cfg.ClientLocalPageFileView, error) {
	views, err := c.client.ClientLocalPageFileViews(hash, offset, length, opts)
	if err != nil {
		return nil, err
	}

	out := make([]cfg.ClientLocalPageFileView, 0, len(views))
	for _, view := range views {
		out = append(out, cfg.ClientLocalPageFileView{
			Path:   view.Path,
			Offset: view.Offset,
			Length: view.Length,
		})
	}
	return out, nil
}

func NewGeeseStorage(config types.GeeseConfig, cacheClient *cache.Client) (Storage, error) {
	return &GeeseStorage{
		config:      config,
		mfs:         nil,
		fs:          nil,
		cacheClient: cacheClient,
	}, nil
}

func (s *GeeseStorage) Mount(localPath string) error {
	log.Info().Str("local_path", localPath).Msg("geese filesystem mounting")

	flags := cfg.DefaultFlags()
	dirMode, err := strconv.ParseInt(s.config.DirMode, 8, 32)
	if err != nil {
		dirMode = defaultGeeseFSDirMode
	}

	fileMode, err := strconv.ParseInt(s.config.FileMode, 8, 32)
	if err != nil {
		fileMode = defaultGeeseFSFileMode
	}

	// Backend config
	s3Config := &cfg.S3Config{}
	s3Config.Init()
	s3Config.AccessKey = s.config.AccessKey
	s3Config.SecretKey = s.config.SecretKey
	s3Config.Region = s.config.Region

	// Set mount flags
	flags.Backend = s3Config
	flags.Endpoint = s.config.EndpointUrl
	flags.MountPoint = localPath
	flags.Foreground = false
	flags.DirMode = os.FileMode(dirMode)
	flags.FileMode = os.FileMode(fileMode)
	flags.MaxFlushers = int64(s.config.MaxFlushers)
	flags.MaxParallelParts = int(s.config.MaxParallelParts)
	flags.FsyncOnClose = s.config.FsyncOnClose
	flags.DebugMain = s.config.Debug
	flags.MemoryLimit = uint64(s.config.MemoryLimit) * 1024 * 1024
	flags.SymlinkZeroed = true
	flags.HTTPTimeout = defaultGeeseFSRequestTimeout
	if s.config.HTTPTimeout > 0 {
		flags.HTTPTimeout = s.config.HTTPTimeout
	}
	flags.NoPreloadDir = true
	if s.config.Debug {
		flags.StatsInterval = 5 * time.Second
	}
	flags.FuseReadAheadKB = defaultGeeseFSFuseReadAheadKb
	flags.MountOptions = withDefaultMountOption(s.config.MountOptions, "max_read", strconv.Itoa(defaultGeeseFSMaxReadBytes))
	flags.PartSizes = []cfg.PartSizeConfig{
		{PartSize: defaultGeeseFSPartSizeBytes, PartCount: 1000},
		{PartSize: 128 * 1024 * 1024, PartCount: 8000},
	}

	// Staged writes route large writes through local temp files before flushing.
	// Keep them disabled for workspace storage so benchmark and production reads
	// validate the normal geesefs/S3/cache path instead of temp-file side effects.
	flags.StagedWriteModeEnabled = s.config.StagedWriteModeEnabled
	flags.StagedWritePath = s.config.StagedWritePath
	flags.StagedWriteDebounce = s.config.StagedWriteDebounce
	flags.EventCallback = func(event cfg.EventType, data map[string]interface{}) {
		log.Debug().Str("local_path", localPath).Str("geesefs_event", string(event)).Interface("data", data).Msg("geesefs: event callback fired")
	}

	// Cache through mode config
	flags.CacheThroughModeEnabled = s.config.CacheThroughEnabled
	if s.cacheClient != nil && !s.config.DisableVolumeCaching {
		flags.MinFileSizeForHashKB = defaultGeeseFSHashMinFileKb
	}

	if s.config.DisableVolumeCaching {
		flags.HashAttr = ""
	}

	if s.config.ReadAheadLargeKB > 0 {
		flags.ReadAheadLargeKB = uint64(s.config.ReadAheadLargeKB)
	}

	if s.config.ReadAheadKB > 0 {
		flags.ReadAheadKB = uint64(s.config.ReadAheadKB)
	}

	if s.config.FuseReadAheadKB > 0 {
		flags.FuseReadAheadKB = uint64(s.config.FuseReadAheadKB)
	}

	if s.config.ReadAheadParallelKB > 0 {
		flags.ReadAheadParallelKB = uint64(s.config.ReadAheadParallelKB)
	}

	log.Debug().
		Str("local_path", localPath).
		Int64("memory_limit_mb", s.config.MemoryLimit).
		Int("max_flushers", int(flags.MaxFlushers)).
		Int("max_parallel_parts", flags.MaxParallelParts).
		Dur("http_timeout", flags.HTTPTimeout).
		Uint64("multipart_part_size_bytes", flags.PartSizes[0].PartSize).
		Uint64("min_file_size_for_hash_kb", flags.MinFileSizeForHashKB).
		Bool("cache_through", flags.CacheThroughModeEnabled).
		Bool("external_cache", s.cacheClient != nil).
		Bool("cache_direct_io", s.config.CacheDirectIO).
		Bool("staged_write", flags.StagedWriteModeEnabled).
		Msg("geesefs mount performance config")

	// If we have a cache client available, use it
	if s.cacheClient != nil {
		flags.ExternalCacheClient = newGeeseContentCache(s.cacheClient)
		flags.ExternalCacheStreamingEnabled = s.config.CacheStreamingEnabled
		flags.ExternalCacheDirectIO = s.config.CacheDirectIO
	}

	fs, mfs, err := core.MountFuse(context.Background(), s.config.BucketName, flags)
	if err != nil {
		log.Error().Err(err).Str("local_path", localPath).Msg("geesefs: mount process exited with error")
		return err
	}

	// Poll until the filesystem is mounted or we timeout
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	timeout := time.After(defaultGeeseFSMountTimeout)

	done := make(chan bool)
	go func() {
		for {
			select {
			case <-timeout:
				done <- false
				return
			case <-ticker.C:
				if isMounted(localPath) {
					done <- true
					return
				}
			}
		}
	}()

	// Wait for confirmation or timeout
	if !<-done {
		return fmt.Errorf("failed to mount GeeseFS filesystem to: '%s'", localPath)
	}

	log.Info().Str("local_path", localPath).Msg("geesefs: filesystem mounted")

	s.mu.Lock()
	s.mfs = mfs
	s.fs = fs
	s.mu.Unlock()

	return nil
}

func (s *GeeseStorage) Mode() string {
	return StorageModeGeese
}

func withDefaultMountOption(options []string, key, value string) []string {
	for _, option := range options {
		for _, part := range strings.Split(option, ",") {
			name := part
			if idx := strings.IndexByte(name, '='); idx >= 0 {
				name = name[:idx]
			}
			if name == key {
				return options
			}
		}
	}
	out := make([]string, 0, len(options)+1)
	out = append(out, options...)
	out = append(out, fmt.Sprintf("%s=%s", key, value))
	return out
}

func (s *GeeseStorage) Unmount(localPath string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.fs != nil {
		log.Info().Str("local_path", localPath).Msg("geesefs: waiting for files to flush")
		s.fs.WaitForFlush()
		log.Info().Str("local_path", localPath).Msg("geesefs: files flushed")
	}

	if s.mfs != nil {
		s.mfs.Unmount()
	}

	log.Info().Str("local_path", localPath).Msg("geesefs: filesystem unmounted")

	s.mfs = nil
	s.fs = nil

	return nil
}

func (s *GeeseStorage) Format(fsName string) error {
	return nil
}
