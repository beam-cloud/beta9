package storage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
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
	defaultGeeseFSFlushTimeout     = 60 * time.Second
	defaultGeeseFSUnmountTimeout   = 10 * time.Second
	defaultGeeseFSFuseReadAheadKb  = 32768
	defaultGeeseFSReadAheadKb      = 32768
	defaultGeeseFSReadAheadLargeKb = 131072
	defaultGeeseFSMaxReadBytes     = 1048576
	defaultGeeseFSPartSizeBytes    = 64 * 1024 * 1024
	defaultGeeseFSHashMinFileKb    = 1024
	defaultGeeseFSMinMemoryLimitMB = 128
)

// VolumeContentReporter receives workspace object content above the configured
// size threshold so the cache reconciliation loop can keep it warm. It is
// implemented by the worker's required-content reporter.
type VolumeContentReporter interface {
	ReportVolumeContent(workspaceID, hash, sourcePath string, sizeBytes int64)
}

// VolumeContentReporterAware is implemented by storage backends that can forward
// cached object content to a VolumeContentReporter.
type VolumeContentReporterAware interface {
	SetVolumeContentReporter(workspaceID string, reporter VolumeContentReporter)
}

type GeeseStorage struct {
	config         types.GeeseConfig
	mfs            core.MountedFS
	fs             *core.Goofys
	mu             sync.Mutex
	cacheClient    *cache.Client
	workspaceID    string
	volumeReporter VolumeContentReporter
}

func (s *GeeseStorage) SetVolumeContentReporter(workspaceID string, reporter VolumeContentReporter) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.workspaceID = workspaceID
	s.volumeReporter = reporter
}

func (s *GeeseStorage) reportVolumeContent(hash, sourcePath string, sizeBytes int64) {
	s.mu.Lock()
	reporter := s.volumeReporter
	workspaceID := s.workspaceID
	s.mu.Unlock()
	if reporter == nil || workspaceID == "" || hash == "" {
		return
	}
	reporter.ReportVolumeContent(workspaceID, hash, sourcePath, sizeBytes)
}

// handleGeeseContentEvent forwards cached object content surfaced by the geesefs
// fork's event callback to the volume content reporter. It is defensive about
// the event payload shape so it degrades to a no-op when the fork does not (yet)
// surface content hashes/sizes.
func (s *GeeseStorage) handleGeeseContentEvent(data map[string]interface{}) {
	if len(data) == 0 {
		return
	}

	// reportVolumeContent reads the reporter under the lock; avoid an
	// unsynchronized read of s.volumeReporter here.
	hash := firstStringValue(data, "content_hash", "hash")
	if hash == "" {
		return
	}
	path := firstStringValue(data, "path", "key", "object", "inode")
	size := firstInt64Value(data, "size_bytes", "size", "content_length")
	s.reportVolumeContent(hash, path, size)
}

func firstStringValue(data map[string]interface{}, keys ...string) string {
	for _, key := range keys {
		if value, ok := data[key]; ok {
			if str, ok := value.(string); ok && str != "" {
				return str
			}
		}
	}
	return ""
}

func firstInt64Value(data map[string]interface{}, keys ...string) int64 {
	for _, key := range keys {
		value, ok := data[key]
		if !ok {
			continue
		}
		switch v := value.(type) {
		case int64:
			return v
		case int:
			return int64(v)
		case uint64:
			return int64(v)
		case float64:
			return int64(v)
		}
	}
	return 0
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
	return c.client.StoreContent(chunks, hash, struct{ RoutingKey string }{RoutingKey: opts.RoutingKey})
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
	return c.client.StoreContentFromLocalFile(cache.LocalContentSource{
		Path:      source.Path,
		CachePath: source.CachePath,
	}, cache.StoreContentOptions{
		RoutingKey: opts.RoutingKey,
		Lock:       opts.Lock,
	})
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
	effectiveMemoryLimitMB := effectiveGeeseMemoryLimitMB(s.config.MemoryLimit, os.Getenv("MEMORY_LIMIT"))
	if effectiveMemoryLimitMB > 0 {
		flags.MemoryLimit = uint64(effectiveMemoryLimitMB) * 1024 * 1024
	}
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

	flags.StagedWriteModeEnabled = s.config.StagedWriteModeEnabled
	if s.cacheClient != nil && s.config.CacheThroughEnabled && !s.config.DisableVolumeCaching {
		// Cache-through needs a stable local source for large files. The buffered
		// multipart path can publish object metadata before the async hash/cache
		// pipeline has finished, which makes hot reads race or miss after churn.
		flags.StagedWriteModeEnabled = true
	}
	flags.StagedWritePath = s.config.StagedWritePath
	flags.StagedWriteDebounce = s.config.StagedWriteDebounce
	flags.EventCallback = func(event cfg.EventType, data map[string]interface{}) {
		log.Debug().Str("local_path", localPath).Str("geesefs_event", string(event)).Interface("data", data).Msg("geesefs: event callback fired")
		s.handleGeeseContentEvent(data)
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
		Int64("configured_memory_limit_mb", s.config.MemoryLimit).
		Int64("effective_memory_limit_mb", effectiveMemoryLimitMB).
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

	mountCtx, cancel := context.WithTimeout(context.Background(), defaultGeeseFSMountTimeout)
	defer cancel()

	fs, mfs, err := core.MountFuse(mountCtx, s.config.BucketName, flags)
	if err != nil {
		log.Error().Err(err).Str("local_path", localPath).Msg("geesefs: mount process exited with error")
		return err
	}

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		if isMounted(localPath) {
			break
		}

		select {
		case <-mountCtx.Done():
			return fmt.Errorf("failed to mount GeeseFS filesystem to: '%s': %w", localPath, mountCtx.Err())
		case <-ticker.C:
		}
	}

	log.Info().Str("local_path", localPath).Msg("geesefs: filesystem mounted")

	s.mu.Lock()
	s.mfs = mfs
	s.fs = fs
	s.mu.Unlock()

	return nil
}

func effectiveGeeseMemoryLimitMB(configuredLimitMB int64, workerLimitText string) int64 {
	workerLimitMB := parseWorkerMemoryLimitMB(workerLimitText)
	if workerLimitMB <= 0 {
		return configuredLimitMB
	}

	maxGeeseMemoryMB := workerLimitMB / 2
	if maxGeeseMemoryMB < defaultGeeseFSMinMemoryLimitMB {
		maxGeeseMemoryMB = defaultGeeseFSMinMemoryLimitMB
		if maxGeeseMemoryMB > workerLimitMB {
			maxGeeseMemoryMB = workerLimitMB
		}
	}

	if configuredLimitMB <= 0 || configuredLimitMB > maxGeeseMemoryMB {
		return maxGeeseMemoryMB
	}
	return configuredLimitMB
}

func parseWorkerMemoryLimitMB(value string) int64 {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0
	}

	lower := strings.ToLower(value)
	multiplier := int64(1)
	switch {
	case strings.HasSuffix(lower, "gib"):
		multiplier = 1024
		lower = strings.TrimSuffix(lower, "gib")
	case strings.HasSuffix(lower, "gi"):
		multiplier = 1024
		lower = strings.TrimSuffix(lower, "gi")
	case strings.HasSuffix(lower, "gb"):
		multiplier = 1000
		lower = strings.TrimSuffix(lower, "gb")
	case strings.HasSuffix(lower, "mi"):
		lower = strings.TrimSuffix(lower, "mi")
	case strings.HasSuffix(lower, "mib"):
		lower = strings.TrimSuffix(lower, "mib")
	case strings.HasSuffix(lower, "mb"):
		lower = strings.TrimSuffix(lower, "mb")
	}

	limit, err := strconv.ParseInt(strings.TrimSpace(lower), 10, 64)
	if err != nil || limit <= 0 {
		return 0
	}
	return limit * multiplier
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

	var errs error

	if s.fs != nil {
		log.Info().Str("local_path", localPath).Msg("geesefs: waiting for files to flush")
		flushed := make(chan struct{})
		go func(fs *core.Goofys) {
			fs.WaitForFlush()
			close(flushed)
		}(s.fs)

		select {
		case <-flushed:
			log.Info().Str("local_path", localPath).Msg("geesefs: files flushed")
		case <-time.After(defaultGeeseFSFlushTimeout):
			errs = errors.Join(errs, fmt.Errorf("timed out waiting for geesefs files to flush after %s", defaultGeeseFSFlushTimeout))
			log.Warn().Str("local_path", localPath).Dur("timeout", defaultGeeseFSFlushTimeout).Msg("geesefs: flush wait timed out during unmount")
		}
	}

	if s.mfs != nil {
		if err := unmountGeeseFS(s.mfs, localPath); err != nil {
			errs = errors.Join(errs, err)
		}
	}

	log.Info().Str("local_path", localPath).Msg("geesefs: filesystem unmounted")

	s.mfs = nil
	s.fs = nil

	return errs
}

func unmountGeeseFS(mfs core.MountedFS, localPath string) error {
	unmounted := make(chan error, 1)
	go func() {
		unmounted <- mfs.Unmount()
	}()

	select {
	case err := <-unmounted:
		return err
	case <-time.After(defaultGeeseFSUnmountTimeout):
		log.Warn().Str("local_path", localPath).Dur("timeout", defaultGeeseFSUnmountTimeout).Msg("geesefs: normal unmount timed out, forcing lazy unmount")
		forceErr := forceUnmount(localPath)
		select {
		case err := <-unmounted:
			return errors.Join(forceErr, err)
		case <-time.After(2 * time.Second):
			return errors.Join(forceErr, fmt.Errorf("timed out waiting for geesefs unmount after force unmount"))
		}
	}
}

func forceUnmount(localPath string) error {
	var errs error
	for _, args := range [][]string{
		{"fusermount3", "-uz", localPath},
		{"fusermount", "-uz", localPath},
		{"umount", "-l", localPath},
	} {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		cmd := exec.CommandContext(ctx, args[0], args[1:]...)
		if err := cmd.Run(); err != nil {
			cancel()
			errs = errors.Join(errs, fmt.Errorf("%s: %w", strings.Join(args, " "), err))
		} else {
			cancel()
			return nil
		}
	}
	return errs
}

func (s *GeeseStorage) Format(fsName string) error {
	return nil
}
