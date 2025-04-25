package storage

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	blobcache "github.com/beam-cloud/blobcache-v2/pkg"
	"github.com/rs/zerolog/log"
	core "github.com/yandex-cloud/geesefs/core"
	cfg "github.com/yandex-cloud/geesefs/core/cfg"
)

const (
	defaultGeeseFSDirMode         = 0755
	defaultGeeseFSFileMode        = 0644
	defaultGeeseFSMountTimeout    = 30 * time.Second
	defaultGeeseFSRequestTimeout  = 60 * time.Second
	defaultGeeseFSFuseReadAheadKb = 32768
)

type GeeseStorage struct {
	config      types.GeeseConfig
	mfs         core.MountedFS
	fs          *core.Goofys
	mu          sync.Mutex
	cacheClient *blobcache.BlobCacheClient
}

func NewGeeseStorage(config types.GeeseConfig, cacheClient *blobcache.BlobCacheClient) (Storage, error) {
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
	flags.FuseReadAheadKB = defaultGeeseFSFuseReadAheadKb
	flags.NoPreloadDir = true

	// If we have a cache client available, use it
	if s.cacheClient != nil {
		flags.ExternalCacheClient = s.cacheClient
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

func (s *GeeseStorage) Unmount(localPath string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

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
