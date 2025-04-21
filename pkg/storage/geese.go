package storage

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog/log"
	core "github.com/yandex-cloud/geesefs/core"
	cfg "github.com/yandex-cloud/geesefs/core/cfg"
)

type GeeseStorage struct {
	config types.GeeseConfig
	mfs    core.MountedFS
	fs     *core.Goofys
	mu     sync.Mutex
}

func NewGeeseStorage(config types.GeeseConfig) (Storage, error) {
	return &GeeseStorage{
		config: config,
		mfs:    nil,
		fs:     nil,
	}, nil
}

func (s *GeeseStorage) Mount(localPath string) error {
	log.Info().Str("local_path", localPath).Msg("geese filesystem mounting")

	flags := cfg.DefaultFlags()
	s3Config := &cfg.S3Config{}
	s3Config.Init()

	s3Config.AccessKey = s.config.AccessKey
	s3Config.SecretKey = s.config.SecretKey
	s3Config.Region = s.config.Region

	flags.Backend = s3Config
	flags.Endpoint = s.config.EndpointUrl
	flags.MountPoint = localPath
	flags.Foreground = false
	flags.DirMode = os.FileMode(0755)
	flags.FileMode = os.FileMode(0644)
	flags.MaxFlushers = int64(s.config.MaxFlushers)
	flags.MaxParallelParts = int(s.config.MaxParallelParts)
	flags.PartSizes = []cfg.PartSizeConfig{
		{
			PartSize:  1024 * 1024,
			PartCount: 10,
		},
	}
	flags.FsyncOnClose = s.config.FsyncOnClose
	flags.MemoryLimit = uint64(s.config.MemoryLimit)
	flags.SymlinkZeroed = true

	fs, mfs, err := core.MountFuse(context.Background(), s.config.BucketName, flags)
	if err != nil {
		log.Error().Err(err).Str("local_path", localPath).Msg("geesefs: mount process exited with error")
	}

	s.mu.Lock()
	s.mfs = mfs
	s.fs = fs
	s.mu.Unlock()

	// Poll until the filesystem is mounted or we timeout
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	timeout := time.After(30 * time.Second)

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
