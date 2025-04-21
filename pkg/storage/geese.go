package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	core "github.com/yandex-cloud/geesefs/core"
	cfg "github.com/yandex-cloud/geesefs/core/cfg"

	"github.com/rs/zerolog/log"
)

type GeeseStorage struct {
	config types.GeeseConfig
}

func NewGeeseStorage(config types.GeeseConfig) (Storage, error) {
	return &GeeseStorage{
		config: config,
	}, nil
}

func (s *GeeseStorage) Mount(localPath string) error {
	log.Info().Str("local_path", localPath).Msg("geese filesystem mounting")

	args := []string{}
	if s.config.Debug {
		args = append(args, "--debug")
	}
	if s.config.Force {
		args = append(args, "-f")
	}
	if s.config.FsyncOnClose {
		args = append(args, "--fsync-on-close")
	}

	flags := &cfg.FlagStorage{
		Backend: &cfg.S3Config{
			AccessKey: s.config.AccessKey,
			SecretKey: s.config.SecretKey,
			Region:    s.config.Region,
		},
		Endpoint:   s.config.EndpointUrl,
		MountPoint: localPath,
		// MemoryLimit:      uint64(s.config.MemoryLimit),
		// MaxFlushers:      int64(s.config.MaxFlushers),
		// MaxParallelParts: int64(s.config.MaxParallelParts),
		// PartSizes:        s.config.PartSizes,
		// DirMode:          os.FileMode(s.config.DirMode),
		// FileMode:         os.FileMode(s.config.FileMode),
		// FsyncOnClose:     s.config.FsyncOnClose,
	}

	// if s.config.MemoryLimit > 0 {
	// 	args = append(args, fmt.Sprintf("--memory-limit=%d", s.config.MemoryLimit))
	// }
	// if s.config.MaxFlushers > 0 {
	// 	args = append(args, fmt.Sprintf("--max-flushers=%d", s.config.MaxFlushers))
	// }
	// if s.config.MaxParallelParts > 0 {
	// 	args = append(args, fmt.Sprintf("--max-parallel-parts=%d", s.config.MaxParallelParts))
	// }
	// if s.config.PartSizes > 0 {
	// 	args = append(args, fmt.Sprintf("--part-sizes=%d", s.config.PartSizes))
	// }
	// if s.config.DirMode != "" {
	// 	args = append(args, fmt.Sprintf("--dir-mode=%s", s.config.DirMode))
	// }
	// if s.config.FileMode != "" {
	// 	args = append(args, fmt.Sprintf("--file-mode=%s", s.config.FileMode))
	// }
	// if s.config.ListType > 0 {
	// 	args = append(args, fmt.Sprintf("--list-type=%d", s.config.ListType))
	// }
	// if s.config.EndpointUrl != "" {
	// 	args = append(args, fmt.Sprintf("--endpoint=%s", s.config.EndpointUrl))
	// }

	// Add bucket and mount path

	// Set bucket credentials as env vars
	if s.config.AccessKey != "" || s.config.SecretKey != "" {
		// flags.
		// cmd.Env = append(cmd.Env,
		// 	fmt.Sprintf("AWS_ACCESS_KEY_ID=%s", s.config.AccessKey),
		// 	fmt.Sprintf("AWS_SECRET_ACCESS_KEY=%s", s.config.SecretKey),
		// )
	}

	// // Mount asynchronously
	// go func() {
	_, mfs, err := core.MountFuse(context.Background(), s.config.BucketName, flags)
	if err != nil {
		log.Error().Err(err).Str("local_path", localPath).Msg("geesefs: mount process exited with error")
	}

	log.Info().Str("local_path", localPath).Msg("geesefs: filesystem mounted")

	mfs.Unmount()

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
	log.Info().Str("local_path", localPath).Msg("geesefs: filesystem unmounted")
	return nil
}

func (s *GeeseStorage) Format(fsName string) error {
	return nil
}
