package storage

import (
	"fmt"
	"os/exec"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/cenkalti/backoff"
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

	if s.config.MemoryLimit > 0 {
		args = append(args, fmt.Sprintf("--memory-limit=%d", s.config.MemoryLimit))
	}
	if s.config.MaxFlushers > 0 {
		args = append(args, fmt.Sprintf("--max-flushers=%d", s.config.MaxFlushers))
	}
	if s.config.MaxParallelParts > 0 {
		args = append(args, fmt.Sprintf("--max-parallel-parts=%d", s.config.MaxParallelParts))
	}
	if s.config.PartSizes > 0 {
		args = append(args, fmt.Sprintf("--part-sizes=%d", s.config.PartSizes))
	}
	if s.config.DirMode != "" {
		args = append(args, fmt.Sprintf("--dir-mode=%d", s.config.DirMode))
	}
	if s.config.FileMode != "" {
		args = append(args, fmt.Sprintf("--file-mode=%d", s.config.FileMode))
	}
	if s.config.ListType > 0 {
		args = append(args, fmt.Sprintf("--list-type=%d", s.config.ListType))
	}
	if s.config.EndpointUrl != "" {
		args = append(args, fmt.Sprintf("--endpoint=%s", s.config.EndpointUrl))
	}

	// Add bucket and mount point
	args = append(args, s.config.BucketName, localPath)

	cmd := exec.Command("geesefs", args...)

	// Set bucket credentials as env vars
	if s.config.AccessKey != "" || s.config.SecretKey != "" {
		cmd.Env = append(cmd.Env,
			fmt.Sprintf("AWS_ACCESS_KEY_ID=%s", s.config.AccessKey),
			fmt.Sprintf("AWS_SECRET_ACCESS_KEY=%s", s.config.SecretKey),
		)
	}

	// Start the mount command in the background
	go func() {
		output, err := cmd.CombinedOutput()
		if err != nil {
			log.Error().Err(err).Str("output", string(output)).Msg("error executing geesefs mount")
		}
	}()

	ticker := time.NewTicker(100 * time.Millisecond)
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

	log.Info().Str("local_path", localPath).Msg("geesefs filesystem mounted")
	return nil
}

func (s *GeeseStorage) Format(fsName string) error {
	return nil
}

func (s *GeeseStorage) Unmount(localPath string) error {
	geeseFsUmount := func() error {
		cmd := exec.Command("geesefs", "umount", localPath)

		output, err := cmd.CombinedOutput()
		if err != nil {
			log.Error().Err(err).Str("output", string(output)).Msg("error executing geesefs umount")
			return err
		}

		log.Info().Str("local_path", localPath).Msg("geesefs filesystem unmounted")
		return nil
	}

	err := backoff.Retry(geeseFsUmount, backoff.WithMaxRetries(backoff.NewConstantBackOff(1*time.Second), 10))
	if err == nil {
		return nil
	}

	// Forcefully kill the fuse mount devices
	err = exec.Command("fuser", "-k", "/dev/fuse").Run()
	if err != nil {
		return fmt.Errorf("error executing fuser -k /dev/fuse: %v", err)
	}

	return nil
}
