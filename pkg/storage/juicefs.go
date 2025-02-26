package storage

import (
	"fmt"
	"os/exec"
	"strconv"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/cenkalti/backoff"
	"github.com/rs/zerolog/log"
)

const juiceFsMountTimeout time.Duration = 30 * time.Second

type JuiceFsStorage struct {
	mountCmd *exec.Cmd
	config   types.JuiceFSConfig
}

func NewJuiceFsStorage(config types.JuiceFSConfig) (Storage, error) {
	return &JuiceFsStorage{
		config: config,
	}, nil
}

func (s *JuiceFsStorage) Mount(localPath string) error {
	log.Info().Str("local_path", localPath).Msg("juicefs filesystem mounting")

	cacheSize := strconv.FormatInt(s.config.CacheSize, 10)

	prefetch := strconv.FormatInt(s.config.Prefetch, 10)
	if s.config.Prefetch <= 0 {
		prefetch = "1"
	}

	bufferSize := strconv.FormatInt(s.config.BufferSize, 10)
	if s.config.BufferSize <= 0 {
		bufferSize = "300"
	}

	s.mountCmd = exec.Command(
		"juicefs",
		"mount",
		s.config.RedisURI,
		localPath,
		"--bucket", s.config.AWSS3Bucket,
		"--cache-size", cacheSize,
		"--prefetch", prefetch,
		"--buffer-size", bufferSize,
		"--no-usage-report",
	)

	// Start the mount command
	if err := s.mountCmd.Start(); err != nil {
		return fmt.Errorf("error starting juicefs mount: %v", err)
	}

	// Wait for the mount command to finish in the background
	go func() {
		if out, err := s.mountCmd.CombinedOutput(); err != nil {
			log.Error().Err(err).Str("output", string(out)).Msg("error with juicefs mount command")
		}
	}()

	ticker := time.NewTicker(100 * time.Millisecond)
	timeout := time.After(juiceFsMountTimeout)

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
		return fmt.Errorf("failed to mount JuiceFS filesystem to: '%s'", localPath)
	}

	log.Info().Str("local_path", localPath).Msg("juicefs filesystem mounted")
	return nil
}

func (s *JuiceFsStorage) Format(fsName string) error {
	blockSize := strconv.FormatInt(s.config.BlockSize, 10)
	if s.config.BlockSize <= 0 {
		blockSize = "4096"
	}

	cmd := exec.Command(
		"juicefs",
		"format",
		"--storage", "s3",
		"--bucket", s.config.AWSS3Bucket,
		"--block-size", blockSize,
		s.config.RedisURI,
		fsName,
		"--no-update",
	)

	if s.config.AWSAccessKey != "" || s.config.AWSSecretKey != "" {
		cmd.Args = append(
			cmd.Args,
			"--access-key", s.config.AWSAccessKey,
			"--secret-key", s.config.AWSSecretKey,
		)
	}

	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("error executing juicefs format: %v, output: %s", err, string(output))
	}

	return nil
}

func (s *JuiceFsStorage) Unmount(localPath string) error {
	juiceFsUmount := func() error {
		cmd := exec.Command("juicefs", "umount", localPath)

		output, err := cmd.CombinedOutput()
		if err != nil {
			log.Error().Err(err).Str("output", string(output)).Msg("error executing juicefs umount")
			return err
		}

		log.Info().Str("local_path", localPath).Msg("juicefs filesystem unmounted")
		return nil
	}

	err := backoff.Retry(juiceFsUmount, backoff.WithMaxRetries(backoff.NewConstantBackOff(1*time.Second), 10))
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
