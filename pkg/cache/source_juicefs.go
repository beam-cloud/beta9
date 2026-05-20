package cache

import (
	"fmt"
	"os/exec"
	"strconv"
	"time"
)

const juiceFsMountTimeout time.Duration = 30 * time.Second

type JuiceFsSource struct {
	mountCmd *exec.Cmd
	config   JuiceFSConfig
}

func NewJuiceFsSource(config JuiceFSConfig) (Source, error) {
	return &JuiceFsSource{
		config: config,
	}, nil
}

func (s *JuiceFsSource) Mount(localPath string) error {
	Logger.Infof("JuiceFS filesystem mounting to: '%s'", localPath)

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
		"-d",
		"--bucket", s.config.Bucket,
		"--cache-size", cacheSize,
		"--prefetch", prefetch,
		"--buffer-size", bufferSize,
		"--no-bgjob",
		"--no-usage-report",
	)

	// Start the mount command in the background
	go func() {
		output, err := s.mountCmd.CombinedOutput()
		if err != nil {
			Logger.Fatalf("error executing juicefs mount: %v, output: %s", err, string(output))
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

	Logger.Infof("JuiceFS filesystem mounted to: '%s'", localPath)
	return nil
}

func (s *JuiceFsSource) Format(fsName string) error {
	blockSize := strconv.FormatInt(s.config.BlockSize, 10)
	if s.config.BlockSize <= 0 {
		blockSize = "4096"
	}

	cmd := exec.Command(
		"juicefs",
		"format",
		"--storage", "s3",
		"--bucket", s.config.Bucket,
		"--block-size", blockSize,
		s.config.RedisURI,
		fsName,
		"--no-update",
	)

	if s.config.AccessKey != "" || s.config.SecretKey != "" {
		cmd.Args = append(
			cmd.Args,
			"--access-key", s.config.AccessKey,
			"--secret-key", s.config.SecretKey,
		)
	}

	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("error executing juicefs format: %v, output: %s", err, string(output))
	}

	return nil
}

func (s *JuiceFsSource) Unmount(localPath string) error {
	cmd := exec.Command("juicefs", "umount", "--force", localPath)

	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("error executing juicefs umount: %v, output: %s", err, string(output))
	}

	Logger.Infof("JuiceFS filesystem unmounted from: '%s'", localPath)
	return nil
}
