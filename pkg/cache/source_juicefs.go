package cache

import (
	"context"
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

	mountCtx, cancelMount := context.WithCancel(context.Background())

	s.mountCmd = exec.CommandContext(
		mountCtx,
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

	type mountResult struct {
		output []byte
		err    error
	}
	resultCh := make(chan mountResult, 1)
	go func() {
		output, err := s.mountCmd.CombinedOutput()
		resultCh <- mountResult{output: output, err: err}
	}()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	timeout := time.NewTimer(juiceFsMountTimeout)
	defer timeout.Stop()

	for {
		select {
		case result := <-resultCh:
			if result.err != nil {
				return fmt.Errorf("error executing juicefs mount: %w, output: %s", result.err, string(result.output))
			}
			if isMounted(localPath) {
				Logger.Infof("JuiceFS filesystem mounted to: '%s'", localPath)
				return nil
			}
			return fmt.Errorf("juicefs mount exited before filesystem was mounted to: '%s'", localPath)
		case <-ticker.C:
			if isMounted(localPath) {
				Logger.Infof("JuiceFS filesystem mounted to: '%s'", localPath)
				return nil
			}
		case <-timeout.C:
			cancelMount()
			return fmt.Errorf("failed to mount JuiceFS filesystem to: '%s': timed out after %s", localPath, juiceFsMountTimeout)
		}
	}
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
