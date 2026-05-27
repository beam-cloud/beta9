package cache

import (
	"errors"
	"fmt"
	"os/exec"
	"strconv"
	"time"

	managedprocess "github.com/beam-cloud/beta9/pkg/common/process"
)

const (
	juiceFsSourceMountTimeout   time.Duration = 30 * time.Second
	juiceFsSourceUnmountTimeout time.Duration = 10 * time.Second
)

type JuiceFsSource struct {
	mountCmd *managedprocess.ManagedCommand
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

	args := []string{
		"mount",
		s.config.RedisURI,
		localPath,
		"--bucket", s.config.Bucket,
		"--cache-size", cacheSize,
		"--prefetch", prefetch,
		"--buffer-size", bufferSize,
		"--no-bgjob",
		"--no-usage-report",
	}

	mountCmd, err := managedprocess.StartManagedCommand("juicefs", args, nil)
	if err != nil {
		return err
	}
	s.mountCmd = mountCmd

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	timeout := time.NewTimer(juiceFsSourceMountTimeout)
	defer timeout.Stop()

	for {
		if isMounted(localPath) {
			Logger.Infof("JuiceFS filesystem mounted to: '%s'", localPath)
			return nil
		}
		if err, done := s.mountCmd.DoneErr(); done {
			return fmt.Errorf("juicefs mount exited before filesystem was mounted to: '%s': %w, output: %s", localPath, err, s.mountCmd.OutputString())
		}

		select {
		case <-ticker.C:
		case <-timeout.C:
			output := s.mountCmd.OutputString()
			_ = s.mountCmd.Terminate(juiceFsSourceUnmountTimeout)
			s.mountCmd = nil
			return fmt.Errorf("failed to mount JuiceFS filesystem to: '%s': timed out after %s, output: %s", localPath, juiceFsSourceMountTimeout, output)
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
	var errs error
	output, err := managedprocess.RunCommandWithTimeout(juiceFsSourceUnmountTimeout, "juicefs", "umount", "--force", localPath)
	if err != nil {
		errs = errors.Join(errs, fmt.Errorf("error executing juicefs umount: %v, output: %s", err, string(output)))
	}

	if s.mountCmd != nil {
		if err := s.mountCmd.Wait(juiceFsSourceUnmountTimeout); errors.Is(err, managedprocess.ErrStillRunning) {
			if terminateErr := s.mountCmd.Terminate(juiceFsSourceUnmountTimeout); terminateErr != nil {
				errs = errors.Join(errs, fmt.Errorf("error terminating juicefs source process: %w, output: %s", terminateErr, s.mountCmd.OutputString()))
			}
		}
		s.mountCmd = nil
	}

	Logger.Infof("JuiceFS filesystem unmounted from: '%s'", localPath)
	return errs
}
