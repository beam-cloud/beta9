package cache

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"time"
)

const mountPointMountTimeout time.Duration = 30 * time.Second

type MountPointSource struct {
	mountCmd *exec.Cmd
	config   MountPointConfig
}

func NewMountPointSource(config MountPointConfig) (Source, error) {
	return &MountPointSource{
		config: config,
	}, nil
}

func (s *MountPointSource) Mount(localPath string) error {
	// NOTE: this is called to force unmount previous mounts
	// It seems like mountpoint doesn't clean up gracefully by itself
	s.Unmount(localPath)
	if err := os.MkdirAll(localPath, 0755); err != nil {
		return fmt.Errorf("failed to create mount path %s: %w", localPath, err)
	}

	mountCtx, cancelMount := context.WithCancel(context.Background())
	s.mountCmd = exec.CommandContext(
		mountCtx,
		"mount-s3",
		s.config.BucketName,
		localPath,
		"--region",
		s.config.Region,
		"--endpoint-url",
		s.config.EndpointURL,
		"--read-only",
	)

	if s.config.ForcePathStyle {
		s.mountCmd.Args = append(s.mountCmd.Args, "--force-path-style")
	}

	if s.config.AccessKey != "" || s.config.SecretKey != "" {
		s.mountCmd.Env = append(os.Environ(),
			fmt.Sprintf("AWS_ACCESS_KEY_ID=%s", s.config.AccessKey),
			fmt.Sprintf("AWS_SECRET_ACCESS_KEY=%s", s.config.SecretKey),
		)
	}

	type mountResult struct {
		output []byte
		err    error
	}
	resultCh := make(chan mountResult, 1)
	go func() {
		output, err := s.mountCmd.CombinedOutput()
		resultCh <- mountResult{output: output, err: err}
	}()

	Logger.Infof("Mountpoint filesystem is being mounted to: '%s'", localPath)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	timeout := time.NewTimer(mountPointMountTimeout)
	defer timeout.Stop()

	for {
		select {
		case result := <-resultCh:
			if result.err != nil {
				cancelMount()
				return fmt.Errorf("error executing mount-s3 mount: %w, output: %s", result.err, string(result.output))
			}
			if isMounted(localPath) {
				Logger.Infof("Mountpoint filesystem mounted to: '%s'", localPath)
				return nil
			}
			cancelMount()
			return fmt.Errorf("mount-s3 exited before filesystem was mounted to: '%s', output: %s", localPath, string(result.output))
		case <-ticker.C:
			if isMounted(localPath) {
				Logger.Infof("Mountpoint filesystem mounted to: '%s'", localPath)
				return nil
			}
		case <-timeout.C:
			cancelMount()
			return fmt.Errorf("failed to mount Mountpoint filesystem to: '%s': timed out after %s", localPath, mountPointMountTimeout)
		}
	}
}

func (s *MountPointSource) Format(fsName string) error {
	return nil
}

func (s *MountPointSource) Unmount(localPath string) error {
	cmd := exec.Command("umount", localPath)

	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("error executing mount-s3 umount: %v, output: %s", err, string(output))
	}

	return nil
}
