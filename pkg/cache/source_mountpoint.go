package cache

import (
	"errors"
	"fmt"
	"os"
	"time"

	managedprocess "github.com/beam-cloud/beta9/pkg/common/process"
)

const (
	mountPointMountTimeout   time.Duration = 30 * time.Second
	mountPointUnmountTimeout time.Duration = 10 * time.Second
)

type MountPointSource struct {
	mountCmd *managedprocess.ManagedCommand
	config   MountPointConfig
}

func NewMountPointSource(config MountPointConfig) (Source, error) {
	return &MountPointSource{
		config: config,
	}, nil
}

func (s *MountPointSource) Mount(localPath string) error {
	// Clear any stale mount before remounting.
	s.Unmount(localPath)
	if err := os.MkdirAll(localPath, 0755); err != nil {
		return fmt.Errorf("failed to create mount path %s: %w", localPath, err)
	}

	args := []string{
		"mount-s3",
		s.config.BucketName,
		localPath,
		"--region",
		s.config.Region,
		"--endpoint-url",
		s.config.EndpointURL,
		"--foreground",
		"--read-only",
	}

	if s.config.ForcePathStyle {
		args = append(args, "--force-path-style")
	}

	env := []string{}
	if s.config.AccessKey != "" || s.config.SecretKey != "" {
		env = append(env,
			fmt.Sprintf("AWS_ACCESS_KEY_ID=%s", s.config.AccessKey),
			fmt.Sprintf("AWS_SECRET_ACCESS_KEY=%s", s.config.SecretKey),
		)
	}

	mountCmd, err := managedprocess.StartManagedCommand(args[0], args[1:], env)
	if err != nil {
		return err
	}
	s.mountCmd = mountCmd

	Logger.Infof("Mountpoint filesystem is being mounted to: '%s'", localPath)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	timeout := time.NewTimer(mountPointMountTimeout)
	defer timeout.Stop()

	for {
		if isMounted(localPath) {
			Logger.Infof("Mountpoint filesystem mounted to: '%s'", localPath)
			return nil
		}
		if err, done := s.mountCmd.DoneErr(); done {
			return fmt.Errorf("mount-s3 exited before filesystem was mounted to: '%s': %w, output: %s", localPath, err, s.mountCmd.OutputString())
		}

		select {
		case <-ticker.C:
		case <-timeout.C:
			output := s.mountCmd.OutputString()
			_ = s.mountCmd.Terminate(mountPointUnmountTimeout)
			s.mountCmd = nil
			return fmt.Errorf("failed to mount Mountpoint filesystem to: '%s': timed out after %s, output: %s", localPath, mountPointMountTimeout, output)
		}
	}
}

func (s *MountPointSource) Format(fsName string) error {
	return nil
}

func (s *MountPointSource) Unmount(localPath string) error {
	var errs error
	if isMounted(localPath) {
		output, err := managedprocess.RunCommandWithTimeout(mountPointUnmountTimeout, "umount", localPath)
		if err != nil {
			errs = errors.Join(errs, fmt.Errorf("error executing mount-s3 umount: %v, output: %s", err, string(output)))
		}
	}

	if s.mountCmd != nil {
		if err := s.mountCmd.Wait(mountPointUnmountTimeout); errors.Is(err, managedprocess.ErrStillRunning) {
			if terminateErr := s.mountCmd.Terminate(mountPointUnmountTimeout); terminateErr != nil {
				errs = errors.Join(errs, fmt.Errorf("error terminating mountpoint source process: %w, output: %s", terminateErr, s.mountCmd.OutputString()))
			}
		}
		s.mountCmd = nil
	}

	return errs
}
