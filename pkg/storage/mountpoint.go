package storage

import (
	"errors"
	"fmt"
	"os"
	"time"

	managedprocess "github.com/beam-cloud/beta9/pkg/common/process"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog/log"
)

const (
	mountpointBinary         = "ms3"
	mountpointMountTimeout   = 30 * time.Second
	mountpointUnmountTimeout = 10 * time.Second
)

type MountPointStorage struct {
	mountCmd *managedprocess.ManagedCommand
	config   types.MountPointConfig
}

func NewMountPointStorage(config types.MountPointConfig) (Storage, error) {
	return &MountPointStorage{
		config: config,
	}, nil
}

func (s *MountPointStorage) Mount(localPath string) error {
	// Clear any stale mount before remounting.
	s.Unmount(localPath)

	if _, err := os.Stat(localPath); os.IsNotExist(err) {
		err := os.MkdirAll(localPath, 0755)
		if err != nil {
			return err
		}
	}

	cmdArgs := newMountpointCmdArgs(s, localPath)
	env := []string{}
	if s.config.AccessKey != "" || s.config.SecretKey != "" {
		env = append(env,
			fmt.Sprintf("AWS_ACCESS_KEY_ID=%s", s.config.AccessKey),
			fmt.Sprintf("AWS_SECRET_ACCESS_KEY=%s", s.config.SecretKey),
		)
	}

	mountCmd, err := managedprocess.StartManagedCommand(mountpointBinary, cmdArgs, env)
	if err != nil {
		os.RemoveAll(localPath)
		return err
	}
	s.mountCmd = mountCmd

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	timeout := time.NewTimer(mountpointMountTimeout)
	defer timeout.Stop()

	for {
		if isMounted(localPath) {
			return nil
		}
		if err, done := s.mountCmd.DoneErr(); done {
			os.RemoveAll(localPath)
			return fmt.Errorf("mountpoint exited before mount completed: %w, output: %s", err, s.mountCmd.OutputString())
		}

		select {
		case <-ticker.C:
		case <-timeout.C:
			output := s.mountCmd.OutputString()
			_ = s.mountCmd.Terminate(mountpointUnmountTimeout)
			s.mountCmd = nil
			os.RemoveAll(localPath)
			return fmt.Errorf("timed out mounting mountpoint filesystem to %q, output: %s", localPath, output)
		}
	}
}

func (s *MountPointStorage) Format(fsName string) error {
	return nil
}

func (s *MountPointStorage) Mode() string {
	return StorageModeMountPoint
}

func (s *MountPointStorage) Unmount(localPath string) error {
	var errs error

	if isMounted(localPath) {
		output, err := managedprocess.RunCommandWithTimeout(mountpointUnmountTimeout, "umount", localPath)
		if err != nil {
			errs = errors.Join(errs, fmt.Errorf("error executing mount-s3 umount: %v, output: %s", err, string(output)))
		}
	}

	if s.mountCmd != nil {
		if err := s.mountCmd.Wait(mountpointUnmountTimeout); errors.Is(err, managedprocess.ErrStillRunning) {
			if terminateErr := s.mountCmd.Terminate(mountpointUnmountTimeout); terminateErr != nil {
				errs = errors.Join(errs, fmt.Errorf("error terminating mountpoint process: %w, output: %s", terminateErr, s.mountCmd.OutputString()))
			}
		} else if err != nil {
			log.Debug().Err(err).Str("output", s.mountCmd.OutputString()).Msg("mountpoint process exited during unmount")
		}
		s.mountCmd = nil
	}

	os.RemoveAll(localPath)

	return errs
}

func newMountpointCmdArgs(s *MountPointStorage, localPath string) []string {
	cmdArgs := []string{s.config.BucketName, localPath, "--foreground", "--allow-other", "--log-directory=/var/log/", "--upload-checksums=off"}
	if s.config.ReadOnly {
		cmdArgs = append(cmdArgs, "--read-only")
	} else {
		cmdArgs = append(cmdArgs, "--allow-delete", "--allow-overwrite")
	}

	if s.config.ForcePathStyle {
		cmdArgs = append(cmdArgs, "--force-path-style")
	}

	if s.config.EndpointURL != "" {
		cmdArgs = append(cmdArgs, fmt.Sprintf("--endpoint-url=%s", s.config.EndpointURL))
	}

	if s.config.Region != "" {
		cmdArgs = append(cmdArgs, fmt.Sprintf("--region=%s", s.config.Region))
	}

	return cmdArgs
}
