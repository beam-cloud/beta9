package storage

import (
	"fmt"
	"os"
	"os/exec"

	"github.com/beam-cloud/beta9/pkg/types"
)

const (
	mountpointBinary = "ms3"
)

type MountPointStorage struct {
	mountCmd *exec.Cmd
	config   types.MountPointConfig
}

func NewMountPointStorage(config types.MountPointConfig) (Storage, error) {
	return &MountPointStorage{
		config: config,
	}, nil
}

func (s *MountPointStorage) Mount(localPath string) error {
	// NOTE: this is called to force unmount previous mounts
	// It seems like mountpoint doesn't clean up gracefully by itself
	s.Unmount(localPath)

	if _, err := os.Stat(localPath); os.IsNotExist(err) {
		err := os.MkdirAll(localPath, 0755)
		if err != nil {
			return err
		}
	}

	cmdArgs := newMountpointCmdArgs(s, localPath)
	s.mountCmd = exec.Command(mountpointBinary, cmdArgs...)

	if s.config.AccessKey != "" || s.config.SecretKey != "" {
		s.mountCmd.Env = append(s.mountCmd.Env,
			fmt.Sprintf("AWS_ACCESS_KEY_ID=%s", s.config.AccessKey),
			fmt.Sprintf("AWS_SECRET_ACCESS_KEY=%s", s.config.SecretKey),
		)
	}

	output, err := s.mountCmd.CombinedOutput()
	if err != nil {
		// Cleanup the temporary mountpoint directory if the mount fails
		os.RemoveAll(localPath)
		return fmt.Errorf("%+v, %s", err, string(output))
	}

	return nil
}

func (s *MountPointStorage) Format(fsName string) error {
	return nil
}

func (s *MountPointStorage) Mode() string {
	return StorageModeMountPoint
}

func (s *MountPointStorage) Unmount(localPath string) error {
	cmd := exec.Command("umount", localPath)

	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("error executing mount-s3 umount: %v, output: %s", err, string(output))
	}

	os.RemoveAll(localPath)

	return nil
}

func newMountpointCmdArgs(s *MountPointStorage, localPath string) []string {
	cmdArgs := []string{s.config.BucketName, localPath, "--allow-other", "--log-directory=/var/log/", "--upload-checksums=off"}
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
