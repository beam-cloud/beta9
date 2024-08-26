package storage

import (
	"fmt"
	"os"
	"os/exec"

	"github.com/beam-cloud/beta9/pkg/types"
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

	endpoint := ""
	if s.config.BucketURL != "" {
		endpoint = fmt.Sprintf("--endpoint-url=%s", s.config.BucketURL)
	}

	// FIXME: cache? https://github.com/awslabs/mountpoint-s3/blob/main/doc/CONFIGURATION.md#caching-configuration
	s.mountCmd = exec.Command(
		"ms3",
		s.config.S3Bucket,
		localPath,
		endpoint,
		"--allow-other",
		"--log-directory=/var/log/",
		"--upload-checksums=off",
	)

	if s.config.AccessKey != "" || s.config.SecretKey != "" {
		s.mountCmd.Env = append(s.mountCmd.Env,
			fmt.Sprintf("AWS_ACCESS_KEY_ID=%s", s.config.AccessKey),
			fmt.Sprintf("AWS_SECRET_ACCESS_KEY=%s", s.config.SecretKey),
		)
	}

	output, err := s.mountCmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("error: %+v, %s", err, string(output))
	}

	return nil
}

func (s *MountPointStorage) Format(fsName string) error {
	return nil
}

func (s *MountPointStorage) Unmount(localPath string) error {
	cmd := exec.Command("umount", localPath)

	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("error executing mount-s3 umount: %v, output: %s", err, string(output))
	}

	return nil
}
