package storage

import (
	"fmt"
	"log"
	"os"
	"os/exec"

	"github.com/beam-cloud/beta9/internal/types"
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
	os.MkdirAll(localPath, 0755)

	s.mountCmd = exec.Command(
		"mount-s3",
		s.config.AWSS3Bucket,
		localPath,
	)

	s.mountCmd.Env = append(s.mountCmd.Env,
		fmt.Sprintf("AWS_ACCESS_KEY_ID=%s", s.config.AWSAccessKey),
		fmt.Sprintf("AWS_SECRET_ACCESS_KEY=%s", s.config.AWSSecretKey),
	)

	go func() {
		output, err := s.mountCmd.CombinedOutput()
		if err != nil {
			log.Printf("error executing mount-s3 mount: %v, output: %s", err, string(output))
		}
	}()

	log.Printf("Mountpoint filesystem is being mounted to: '%s'\n", localPath)
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
