package cache

import (
	"fmt"
	"os"
	"os/exec"
)

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
	os.MkdirAll(localPath, 0755)

	s.mountCmd = exec.Command(
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
		s.mountCmd.Env = append(s.mountCmd.Env,
			fmt.Sprintf("AWS_ACCESS_KEY_ID=%s", s.config.AccessKey),
			fmt.Sprintf("AWS_SECRET_ACCESS_KEY=%s", s.config.SecretKey),
		)
	}

	go func() {
		output, err := s.mountCmd.CombinedOutput()
		if err != nil {
			Logger.Fatalf("error executing mount-s3 mount: %v, output: %s", err, string(output))
		}
	}()

	Logger.Infof("Mountpoint filesystem is being mounted to: '%s'", localPath)
	return nil
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
