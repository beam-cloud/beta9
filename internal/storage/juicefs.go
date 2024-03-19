package storage

import (
	"fmt"
	"log"
	"os/exec"

	"github.com/beam-cloud/beta9/internal/types"
)

type JuiceFsStorage struct {
	mountCmd *exec.Cmd
	config   types.JuiceFSConfig
}

func NewJuiceFsStorage(config types.JuiceFSConfig) (Storage, error) {
	return &JuiceFsStorage{
		config: config,
	}, nil
}

func (s *JuiceFsStorage) Mount(localPath string) error {
	s.mountCmd = exec.Command(
		"juicefs",
		"mount",
		s.config.RedisURI,
		localPath,
	)

	go func() {
		output, err := s.mountCmd.CombinedOutput()
		if err != nil {
			log.Printf("error executing juicefs mount: %v, output: %s", err, string(output))
		}
	}()

	log.Printf("JuiceFS filesystem is being mounted to: '%s'\n", localPath)
	return nil
}

func (s *JuiceFsStorage) Format(fsName string) error {
	log.Printf("Formatting JuiceFS filesystem with name: '%s'\n", fsName)

	cmd := exec.Command(
		"juicefs",
		"format",
		"--storage", "s3",
		"--bucket", s.config.AWSS3Bucket,
		s.config.RedisURI,
		fsName,
	)

	if s.config.AWSAccessKey != "" || s.config.AWSSecretKey != "" {
		cmd.Args = append(
			cmd.Args,
			"--access-key", s.config.AWSAccessKey,
			"--secret-key", s.config.AWSSecretKey,
		)
	}

	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("error executing juicefs format: %v, output: %s", err, string(output))
	}

	log.Printf("JuiceFS filesystem formatted: '%s'\n", fsName)
	return nil
}

func (s *JuiceFsStorage) Unmount(localPath string) error {
	cmd := exec.Command("juicefs", "umount", localPath)

	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("error executing juicefs umount: %v, output: %s", err, string(output))
	}

	log.Printf("JuiceFS filesystem unmounted from: '%s'\n", localPath)
	return nil
}
