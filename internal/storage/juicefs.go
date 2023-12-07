package storage

import (
	"fmt"
	"log"
	"os/exec"

	"github.com/beam-cloud/beam/internal/common"
)

type JuiceFsStorage struct {
	mountCmd *exec.Cmd
}

func NewJuiceFsStorage() (Storage, error) {
	return &JuiceFsStorage{}, nil
}

func (s *JuiceFsStorage) Mount(localPath string) error {
	s.mountCmd = exec.Command(
		"juicefs",
		"mount",
		common.Secrets().Get("BEAM_JUICEFS_REDIS"),
		localPath,
	)

	go func() {
		output, err := s.mountCmd.CombinedOutput()
		if err != nil {
			log.Printf("error executing juicefs mount: %v, output: %s", err, string(output))
		}
	}()

	log.Printf("Juicefs filesystem is being mounted to: '%s'\n", localPath)
	return nil
}

func (s *JuiceFsStorage) Format(fsName string) error {
	log.Printf("Formatting juicefs filesystem with name: '%s'\n", fsName)

	cmd := exec.Command(
		"juicefs",
		"format",
		"--storage", "s3",
		"--bucket", common.Secrets().Get("BEAM_JUICEFS_S3_BUCKET"),
		"--access-key", common.Secrets().Get("BEAM_JUICEFS_AWS_ACCESS_KEY"),
		"--secret-key", common.Secrets().Get("BEAM_JUICEFS_AWS_SECRET_KEY"),
		common.Secrets().Get("BEAM_JUICEFS_REDIS"),
		fsName,
	)

	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("error executing juicefs format: %v, output: %s", err, string(output))
	}

	log.Printf("Juicefs filesystem formatted: '%s'\n", fsName)
	return nil
}

func (s *JuiceFsStorage) Unmount(localPath string) error {
	cmd := exec.Command("juicefs", "umount", localPath)

	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("error executing juicefs umount: %v, output: %s", err, string(output))
	}

	log.Printf("Juicefs filesystem unmounted from: '%s'\n", localPath)
	return nil
}
