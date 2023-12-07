package storage

import (
	"fmt"
	"os/exec"

	"github.com/beam-cloud/beam/internal/common"
)

type JuiceFsStorage struct {
}

func NewJuiceFsStorage() (Storage, error) {
	return &JuiceFsStorage{}, nil
}

func (s *JuiceFsStorage) Mount(path string) error {
	return nil
}

func (s *JuiceFsStorage) Format(fsName string) error {
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

	return nil
}
