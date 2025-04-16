package common

import (
	"archive/zip"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/mholt/archiver/v3"
	"github.com/rs/zerolog/log"
)

func ExtractObjectFile(ctx context.Context, objectPath, destPath string) error {
	if _, err := os.Stat(destPath); !os.IsNotExist(err) {
		// Folder already exists, so skip extraction
		return nil
	}

	os.MkdirAll(destPath, 0755)

	// Check if the object file exists
	if _, err := os.Stat(objectPath); os.IsNotExist(err) {
		return err
	}

	zip := archiver.NewZip()
	if err := zip.Unarchive(objectPath, destPath); err != nil {
		return err
	}

	return nil
}

func UnzipBytesToPath(destPath string, objBytes []byte, request *types.ContainerRequest) error {
	if _, err := os.Stat(destPath); !os.IsNotExist(err) {
		return nil
	}

	if err := os.MkdirAll(destPath, 0755); err != nil {
		return err
	}

	zipReader, err := zip.NewReader(bytes.NewReader(objBytes), int64(len(objBytes)))
	if err != nil {
		log.Error().Str("container_id", request.ContainerId).Err(err).Msg("error creating zip reader")
		return err
	}

	// Extract each file
	for _, zipFile := range zipReader.File {
		filePath := filepath.Join(destPath, zipFile.Name)

		if !strings.HasPrefix(filePath, filepath.Clean(destPath)+string(os.PathSeparator)) {
			return fmt.Errorf("illegal file path: %s", filePath)
		}

		if zipFile.FileInfo().IsDir() {
			if err := os.MkdirAll(filePath, zipFile.Mode()); err != nil {
				return err
			}
			continue
		}

		if err := os.MkdirAll(filepath.Dir(filePath), 0755); err != nil {
			return err
		}

		destFile, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, zipFile.Mode())
		if err != nil {
			return err
		}

		srcFile, err := zipFile.Open()
		if err != nil {
			destFile.Close()
			return err
		}

		_, err = io.Copy(destFile, srcFile)
		srcFile.Close()
		destFile.Close()

		if err != nil {
			return err
		}
	}

	return nil
}
