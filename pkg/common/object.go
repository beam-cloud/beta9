package common

import (
	"context"
	"os"
	"path"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/mholt/archiver/v3"
)

func ExtractObjectFile(ctx context.Context, objectId, workspaceName, destPath string) error {
	if _, err := os.Stat(destPath); !os.IsNotExist(err) {
		// Folder already exists, so skip extraction
		return nil
	}

	os.MkdirAll(destPath, 0755)

	// Check if the object file exists
	objectFilePath := path.Join(types.DefaultObjectPath, workspaceName, objectId)
	if _, err := os.Stat(objectFilePath); os.IsNotExist(err) {
		return err
	}

	zip := archiver.NewZip()
	if err := zip.Unarchive(objectFilePath, destPath); err != nil {
		return err
	}

	return nil
}
