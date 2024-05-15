package common

import (
	"context"
	"os"
	"path"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/mholt/archiver/v3"
)

func ExtractObjectFile(ctx context.Context, objectId string, workspaceName string) error {
	extractedObjectPath := path.Join(types.DefaultExtractedObjectPath, workspaceName)
	os.MkdirAll(extractedObjectPath, 0644)

	destPath := path.Join(types.DefaultExtractedObjectPath, workspaceName, objectId)
	if _, err := os.Stat(destPath); !os.IsNotExist(err) {
		// Folder already exists, so skip extraction
		return nil
	}

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
