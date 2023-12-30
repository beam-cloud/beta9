package common

import (
	"context"
	"os"
	"path"

	"github.com/beam-cloud/beam/internal/types"
	"github.com/mholt/archiver/v3"
)

func ExtractObjectFile(ctx context.Context, objectId string, contextName string) error {
	extractedObjectPath := path.Join(types.DefaultExtractedObjectPath, contextName)
	os.MkdirAll(extractedObjectPath, 0644)

	// TODO: add some sort of lock

	destPath := path.Join(types.DefaultExtractedObjectPath, contextName, objectId)
	if _, err := os.Stat(destPath); !os.IsNotExist(err) {
		// Folder already exists, so skip extraction
		return nil
	}

	// Check if the object file exists
	objectFilePath := path.Join(types.DefaultObjectPath, contextName, objectId)
	if _, err := os.Stat(objectFilePath); os.IsNotExist(err) {
		return err
	}

	zip := archiver.NewZip()
	if err := zip.Unarchive(objectFilePath, destPath); err != nil {
		return err
	}

	return nil
}
