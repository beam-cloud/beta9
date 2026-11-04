package worker

import (
	"os"
	"strconv"

	"github.com/beam-cloud/beta9/pkg/clients"
)

func checkpointUploadOptions() clients.StorageUploadOptions {
	partMiB, err := strconv.ParseInt(os.Getenv("BETA9_CHECKPOINT_MULTIPART_MIB"), 10, 64)
	if err != nil || partMiB < 5 || partMiB > 64 {
		return clients.StorageUploadOptions{}
	}
	workers, err := strconv.Atoi(os.Getenv("BETA9_CHECKPOINT_MULTIPART_CONCURRENCY"))
	if err != nil || workers < 1 || workers > 8 {
		workers = 2
	}
	return clients.StorageUploadOptions{PartSize: partMiB << 20, Concurrency: workers}
}
