package worker

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"

	types "github.com/beam-cloud/beta9/pkg/types"
	"github.com/beam-cloud/go-runc"
	"github.com/hashicorp/go-multierror"
	"github.com/panjf2000/ants/v2"
	"github.com/rs/zerolog/log"
)

const CacheWorkerPoolSize = 20

type NvidiaCRIUManager struct {
	runcHandle       runc.Runc
	cpStorageConfig  types.CheckpointStorageConfig
	fileCacheManager *FileCacheManager
}

func InitializeNvidiaCRIU(ctx context.Context, config types.CRIUConfig, fileCacheManager *FileCacheManager) (CRIUManager, error) {
	runcHandle := runc.Runc{}
	return &NvidiaCRIUManager{runcHandle: runcHandle, cpStorageConfig: config.Storage, fileCacheManager: fileCacheManager}, nil
}

func (c *NvidiaCRIUManager) CreateCheckpoint(ctx context.Context, request *types.ContainerRequest) (string, error) {
	checkpointPath := fmt.Sprintf("%s/%s", c.cpStorageConfig.MountPath, request.StubId)
	err := c.runcHandle.Checkpoint(ctx, request.ContainerId, &runc.CheckpointOpts{
		LeaveRunning: true,
		SkipInFlight: true,
		AllowOpenTCP: true,
		LinkRemap:    true,
		ImagePath:    checkpointPath,
	})
	if err != nil {
		return "", err
	}

	return checkpointPath, nil
}

func (c *NvidiaCRIUManager) RestoreCheckpoint(ctx context.Context, opts *RestoreOpts) (int, error) {
	bundlePath := filepath.Dir(opts.configPath)
	imagePath := fmt.Sprintf("%s/%s", c.cpStorageConfig.MountPath, opts.request.StubId)
	originalImagePath := imagePath

	if c.fileCacheManager.CacheAvailable() {
		cachedCheckpointPath := filepath.Join(baseFileCachePath, imagePath)
		checkpointCached := c.checkpointCached(cachedCheckpointPath, opts.request.ContainerId)
		if checkpointCached {
			imagePath = cachedCheckpointPath
		} else {
			go c.cacheDir(opts.request.ContainerId, imagePath)
		}
	}

	exitCode, err := c.runcHandle.Restore(ctx, opts.request.ContainerId, bundlePath, &runc.RestoreOpts{
		CheckpointOpts: runc.CheckpointOpts{
			WorkDir:      originalImagePath,
			ImagePath:    imagePath,
			OutputWriter: opts.runcOpts.OutputWriter,
		},
		Started: opts.runcOpts.Started,
	})
	if err != nil {
		return -1, err
	}

	return exitCode, nil
}

func (c *NvidiaCRIUManager) Available() bool {
	// TODO: check for correct version of criu, correct driver version, etc.
	return true
}

func (c *NvidiaCRIUManager) Run(ctx context.Context, request *types.ContainerRequest, bundlePath string, runcOpts *runc.CreateOpts) (int, error) {
	exitCode, err := c.runcHandle.Run(ctx, request.ContainerId, bundlePath, runcOpts)
	if err != nil {
		return -1, err
	}

	return exitCode, nil
}

// Caches a checkpoint nearby if the file cache is available
func (c *NvidiaCRIUManager) CacheCheckpoint(containerId, checkpointPath string) (string, error) {
	if !c.fileCacheManager.CacheAvailable() {
		log.Warn().Str("container_id", containerId).Msg("file cache unavailable, skipping checkpoint caching")
		return checkpointPath, nil
	}

	cachedCheckpointPath := filepath.Join(baseFileCachePath, checkpointPath)
	checkpointCached := c.checkpointCached(cachedCheckpointPath, containerId)
	if checkpointCached {
		return cachedCheckpointPath, nil
	}

	err := c.cacheDir(containerId, checkpointPath)
	if err != nil {
		return "", err
	}
	return cachedCheckpointPath, nil
}

func (c *NvidiaCRIUManager) checkpointCached(cachedCheckpointPath string, containerId string) bool {
	// If the checkpoint is already cached, we can use that path without the extra grpc call
	if _, err := os.Stat(cachedCheckpointPath); err == nil {
		log.Info().Str("container_id", containerId).Msgf("checkpoint already cached: %s", cachedCheckpointPath)
		return true
	}
	return false
}

func (c *NvidiaCRIUManager) cacheDir(containerId, checkpointPath string) error {
	log.Info().Str("container_id", containerId).Msgf("caching checkpoint nearby: %s", checkpointPath)
	client := c.fileCacheManager.GetClient()
	wg := sync.WaitGroup{}
	p, err := ants.NewPool(CacheWorkerPoolSize)
	if err != nil {
		return err
	}
	defer p.Release()

	var storeContentErrMu sync.Mutex
	var storeContentErr *multierror.Error

	storeContentErr = multierror.Append(storeContentErr, filepath.WalkDir(checkpointPath, func(path string, info fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		wg.Add(1)
		poolSubmitErr := p.Submit(func() {
			defer wg.Done()
			_, err := client.StoreContentFromSource(path[1:], 0)
			if err != nil {
				storeContentErrMu.Lock()
				storeContentErr = multierror.Append(storeContentErr, err)
				storeContentErrMu.Unlock()
				log.Error().Err(err).Str("container_id", containerId).Msgf("error caching checkpoint file: %s", path)
			}
		})

		if poolSubmitErr != nil {
			wg.Done()
			return poolSubmitErr
		}

		return nil
	}))

	wg.Wait() // Wait for all tasks to finish
	return storeContentErr.ErrorOrNil()
}
