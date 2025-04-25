package worker

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	types "github.com/beam-cloud/beta9/pkg/types"
	blobcache "github.com/beam-cloud/blobcache-v2/pkg"
	"github.com/beam-cloud/go-runc"
	"github.com/hashicorp/go-multierror"
	"github.com/panjf2000/ants/v2"
	"github.com/rs/zerolog/log"
)

const (
	CacheWorkerPoolSize    = 20
	minNvidiaDriverVersion = 570
)

type NvidiaCRIUManager struct {
	runcHandle       runc.Runc
	cpStorageConfig  types.CheckpointStorageConfig
	fileCacheManager *FileCacheManager
	gpuCnt           int
	available        bool
}

func InitializeNvidiaCRIU(ctx context.Context, config types.CRIUConfig, fileCacheManager *FileCacheManager) (CRIUManager, error) {
	gpuCnt := 0
	var err error
	gpuCntEnv := os.Getenv(gpuCntEnvKey)
	if gpuCntEnv != "" {
		gpuCnt, err = strconv.Atoi(gpuCntEnv)
		if err != nil {
			return nil, fmt.Errorf("failed to parse GPU_COUNT: %v", err)
		}
	}

	available := crCompatible(gpuCnt)

	runcHandle := runc.Runc{}
	return &NvidiaCRIUManager{runcHandle: runcHandle, cpStorageConfig: config.Storage, fileCacheManager: fileCacheManager, gpuCnt: gpuCnt, available: available}, nil
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
	imagePath := filepath.Join(c.cpStorageConfig.MountPath, opts.request.StubId)
	workDir := filepath.Join("/tmp", imagePath)
	err := c.setupRestoreWorkDir(workDir)
	if err != nil {
		return -1, err
	}

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
			AllowOpenTCP: true,
			// Logs, irmap cache, sockets for lazy server and other go to working dir
			// must be overriden bc blobcache is read-only
			WorkDir:      workDir,
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
	return c.available
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
	if _, err := os.Stat(cachedCheckpointPath); err != nil {
		log.Info().Str("container_id", containerId).Msgf("checkpoint not cached nearby: %s", cachedCheckpointPath)
		return false
	}
	return true
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
			sourcePath := path[1:]

			defer wg.Done()
			_, err := client.StoreContentFromFUSE(blobcache.ContentSourceFUSE{
				Path: sourcePath,
			}, blobcache.StoreContentOptions{
				CreateCacheFSEntry: true,
				RoutingKey:         sourcePath,
				Lock:               true,
			})
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
	err = storeContentErr.ErrorOrNil()
	if err != nil {
		log.Error().Str("container_id", containerId).Msgf("error caching checkpoint: %v", err)
	} else {
		log.Info().Str("container_id", containerId).Msgf("cached checkpoint: %s", checkpointPath)
	}
	return err
}

// getNvidiaDriverVersion returns the NVIDIA driver version as an integer
func getNvidiaDriverVersion() (int, error) {
	cmd := exec.Command("nvidia-smi", "--query-gpu=driver_version", "--format=csv,noheader")
	output, err := cmd.Output()
	if err != nil {
		return 0, fmt.Errorf("failed to execute nvidia-smi: %v", err)
	}

	versionStr := strings.TrimSpace(string(output))
	parts := strings.Split(versionStr, ".")
	if len(parts) < 1 {
		return 0, fmt.Errorf("unexpected driver version format: %s", versionStr)
	}

	majorVersion, err := strconv.Atoi(parts[0])
	if err != nil {
		return 0, fmt.Errorf("failed to parse driver version: %v", err)
	}

	return majorVersion, nil
}

func crCompatible(gpuCnt int) bool {
	if gpuCnt == 0 {
		return true
	}

	// Check NVIDIA driver version is >= 570
	driverVersion, err := getNvidiaDriverVersion()
	if err != nil {
		log.Error().Msgf("Failed to get NVIDIA driver version: %v", err)
		return false
	}

	if driverVersion < minNvidiaDriverVersion {
		log.Error().Msgf("NVIDIA driver version %d is less than required version 570", driverVersion)
		return false
	}

	return true
}

func (c *NvidiaCRIUManager) setupRestoreWorkDir(workDir string) error {
	if _, err := os.Stat(workDir); os.IsNotExist(err) {
		err := os.MkdirAll(workDir, 0755)
		if err != nil {
			return err
		}
	}

	return nil
}
