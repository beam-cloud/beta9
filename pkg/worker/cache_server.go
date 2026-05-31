package worker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog/log"
)

// RunCacheServer runs the worker cache manager without accepting user
// containers. It uses the same worker image and cache registration path as a
// normal worker, but never starts the container runtime.
func RunCacheServer() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	configManager, err := common.NewConfigManager[types.AppConfig]()
	if err != nil {
		return err
	}
	config := configManager.GetConfig()

	workerToken := os.Getenv("WORKER_TOKEN")
	if workerToken == "" {
		return errors.New("WORKER_TOKEN is required for cache server mode")
	}

	workerRepoClient, err := NewWorkerRepositoryClient(context.TODO(), config, workerToken)
	if err != nil {
		return err
	}

	poolName := os.Getenv("WORKER_POOL_NAME")
	if poolName == "" {
		poolName = "default"
	}
	poolConfig := config.Worker.Pools[poolName]

	workerID := os.Getenv("WORKER_ID")
	if workerID == "" {
		workerID = fmt.Sprintf("cache-server-%s", cacheNodeID())
	}

	podAddr, err := GetPodAddr()
	if err != nil {
		return err
	}

	manager := NewWorkerCacheManager(ctx, config, poolConfig, workerRepoClient, workerID, poolName, podAddr)
	client, err := manager.Start()
	if err != nil {
		cancel()
		return err
	}
	if client == nil {
		cancel()
		return errors.New("cache server mode requires cache to be enabled")
	}

	log.Info().
		Str("worker_id", workerID).
		Str("pool_name", poolName).
		Str("node_id", manager.nodeID).
		Str("locality", manager.locality).
		Bool("running_cache_server", manager.runningCacheServer()).
		Msg("cache server process started")

	terminate := make(chan os.Signal, 1)
	signal.Notify(terminate, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-terminate:
		log.Info().Msg("cache server shutdown signal received")
	case <-ctx.Done():
	}

	return manager.Close()
}
