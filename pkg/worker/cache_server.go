package worker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/beam-cloud/beta9/pkg/common"
	repo "github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog/log"
)

// RunCacheServer runs the worker cache manager without accepting user
// containers. It uses the same worker image and cache registration path as a
// normal worker, but never starts the container runtime.
func RunCacheServer() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	configManager, err := common.NewConfigManager[types.AppConfig]()
	if err != nil {
		return err
	}
	config := configManager.GetConfig()

	coordinatorToken := config.Cache.Coordinator.Token
	if coordinatorToken == "" {
		return errors.New("cache.coordinator.token is required for cache server mode")
	}

	workerRepoClient, err := NewWorkerRepositoryClient(context.TODO(), config, coordinatorToken)
	if err != nil {
		return err
	}

	poolName := os.Getenv(types.WorkerPoolEnv)
	if poolName == "" {
		poolName = "default"
	}
	poolConfig := config.Worker.Pools[poolName]

	workerID := os.Getenv(types.WorkerIDEnv)
	if workerID == "" {
		workerID = fmt.Sprintf("cache-server-%s", cacheNodeID())
	}

	podAddr, err := GetPodAddr()
	if err != nil {
		return err
	}

	eventRepo := repo.NewEventClientRepo(config)
	manager := NewWorkerCacheManager(ctx, config, poolConfig, workerRepoClient, eventRepo, nil, workerID, poolName, podAddr)
	client, err := manager.Start()
	if err != nil {
		return err
	}
	if client == nil {
		return errors.New("cache server mode requires cache to be enabled")
	}

	log.Info().
		Str("worker_id", workerID).
		Str("pool_name", poolName).
		Str("node_id", manager.nodeID).
		Str("locality", manager.locality).
		Bool("running_cache_server", manager.runningCacheServer()).
		Msg("cache server process started")

	<-ctx.Done()
	stop()
	log.Info().Msg("cache server shutdown signal received")
	if err := manager.Drain(); err != nil {
		log.Warn().Err(err).Msg("cache server drain failed")
	}

	return manager.Close()
}
