package scheduler

import (
	"context"
	"time"

	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog/log"
)

type PoolHealthMonitorOptions struct {
	Controller       WorkerPoolController
	WorkerPoolConfig *types.WorkerPoolConfig
	WorkerConfig     *types.WorkerConfig
	WorkerRepo       repository.WorkerRepository
	WorkerPoolRepo   repository.WorkerPoolRepository
	ProviderRepo     repository.ProviderRepository
}

type PoolHealthMonitor struct {
	ctx              context.Context
	wpc              WorkerPoolController
	workerPoolConfig *types.WorkerPoolConfig
	workerConfig     *types.WorkerConfig
	workerRepo       repository.WorkerRepository
	workerPoolRepo   repository.WorkerPoolRepository
	providerRepo     repository.ProviderRepository
}

func NewPoolHealthMonitor(opts PoolHealthMonitorOptions) *PoolHealthMonitor {
	return &PoolHealthMonitor{
		ctx:              opts.Controller.Context(),
		wpc:              opts.Controller,
		workerPoolConfig: opts.WorkerPoolConfig,
		workerConfig:     opts.WorkerConfig,
		workerRepo:       opts.WorkerRepo,
		providerRepo:     opts.ProviderRepo,
		workerPoolRepo:   opts.WorkerPoolRepo,
	}
}

func (p *PoolHealthMonitor) Start() {
	ticker := time.NewTicker(poolHealthCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-p.ctx.Done():
			return
		case <-ticker.C:
			p.checkPoolHealth()
		}
	}
}

func (p *PoolHealthMonitor) checkPoolHealth() {
	log.Info().Msg("checking pool health for pool " + p.wpc.Name())
	pool, err := p.workerPoolRepo.GetWorkerPool(p.ctx, p.wpc.Name())
	if err != nil {
		log.Error().Msg("error getting worker pool " + p.wpc.Name() + ": " + err.Error())
		return
	}

	log.Printf("pool: %+v", pool)

	log.Info().Msg("pool health check complete for pool " + p.wpc.Name())
}
