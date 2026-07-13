package compute

import (
	"context"
	"sync"
	"time"

	"github.com/beam-cloud/beta9/pkg/clients"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/network"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/scheduler"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog/log"
)

type Service struct {
	appConfig            types.AppConfig
	backendRepo          repository.BackendRepository
	containerRepo        repository.ContainerRepository
	scheduler            *scheduler.Scheduler
	eventRepo            repository.EventRepository
	workerRepo           repository.WorkerRepository
	workerPoolRepo       repository.WorkerPoolRepository
	usageMetricsRepo     repository.UsageMetricsRepository
	computeRepo          repository.ComputeRepository
	keyEventManager      *common.KeyEventManager
	redisClient          *common.RedisClient
	telemetryCredentials telemetryCredentialIssuer
	billing              managedComputeBillingClient
	rentalUsage          *clients.MarketplaceUsageClient
	tailscale            *network.Tailscale
	routePrewarm         routePrewarmer
	reconcileOnce        sync.Once
}

type Options struct {
	Config           types.AppConfig
	BackendRepo      repository.BackendRepository
	ContainerRepo    repository.ContainerRepository
	Scheduler        *scheduler.Scheduler
	EventRepo        repository.EventRepository
	WorkerRepo       repository.WorkerRepository
	WorkerPoolRepo   repository.WorkerPoolRepository
	UsageMetricsRepo repository.UsageMetricsRepository
	ComputeRepo      repository.ComputeRepository
	KeyEventManager  *common.KeyEventManager
	RedisClient      *common.RedisClient
	Tailscale        *network.Tailscale
}

func New(opts Options) *Service {
	service := &Service{
		appConfig:        opts.Config,
		backendRepo:      opts.BackendRepo,
		containerRepo:    opts.ContainerRepo,
		scheduler:        opts.Scheduler,
		eventRepo:        opts.EventRepo,
		workerRepo:       opts.WorkerRepo,
		workerPoolRepo:   opts.WorkerPoolRepo,
		usageMetricsRepo: opts.UsageMetricsRepo,
		computeRepo:      opts.ComputeRepo,
		keyEventManager:  opts.KeyEventManager,
		redisClient:      opts.RedisClient,
		billing:          newManagedComputeBillingClient(opts.Config.ManagedCompute.Billing),
		rentalUsage:      clients.NewMarketplaceUsageClient(opts.Config.ManagedCompute.Billing),
		tailscale:        opts.Tailscale,
		routePrewarm:     routePrewarmer{lastAttempt: map[string]time.Time{}},
	}
	service.telemetryCredentials = newTelemetryCredentialIssuer(opts.Config.Database.S2)
	return service
}

func (s *Service) Start(ctx context.Context) {
	if s == nil || s.computeRepo == nil {
		return
	}
	s.reconcileOnce.Do(func() {
		if err := s.ReconcilePlatformPools(ctx); err != nil {
			log.Error().Err(err).Msg("platform pool reconciliation failed")
		}
		go s.runReconciler(ctx)
	})
}
