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
	redis "github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
)

const (
	managedPoolReconcileRetryInterval = 5 * time.Second
	managedPoolReconcileInterval      = 30 * time.Second
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
	managedPoolRepo      repository.ManagedPoolRepository
	keyEventManager      *common.KeyEventManager
	redisClient          *common.RedisClient
	telemetryCredentials telemetryCredentialIssuer
	billing              managedComputeBillingClient
	rentalUsage          *clients.MarketplaceUsageClient
	tailscale            *network.Tailscale
	routePrewarm         routePrewarmer
	reconcileOnce        sync.Once
	managedPoolReady     chan struct{}
	managedPoolReadyOnce sync.Once
	managedPoolSyncMu    sync.Mutex
	managedPoolInstances map[string]string
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
	ManagedPoolRepo  repository.ManagedPoolRepository
	KeyEventManager  *common.KeyEventManager
	RedisClient      *common.RedisClient
	Tailscale        *network.Tailscale
}

func New(opts Options) *Service {
	service := &Service{
		appConfig:            opts.Config,
		backendRepo:          opts.BackendRepo,
		containerRepo:        opts.ContainerRepo,
		scheduler:            opts.Scheduler,
		eventRepo:            opts.EventRepo,
		workerRepo:           opts.WorkerRepo,
		workerPoolRepo:       opts.WorkerPoolRepo,
		usageMetricsRepo:     opts.UsageMetricsRepo,
		computeRepo:          opts.ComputeRepo,
		managedPoolRepo:      opts.ManagedPoolRepo,
		keyEventManager:      opts.KeyEventManager,
		redisClient:          opts.RedisClient,
		billing:              newManagedComputeBillingClient(opts.Config.ManagedCompute.Billing),
		rentalUsage:          clients.NewMarketplaceUsageClient(opts.Config.ManagedCompute.Billing),
		tailscale:            opts.Tailscale,
		routePrewarm:         routePrewarmer{lastAttempt: map[string]time.Time{}},
		managedPoolReady:     make(chan struct{}),
		managedPoolInstances: map[string]string{},
	}
	service.telemetryCredentials = newTelemetryCredentialIssuer(opts.Config.Database.S2)
	return service
}

func (s *Service) Start(ctx context.Context) {
	if s == nil {
		return
	}
	s.reconcileOnce.Do(func() {
		if s.computeRepo != nil {
			go s.runReconciler(ctx)
		}
		if err := s.requireManagedPoolReconciler(); err != nil {
			log.Error().Err(err).Msg("managed pool reconciliation is unavailable")
			return
		}
		go s.runManagedPoolReconciler(ctx)
	})
}

// Ready closes after this replica has loaded every managed pool into its local scheduler.
func (s *Service) Ready() <-chan struct{} {
	if s == nil || s.managedPoolReady == nil {
		ready := make(chan struct{})
		close(ready)
		return ready
	}
	return s.managedPoolReady
}

func (s *Service) markManagedPoolsReady() {
	if s == nil || s.managedPoolReady == nil {
		return
	}
	s.managedPoolReadyOnce.Do(func() { close(s.managedPoolReady) })
}

func (s *Service) runManagedPoolReconciler(ctx context.Context) {
	var messages <-chan *redis.Message
	var subscriptionErrors <-chan error
	if s.redisClient != nil {
		messages, subscriptionErrors = s.redisClient.Subscribe(ctx, common.RedisKeys.ComputeManagedPoolUpdates())
	}

	timer := time.NewTimer(0)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-messages:
			if !ok {
				messages = nil
				continue
			}
			resetManagedPoolTimer(timer, 0)
		case err, ok := <-subscriptionErrors:
			if !ok {
				subscriptionErrors = nil
				continue
			}
			log.Warn().Err(err).Msg("managed pool update subscription failed; periodic reconciliation remains active")
		case <-timer.C:
			started := time.Now()
			if err := s.ReconcileManagedPools(ctx); err != nil {
				log.Warn().Err(err).Msg("managed pool reconciliation failed")
				resetManagedPoolTimer(timer, managedPoolReconcileRetryInterval)
				continue
			}
			s.markManagedPoolsReady()
			log.Debug().Dur("duration", time.Since(started)).Msg("managed pools reconciled")
			resetManagedPoolTimer(timer, managedPoolReconcileInterval)
		}
	}
}

func resetManagedPoolTimer(timer *time.Timer, delay time.Duration) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timer.Reset(delay)
}
