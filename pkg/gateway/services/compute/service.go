package compute

import (
	"context"
	"sync"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/network"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/scheduler"
	"github.com/beam-cloud/beta9/pkg/types"
)

type Service struct {
	appConfig            types.AppConfig
	backendRepo          repository.BackendRepository
	containerRepo        repository.ContainerRepository
	scheduler            *scheduler.Scheduler
	eventRepo            repository.EventRepository
	workerRepo           repository.WorkerRepository
	usageMetricsRepo     repository.UsageMetricsRepository
	computeRepo          repository.ComputeRepository
	keyEventManager      *common.KeyEventManager
	telemetryCredentials telemetryCredentialIssuer
	billing              managedComputeBillingClient
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
	UsageMetricsRepo repository.UsageMetricsRepository
	ComputeRepo      repository.ComputeRepository
	KeyEventManager  *common.KeyEventManager
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
		usageMetricsRepo: opts.UsageMetricsRepo,
		computeRepo:      opts.ComputeRepo,
		keyEventManager:  opts.KeyEventManager,
		billing:          newManagedComputeBillingClient(opts.Config.ManagedCompute.Billing),
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
		go s.runReconciler(ctx)
	})
}
