package compute

import (
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/scheduler"
	"github.com/beam-cloud/beta9/pkg/types"
)

type Service struct {
	appConfig       types.AppConfig
	backendRepo     repository.BackendRepository
	containerRepo   repository.ContainerRepository
	scheduler       *scheduler.Scheduler
	eventRepo       repository.EventRepository
	workerRepo      repository.WorkerRepository
	computeRepo     repository.ComputeRepository
	keyEventManager *common.KeyEventManager
}

type Options struct {
	Config          types.AppConfig
	BackendRepo     repository.BackendRepository
	ContainerRepo   repository.ContainerRepository
	Scheduler       *scheduler.Scheduler
	EventRepo       repository.EventRepository
	WorkerRepo      repository.WorkerRepository
	ComputeRepo     repository.ComputeRepository
	KeyEventManager *common.KeyEventManager
}

func New(opts Options) *Service {
	return &Service{
		appConfig:       opts.Config,
		backendRepo:     opts.BackendRepo,
		containerRepo:   opts.ContainerRepo,
		scheduler:       opts.Scheduler,
		eventRepo:       opts.EventRepo,
		workerRepo:      opts.WorkerRepo,
		computeRepo:     opts.ComputeRepo,
		keyEventManager: opts.KeyEventManager,
	}
}
