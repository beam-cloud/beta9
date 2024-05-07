package providers

import (
	"context"
	"log"
	"time"

	"github.com/beam-cloud/beta9/internal/network"
	"github.com/beam-cloud/beta9/internal/repository"
	"github.com/beam-cloud/beta9/internal/types"
)

type ExternalProvider struct {
	Ctx                  context.Context
	Name                 string
	ClusterName          string
	AppConfig            types.AppConfig
	ProviderRepo         repository.ProviderRepository
	Tailscale            *network.Tailscale
	WorkerRepo           repository.WorkerRepository
	ListMachinesFunc     func(ctx context.Context, poolName string) (map[string]string, error)
	TerminateMachineFunc func(ctx context.Context, poolName, instanceId, machineId string) error
}

type ExternalProviderConfig struct {
	Name                 string
	ClusterName          string
	AppConfig            types.AppConfig
	TailScale            *network.Tailscale
	ProviderRepo         repository.ProviderRepository
	WorkerRepo           repository.WorkerRepository
	ListMachinesFunc     func(ctx context.Context, poolName string) (map[string]string, error)
	TerminateMachineFunc func(ctx context.Context, poolName, instanceId, machineId string) error
}

func NewExternalProvider(ctx context.Context, cfg *ExternalProviderConfig) *ExternalProvider {
	return &ExternalProvider{
		Ctx:                  ctx,
		Name:                 cfg.Name,
		ClusterName:          cfg.ClusterName,
		AppConfig:            cfg.AppConfig,
		Tailscale:            cfg.TailScale,
		ProviderRepo:         cfg.ProviderRepo,
		WorkerRepo:           cfg.WorkerRepo,
		ListMachinesFunc:     cfg.ListMachinesFunc,
		TerminateMachineFunc: cfg.TerminateMachineFunc,
	}
}

func (p *ExternalProvider) GetName() string {
	return p.Name
}

func (p *ExternalProvider) Reconcile(ctx context.Context, poolName string) {
	ticker := time.NewTicker(reconcileInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			machines, err := p.ListMachinesFunc(ctx, poolName)
			if err != nil {
				log.Printf("<provider %s>: unable to list machines - %v\n", p.Name, err)
				continue
			}

			for machineId, instanceId := range machines {
				func() {
					err := p.ProviderRepo.SetMachineLock(string(types.ProviderEC2), poolName, machineId)
					if err != nil {
						return
					}
					defer p.ProviderRepo.RemoveMachineLock(string(types.ProviderEC2), poolName, machineId)

					_, err = p.ProviderRepo.GetMachine(string(types.ProviderEC2), poolName, machineId)
					if err != nil {
						p.TerminateMachineFunc(ctx, poolName, instanceId, machineId)
						return
					}

					workers, err := p.WorkerRepo.GetAllWorkersOnMachine(machineId)
					if err != nil || len(workers) > 0 {
						return
					}

					if len(workers) == 0 {
						p.TerminateMachineFunc(ctx, poolName, instanceId, machineId)
						return
					}
				}()
			}
		}
	}
}
