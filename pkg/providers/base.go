package providers

import (
	"context"
	"log"
	"strconv"
	"time"

	"github.com/beam-cloud/beta9/pkg/network"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
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
					err := p.ProviderRepo.SetMachineLock(p.Name, poolName, machineId)
					if err != nil {
						return
					}
					defer p.ProviderRepo.RemoveMachineLock(p.Name, poolName, machineId)

					machine, err := p.ProviderRepo.GetMachine(p.Name, poolName, machineId)
					if err != nil {
						log.Printf("<provider %s>: unable to retrieve machine <machineId: %s> - %v\n", p.Name, machineId, err)
						p.TerminateMachineFunc(ctx, poolName, instanceId, machineId)
						return
					}

					workers, err := p.WorkerRepo.GetAllWorkersOnMachine(machineId)
					if err != nil {
						log.Printf("<provider %s>: unable to retrieve workers for machine <machineId: %s> - %v\n", p.Name, machineId, err)
						return
					}

					if len(workers) > 0 {
						p.ProviderRepo.SetLastWorkerSeen(p.Name, poolName, machineId)
						return
					}

					if !machine.AutoConsolidate {
						return
					}

					lastWorkerSeen, err := strconv.ParseInt(machine.LastKeepalive, 10, 64)
					if err != nil {
						return
					}

					if len(workers) == 0 && (time.Since(time.Unix(lastWorkerSeen, 0)) > types.MachineEmptyConsolidationPeriodM) {
						p.TerminateMachineFunc(ctx, poolName, instanceId, machineId)
						return
					}
				}()
			}
		}
	}
}
