package providers

import (
	"context"
	"errors"
	"log"

	"github.com/beam-cloud/beta9/pkg/network"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
)

type LambdaLabsProvider struct {
	*ExternalProvider
}

func NewLambdaLabsProvider(ctx context.Context, appConfig types.AppConfig, providerRepo repository.ProviderRepository, workerRepo repository.WorkerRepository, tailscale *network.Tailscale) (*LambdaLabsProvider, error) {
	lambdaLabsProvider := &LambdaLabsProvider{}

	baseProvider := NewExternalProvider(ctx, &ExternalProviderConfig{
		Name:                 string(types.ProviderLambdaLabs),
		ClusterName:          appConfig.ClusterName,
		AppConfig:            appConfig,
		TailScale:            tailscale,
		ProviderRepo:         providerRepo,
		WorkerRepo:           workerRepo,
		ListMachinesFunc:     lambdaLabsProvider.listMachines,
		TerminateMachineFunc: lambdaLabsProvider.TerminateMachine,
	})
	lambdaLabsProvider.ExternalProvider = baseProvider

	return lambdaLabsProvider, nil
}

func (p *LambdaLabsProvider) ProvisionMachine(ctx context.Context, poolName, token string, compute types.ProviderComputeRequest) (string, error) {
	return "", errors.New("not implemented")
}

func (p *LambdaLabsProvider) listMachines(ctx context.Context, poolName string) (map[string]string, error) {
	return map[string]string{}, errors.New("not implemented")
}

func (p *LambdaLabsProvider) TerminateMachine(ctx context.Context, poolName, instanceId, machineId string) error {
	if instanceId == "" {
		return errors.New("invalid instance ID")
	}

	err := p.ProviderRepo.RemoveMachine(p.Name, poolName, machineId)
	if err != nil {
		log.Printf("<provider %s>: Unable to remove machine state <machineId: %s>: %+v\n", p.Name, machineId, err)
		return err
	}

	log.Printf("<provider %s>: Terminated machine <machineId: %s> due to inactivity\n", p.Name, machineId)
	return nil
}
