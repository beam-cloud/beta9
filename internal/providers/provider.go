package providers

import "context"

type Provider interface {
	ListMachines() error
	TerminateMachine(ctx context.Context, id string) error
	ProvisionMachine(ctx context.Context, poolName string, machineId string) error
}
